#pragma once

#include "matching/levels.hpp"

#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace matching {

// One instrument, one serialized command stream. Parallelize independent
// instruments outside this class; this engine intentionally has one writer.
template <typename Levels>
class Engine {
public:
    Engine() { active_.reserve(4096); }

    // The ID index contains iterators into this engine. Copying it would create
    // dangling cross-engine references, so ownership is deliberately fixed.
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
    Engine(Engine&&) = delete;
    Engine& operator=(Engine&&) = delete;

    Outcome submit(const NewOrder& incoming) {
        Outcome result;
        result.sequence = ++sequence_;
        result.id = incoming.id;
        result.reject = validate(incoming);
        if (!result.accepted()) return result;
        if (active_.contains(incoming.id)) {
            result.reject = Reject::DuplicateId;
            return result;
        }
        // A conservative entry limit, including market orders, avoids accepting
        // a command whose remainder could exceed supported live-order capacity.
        if (active_.size() >= max_live_orders) {
            result.reject = Reject::Capacity;
            return result;
        }
        execute(incoming, result);
        return result;
    }

    Outcome cancel(OrderId id) {
        Outcome result;
        result.sequence = ++sequence_;
        result.id = id;
        const auto it = active_.find(id);
        if (it == active_.end()) {
            result.reject = Reject::UnknownOrder;
            return result;
        }
        remove(it);
        return result;
    }

    // quantity means NEW REMAINING quantity. A same-price reduction preserves
    // FIFO position; an increase or price change gets a new priority and can
    // immediately execute against the opposite side.
    Outcome amend(OrderId id, Price price, Quantity quantity) {
        Outcome result;
        result.sequence = ++sequence_;
        result.id = id;
        const auto it = active_.find(id);
        if (it == active_.end()) {
            result.reject = Reject::UnknownOrder;
            return result;
        }
        if (price <= 0) {
            result.reject = Reject::InvalidPrice;
            return result;
        }
        if (quantity <= 0 || quantity > max_order_quantity) {
            result.reject = Reject::InvalidQuantity;
            return result;
        }

        auto& resting = *it->second.order;
        if (price == resting.price && quantity <= resting.quantity) {
            auto* level = side_levels(resting.side).find(price);
            level->quantity -= resting.quantity - quantity;
            resting.quantity = quantity;
            result.resting = quantity;
            return result;
        }

        const auto side = resting.side;
        remove(it);
        result.priority_lost = true;
        execute(NewOrder{id, side, OrderType::Limit, price, quantity}, result);
        return result;
    }

    // Added for QuorumLog snapshots (not in the upstream matching-engine repo):
    // rebuild a book from orders() output and the command sequence, preserving
    // every FIFO priority. Only valid on an empty engine. Orders must arrive in
    // the order orders() reports them.
    void restore(std::uint64_t sequence, const std::vector<RestingOrder>& saved_orders) {
        if (sequence_ != 0 || !active_.empty()) fail("restore requires an empty engine");
        for (const auto& saved : saved_orders) {
            auto& level = side_levels(saved.side).get_or_create(saved.price);
            level.orders.push_back(saved);
            active_.emplace(saved.id,
                Locator{saved.side, saved.price, std::prev(level.orders.end())});
            level.quantity += saved.quantity;
        }
        sequence_ = sequence;
        check_invariants();
    }

    [[nodiscard]] std::size_t active_count() const { return active_.size(); }
    [[nodiscard]] bool contains(OrderId id) const { return active_.contains(id); }
    [[nodiscard]] std::uint64_t sequence() const { return sequence_; }

    [[nodiscard]] std::vector<LevelView> book() const {
        std::vector<LevelView> views;
        visit([&](Side side, const PriceLevel& level) {
            views.push_back({side, level.price, level.quantity, level.orders.size()});
        });
        return views;
    }

    [[nodiscard]] std::vector<RestingOrder> orders() const {
        std::vector<RestingOrder> views;
        views.reserve(active_.size());
        visit([&](Side, const PriceLevel& level) {
            views.insert(views.end(), level.orders.begin(), level.orders.end());
        });
        return views;
    }

    [[nodiscard]] std::uint64_t state_digest() const {
        Digest digest;
        digest.add(sequence_);
        digest.add(active_.size());
        visit([&](Side side, const PriceLevel& level) {
            digest.add(static_cast<std::uint64_t>(side));
            digest.add(static_cast<std::uint64_t>(level.price));
            digest.add(static_cast<std::uint64_t>(level.quantity));
            digest.add(level.orders.size());
            for (const auto& order : level.orders) {
                digest.add(order.id);
                digest.add(static_cast<std::uint64_t>(order.quantity));
                digest.add(order.priority);
            }
        });
        return digest.value();
    }

    // Diagnostic full scan. Keep it outside timed benchmark regions.
    void check_invariants() const {
        std::unordered_set<OrderId> seen;
        visit([&](Side side, const PriceLevel& level) {
            if (level.orders.empty()) fail("empty price level");
            Quantity sum = 0;
            std::uint64_t last_priority = 0;
            for (auto order = level.orders.begin(); order != level.orders.end(); ++order) {
                if (order->quantity <= 0 || order->quantity > max_order_quantity)
                    fail("invalid resting quantity");
                if (order->side != side || order->price != level.price)
                    fail("order stored on wrong level");
                if (order->priority <= last_priority || order->priority > sequence_)
                    fail("FIFO priority violation");
                if (!seen.insert(order->id).second) fail("duplicate active ID");
                const auto found = active_.find(order->id);
                if (found == active_.end() || found->second.side != side ||
                    found->second.price != level.price || found->second.order != order)
                    fail("ID index mismatch");
                sum += order->quantity;
                last_priority = order->priority;
            }
            if (sum != level.quantity) fail("level aggregate mismatch");
        });
        if (seen.size() != active_.size()) fail("unreachable indexed order");
        if (active_.size() > max_live_orders) fail("live-order capacity exceeded");
        if (!bids_.empty() && !asks_.empty() &&
            bids_.best(Side::Buy).price >= asks_.best(Side::Sell).price)
            fail("crossed resting book");
    }

private:
    struct Locator {
        Side side;
        Price price;
        std::list<RestingOrder>::iterator order;
    };
    using Index = std::unordered_map<OrderId, Locator>;

    Levels& side_levels(Side side) { return side == Side::Buy ? bids_ : asks_; }

    template <typename F>
    void visit(F&& fn) const {
        bids_.visit(Side::Buy, [&](const auto& level) { fn(Side::Buy, level); });
        asks_.visit(Side::Sell, [&](const auto& level) { fn(Side::Sell, level); });
    }

    static Reject validate(const NewOrder& incoming) {
        if (incoming.id == 0) return Reject::InvalidId;
        if (incoming.side != Side::Buy && incoming.side != Side::Sell)
            return Reject::InvalidSide;
        if (incoming.type != OrderType::Limit && incoming.type != OrderType::Market)
            return Reject::InvalidType;
        if ((incoming.type == OrderType::Limit && incoming.price <= 0) ||
            (incoming.type == OrderType::Market && incoming.price != 0))
            return Reject::InvalidPrice;
        if (incoming.quantity <= 0 || incoming.quantity > max_order_quantity)
            return Reject::InvalidQuantity;
        return Reject::None;
    }

    void execute(const NewOrder& incoming, Outcome& result) {
        Quantity remaining = incoming.quantity;
        auto& opposite = side_levels(incoming.side == Side::Buy ? Side::Sell : Side::Buy);
        const auto opposite_side = incoming.side == Side::Buy ? Side::Sell : Side::Buy;

        while (remaining > 0 && !opposite.empty()) {
            auto& level = opposite.best(opposite_side);
            if (incoming.type == OrderType::Limit &&
                ((incoming.side == Side::Buy && incoming.price < level.price) ||
                 (incoming.side == Side::Sell && incoming.price > level.price))) break;

            auto& maker = level.orders.front();
            const auto fill = std::min(remaining, maker.quantity);
            // Reserve output space before mutating this particular fill.
            result.trades.push_back({maker.id, incoming.id, incoming.side, level.price, fill});
            remaining -= fill;
            maker.quantity -= fill;
            level.quantity -= fill;
            if (maker.quantity == 0) {
                active_.erase(maker.id);
                level.orders.pop_front();
            }
            if (level.orders.empty()) {
                const auto price = level.price;
                opposite.erase(price);
            }
        }

        if (remaining > 0 && incoming.type == OrderType::Limit) {
            auto& level = side_levels(incoming.side).get_or_create(incoming.price);
            level.orders.push_back({incoming.id, incoming.side, incoming.price,
                                    remaining, result.sequence});
            const auto order = std::prev(level.orders.end());
            active_.emplace(incoming.id, Locator{incoming.side, incoming.price, order});
            level.quantity += remaining;
            result.resting = remaining;
        } else if (incoming.type == OrderType::Market) {
            result.unfilled = remaining;
        }
    }

    void remove(typename Index::iterator it) {
        const auto side = it->second.side;
        const auto price = it->second.price;
        auto& levels = side_levels(side);
        auto* level = levels.find(price);
        level->quantity -= it->second.order->quantity;
        level->orders.erase(it->second.order);
        active_.erase(it);
        if (level->orders.empty()) levels.erase(price);
    }

    [[noreturn]] static void fail(const char* message) {
        throw std::logic_error(std::string("book invariant: ") + message);
    }

    Levels bids_;
    Levels asks_;
    Index active_;
    std::uint64_t sequence_{};
};

using TreeEngine = Engine<TreeLevels>;
using FlatEngine = Engine<FlatLevels>;

} // namespace matching
