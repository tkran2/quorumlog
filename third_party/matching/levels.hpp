#pragma once

#include "matching/types.hpp"

#include <algorithm>
#include <list>
#include <map>
#include <memory>
#include <vector>

namespace matching {

struct PriceLevel {
    Price price{};
    Quantity quantity{};
    std::list<RestingOrder> orders; // FIFO; stable iterators for cancellation.
};

// Balanced tree: O(log P) lookup/insertion/deletion, P = price levels.
class TreeLevels {
public:
    PriceLevel& get_or_create(Price price) {
        auto [it, inserted] = levels_.try_emplace(price);
        if (inserted) it->second.price = price;
        return it->second;
    }

    PriceLevel* find(Price price) {
        const auto it = levels_.find(price);
        return it == levels_.end() ? nullptr : &it->second;
    }

    const PriceLevel* find(Price price) const {
        const auto it = levels_.find(price);
        return it == levels_.end() ? nullptr : &it->second;
    }

    PriceLevel& best(Side side) {
        return side == Side::Buy ? levels_.rbegin()->second : levels_.begin()->second;
    }

    const PriceLevel& best(Side side) const {
        return side == Side::Buy ? levels_.rbegin()->second : levels_.begin()->second;
    }

    void erase(Price price) { levels_.erase(price); }
    [[nodiscard]] bool empty() const { return levels_.empty(); }

    template <typename F>
    void visit(Side side, F&& fn) const {
        if (side == Side::Buy) {
            for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) fn(it->second);
        } else {
            for (const auto& [price, level] : levels_) {
                (void)price;
                fn(level);
            }
        }
    }

private:
    std::map<Price, PriceLevel> levels_;
};

// Sorted array of stable level pointers: O(log P) search, O(P) level insertion
// and deletion. Moving array entries does not invalidate order iterators.
class FlatLevels {
public:
    PriceLevel& get_or_create(Price price) {
        const auto it = lower_bound(price);
        if (it != levels_.end() && (*it)->price == price) return **it;
        auto level = std::make_unique<PriceLevel>();
        level->price = price;
        return **levels_.insert(it, std::move(level));
    }

    PriceLevel* find(Price price) {
        const auto it = lower_bound(price);
        return it == levels_.end() || (*it)->price != price ? nullptr : it->get();
    }

    const PriceLevel* find(Price price) const {
        const auto it = std::lower_bound(levels_.begin(), levels_.end(), price,
            [](const auto& level, Price target) { return level->price < target; });
        return it == levels_.end() || (*it)->price != price ? nullptr : it->get();
    }

    PriceLevel& best(Side side) {
        return side == Side::Buy ? *levels_.back() : *levels_.front();
    }

    const PriceLevel& best(Side side) const {
        return side == Side::Buy ? *levels_.back() : *levels_.front();
    }

    void erase(Price price) {
        const auto it = lower_bound(price);
        if (it != levels_.end() && (*it)->price == price) levels_.erase(it);
    }

    [[nodiscard]] bool empty() const { return levels_.empty(); }

    template <typename F>
    void visit(Side side, F&& fn) const {
        if (side == Side::Buy) {
            for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) fn(**it);
        } else {
            for (const auto& level : levels_) fn(*level);
        }
    }

private:
    using Storage = std::vector<std::unique_ptr<PriceLevel>>;

    Storage::iterator lower_bound(Price price) {
        return std::lower_bound(levels_.begin(), levels_.end(), price,
            [](const auto& level, Price target) { return level->price < target; });
    }

    Storage levels_;
};

} // namespace matching
