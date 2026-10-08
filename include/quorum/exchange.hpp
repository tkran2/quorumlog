#pragma once

#include "matching/engine.hpp"
#include "quorum/codec.hpp"
#include "quorum/state_machine.hpp"

#include <memory>
#include <string>

namespace quorum::exchange {

enum class Op : std::uint8_t { Limit = 1, Market = 2, Cancel = 3, Amend = 4, Summary = 5 };

struct Summary {
    std::uint64_t commands{};
    std::uint64_t active{};
    std::uint64_t trades{};
    std::uint64_t rejected{};
    std::uint64_t events{}; // Fingerprint of every outcome so far.
    std::uint64_t state{};  // Fingerprint of the resting book.
    bool operator==(const Summary&) const = default;
};

inline std::string limit(matching::OrderId id, matching::Side side, matching::Price price,
                         matching::Quantity quantity) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(Op::Limit));
    out.u64(id);
    out.u8(side == matching::Side::Buy ? 0 : 1);
    out.i64(price);
    out.i64(quantity);
    return out.take();
}

inline std::string market(matching::OrderId id, matching::Side side, matching::Quantity quantity) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(Op::Market));
    out.u64(id);
    out.u8(side == matching::Side::Buy ? 0 : 1);
    out.i64(quantity);
    return out.take();
}

inline std::string cancel(matching::OrderId id) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(Op::Cancel));
    out.u64(id);
    return out.take();
}

inline std::string amend(matching::OrderId id, matching::Price price, matching::Quantity quantity) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(Op::Amend));
    out.u64(id);
    out.i64(price);
    out.i64(quantity);
    return out.take();
}

inline std::string summary() {
    Writer out;
    out.u8(static_cast<std::uint8_t>(Op::Summary));
    return out.take();
}

inline std::string encode(const matching::Outcome& outcome) {
    Writer out;
    out.u8(0);
    out.u64(outcome.sequence);
    out.u64(outcome.id);
    out.u8(static_cast<std::uint8_t>(outcome.reject));
    out.i64(outcome.resting);
    out.i64(outcome.unfilled);
    out.u8(outcome.priority_lost ? 1 : 0);
    out.u32(static_cast<std::uint32_t>(outcome.trades.size()));
    for (const auto& trade : outcome.trades) {
        out.u64(trade.maker_id);
        out.u64(trade.taker_id);
        out.u8(trade.taker_side == matching::Side::Buy ? 0 : 1);
        out.i64(trade.price);
        out.i64(trade.quantity);
    }
    return out.take();
}

inline std::string encode(const Summary& s) {
    Writer out;
    out.u8(1);
    out.u64(s.commands);
    out.u64(s.active);
    out.u64(s.trades);
    out.u64(s.rejected);
    out.u64(s.events);
    out.u64(s.state);
    return out.take();
}

inline matching::Outcome decode_outcome(std::string_view bytes) {
    Reader in(bytes);
    if (in.u8() != 0) throw DecodeError("not an order outcome");
    matching::Outcome o;
    o.sequence = in.u64();
    o.id = in.u64();
    const auto reject = in.u8();
    if (reject > static_cast<std::uint8_t>(matching::Reject::Capacity))
        throw DecodeError("unknown reject code");
    o.reject = static_cast<matching::Reject>(reject);
    o.resting = in.i64();
    o.unfilled = in.i64();
    o.priority_lost = in.boolean();
    const auto count = in.u32();
    if (count > in.remaining() / 33 + 1) throw DecodeError("trade count exceeds frame");
    for (std::uint32_t i = 0; i < count; ++i) {
        matching::Trade t;
        t.maker_id = in.u64();
        t.taker_id = in.u64();
        t.taker_side = in.boolean() ? matching::Side::Sell : matching::Side::Buy;
        t.price = in.i64();
        t.quantity = in.i64();
        o.trades.push_back(t);
    }
    in.expect_done();
    return o;
}

inline Summary decode_summary(std::string_view bytes) {
    Reader in(bytes);
    if (in.u8() != 1) throw DecodeError("not a summary");
    Summary s;
    s.commands = in.u64();
    s.active = in.u64();
    s.trades = in.u64();
    s.rejected = in.u64();
    s.events = in.u64();
    s.state = in.u64();
    in.expect_done();
    return s;
}

// The matching engine as a replicated state machine. Every replica applies the
// same committed command log, so every replica must report the same
// fingerprints as a single standalone engine fed the same commands.
class Book final : public StateMachine {
public:
    Book() : engine_(std::make_unique<matching::TreeEngine>()) {}

    std::string apply(std::string_view command) override {
        try {
            Reader in(command);
            const auto op = static_cast<Op>(in.u8());
            switch (op) {
            case Op::Limit: {
                matching::NewOrder order;
                order.id = in.u64();
                order.side = in.boolean() ? matching::Side::Sell : matching::Side::Buy;
                order.type = matching::OrderType::Limit;
                order.price = in.i64();
                order.quantity = in.i64();
                in.expect_done();
                return record(engine_->submit(order));
            }
            case Op::Market: {
                matching::NewOrder order;
                order.id = in.u64();
                order.side = in.boolean() ? matching::Side::Sell : matching::Side::Buy;
                order.type = matching::OrderType::Market;
                order.quantity = in.i64();
                in.expect_done();
                return record(engine_->submit(order));
            }
            case Op::Cancel: {
                const auto id = in.u64();
                in.expect_done();
                return record(engine_->cancel(id));
            }
            case Op::Amend: {
                const auto id = in.u64();
                const auto price = in.i64();
                const auto quantity = in.i64();
                in.expect_done();
                return record(engine_->amend(id, price, quantity));
            }
            case Op::Summary:
                in.expect_done();
                return encode(summarize());
            }
        } catch (const DecodeError&) {
        }
        // A malformed command is a deterministic no-op on every replica.
        return std::string(1, '\x02');
    }

    [[nodiscard]] Summary summarize() const {
        return {engine_->sequence(), engine_->active_count(), trades_, rejected_,
                events_.value(), engine_->state_digest()};
    }

    [[nodiscard]] std::string snapshot() const override {
        Writer out;
        out.u64(engine_->sequence());
        out.u64(trades_);
        out.u64(rejected_);
        out.u64(events_.value());
        const auto orders = engine_->orders();
        out.u64(orders.size());
        for (const auto& order : orders) {
            out.u64(order.id);
            out.u8(order.side == matching::Side::Buy ? 0 : 1);
            out.i64(order.price);
            out.i64(order.quantity);
            out.u64(order.priority);
        }
        return out.take();
    }

    void restore(std::string_view snapshot) override {
        Reader in(snapshot);
        const auto sequence = in.u64();
        const auto trades = in.u64();
        const auto rejected = in.u64();
        const auto events = in.u64();
        const auto count = in.u64();
        std::vector<matching::RestingOrder> orders;
        for (std::uint64_t i = 0; i < count; ++i) {
            matching::RestingOrder order;
            order.id = in.u64();
            order.side = in.boolean() ? matching::Side::Sell : matching::Side::Buy;
            order.price = in.i64();
            order.quantity = in.i64();
            order.priority = in.u64();
            orders.push_back(order);
        }
        in.expect_done();
        auto engine = std::make_unique<matching::TreeEngine>();
        engine->restore(sequence, orders);
        engine_ = std::move(engine);
        trades_ = trades;
        rejected_ = rejected;
        events_ = matching::Digest(events);
    }

    [[nodiscard]] std::uint64_t digest() const override {
        matching::Digest d;
        d.add(events_.value());
        d.add(engine_->state_digest());
        return d.value();
    }

    void check_invariants() const { engine_->check_invariants(); }

private:
    std::string record(const matching::Outcome& outcome) {
        events_.add(outcome);
        trades_ += outcome.trades.size();
        if (!outcome.accepted()) ++rejected_;
        return encode(outcome);
    }

    std::unique_ptr<matching::TreeEngine> engine_;
    matching::Digest events_;
    std::uint64_t trades_{};
    std::uint64_t rejected_{};
};

} // namespace quorum::exchange
