#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace matching {

using OrderId = std::uint64_t;
using Price = std::int64_t;
using Quantity = std::int64_t;

// Keep aggregate level quantities within int64_t at the supported capacity.
inline constexpr Quantity max_order_quantity = 1'000'000'000;
inline constexpr std::size_t max_live_orders = 1'000'000;

enum class Side { Buy, Sell };
enum class OrderType { Limit, Market };
enum class Reject {
    None, InvalidId, InvalidSide, InvalidType, InvalidPrice,
    InvalidQuantity, DuplicateId, UnknownOrder, Capacity
};

inline std::string_view side_name(Side side) {
    return side == Side::Buy ? "BUY" : "SELL";
}

inline std::string_view reject_name(Reject reject) {
    switch (reject) {
    case Reject::None: return "NONE";
    case Reject::InvalidId: return "INVALID_ID";
    case Reject::InvalidSide: return "INVALID_SIDE";
    case Reject::InvalidType: return "INVALID_TYPE";
    case Reject::InvalidPrice: return "INVALID_PRICE";
    case Reject::InvalidQuantity: return "INVALID_QUANTITY";
    case Reject::DuplicateId: return "DUPLICATE_ID";
    case Reject::UnknownOrder: return "UNKNOWN_ORDER";
    case Reject::Capacity: return "CAPACITY";
    }
    return "INVALID_REJECT";
}

struct NewOrder {
    OrderId id{};
    Side side{Side::Buy};
    OrderType type{OrderType::Limit};
    Price price{}; // Integer ticks. Market orders must use zero.
    Quantity quantity{};
};

struct RestingOrder {
    OrderId id{};
    Side side{Side::Buy};
    Price price{};
    Quantity quantity{}; // Remaining quantity, not original size.
    std::uint64_t priority{};
    bool operator==(const RestingOrder&) const = default;
};

struct Trade {
    OrderId maker_id{};
    OrderId taker_id{};
    Side taker_side{Side::Buy};
    Price price{}; // The resting maker's price.
    Quantity quantity{};
    bool operator==(const Trade&) const = default;
};

struct Outcome {
    std::uint64_t sequence{};
    OrderId id{};
    Reject reject{Reject::None};
    Quantity resting{};
    Quantity unfilled{}; // Canceled remainder of a market order.
    bool priority_lost{};
    std::vector<Trade> trades;

    [[nodiscard]] bool accepted() const { return reject == Reject::None; }
    bool operator==(const Outcome&) const = default;
};

struct LevelView {
    Side side{Side::Buy};
    Price price{};
    Quantity quantity{};
    std::size_t order_count{};
    bool operator==(const LevelView&) const = default;
};

// Explicit byte order makes the digest reproducible across platforms.
// This is a regression fingerprint, not a cryptographic hash.
class Digest {
public:
    Digest() = default;
    // Added for QuorumLog snapshots: resume a fingerprint from a saved value.
    explicit Digest(std::uint64_t resume_from) : value_(resume_from) {}

    void add(std::uint64_t value) {
        for (int i = 0; i < 8; ++i) {
            value_ ^= value & 0xffU;
            value_ *= 1099511628211ULL;
            value >>= 8;
        }
    }

    void add(const Outcome& result) {
        add(result.sequence);
        add(result.id);
        add(static_cast<std::uint64_t>(result.reject));
        add(static_cast<std::uint64_t>(result.resting));
        add(static_cast<std::uint64_t>(result.unfilled));
        add(result.priority_lost ? 1U : 0U);
        add(result.trades.size());
        for (const auto& trade : result.trades) {
            add(trade.maker_id);
            add(trade.taker_id);
            add(static_cast<std::uint64_t>(trade.taker_side));
            add(static_cast<std::uint64_t>(trade.price));
            add(static_cast<std::uint64_t>(trade.quantity));
        }
    }

    [[nodiscard]] std::uint64_t value() const { return value_; }

private:
    std::uint64_t value_{14695981039346656037ULL};
};

} // namespace matching
