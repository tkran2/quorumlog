#pragma once

#include "quorum/types.hpp"

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace quorum {

// Explicit little-endian encoding: the bytes are identical on every platform,
// so logs, snapshots, and network frames are portable between machines.
class Writer {
public:
    void u8(std::uint8_t value) { out_.push_back(static_cast<char>(value)); }
    void u32(std::uint32_t value) {
        for (int i = 0; i < 4; ++i) u8(static_cast<std::uint8_t>(value >> (8 * i)));
    }
    void u64(std::uint64_t value) {
        for (int i = 0; i < 8; ++i) u8(static_cast<std::uint8_t>(value >> (8 * i)));
    }
    void i64(std::int64_t value) { u64(static_cast<std::uint64_t>(value)); }
    void bytes(std::string_view value) {
        u32(static_cast<std::uint32_t>(value.size()));
        out_.append(value);
    }
    void raw(std::string_view value) { out_.append(value); }

    [[nodiscard]] const std::string& str() const { return out_; }
    [[nodiscard]] std::string take() { return std::move(out_); }

private:
    std::string out_;
};

struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Bounds-checked reader. Malformed input raises DecodeError; it never reads
// outside the buffer.
class Reader {
public:
    explicit Reader(std::string_view input) : in_(input) {}

    std::uint8_t u8() {
        need(1);
        return static_cast<std::uint8_t>(in_[pos_++]);
    }
    std::uint32_t u32() {
        need(4);
        std::uint32_t value = 0;
        for (int i = 0; i < 4; ++i)
            value |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(in_[pos_++])) << (8 * i);
        return value;
    }
    std::uint64_t u64() {
        need(8);
        std::uint64_t value = 0;
        for (int i = 0; i < 8; ++i)
            value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(in_[pos_++])) << (8 * i);
        return value;
    }
    std::int64_t i64() { return static_cast<std::int64_t>(u64()); }
    std::string bytes() {
        const auto size = u32();
        need(size);
        std::string value(in_.substr(pos_, size));
        pos_ += size;
        return value;
    }
    bool boolean() {
        const auto value = u8();
        if (value > 1) throw DecodeError("invalid boolean");
        return value == 1;
    }

    [[nodiscard]] bool done() const { return pos_ == in_.size(); }
    [[nodiscard]] std::size_t remaining() const { return in_.size() - pos_; }
    void expect_done() const {
        if (!done()) throw DecodeError("trailing bytes");
    }

private:
    void need(std::size_t count) const {
        if (in_.size() - pos_ < count) throw DecodeError("truncated input");
    }

    std::string_view in_;
    std::size_t pos_{};
};

// CRC-32 (IEEE 802.3, reflected). Detects torn and corrupted records; it is
// not a defense against deliberate tampering.
inline std::uint32_t crc32(std::string_view data, std::uint32_t seed = 0) {
    static const auto table = [] {
        std::array<std::uint32_t, 256> values{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1U) ? (0xEDB88320U ^ (c >> 1U)) : (c >> 1U);
            values[i] = c;
        }
        return values;
    }();
    std::uint32_t crc = ~seed;
    for (const char ch : data)
        crc = table[(crc ^ static_cast<std::uint8_t>(ch)) & 0xFFU] ^ (crc >> 8U);
    return ~crc;
}

// FNV-1a over explicit bytes; used for cheap, platform-stable fingerprints.
inline std::uint64_t fnv1a(std::string_view data, std::uint64_t seed = 14695981039346656037ULL) {
    std::uint64_t hash = seed;
    for (const char ch : data) {
        hash ^= static_cast<std::uint8_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline void encode_entry(Writer& out, const Entry& entry) {
    out.u64(entry.term);
    out.bytes(entry.data);
}

inline Entry decode_entry(Reader& in) {
    Entry entry;
    entry.term = in.u64();
    entry.data = in.bytes();
    return entry;
}

inline std::string encode_message(const Message& m) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(m.type));
    out.u32(m.from);
    out.u32(m.to);
    out.u64(m.term);
    switch (m.type) {
    case MsgType::RequestVote:
        out.u8(m.pre_vote ? 1 : 0);
        out.u64(m.last_log_index);
        out.u64(m.last_log_term);
        break;
    case MsgType::VoteReply:
        out.u8(m.pre_vote ? 1 : 0);
        out.u8(m.granted ? 1 : 0);
        break;
    case MsgType::AppendEntries:
        out.u64(m.prev_index);
        out.u64(m.prev_term);
        out.u64(m.leader_commit);
        out.u32(static_cast<std::uint32_t>(m.entries.size()));
        for (const auto& entry : m.entries) encode_entry(out, entry);
        break;
    case MsgType::AppendReply:
        out.u8(m.success ? 1 : 0);
        out.u64(m.match_index);
        out.u64(m.conflict_index);
        out.u64(m.conflict_term);
        break;
    case MsgType::InstallSnapshot:
        out.u64(m.snap_index);
        out.u64(m.snap_term);
        out.bytes(m.snapshot);
        break;
    case MsgType::SnapshotReply:
        out.u64(m.match_index);
        break;
    }
    return out.take();
}

inline Message decode_message(std::string_view bytes) {
    Reader in(bytes);
    Message m;
    const auto type = in.u8();
    if (type < 1 || type > 6) throw DecodeError("unknown message type");
    m.type = static_cast<MsgType>(type);
    m.from = in.u32();
    m.to = in.u32();
    m.term = in.u64();
    switch (m.type) {
    case MsgType::RequestVote:
        m.pre_vote = in.boolean();
        m.last_log_index = in.u64();
        m.last_log_term = in.u64();
        break;
    case MsgType::VoteReply:
        m.pre_vote = in.boolean();
        m.granted = in.boolean();
        break;
    case MsgType::AppendEntries: {
        m.prev_index = in.u64();
        m.prev_term = in.u64();
        m.leader_commit = in.u64();
        const auto count = in.u32();
        // Each entry needs at least 12 bytes; reject absurd counts before reserving.
        if (count > in.remaining() / 12 + 1) throw DecodeError("entry count exceeds frame");
        m.entries.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) m.entries.push_back(decode_entry(in));
        break;
    }
    case MsgType::AppendReply:
        m.success = in.boolean();
        m.match_index = in.u64();
        m.conflict_index = in.u64();
        m.conflict_term = in.u64();
        break;
    case MsgType::InstallSnapshot:
        m.snap_index = in.u64();
        m.snap_term = in.u64();
        m.snapshot = in.bytes();
        break;
    case MsgType::SnapshotReply:
        m.match_index = in.u64();
        break;
    }
    in.expect_done();
    return m;
}

} // namespace quorum
