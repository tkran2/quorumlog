#pragma once

#include "quorum/codec.hpp"
#include "quorum/state_machine.hpp"

#include <map>
#include <optional>
#include <string>

namespace quorum::kv {

enum class Op : std::uint8_t { Put = 1, Get = 2, Cas = 3, Append = 4, Delete = 5 };
enum class Status : std::uint8_t { Ok = 0, NotFound = 1, Mismatch = 2, BadRequest = 3 };

struct Command {
    Op op{Op::Get};
    std::string key;
    std::string value;    // Put/Append value, or Cas replacement.
    std::string expected; // Cas only.
};

struct Result {
    Status status{Status::Ok};
    std::string value; // Get: the value. Cas mismatch: the current value.
    bool operator==(const Result&) const = default;
};

inline std::string encode(const Command& c) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(c.op));
    out.bytes(c.key);
    if (c.op == Op::Put || c.op == Op::Append || c.op == Op::Cas) out.bytes(c.value);
    if (c.op == Op::Cas) out.bytes(c.expected);
    return out.take();
}

inline Command decode_command(std::string_view bytes) {
    Reader in(bytes);
    Command c;
    const auto op = in.u8();
    if (op < 1 || op > 5) throw DecodeError("unknown key-value operation");
    c.op = static_cast<Op>(op);
    c.key = in.bytes();
    if (c.op == Op::Put || c.op == Op::Append || c.op == Op::Cas) c.value = in.bytes();
    if (c.op == Op::Cas) c.expected = in.bytes();
    in.expect_done();
    return c;
}

inline std::string encode(const Result& r) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(r.status));
    out.bytes(r.value);
    return out.take();
}

inline Result decode_result(std::string_view bytes) {
    Reader in(bytes);
    Result r;
    const auto status = in.u8();
    if (status > 3) throw DecodeError("unknown key-value status");
    r.status = static_cast<Status>(status);
    r.value = in.bytes();
    in.expect_done();
    return r;
}

inline std::string put(std::string key, std::string value) {
    return encode(Command{Op::Put, std::move(key), std::move(value), {}});
}
inline std::string get(std::string key) { return encode(Command{Op::Get, std::move(key), {}, {}}); }
inline std::string append(std::string key, std::string value) {
    return encode(Command{Op::Append, std::move(key), std::move(value), {}});
}
inline std::string cas(std::string key, std::string expected, std::string value) {
    return encode(Command{Op::Cas, std::move(key), std::move(value), std::move(expected)});
}
inline std::string del(std::string key) { return encode(Command{Op::Delete, std::move(key), {}, {}}); }

// The sequential specification of one key. The state machine and the
// linearizability checker both use it, so they cannot disagree about what an
// operation means.
inline Result step(std::optional<std::string>& slot, const Command& c) {
    switch (c.op) {
    case Op::Put:
        slot = c.value;
        return {Status::Ok, {}};
    case Op::Get:
        return slot ? Result{Status::Ok, *slot} : Result{Status::NotFound, {}};
    case Op::Append:
        if (slot) slot->append(c.value); else slot = c.value;
        return {Status::Ok, {}};
    case Op::Cas:
        if (!slot) return {Status::NotFound, {}};
        if (*slot != c.expected) return {Status::Mismatch, *slot};
        slot = c.value;
        return {Status::Ok, {}};
    case Op::Delete:
        if (!slot) return {Status::NotFound, {}};
        slot.reset();
        return {Status::Ok, {}};
    }
    return {Status::BadRequest, {}};
}

class Store final : public StateMachine {
public:
    std::string apply(std::string_view command) override {
        Command c;
        try {
            c = decode_command(command);
        } catch (const DecodeError&) {
            return encode(Result{Status::BadRequest, {}});
        }
        std::optional<std::string> slot;
        const auto it = data_.find(c.key);
        if (it != data_.end()) slot = it->second;
        const auto result = step(slot, c);
        if (slot) data_[c.key] = std::move(*slot);
        else if (it != data_.end()) data_.erase(it);
        return encode(result);
    }

    [[nodiscard]] std::string snapshot() const override {
        Writer out;
        out.u64(data_.size());
        for (const auto& [key, value] : data_) {
            out.bytes(key);
            out.bytes(value);
        }
        return out.take();
    }

    void restore(std::string_view snapshot) override {
        Reader in(snapshot);
        std::map<std::string, std::string> data;
        const auto count = in.u64();
        for (std::uint64_t i = 0; i < count; ++i) {
            auto key = in.bytes();
            data.emplace(std::move(key), in.bytes());
        }
        in.expect_done();
        data_ = std::move(data);
    }

    [[nodiscard]] std::uint64_t digest() const override { return fnv1a(snapshot()); }

    [[nodiscard]] bool read_only(std::string_view command) const override {
        return !command.empty() && command[0] == static_cast<char>(Op::Get);
    }

    [[nodiscard]] std::string query(std::string_view command) const override {
        const auto c = decode_command(command);
        const auto it = data_.find(c.key);
        return encode(it == data_.end() ? Result{Status::NotFound, {}}
                                        : Result{Status::Ok, it->second});
    }

    [[nodiscard]] std::size_t size() const { return data_.size(); }

private:
    std::map<std::string, std::string> data_; // Ordered: snapshots are byte-identical.
};

} // namespace quorum::kv
