#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace quorum {

// A deterministic state machine: the same commands in the same order must give
// the same results and the same state on every replica.
class StateMachine {
public:
    virtual ~StateMachine() = default;

    virtual std::string apply(std::string_view command) = 0;
    [[nodiscard]] virtual std::string snapshot() const = 0;
    virtual void restore(std::string_view snapshot) = 0;
    // Fingerprint of the full state, used to compare replicas.
    [[nodiscard]] virtual std::uint64_t digest() const = 0;

    // Used only by the stale-read test bug: answer a read from local state.
    [[nodiscard]] virtual bool read_only(std::string_view) const { return false; }
    [[nodiscard]] virtual std::string query(std::string_view) const { return {}; }
};

} // namespace quorum
