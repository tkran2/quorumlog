#pragma once

#include "quorum/kv.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace quorum {

// One client operation as an outside observer saw it. Times come from a single
// global counter, so "A returned before B was invoked" is unambiguous.
struct Operation {
    static constexpr std::uint64_t pending = std::numeric_limits<std::uint64_t>::max();

    std::uint64_t client{};
    std::uint64_t invoke{};
    std::uint64_t ret{pending}; // `pending`: no answer was ever observed.
    std::string input;          // Encoded kv::Command.
    std::string output;         // Encoded kv::Result; ignored while pending.

    [[nodiscard]] bool completed() const { return ret != pending; }
};

enum class Verdict { Linearizable, Violation, Inconclusive };

struct CheckResult {
    Verdict verdict{Verdict::Linearizable};
    std::string key;           // The key whose history failed, if any.
    std::uint64_t states{};    // Search states explored across all keys.
};

namespace detail {

// Decide whether one key's history has a legal sequential order that respects
// real time. This is the Wing-Gong search with Lowe's memoization: try each
// operation that could go next, and remember every (linearized set, state)
// pair already explored so no dead end is searched twice.
class SingleKeyChecker {
public:
    SingleKeyChecker(std::vector<Operation> ops, std::uint64_t budget)
        : ops_(std::move(ops)), budget_(budget) {
        commands_.reserve(ops_.size());
        results_.reserve(ops_.size());
        for (const auto& op : ops_) {
            commands_.push_back(kv::decode_command(op.input));
            results_.push_back(op.completed() ? kv::decode_result(op.output) : kv::Result{});
        }
    }

    Verdict run(std::uint64_t& explored) {
        const std::size_t n = ops_.size();
        if (n == 0) return Verdict::Linearizable;

        // Event list: a call and a return per operation, in time order. An
        // operation with no answer may take effect at any later point, so its
        // return sorts after everything else.
        struct Event {
            std::uint64_t time;
            std::size_t op;
            bool is_call;
        };
        std::vector<Event> events;
        events.reserve(2 * n);
        for (std::size_t i = 0; i < n; ++i) {
            events.push_back({ops_[i].invoke, i, true});
            events.push_back({ops_[i].ret, i, false});
        }
        std::stable_sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
            if (a.time != b.time) return a.time < b.time;
            return a.is_call && !b.is_call; // Only pending returns can tie; calls go first.
        });

        // Doubly linked list over 2n nodes plus a head sentinel at index 0.
        const std::size_t total = 2 * n + 1;
        next_.assign(total, 0);
        prev_.assign(total, 0);
        node_op_.assign(total, 0);
        node_call_.assign(total, false);
        match_.assign(total, 0);
        std::vector<std::size_t> call_node(n), return_node(n);
        for (std::size_t k = 0; k < events.size(); ++k) {
            const std::size_t node = k + 1;
            node_op_[node] = events[k].op;
            node_call_[node] = events[k].is_call;
            (events[k].is_call ? call_node : return_node)[events[k].op] = node;
            prev_[node] = node - 1;
            next_[node - 1] = node;
        }
        next_[total - 1] = 0;
        for (std::size_t i = 0; i < n; ++i) match_[call_node[i]] = return_node[i];

        std::vector<std::uint64_t> linearized((n + 63) / 64, 0);
        std::optional<std::string> state; // The key starts absent.
        struct Frame {
            std::size_t node;
            std::optional<std::string> state;
        };
        std::vector<Frame> stack;
        std::unordered_set<std::string> seen;

        std::size_t entry = next_[0];
        while (next_[0] != 0) {
            if (entry == 0) return Verdict::Violation; // Defensive; a return always stops us first.
            if (node_call_[entry]) {
                const std::size_t op = node_op_[entry];
                auto candidate = state;
                const auto result = kv::step(candidate, commands_[op]);
                const bool legal = !ops_[op].completed() || result == results_[op];
                if (legal) {
                    linearized[op / 64] |= 1ULL << (op % 64);
                    if (seen.insert(cache_key(linearized, candidate)).second) {
                        if (++explored > budget_) return Verdict::Inconclusive;
                        stack.push_back({entry, std::move(state)});
                        state = std::move(candidate);
                        lift(entry);
                        entry = next_[0];
                        continue;
                    }
                    linearized[op / 64] &= ~(1ULL << (op % 64));
                }
                entry = next_[entry];
            } else {
                // A return whose call is still unplaced: every operation that
                // could legally come before it has been tried. Undo the most
                // recent choice and try the next one.
                if (stack.empty()) return Verdict::Violation;
                auto frame = std::move(stack.back());
                stack.pop_back();
                const std::size_t op = node_op_[frame.node];
                linearized[op / 64] &= ~(1ULL << (op % 64));
                state = std::move(frame.state);
                unlift(frame.node);
                entry = next_[frame.node];
            }
        }
        return Verdict::Linearizable;
    }

private:
    static std::string cache_key(const std::vector<std::uint64_t>& bits,
                                 const std::optional<std::string>& state) {
        std::string key(reinterpret_cast<const char*>(bits.data()), bits.size() * 8);
        key.push_back(state ? '\x01' : '\x00');
        if (state) key.append(*state);
        return key;
    }

    void lift(std::size_t call) {
        const std::size_t ret = match_[call];
        next_[prev_[call]] = next_[call];
        if (next_[call] != 0) prev_[next_[call]] = prev_[call];
        next_[prev_[ret]] = next_[ret];
        if (next_[ret] != 0) prev_[next_[ret]] = prev_[ret];
    }

    void unlift(std::size_t call) {
        const std::size_t ret = match_[call];
        next_[prev_[ret]] = ret;
        if (next_[ret] != 0) prev_[next_[ret]] = ret;
        next_[prev_[call]] = call;
        if (next_[call] != 0) prev_[next_[call]] = call;
    }

    std::vector<Operation> ops_;
    std::uint64_t budget_;
    std::vector<kv::Command> commands_;
    std::vector<kv::Result> results_;
    std::vector<std::size_t> next_, prev_, node_op_, match_;
    std::vector<bool> node_call_;
};

} // namespace detail

// Check a key-value history. Operations on different keys never interact, so
// each key is checked on its own, which keeps the search small.
inline CheckResult check_linearizable(const std::vector<Operation>& history,
                                      std::uint64_t budget = 20'000'000) {
    std::map<std::string, std::vector<Operation>> by_key;
    for (const auto& op : history) by_key[kv::decode_command(op.input).key].push_back(op);

    CheckResult result;
    for (auto& [key, ops] : by_key) {
        detail::SingleKeyChecker checker(std::move(ops), budget);
        const auto verdict = checker.run(result.states);
        if (verdict != Verdict::Linearizable) {
            result.verdict = verdict;
            result.key = key;
            return result;
        }
    }
    return result;
}

} // namespace quorum
