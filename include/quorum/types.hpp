#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace quorum {

using NodeId = std::uint32_t; // Node IDs start at 1.
using Term = std::uint64_t;
using Index = std::uint64_t;  // Log indexes start at 1; 0 means "nothing".

inline constexpr NodeId no_node = 0;

// One replicated log record. An empty payload is a leader's no-op.
struct Entry {
    Term term{};
    std::string data;
    bool operator==(const Entry&) const = default;
};

enum class MsgType : std::uint8_t {
    RequestVote = 1,
    VoteReply = 2,
    AppendEntries = 3,
    AppendReply = 4,
    InstallSnapshot = 5,
    SnapshotReply = 6,
};

// A single flat record keeps the wire codec and the simulator simple. Only the
// fields relevant to `type` are meaningful.
struct Message {
    MsgType type{MsgType::RequestVote};
    NodeId from{};
    NodeId to{};
    Term term{};

    // RequestVote / VoteReply
    bool pre_vote{};
    bool granted{};
    Index last_log_index{};
    Term last_log_term{};

    // AppendEntries
    Index prev_index{};
    Term prev_term{};
    Index leader_commit{};
    std::vector<Entry> entries;

    // AppendReply / SnapshotReply
    bool success{};
    Index match_index{};
    Index conflict_index{};
    Term conflict_term{};

    // InstallSnapshot
    Index snap_index{};
    Term snap_term{};
    std::string snapshot;

    bool operator==(const Message&) const = default;
};

// Deliberately broken rules, used only to prove that the simulator's checks
// can fail. Every flag is false in normal operation and in the server binary.
struct TestBugs {
    bool commit_old_term{};        // Count replicas for an entry from an earlier term.
    bool skip_up_to_date_check{};  // Grant votes to candidates with stale logs.
    bool match_uses_last_index{};  // Follower acknowledges entries it never compared.
    bool accept_stale_term_reply{};// Leader counts acknowledgments from older terms.
    bool no_dedupe{};              // Apply a retried client command twice.
    bool stale_read{};             // Leader answers reads without committing them.
    bool send_before_sync{};       // Release messages before state is durable.
    bool forget_vote{};            // Do not persist the vote before replying.

    [[nodiscard]] bool any() const {
        return commit_old_term || skip_up_to_date_check || match_uses_last_index ||
            accept_stale_term_reply || no_dedupe || stale_read || send_before_sync ||
            forget_vote;
    }
};

struct Config {
    // A follower starts an election after a timeout drawn uniformly from
    // [election_ticks, 2 * election_ticks). Leaders heartbeat every
    // heartbeat_ticks. The host decides how long a tick is.
    int election_ticks{10};
    int heartbeat_ticks{2};
    std::size_t max_batch_entries{512};
    std::size_t max_batch_bytes{1U << 20U};
    bool pre_vote{true};
    TestBugs bugs;
};

struct HardState {
    Term term{};
    NodeId vote{no_node};
    bool operator==(const HardState&) const = default;
};

struct Snapshot {
    Index index{};
    Term term{};
    std::string data;
    bool operator==(const Snapshot&) const = default;
};

} // namespace quorum
