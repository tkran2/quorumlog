// Unit and scenario tests for the consensus core, codecs, state machines, and
// the linearizability checker. No third-party test framework is required.

#include "matching/command.hpp"
#include "quorum/exchange.hpp"
#include "quorum/kv.hpp"
#include "quorum/linearizability.hpp"
#include "quorum/replica.hpp"
#include "quorum/sim.hpp"

#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

using namespace quorum;

std::size_t assertions = 0;
std::size_t cases = 0;

void check(bool condition, const char* expression, int line) {
    ++assertions;
    if (!condition) throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
}
#define CHECK(...) check((__VA_ARGS__), #__VA_ARGS__, __LINE__)

template <typename F>
void test(const char* name, F&& body) {
    try {
        body();
        ++cases;
    } catch (const std::exception& error) {
        throw std::runtime_error(std::string(name) + ": " + error.what());
    }
}

template <typename F>
bool throws(F&& body) {
    try {
        body();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

// Staged scenarios drive one node's clock at a time. With pre-vote on, the
// other nodes would (correctly) refuse to help while their own clocks say the
// leader was heard recently, so most scenarios use plain elections and the
// pre-vote tests opt in explicitly.
Config plain() {
    Config config;
    config.pre_vote = false;
    return config;
}

Config with_pre_vote() { return Config{}; }

// A hand-driven cluster: the test decides which node's clock advances and
// which messages arrive, so a specific interleaving can be staged exactly.
struct Net {
    explicit Net(int n, Config config_ = plain()) : config(config_) {
        for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
        for (int i = 0; i < n; ++i) {
            stores.push_back(std::make_unique<MemStorage>());
            nodes.push_back(std::make_unique<RaftNode>(ids[static_cast<std::size_t>(i)], ids,
                config, *stores.back(), 1000 + static_cast<std::uint64_t>(i)));
        }
    }

    RaftNode& operator[](NodeId id) { return *nodes[id - 1]; }

    void pump(NodeId id) {
        auto& node = (*this)[id];
        node.flush();
        stores[id - 1]->sync();
        for (auto& m : node.take_messages()) wire.push_back(std::move(m));
    }

    // Deliver until nothing is in flight. Messages touching a `down` node or
    // rejected by `allow` are lost.
    void run() {
        while (!wire.empty()) {
            Message m = std::move(wire.front());
            wire.pop_front();
            if (down.count(m.from) || down.count(m.to) || !allow(m)) continue;
            (*this)[m.to].step(m);
            pump(m.to);
        }
    }

    void tick(NodeId id, int times = 1) {
        for (int i = 0; i < times; ++i) (*this)[id].tick();
        pump(id);
    }

    // Advance one node's clock until it asks for votes, then let the cluster react.
    void campaign(NodeId id) {
        const auto asked = [&] {
            for (const auto& m : wire)
                if (m.from == id && m.type == MsgType::RequestVote) return true;
            return false;
        };
        for (int guard = 0; guard < 1000 && !asked(); ++guard) {
            (*this)[id].tick();
            pump(id);
            if (ids.size() == 1 && (*this)[id].role() == Role::Leader) break;
        }
        run();
    }

    void heartbeat(NodeId leader) {
        tick(leader, config.heartbeat_ticks);
        run();
    }

    Index propose(NodeId leader, const std::string& data) {
        const auto index = (*this)[leader].propose(data);
        pump(leader);
        run();
        return index;
    }

    // Kill and restart a node; anything it had not synced is gone.
    void restart(NodeId id) {
        stores[id - 1]->crash();
        nodes[id - 1] = std::make_unique<RaftNode>(id, ids, config, *stores[id - 1], ++restarts + 5000);
    }

    Config config;
    std::vector<NodeId> ids;
    std::vector<std::unique_ptr<MemStorage>> stores;
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::deque<Message> wire;
    std::set<NodeId> down;
    std::function<bool(const Message&)> allow = [](const Message&) { return true; };
    std::uint64_t restarts{};
};

bool is_vote(const Message& m) {
    return m.type == MsgType::RequestVote || m.type == MsgType::VoteReply;
}

bool same_log(const RaftNode& a, const RaftNode& b) {
    if (a.log().base_index() != b.log().base_index()) return false;
    return a.log().entries() == b.log().entries();
}

// ---------------------------------------------------------------------------

void raft_tests() {
    test("one leader is elected and the others follow", [] {
        Net net(3);
        net.campaign(1);
        CHECK(net[1].role() == Role::Leader);
        CHECK(net[1].term() == 1);
        net.heartbeat(1);
        for (NodeId id = 2; id <= 3; ++id) {
            CHECK(net[id].role() == Role::Follower);
            CHECK(net[id].leader() == 1);
            CHECK(net[id].term() == 1);
            CHECK(net[id].commit_index() == 1); // The leader's no-op.
        }
    });

    test("a single node elects itself and commits alone", [] {
        Net net(1);
        net.campaign(1);
        CHECK(net[1].role() == Role::Leader);
        CHECK(net.propose(1, "a") == 2);
        CHECK(net[1].commit_index() == 2);
    });

    test("entries replicate, commit on a majority, and reach every log", [] {
        Net net(5);
        net.campaign(2);
        CHECK(net[2].role() == Role::Leader);
        for (int i = 0; i < 20; ++i) net.propose(2, "cmd" + std::to_string(i));
        CHECK(net[2].commit_index() == 21);
        net.heartbeat(2);
        for (NodeId id = 1; id <= 5; ++id) {
            CHECK(net[id].commit_index() == 21);
            CHECK(same_log(net[id], net[2]));
        }
    });

    test("a proposal on a follower is refused", [] {
        Net net(3);
        net.campaign(1);
        CHECK(net[2].propose("x") == 0);
        CHECK(net[2].log().last_index() == 1);
    });

    test("no commit without a majority", [] {
        Net net(5);
        net.campaign(1);
        net.heartbeat(1);
        net.down = {3, 4, 5};
        net.propose(1, "lonely");
        CHECK(net[1].log().last_index() == 2);
        CHECK(net[2].log().last_index() == 2);
        CHECK(net[1].commit_index() == 1); // Two of five hold it.
        net.down = {4, 5};
        net.heartbeat(1); // Node 3 rejects the heartbeat, is repaired, and acknowledges.
        net.heartbeat(1);
        CHECK(net[1].commit_index() == 2);
    });

    test("a deposed leader's uncommitted entries are replaced", [] {
        Net net(3);
        net.campaign(1);
        net.heartbeat(1);
        net.down = {2, 3};
        for (int i = 0; i < 4; ++i) net.propose(1, "lost" + std::to_string(i));
        CHECK(net[1].log().last_index() == 5);
        CHECK(net[1].commit_index() == 1);

        net.down = {1};
        net.campaign(2);
        CHECK(net[2].role() == Role::Leader);
        net.propose(2, "kept");
        CHECK(net[2].commit_index() == 3);

        net.down.clear();
        net.heartbeat(2);
        net.heartbeat(2);
        CHECK(net[1].role() == Role::Follower);
        CHECK(same_log(net[1], net[2]));
        CHECK(net[1].log().at(3).data == "kept");
        CHECK(net[1].commit_index() == 3);
    });

    test("a follower that missed many entries catches up in bounded batches", [] {
        Config config = plain();
        config.max_batch_entries = 7;
        Net net(3, config);
        net.campaign(1);
        net.down = {3};
        for (int i = 0; i < 100; ++i) net.propose(1, std::string(10, static_cast<char>('a' + i % 26)));
        net.down.clear();
        net.heartbeat(1);
        CHECK(same_log(net[3], net[1]));
        net.heartbeat(1);
        CHECK(net[3].commit_index() == 101);
    });

    test("votes require an up-to-date log", [] {
        Net net(3);
        net.campaign(1);
        net.down = {3};
        net.propose(1, "a");
        net.propose(1, "b");
        // Node 3 is behind. It cannot win, even after the leader disappears.
        net.down = {1};
        net.campaign(3);
        net.campaign(3);
        CHECK(net[3].role() != Role::Leader);
        net.campaign(2);
        CHECK(net[2].role() == Role::Leader);
        CHECK(net[2].log().at(2).data == "a");
    });

    test("a vote survives a restart", [] {
        Net net(3);
        net.allow = [](const Message& m) { return m.to != 3 && m.from != 3; };
        net.campaign(1); // Node 2 votes for node 1 in term 1.
        CHECK(net[1].role() == Role::Leader);
        CHECK(net[2].vote() == 1);
        net.restart(2);
        CHECK(net[2].term() == 1);
        CHECK(net[2].vote() == 1);
        // Node 3 now asks for a vote in the same term and must be refused.
        Message ask;
        ask.type = MsgType::RequestVote;
        ask.from = 3;
        ask.to = 2;
        ask.term = 1;
        ask.last_log_index = 100;
        ask.last_log_term = 1;
        net[2].step(ask);
        const auto replies = net[2].take_messages();
        CHECK(replies.size() == 1);
        CHECK(!replies[0].granted);
    });

    test("unsynced state is lost on a crash and the node still recovers", [] {
        Net net(3);
        net.campaign(1);
        net.heartbeat(1);
        // Deliver an entry to node 2 but kill it before it syncs.
        Message append;
        append.type = MsgType::AppendEntries;
        append.from = 1;
        append.to = 2;
        append.term = 1;
        append.prev_index = 1;
        append.prev_term = 1;
        append.entries = {Entry{1, "volatile"}};
        net[2].step(append);
        CHECK(net[2].log().last_index() == 2);
        net.restart(2); // No sync happened.
        CHECK(net[2].log().last_index() == 1);
        net.propose(1, "durable");
        net.heartbeat(1);
        CHECK(same_log(net[2], net[1]));
    });

    test("pre-vote keeps an isolated node from inflating its term", [] {
        Net net(3, with_pre_vote());
        net.campaign(1);
        net.heartbeat(1);
        net.down = {3};
        for (int i = 0; i < 20; ++i) net.campaign(3);
        CHECK(net[3].term() == 1);
        net.down.clear();
        net.heartbeat(1);
        CHECK(net[1].role() == Role::Leader); // The healthy leader was not deposed.
        CHECK(net[1].term() == 1);
    });

    test("without pre-vote the same isolated node disrupts the cluster", [] {
        Net net(3);
        net.campaign(1);
        net.heartbeat(1);
        net.down = {3};
        for (int i = 0; i < 5; ++i) net.campaign(3);
        CHECK(net[3].term() > 1);
        net.down.clear();
        net.heartbeat(1);
        CHECK(net[1].role() == Role::Follower); // Forced out by the higher term.
    });

    test("followers refuse pre-votes while they still hear the leader", [] {
        Net net(3, with_pre_vote());
        net.campaign(1);
        net.heartbeat(1);
        // Node 3 can send but receives nothing from the leader.
        net.allow = [](const Message& m) { return !(m.from == 1 && m.to == 3); };
        net.campaign(3);
        CHECK(net[3].role() == Role::PreCandidate);
        CHECK(net[1].role() == Role::Leader);
        CHECK(net[2].leader() == 1);
    });

    test("pre-vote still elects a new leader once the old one is gone", [] {
        Net net(3, with_pre_vote());
        net.campaign(1);
        net.heartbeat(1);
        CHECK(net[1].role() == Role::Leader);
        net.down = {1};
        // Let both survivors' clocks run; their leases expire and one wins.
        for (int round = 0; round < 200 && net[2].role() != Role::Leader &&
                            net[3].role() != Role::Leader; ++round) {
            net.tick(2);
            net.tick(3);
            net.run();
        }
        const bool two = net[2].role() == Role::Leader;
        const bool three = net[3].role() == Role::Leader;
        CHECK(two != three);
        CHECK(net[two ? 2 : 3].term() == 2); // Exactly one real election happened.
    });

    test("a leader that hears no majority steps down", [] {
        Net net(3);
        net.campaign(1);
        net.heartbeat(1);
        net.down = {2, 3};
        net.tick(1, 3 * net.config.election_ticks);
        CHECK(net[1].role() != Role::Leader);
    });

    test("a compacted leader brings a follower back with a snapshot", [] {
        Net net(3);
        net.campaign(1);
        net.down = {3};
        for (int i = 0; i < 10; ++i) net.propose(1, "e" + std::to_string(i));
        CHECK(net[1].commit_index() == 11);
        net[1].compact(9, "STATE@9");
        CHECK(net[1].log().base_index() == 9);
        CHECK(throws([&] { (void)net[1].log().at(5); }));

        net.down.clear();
        net.heartbeat(1);
        net.heartbeat(1);
        auto restore = net[3].take_restore();
        CHECK(restore.has_value());
        CHECK(restore->index == 9);
        CHECK(restore->data == "STATE@9");
        CHECK(net[3].log().base_index() == 9);
        CHECK(net[3].log().last_index() == 11);
        CHECK(net[3].commit_index() == 11);
        // The snapshot is durable on the follower too.
        net.restart(3);
        CHECK(net[3].log().base_index() == 9);
        CHECK(net[3].take_restore()->data == "STATE@9");
    });

    test("compaction rules are enforced", [] {
        Net net(1);
        net.campaign(1);
        net.propose(1, "a");
        CHECK(throws([&] { net[1].compact(5, "beyond commit"); }));
        net[1].compact(2, "ok");
        CHECK(throws([&] { net[1].compact(2, "already compacted"); }));
    });

    test("duplicated and reordered messages are harmless", [] {
        Net net(3);
        net.campaign(1);
        std::vector<Message> seen;
        net.allow = [&](const Message& m) {
            seen.push_back(m);
            return true;
        };
        for (int i = 0; i < 5; ++i) net.propose(1, "x" + std::to_string(i));
        net.heartbeat(1);
        const auto before = net[2].log().entries();
        const auto commit = net[2].commit_index();
        net.allow = [](const Message&) { return true; };
        // Replay everything ever sent, newest first.
        for (auto it = seen.rbegin(); it != seen.rend(); ++it) {
            net[it->to].step(*it);
            net.pump(it->to);
        }
        net.run();
        CHECK(net[2].log().entries() == before);
        CHECK(net[2].commit_index() == commit);
        CHECK(net[1].role() == Role::Leader);
        CHECK(same_log(net[1], net[2]));
        CHECK(same_log(net[1], net[3]));
    });

    test("messages from strangers and wrong addresses are ignored", [] {
        Net net(3);
        Message m;
        m.type = MsgType::AppendEntries;
        m.from = 9;
        m.to = 1;
        m.term = 50;
        net[1].step(m);
        CHECK(net[1].term() == 0);
        m.from = 2;
        m.to = 3;
        net[1].step(m);
        CHECK(net[1].term() == 0);
    });

    test("invalid configurations are rejected", [] {
        MemStorage storage;
        Config config;
        CHECK(throws([&] { RaftNode node(4, {1, 2, 3}, config, storage, 1); }));
        config.heartbeat_ticks = config.election_ticks;
        CHECK(throws([&] { RaftNode node(1, {1, 2, 3}, config, storage, 1); }));
    });
}

// Figure 8 of the Raft paper, staged message by message. S1's entry from an
// old term reaches a majority, yet a later leader may still overwrite it. The
// correct rule refuses to call it committed; the injected bug commits it and
// two nodes end up with different committed entries at the same index.
void figure8(bool bug) {
    Config config = plain();
    config.max_batch_entries = 1;
    config.bugs.commit_old_term = bug;
    Net net(5, config);

    net.campaign(1);
    CHECK(net[1].role() == Role::Leader && net[1].term() == 1);
    net.heartbeat(1);

    // (a) S1 appends X at index 2. Only S2 receives it.
    net.allow = [](const Message& m) { return !(m.from == 1 && m.to != 2); };
    CHECK(net.propose(1, "X") == 2);
    CHECK(net[2].log().last_index() == 2);
    CHECK(net[3].log().last_index() == 1);
    CHECK(net[1].commit_index() == 1);

    // (b) S1 goes quiet. S5 wins term 2 with S3 and S4, appends its own entry
    // at index 2, and reaches nobody.
    net.down = {1};
    net.allow = is_vote;
    net.campaign(5);
    CHECK(net[5].role() == Role::Leader && net[5].term() == 2);
    CHECK(net[5].log().term_at(2) == 2);

    // (c) S5 goes quiet. S1 returns and wins term 3, then repairs S3 and S4 one
    // entry at a time: X at index 2 arrives, S1's own term-3 entry does not.
    net.down = {5};
    net.campaign(1);
    net.campaign(1);
    CHECK(net[1].role() == Role::Leader && net[1].term() == 3);
    net.allow = [](const Message& m) {
        const NodeId other = m.from == 1 ? m.to : m.from;
        if ((m.from != 1 && m.to != 1) || (other != 3 && other != 4)) return false;
        if (m.type == MsgType::AppendEntries && !m.entries.empty() &&
            m.prev_index + m.entries.size() >= 3)
            return false;
        return true;
    };
    net.heartbeat(1);
    CHECK(net[3].log().term_at(2) == 1 && net[3].log().at(2).data == "X");
    CHECK(net[4].log().term_at(2) == 1);
    // X is now on S1, S2, S3, S4: four of five. It is still not safe.
    CHECK(net[1].commit_index() == (bug ? 2U : 1U));

    // (d) S1 goes quiet. S5 returns; its index-2 entry has a newer term than X,
    // so S2, S3, S4 vote for it and it overwrites X everywhere.
    net.down = {1};
    net.allow = [](const Message&) { return true; };
    net.campaign(5);
    net.campaign(5);
    CHECK(net[5].role() == Role::Leader && net[5].term() == 4);
    net.heartbeat(5);
    net.heartbeat(5);
    CHECK(net[5].commit_index() == 3);
    CHECK(net[5].log().term_at(2) == 2);
    for (NodeId id = 2; id <= 4; ++id) CHECK(same_log(net[id], net[5]));

    net.down.clear();
    if (bug) {
        // S1 "committed" X at index 2; the cluster committed something else.
        CHECK(net[1].log().term_at(2) != net[5].log().term_at(2));
        CHECK(throws([&] { net.heartbeat(5); })); // S1 is told to overwrite a committed entry.
    } else {
        net.heartbeat(5);
        net.heartbeat(5);
        CHECK(same_log(net[1], net[5]));
        CHECK(net[1].commit_index() == 3);
    }
}

// A leader must ignore acknowledgments sent in an earlier term: its log may
// have been rewritten since, so the index they name can now hold a different
// entry. Staged: S2 acknowledges index 3 to S1 in term 1, the answer is held
// back, S1's log is replaced, S1 leads again in term 3, and the old answer
// finally arrives.
void stale_acknowledgment(bool bug) {
    Config config = plain();
    config.bugs.accept_stale_term_reply = bug;
    Net net(5, config);
    net.campaign(1);
    net.heartbeat(1);

    // S1 appends two entries that reach only S2. S2's answer is intercepted.
    std::optional<Message> held;
    net.allow = [&](const Message& m) {
        if (m.from == 1 && m.to != 2) return false;
        if (m.from == 2 && m.to == 1 && m.type == MsgType::AppendReply && m.success &&
            m.match_index == 3) {
            held = m;
            return false;
        }
        return true;
    };
    (void)net[1].propose("A");
    (void)net[1].propose("B");
    net.pump(1);
    net.run();
    CHECK(held.has_value() && held->term == 1);
    CHECK(net[1].commit_index() == 1);

    // S3 takes over in term 2 and replaces those entries everywhere.
    net.down = {1};
    net.allow = [](const Message&) { return true; };
    net.campaign(3);
    CHECK(net[3].role() == Role::Leader && net[3].term() == 2);
    net.heartbeat(3);
    net.down.clear();
    net.heartbeat(3);
    net.heartbeat(3);
    CHECK(net[1].log().last_index() == 2 && net[1].log().term_at(2) == 2);

    // S1 leads again in term 3. Its new entry sits at index 3 and reaches no one.
    net.down = {3};
    net.allow = is_vote;
    net.campaign(1);
    CHECK(net[1].role() == Role::Leader && net[1].term() == 3);
    CHECK(net[1].log().last_index() == 3);

    // The term-1 answer arrives, claiming S2 holds everything through index 3.
    // It did, in term 1. Index 3 is a different entry now.
    net[1].step(*held);
    net.pump(1);
    // One genuine acknowledgment, from S4.
    net.allow = [](const Message& m) {
        return (m.from == 1 && m.to == 4) || (m.from == 4 && m.to == 1);
    };
    net.heartbeat(1);
    CHECK(net[4].log().last_index() == 3);
    CHECK(net[2].log().last_index() == 2); // S2 never received index 3.

    // Index 3 is on two of five nodes. Counting the stale answer makes it three.
    CHECK(net[1].commit_index() == (bug ? 3U : 2U));
}

// ---------------------------------------------------------------------------

Operation op(std::uint64_t client, std::uint64_t invoke, std::uint64_t ret, std::string input,
             kv::Result output = {}) {
    Operation o;
    o.client = client;
    o.invoke = invoke;
    o.ret = ret;
    o.input = std::move(input);
    o.output = kv::encode(output);
    return o;
}

const std::uint64_t never = Operation::pending;
const kv::Result ok{kv::Status::Ok, {}};
kv::Result value(std::string v) { return {kv::Status::Ok, std::move(v)}; }
const kv::Result missing{kv::Status::NotFound, {}};

bool lin(const std::vector<Operation>& history) {
    return check_linearizable(history).verdict == Verdict::Linearizable;
}

void checker_tests() {
    test("checker: sequential history", [] {
        CHECK(lin({op(1, 1, 2, kv::get("k"), missing), op(1, 3, 4, kv::put("k", "a"), ok),
                   op(1, 5, 6, kv::get("k"), value("a"))}));
        CHECK(lin({}));
    });

    test("checker: a read may land on either side of a concurrent write", [] {
        CHECK(lin({op(1, 1, 4, kv::put("k", "a"), ok), op(2, 2, 3, kv::get("k"), missing)}));
        CHECK(lin({op(1, 1, 4, kv::put("k", "a"), ok), op(2, 2, 3, kv::get("k"), value("a"))}));
    });

    test("checker: stale read after a completed write", [] {
        CHECK(!lin({op(1, 1, 2, kv::put("k", "a"), ok), op(1, 3, 4, kv::put("k", "b"), ok),
                    op(2, 5, 6, kv::get("k"), value("a"))}));
    });

    test("checker: acknowledged write that vanished", [] {
        CHECK(!lin({op(1, 1, 2, kv::append("k", "x;"), ok), op(2, 3, 4, kv::get("k"), missing)}));
        CHECK(!lin({op(1, 1, 2, kv::append("k", "x;"), ok), op(1, 3, 4, kv::append("k", "y;"), ok),
                    op(2, 5, 6, kv::get("k"), value("y;"))}));
    });

    test("checker: a command applied twice", [] {
        CHECK(!lin({op(1, 1, 2, kv::append("k", "x;"), ok), op(2, 3, 4, kv::get("k"), value("x;x;"))}));
    });

    test("checker: reads must agree on one order of concurrent writes", [] {
        // Two appends overlap; one reader sees a;b; and a later reader sees b;a;.
        CHECK(!lin({op(1, 1, 10, kv::append("k", "a;"), ok), op(2, 2, 11, kv::append("k", "b;"), ok),
                    op(3, 12, 13, kv::get("k"), value("a;b;")),
                    op(4, 14, 15, kv::get("k"), value("b;a;"))}));
        CHECK(lin({op(1, 1, 10, kv::append("k", "a;"), ok), op(2, 2, 11, kv::append("k", "b;"), ok),
                   op(3, 12, 13, kv::get("k"), value("b;a;")),
                   op(4, 14, 15, kv::get("k"), value("b;a;"))}));
    });

    test("checker: an unanswered write may or may not have happened", [] {
        CHECK(lin({op(1, 1, never, kv::put("k", "a")), op(2, 5, 6, kv::get("k"), value("a"))}));
        CHECK(lin({op(1, 1, never, kv::put("k", "a")), op(2, 5, 6, kv::get("k"), missing)}));
        // But it cannot take effect before it was sent.
        CHECK(!lin({op(2, 1, 2, kv::get("k"), value("a")), op(1, 5, never, kv::put("k", "a"))}));
        // And once seen, it cannot be unseen.
        CHECK(!lin({op(1, 1, never, kv::put("k", "a")), op(2, 5, 6, kv::get("k"), value("a")),
                    op(2, 7, 8, kv::get("k"), missing)}));
    });

    test("checker: compare-and-swap results constrain the order", [] {
        CHECK(lin({op(1, 1, 2, kv::put("k", "a"), ok), op(2, 3, 4, kv::cas("k", "a", "b"), ok),
                   op(3, 5, 6, kv::get("k"), value("b"))}));
        CHECK(!lin({op(1, 1, 2, kv::put("k", "a"), ok), op(2, 3, 4, kv::cas("k", "z", "b"), ok)}));
        // Two overlapping swaps from the same value cannot both succeed.
        CHECK(!lin({op(1, 1, 2, kv::put("k", "a"), ok), op(2, 3, 9, kv::cas("k", "a", "b"), ok),
                    op(3, 4, 8, kv::cas("k", "a", "c"), ok)}));
        CHECK(lin({op(1, 1, 2, kv::put("k", "a"), ok), op(2, 3, 9, kv::cas("k", "a", "b"), ok),
                   op(3, 4, 8, kv::cas("k", "a", "c"), kv::Result{kv::Status::Mismatch, "b"})}));
    });

    test("checker: keys are independent", [] {
        CHECK(lin({op(1, 1, 2, kv::put("a", "1"), ok), op(2, 3, 4, kv::get("b"), missing)}));
        const auto result = check_linearizable(
            {op(1, 1, 2, kv::put("a", "1"), ok), op(2, 3, 4, kv::get("a"), missing),
             op(3, 5, 6, kv::put("b", "1"), ok)});
        CHECK(result.verdict == Verdict::Violation && result.key == "a");
    });

    test("checker: random histories from a real sequential run always pass", [] {
        // Generate operations whose true order is known, give each a time window
        // that contains its true instant, and check that the search finds an order.
        Rng rng(77);
        for (int round = 0; round < 200; ++round) {
            std::map<std::string, std::optional<std::string>> truth;
            std::vector<Operation> history;
            const int n = 20 + static_cast<int>(rng.below(40));
            for (int i = 0; i < n; ++i) {
                const std::string key = "k" + std::to_string(rng.below(2));
                const std::string token = "t" + std::to_string(i) + ";";
                const auto roll = rng.below(4);
                const std::string input = roll == 0 ? kv::put(key, token)
                    : roll == 1 ? kv::append(key, token)
                    : roll == 2 ? kv::get(key)
                    : kv::cas(key, truth[key].value_or("?"), token);
                const auto output = kv::step(truth[key], kv::decode_command(input));
                // True instant is 100*i + 50; windows stretch up to 250 around it.
                const std::uint64_t instant = 100 * static_cast<std::uint64_t>(i) + 500;
                Operation o;
                o.client = static_cast<std::uint64_t>(i);
                o.invoke = instant - 1 - rng.below(250);
                o.ret = rng.below(10) == 0 ? never : instant + 1 + rng.below(250);
                o.input = input;
                o.output = kv::encode(output);
                history.push_back(std::move(o));
            }
            // Times must be unique for the checker's event order.
            std::set<std::uint64_t> used;
            for (auto& o : history) {
                while (!used.insert(o.invoke).second) ++o.invoke;
            }
            for (auto& o : history) {
                if (!o.completed()) continue;
                while (!used.insert(o.ret).second) ++o.ret;
            }
            CHECK(lin(history));
            // Corrupt one completed read with a value nobody ever wrote.
            for (auto& o : history) {
                if (o.completed() && kv::decode_command(o.input).op == kv::Op::Get) {
                    o.output = kv::encode(value("never-written"));
                    CHECK(!lin(history));
                    break;
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------

void codec_and_machine_tests() {
    test("message codec round-trips every message type", [] {
        Message m;
        m.from = 3;
        m.to = 7;
        m.term = 0x0102030405060708ULL;
        m.type = MsgType::RequestVote;
        m.pre_vote = true;
        m.last_log_index = 99;
        m.last_log_term = 5;
        CHECK(decode_message(encode_message(m)) == m);

        Message r;
        r.type = MsgType::VoteReply;
        r.from = 1;
        r.to = 2;
        r.term = 9;
        r.granted = true;
        CHECK(decode_message(encode_message(r)) == r);

        Message a;
        a.type = MsgType::AppendEntries;
        a.from = 1;
        a.to = 2;
        a.term = 4;
        a.prev_index = 10;
        a.prev_term = 3;
        a.leader_commit = 8;
        a.entries = {Entry{3, "abc"}, Entry{4, std::string("\0\xff binary", 9)}, Entry{4, {}}};
        CHECK(decode_message(encode_message(a)) == a);

        Message ar;
        ar.type = MsgType::AppendReply;
        ar.from = 2;
        ar.to = 1;
        ar.term = 4;
        ar.success = false;
        ar.conflict_index = 6;
        ar.conflict_term = 2;
        CHECK(decode_message(encode_message(ar)) == ar);

        Message s;
        s.type = MsgType::InstallSnapshot;
        s.from = 1;
        s.to = 3;
        s.term = 4;
        s.snap_index = 1000;
        s.snap_term = 3;
        s.snapshot = std::string(5000, 'z');
        CHECK(decode_message(encode_message(s)) == s);

        Message sr;
        sr.type = MsgType::SnapshotReply;
        sr.from = 3;
        sr.to = 1;
        sr.term = 4;
        sr.match_index = 1000;
        CHECK(decode_message(encode_message(sr)) == sr);
    });

    test("decoders reject malformed input without reading out of bounds", [] {
        Message a;
        a.type = MsgType::AppendEntries;
        a.from = 1;
        a.to = 2;
        a.entries = {Entry{3, "payload"}};
        const auto good = encode_message(a);
        for (std::size_t cut = 0; cut < good.size(); ++cut)
            CHECK(throws([&] { (void)decode_message(std::string_view(good).substr(0, cut)); }));
        CHECK(throws([&] { (void)decode_message(good + "x"); }));
        // Random bytes: must either decode or throw DecodeError, never crash.
        Rng rng(5);
        for (int i = 0; i < 20000; ++i) {
            std::string noise(rng.below(64), '\0');
            for (auto& ch : noise) ch = static_cast<char>(rng.below(256));
            try {
                (void)decode_message(noise);
            } catch (const DecodeError&) {
            }
            try {
                (void)kv::decode_command(noise);
            } catch (const DecodeError&) {
            }
        }
        // A forged entry count cannot force a huge allocation.
        Writer forged;
        forged.u8(3);
        forged.u32(1);
        forged.u32(2);
        forged.u64(1);
        forged.u64(0);
        forged.u64(0);
        forged.u64(0);
        forged.u32(0xFFFFFFFFU);
        CHECK(throws([&] { (void)decode_message(forged.str()); }));
    });

    test("crc32 matches the standard check value", [] {
        CHECK(crc32("123456789") == 0xCBF43926U);
        CHECK(crc32("") == 0);
    });

    test("key-value semantics", [] {
        kv::Store store;
        const auto run = [&](const std::string& command) { return kv::decode_result(store.apply(command)); };
        CHECK(run(kv::get("a")).status == kv::Status::NotFound);
        CHECK(run(kv::put("a", "1")) == ok);
        CHECK(run(kv::get("a")) == value("1"));
        CHECK(run(kv::append("a", "2")) == ok);
        CHECK(run(kv::append("b", "x")) == ok);
        CHECK(run(kv::get("a")) == value("12"));
        CHECK(run(kv::cas("a", "12", "3")) == ok);
        CHECK(run(kv::cas("a", "12", "4")) == (kv::Result{kv::Status::Mismatch, "3"}));
        CHECK(run(kv::cas("zz", "", "4")).status == kv::Status::NotFound);
        CHECK(run(kv::del("b")) == ok);
        CHECK(run(kv::del("b")).status == kv::Status::NotFound);
        CHECK(kv::decode_result(store.apply("garbage")).status == kv::Status::BadRequest);

        kv::Store copy;
        copy.restore(store.snapshot());
        CHECK(copy.digest() == store.digest());
        CHECK(kv::decode_result(copy.apply(kv::get("a"))) == value("3"));
        CHECK(throws([&] { copy.restore("not a snapshot"); }));
    });
}

std::vector<std::string> session_commands(const std::string& path) {
    std::ifstream file(path);
    if (!file) throw std::runtime_error("cannot open " + path);
    std::vector<std::string> commands;
    for (std::string line; std::getline(file, line);) {
        const auto parsed = matching::parse_command(line);
        if (!parsed) continue;
        const auto& c = *parsed;
        switch (c.action) {
        case matching::Action::Submit:
            commands.push_back(c.order.type == matching::OrderType::Limit
                ? exchange::limit(c.order.id, c.order.side, c.order.price, c.order.quantity)
                : exchange::market(c.order.id, c.order.side, c.order.quantity));
            break;
        case matching::Action::Cancel: commands.push_back(exchange::cancel(c.order.id)); break;
        case matching::Action::Amend:
            commands.push_back(exchange::amend(c.order.id, c.order.price, c.order.quantity));
            break;
        default: break; // BOOK, ORDERS, CHECK do not change state.
        }
    }
    return commands;
}

void exchange_tests(const std::string& session_path) {
    test("replicated book reproduces the standalone engine's fingerprints", [&] {
        // These two values are printed by `matching-engine --file examples/session.txt`.
        const std::uint64_t expected_events = 0xdf3ac538e031b5bcULL;
        const std::uint64_t expected_state = 0xc41876d9ad53c569ULL;
        const auto commands = session_commands(session_path);
        CHECK(commands.size() == 12);
        exchange::Book book;
        for (const auto& command : commands) (void)book.apply(command);
        const auto summary = book.summarize();
        CHECK(summary.commands == 12 && summary.active == 0);
        CHECK(summary.trades == 6 && summary.rejected == 1);
        CHECK(summary.events == expected_events);
        CHECK(summary.state == expected_state);
        CHECK(exchange::decode_summary(book.apply(exchange::summary())) == summary);
    });

    test("a book restored from any snapshot point finishes identically", [&] {
        const auto commands = session_commands(session_path);
        exchange::Book whole;
        for (const auto& command : commands) (void)whole.apply(command);
        for (std::size_t cut = 0; cut <= commands.size(); ++cut) {
            exchange::Book first;
            for (std::size_t i = 0; i < cut; ++i) (void)first.apply(commands[i]);
            exchange::Book second;
            second.restore(first.snapshot());
            second.check_invariants();
            CHECK(second.digest() == first.digest());
            for (std::size_t i = cut; i < commands.size(); ++i) (void)second.apply(commands[i]);
            CHECK(second.summarize() == whole.summarize());
        }
    });

    test("random order flow: snapshot and restore preserve FIFO priority", [] {
        Rng rng(2027);
        exchange::Book live;
        std::string snapshot;
        std::vector<std::string> after;
        std::uint64_t next_id = 1;
        for (int i = 0; i < 6000; ++i) {
            if (i == 3000) snapshot = live.snapshot();
            const auto roll = rng.below(10);
            const auto side = rng.below(2) ? matching::Side::Buy : matching::Side::Sell;
            std::string command;
            if (roll < 6) command = exchange::limit(next_id++, side,
                9990 + static_cast<matching::Price>(rng.below(21)), 1 + static_cast<matching::Quantity>(rng.below(50)));
            else if (roll < 7) command = exchange::market(next_id++, side, 1 + static_cast<matching::Quantity>(rng.below(80)));
            else if (roll < 9) command = exchange::cancel(1 + rng.below(next_id));
            else command = exchange::amend(1 + rng.below(next_id),
                9990 + static_cast<matching::Price>(rng.below(21)), 1 + static_cast<matching::Quantity>(rng.below(50)));
            const auto outcome = exchange::decode_outcome(live.apply(command));
            CHECK(outcome.sequence == static_cast<std::uint64_t>(i) + 1);
            if (i >= 3000) after.push_back(command);
        }
        exchange::Book restored;
        restored.restore(snapshot);
        for (const auto& command : after) (void)restored.apply(command);
        restored.check_invariants();
        CHECK(restored.summarize() == live.summarize());
        CHECK(live.summarize().trades > 500);
    });

    test("malformed exchange commands are deterministic no-ops", [] {
        exchange::Book book;
        const auto before = book.digest();
        CHECK(book.apply("") == std::string(1, '\x02'));
        CHECK(book.apply(std::string("\x01\x02", 2)) == std::string(1, '\x02'));
        CHECK(book.apply(std::string(1, '\x09')) == std::string(1, '\x02'));
        CHECK(book.digest() == before);
    });
}

void replica_tests() {
    test("a retried command is applied exactly once", [] {
        MemStorage storage;
        kv::Store store;
        Replica replica(1, {1}, Config{}, storage, store, 1);
        const auto settle = [&] {
            replica.flush();
            storage.sync();
            (void)replica.take_messages();
            replica.advance();
            return replica.take_replies();
        };
        for (int i = 0; i < 30; ++i) replica.tick();
        (void)settle();
        CHECK(replica.raft().role() == Role::Leader);

        replica.submit({7, 1, kv::append("k", "x;")});
        auto replies = settle();
        CHECK(replies.size() == 1 && replies[0].status == ReplyStatus::Ok);
        // The client never saw that answer and tries again, three times.
        for (int i = 0; i < 3; ++i) {
            replica.submit({7, 1, kv::append("k", "x;")});
            replies = settle();
            CHECK(replies.size() == 1 && replies[0].status == ReplyStatus::Ok);
        }
        replica.submit({7, 2, kv::get("k")});
        replies = settle();
        CHECK(kv::decode_result(replies[0].result) == value("x;"));
        // An old sequence number is refused, not re-run.
        replica.submit({7, 1, kv::append("k", "x;")});
        replies = settle();
        CHECK(replies[0].status == ReplyStatus::Stale);
    });

    test("a follower redirects clients to the leader", [] {
        MemStorage storage;
        kv::Store store;
        Replica replica(2, {1, 2, 3}, Config{}, storage, store, 1);
        Message hb;
        hb.type = MsgType::AppendEntries;
        hb.from = 1;
        hb.to = 2;
        hb.term = 3;
        replica.step(hb);
        replica.submit({9, 1, kv::get("k")});
        replica.advance();
        const auto replies = replica.take_replies();
        CHECK(replies.size() == 1);
        CHECK(replies[0].status == ReplyStatus::NotLeader);
        CHECK(replies[0].leader == 1);
    });

    test("sessions and state survive a snapshot and restart", [] {
        MemStorage storage;
        ReplicaOptions options;
        options.snapshot_threshold = 8;
        options.snapshot_keep = 2;
        std::uint64_t digest = 0;
        Index applied = 0;
        {
            kv::Store store;
            Replica replica(1, {1}, Config{}, storage, store, 1, options);
            for (int i = 0; i < 30; ++i) replica.tick();
            for (std::uint64_t seq = 1; seq <= 40; ++seq) {
                replica.submit({5, seq, kv::append("k", std::to_string(seq) + ";")});
                replica.flush();
                storage.sync();
                (void)replica.take_messages();
                replica.advance();
                (void)replica.take_replies();
            }
            CHECK(replica.raft().log().base_index() > 20); // Compaction happened.
            digest = replica.digest();
            applied = replica.applied();
            storage.sync();
        }
        kv::Store store;
        Replica replica(1, {1}, Config{}, storage, store, 2, options);
        for (int i = 0; i < 30; ++i) replica.tick();
        replica.flush();
        storage.sync();
        (void)replica.take_messages();
        replica.advance();
        (void)replica.take_replies();
        CHECK(replica.applied() >= applied);
        // Same data; the retry of the last command is still recognized.
        replica.submit({5, 40, kv::append("k", "40;")});
        replica.advance();
        const auto replies = replica.take_replies();
        CHECK(replies.size() == 1 && replies[0].status == ReplyStatus::Ok);
        kv::Store expected;
        for (std::uint64_t seq = 1; seq <= 40; ++seq)
            (void)expected.apply(kv::append("k", std::to_string(seq) + ";"));
        CHECK(store.digest() == expected.digest());
        (void)digest;
    });
}

void simulation_tests() {
    test("simulation: 240 fault-injected clusters keep every guarantee", [] {
        std::uint64_t operations = 0;
        for (std::uint64_t seed = 1; seed <= 240; ++seed) {
            SimOptions options;
            options.seed = seed;
            options.ticks = 2500;
            Simulation simulation(options);
            const auto result = simulation.run();
            if (!result.ok)
                throw std::runtime_error("seed " + std::to_string(seed) + ": " + result.kind +
                    ": " + result.detail);
            operations += result.stats.operations;
        }
        CHECK(operations > 30000);
    });

    test("simulation: the same seed replays identically", [] {
        SimOptions options;
        options.seed = 31337;
        Simulation a(options), b(options);
        const auto ra = a.run();
        const auto rb = b.run();
        CHECK(ra.ok && rb.ok);
        CHECK(ra.stats.messages == rb.stats.messages);
        CHECK(ra.stats.committed == rb.stats.committed);
        CHECK(a.history().size() == b.history().size());
        for (std::size_t i = 0; i < a.history().size(); ++i) {
            CHECK(a.history()[i].invoke == b.history()[i].invoke);
            CHECK(a.history()[i].ret == b.history()[i].ret);
            CHECK(a.history()[i].output == b.history()[i].output);
        }
    });

    // Mutation tests: a checker that never fails proves nothing. Each of these
    // deliberate bugs must be caught within a fixed seed budget.
    struct Mutant {
        const char* name;
        bool TestBugs::* flag;
        std::uint64_t budget;
    };
    const Mutant mutants[] = {
        {"votes ignore log freshness", &TestBugs::skip_up_to_date_check, 60},
        {"retried command applied twice", &TestBugs::no_dedupe, 60},
        {"leader serves reads without committing them", &TestBugs::stale_read, 60},
        {"vote forgotten across a restart", &TestBugs::forget_vote, 300},
        {"messages released before sync", &TestBugs::send_before_sync, 400},
    };
    for (const auto& mutant : mutants) {
        test(mutant.name, [&] {
            for (std::uint64_t seed = 1; seed <= mutant.budget; ++seed) {
                SimOptions options;
                options.seed = seed;
                options.raft.bugs.*mutant.flag = true;
                Simulation simulation(options);
                if (!simulation.run().ok) return;
            }
            throw std::runtime_error("injected bug was not detected within the seed budget");
        });
    }
}

} // namespace

int main(int argc, char** argv) {
    const std::string session = argc > 1 ? argv[1] : "examples/session.txt";
    try {
        raft_tests();
        test("Raft Figure 8: an old-term entry on a majority is not committed", [] { figure8(false); });
        test("Raft Figure 8: the injected commit bug loses a committed entry", [] { figure8(true); });
        test("an acknowledgment from an earlier term is not counted", [] { stale_acknowledgment(false); });
        test("the injected stale-acknowledgment bug commits without a majority", [] { stale_acknowledgment(true); });
        checker_tests();
        codec_and_machine_tests();
        exchange_tests(session);
        replica_tests();
        simulation_tests();
    } catch (const std::exception& error) {
        std::cerr << "FAILED " << error.what() << '\n';
        return 1;
    }
    std::cout << "core tests passed: " << cases << " cases, " << assertions << " assertions\n";
    return 0;
}
