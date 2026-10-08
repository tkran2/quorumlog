#pragma once

#include "quorum/kv.hpp"
#include "quorum/linearizability.hpp"
#include "quorum/replica.hpp"

#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <vector>

namespace quorum {

struct SimOptions {
    std::uint64_t seed{1};
    int nodes{5};
    int clients{4};
    int keys{3};
    int ticks{4000};         // Length of the fault phase.
    int settle_ticks{2000};  // Recovery budget after faults stop.
    int min_delay{1};
    int max_delay{3};
    double drop{0.03};
    double duplicate{0.02};
    double straggle{0.01};   // Chance a message is delayed far beyond max_delay.
    int straggle_ticks{150}; // Enough to outlive an election or two.
    double fault_rate{0.02}; // Chance per tick of one fault action.
    int op_gap{6};           // Mean idle ticks between one client's operations.
    int client_timeout{25};
    std::size_t snapshot_threshold{24};
    std::size_t snapshot_keep{6};
    Config raft;
    // Vary cluster size (3 to 5) and replication batch size by seed, so one
    // run of many seeds covers small quorums and partially replicated batches.
    bool vary{true};
    bool trace{};
};

struct SimStats {
    std::uint64_t operations{};       // Client operations that returned.
    std::uint64_t pending{};          // Operations with no answer at the end.
    std::uint64_t leader_changes{};
    std::uint64_t max_term{};
    std::uint64_t messages{};
    std::uint64_t dropped{};
    std::uint64_t crashes{};
    std::uint64_t restarts{};
    std::uint64_t partitions{};
    std::uint64_t stalls{};
    std::uint64_t snapshots_installed{};
    std::uint64_t committed{};
    std::uint64_t checker_states{};
    std::int64_t settled_after{-1}; // Ticks from the end of faults to convergence.
};

struct SimResult {
    bool ok{true};
    std::string kind;   // Which property failed.
    std::string detail;
    std::int64_t tick{};
    SimStats stats;
};

// A whole cluster in one thread with a fake clock and a fake network. Every
// source of nondeterminism (message delay, loss, duplication, partitions,
// crashes, stalls, client timing) is drawn from one seeded generator, so a
// failing seed replays exactly. After each step the simulator checks the
// properties Raft promises instead of trusting the code under test.
class Simulation {
public:
    explicit Simulation(const SimOptions& options)
        : options_(options), rng_(options.seed * 0x9E3779B97F4A7C15ULL + 0x1234567ULL) {
        if (options_.vary) {
            options_.nodes = 3 + static_cast<int>(options_.seed % 3);
            static constexpr std::size_t batches[] = {1, 2, 4, 512};
            options_.raft.max_batch_entries = batches[(options_.seed / 3) % 4];
        }
        const auto n = static_cast<std::size_t>(options_.nodes);
        for (std::size_t i = 0; i < n; ++i) members_.push_back(static_cast<NodeId>(i + 1));
        blocked_.assign(n, std::vector<bool>(n, false));
        for (std::size_t i = 0; i < n; ++i) {
            nodes_.push_back(std::make_unique<Node>());
            nodes_.back()->id = members_[i];
        }
        clients_.resize(static_cast<std::size_t>(options_.clients));
        for (std::size_t i = 0; i < clients_.size(); ++i) {
            clients_[i].id = i + 1;
            clients_[i].target = static_cast<int>(rng_.below(n));
            clients_[i].next_start = 1 + static_cast<std::int64_t>(rng_.below(20));
        }
        drop_ = options_.drop;
        duplicate_ = options_.duplicate;
        straggle_ = options_.straggle;
    }

    SimResult run() {
        try {
            return run_checked();
        } catch (const std::exception& error) {
            // The core throws when asked to do something Raft forbids, such as
            // overwriting a committed entry. That is a failed run, not a crash.
            fail("internal-invariant", error.what());
            return result_;
        }
    }

    [[nodiscard]] const std::vector<Operation>& history() const { return history_; }
    [[nodiscard]] const SimOptions& options() const { return options_; }

private:
    SimResult run_checked() {
        for (std::size_t i = 0; i < nodes_.size(); ++i) start(i);
        const std::int64_t end = options_.ticks + options_.settle_ticks;
        bool settled = false;
        for (now_ = 1; now_ <= end && result_.ok; ++now_) {
            const bool faults = now_ <= options_.ticks;
            if (faults && rng_.chance(options_.fault_rate)) nemesis();
            if (now_ == options_.ticks + 1) stop_faults();

            while (result_.ok && !queue_.empty() && queue_.top().time <= now_) {
                Event event = queue_.top();
                queue_.pop();
                deliver(std::move(event));
            }
            for (std::size_t c = 0; result_.ok && c < clients_.size(); ++c) client_step(c);
            for (std::size_t i = 0; result_.ok && i < nodes_.size(); ++i) {
                auto& node = *nodes_[i];
                if (!node.alive || now_ < node.stalled_until) continue;
                node.replica->tick();
                pump(i);
            }
            if (result_.ok && now_ % 20 == 0) check_log_matching();
            if (result_.ok && !faults && converged()) {
                settled = true;
                result_.stats.settled_after = now_ - options_.ticks;
                break;
            }
        }
        if (result_.ok && !settled)
            fail("liveness", "cluster did not converge after faults stopped: " + describe());
        if (result_.ok) check_log_matching();

        for (const auto& op : history_) {
            if (op.completed()) ++result_.stats.operations; else ++result_.stats.pending;
        }
        if (result_.ok) {
            const auto check = check_linearizable(history_);
            result_.stats.checker_states = check.states;
            if (check.verdict == Verdict::Violation)
                fail("linearizability", "no legal order exists for key " + check.key);
            else if (check.verdict == Verdict::Inconclusive)
                fail("checker-budget", "search budget exhausted on key " + check.key);
        }
        return result_;
    }

    struct Node {
        NodeId id{};
        MemStorage storage;
        std::unique_ptr<kv::Store> machine;
        std::unique_ptr<Replica> replica;
        bool alive{};
        bool die_mid_step{};
        std::int64_t stalled_until{};
        std::uint64_t incarnation{};
        bool was_leader{};
        Term leader_term{};
        Term seen_term{};
        Index seen_commit{};
        NodeId seen_vote{no_node};
        Term seen_vote_term{};
        Index seen_last{};
    };

    struct Client {
        std::uint64_t id{};
        std::uint64_t seq{};
        bool busy{};
        ClientRequest request;
        int target{};
        std::int64_t deadline{};
        std::int64_t next_start{};
        bool redirected{};
        std::size_t history_index{};
        std::map<std::string, std::string> last_seen;
    };

    enum class Kind { Peer, Request, Reply };

    struct Event {
        std::int64_t time{};
        std::uint64_t order{};
        Kind kind{Kind::Peer};
        std::size_t target{}; // Node index for Peer/Request, client index for Reply.
        Message message;
        ClientRequest request;
        ClientReply reply;
        bool operator>(const Event& other) const {
            return time != other.time ? time > other.time : order > other.order;
        }
    };

    struct Committed {
        Term term{};        // Term stored in the entry.
        Term commit_term{}; // Term of the leader that committed it.
        std::uint64_t hash{};
        std::uint64_t digest{};
    };

    void fail(const std::string& kind, const std::string& detail) {
        if (!result_.ok) return;
        result_.ok = false;
        result_.kind = kind;
        result_.detail = detail;
        result_.tick = now_;
    }

    void trace(const std::string& text) const {
        if (options_.trace) std::fprintf(stderr, "[%6lld] %s\n", static_cast<long long>(now_), text.c_str());
    }

    void start(std::size_t index) {
        auto& node = *nodes_[index];
        ++node.incarnation;
        node.machine = std::make_unique<kv::Store>();
        ReplicaOptions replica_options;
        replica_options.snapshot_threshold = options_.snapshot_threshold;
        replica_options.snapshot_keep = options_.snapshot_keep;
        node.replica = std::make_unique<Replica>(node.id, members_, options_.raft, node.storage,
            *node.machine, options_.seed * 7919 + node.incarnation * 104729, replica_options);
        node.replica->on_apply = [this, index](Index i, const Entry& entry) {
            observe_apply(index, i, entry);
        };
        node.replica->on_restore = [this, index](Index i) { observe_restore(index, i); };
        node.alive = true;
        node.die_mid_step = false;
        node.was_leader = false;
        node.seen_commit = 0;
        trace("start node " + std::to_string(node.id));
        pump(index);
    }

    void crash(std::size_t index) {
        auto& node = *nodes_[index];
        node.storage.crash();
        node.replica.reset();
        node.machine.reset();
        node.alive = false;
        node.die_mid_step = false;
        node.stalled_until = 0;
        ++result_.stats.crashes;
        trace("crash node " + std::to_string(node.id));
    }

    // The host loop the real server also follows: flush, make state durable,
    // only then release messages, apply, and answer clients.
    void pump(std::size_t index) {
        auto& node = *nodes_[index];
        auto& replica = *node.replica;
        replica.flush();
        if (options_.raft.bugs.send_before_sync) {
            for (auto& message : replica.take_messages()) send(std::move(message));
        }
        if (node.die_mid_step) {
            // Death between computing a step and making it durable: everything
            // not yet synced is lost, and nothing may have left the machine.
            crash(index);
            return;
        }
        node.storage.sync();
        for (auto& message : replica.take_messages()) send(std::move(message));
        replica.advance();
        for (auto& reply : replica.take_replies()) send_reply(index, std::move(reply));
        if (result_.ok) observe(index);
        if (result_.ok) targeted_fault(index);
    }

    // Random faults rarely land in the few ticks where they hurt most. These
    // two aim at exactly those moments: right after a node casts a vote, and
    // right after a leader hands out entries that may reach only some peers.
    void targeted_fault(std::size_t index) {
        auto& node = *nodes_[index];
        const auto& raft = node.replica->raft();
        const NodeId vote = raft.vote();
        const Term term = raft.term();
        const Index last = raft.log().last_index();
        const bool voted_for_peer =
            vote != no_node && vote != node.id && (vote != node.seen_vote || term != node.seen_vote_term);
        const bool leader_grew = raft.role() == Role::Leader && last > node.seen_last;
        node.seen_vote = vote;
        node.seen_vote_term = term;
        node.seen_last = last;
        if (now_ > options_.ticks) return;
        if (voted_for_peer && rng_.chance(0.04)) {
            trace("bounce voter " + std::to_string(node.id));
            crash(index);
            ++result_.stats.restarts;
            start(index);
        } else if (leader_grew && rng_.chance(0.004)) {
            trace("crash leader " + std::to_string(node.id) + " after partial replication");
            crash(index);
        }
    }

    void send(Message message) {
        ++result_.stats.messages;
        if (message.type == MsgType::InstallSnapshot) ++result_.stats.snapshots_installed;
        if (rng_.chance(drop_)) {
            ++result_.stats.dropped;
            return;
        }
        Event event;
        event.kind = Kind::Peer;
        event.target = message.to - 1;
        event.message = std::move(message);
        const bool twice = rng_.chance(duplicate_);
        if (twice) {
            Event copy = event;
            schedule(std::move(copy));
        }
        schedule(std::move(event));
    }

    void send_reply(std::size_t from_node, ClientReply reply) {
        (void)from_node;
        if (reply.client == 0 || reply.client > clients_.size()) return;
        if (rng_.chance(drop_)) return;
        Event event;
        event.kind = Kind::Reply;
        event.target = static_cast<std::size_t>(reply.client - 1);
        event.reply = std::move(reply);
        schedule(std::move(event));
    }

    void schedule(Event event) {
        const auto spread = static_cast<std::uint64_t>(options_.max_delay - options_.min_delay + 1);
        event.time = now_ + options_.min_delay + static_cast<std::int64_t>(rng_.below(spread));
        // An asynchronous network may hold a message for a long time and then
        // deliver it into a later term. Correct nodes must recognize it as old.
        if (straggle_ > 0 && rng_.chance(straggle_))
            event.time += 1 + static_cast<std::int64_t>(
                rng_.below(static_cast<std::uint64_t>(options_.straggle_ticks)));
        event.order = ++order_;
        queue_.push(std::move(event));
    }

    void deliver(Event event) {
        if (event.kind == Kind::Reply) {
            client_reply(event.target, event.reply);
            return;
        }
        auto& node = *nodes_[event.target];
        if (!node.alive) return;
        if (now_ < node.stalled_until) {
            // A stalled process is not dead: its inbox waits for it.
            event.time = node.stalled_until;
            event.order = ++order_;
            queue_.push(std::move(event));
            return;
        }
        if (event.kind == Kind::Peer) {
            if (blocked_[event.message.from - 1][event.target]) {
                ++result_.stats.dropped;
                return;
            }
            node.replica->step(event.message);
        } else {
            node.replica->submit(event.request);
        }
        pump(event.target);
    }

    // ---- Faults -----------------------------------------------------------

    void nemesis() {
        const auto n = nodes_.size();
        const auto roll = rng_.below(100);
        if (roll < 16) {
            // Split the cluster into two groups that cannot talk to each other.
            std::vector<bool> side(n);
            for (std::size_t i = 0; i < n; ++i) side[i] = rng_.chance(0.5);
            for (std::size_t a = 0; a < n; ++a)
                for (std::size_t b = 0; b < n; ++b) blocked_[a][b] = side[a] != side[b];
            ++result_.stats.partitions;
            trace("partition");
        } else if (roll < 30) {
            // Cut one node off entirely, preferring the current leader.
            std::size_t victim = rng_.below(n);
            if (rng_.chance(0.6))
                for (std::size_t i = 0; i < n; ++i)
                    if (nodes_[i]->alive && nodes_[i]->replica->raft().role() == Role::Leader)
                        victim = i;
            for (std::size_t i = 0; i < n; ++i)
                if (i != victim) blocked_[i][victim] = blocked_[victim][i] = true;
            ++result_.stats.partitions;
            trace("isolate node " + std::to_string(victim + 1));
        } else if (roll < 38) {
            // One-way failure: the victim can send but hears nothing.
            const std::size_t victim = rng_.below(n);
            for (std::size_t i = 0; i < n; ++i)
                if (i != victim) blocked_[i][victim] = true;
            ++result_.stats.partitions;
            trace("deafen node " + std::to_string(victim + 1));
        } else if (roll < 58) {
            for (auto& row : blocked_) row.assign(n, false);
            trace("heal network");
        } else if (roll < 78) {
            std::size_t victim = rng_.below(n);
            if (rng_.chance(0.4))
                for (std::size_t i = 0; i < n; ++i)
                    if (nodes_[i]->alive && nodes_[i]->replica->raft().role() == Role::Leader)
                        victim = i;
            if (!nodes_[victim]->alive) return;
            const auto how = rng_.below(100);
            if (how < 30) {
                nodes_[victim]->die_mid_step = true;
            } else if (how < 55) {
                // Bounce: die and come straight back, inside one election.
                crash(victim);
                ++result_.stats.restarts;
                start(victim);
            } else {
                crash(victim);
            }
        } else if (roll < 92) {
            std::vector<std::size_t> dead;
            for (std::size_t i = 0; i < n; ++i)
                if (!nodes_[i]->alive) dead.push_back(i);
            if (dead.empty()) return;
            ++result_.stats.restarts;
            start(dead[rng_.below(dead.size())]);
        } else {
            // Freeze a process (a long pause, or a suspended machine). It keeps
            // its old beliefs, including "I am the leader", while time moves on.
            const std::size_t victim = rng_.below(n);
            if (!nodes_[victim]->alive) return;
            nodes_[victim]->stalled_until = now_ + 5 + static_cast<std::int64_t>(rng_.below(60));
            ++result_.stats.stalls;
            trace("stall node " + std::to_string(victim + 1));
        }
    }

    void stop_faults() {
        drop_ = 0;
        duplicate_ = 0;
        straggle_ = 0;
        for (auto& row : blocked_) row.assign(nodes_.size(), false);
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            nodes_[i]->stalled_until = 0;
            nodes_[i]->die_mid_step = false;
            if (!nodes_[i]->alive) {
                ++result_.stats.restarts;
                start(i);
            }
        }
        trace("faults stopped");
    }

    // ---- Clients ----------------------------------------------------------

    void client_step(std::size_t index) {
        auto& client = clients_[index];
        if (client.busy) {
            if (now_ < client.deadline) return;
            // No answer in time. The request may or may not have taken effect;
            // retry the SAME sequence number, at the hinted leader if a server
            // named one, otherwise at the next node.
            if (!client.redirected)
                client.target = static_cast<int>((static_cast<std::size_t>(client.target) + 1) % nodes_.size());
            client.redirected = false;
            send_request(client);
            return;
        }
        if (now_ > options_.ticks || now_ < client.next_start) return;

        const std::string key = "k" + std::to_string(rng_.below(static_cast<std::uint64_t>(options_.keys)));
        const std::string token = "c" + std::to_string(client.id) + "." + std::to_string(client.seq + 1) + ";";
        const auto roll = rng_.below(100);
        std::string payload;
        if (roll < 22) payload = kv::put(key, token);
        else if (roll < 54) payload = kv::append(key, token);
        else if (roll < 84) payload = kv::get(key);
        else {
            const auto seen = client.last_seen.find(key);
            const std::string expected =
                seen != client.last_seen.end() && rng_.chance(0.7) ? seen->second : token;
            payload = kv::cas(key, expected, token);
        }

        client.busy = true;
        client.request = ClientRequest{client.id, ++client.seq, std::move(payload)};
        Operation op;
        op.client = client.id;
        op.invoke = ++clock_;
        op.input = client.request.payload;
        client.history_index = history_.size();
        history_.push_back(std::move(op));
        send_request(client);
    }

    void send_request(Client& client) {
        client.deadline = now_ + options_.client_timeout;
        if (rng_.chance(drop_)) return;
        Event event;
        event.kind = Kind::Request;
        event.target = static_cast<std::size_t>(client.target);
        event.request = client.request;
        schedule(std::move(event));
    }

    void client_reply(std::size_t index, const ClientReply& reply) {
        auto& client = clients_[index];
        if (!client.busy || reply.seq != client.seq) return; // Late duplicate.
        switch (reply.status) {
        case ReplyStatus::Ok: {
            auto& op = history_[client.history_index];
            op.ret = ++clock_;
            op.output = reply.result;
            client.busy = false;
            client.next_start = now_ + 1 + static_cast<std::int64_t>(
                rng_.below(static_cast<std::uint64_t>(2 * options_.op_gap)));
            const auto command = kv::decode_command(op.input);
            if (command.op == kv::Op::Get) {
                const auto result = kv::decode_result(op.output);
                if (result.status == kv::Status::Ok) client.last_seen[command.key] = result.value;
            }
            break;
        }
        case ReplyStatus::NotLeader:
            if (reply.leader != no_node && static_cast<int>(reply.leader - 1) != client.target) {
                client.target = static_cast<int>(reply.leader - 1);
                client.redirected = true;
            }
            client.deadline = now_ + 1 + static_cast<std::int64_t>(rng_.below(3));
            break;
        case ReplyStatus::Stale:
            fail("session", "server called an in-flight request stale");
            break;
        }
    }

    // ---- Checks -----------------------------------------------------------

    void observe(std::size_t index) {
        auto& node = *nodes_[index];
        const auto& raft = node.replica->raft();
        if (raft.term() < node.seen_term)
            return fail("term-regression", "node " + std::to_string(node.id) + " moved to an older term");
        node.seen_term = raft.term();
        result_.stats.max_term = std::max<std::uint64_t>(result_.stats.max_term, raft.term());
        if (raft.commit_index() < node.seen_commit)
            return fail("commit-regression", "node " + std::to_string(node.id) + " un-committed entries");
        const bool leader = raft.role() == Role::Leader;
        if (leader && raft.commit_index() > node.seen_commit &&
            raft.log().has(raft.commit_index())) {
            // Commit audit: when a leader declares an index committed, a
            // majority must really hold that entry on stable storage.
            const Index n = raft.commit_index();
            const Term term = raft.log().term_at(n);
            std::size_t holders = 0;
            for (const auto& other : nodes_)
                if (other->storage.durably_has(n, term)) ++holders;
            if (holders < raft.quorum())
                return fail("commit-without-quorum", "leader " + std::to_string(node.id) +
                    " committed index " + std::to_string(n) + " held durably by only " +
                    std::to_string(holders) + " of " + std::to_string(nodes_.size()));
        }
        node.seen_commit = raft.commit_index();

        if (leader && (!node.was_leader || raft.term() != node.leader_term)) {
            // Election Safety: at most one leader per term.
            const auto [it, inserted] = leaders_.emplace(raft.term(), node.id);
            if (!inserted && it->second != node.id)
                return fail("election-safety", "two leaders in term " + std::to_string(raft.term()));
            // Leader Completeness: a leader holds every entry committed in an
            // earlier term. (A leader of an OLD term may surface late, for
            // example after a long pause; it owes nothing to later terms and
            // can no longer commit anything.)
            for (Index i = raft.log().base_index() + 1; i < ghost_.size(); ++i) {
                if (!ghost_[i] || ghost_[i]->commit_term >= raft.term()) continue;
                if (!raft.log().has(i) || raft.log().term_at(i) != ghost_[i]->term ||
                    fnv1a(raft.log().at(i).data) != ghost_[i]->hash)
                    return fail("leader-completeness", "leader " + std::to_string(node.id) +
                        " of term " + std::to_string(raft.term()) +
                        " is missing committed index " + std::to_string(i));
            }
            node.leader_term = raft.term();
            ++result_.stats.leader_changes;
            trace("leader " + std::to_string(node.id) + " term " + std::to_string(raft.term()));
        }
        node.was_leader = leader;
    }

    // State Machine Safety: every node applies the same entry at each index,
    // and ends up in the same state after it.
    void observe_apply(std::size_t index, Index i, const Entry& entry) {
        auto& node = *nodes_[index];
        if (ghost_.size() <= i) ghost_.resize(i + 1);
        const auto hash = fnv1a(entry.data);
        const auto digest = node.replica->digest();
        if (!ghost_[i]) {
            // The first node to apply an index is the leader that committed it:
            // followers only learn the commit point from that leader afterward.
            ghost_[i] = Committed{entry.term, node.replica->raft().term(), hash, digest};
            ++result_.stats.committed;
            return;
        }
        if (ghost_[i]->term != entry.term || ghost_[i]->hash != hash)
            return fail("state-machine-safety", "node " + std::to_string(node.id) +
                " applied a different entry at index " + std::to_string(i));
        if (ghost_[i]->digest != digest)
            fail("replica-divergence", "node " + std::to_string(node.id) +
                " reached a different state at index " + std::to_string(i));
    }

    void observe_restore(std::size_t index, Index i) {
        auto& node = *nodes_[index];
        if (i < ghost_.size() && ghost_[i] && ghost_[i]->digest != node.replica->digest())
            fail("snapshot-divergence", "node " + std::to_string(node.id) +
                " loaded a snapshot that differs from the state at index " + std::to_string(i));
    }

    // Log Matching: if two logs agree on the term at an index, they are
    // identical up to and including that index.
    void check_log_matching() {
        for (std::size_t a = 0; a < nodes_.size() && result_.ok; ++a) {
            for (std::size_t b = a + 1; b < nodes_.size() && result_.ok; ++b) {
                if (!nodes_[a]->alive || !nodes_[b]->alive) continue;
                const auto& la = nodes_[a]->replica->raft().log();
                const auto& lb = nodes_[b]->replica->raft().log();
                const Index low = std::max(la.base_index(), lb.base_index()) + 1;
                const Index high = std::min(la.last_index(), lb.last_index());
                bool agreed = false;
                for (Index i = high; i >= low && i > 0; --i) {
                    if (!agreed && la.term_at(i) == lb.term_at(i)) agreed = true;
                    if (agreed && !(la.at(i) == lb.at(i))) {
                        fail("log-matching", "nodes " + std::to_string(a + 1) + " and " +
                            std::to_string(b + 1) + " differ at index " + std::to_string(i) +
                            " below a matching entry");
                        break;
                    }
                }
            }
        }
    }

    [[nodiscard]] std::string describe() const {
        std::string text;
        for (const auto& node : nodes_) {
            text += "[node " + std::to_string(node->id);
            if (!node->alive) {
                text += " down] ";
                continue;
            }
            const auto& raft = node->replica->raft();
            text += std::string(" ") + role_name(raft.role()) + " term=" + std::to_string(raft.term()) +
                " commit=" + std::to_string(raft.commit_index()) +
                " applied=" + std::to_string(node->replica->applied()) +
                " last=" + std::to_string(raft.log().last_index()) + "] ";
        }
        for (const auto& client : clients_)
            if (client.busy)
                text += "[client " + std::to_string(client.id) + " waiting on seq " +
                    std::to_string(client.seq) + " at node " + std::to_string(client.target + 1) + "] ";
        return text;
    }

    [[nodiscard]] bool converged() const {
        for (const auto& client : clients_)
            if (client.busy) return false;
        Index applied = 0;
        std::uint64_t digest = 0;
        bool leader = false;
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            const auto& node = *nodes_[i];
            if (!node.alive) return false;
            const auto& raft = node.replica->raft();
            leader = leader || raft.role() == Role::Leader;
            if (node.replica->applied() != raft.log().last_index()) return false;
            if (i == 0) {
                applied = node.replica->applied();
                digest = node.replica->digest();
            } else if (node.replica->applied() != applied || node.replica->digest() != digest) {
                return false;
            }
        }
        return leader;
    }

    SimOptions options_;
    Rng rng_;
    std::vector<NodeId> members_;
    std::vector<std::unique_ptr<Node>> nodes_;
    std::vector<Client> clients_;
    std::vector<std::vector<bool>> blocked_; // blocked_[from][to]
    std::priority_queue<Event, std::vector<Event>, std::greater<>> queue_;
    std::int64_t now_{};
    std::uint64_t order_{};
    std::uint64_t clock_{};
    double drop_{};
    double duplicate_{};
    double straggle_{};
    std::vector<std::optional<Committed>> ghost_;
    std::map<Term, NodeId> leaders_;
    std::vector<Operation> history_;
    SimResult result_;
};

} // namespace quorum
