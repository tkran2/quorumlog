#pragma once

#include "quorum/codec.hpp"
#include "quorum/raft.hpp"
#include "quorum/state_machine.hpp"

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace quorum {

// A client names itself and numbers its requests. It sends one request at a
// time and retries with the same sequence number until it gets an answer.
struct ClientRequest {
    std::uint64_t client{};
    std::uint64_t seq{};
    std::string payload;
};

enum class ReplyStatus : std::uint8_t {
    Ok = 0,
    NotLeader = 1, // Retry at `leader` if it is nonzero, otherwise at any node.
    Stale = 2,     // The client already moved past this sequence number.
};

struct ClientReply {
    std::uint64_t client{};
    std::uint64_t seq{};
    ReplyStatus status{ReplyStatus::Ok};
    NodeId leader{no_node};
    std::string result;
};

struct ReplicaOptions {
    // Take a snapshot once this many applied entries sit in the log...
    std::size_t snapshot_threshold{20000};
    // ...and drop the log prefix after this many more, so followers that are
    // only slightly behind can still catch up from the log.
    std::size_t snapshot_keep{2000};
};

// One server's replicated state: a Raft node, the state machine it drives, and
// the client sessions that make retried commands apply exactly once. Both the
// simulator and the TCP server run this class unchanged.
//
// Host loop, per batch of inputs:
//   tick()/step()/submit() ... ; flush(); storage.sync();
//   send take_messages(); advance(); send take_replies().
class Replica {
public:
    using ApplyObserver = std::function<void(Index, const Entry&)>;
    using RestoreObserver = std::function<void(Index)>;

    Replica(NodeId id, std::vector<NodeId> members, const Config& config, Storage& storage,
            StateMachine& machine, std::uint64_t seed, ReplicaOptions options = {})
        : raft_(id, std::move(members), config, storage, seed), machine_(machine),
          bugs_(config.bugs), options_(options) {}

    void tick() { raft_.tick(); }
    void step(const Message& message) { raft_.step(message); }
    void flush() { raft_.flush(); }
    [[nodiscard]] std::vector<Message> take_messages() { return raft_.take_messages(); }
    [[nodiscard]] std::vector<ClientReply> take_replies() { return std::exchange(replies_, {}); }

    void submit(const ClientRequest& request) {
        const auto session = sessions_.find(request.client);
        if (!bugs_.no_dedupe && session != sessions_.end()) {
            if (request.seq == session->second.seq) {
                // Already applied; the earlier answer was lost on the way back.
                reply(request, ReplyStatus::Ok, session->second.result);
                return;
            }
            if (request.seq < session->second.seq) {
                reply(request, ReplyStatus::Stale, {});
                return;
            }
        }
        if (raft_.role() != Role::Leader) {
            reply(request, ReplyStatus::NotLeader, {});
            return;
        }
        if (bugs_.stale_read && machine_.read_only(request.payload)) {
            // BUG (test only): a deposed leader that has not noticed yet answers
            // from state that newer writes have already replaced elsewhere.
            reply(request, ReplyStatus::Ok, machine_.query(request.payload));
            return;
        }
        const auto waiting = waiting_.find(request.client);
        if (waiting != waiting_.end() && waiting->second.seq == request.seq &&
            waiting->second.term == raft_.term())
            return; // Already proposed in this term; the answer is on its way.

        Writer envelope;
        envelope.u64(request.client);
        envelope.u64(request.seq);
        envelope.raw(request.payload);
        raft_.propose(envelope.take());
        waiting_[request.client] = Waiting{request.seq, raft_.term()};
    }

    // Apply newly committed entries, answer clients, and compact the log.
    // Call after storage.sync(): replies must never outrun durability.
    void advance() {
        if (auto snapshot = raft_.take_restore()) restore(*snapshot);
        while (applied_ < raft_.commit_index()) {
            ++applied_;
            const auto& entry = raft_.log().at(applied_);
            apply(entry);
            if (on_apply) on_apply(applied_, entry);
        }

        // Requests accepted under an old leadership may never commit. Release
        // them so their clients retry; the session table makes that safe.
        const bool leading = raft_.role() == Role::Leader;
        for (auto it = waiting_.begin(); it != waiting_.end();) {
            if (leading && it->second.term == raft_.term()) {
                ++it;
                continue;
            }
            ClientReply r;
            r.client = it->first;
            r.seq = it->second.seq;
            r.status = ReplyStatus::NotLeader;
            r.leader = leading ? raft_.id() : raft_.leader();
            replies_.push_back(std::move(r));
            it = waiting_.erase(it);
        }

        compact();
    }

    [[nodiscard]] const RaftNode& raft() const { return raft_; }
    [[nodiscard]] Index applied() const { return applied_; }
    [[nodiscard]] std::size_t waiting() const { return waiting_.size(); }

    // Fingerprint of everything replicated: the machine and the session table.
    [[nodiscard]] std::uint64_t digest() const {
        std::uint64_t hash = machine_.digest();
        for (const auto& [client, session] : sessions_) {
            Writer out;
            out.u64(client);
            out.u64(session.seq);
            out.bytes(session.result);
            hash = fnv1a(out.str(), hash);
        }
        return hash;
    }

    ApplyObserver on_apply;     // Test hook: called after each applied entry.
    RestoreObserver on_restore; // Test hook: called after loading a snapshot.

private:
    struct Session {
        std::uint64_t seq{};
        std::string result;
    };
    struct Waiting {
        std::uint64_t seq{};
        Term term{}; // Leadership term in which the request was proposed.
    };

    void reply(const ClientRequest& request, ReplyStatus status, std::string result) {
        ClientReply r;
        r.client = request.client;
        r.seq = request.seq;
        r.status = status;
        r.leader = status == ReplyStatus::NotLeader ? raft_.leader() : raft_.id();
        r.result = std::move(result);
        replies_.push_back(std::move(r));
    }

    void apply(const Entry& entry) {
        if (entry.data.size() < 16) return; // A leader's no-op (empty payload).
        Reader in(entry.data);
        const auto client = in.u64();
        const auto seq = in.u64();
        const std::string_view payload = std::string_view(entry.data).substr(16);

        const auto found = sessions_.find(client);
        if (bugs_.no_dedupe || found == sessions_.end() || seq > found->second.seq) {
            auto& session = sessions_[client];
            session.result = machine_.apply(payload);
            session.seq = seq;
        } else if (seq < found->second.seq) {
            return; // A late duplicate of a request the client has moved past.
        }
        // Otherwise this is a retry of the command already recorded: the state
        // machine is not touched again and the saved answer is returned.
        const auto& session = sessions_[client];

        const auto waiting = waiting_.find(client);
        if (waiting != waiting_.end() && waiting->second.seq == seq) {
            ClientReply r;
            r.client = client;
            r.seq = seq;
            r.status = ReplyStatus::Ok;
            r.leader = raft_.id();
            r.result = session.result;
            replies_.push_back(std::move(r));
            waiting_.erase(waiting);
        }
    }

    [[nodiscard]] std::string serialize() const {
        Writer out;
        out.u64(sessions_.size());
        for (const auto& [client, session] : sessions_) {
            out.u64(client);
            out.u64(session.seq);
            out.bytes(session.result);
        }
        out.bytes(machine_.snapshot());
        return out.take();
    }

    void restore(const Snapshot& snapshot) {
        Reader in(snapshot.data);
        std::map<std::uint64_t, Session> sessions;
        const auto count = in.u64();
        for (std::uint64_t i = 0; i < count; ++i) {
            const auto client = in.u64();
            Session session;
            session.seq = in.u64();
            session.result = in.bytes();
            sessions.emplace(client, std::move(session));
        }
        const auto machine = in.bytes();
        in.expect_done();
        machine_.restore(machine);
        sessions_ = std::move(sessions);
        applied_ = snapshot.index;
        staged_.reset();
        if (on_restore) on_restore(applied_);
    }

    void compact() {
        const Index base = raft_.log().base_index();
        if (staged_ && staged_->index <= base) staged_.reset(); // Superseded by an install.
        if (!staged_ && applied_ - base >= options_.snapshot_threshold) {
            // Capture state now; drop the log prefix later.
            staged_ = Snapshot{applied_, 0, serialize()};
        }
        if (staged_ && applied_ - staged_->index >= options_.snapshot_keep) {
            raft_.compact(staged_->index, std::move(staged_->data));
            staged_.reset();
        }
    }

    RaftNode raft_;
    StateMachine& machine_;
    TestBugs bugs_;
    ReplicaOptions options_;

    Index applied_{};
    std::map<std::uint64_t, Session> sessions_;   // Replicated; part of every snapshot.
    std::map<std::uint64_t, Waiting> waiting_;    // Requests proposed here, awaiting commit.
    std::optional<Snapshot> staged_;
    std::vector<ClientReply> replies_;
};

} // namespace quorum
