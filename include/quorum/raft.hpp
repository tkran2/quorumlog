#pragma once

#include "quorum/log.hpp"
#include "quorum/storage.hpp"

#include <algorithm>
#include <optional>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace quorum {

enum class Role { Follower, PreCandidate, Candidate, Leader };

inline const char* role_name(Role role) {
    switch (role) {
    case Role::Follower: return "follower";
    case Role::PreCandidate: return "pre-candidate";
    case Role::Candidate: return "candidate";
    case Role::Leader: return "leader";
    }
    return "?";
}

// splitmix64: tiny, fast, and identical on every platform, which keeps
// simulated runs reproducible from a seed.
class Rng {
public:
    explicit Rng(std::uint64_t seed) : state_(seed) {}
    std::uint64_t next() {
        std::uint64_t z = (state_ += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31U);
    }
    // Uniform in [0, bound). The modulo bias is irrelevant at these sizes.
    std::uint64_t below(std::uint64_t bound) { return bound == 0 ? 0 : next() % bound; }
    bool chance(double probability) {
        return static_cast<double>(next() >> 11U) * (1.0 / 9007199254740992.0) < probability;
    }

private:
    std::uint64_t state_;
};

// The consensus core. It performs no I/O and reads no clock: the host feeds it
// ticks, peer messages, and proposals, then collects outgoing messages. That
// makes every execution replayable, so the same code runs under the
// deterministic simulator and inside the real server.
//
// Host contract, once per batch of inputs:
//   1. call tick()/step()/propose() as needed, then flush()
//   2. storage.sync()
//   3. send take_messages()
//   4. take_restore() if present, then apply entries up to commit_index()
class RaftNode {
public:
    RaftNode(NodeId id, std::vector<NodeId> members, const Config& config,
             Storage& storage, std::uint64_t seed)
        : id_(id), config_(config), storage_(storage),
          rng_(seed ^ (0xD6E8FEB86659FD93ULL * (id + 1ULL))) {
        std::sort(members.begin(), members.end());
        members.erase(std::unique(members.begin(), members.end()), members.end());
        if (std::find(members.begin(), members.end(), id_) == members.end())
            throw std::invalid_argument("node is not in its own member list");
        if (config_.election_ticks < 2 || config_.heartbeat_ticks < 1 ||
            config_.heartbeat_ticks >= config_.election_ticks)
            throw std::invalid_argument("heartbeat must be shorter than the election timeout");
        cluster_size_ = members.size();
        for (const auto member : members)
            if (member != id_) peers_.push_back(member);
        progress_.resize(peers_.size());

        const auto hard = storage_.load_hard_state();
        term_ = hard.term;
        vote_ = hard.vote;
        snapshot_ = storage_.load_snapshot();
        log_.reset(snapshot_.index, snapshot_.term, false);
        for (auto& entry : storage_.load_entries()) log_.append(std::move(entry));
        // Only the snapshot is known to be committed after a restart. Later
        // entries become committed again when a leader says so.
        commit_ = snapshot_.index;
        if (snapshot_.index > 0) restore_pending_ = true;
        reset_election_timer();
    }

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void tick() {
        if (role_ == Role::Leader) {
            if (++heartbeat_elapsed_ >= config_.heartbeat_ticks) {
                heartbeat_elapsed_ = 0;
                for (std::size_t slot = 0; slot < peers_.size(); ++slot) send_append(slot, true);
            }
            // Check-quorum: a leader that cannot hear a majority stops acting as
            // leader, so a one-way partition cannot stall the cluster forever.
            if (++quorum_elapsed_ >= config_.election_ticks) {
                quorum_elapsed_ = 0;
                std::size_t active = 1;
                for (auto& progress : progress_) {
                    if (progress.recent_active) ++active;
                    progress.recent_active = false;
                    progress.snapshot_in_flight = false; // Allow one snapshot retry per window.
                }
                if (active < quorum()) become_follower(term_, no_node);
            }
            return;
        }
        if (++election_elapsed_ >= election_timeout_) campaign(config_.pre_vote);
    }

    void step(const Message& m) {
        if (m.to != id_ || !is_peer(m.from)) return;

        if (m.term > term_) {
            // Pre-vote traffic never changes anyone's term: a request carries the
            // term the candidate would use, and a grant echoes it back.
            const bool pre_vote_request = m.type == MsgType::RequestVote && m.pre_vote;
            const bool pre_vote_grant = m.type == MsgType::VoteReply && m.pre_vote && m.granted;
            if (!pre_vote_request && !pre_vote_grant) {
                const bool from_leader =
                    m.type == MsgType::AppendEntries || m.type == MsgType::InstallSnapshot;
                become_follower(m.term, from_leader ? m.from : no_node);
            }
        } else if (m.term < term_) {
            // Tell a stale leader or candidate about the newer term; drop stale replies.
            if (m.type == MsgType::AppendEntries || m.type == MsgType::InstallSnapshot) {
                out_.push_back(make(m.from, MsgType::AppendReply));
            } else if (m.type == MsgType::RequestVote) {
                auto reply = make(m.from, MsgType::VoteReply);
                reply.pre_vote = m.pre_vote;
                out_.push_back(std::move(reply));
            } else if (config_.bugs.accept_stale_term_reply && m.type == MsgType::AppendReply) {
                on_append_reply(m);
            }
            return;
        }

        switch (m.type) {
        case MsgType::RequestVote: on_request_vote(m); break;
        case MsgType::VoteReply: on_vote_reply(m); break;
        case MsgType::AppendEntries:
            if (role_ == Role::Leader) return; // Impossible with one leader per term.
            follow(m.from);
            on_append_entries(m);
            break;
        case MsgType::AppendReply: on_append_reply(m); break;
        case MsgType::InstallSnapshot:
            if (role_ == Role::Leader) return;
            follow(m.from);
            on_install_snapshot(m);
            break;
        case MsgType::SnapshotReply: on_snapshot_reply(m); break;
        }
    }

    // Append a command to the leader's log. Returns its index, or 0 when this
    // node is not the leader. Replication starts at the next flush().
    Index propose(std::string data) {
        if (role_ != Role::Leader) return 0;
        append_local(Entry{term_, std::move(data)});
        return log_.last_index();
    }

    // Send any entries followers have not been sent yet. Calling this once per
    // batch of proposals is what turns many proposals into few messages.
    void flush() {
        if (role_ != Role::Leader) return;
        for (std::size_t slot = 0; slot < peers_.size(); ++slot) send_append(slot, false);
        maybe_commit();
    }

    // Replace the log prefix through `through` with a state-machine snapshot.
    void compact(Index through, std::string data) {
        if (through <= log_.base_index() || through > commit_)
            throw std::logic_error("can only compact committed, uncompacted entries");
        snapshot_ = Snapshot{through, log_.term_at(through), std::move(data)};
        log_.reset(snapshot_.index, snapshot_.term, true);
        storage_.save_snapshot(snapshot_, true);
    }

    [[nodiscard]] std::vector<Message> take_messages() { return std::exchange(out_, {}); }

    // Present when the host must load a snapshot into its state machine before
    // applying further entries (after a restart or an InstallSnapshot).
    [[nodiscard]] std::optional<Snapshot> take_restore() {
        if (!restore_pending_) return std::nullopt;
        restore_pending_ = false;
        return snapshot_;
    }

    [[nodiscard]] NodeId id() const { return id_; }
    [[nodiscard]] Role role() const { return role_; }
    [[nodiscard]] Term term() const { return term_; }
    [[nodiscard]] NodeId vote() const { return vote_; }
    [[nodiscard]] NodeId leader() const { return leader_; }
    [[nodiscard]] Index commit_index() const { return commit_; }
    [[nodiscard]] const Log& log() const { return log_; }
    [[nodiscard]] std::size_t cluster_size() const { return cluster_size_; }
    [[nodiscard]] std::size_t quorum() const { return cluster_size_ / 2 + 1; }

private:
    struct Progress {
        Index next{1};  // Next index to send. Advanced optimistically when sent.
        Index match{};  // Highest index known to be stored on the follower.
        bool recent_active{};
        bool snapshot_in_flight{};
    };

    [[nodiscard]] bool is_peer(NodeId node) const {
        return std::find(peers_.begin(), peers_.end(), node) != peers_.end();
    }

    [[nodiscard]] std::size_t slot_of(NodeId node) const {
        return static_cast<std::size_t>(
            std::find(peers_.begin(), peers_.end(), node) - peers_.begin());
    }

    [[nodiscard]] Message make(NodeId to, MsgType type) const {
        Message m;
        m.type = type;
        m.from = id_;
        m.to = to;
        m.term = term_;
        return m;
    }

    void reset_election_timer() {
        election_elapsed_ = 0;
        election_timeout_ = config_.election_ticks +
            static_cast<int>(rng_.below(static_cast<std::uint64_t>(config_.election_ticks)));
    }

    void persist_hard_state() {
        // The forget_vote bug drops the vote, so a restarted node can vote twice.
        storage_.save_hard_state({term_, config_.bugs.forget_vote ? no_node : vote_});
    }

    void append_local(Entry entry) {
        log_.append(entry);
        storage_.append(log_.last_index(), {std::move(entry)});
    }

    void become_follower(Term term, NodeId leader) {
        if (term > term_) {
            term_ = term;
            vote_ = no_node;
            persist_hard_state();
        }
        role_ = Role::Follower;
        leader_ = leader;
        votes_.clear();
        reset_election_timer();
    }

    // Accept `leader` as this term's leader.
    void follow(NodeId leader) {
        role_ = Role::Follower;
        leader_ = leader;
        election_elapsed_ = 0;
    }

    void campaign(bool pre) {
        reset_election_timer();
        votes_.clear();
        votes_.insert(id_);
        leader_ = no_node;
        if (pre) {
            // Ask whether an election could succeed before disturbing the cluster
            // with a higher term. A partitioned node therefore cannot inflate
            // terms and depose a healthy leader when it reconnects.
            role_ = Role::PreCandidate;
            if (votes_.size() >= quorum()) {
                campaign(false);
                return;
            }
            request_votes(term_ + 1, true);
            return;
        }
        role_ = Role::Candidate;
        ++term_;
        vote_ = id_;
        persist_hard_state();
        if (votes_.size() >= quorum()) {
            become_leader();
            return;
        }
        request_votes(term_, false);
    }

    void request_votes(Term term, bool pre) {
        for (const auto peer : peers_) {
            auto m = make(peer, MsgType::RequestVote);
            m.term = term;
            m.pre_vote = pre;
            m.last_log_index = log_.last_index();
            m.last_log_term = log_.last_term();
            out_.push_back(std::move(m));
        }
    }

    void become_leader() {
        role_ = Role::Leader;
        leader_ = id_;
        heartbeat_elapsed_ = 0;
        quorum_elapsed_ = 0;
        votes_.clear();
        for (auto& progress : progress_) progress = Progress{log_.last_index() + 1, 0, false, false};
        // A leader may only count replicas for entries of its own term. The
        // no-op gives it such an entry immediately, which commits everything
        // before it as well.
        append_local(Entry{term_, {}});
        for (std::size_t slot = 0; slot < peers_.size(); ++slot) send_append(slot, true);
        maybe_commit();
    }

    void on_request_vote(const Message& m) {
        bool up_to_date = m.last_log_term > log_.last_term() ||
            (m.last_log_term == log_.last_term() && m.last_log_index >= log_.last_index());
        if (config_.bugs.skip_up_to_date_check) up_to_date = true;

        auto reply = make(m.from, MsgType::VoteReply);
        reply.pre_vote = m.pre_vote;
        if (m.pre_vote) {
            // Refuse while we still hear from a leader; do not change any state.
            const bool leader_alive =
                leader_ != no_node && election_elapsed_ < config_.election_ticks;
            reply.granted = m.term > term_ && up_to_date && !leader_alive;
            if (reply.granted) reply.term = m.term;
        } else {
            reply.granted = (vote_ == no_node || vote_ == m.from) && up_to_date;
            if (reply.granted) {
                vote_ = m.from;
                persist_hard_state();
                election_elapsed_ = 0;
            }
        }
        out_.push_back(std::move(reply));
    }

    void on_vote_reply(const Message& m) {
        if (!m.granted) return;
        if (m.pre_vote) {
            if (role_ != Role::PreCandidate || m.term != term_ + 1) return;
            votes_.insert(m.from);
            if (votes_.size() >= quorum()) campaign(false);
        } else {
            if (role_ != Role::Candidate) return;
            votes_.insert(m.from);
            if (votes_.size() >= quorum()) become_leader();
        }
    }

    void on_append_entries(const Message& m) {
        auto reply = make(m.from, MsgType::AppendReply);
        Index prev = m.prev_index;
        Term prev_term = m.prev_term;
        std::size_t skip = 0;

        if (prev < log_.base_index()) {
            // Indexes inside our snapshot are committed, so they already agree
            // with this leader. Start comparing at the snapshot boundary.
            const Index covered = log_.base_index() - prev;
            if (covered >= m.entries.size()) {
                reply.success = true;
                reply.match_index = prev + m.entries.size();
                advance_commit(std::min(m.leader_commit, reply.match_index));
                out_.push_back(std::move(reply));
                return;
            }
            skip = static_cast<std::size_t>(covered);
            prev = log_.base_index();
            prev_term = log_.base_term();
        }

        if (prev > log_.last_index()) {
            reply.conflict_index = log_.last_index() + 1;
            out_.push_back(std::move(reply));
            return;
        }
        if (log_.term_at(prev) != prev_term) {
            // Report the whole conflicting term so the leader can skip it in one
            // round trip instead of probing one index at a time.
            reply.conflict_term = log_.term_at(prev);
            Index first = prev;
            while (first - 1 > log_.base_index() && log_.term_at(first - 1) == reply.conflict_term)
                --first;
            reply.conflict_index = first;
            out_.push_back(std::move(reply));
            return;
        }

        Index index = prev;
        std::vector<Entry> fresh;
        Index fresh_first = 0;
        for (std::size_t k = skip; k < m.entries.size(); ++k) {
            ++index;
            const auto& entry = m.entries[k];
            if (fresh.empty() && index <= log_.last_index()) {
                if (log_.term_at(index) == entry.term) continue; // Already have it.
                if (index <= commit_)
                    throw std::logic_error("leader asked to overwrite a committed entry");
                log_.truncate_from(index);
                storage_.truncate_from(index);
            }
            if (fresh.empty()) fresh_first = index;
            log_.append(entry);
            fresh.push_back(entry);
        }
        if (!fresh.empty()) storage_.append(fresh_first, fresh);

        // Only the prefix just compared is known to match the leader. Using
        // last_index() here would acknowledge stale entries beyond it.
        reply.success = true;
        reply.match_index = config_.bugs.match_uses_last_index
            ? log_.last_index() : m.prev_index + m.entries.size();
        advance_commit(std::min(m.leader_commit, reply.match_index));
        out_.push_back(std::move(reply));
    }

    void on_append_reply(const Message& m) {
        if (role_ != Role::Leader) return;
        const auto slot = slot_of(m.from);
        auto& progress = progress_[slot];
        progress.recent_active = true;

        if (m.success) {
            if (m.match_index > progress.match) {
                progress.match = m.match_index;
                maybe_commit();
            }
            if (progress.next <= progress.match) progress.next = progress.match + 1;
            send_append(slot, false);
            return;
        }

        Index next = m.conflict_index;
        if (m.conflict_term != 0) {
            // If we also have entries from the follower's conflicting term, resume
            // just after our last one; otherwise skip that term entirely.
            Index i = log_.last_index();
            while (i > log_.base_index() && log_.term_at(i) > m.conflict_term) --i;
            if (i > log_.base_index() && log_.term_at(i) == m.conflict_term) next = i + 1;
        }
        next = std::max(next, progress.match + 1);
        next = std::min(next, log_.last_index() + 1);
        if (next < progress.next) progress.next = next;
        send_append(slot, false);
    }

    void on_install_snapshot(const Message& m) {
        auto reply = make(m.from, MsgType::SnapshotReply);
        if (m.snap_index <= commit_) {
            // Nothing new. Everything through our commit index matches the leader.
            reply.match_index = commit_;
            out_.push_back(std::move(reply));
            return;
        }
        const bool keep_suffix =
            log_.has(m.snap_index) && log_.term_at(m.snap_index) == m.snap_term;
        snapshot_ = Snapshot{m.snap_index, m.snap_term, m.snapshot};
        log_.reset(m.snap_index, m.snap_term, keep_suffix);
        storage_.save_snapshot(snapshot_, keep_suffix);
        commit_ = m.snap_index;
        restore_pending_ = true;
        reply.match_index = m.snap_index;
        out_.push_back(std::move(reply));
    }

    void on_snapshot_reply(const Message& m) {
        if (role_ != Role::Leader) return;
        const auto slot = slot_of(m.from);
        auto& progress = progress_[slot];
        progress.recent_active = true;
        progress.snapshot_in_flight = false;
        if (m.match_index > progress.match) {
            progress.match = m.match_index;
            maybe_commit();
        }
        progress.next = std::max(progress.next, progress.match + 1);
        send_append(slot, false);
    }

    void send_append(std::size_t slot, bool heartbeat) {
        auto& progress = progress_[slot];
        if (progress.next <= log_.base_index()) {
            // The entries this follower needs were compacted away.
            if (progress.snapshot_in_flight) return;
            auto m = make(peers_[slot], MsgType::InstallSnapshot);
            m.snap_index = snapshot_.index;
            m.snap_term = snapshot_.term;
            m.snapshot = snapshot_.data;
            out_.push_back(std::move(m));
            progress.snapshot_in_flight = true;
            return;
        }
        if (!heartbeat && progress.next > log_.last_index()) return;

        auto m = make(peers_[slot], MsgType::AppendEntries);
        m.prev_index = progress.next - 1;
        m.prev_term = log_.term_at(m.prev_index);
        m.leader_commit = commit_;
        std::size_t bytes = 0;
        for (Index i = progress.next;
             i <= log_.last_index() && m.entries.size() < config_.max_batch_entries; ++i) {
            const auto& entry = log_.at(i);
            if (!m.entries.empty() && bytes + entry.data.size() > config_.max_batch_bytes) break;
            bytes += entry.data.size() + 16;
            m.entries.push_back(entry);
        }
        // Optimistic: assume delivery so the next batch can go out without
        // waiting. A lost message shows up as a rejected heartbeat, which pulls
        // `next` back.
        progress.next += m.entries.size();
        out_.push_back(std::move(m));
    }

    void maybe_commit() {
        if (role_ != Role::Leader) return;
        std::vector<Index> matches;
        matches.reserve(cluster_size_);
        matches.push_back(log_.last_index());
        for (const auto& progress : progress_) matches.push_back(progress.match);
        std::sort(matches.begin(), matches.end(), std::greater<>());
        const Index candidate = matches[quorum() - 1];
        if (candidate <= commit_) return;
        // Raft's commit rule: a majority alone is not enough for an entry from
        // an earlier term (Figure 8 of the paper). Commit only through an entry
        // of the current term.
        if (!config_.bugs.commit_old_term && log_.term_at(candidate) != term_) return;
        commit_ = candidate;
    }

    void advance_commit(Index target) {
        if (target > commit_) commit_ = target;
    }

    NodeId id_;
    Config config_;
    Storage& storage_;
    Rng rng_;
    std::size_t cluster_size_{};
    std::vector<NodeId> peers_;       // Sorted, excluding this node.
    std::vector<Progress> progress_;  // Parallel to peers_; meaningful on a leader.

    Term term_{};
    NodeId vote_{no_node};
    Log log_;
    Snapshot snapshot_;
    Index commit_{};
    bool restore_pending_{};

    Role role_{Role::Follower};
    NodeId leader_{no_node};
    std::set<NodeId> votes_;
    int election_elapsed_{};
    int election_timeout_{};
    int heartbeat_elapsed_{};
    int quorum_elapsed_{};
    std::vector<Message> out_;
};

} // namespace quorum
