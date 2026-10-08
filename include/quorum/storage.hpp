#pragma once

#include "quorum/types.hpp"

#include <stdexcept>
#include <vector>

namespace quorum {

// Durable state required by Raft. Mutations may be buffered; they must survive
// a crash once sync() returns. The host calls sync() before it releases any
// message or client reply that depends on them.
class Storage {
public:
    virtual ~Storage() = default;

    virtual void save_hard_state(const HardState& state) = 0;
    // Append entries so that the first lands at `first_index` (always last + 1).
    virtual void append(Index first_index, const std::vector<Entry>& entries) = 0;
    // Remove `index` and everything after it.
    virtual void truncate_from(Index index) = 0;
    // Replace the snapshot. keep_suffix retains log entries after snapshot.index.
    virtual void save_snapshot(const Snapshot& snapshot, bool keep_suffix) = 0;
    virtual void sync() = 0;

    [[nodiscard]] virtual HardState load_hard_state() const = 0;
    [[nodiscard]] virtual Snapshot load_snapshot() const = 0;
    // Entries after the snapshot, in index order.
    [[nodiscard]] virtual std::vector<Entry> load_entries() const = 0;
};

// Storage model for tests and the simulator. Writes land in a working copy;
// sync() makes them durable; crash() discards whatever was not synced, exactly
// like a process that dies with data still in an OS buffer after power loss.
class MemStorage final : public Storage {
public:
    void save_hard_state(const HardState& state) override {
        working_.hard = state;
        dirty_ = true;
    }

    void append(Index first_index, const std::vector<Entry>& entries) override {
        if (first_index != working_.snapshot.index + working_.entries.size() + 1)
            throw std::logic_error("non-contiguous append");
        working_.entries.insert(working_.entries.end(), entries.begin(), entries.end());
        dirty_ = true;
    }

    void truncate_from(Index index) override {
        if (index <= working_.snapshot.index) throw std::logic_error("truncate into snapshot");
        const auto keep = static_cast<std::size_t>(index - working_.snapshot.index - 1);
        if (keep < working_.entries.size()) working_.entries.resize(keep);
        dirty_ = true;
    }

    void save_snapshot(const Snapshot& snapshot, bool keep_suffix) override {
        const auto last = working_.snapshot.index + working_.entries.size();
        if (keep_suffix && snapshot.index > working_.snapshot.index && snapshot.index <= last) {
            working_.entries.erase(working_.entries.begin(),
                working_.entries.begin() +
                    static_cast<std::ptrdiff_t>(snapshot.index - working_.snapshot.index));
        } else {
            working_.entries.clear();
        }
        working_.snapshot = snapshot;
        dirty_ = true;
    }

    void sync() override {
        if (!dirty_) return;
        durable_ = working_;
        dirty_ = false;
        ++sync_count_;
    }

    [[nodiscard]] HardState load_hard_state() const override { return working_.hard; }
    [[nodiscard]] Snapshot load_snapshot() const override { return working_.snapshot; }
    [[nodiscard]] std::vector<Entry> load_entries() const override { return working_.entries; }

    // Simulated process death: unsynced writes are gone.
    void crash() {
        working_ = durable_;
        dirty_ = false;
    }

    // True when the synced state holds this exact entry (or a snapshot that
    // already covers it). The simulator uses this to audit commit decisions.
    [[nodiscard]] bool durably_has(Index index, Term term) const {
        if (index <= durable_.snapshot.index) return true;
        const auto offset = static_cast<std::size_t>(index - durable_.snapshot.index - 1);
        return offset < durable_.entries.size() && durable_.entries[offset].term == term;
    }

    [[nodiscard]] bool dirty() const { return dirty_; }
    [[nodiscard]] std::uint64_t sync_count() const { return sync_count_; }

private:
    struct State {
        HardState hard;
        Snapshot snapshot;
        std::vector<Entry> entries;
    };

    State working_;
    State durable_;
    bool dirty_{};
    std::uint64_t sync_count_{};
};

} // namespace quorum
