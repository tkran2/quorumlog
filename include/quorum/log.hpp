#pragma once

#include "quorum/types.hpp"

#include <stdexcept>
#include <utility>
#include <vector>

namespace quorum {

// In-memory log with a compacted prefix. `base_index` is the last index covered
// by the snapshot; entries_[0] holds index base_index + 1.
class Log {
public:
    [[nodiscard]] Index base_index() const { return base_index_; }
    [[nodiscard]] Term base_term() const { return base_term_; }
    [[nodiscard]] Index last_index() const { return base_index_ + entries_.size(); }
    [[nodiscard]] Term last_term() const {
        return entries_.empty() ? base_term_ : entries_.back().term;
    }
    [[nodiscard]] bool has(Index index) const {
        return index > base_index_ && index <= last_index();
    }

    // Valid for base_index (the snapshot's term) and every stored entry.
    [[nodiscard]] Term term_at(Index index) const {
        if (index == base_index_) return base_term_;
        return at(index).term;
    }

    [[nodiscard]] const Entry& at(Index index) const {
        if (!has(index)) throw std::out_of_range("log index outside stored range");
        return entries_[static_cast<std::size_t>(index - base_index_ - 1)];
    }

    void append(Entry entry) { entries_.push_back(std::move(entry)); }

    // Remove `index` and everything after it.
    void truncate_from(Index index) {
        if (index <= base_index_) throw std::logic_error("cannot truncate into a snapshot");
        if (index > last_index()) return;
        entries_.resize(static_cast<std::size_t>(index - base_index_ - 1));
    }

    // Move the base forward. With keep_suffix, entries after `index` survive
    // (local compaction, or an installed snapshot that agrees with our log).
    void reset(Index index, Term term, bool keep_suffix) {
        if (keep_suffix && has(index)) {
            entries_.erase(entries_.begin(),
                entries_.begin() + static_cast<std::ptrdiff_t>(index - base_index_));
        } else {
            entries_.clear();
        }
        base_index_ = index;
        base_term_ = term;
    }

    [[nodiscard]] const std::vector<Entry>& entries() const { return entries_; }

private:
    Index base_index_{};
    Term base_term_{};
    std::vector<Entry> entries_;
};

} // namespace quorum
