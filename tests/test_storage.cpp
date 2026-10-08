// Crash-recovery tests for the on-disk log, snapshot, and metadata files.

#include "quorum/file_storage.hpp"
#include "quorum/raft.hpp"

#include <cstdlib>
#include <dirent.h>
#include <fstream>
#include <memory>
#include <iostream>
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
bool throws(F&& body) {
    try {
        body();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

std::string read_all(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    std::ostringstream out;
    out << file.rdbuf();
    return out.str();
}

void write_all(const std::string& path, const std::string& bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

void remove_tree(const std::string& dir) {
    if (DIR* handle = ::opendir(dir.c_str())) {
        while (const dirent* item = ::readdir(handle)) {
            const std::string name = item->d_name;
            if (name != "." && name != "..") ::unlink((dir + "/" + name).c_str());
        }
        ::closedir(handle);
    }
    ::rmdir(dir.c_str());
}

struct TempDir {
    TempDir() {
        std::string pattern = "/tmp/quorumlog-test-XXXXXX";
        if (!::mkdtemp(pattern.data())) throw std::runtime_error("mkdtemp failed");
        path = pattern;
    }
    ~TempDir() { remove_tree(path); }
    std::string path;
};

template <typename F>
void test(const char* name, F&& body) {
    try {
        TempDir dir;
        body(dir.path);
        ++cases;
    } catch (const std::exception& error) {
        throw std::runtime_error(std::string(name) + ": " + error.what());
    }
}

std::vector<Entry> make_entries(Index first, int count, Term term) {
    std::vector<Entry> entries;
    for (int i = 0; i < count; ++i)
        entries.push_back({term, "entry-" + std::to_string(first + static_cast<Index>(i))});
    return entries;
}

struct Loaded {
    HardState hard;
    Snapshot snapshot;
    std::vector<Entry> entries;
    bool operator==(const Loaded&) const = default;
};

Loaded load(const Storage& storage) {
    return {storage.load_hard_state(), storage.load_snapshot(), storage.load_entries()};
}

Loaded reopen(const std::string& dir) {
    FileStorage storage(dir, Durability::None);
    return load(storage);
}

void run() {
    test("a new directory is empty", [](const std::string& dir) {
        const auto state = reopen(dir);
        CHECK(state.hard == HardState{});
        CHECK(state.snapshot.index == 0);
        CHECK(state.entries.empty());
    });

    test("synced state survives a reopen", [](const std::string& dir) {
        const auto entries = make_entries(1, 50, 3);
        {
            FileStorage storage(dir, Durability::Fsync);
            storage.save_hard_state({7, 2});
            storage.append(1, entries);
            storage.sync();
            CHECK(storage.fsync_count() == 1);
        }
        const auto state = reopen(dir);
        CHECK(state.hard == (HardState{7, 2}));
        CHECK(state.entries == entries);
    });

    test("one sync covers a whole batch of appends", [](const std::string& dir) {
        FileStorage storage(dir, Durability::Fsync);
        for (Index i = 1; i <= 200; ++i) storage.append(i, make_entries(i, 1, 1));
        storage.sync();
        CHECK(storage.fsync_count() == 1);
        storage.sync(); // Nothing new: no fsync.
        CHECK(storage.fsync_count() == 1);
    });

    test("binary and empty payloads round-trip", [](const std::string& dir) {
        std::vector<Entry> entries = {
            {1, {}}, {1, std::string("\0\0\xff\x01", 4)}, {2, std::string(100000, 'q')}};
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, entries);
            storage.sync();
        }
        CHECK(reopen(dir).entries == entries);
    });

    test("truncation removes a suffix, on disk and in the write buffer", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, make_entries(1, 10, 1));
            storage.sync();
            storage.truncate_from(6);                    // On disk.
            storage.append(6, make_entries(6, 3, 2));    // 6, 7, 8 in term 2.
            storage.truncate_from(8);                    // Still buffered.
            storage.append(8, make_entries(8, 2, 3));    // 8, 9 in term 3.
            storage.sync();
            CHECK(throws([&] { storage.append(20, make_entries(20, 1, 3)); }));
        }
        const auto state = reopen(dir);
        CHECK(state.entries.size() == 9);
        CHECK(state.entries[4].term == 1 && state.entries[5].term == 2);
        CHECK(state.entries[6].term == 2 && state.entries[7].term == 3);
        CHECK(state.entries[8].data == "entry-9");
    });

    test("a torn final record is discarded at every possible cut", [](const std::string& dir) {
        const auto entries = make_entries(1, 5, 1);
        std::string full;
        std::string four;
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, std::vector<Entry>(entries.begin(), entries.begin() + 4));
            storage.sync();
            four = read_all(dir + "/wal");
            storage.append(5, {entries[4]});
            storage.sync();
            full = read_all(dir + "/wal");
        }
        CHECK(full.size() > four.size());
        for (std::size_t cut = four.size(); cut < full.size(); ++cut) {
            write_all(dir + "/wal", full.substr(0, cut));
            FileStorage storage(dir, Durability::None);
            CHECK(storage.load_entries().size() == 4);
            CHECK(storage.torn_bytes_discarded() == cut - four.size());
            // The log is usable again: the next append lands where the tear was.
            storage.append(5, {entries[4]});
            storage.sync();
            CHECK(read_all(dir + "/wal") == full);
        }
    });

    test("a corrupted final record is discarded", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, make_entries(1, 5, 1));
            storage.sync();
        }
        auto bytes = read_all(dir + "/wal");
        bytes[bytes.size() - 3] = static_cast<char>(bytes[bytes.size() - 3] ^ 0x40);
        write_all(dir + "/wal", bytes);
        CHECK(reopen(dir).entries.size() == 4);
    });

    test("garbage after the last record is discarded", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, make_entries(1, 3, 1));
            storage.sync();
        }
        write_all(dir + "/wal", read_all(dir + "/wal") + std::string(37, '\x5a'));
        CHECK(reopen(dir).entries.size() == 3);
        CHECK(reopen(dir).entries.size() == 3); // And the file was repaired.
    });

    test("compaction keeps the suffix", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::Fsync);
            storage.save_hard_state({2, 1});
            storage.append(1, make_entries(1, 20, 2));
            storage.sync();
            const auto before = storage.wal_bytes();
            storage.save_snapshot({12, 2, "state at 12"}, true);
            CHECK(storage.wal_bytes() < before);
            storage.append(21, make_entries(21, 2, 2));
            storage.truncate_from(22);
            storage.sync();
        }
        const auto state = reopen(dir);
        CHECK(state.snapshot == (Snapshot{12, 2, "state at 12"}));
        CHECK(state.entries.size() == 9); // 13..21
        CHECK(state.entries.front().data == "entry-13");
        CHECK(state.entries.back().data == "entry-21");
        CHECK(state.hard == (HardState{2, 1}));
    });

    test("an installed snapshot discards a conflicting log", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, make_entries(1, 8, 1));
            storage.sync();
            storage.save_snapshot({30, 4, "from the leader"}, false);
            storage.append(31, make_entries(31, 2, 4));
            storage.sync();
        }
        const auto state = reopen(dir);
        CHECK(state.snapshot.index == 30);
        CHECK(state.entries.size() == 2);
        CHECK(state.entries[0].data == "entry-31");
    });

    test("crash between publishing a snapshot and replacing the log", [](const std::string& dir) {
        // Case 1: compaction. The old log still holds entries before the
        // snapshot; they must be skipped, and nothing after it may be lost.
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, make_entries(1, 10, 1));
            storage.sync();
        }
        const auto old_log = read_all(dir + "/wal");
        {
            FileStorage storage(dir, Durability::None);
            (void)storage.load_entries();
            storage.save_snapshot({6, 1, "six"}, true);
        }
        write_all(dir + "/wal", old_log); // As if the log rewrite never happened.
        auto state = reopen(dir);
        CHECK(state.snapshot.index == 6);
        CHECK(state.entries.size() == 4);
        CHECK(state.entries.front().data == "entry-7");

        // Case 2: an installed snapshot that says the log must go. The stale
        // log is still on disk, and must be ignored rather than replayed.
        const auto stale_log = read_all(dir + "/wal");
        {
            FileStorage storage(dir, Durability::None);
            (void)storage.load_entries();
            storage.save_snapshot({50, 9, "fifty"}, false);
        }
        write_all(dir + "/wal", stale_log);
        state = reopen(dir);
        CHECK(state.snapshot.index == 50);
        CHECK(state.entries.empty());
        // The storage keeps working afterward.
        {
            FileStorage storage(dir, Durability::None);
            storage.append(51, make_entries(51, 1, 9));
            storage.sync();
        }
        CHECK(reopen(dir).entries.size() == 1);
    });

    test("damaged metadata or snapshot stops startup", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::None);
            storage.save_hard_state({3, 1});
            storage.append(1, make_entries(1, 5, 3));
            storage.sync();
            storage.save_snapshot({3, 3, "abc"}, true);
        }
        const auto meta = read_all(dir + "/meta");
        auto bad = meta;
        bad[5] = static_cast<char>(bad[5] ^ 1);
        write_all(dir + "/meta", bad);
        CHECK(throws([&] { (void)reopen(dir); }));
        write_all(dir + "/meta", meta);

        const auto snapshot = read_all(dir + "/snapshot");
        bad = snapshot;
        bad[bad.size() / 2] = static_cast<char>(bad[bad.size() / 2] ^ 1);
        write_all(dir + "/snapshot", bad);
        CHECK(throws([&] { (void)reopen(dir); }));
        write_all(dir + "/snapshot", snapshot);
        CHECK(reopen(dir).entries.size() == 2);
    });

    test("a log with a hole after the snapshot stops startup", [](const std::string& dir) {
        {
            FileStorage storage(dir, Durability::None);
            storage.append(1, make_entries(1, 10, 1));
            storage.sync();
            storage.save_snapshot({8, 1, "eight"}, true); // Log now starts at 9.
        }
        const auto log = read_all(dir + "/wal");
        ::unlink((dir + "/snapshot").c_str()); // Entries 1..8 are now simply gone.
        write_all(dir + "/wal", log);
        CHECK(throws([&] { (void)reopen(dir); }));
    });

    test("random operations match the in-memory model across restarts", [](const std::string& dir) {
        Rng rng(99);
        MemStorage model;
        auto disk = std::make_unique<FileStorage>(dir, Durability::None);
        (void)disk->load_entries();
        Index base = 0;
        Index last = 0;
        for (int step = 0; step < 3000; ++step) {
            const auto roll = rng.below(100);
            if (roll < 55) {
                const int count = 1 + static_cast<int>(rng.below(4));
                auto entries = make_entries(last + 1, count, 1 + rng.below(5));
                for (auto& e : entries) e.data += std::string(rng.below(40), 'x');
                model.append(last + 1, entries);
                disk->append(last + 1, entries);
                last += static_cast<Index>(count);
            } else if (roll < 68 && last > base) {
                const Index from = base + 1 + rng.below(last - base);
                model.truncate_from(from);
                disk->truncate_from(from);
                last = from - 1;
            } else if (roll < 76) {
                const HardState hard{rng.below(50), static_cast<NodeId>(rng.below(4))};
                model.save_hard_state(hard);
                disk->save_hard_state(hard);
            } else if (roll < 82 && last > base) {
                const Index at = base + 1 + rng.below(last - base);
                const Snapshot snap{at, 1, "snap-" + std::to_string(at)};
                model.save_snapshot(snap, true);
                disk->save_snapshot(snap, true);
                base = at;
            } else if (roll < 84) {
                const Index at = last + 1 + rng.below(20);
                const Snapshot snap{at, 6, "install-" + std::to_string(at)};
                model.save_snapshot(snap, false);
                disk->save_snapshot(snap, false);
                base = last = at;
            } else if (roll < 94) {
                model.sync();
                disk->sync();
            } else {
                model.sync();
                disk->sync();
                disk.reset();
                disk = std::make_unique<FileStorage>(dir, Durability::None);
                CHECK(load(*disk) == load(model));
            }
        }
        model.sync();
        disk->sync();
        disk.reset();
        CHECK(reopen(dir) == load(model));
    });

    test("a Raft node recovers its log and vote from disk", [](const std::string& dir) {
        Index last = 0;
        {
            FileStorage storage(dir, Durability::Fsync);
            RaftNode node(1, {1}, Config{}, storage, 1);
            for (int i = 0; i < 40; ++i) node.tick();
            CHECK(node.role() == Role::Leader);
            for (int i = 0; i < 25; ++i) node.propose("cmd" + std::to_string(i));
            node.flush();
            storage.sync();
            last = node.log().last_index();
            CHECK(node.commit_index() == last);
        }
        FileStorage storage(dir, Durability::Fsync);
        RaftNode node(1, {1}, Config{}, storage, 2);
        CHECK(node.term() == 1);
        CHECK(node.vote() == 1);
        CHECK(node.log().last_index() == last);
        CHECK(node.log().at(last).data == "cmd24");
    });
}

} // namespace

int main() {
    try {
        run();
    } catch (const std::exception& error) {
        std::cerr << "FAILED " << error.what() << '\n';
        return 1;
    }
    std::cout << "storage tests passed: " << cases << " cases, " << assertions << " assertions\n";
    return 0;
}
