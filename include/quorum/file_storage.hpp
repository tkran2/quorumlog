#pragma once

#include "quorum/codec.hpp"
#include "quorum/storage.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace quorum {

enum class Durability {
    None,  // Never fsync. Survives a killed process, not a power failure.
    Fsync, // fsync before any message or reply depends on the data.
    Full,  // Like Fsync, plus F_FULLFSYNC where the platform has it (macOS).
};

// Raft's durable state in one directory:
//
//   meta      term and vote, replaced atomically (write temp, fsync, rename)
//   snapshot  state-machine snapshot, replaced atomically
//   wal       append-only log: a header, then one checksummed record per entry
//
// Appends are buffered and reach the disk in one write and one fsync per
// sync() call, so a batch of client commands shares a single fsync.
class FileStorage final : public Storage {
public:
    FileStorage(std::string directory, Durability durability)
        : dir_(std::move(directory)), durability_(durability) {
        if (::mkdir(dir_.c_str(), 0755) != 0 && errno != EEXIST) fail("mkdir " + dir_);
        ::unlink(path("meta.tmp").c_str());
        ::unlink(path("snapshot.tmp").c_str());
        ::unlink(path("wal.tmp").c_str());
        recover_meta();
        recover_snapshot();
        recover_wal();
    }

    ~FileStorage() override {
        if (fd_ >= 0) ::close(fd_);
    }

    FileStorage(const FileStorage&) = delete;
    FileStorage& operator=(const FileStorage&) = delete;

    void save_hard_state(const HardState& state) override {
        if (state == hard_) return;
        hard_ = state;
        hard_dirty_ = true;
    }

    void append(Index first_index, const std::vector<Entry>& entries) override {
        if (first_index != snapshot_.index + offsets_.size() + 1)
            throw std::logic_error("non-contiguous append");
        Index index = first_index;
        for (const auto& entry : entries) {
            offsets_.push_back(file_size_ + pending_.size());
            append_record(pending_, index++, entry);
        }
    }

    void truncate_from(Index index) override {
        if (index <= snapshot_.index) throw std::logic_error("truncate into snapshot");
        const auto keep = static_cast<std::size_t>(index - snapshot_.index - 1);
        if (keep >= offsets_.size()) return;
        const std::uint64_t offset = offsets_[keep];
        offsets_.resize(keep);
        if (offset >= file_size_) {
            pending_.resize(static_cast<std::size_t>(offset - file_size_));
            return;
        }
        pending_.clear();
        if (::ftruncate(fd_, static_cast<off_t>(offset)) != 0) fail("truncate wal");
        if (::lseek(fd_, static_cast<off_t>(offset), SEEK_SET) < 0) fail("seek wal");
        file_size_ = offset;
        wal_dirty_ = true;
    }

    void save_snapshot(const Snapshot& snapshot, bool keep_suffix) override {
        write_pending();
        const Index last = snapshot_.index + offsets_.size();
        const bool keeping =
            keep_suffix && snapshot.index >= snapshot_.index && snapshot.index <= last;
        const auto dropped = keeping ? static_cast<std::size_t>(snapshot.index - snapshot_.index) : offsets_.size();

        // 1. Publish the snapshot. Its header names the oldest log generation
        //    that may be combined with it; a log that must be discarded is
        //    thereby dead even if we crash before replacing the file.
        Writer body;
        body.u32(snapshot_magic);
        body.u64(snapshot.index);
        body.u64(snapshot.term);
        body.u64(keeping ? generation_ : generation_ + 1);
        body.bytes(snapshot.data);
        std::string bytes = body.take();
        Writer crc;
        crc.u32(crc32(bytes));
        bytes += crc.str();
        replace_file("snapshot", bytes);

        // 2. Rewrite the log without the compacted prefix.
        const std::uint64_t from = dropped < offsets_.size() ? offsets_[dropped] : file_size_;
        std::string tail(static_cast<std::size_t>(file_size_ - from), '\0');
        if (!tail.empty()) read_exact(from, tail.data(), tail.size());
        const std::uint64_t new_generation = generation_ + 1;
        std::string wal = wal_header(new_generation);
        const std::uint64_t shift = from - wal.size();
        wal += tail;
        ::close(fd_);
        fd_ = -1;
        replace_file("wal", wal);
        open_wal();
        generation_ = new_generation;
        file_size_ = wal.size();
        if (::lseek(fd_, static_cast<off_t>(file_size_), SEEK_SET) < 0) fail("seek wal");
        std::vector<std::uint64_t> offsets;
        for (std::size_t i = dropped; i < offsets_.size(); ++i) offsets.push_back(offsets_[i] - shift);
        offsets_ = std::move(offsets);
        snapshot_ = snapshot;
        wal_dirty_ = false; // replace_file already made the new log durable.
        ++snapshots_written_;
    }

    void sync() override {
        if (!pending_.empty()) write_pending();
        if (wal_dirty_) {
            flush(fd_);
            wal_dirty_ = false;
            ++fsyncs_;
        }
        if (hard_dirty_) {
            Writer out;
            out.u32(meta_magic);
            out.u64(hard_.term);
            out.u32(hard_.vote);
            std::string bytes = out.take();
            Writer crc;
            crc.u32(crc32(bytes));
            bytes += crc.str();
            replace_file("meta", bytes);
            hard_dirty_ = false;
        }
    }

    [[nodiscard]] HardState load_hard_state() const override { return hard_; }
    [[nodiscard]] Snapshot load_snapshot() const override { return snapshot_; }
    [[nodiscard]] std::vector<Entry> load_entries() const override {
        return std::move(recovered_); // Only needed once, at startup.
    }

    [[nodiscard]] std::uint64_t fsync_count() const { return fsyncs_; }
    [[nodiscard]] std::uint64_t snapshots_written() const { return snapshots_written_; }
    [[nodiscard]] std::uint64_t wal_bytes() const { return file_size_ + pending_.size(); }
    [[nodiscard]] std::uint64_t torn_bytes_discarded() const { return torn_bytes_; }

    static constexpr std::uint32_t meta_magic = 0x54454D51;     // "QMET"
    static constexpr std::uint32_t snapshot_magic = 0x504E5351; // "QSNP"
    static constexpr std::uint32_t wal_magic = 0x4C415751;      // "QWAL"
    static constexpr std::size_t wal_header_size = 16;
    static constexpr std::uint32_t max_record = 256U << 20U;

private:
    [[nodiscard]] std::string path(const char* name) const { return dir_ + "/" + name; }

    [[noreturn]] static void fail(const std::string& what) {
        throw std::runtime_error("storage: " + what + ": " + std::strerror(errno));
    }

    static std::string wal_header(std::uint64_t generation) {
        Writer out;
        out.u32(wal_magic);
        out.u64(generation);
        std::string bytes = out.take();
        Writer crc;
        crc.u32(crc32(bytes));
        return bytes + crc.str();
    }

    // Record: [u32 length][u32 crc][u64 index][u64 term][data]; length covers
    // index, term, and data, and so does the checksum.
    static void append_record(std::string& out, Index index, const Entry& entry) {
        Writer body;
        body.u64(index);
        body.u64(entry.term);
        body.raw(entry.data);
        Writer head;
        head.u32(static_cast<std::uint32_t>(body.str().size()));
        head.u32(crc32(body.str()));
        out += head.str();
        out += body.str();
    }

    void flush(int fd) const {
        if (durability_ == Durability::None) return;
#ifdef F_FULLFSYNC
        if (durability_ == Durability::Full && ::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
        if (::fsync(fd) != 0) fail("fsync");
    }

    void flush_directory() const {
        if (durability_ == Durability::None) return;
        const int fd = ::open(dir_.c_str(), O_RDONLY);
        if (fd < 0) fail("open directory");
        // Some filesystems refuse fsync on a directory; that is not fatal.
        (void)::fsync(fd);
        ::close(fd);
    }

    static void write_all(int fd, const char* data, std::size_t size) {
        while (size > 0) {
            const auto written = ::write(fd, data, size);
            if (written < 0) {
                if (errno == EINTR) continue;
                fail("write");
            }
            data += written;
            size -= static_cast<std::size_t>(written);
        }
    }

    void read_exact(std::uint64_t offset, char* data, std::size_t size) const {
        while (size > 0) {
            const auto got = ::pread(fd_, data, size, static_cast<off_t>(offset));
            if (got < 0) {
                if (errno == EINTR) continue;
                fail("read wal");
            }
            if (got == 0) throw std::runtime_error("storage: unexpected end of wal");
            data += got;
            offset += static_cast<std::uint64_t>(got);
            size -= static_cast<std::size_t>(got);
        }
    }

    // Write `name`.tmp, make it durable, then rename it over `name`. A reader
    // sees either the old file or the complete new one, never a mixture.
    void replace_file(const char* name, const std::string& bytes) const {
        const std::string temp = path(name) + ".tmp";
        const int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) fail("create " + temp);
        write_all(fd, bytes.data(), bytes.size());
        flush(fd);
        ::close(fd);
        if (::rename(temp.c_str(), path(name).c_str()) != 0) fail("rename " + temp);
        flush_directory();
    }

    void write_pending() {
        if (pending_.empty()) return;
        write_all(fd_, pending_.data(), pending_.size());
        file_size_ += pending_.size();
        pending_.clear();
        wal_dirty_ = true;
    }

    static bool read_file(const std::string& name, std::string& out) {
        std::FILE* file = std::fopen(name.c_str(), "rb");
        if (!file) return false;
        out.clear();
        char buffer[1 << 16];
        std::size_t got = 0;
        while ((got = std::fread(buffer, 1, sizeof buffer, file)) > 0) out.append(buffer, got);
        std::fclose(file);
        return true;
    }

    static bool checksum_ok(const std::string& bytes) {
        if (bytes.size() < 4) return false;
        Reader tail(std::string_view(bytes).substr(bytes.size() - 4));
        return tail.u32() == crc32(std::string_view(bytes).substr(0, bytes.size() - 4));
    }

    void recover_meta() {
        std::string bytes;
        if (!read_file(path("meta"), bytes)) return;
        // meta is only ever replaced by rename, so a damaged one is real corruption.
        if (!checksum_ok(bytes)) throw std::runtime_error("storage: meta file is corrupt");
        Reader in(bytes);
        if (in.u32() != meta_magic) throw std::runtime_error("storage: meta file has a bad magic");
        hard_.term = in.u64();
        hard_.vote = in.u32();
    }

    void recover_snapshot() {
        std::string bytes;
        if (!read_file(path("snapshot"), bytes)) return;
        if (!checksum_ok(bytes)) throw std::runtime_error("storage: snapshot file is corrupt");
        Reader in(std::string_view(bytes).substr(0, bytes.size() - 4));
        if (in.u32() != snapshot_magic) throw std::runtime_error("storage: snapshot has a bad magic");
        snapshot_.index = in.u64();
        snapshot_.term = in.u64();
        min_generation_ = in.u64();
        snapshot_.data = in.bytes();
        in.expect_done();
    }

    void open_wal() {
        fd_ = ::open(path("wal").c_str(), O_RDWR);
        if (fd_ < 0) fail("open wal");
    }

    void create_wal(std::uint64_t generation) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
        replace_file("wal", wal_header(generation));
        open_wal();
        generation_ = generation;
        file_size_ = wal_header_size;
        if (::lseek(fd_, static_cast<off_t>(file_size_), SEEK_SET) < 0) fail("seek wal");
    }

    void recover_wal() {
        std::string bytes;
        const std::uint64_t wanted = std::max<std::uint64_t>(1, min_generation_);
        if (!read_file(path("wal"), bytes) || bytes.size() < wal_header_size ||
            !checksum_ok(bytes.substr(0, wal_header_size))) {
            // A new directory, or a crash before the first log was published.
            // (Later logs are replaced by rename, so they never go missing.)
            create_wal(wanted);
            return;
        }
        Reader header(std::string_view(bytes).substr(0, wal_header_size));
        if (header.u32() != wal_magic) throw std::runtime_error("storage: wal has a bad magic");
        const std::uint64_t generation = header.u64();
        if (generation < min_generation_) {
            // The snapshot superseded this whole log (we crashed between
            // publishing the snapshot and replacing the log).
            create_wal(min_generation_);
            return;
        }
        generation_ = generation;

        std::uint64_t offset = wal_header_size;
        Index expected = snapshot_.index + 1;
        while (bytes.size() - offset >= 8) {
            Reader head(std::string_view(bytes).substr(static_cast<std::size_t>(offset), 8));
            const std::uint32_t length = head.u32();
            const std::uint32_t crc = head.u32();
            if (length < 16 || length > max_record || bytes.size() - offset - 8 < length) break;
            const std::string_view body(bytes.data() + offset + 8, length);
            if (crc32(body) != crc) break;
            Reader in(body);
            const Index index = in.u64();
            Entry entry;
            entry.term = in.u64();
            entry.data = std::string(body.substr(16));
            if (index >= expected) {
                if (index != expected)
                    throw std::runtime_error("storage: wal is missing entries before index " +
                        std::to_string(index));
                offsets_.push_back(offset);
                recovered_.push_back(std::move(entry));
                ++expected;
            } // else: already covered by the snapshot.
            offset += 8 + length;
        }
        // Anything after the last good record is a write that never finished.
        torn_bytes_ = bytes.size() - offset;
        open_wal();
        if (torn_bytes_ > 0) {
            if (::ftruncate(fd_, static_cast<off_t>(offset)) != 0) fail("truncate wal");
            flush(fd_);
        }
        file_size_ = offset;
        if (::lseek(fd_, static_cast<off_t>(file_size_), SEEK_SET) < 0) fail("seek wal");
    }

    std::string dir_;
    Durability durability_;
    int fd_{-1};
    std::uint64_t generation_{1};
    std::uint64_t min_generation_{};
    std::uint64_t file_size_{};
    std::string pending_;                  // Appended but not yet written.
    std::vector<std::uint64_t> offsets_;   // File offset of each entry after the snapshot.
    HardState hard_;
    Snapshot snapshot_;
    mutable std::vector<Entry> recovered_;
    bool hard_dirty_{};
    bool wal_dirty_{};
    std::uint64_t fsyncs_{};
    std::uint64_t snapshots_written_{};
    std::uint64_t torn_bytes_{};
};

} // namespace quorum
