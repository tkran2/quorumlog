#pragma once

#include "quorum/codec.hpp"
#include "quorum/replica.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <poll.h>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

// Wire protocol shared by the server, the command-line client, and the load
// generator. Everything is a length-prefixed, checksummed frame over TCP:
//
//   [u32 payload length][u32 crc32(payload)][payload]
//
// and every payload starts with a one-byte frame type.
namespace quorum::net {

struct Address {
    std::string host;
    std::uint16_t port{};
};

inline Address parse_address(const std::string& text) {
    const auto colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == text.size())
        throw std::invalid_argument("address must be host:port, got '" + text + "'");
    const int port = std::stoi(text.substr(colon + 1));
    if (port < 1 || port > 65535) throw std::invalid_argument("port out of range in '" + text + "'");
    return {text.substr(0, colon), static_cast<std::uint16_t>(port)};
}

inline constexpr std::uint32_t max_frame = 128U << 20U;

enum class FrameType : std::uint8_t {
    Hello = 0,       // First frame on a peer link: who is calling.
    Request = 1,     // Client command.
    Reply = 2,
    Peer = 3,        // One Raft message.
    Status = 4,      // Ask a node to describe itself.
    StatusReply = 5,
};

inline void append_frame(std::string& out, std::string_view payload) {
    Writer head;
    head.u32(static_cast<std::uint32_t>(payload.size()));
    head.u32(crc32(payload));
    out += head.str();
    out.append(payload);
}

// Reassembles frames from a byte stream that arrives in arbitrary pieces.
class FrameReader {
public:
    void feed(const char* data, std::size_t size) { buffer_.append(data, size); }

    // Extracts the next complete frame. Throws DecodeError on a corrupt stream.
    bool next(std::string& payload) {
        if (buffer_.size() - offset_ < 8) return compact();
        Reader head(std::string_view(buffer_).substr(offset_, 8));
        const auto length = head.u32();
        const auto crc = head.u32();
        if (length == 0 || length > max_frame) throw DecodeError("bad frame length");
        if (buffer_.size() - offset_ - 8 < length) return compact();
        payload.assign(buffer_, offset_ + 8, length);
        if (crc32(payload) != crc) throw DecodeError("frame checksum mismatch");
        offset_ += 8 + length;
        return true;
    }

private:
    bool compact() {
        if (offset_ > 0 && offset_ >= buffer_.size() / 2) {
            buffer_.erase(0, offset_);
            offset_ = 0;
        }
        return false;
    }

    std::string buffer_;
    std::size_t offset_{};
};

inline std::string encode_request(const ClientRequest& request) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(FrameType::Request));
    out.u64(request.client);
    out.u64(request.seq);
    out.bytes(request.payload);
    return out.take();
}

inline ClientRequest decode_request(std::string_view payload) {
    Reader in(payload);
    if (in.u8() != static_cast<std::uint8_t>(FrameType::Request)) throw DecodeError("not a request");
    ClientRequest request;
    request.client = in.u64();
    request.seq = in.u64();
    request.payload = in.bytes();
    in.expect_done();
    return request;
}

inline std::string encode_reply(const ClientReply& reply) {
    Writer out;
    out.u8(static_cast<std::uint8_t>(FrameType::Reply));
    out.u64(reply.client);
    out.u64(reply.seq);
    out.u8(static_cast<std::uint8_t>(reply.status));
    out.u32(reply.leader);
    out.bytes(reply.result);
    return out.take();
}

inline ClientReply decode_reply(std::string_view payload) {
    Reader in(payload);
    if (in.u8() != static_cast<std::uint8_t>(FrameType::Reply)) throw DecodeError("not a reply");
    ClientReply reply;
    reply.client = in.u64();
    reply.seq = in.u64();
    const auto status = in.u8();
    if (status > 2) throw DecodeError("unknown reply status");
    reply.status = static_cast<ReplyStatus>(status);
    reply.leader = in.u32();
    reply.result = in.bytes();
    in.expect_done();
    return reply;
}

inline void set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::runtime_error(std::string("fcntl: ") + std::strerror(errno));
}

inline void set_nodelay(int fd) {
    const int one = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

inline sockaddr_in resolve(const Address& address) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* found = nullptr;
    const auto port = std::to_string(address.port);
    if (::getaddrinfo(address.host.c_str(), port.c_str(), &hints, &found) != 0 || !found)
        throw std::runtime_error("cannot resolve " + address.host);
    sockaddr_in result{};
    std::memcpy(&result, found->ai_addr, sizeof result);
    ::freeaddrinfo(found);
    return result;
}

inline int listen_on(const Address& address) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
    const int one = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    const auto where = resolve(address);
    if (::bind(fd, reinterpret_cast<const sockaddr*>(&where), sizeof where) != 0 ||
        ::listen(fd, 256) != 0) {
        const std::string reason = std::strerror(errno);
        ::close(fd);
        throw std::runtime_error("cannot listen on " + address.host + ":" +
            std::to_string(address.port) + ": " + reason);
    }
    set_nonblocking(fd);
    return fd;
}

// Starts a non-blocking connect. Returns the socket, or -1 if it failed at once.
// Completion is signaled by the socket becoming writable; check SO_ERROR then.
inline int start_connect(const Address& address) {
    sockaddr_in where{};
    try {
        where = resolve(address);
    } catch (const std::exception&) {
        return -1;
    }
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_nonblocking(fd);
    set_nodelay(fd);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&where), sizeof where) != 0 &&
        errno != EINPROGRESS) {
        ::close(fd);
        return -1;
    }
    return fd;
}

inline bool connect_finished(int fd) {
    int error = 0;
    socklen_t size = sizeof error;
    return ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) == 0 && error == 0;
}

inline std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

inline std::uint64_t random_client_id() {
    std::random_device device;
    return (static_cast<std::uint64_t>(device()) << 32U) ^ device() ^
        static_cast<std::uint64_t>(now_ns());
}

// A simple blocking client: one request at a time, retried with the same
// sequence number until some node answers or the deadline passes.
class Client {
public:
    Client(std::vector<Address> nodes, std::uint64_t id)
        : nodes_(std::move(nodes)), id_(id == 0 ? random_client_id() : id) {
        if (nodes_.empty()) throw std::invalid_argument("client needs at least one node");
    }

    ~Client() { disconnect(); }
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // Returns the state machine's answer, or nothing if no node answered in
    // time. "Nothing" means UNKNOWN: the command may still have been applied.
    std::optional<std::string> call(const std::string& payload,
                                    std::chrono::milliseconds deadline = std::chrono::seconds(10)) {
        const ClientRequest request{id_, ++seq_, payload};
        std::string frame;
        append_frame(frame, encode_request(request));
        const auto give_up = now_ns() + deadline.count() * 1'000'000LL;
        while (now_ns() < give_up) {
            if (fd_ < 0 && !connect()) {
                advance_target();
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
            if (!send_all(frame)) {
                disconnect();
                advance_target();
                continue;
            }
            // Wait for the answer to THIS request; skip late answers to older ones.
            ClientReply reply;
            bool answered = false;
            const auto attempt_end = now_ns() + static_cast<std::int64_t>(attempt_ms_) * 1'000'000LL;
            try {
                std::string reply_bytes;
                while (!answered) {
                    const auto left = (attempt_end - now_ns()) / 1'000'000LL;
                    if (left <= 0 || !receive(reply_bytes, static_cast<int>(left))) break;
                    reply = decode_reply(reply_bytes);
                    answered = reply.client == id_ && reply.seq == request.seq;
                }
            } catch (const DecodeError&) {
                answered = false;
            }
            if (!answered) {
                disconnect();
                advance_target();
                continue;
            }
            if (reply.status == ReplyStatus::Ok) return reply.result;
            if (reply.status == ReplyStatus::Stale)
                throw std::runtime_error("server rejected the request as stale");
            ++redirects_;
            if (reply.leader == no_node || reply.leader > nodes_.size()) {
                // No leader known yet: an election is in progress.
                std::this_thread::sleep_for(std::chrono::milliseconds(30));
                disconnect();
                advance_target();
            } else if (reply.leader - 1 != target_) {
                disconnect();
                target_ = reply.leader - 1;
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }
        return std::nullopt;
    }

    // One node's own description of itself (role, term, indexes, state digest).
    static std::optional<std::string> status(const Address& node, int timeout_ms = 500) {
        Client probe({node}, 1);
        probe.attempt_ms_ = timeout_ms;
        if (!probe.connect()) return std::nullopt;
        std::string frame;
        append_frame(frame, std::string(1, static_cast<char>(FrameType::Status)));
        if (!probe.send_all(frame)) return std::nullopt;
        std::string reply;
        if (!probe.receive(reply, timeout_ms) || reply.empty() ||
            reply[0] != static_cast<char>(FrameType::StatusReply))
            return std::nullopt;
        return reply.substr(1);
    }

    [[nodiscard]] std::uint64_t id() const { return id_; }
    [[nodiscard]] std::uint64_t redirects() const { return redirects_; }

private:
    void advance_target() { target_ = (target_ + 1) % nodes_.size(); }

    void disconnect() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
        reader_ = FrameReader{};
    }

    bool connect() {
        disconnect();
        const int fd = start_connect(nodes_[target_]);
        if (fd < 0) return false;
        pollfd waiter{fd, POLLOUT, 0};
        if (::poll(&waiter, 1, attempt_ms_) <= 0 || !connect_finished(fd)) {
            ::close(fd);
            return false;
        }
        fd_ = fd;
        return true;
    }

    bool send_all(const std::string& bytes) {
        std::size_t sent = 0;
        while (sent < bytes.size()) {
            const auto n = ::send(fd_, bytes.data() + sent, bytes.size() - sent, 0);
            if (n > 0) {
                sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                pollfd waiter{fd_, POLLOUT, 0};
                if (::poll(&waiter, 1, attempt_ms_) <= 0) return false;
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        return true;
    }

    bool receive(std::string& payload, int timeout_ms) {
        const auto give_up = now_ns() + static_cast<std::int64_t>(timeout_ms) * 1'000'000LL;
        try {
            while (!reader_.next(payload)) {
                const auto left = (give_up - now_ns()) / 1'000'000LL;
                if (left <= 0) return false;
                pollfd waiter{fd_, POLLIN, 0};
                if (::poll(&waiter, 1, static_cast<int>(left)) <= 0) return false;
                char buffer[1 << 16];
                const auto n = ::recv(fd_, buffer, sizeof buffer, 0);
                if (n == 0) return false;
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
                    return false;
                }
                reader_.feed(buffer, static_cast<std::size_t>(n));
            }
        } catch (const DecodeError&) {
            return false;
        }
        return true;
    }

    std::vector<Address> nodes_;
    std::uint64_t id_;
    std::uint64_t seq_{};
    std::size_t target_{};
    int fd_{-1};
    int attempt_ms_{400};
    FrameReader reader_;
    std::uint64_t redirects_{};
};

} // namespace quorum::net
