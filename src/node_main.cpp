// quorum-node: one replica of the cluster. A single thread runs a poll() loop
// that reads peer and client frames, advances the Raft core, makes the batch
// durable with one fsync, and only then sends messages and client replies.

#include "quorum/exchange.hpp"
#include "quorum/file_storage.hpp"
#include "quorum/kv.hpp"
#include "quorum/net.hpp"
#include "quorum/replica.hpp"

#include <csignal>
#include <cstdio>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <unordered_map>

namespace {

using namespace quorum;

volatile std::sig_atomic_t stop_requested = 0;
void on_signal(int) { stop_requested = 1; }

struct Options {
    NodeId id{};
    std::map<NodeId, net::Address> members;
    std::string data;
    std::string machine{"kv"};
    Durability durability{Durability::Fsync};
    int tick_ms{10};
    Config raft;
    ReplicaOptions replica;
    bool verbose{};
};

void usage() {
    std::cout <<
        "Usage: quorum-node --id N --cluster 1=host:port,2=host:port,... --data DIR [options]\n"
        "  --machine kv|exchange     replicated state machine (default kv)\n"
        "  --durability none|fsync|full   (default fsync; full adds F_FULLFSYNC on macOS)\n"
        "  --tick-ms N               length of one logical tick (default 10)\n"
        "  --election-ticks N        base election timeout in ticks (default 20)\n"
        "  --heartbeat-ticks N       leader heartbeat interval in ticks (default 5)\n"
        "  --snapshot-every N        applied entries before a snapshot (default 20000)\n"
        "  --snapshot-keep N         entries kept after a snapshot (default 2000)\n"
        "  --no-pre-vote\n"
        "  --verbose                 log role changes to stderr\n";
}

Options parse(int argc, char** argv) {
    Options o;
    o.raft.election_ticks = 20;
    o.raft.heartbeat_ticks = 5;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto value = [&]() -> std::string {
            if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
            return argv[++i];
        };
        if (arg == "--id") o.id = static_cast<NodeId>(std::stoul(value()));
        else if (arg == "--cluster") {
            std::stringstream list(value());
            for (std::string item; std::getline(list, item, ',');) {
                const auto eq = item.find('=');
                if (eq == std::string::npos) throw std::invalid_argument("--cluster items are ID=host:port");
                o.members[static_cast<NodeId>(std::stoul(item.substr(0, eq)))] =
                    net::parse_address(item.substr(eq + 1));
            }
        } else if (arg == "--data") o.data = value();
        else if (arg == "--machine") o.machine = value();
        else if (arg == "--durability") {
            const auto v = value();
            if (v == "none") o.durability = Durability::None;
            else if (v == "fsync") o.durability = Durability::Fsync;
            else if (v == "full") o.durability = Durability::Full;
            else throw std::invalid_argument("--durability must be none, fsync, or full");
        } else if (arg == "--tick-ms") o.tick_ms = std::stoi(value());
        else if (arg == "--election-ticks") o.raft.election_ticks = std::stoi(value());
        else if (arg == "--heartbeat-ticks") o.raft.heartbeat_ticks = std::stoi(value());
        else if (arg == "--snapshot-every") o.replica.snapshot_threshold = std::stoull(value());
        else if (arg == "--snapshot-keep") o.replica.snapshot_keep = std::stoull(value());
        else if (arg == "--no-pre-vote") o.raft.pre_vote = false;
        else if (arg == "--verbose") o.verbose = true;
        else if (arg == "--help") {
            usage();
            std::exit(0);
        } else throw std::invalid_argument("unknown option: " + arg);
    }
    if (o.id == 0 || !o.members.count(o.id)) throw std::invalid_argument("--id must name a member of --cluster");
    if (o.data.empty()) throw std::invalid_argument("--data is required");
    if (o.machine != "kv" && o.machine != "exchange") throw std::invalid_argument("--machine must be kv or exchange");
    if (o.tick_ms < 1 || o.tick_ms > 1000) throw std::invalid_argument("--tick-ms must be 1 to 1000");
    NodeId expected = 1;
    for (const auto& [id, address] : o.members) {
        (void)address;
        if (id != expected++) throw std::invalid_argument("member IDs must be 1..N with no gaps");
    }
    return o;
}

class Server {
public:
    explicit Server(const Options& options)
        : options_(options), storage_(options.data, options.durability),
          machine_(options.machine == "kv"
              ? std::unique_ptr<StateMachine>(std::make_unique<kv::Store>())
              : std::unique_ptr<StateMachine>(std::make_unique<exchange::Book>())),
          replica_(options.id, member_ids(options), options.raft, storage_, *machine_,
                   static_cast<std::uint64_t>(net::now_ns()) ^ (options.id * 0x9E3779B97F4A7C15ULL),
                   options.replica) {
        listen_fd_ = net::listen_on(options_.members.at(options_.id));
        for (const auto& [id, address] : options_.members)
            if (id != options_.id) links_[id] = Link{address, -1, false, {}, 0, 0};
    }

    ~Server() {
        for (auto& [id, conn] : conns_) ::close(conn.fd);
        for (auto& [id, link] : links_)
            if (link.fd >= 0) ::close(link.fd);
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

    void run() {
        const std::int64_t tick_ns = static_cast<std::int64_t>(options_.tick_ms) * 1'000'000LL;
        std::int64_t next_tick = net::now_ns() + tick_ns;
        std::vector<pollfd> fds;
        std::vector<std::uint64_t> owners; // Parallel to fds: connection ID, or 0.
        std::vector<NodeId> link_of;       // Parallel to fds: peer ID for outbound links.

        while (!stop_requested) {
            fds.clear();
            owners.clear();
            link_of.clear();
            fds.push_back({listen_fd_, POLLIN, 0});
            owners.push_back(0);
            link_of.push_back(no_node);
            for (auto& [id, conn] : conns_) {
                short events = POLLIN;
                if (conn.out.size() > conn.sent) events |= POLLOUT;
                fds.push_back({conn.fd, events, 0});
                owners.push_back(id);
                link_of.push_back(no_node);
            }
            for (auto& [id, link] : links_) {
                if (link.fd < 0) continue;
                short events = POLLIN; // Readable only to notice a closed link.
                if (link.connecting || link.out.size() > link.sent) events |= POLLOUT;
                fds.push_back({link.fd, events, 0});
                owners.push_back(0);
                link_of.push_back(id);
            }

            const auto wait_ns = next_tick - net::now_ns();
            const int timeout = wait_ns <= 0 ? 0 : static_cast<int>((wait_ns + 999'999) / 1'000'000);
            const int ready = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), timeout);
            if (ready < 0 && errno != EINTR) throw std::runtime_error(std::string("poll: ") + std::strerror(errno));

            if (ready > 0) {
                if (fds[0].revents & POLLIN) accept_all();
                for (std::size_t i = 1; i < fds.size(); ++i) {
                    if (fds[i].revents == 0) continue;
                    if (link_of[i] != no_node) service_link(link_of[i], fds[i].revents);
                    else service_connection(owners[i], fds[i].revents);
                }
            }

            const auto now = net::now_ns();
            int ticks = 0;
            while (now >= next_tick && ticks < 4) {
                replica_.tick();
                next_tick += tick_ns;
                ++ticks;
            }
            if (now >= next_tick) next_tick = now + tick_ns; // Fell far behind; do not burst.
            dial_peers(now);

            // The order below is the whole durability argument: nothing leaves
            // this process until the state it depends on is on disk.
            replica_.flush();
            storage_.sync();
            for (auto& message : replica_.take_messages()) send_peer(message);
            replica_.advance();
            for (auto& reply : replica_.take_replies()) send_reply(reply);
            report_role();
        }
        storage_.sync();
    }

private:
    struct Connection {
        int fd{-1};
        net::FrameReader reader;
        std::string out;
        std::size_t sent{};
    };

    struct Link {
        net::Address address;
        int fd{-1};
        bool connecting{};
        std::string out;
        std::size_t sent{};
        std::int64_t retry_at{};
    };

    static constexpr std::size_t max_backlog = 64U << 20U;

    static std::vector<NodeId> member_ids(const Options& options) {
        std::vector<NodeId> ids;
        for (const auto& [id, address] : options.members) {
            (void)address;
            ids.push_back(id);
        }
        return ids;
    }

    void accept_all() {
        while (true) {
            const int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) return;
            net::set_nonblocking(fd);
            net::set_nodelay(fd);
            Connection conn;
            conn.fd = fd;
            conns_.emplace(++next_conn_, std::move(conn));
        }
    }

    void close_connection(std::uint64_t id) {
        const auto it = conns_.find(id);
        if (it == conns_.end()) return;
        ::close(it->second.fd);
        conns_.erase(it);
    }

    void service_connection(std::uint64_t id, short events) {
        const auto it = conns_.find(id);
        if (it == conns_.end()) return;
        auto& conn = it->second;
        if (events & POLLIN) {
            char buffer[1 << 16];
            while (true) {
                const auto n = ::recv(conn.fd, buffer, sizeof buffer, 0);
                if (n > 0) {
                    conn.reader.feed(buffer, static_cast<std::size_t>(n));
                    if (static_cast<std::size_t>(n) < sizeof buffer) break;
                    continue;
                }
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) break;
                close_connection(id); // Orderly close or a hard error.
                return;
            }
            try {
                std::string payload;
                while (conn.reader.next(payload)) handle_frame(id, payload);
            } catch (const DecodeError&) {
                close_connection(id); // A corrupt stream cannot be resynchronized.
                return;
            }
        }
        if (events & (POLLERR | POLLNVAL)) {
            close_connection(id);
            return;
        }
        if (events & POLLOUT) {
            if (!drain(conn.fd, conn.out, conn.sent)) close_connection(id);
        } else if ((events & POLLHUP) && !(events & POLLIN)) {
            close_connection(id);
        }
    }

    void handle_frame(std::uint64_t conn_id, const std::string& payload) {
        switch (static_cast<net::FrameType>(static_cast<std::uint8_t>(payload[0]))) {
        case net::FrameType::Peer:
            replica_.step(decode_message(std::string_view(payload).substr(1)));
            break;
        case net::FrameType::Request: {
            const auto request = net::decode_request(payload);
            client_conn_[request.client] = conn_id;
            replica_.submit(request);
            break;
        }
        case net::FrameType::Status: {
            const auto it = conns_.find(conn_id);
            if (it == conns_.end()) break;
            std::string frame;
            net::append_frame(frame, std::string(1, static_cast<char>(net::FrameType::StatusReply)) + status());
            it->second.out += frame;
            (void)drain(it->second.fd, it->second.out, it->second.sent);
            break;
        }
        default:
            throw DecodeError("unexpected frame type");
        }
    }

    [[nodiscard]] std::string status() const {
        const auto& raft = replica_.raft();
        char digest[32];
        std::snprintf(digest, sizeof digest, "%016llx", static_cast<unsigned long long>(replica_.digest()));
        std::ostringstream out;
        out << "id=" << raft.id() << " role=" << role_name(raft.role()) << " term=" << raft.term()
            << " leader=" << raft.leader() << " commit=" << raft.commit_index()
            << " applied=" << replica_.applied() << " last=" << raft.log().last_index()
            << " base=" << raft.log().base_index() << " digest=" << digest
            << " fsyncs=" << storage_.fsync_count() << " snapshots=" << storage_.snapshots_written()
            << " wal_bytes=" << storage_.wal_bytes() << " machine=" << options_.machine;
        return out.str();
    }

    // Write as much as the socket accepts. False means the peer is gone.
    static bool drain(int fd, std::string& out, std::size_t& sent) {
        while (sent < out.size()) {
            const auto n = ::send(fd, out.data() + sent, out.size() - sent, 0);
            if (n > 0) {
                sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        out.clear();
        sent = 0;
        return true;
    }

    void drop_link(Link& link, std::int64_t now) {
        if (link.fd >= 0) ::close(link.fd);
        link.fd = -1;
        link.connecting = false;
        link.out.clear(); // Raft resends whatever mattered.
        link.sent = 0;
        link.retry_at = now + 50'000'000LL;
    }

    void dial_peers(std::int64_t now) {
        for (auto& [id, link] : links_) {
            if (link.fd >= 0 || now < link.retry_at) continue;
            link.fd = net::start_connect(link.address);
            link.connecting = link.fd >= 0;
            if (link.fd < 0) link.retry_at = now + 50'000'000LL;
        }
    }

    void service_link(NodeId id, short events) {
        auto& link = links_.at(id);
        const auto now = net::now_ns();
        if (events & (POLLERR | POLLNVAL)) return drop_link(link, now);
        if (link.connecting) {
            if (!(events & POLLOUT)) {
                if (events & POLLHUP) drop_link(link, now);
                return;
            }
            if (!net::connect_finished(link.fd)) return drop_link(link, now);
            link.connecting = false;
        }
        if (events & POLLIN) {
            // The peer never sends on this link; readable means it closed.
            char byte;
            const auto n = ::recv(link.fd, &byte, 1, 0);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
                return drop_link(link, now);
        }
        if ((events & POLLOUT) && !drain(link.fd, link.out, link.sent)) drop_link(link, now);
    }

    void send_peer(const Message& message) {
        const auto it = links_.find(message.to);
        if (it == links_.end()) return;
        auto& link = it->second;
        // Not connected, or the peer is not keeping up: drop. Raft tolerates
        // loss, and a bounded queue keeps a dead peer from eating memory.
        if (link.fd < 0 || link.connecting || link.out.size() - link.sent > max_backlog) return;
        net::append_frame(link.out,
            std::string(1, static_cast<char>(net::FrameType::Peer)) + encode_message(message));
        if (!drain(link.fd, link.out, link.sent)) drop_link(link, net::now_ns());
    }

    void send_reply(const ClientReply& reply) {
        const auto route = client_conn_.find(reply.client);
        if (route == client_conn_.end()) return;
        const auto it = conns_.find(route->second);
        if (it == conns_.end()) {
            client_conn_.erase(route);
            return;
        }
        net::append_frame(it->second.out, net::encode_reply(reply));
        if (!drain(it->second.fd, it->second.out, it->second.sent)) close_connection(route->second);
    }

    void report_role() {
        const auto& raft = replica_.raft();
        if (raft.role() == last_role_ && raft.term() == last_term_) return;
        last_role_ = raft.role();
        last_term_ = raft.term();
        if (options_.verbose)
            std::fprintf(stderr, "node %u: %s in term %llu (leader %u)\n", raft.id(),
                role_name(raft.role()), static_cast<unsigned long long>(raft.term()), raft.leader());
    }

    Options options_;
    FileStorage storage_;
    std::unique_ptr<StateMachine> machine_;
    Replica replica_;
    int listen_fd_{-1};
    std::map<std::uint64_t, Connection> conns_;
    std::uint64_t next_conn_{};
    std::map<NodeId, Link> links_;
    std::unordered_map<std::uint64_t, std::uint64_t> client_conn_; // client ID -> connection ID
    Role last_role_{Role::Follower};
    Term last_term_{};
};

} // namespace

int main(int argc, char** argv) {
    try {
        const auto options = parse(argc, argv);
        std::signal(SIGPIPE, SIG_IGN);
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);
        Server server(options);
        server.run();
    } catch (const std::exception& error) {
        std::cerr << "quorum-node: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
