// quorum-bench: closed-loop load generator. Each simulated client keeps one
// request outstanding, retries it with the same sequence number on timeout or
// redirect, and records what it saw. With --check the recorded history is
// tested for linearizability afterward.

#include "quorum/exchange.hpp"
#include "quorum/kv.hpp"
#include "quorum/linearizability.hpp"
#include "quorum/net.hpp"

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

namespace {

using namespace quorum;

struct Options {
    std::vector<net::Address> nodes;
    int clients{16};
    double seconds{5};
    std::string workload{"kv"};
    int keys{1000};
    int value_bytes{64};
    int read_percent{50};
    int attempt_ms{300};
    std::uint64_t seed{1};
    bool check{};
    std::string timeline;
    std::string history;
};

void usage() {
    std::cout <<
        "Usage: quorum-bench --cluster 1=host:port,... [options]\n"
        "  --clients N        concurrent closed-loop clients (default 16)\n"
        "  --seconds S        measurement window (default 5)\n"
        "  --workload kv|exchange   (default kv)\n"
        "  --keys N           key space for kv (default 1000; 256 with --check)\n"
        "  --value-bytes N    value size for kv writes (default 64)\n"
        "  --read-percent N   share of reads in the kv workload (default 50)\n"
        "  --check            kv only: unique values, mixed put/append/get/cas, then\n"
        "                     verify the recorded history is linearizable\n"
        "  --attempt-ms N     per-attempt timeout before retrying elsewhere (default 300)\n"
        "  --timeline FILE    completions per 50 ms bucket, as CSV\n"
        "  --history FILE     write the recorded history (with --check)\n"
        "  --seed N\n";
}

std::vector<net::Address> parse_cluster(const std::string& text) {
    std::map<NodeId, net::Address> members;
    std::stringstream list(text);
    NodeId next = 1;
    for (std::string item; std::getline(list, item, ',');) {
        const auto eq = item.find('=');
        if (eq == std::string::npos) members[next++] = net::parse_address(item);
        else members[static_cast<NodeId>(std::stoul(item.substr(0, eq)))] = net::parse_address(item.substr(eq + 1));
    }
    std::vector<net::Address> nodes;
    for (const auto& [id, address] : members) {
        (void)id;
        nodes.push_back(address);
    }
    if (nodes.empty()) throw std::invalid_argument("--cluster is empty");
    return nodes;
}

struct Session {
    std::uint64_t client{};
    std::uint64_t seq{};
    std::size_t target{};
    int fd{-1};
    bool connecting{};
    net::FrameReader reader;
    std::string out;
    std::size_t sent{};
    bool busy{};
    bool queued{};            // Request frame placed on the current connection.
    std::string payload;
    std::int64_t invoked{};
    std::int64_t deadline{};
    std::int64_t resume_at{}; // Do not reconnect before this time.
    std::size_t history_slot{};
    std::uint64_t counter{};  // Per-session unique value / order counter.
    std::vector<std::uint64_t> live_orders;
};

class Bench {
public:
    explicit Bench(const Options& options) : o_(options), rng_(options.seed * 2654435761ULL + 17) {
        sessions_.resize(static_cast<std::size_t>(o_.clients));
        const auto base = net::random_client_id() | 1ULL;
        for (std::size_t i = 0; i < sessions_.size(); ++i) {
            sessions_[i].client = base + i * 0x100000001ULL;
            sessions_[i].target = i % o_.nodes.size();
        }
        // The checker assumes every key starts absent, so a checked run uses
        // keys no earlier run can have touched.
        prefix_ = o_.check ? "run" + std::to_string(base % 1000000007ULL) + "-" : "key";
    }

    int run() {
        start_ = net::now_ns();
        end_ = start_ + static_cast<std::int64_t>(o_.seconds * 1e9);
        const std::int64_t grace = end_ + 3'000'000'000LL; // Let in-flight requests finish.
        std::vector<pollfd> fds;
        std::vector<std::size_t> owner;

        while (true) {
            const auto now = net::now_ns();
            bool any_busy = false;
            for (auto& s : sessions_) {
                if (!s.busy && now < end_) begin(s);
                if (s.busy) {
                    any_busy = true;
                    if (now >= s.deadline) retry(s, now, true);
                    if (s.fd < 0 && now >= s.resume_at) connect(s);
                }
            }
            if (now >= grace || (now >= end_ && !any_busy)) break;

            fds.clear();
            owner.clear();
            for (std::size_t i = 0; i < sessions_.size(); ++i) {
                auto& s = sessions_[i];
                if (s.fd < 0) continue;
                short events = POLLIN;
                if (s.connecting || s.out.size() > s.sent) events |= POLLOUT;
                fds.push_back({s.fd, events, 0});
                owner.push_back(i);
            }
            if (fds.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            if (::poll(fds.data(), static_cast<nfds_t>(fds.size()), 2) <= 0) continue;
            for (std::size_t k = 0; k < fds.size(); ++k)
                if (fds[k].revents != 0) service(sessions_[owner[k]], fds[k].revents);
        }
        for (auto& s : sessions_)
            if (s.fd >= 0) ::close(s.fd);
        return report();
    }

private:
    std::int64_t stamp() {
        auto t = net::now_ns();
        if (t <= last_stamp_) t = last_stamp_ + 1;
        last_stamp_ = t;
        return t;
    }

    void begin(Session& s) {
        s.payload = o_.workload == "exchange" ? next_order(s) : next_kv(s);
        ++s.seq;
        s.busy = true;
        s.queued = false;
        s.invoked = stamp();
        s.deadline = s.invoked + static_cast<std::int64_t>(o_.attempt_ms) * 1'000'000LL;
        if (o_.check) {
            Operation op;
            op.client = s.client;
            op.invoke = static_cast<std::uint64_t>(s.invoked);
            op.input = s.payload;
            s.history_slot = history_.size();
            history_.push_back(std::move(op));
        }
        if (s.fd >= 0 && !s.connecting) queue(s);
    }

    std::string next_kv(Session& s) {
        const std::string key = prefix_ + std::to_string(rng_.below(static_cast<std::uint64_t>(o_.keys)));
        if (!o_.check) {
            if (rng_.below(100) < static_cast<std::uint64_t>(o_.read_percent)) return kv::get(key);
            std::string value(static_cast<std::size_t>(o_.value_bytes), 'v');
            return kv::put(key, value);
        }
        // Every written value is unique, so each read pins down exactly which
        // writes it observed.
        const std::string token = std::to_string(s.client % 100000) + "." + std::to_string(++s.counter) + ";";
        const auto roll = rng_.below(100);
        if (roll < 30) return kv::put(key, token);
        if (roll < 50) return kv::append(key, token);
        if (roll < 90) return kv::get(key);
        return kv::cas(key, token, token); // Almost always a mismatch; still constrains order.
    }

    std::string next_order(Session& s) {
        const auto roll = rng_.below(100);
        const auto side = rng_.below(2) ? matching::Side::Buy : matching::Side::Sell;
        const auto index = static_cast<std::uint64_t>(&s - sessions_.data()) + 1;
        if (roll < 60 || s.live_orders.empty()) {
            const std::uint64_t id = (index << 32U) | ++s.counter;
            s.live_orders.push_back(id);
            if (s.live_orders.size() > 64) s.live_orders.erase(s.live_orders.begin());
            return exchange::limit(id, side, 10000 - 10 + static_cast<matching::Price>(rng_.below(21)),
                1 + static_cast<matching::Quantity>(rng_.below(100)));
        }
        if (roll < 70) return exchange::market((index << 32U) | ++s.counter, side,
            1 + static_cast<matching::Quantity>(rng_.below(150)));
        const auto id = s.live_orders[rng_.below(s.live_orders.size())];
        if (roll < 90) return exchange::cancel(id);
        return exchange::amend(id, 10000 - 10 + static_cast<matching::Price>(rng_.below(21)),
            1 + static_cast<matching::Quantity>(rng_.below(100)));
    }

    void connect(Session& s) {
        s.fd = net::start_connect(o_.nodes[s.target]);
        s.connecting = s.fd >= 0;
        s.reader = net::FrameReader{};
        s.out.clear();
        s.sent = 0;
        s.queued = false;
        if (s.fd < 0) {
            s.target = (s.target + 1) % o_.nodes.size();
            s.resume_at = net::now_ns() + 20'000'000LL;
        }
    }

    void queue(Session& s) {
        net::append_frame(s.out, net::encode_request(ClientRequest{s.client, s.seq, s.payload}));
        s.queued = true;
        flush(s);
    }

    void flush(Session& s) {
        while (s.sent < s.out.size()) {
            const auto n = ::send(s.fd, s.out.data() + s.sent, s.out.size() - s.sent, 0);
            if (n > 0) {
                s.sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            if (n < 0 && errno == EINTR) continue;
            retry(s, net::now_ns(), false);
            return;
        }
        s.out.clear();
        s.sent = 0;
    }

    // Abandon this connection and try another node, keeping the same sequence
    // number so the server can recognize the request if it already applied.
    void retry(Session& s, std::int64_t now, bool timed_out, std::size_t hint = SIZE_MAX) {
        if (timed_out) ++timeouts_;
        if (s.fd >= 0) ::close(s.fd);
        s.fd = -1;
        s.connecting = false;
        s.queued = false;
        s.target = hint != SIZE_MAX ? hint : (s.target + 1) % o_.nodes.size();
        s.resume_at = now + (hint != SIZE_MAX ? 0 : 10'000'000LL);
        s.deadline = now + static_cast<std::int64_t>(o_.attempt_ms) * 1'000'000LL;
    }

    void service(Session& s, short events) {
        if (s.fd < 0) return;
        if (events & (POLLERR | POLLNVAL)) return retry(s, net::now_ns(), false);
        if (s.connecting) {
            if (!(events & POLLOUT)) {
                if (events & POLLHUP) retry(s, net::now_ns(), false);
                return;
            }
            if (!net::connect_finished(s.fd)) return retry(s, net::now_ns(), false);
            s.connecting = false;
            if (s.busy && !s.queued) queue(s);
            if (s.fd < 0) return;
        }
        if (events & POLLOUT) {
            flush(s);
            if (s.fd < 0) return;
        }
        if (!(events & (POLLIN | POLLHUP))) return;
        char buffer[1 << 16];
        const auto n = ::recv(s.fd, buffer, sizeof buffer, 0);
        if (n == 0) return retry(s, net::now_ns(), false);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            return retry(s, net::now_ns(), false);
        }
        s.reader.feed(buffer, static_cast<std::size_t>(n));
        try {
            std::string payload;
            while (s.fd >= 0 && s.reader.next(payload)) on_reply(s, net::decode_reply(payload));
        } catch (const DecodeError&) {
            ++errors_;
            retry(s, net::now_ns(), false);
        }
    }

    void on_reply(Session& s, const ClientReply& reply) {
        if (!s.busy || reply.client != s.client || reply.seq != s.seq) return;
        const auto now = net::now_ns();
        switch (reply.status) {
        case ReplyStatus::Ok: {
            const auto done = stamp();
            s.busy = false;
            if (o_.check) {
                history_[s.history_slot].ret = static_cast<std::uint64_t>(done);
                history_[s.history_slot].output = reply.result;
            }
            if (done <= end_) {
                latencies_.push_back(done - s.invoked);
                completions_.push_back(done);
            } else {
                ++late_;
            }
            if (now < end_) begin(s);
            break;
        }
        case ReplyStatus::NotLeader:
            ++redirects_;
            if (reply.leader != no_node && reply.leader <= o_.nodes.size()) {
                if (reply.leader - 1 == s.target) {
                    // "I am the leader now, ask me again."
                    s.queued = false;
                    s.deadline = now + static_cast<std::int64_t>(o_.attempt_ms) * 1'000'000LL;
                    queue(s);
                } else {
                    retry(s, now, false, reply.leader - 1);
                }
            } else {
                retry(s, now, false);
                s.resume_at = now + 20'000'000LL; // An election is under way.
            }
            break;
        case ReplyStatus::Stale:
            ++errors_;
            s.busy = false;
            break;
        }
    }

    static std::int64_t percentile(const std::vector<std::int64_t>& sorted, double p) {
        if (sorted.empty()) return 0;
        const auto rank = static_cast<std::size_t>(p * static_cast<double>(sorted.size() - 1) + 0.5);
        return sorted[std::min(rank, sorted.size() - 1)];
    }

    int report() {
        std::sort(latencies_.begin(), latencies_.end());
        std::sort(completions_.begin(), completions_.end());
        // Longest stretch with no completed request: the client's view of an outage.
        std::int64_t gap = 0;
        std::int64_t gap_at = 0;
        std::int64_t previous = start_;
        for (const auto t : completions_) {
            if (t - previous > gap) {
                gap = t - previous;
                gap_at = previous;
            }
            previous = t;
        }
        if (end_ - previous > gap) {
            gap = end_ - previous;
            gap_at = previous;
        }
        std::uint64_t pending = 0;
        for (const auto& s : sessions_)
            if (s.busy) ++pending;

        if (!o_.timeline.empty()) {
            std::ofstream file(o_.timeline);
            file << "ms,completions\n";
            const std::int64_t bucket = 50'000'000LL;
            std::size_t i = 0;
            for (std::int64_t t = start_; t < end_; t += bucket) {
                std::size_t count = 0;
                while (i < completions_.size() && completions_[i] < t + bucket) {
                    ++count;
                    ++i;
                }
                file << (t - start_) / 1'000'000LL << ',' << count << '\n';
            }
        }

        std::string verdict = "not-checked";
        std::uint64_t states = 0;
        if (o_.check) {
            const auto result = check_linearizable(history_);
            states = result.states;
            verdict = result.verdict == Verdict::Linearizable ? "linearizable"
                : result.verdict == Verdict::Violation ? "VIOLATION on " + result.key
                : "inconclusive on " + result.key;
            if (!o_.history.empty()) {
                std::ofstream file(o_.history);
                file << "client,invoke_ns,return_ns,input_hex,output_hex\n";
                const auto hex = [](const std::string& bytes) {
                    static const char* digits = "0123456789abcdef";
                    std::string out;
                    for (const char raw : bytes) {
                        const auto ch = static_cast<unsigned char>(raw);
                        out.push_back(digits[ch >> 4U]);
                        out.push_back(digits[ch & 15U]);
                    }
                    return out;
                };
                for (const auto& op : history_)
                    file << op.client << ',' << op.invoke - static_cast<std::uint64_t>(start_) << ','
                        << (op.completed() ? std::to_string(op.ret - static_cast<std::uint64_t>(start_)) : "")
                        << ',' << hex(op.input) << ',' << hex(op.output) << '\n';
            }
        }

        // After the load stops, every reachable replica should report the same state.
        std::string digest;
        int reachable = 0;
        bool agree = false;
        for (int attempt = 0; attempt < 60 && !agree; ++attempt) {
            std::vector<std::string> seen;
            for (const auto& node : o_.nodes) {
                const auto status = net::Client::status(node, 200);
                if (!status) continue;
                const auto word = [&](const char* name) {
                    const auto at = status->find(name);
                    if (at == std::string::npos) return std::string();
                    const auto from = at + std::strlen(name);
                    return status->substr(from, status->find(' ', from) - from);
                };
                // Compare what was applied and the resulting state. Log bases
                // may differ: each node compacts on its own schedule.
                seen.push_back(word(" applied=") + " " + word(" digest="));
            }
            reachable = static_cast<int>(seen.size());
            agree = !seen.empty() && std::all_of(seen.begin(), seen.end(),
                [&](const std::string& s) { return s == seen.front(); });
            if (agree) digest = seen.front().substr(seen.front().find(' ') + 1);
            else std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        const double seconds = static_cast<double>(end_ - start_) / 1e9;
        const auto us = [](std::int64_t ns) { return static_cast<double>(ns) / 1e3; };
        std::cout.setf(std::ios::fixed);
        std::cout.precision(1);
        std::cout << "{\n"
            << "  \"workload\": \"" << o_.workload << (o_.check ? "-check" : "") << "\",\n"
            << "  \"nodes\": " << o_.nodes.size() << ",\n"
            << "  \"clients\": " << o_.clients << ",\n"
            << "  \"seconds\": " << seconds << ",\n"
            << "  \"completed\": " << latencies_.size() << ",\n"
            << "  \"throughput_per_s\": " << static_cast<double>(latencies_.size()) / seconds << ",\n"
            << "  \"latency_us\": {\"p50\": " << us(percentile(latencies_, 0.50))
            << ", \"p90\": " << us(percentile(latencies_, 0.90))
            << ", \"p99\": " << us(percentile(latencies_, 0.99))
            << ", \"p999\": " << us(percentile(latencies_, 0.999))
            << ", \"max\": " << us(latencies_.empty() ? 0 : latencies_.back()) << "},\n"
            << "  \"longest_stall_ms\": " << static_cast<double>(gap) / 1e6 << ",\n"
            << "  \"stall_began_ms\": " << static_cast<double>(gap_at - start_) / 1e6 << ",\n"
            << "  \"redirects\": " << redirects_ << ",\n"
            << "  \"timeouts\": " << timeouts_ << ",\n"
            << "  \"errors\": " << errors_ << ",\n"
            << "  \"completed_after_window\": " << late_ << ",\n"
            << "  \"unanswered\": " << pending << ",\n"
            << "  \"history\": \"" << verdict << "\",\n"
            << "  \"checker_states\": " << states << ",\n"
            << "  \"replicas_reachable\": " << reachable << ",\n"
            << "  \"replicas_agree\": " << (agree ? "true" : "false") << ",\n"
            << "  \"state_digest\": \"" << digest << "\"\n"
            << "}\n";
        if (errors_ > 0) return 1;
        if (o_.check && verdict != "linearizable") return 1;
        return agree ? 0 : 1;
    }

    Options o_;
    Rng rng_;
    std::string prefix_;
    std::vector<Session> sessions_;
    std::vector<Operation> history_;
    std::vector<std::int64_t> latencies_;
    std::vector<std::int64_t> completions_;
    std::int64_t start_{};
    std::int64_t end_{};
    std::int64_t last_stamp_{};
    std::uint64_t redirects_{};
    std::uint64_t timeouts_{};
    std::uint64_t errors_{};
    std::uint64_t late_{};
};

} // namespace

int main(int argc, char** argv) {
    try {
        Options o;
        bool keys_given = false;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            const auto value = [&]() -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
                return argv[++i];
            };
            if (arg == "--cluster") o.nodes = parse_cluster(value());
            else if (arg == "--clients") o.clients = std::stoi(value());
            else if (arg == "--seconds") o.seconds = std::stod(value());
            else if (arg == "--workload") o.workload = value();
            else if (arg == "--keys") { o.keys = std::stoi(value()); keys_given = true; }
            else if (arg == "--value-bytes") o.value_bytes = std::stoi(value());
            else if (arg == "--read-percent") o.read_percent = std::stoi(value());
            else if (arg == "--attempt-ms") o.attempt_ms = std::stoi(value());
            else if (arg == "--seed") o.seed = std::stoull(value());
            else if (arg == "--check") o.check = true;
            else if (arg == "--timeline") o.timeline = value();
            else if (arg == "--history") o.history = value();
            else if (arg == "--help") {
                usage();
                return 0;
            } else throw std::invalid_argument("unknown option: " + arg);
        }
        if (o.nodes.empty()) {
            usage();
            return 2;
        }
        if (o.workload != "kv" && o.workload != "exchange") throw std::invalid_argument("--workload must be kv or exchange");
        if (o.check && o.workload != "kv") throw std::invalid_argument("--check needs the kv workload");
        if (o.check && !keys_given) o.keys = 256;
        if (o.clients < 1 || o.clients > 4096) throw std::invalid_argument("--clients must be 1 to 4096");
        if (o.keys < 1 || o.value_bytes < 0 || o.seconds <= 0 || o.attempt_ms < 10)
            throw std::invalid_argument("invalid numeric option");
        std::signal(SIGPIPE, SIG_IGN);
        Bench bench(o);
        return bench.run();
    } catch (const std::exception& error) {
        std::cerr << "quorum-bench: " << error.what() << '\n';
        return 2;
    }
}
