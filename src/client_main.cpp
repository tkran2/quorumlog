// quorum-cli: talk to a running cluster from the terminal.

#include "matching/command.hpp"
#include "quorum/exchange.hpp"
#include "quorum/kv.hpp"
#include "quorum/net.hpp"

#include <csignal>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>

namespace {

using namespace quorum;

void usage() {
    std::cout <<
        "Usage: quorum-cli --cluster 1=host:port,2=host:port,... COMMAND\n"
        "Key-value cluster:\n"
        "  put KEY VALUE | get KEY | append KEY VALUE | cas KEY EXPECTED NEW | del KEY\n"
        "Exchange cluster:\n"
        "  replay FILE [--quiet]   send a matching-engine session file, one command at a time\n"
        "  replay FILE --local     run the same file through one in-process engine instead\n"
        "  summary                 command count, trades, and both fingerprints\n"
        "Any cluster:\n"
        "  status                  one line per node\n"
        "  leader                  wait for a leader and print its ID\n"
        "Options: --client-id N   reuse a client identity (default: random)\n"
        "         --timeout-ms N  give up after this long (default 10000)\n";
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
    NodeId expected = 1;
    for (const auto& [id, address] : members) {
        if (id != expected++) throw std::invalid_argument("member IDs must be 1..N with no gaps");
        nodes.push_back(address);
    }
    if (nodes.empty()) throw std::invalid_argument("--cluster is empty");
    return nodes;
}

std::string field(const std::string& status, const std::string& name) {
    const auto at = status.find(name + "=");
    if (at == std::string::npos) return {};
    const auto start = at + name.size() + 1;
    return status.substr(start, status.find(' ', start) - start);
}

int print_kv(const std::optional<std::string>& answer) {
    if (!answer) {
        std::cout << "UNKNOWN no answer before the deadline; the command may or may not have been applied\n";
        return 3;
    }
    const auto result = kv::decode_result(*answer);
    switch (result.status) {
    case kv::Status::Ok: std::cout << "OK" << (result.value.empty() ? "" : " ") << result.value << '\n'; return 0;
    case kv::Status::NotFound: std::cout << "NOT_FOUND\n"; return 1;
    case kv::Status::Mismatch: std::cout << "MISMATCH " << result.value << '\n'; return 1;
    case kv::Status::BadRequest: std::cout << "BAD_REQUEST\n"; return 2;
    }
    return 2;
}

void print_summary(const exchange::Summary& s) {
    // Same line the standalone matching-engine prints, so the two can be diffed.
    std::cout << "SUMMARY commands=" << s.commands << " active=" << s.active
        << " trades=" << s.trades << " rejected=" << s.rejected
        << " events=" << std::hex << s.events << " state=" << s.state << std::dec << '\n';
}

} // namespace

int main(int argc, char** argv) {
    // A node that dies mid-request must surface as a failed write, not kill us.
    std::signal(SIGPIPE, SIG_IGN);
    try {
        std::string cluster;
        std::uint64_t client_id = 0;
        int timeout_ms = 10000;
        bool quiet = false;
        bool local = false;
        std::vector<std::string> words;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            const auto value = [&]() -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
                return argv[++i];
            };
            if (arg == "--cluster") cluster = value();
            else if (arg == "--client-id") client_id = std::stoull(value());
            else if (arg == "--timeout-ms") timeout_ms = std::stoi(value());
            else if (arg == "--quiet") quiet = true;
            else if (arg == "--local") local = true;
            else if (arg == "--help") {
                usage();
                return 0;
            } else words.push_back(arg);
        }
        if ((cluster.empty() && !local) || words.empty()) {
            usage();
            return 2;
        }
        if (local && words[0] != "replay") throw std::invalid_argument("--local only applies to replay");
        const auto nodes = local ? std::vector<net::Address>{{"127.0.0.1", 1}} : parse_cluster(cluster);
        const auto deadline = std::chrono::milliseconds(timeout_ms);
        const std::string& command = words[0];
        const auto need = [&](std::size_t count) {
            if (words.size() != count + 1) throw std::invalid_argument("wrong argument count for " + command);
        };

        if (command == "status") {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                const auto status = net::Client::status(nodes[i]);
                std::cout << (status ? *status : "id=" + std::to_string(i + 1) + " unreachable") << '\n';
            }
            return 0;
        }
        if (command == "leader") {
            const auto give_up = net::now_ns() + static_cast<std::int64_t>(timeout_ms) * 1'000'000LL;
            while (net::now_ns() < give_up) {
                for (const auto& node : nodes) {
                    const auto status = net::Client::status(node, 200);
                    if (status && field(*status, "role") == "leader") {
                        std::cout << field(*status, "id") << '\n';
                        return 0;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            std::cout << "none\n";
            return 1;
        }

        net::Client client(nodes, client_id);
        // The unreplicated reference: one engine in this process.
        exchange::Book reference;
        const auto call = [&](const std::string& payload) -> std::optional<std::string> {
            if (local) return reference.apply(payload);
            return client.call(payload, deadline);
        };
        if (command == "put") { need(2); return print_kv(client.call(kv::put(words[1], words[2]), deadline)); }
        if (command == "get") { need(1); return print_kv(client.call(kv::get(words[1]), deadline)); }
        if (command == "append") { need(2); return print_kv(client.call(kv::append(words[1], words[2]), deadline)); }
        if (command == "cas") { need(3); return print_kv(client.call(kv::cas(words[1], words[2], words[3]), deadline)); }
        if (command == "del") { need(1); return print_kv(client.call(kv::del(words[1]), deadline)); }

        if (command == "summary") {
            need(0);
            const auto answer = client.call(exchange::summary(), deadline);
            if (!answer) throw std::runtime_error("no answer before the deadline");
            print_summary(exchange::decode_summary(*answer));
            return 0;
        }
        if (command == "replay") {
            need(1);
            std::ifstream file(words[1]);
            if (!file) throw std::runtime_error("cannot open " + words[1]);
            std::size_t line_number = 0;
            for (std::string line; std::getline(file, line);) {
                ++line_number;
                std::optional<matching::Command> parsed;
                try {
                    parsed = matching::parse_command(line);
                } catch (const std::exception& error) {
                    throw std::runtime_error("line " + std::to_string(line_number) + ": " + error.what());
                }
                if (!parsed) continue;
                const auto& c = *parsed;
                std::string payload;
                if (c.action == matching::Action::Submit)
                    payload = c.order.type == matching::OrderType::Limit
                        ? exchange::limit(c.order.id, c.order.side, c.order.price, c.order.quantity)
                        : exchange::market(c.order.id, c.order.side, c.order.quantity);
                else if (c.action == matching::Action::Cancel) payload = exchange::cancel(c.order.id);
                else if (c.action == matching::Action::Amend)
                    payload = exchange::amend(c.order.id, c.order.price, c.order.quantity);
                else if (c.action == matching::Action::Quit) break;
                else continue; // BOOK, ORDERS, CHECK, HELP are local to the standalone engine.

                const auto answer = call(payload);
                if (!answer)
                    throw std::runtime_error("line " + std::to_string(line_number) +
                        ": no answer before the deadline; stopping so the replay stays in order");
                if (quiet) continue;
                const auto result = exchange::decode_outcome(*answer);
                if (result.accepted())
                    std::cout << "ACCEPT " << result.sequence << ' ' << result.id
                        << " resting=" << result.resting << " unfilled=" << result.unfilled
                        << " priority_lost=" << result.priority_lost << '\n';
                else
                    std::cout << "REJECT " << result.sequence << ' ' << result.id << ' '
                        << matching::reject_name(result.reject) << '\n';
                for (const auto& trade : result.trades)
                    std::cout << "TRADE " << result.sequence << ' ' << trade.maker_id << ' '
                        << trade.taker_id << ' ' << matching::side_name(trade.taker_side) << ' '
                        << trade.price << ' ' << trade.quantity << '\n';
            }
            const auto answer = call(exchange::summary());
            if (!answer) throw std::runtime_error("no answer to the final summary");
            print_summary(exchange::decode_summary(*answer));
            return 0;
        }
        throw std::invalid_argument("unknown command: " + command);
    } catch (const std::exception& error) {
        std::cerr << "quorum-cli: " << error.what() << '\n';
        return 2;
    }
}
