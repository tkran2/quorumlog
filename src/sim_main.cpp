// Deterministic fault simulator. Runs many seeded clusters and reports any
// broken safety property, non-linearizable history, or failure to recover.

#include "quorum/sim.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

namespace {

struct BugName {
    const char* name;
    bool quorum::TestBugs::* flag;
    const char* description;
};

const BugName bug_table[] = {
    {"commit-old-term", &quorum::TestBugs::commit_old_term,
     "leader commits an earlier-term entry by counting replicas (Raft Figure 8)"},
    {"skip-up-to-date", &quorum::TestBugs::skip_up_to_date_check,
     "voters ignore how current the candidate's log is"},
    {"match-last-index", &quorum::TestBugs::match_uses_last_index,
     "follower acknowledges entries it never compared with the leader"},
    {"stale-term-reply", &quorum::TestBugs::accept_stale_term_reply,
     "leader counts acknowledgments sent in an older term"},
    {"no-dedupe", &quorum::TestBugs::no_dedupe,
     "a retried client command is applied twice"},
    {"stale-read", &quorum::TestBugs::stale_read,
     "leader answers reads locally without committing them"},
    {"send-before-sync", &quorum::TestBugs::send_before_sync,
     "messages leave before the state they promise is durable"},
    {"forget-vote", &quorum::TestBugs::forget_vote,
     "a restarted node forgets it already voted this term"},
};

void usage() {
    std::cout <<
        "Usage: quorum-sim [options]\n"
        "  --seeds N        number of seeds to run (default 200)\n"
        "  --first S        first seed (default 1)\n"
        "  --nodes N        fixed cluster size, 1 to 9 (default: 3 to 5, varied by seed)\n"
        "  --batch N        fixed replication batch size (default: 1 to 512, varied by seed)\n"
        "  --clients N      concurrent clients (default 4)\n"
        "  --ticks N        length of the fault phase (default 4000)\n"
        "  --no-pre-vote    disable the pre-vote phase\n"
        "  --bug NAME       inject a deliberate bug (see --list-bugs)\n"
        "  --hunt           with --bug: stop at the first seed that exposes it\n"
        "  --trace          print faults and leader changes (use with one seed)\n"
        "  --csv            one CSV row per seed\n"
        "  --list-bugs\n";
}

} // namespace

int main(int argc, char** argv) {
    quorum::SimOptions options;
    std::uint64_t seeds = 200;
    std::uint64_t first = 1;
    bool hunt = false;
    bool csv = false;
    std::string bug;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            const auto value = [&]() -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
                return argv[++i];
            };
            if (arg == "--seeds") seeds = std::stoull(value());
            else if (arg == "--first") first = std::stoull(value());
            else if (arg == "--nodes") { options.nodes = std::stoi(value()); options.vary = false; }
            else if (arg == "--batch") { options.raft.max_batch_entries = std::stoull(value()); options.vary = false; }
            else if (arg == "--clients") options.clients = std::stoi(value());
            else if (arg == "--ticks") options.ticks = std::stoi(value());
            else if (arg == "--no-pre-vote") options.raft.pre_vote = false;
            else if (arg == "--bug") bug = value();
            else if (arg == "--hunt") hunt = true;
            else if (arg == "--trace") options.trace = true;
            else if (arg == "--csv") csv = true;
            else if (arg == "--list-bugs") {
                for (const auto& entry : bug_table)
                    std::cout << entry.name << "\t" << entry.description << '\n';
                return 0;
            } else if (arg == "--help") {
                usage();
                return 0;
            } else throw std::invalid_argument("unknown option: " + arg);
        }
        if (options.nodes < 1 || options.nodes > 9) throw std::invalid_argument("--nodes must be 1 to 9");
        if (options.clients < 1 || options.clients > 32) throw std::invalid_argument("--clients must be 1 to 32");
        if (options.ticks < 100) throw std::invalid_argument("--ticks must be at least 100");
        if (!bug.empty()) {
            bool found = false;
            for (const auto& entry : bug_table)
                if (bug == entry.name) {
                    options.raft.bugs.*entry.flag = true;
                    found = true;
                }
            if (!found) throw std::invalid_argument("unknown bug: " + bug);
        }
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 2;
    }

    if (csv)
        std::cout << "seed,ok,kind,tick,operations,pending,leader_changes,max_term,messages,"
                     "dropped,crashes,restarts,partitions,stalls,snapshots,committed,"
                     "checker_states,settled_after\n";

    const auto started = std::chrono::steady_clock::now();
    quorum::SimStats total;
    std::uint64_t failures = 0;
    std::uint64_t ran = 0;
    for (std::uint64_t seed = first; seed < first + seeds; ++seed) {
        options.seed = seed;
        quorum::Simulation simulation(options);
        const auto result = simulation.run();
        ++ran;
        const auto& s = result.stats;
        total.operations += s.operations;
        total.pending += s.pending;
        total.leader_changes += s.leader_changes;
        total.messages += s.messages;
        total.dropped += s.dropped;
        total.crashes += s.crashes;
        total.restarts += s.restarts;
        total.partitions += s.partitions;
        total.stalls += s.stalls;
        total.snapshots_installed += s.snapshots_installed;
        total.committed += s.committed;
        total.checker_states += s.checker_states;
        if (csv)
            std::cout << seed << ',' << (result.ok ? 1 : 0) << ',' << result.kind << ','
                << result.tick << ',' << s.operations << ',' << s.pending << ','
                << s.leader_changes << ',' << s.max_term << ',' << s.messages << ','
                << s.dropped << ',' << s.crashes << ',' << s.restarts << ',' << s.partitions
                << ',' << s.stalls << ',' << s.snapshots_installed << ',' << s.committed << ','
                << s.checker_states << ',' << s.settled_after << '\n';
        if (!result.ok) {
            ++failures;
            if (!csv)
                std::cout << "FAIL seed=" << seed << " tick=" << result.tick << " " << result.kind
                    << ": " << result.detail << '\n';
            if (hunt) break;
        }
    }
    const double seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - started).count();

    if (!csv) {
        std::cout << "seeds=" << ran << " failures=" << failures
            << " nodes=" << (options.vary ? std::string("3-5") : std::to_string(options.nodes))
            << " clients=" << options.clients
            << " bug=" << (bug.empty() ? "none" : bug) << '\n'
            << "operations=" << total.operations << " unanswered=" << total.pending
            << " committed=" << total.committed << " leader_changes=" << total.leader_changes << '\n'
            << "messages=" << total.messages << " dropped=" << total.dropped
            << " crashes=" << total.crashes << " restarts=" << total.restarts
            << " partitions=" << total.partitions << " stalls=" << total.stalls
            << " snapshot_installs=" << total.snapshots_installed << '\n'
            << "checker_states=" << total.checker_states << " seconds=" << seconds << '\n';
    }
    // With an injected bug, success means the simulator caught it.
    if (!bug.empty()) return failures > 0 ? 0 : 1;
    return failures == 0 ? 0 : 1;
}
