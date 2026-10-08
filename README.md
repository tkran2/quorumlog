# QuorumLog

A replicated log in C++20 built on the Raft consensus protocol, with the
testing machinery needed to believe it: a deterministic fault-injecting
simulator, a linearizability checker, and deliberately broken variants that
show the checks can fail.

Three or five processes agree on one ordered sequence of commands and apply it
to identical state machines. Any minority can crash, stall, or be cut off, and
clients still see one consistent history. Two state machines are included: a
key-value store, and the matching engine from an earlier project.

The core has no third-party dependencies.

## Evidence

| Claim | How it was checked | Result |
| --- | --- | --- |
| Safety under faults | 20,000 seeded simulated clusters with message loss, partitions, crashes, and stalls | 0 violations across 5,992,587 client operations |
| The checks can fail | 8 protocol bugs compiled in behind a test flag, 150,000 seeds each | 7 caught by random simulation; the 8th by a hand-staged scenario |
| Real processes agree with the simulation | 7 leader kills under load on a live three-node cluster | 889,141 operations, every history linearizable |
| Exactly-once commands | 1,500 orders replayed into a replicated matching engine with the leader killed partway | Output identical to one engine that saw each order once |
| Crash recovery | Log cut at every byte offset of the final record; crash between snapshot and log rewrite | Recovers to the last synced state every time |
| Memory safety | Unit and simulator tests under AddressSanitizer and UBSan | Clean |

## Run it

You need a C++20 compiler, `make`, and Python 3 (standard library only, for
the scripts that start local clusters).

```bash
make -j2
make test          # 52 core cases, 15 storage cases, 9 real-cluster tests
make demo          # start three nodes, kill the leader, replicate the engine
```

`make demo` prints, in part:

```text
== Killing node 1, the leader, with SIGKILL
   node 3 took over about 247 ms later
   node 1: down
   node 2: follower  term 2  applied    3  state 821a73d546f9b868
   node 3: leader    term 2  applied    5  state 6d66bdfd43d82efb

== Everything that was acknowledged is still there
   $ quorum-cli get account
     OK 100+25

== Now the same log drives a matching engine on three replicas
   $ quorum-cli replay examples/session.txt --quiet
     SUMMARY commands=12 active=0 trades=6 rejected=1 events=df3ac538e031b5bc state=c41876d9ad53c569
   A single unreplicated engine, fed the same file, prints:
     SUMMARY commands=12 active=0 trades=6 rejected=1 events=df3ac538e031b5bc state=c41876d9ad53c569
   identical fingerprints
```

### Run a cluster by hand

Three terminals:

```bash
./build/quorum-node --id 1 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/1
./build/quorum-node --id 2 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/2
./build/quorum-node --id 3 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data data/3
```

A fourth:

```bash
C=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
./build/quorum-cli --cluster $C status
./build/quorum-cli --cluster $C put color red
./build/quorum-cli --cluster $C cas color red green
./build/quorum-cli --cluster $C get color
```

Press Ctrl-C in the leader's terminal and repeat the `get`. Start it again and
run `status`: it rejoins as a follower with the same state fingerprint.

## The simulator

```bash
make soak                                   # 2,000 seeded clusters, about 20 seconds
./build/quorum-sim --seeds 20000            # the run recorded below
./build/quorum-sim --first 80 --seeds 1 --trace
```

The consensus code performs no I/O and reads no clock, so an entire cluster
runs in one thread as a pure function of a seed. Each seed chooses a cluster of
three to five nodes and a schedule of faults:

- messages delayed, dropped, duplicated, or held long enough to arrive in a
  later term;
- partitions, isolated nodes, and nodes that can send but not receive;
- crashes that lose unsynced writes, including crashes between deciding and
  writing;
- stalled processes that wake up still believing they lead.

After every step it checks election safety, leader completeness, log matching,
state-machine safety, that every commit really was on a majority's stable
storage, and finally that the clients' observed history is linearizable.

The recorded 20,000-seed run:

| | |
| --- | ---: |
| Violations | 0 |
| Client operations checked | 5,992,587 |
| Crashes | 263,661 |
| Partitions | 645,747 |
| Process stalls | 79,946 |
| Messages (dropped) | 179,486,229 (26,906,575) |
| Leader changes | 257,206 |
| Snapshot installs | 158,160 |

### Proving the checks can fail

```bash
make mutants                                # 3,000 seeds per bug, about a minute: catches six
./build/quorum-sim --list-bugs
./build/quorum-sim --bug forget-vote --hunt --seeds 2000
python3 scripts/mutants.py build/quorum-sim --budget 150000   # the table below, about 40 minutes
```

| Injected bug | First failing seed | Check that fired |
| --- | ---: | --- |
| A retried client command is applied twice | 1 | linearizability |
| The leader answers reads without committing them | 1 | linearizability |
| Voters ignore how current the candidate's log is | 2 | leader completeness |
| Messages leave before the state they promise is durable | 25 | commit without quorum |
| A restarted node forgets it already voted | 80 | election safety |
| A follower acknowledges entries it never compared | 1,375 | state-machine safety |
| The leader commits an old-term entry by counting replicas (Raft Figure 8) | 119,967 | leader completeness |
| The leader counts an acknowledgment sent in an older term | not found in 150,000 | staged test only |

The last row is the useful one. Random schedules did not produce the one
delayed message that bug needs, so it is covered by a scenario in
`tests/test_core.cpp` that delivers that message by hand. Simulation is strong
evidence, not proof.

## Performance

Three `quorum-node` processes and the load generator on one two-core cloud VM
over loopback, fsync on every batch. These numbers show the software's overhead
and how group commit scales; they are not a claim about real networks or disks.
Details and the full table: [`benchmarks/README.md`](benchmarks/README.md).

| Clients | Throughput | p50 | p99 | Commands per fsync |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,504 /s | 0.38 ms | 0.64 ms | 1.0 |
| 16 | 15,877 /s | 0.99 ms | 1.67 ms | 4.6 |
| 64 | 38,130 /s | 1.60 ms | 3.11 ms | 17.4 |

Killing the leader under load, seven trials: clients were stalled for a median
of 288 ms (205 to 376), with a 200 to 400 ms election timeout. No acknowledged
write was lost and every recorded history was linearizable.

```bash
make bench         # about two minutes; writes results/bench.json
```

## What is implemented

- Leader election, log replication, and the commit rule from the Raft paper
- Pre-vote and check-quorum, so a partitioned node cannot disrupt a healthy
  leader and a one-way partition cannot leave a leader that commits nothing
- Fast log repair with conflict hints
- Snapshots, log compaction, and InstallSnapshot for followers that fell behind
- Client sessions for exactly-once commands across retries and leader changes
- A write-ahead log with per-record checksums, torn-write recovery, atomic
  metadata and snapshot replacement, and group commit
- A single-threaded `poll()` server with checksummed framing and bounded queues

Not implemented: membership changes, leadership transfer, lease or read-index
reads (reads go through the log), chunked snapshots, TLS.

## Layout

```text
include/quorum/
  raft.hpp              the consensus core: no I/O, no clock
  replica.hpp           core + state machine + client sessions
  storage.hpp           storage interface and the in-memory model used by tests
  file_storage.hpp      write-ahead log, snapshot, and metadata on disk
  sim.hpp               deterministic cluster simulator and its checks
  linearizability.hpp   history checker
  kv.hpp  exchange.hpp  the two state machines
  net.hpp  codec.hpp    framing, client library, wire format
src/                    quorum-node, quorum-cli, quorum-bench, quorum-sim
tests/                  test_core.cpp  test_storage.cpp  test_cluster.py
scripts/                local cluster helper, demo, benchmark, mutation check
third_party/matching/   the matching engine's headers (see its README)
docs/                   design notes, interview guide, resume entry, setup
benchmarks/             recorded results and how to read them
```

Read [`docs/design.md`](docs/design.md) for why each piece is the way it is.

MIT license.
