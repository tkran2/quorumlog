# Design notes

This document explains the decisions that shaped QuorumLog and what was given
up for each. Read `README.md` first for what the system does.

## 1. The consensus core does no I/O

`RaftNode` (`include/quorum/raft.hpp`) never opens a socket, reads a clock, or
calls a random-number generator it was not handed. The host feeds it three
kinds of input and collects two kinds of output:

```text
inputs:   tick()            one unit of time has passed
          step(message)     a peer's message arrived
          propose(command)  a client wants this appended
outputs:  take_messages()   messages to send to peers
          commit_index()    how much of the log is safe to apply
```

Because of that, the same class runs in two places with no `#ifdef`:

- inside `quorum-node`, where a tick is 10 ms of real time and messages cross
  TCP sockets;
- inside the simulator, where a tick is a loop iteration and messages sit in a
  priority queue that the test controls.

The cost is an inversion of control that makes the core slightly awkward to
read: there is no "send and wait for a reply" anywhere, only state transitions.
The benefit is that every execution is a pure function of its inputs, so any
failure can be replayed from a seed.

## 2. The host loop is the durability argument

Both hosts run the same four steps per batch of inputs:

```text
1. tick / step / propose ... then flush()      decide what to do
2. storage.sync()                              make those decisions durable
3. send take_messages()                        only now tell peers
4. advance(); send take_replies()              only now apply and tell clients
```

Raft's safety proof assumes a node never forgets a vote it cast or an entry it
acknowledged. The only way to forget is to crash between promising and writing.
Putting the single `sync()` between "decide" and "tell anyone" makes that
window empty.

It also gives group commit for free. Every proposal that arrived during one
loop iteration is appended to the write-ahead log buffer and shares one
`write()` and one `fsync()`. Under load the loop naturally batches more, so
throughput rises with concurrency while fsyncs per second stay roughly flat.

The simulator can crash a node between steps 1 and 2 ("mid-step crash"). The
`send-before-sync` injected bug swaps steps 2 and 3, and the simulator's commit
audit catches it within tens of seeds.

## 3. Which parts of Raft are implemented

| Mechanism | Why it is here |
| --- | --- |
| Leader election with randomized timeouts | Core |
| Log replication with the term-and-index consistency check | Core |
| Commit only through an entry of the leader's own term | Safety; Figure 8 of the paper. A leader appends a no-op on election so this never delays commits for long |
| Conflict hints in rejected AppendEntries | A follower that diverged by a whole term is repaired in one round trip instead of one index at a time |
| Pre-vote | A node cut off from the cluster would otherwise keep raising its term and depose a healthy leader when it reconnects |
| Check-quorum | A leader that stops hearing from a majority steps down, so a one-way partition cannot leave a "leader" that accepts requests it can never commit |
| Snapshots and InstallSnapshot | The log cannot grow forever, and a follower that was down past the compaction point still has to catch up |
| Client sessions | Exactly-once commands (section 5) |

Not implemented: membership changes, leadership transfer, and lease or
read-index reads. Cluster membership is fixed at start-up. Reads go through
the log like writes, which costs a round of replication per read and makes
their correctness identical to that of writes.

## 4. Storage format and crash recovery

`FileStorage` (`include/quorum/file_storage.hpp`) keeps three files in one
directory.

```text
meta      current term and vote
snapshot  latest state-machine snapshot, with the log index and term it covers
wal       append-only log
```

**Small files are replaced, never edited.** `meta` and `snapshot` are written
to a temporary file, fsynced, and renamed over the old one, and then the
directory is fsynced. A reader sees the old file or the new one, never a mix.
Both end in a checksum. Since neither is ever partially written, a failed
checksum there is real corruption, and the node refuses to start rather than
guess.

**The log is checksummed per record.**

```text
record: [u32 length][u32 crc32][u64 index][u64 term][payload]
```

On open, records are read until one fails its length or checksum test.
Everything after that point is a write that never finished, so the file is
truncated there. Because `sync()` returns only after `fsync`, an entry that was
acknowledged to anyone is always before the truncation point.

**Compaction is two steps that can be interrupted.** Publishing a snapshot and
rewriting the log without its prefix cannot be made atomic together, so each
log file carries a generation number and the snapshot records the oldest
generation it may be combined with. A crash between the two steps leaves a new
snapshot beside an old log; on open the old log's generation is too low, and it
is discarded rather than replayed on top of a snapshot that already includes
it. `tests/test_storage.cpp` cuts the files at every interesting byte offset
and checks recovery.

Three durability levels are available: `none` (no fsync; fast, unsafe on power
loss, used to measure protocol overhead), `fsync` (the default), and `full`
(F_FULLFSYNC on macOS, where plain fsync does not flush the drive cache).

## 5. Exactly-once commands

A client that times out cannot know whether its command was applied. It must
retry, and the retry must not apply the command twice.

Each client picks a random 64-bit ID and numbers its requests. The replicated
state includes a session table: for each client, the highest sequence number
applied and the reply it produced. When a committed entry carries a sequence
number already in the table, the state machine is not touched and the saved
reply is returned.

The table is part of the snapshot and is included in the state fingerprint, so
replicas must agree on it. A client sends one request at a time, which is why
remembering only the latest reply per client is enough.

The `no-dedupe` injected bug removes this check. The linearizability checker
catches it on the first seed, because an `append` applied twice produces a
value no sequential order can explain.

## 6. The simulator

`Simulation` (`include/quorum/sim.hpp`) runs three to five replicas and several
clients in one thread. One seeded generator decides everything that is
nondeterministic in a real deployment:

- message delay, loss (3%), duplication (2%), and long delays that deliver a
  message into a later term;
- partitions into two groups, isolating one node (preferring the leader),
  one-way "deaf" nodes, and healing;
- crashes that lose unsynced writes, crashes between deciding and syncing,
  immediate restarts, and long stalls after which a node still believes its
  old state;
- when clients send, what they send, and when they give up and retry.

Random faults seldom land in the few ticks where they matter, so two faults
are aimed: a crash right after a node votes, and a crash or partition right
after a leader has sent entries that may have reached only some followers.
The cluster size and the replication batch size also vary with the seed.

After every step the simulator checks what Raft promises, using its own view
of the cluster rather than trusting the code under test:

| Check | Statement |
| --- | --- |
| Election safety | At most one leader per term |
| Leader completeness | A leader's log contains every entry committed in an earlier term |
| Log matching | Two logs that agree on the term at an index are identical up to it |
| State machine safety | Every replica applies the same command at each index and reaches the same state |
| Commit audit | When a leader declares an index committed, a majority really holds that entry on synced storage |
| Monotonic term and commit | Neither ever decreases on a live node |
| Liveness | After faults stop, clients finish within a bound |
| Linearizability | The clients' observed history has a legal sequential order that respects real time |

### Why mutation testing

A test suite that has never failed has not shown it can. Eight wrong versions
of the rules are compiled in behind `Config::bugs` (all off in the server), and
`scripts/mutants.py` measures how many seeds the simulator needs to notice each.

With a budget of 150,000 seeds per bug, seven are caught. The fastest take one
or two seeds. Committing an old-term entry by counting replicas (Figure 8)
took 119,967 seeds, which says something about how rare that interleaving is
under random scheduling. The eighth, counting an acknowledgment that was sent
in an older term, was **not** found by random search at all. It is covered by a
hand-staged scenario in `tests/test_core.cpp` that delivers one delayed message
at exactly the wrong moment.

The honest reading: randomized simulation is good at bugs with many triggering
schedules and weak at bugs that need one specific message to be late by a whole
term. Both kinds of test are needed.

## 7. The linearizability checker

`check_linearizable` (`include/quorum/linearizability.hpp`) takes a history of
client operations, each with an invocation time, a return time, and a result,
and decides whether some sequential order explains all the results while
respecting real time (if A returned before B was invoked, A comes first).

It is the Wing-Gong search with Lowe's memoization: repeatedly pick an
operation that could legally go next, apply it to a model of the key-value
store, and backtrack when a result does not match. Each (set of operations
placed, model state) pair is remembered so no dead end is explored twice.

Two details make it practical:

- **Per-key decomposition.** Operations on different keys do not interact, so
  each key is checked alone.
- **Unanswered operations.** A request with no reply may have taken effect at
  any later time or never. Its return is placed after everything else, so the
  search may put it anywhere after its invocation, including after every
  observed result, which is the same as it never happening.

The checker shares the key-value step function with the real state machine
(`kv::step`), which is deliberate: the model and the implementation cannot
disagree about what `cas` means. What the checker tests is the ordering.

## 8. Networking

One thread, non-blocking sockets, `poll()`. Frames are
`[u32 length][u32 crc32][payload]` with a one-byte type.

Each pair of nodes uses two one-directional TCP connections, one per sender,
which removes the "who connects to whom" race. Outbound queues to peers are
bounded: if a peer is not keeping up, messages to it are dropped, and Raft's
retransmission repairs the gap. That choice trades a little recovery latency
for a guarantee that a dead peer cannot consume memory.

A single thread caps throughput at one core. It was chosen because the
interesting cost here is fsync, not CPU, and because a single-threaded host is
the one the simulator models exactly.

## 9. Reusing the matching engine

`exchange::Book` (`include/quorum/exchange.hpp`) wraps the matching engine from
the earlier project as a replicated state machine. Two small additions were
made to the vendored copy, listed in `third_party/matching/README.md`: a way to
restore the book from a snapshot, and a way to resume a fingerprint.

The engine was already deterministic (same commands, same trades, same
fingerprints), which is exactly the property state-machine replication needs.
`make test` replays the engine's example session through a three-node cluster,
kills the leader partway, and checks that the final summary line is
byte-for-byte what the standalone engine prints.

## 10. What would change for production

- Membership changes (joint consensus or single-server changes).
- Read-index or lease reads so reads do not pay for replication.
- A thread per disk and per network direction, or io_uring, once fsync is no
  longer the bottleneck.
- Snapshots streamed in chunks instead of one message.
- Background scrubbing of the log, and repairing a damaged replica from its
  peers instead of refusing to start.
- TLS and authentication between nodes and for clients.
