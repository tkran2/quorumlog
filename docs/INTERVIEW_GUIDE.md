# Own the project in an interview

Before presenting this, run `make demo`, run `make soak`, and reproduce one
injected bug yourself:

```bash
./build/quorum-sim --bug forget-vote --hunt --seeds 2000
./build/quorum-sim --bug forget-vote --first 80 --seeds 1 --trace | tail -40
```

Read the trace until you can say in your own words why two leaders appeared.
An interviewer who knows Raft will ask about exactly that kind of interleaving.

## A 45-second explanation

"I built a replicated log in C++20 using Raft, and spent most of the effort on
proving it correct rather than making it fast. The consensus code does no I/O,
so the same class runs inside a real TCP server and inside a deterministic
simulator that injects message loss, partitions, crashes, and stalls from a
seed. After every step the simulator checks Raft's safety properties and runs a
linearizability checker over what clients saw. I ran 20,000 seeded clusters
with about six million client operations and no violations. To show the checks
can fail, I compiled in eight deliberate protocol bugs: random simulation
caught seven, and the eighth needed a hand-built schedule, which taught me
where random testing is weak. On top of the log I replicated my matching
engine, and a three-node cluster with the leader killed mid-session produces
byte-identical output to the single-process engine."

## Questions you should be able to answer

**1. What problem does consensus solve?**

Several machines must agree on one sequence of commands even though any of
them can crash and messages can be lost, delayed, duplicated, or reordered.
If they apply the same commands in the same order to the same deterministic
program, they stay identical, and the service survives losing a minority.

**2. Why a majority?**

Any two majorities share at least one node. So a decision made by one majority
is known to at least one member of any later majority, and cannot be silently
contradicted. With three nodes one can fail; with five, two.

**3. Walk through an election.**

A follower that hears nothing from a leader for a randomized timeout becomes a
candidate, raises its term, votes for itself, and asks the others. A node votes
at most once per term, and only for a candidate whose log is at least as
up to date as its own (higher last term, or same term and at least as long).
A majority of votes makes a leader. Randomized timeouts make split votes rare.

**4. Why the "at least as up to date" rule?**

It is what makes a new leader hold every committed entry. A committed entry is
on a majority; a candidate needs a majority of votes; the two majorities
overlap in a node that has the entry and will refuse a candidate that lacks it.
Turn the rule off (`--bug skip-up-to-date`) and the simulator shows a leader
missing a committed entry on the second seed.

**5. Explain Figure 8. Why can't a leader commit an old-term entry once it is
on a majority?**

Because being on a majority is not enough to stop it from being overwritten.
An entry from term 2 can sit on a majority while a node that has a term-3 entry
at the same index can still win an election (its last term is higher) and
overwrite it. So a leader only counts replicas for entries of its own term.
Committing one of those also commits everything before it. That is why a new
leader appends a no-op immediately: it gives it a current-term entry to commit.

`tests/test_core.cpp` stages the exact schedule from the paper, with the rule
on and off. Under random simulation the off case took 119,967 seeds to
surface, which is the best illustration I have of why this bug is famous.

**6. What must be on disk before a node replies, and why?**

Its current term, its vote, and any log entries it is acknowledging. If a node
votes, crashes before writing the vote, and restarts, it can vote again in the
same term and two leaders can be elected. If it acknowledges an entry it then
loses, the leader may have counted it toward a majority that no longer exists.

In the code this is one line of ordering: `flush(); storage.sync();` then send.
The `send-before-sync` and `forget-vote` bugs each break it, and the simulator
catches both within 100 seeds.

**7. How do you make fsync affordable?**

Group commit. Everything that arrives during one pass of the event loop goes
into the log buffer and shares one write and one fsync. The more concurrent
clients, the more commands per fsync. The benchmark reports commands per fsync
directly.

**8. What does linearizable mean, and how do you check it?**

Every operation appears to take effect at one instant between when the client
sent it and when it got the answer. To check it, record each operation's start
time, end time, and result, and search for an order of the operations that
(a) respects real time and (b) makes every result correct when replayed on a
single copy. I implemented the Wing-Gong search with memoization of
(operations placed, state) pairs, and split the history by key.

The hard case is a request that never got an answer. It might have happened or
not, at any later time, so the checker lets it land anywhere after it was sent.

**9. A client times out and retries. How do you avoid applying it twice?**

The client sends an ID and a sequence number with each command. The replicated
state keeps, per client, the last sequence number applied and its reply. A
committed entry with an already-seen sequence number is not applied again; the
saved reply is returned. Because the table is replicated and snapshotted, this
works across leader changes. One test kills the leader in the middle of 1,500
orders and checks that the final book equals a single engine that saw each
order exactly once.

**10. What is pre-vote for?**

A node that is cut off keeps timing out and raising its term. When it
reconnects, its high term forces the healthy leader to step down for no reason.
With pre-vote a node first asks "would you vote for me?" without changing its
term, and others say no while they still hear from a leader.

**11. And check-quorum?**

The mirror image. A leader that can send but not receive keeps heartbeating, so
followers never time out, yet it can commit nothing. With check-quorum a leader
that has not heard from a majority within an election timeout steps down.

**12. How does a follower that fell far behind catch up?**

If the leader still has the entries, by normal replication; a rejected
AppendEntries carries the follower's conflicting term and its first index, so
the leader skips a whole term per round trip. If the entries were compacted
away, the leader sends its snapshot and the follower replaces its state.

**13. How does the deterministic simulator work?**

One thread, one seeded random generator, a fake clock, and a priority queue of
in-flight messages. Each loop iteration it may deliver, drop, duplicate, or
delay messages, start or heal a partition, crash, restart, or freeze a node,
and let clients act. Because the consensus core takes all its inputs as
function calls, the whole cluster is a pure function of the seed. A failing
seed is a reproducible bug report: `quorum-sim --first N --seeds 1 --trace`.

**14. Random testing found seven of your eight bugs. What about the eighth?**

Counting an acknowledgment that was sent in an older term. For it to matter, a
reply has to be delayed across a term change and arrive when the same node is
leader again with a different log at that index. Random delays almost never
line that up; 150,000 seeds did not. I wrote a staged test that delivers that
one message at that moment and shows the commit happening without a majority.
My conclusion is that simulation finds bugs with many triggering schedules and
that the rare ones need a targeted scenario or a model checker.

**15. Why is the storage code careful about a crash between writing the
snapshot and rewriting the log?**

Those are two files and cannot be replaced atomically together. If the node
dies after the new snapshot is in place but before the log is rewritten, it
wakes up with a snapshot that already includes entries the old log would apply
again. Each log has a generation number, and the snapshot says which
generations are compatible, so the stale log is recognized and dropped.

**16. Is this fast?**

Fast enough to show the design works, and I would not claim more. The numbers
in `benchmarks/README.md` are from three processes on one two-core cloud
machine over loopback. They measure protocol and fsync overhead, not a real
network. A single-threaded server is also a deliberate ceiling.

**17. What is missing compared with a production system?**

Membership changes, read-index or lease reads (reads go through the log here),
leadership transfer, chunked snapshots, TLS, and any multi-threading. I would
also want a real model-checked specification for the cases simulation missed.

**18. Why replicate a matching engine?**

It is the natural consumer: an exchange needs every replica to produce exactly
the same trades. My engine was already deterministic and already printed
fingerprints of its events and state, so "replicated correctly" became a
byte-for-byte comparison rather than an opinion.

## Numbers to know

| Fact | Value |
| --- | --- |
| Seeded simulated clusters with zero violations | 20,000 |
| Client operations checked for linearizability in that run | 5,992,587 |
| Faults injected in that run | 263,661 crashes, 645,747 partitions, 79,946 stalls, 26.9 million dropped messages |
| Injected bugs caught by random simulation | 7 of 8, within 150,000 seeds each |
| Seeds needed for the Figure 8 bug | 119,967 |
| Tests | 52 core cases, 15 storage cases, 9 real-cluster tests |
| Source size | about 7,000 lines of C++ and Python |

## What not to claim

- Not production-ready, and not benchmarked on real hardware or a real network.
- Not formally verified. The simulator is strong evidence, not a proof, and the
  missed mutant shows its limit.
- Raft itself is not yours. The implementation, the simulator, the checker,
  and the measurements are.
