# QuorumLog project entry

Use these bullets after you have run `make demo`, reproduced one injected bug
from its seed, and can explain Raft's commit rule (Figure 8) without notes.
The project is ready to publish; no repository URL exists until you push it
(see `mac-and-github.md`). Do not call it production-ready, and keep the
benchmark caveat: one two-core cloud VM over loopback.

**QuorumLog | Fault-Tolerant Replicated Log (Raft)**
C++20, Raft consensus, deterministic simulation, POSIX sockets, write-ahead logging

- Implemented Raft in C++20 (elections, log repair, snapshots, pre-vote,
  exactly-once client sessions) with an I/O-free core that runs unchanged in a
  TCP server and a deterministic fault-injecting simulator.
- Verified linearizability across 20,000 seeded clusters and 6.0 million client
  operations under partitions, crashes, and message loss with zero violations;
  the simulator caught 7 of 8 deliberately injected protocol bugs.
- Replicated a matching engine across three nodes with byte-identical output
  through leader failure; on a local cluster, group commit sustained 38,000
  key-value commands/s with fsync and failover took a median 288 ms.

Shorter two-bullet version:

- Built a Raft-replicated log in C++20 with an I/O-free consensus core;
  verified linearizability across 20,000 seeded fault-injected clusters (6.0M
  operations, zero violations) and caught 7 of 8 injected protocol bugs.
- Replicated a matching engine across three nodes with byte-identical output
  through leader failure; 38,000 key-value commands/s with fsync via group
  commit and a median 288 ms failover on a local three-node cluster.

Where it fits: first project on a software engineering resume and on a
quantitative developer resume, where it pairs with the matching engine it
replicates. It is not the right lead for a trading resume.

If an interviewer asks about the eighth bug, that is the best part of the
story: random simulation did not find it in 150,000 seeds, and you wrote a
staged test that does.

Sources: `benchmarks/README.md` (soak totals, mutation table, throughput and
failover), `README.md` (test counts). Know the qualifiers: 38,000/s is the
key-value workload at 64 clients (the matching engine ran at 15,425 orders/s
with 16 clients, the same as key-value at 16), and "local cluster" means three
processes plus the load generator on one two-core cloud VM over loopback.
