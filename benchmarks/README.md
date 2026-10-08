# Recorded results

Three kinds of result are stored here. The first two are deterministic and
should reproduce exactly on any machine. The third is a timing measurement on
one machine and will not.

## 1. Simulation soak (deterministic)

```bash
./build/quorum-sim --seeds 20000 --csv > soak.csv      # about three minutes
```

`soak-20000.csv.gz` has one row per seed. Totals:

| | |
| --- | ---: |
| Seeds | 20,000 |
| Failures | 0 |
| Client operations checked for linearizability | 5,992,587 |
| Operations left unanswered at the end | 0 |
| Committed log entries | 6,364,543 |
| Messages sent | 179,486,229 |
| Messages dropped | 26,906,575 |
| Crashes (each followed by a restart) | 263,661 |
| Partitions | 645,747 |
| Process stalls | 79,946 |
| Snapshot installs | 158,160 |
| Leader changes | 257,206 |
| Highest term reached in any run | 43 |

The simulator draws every random choice from one seeded generator and the
consensus core does no I/O, so `make soak` (the first 2,000 of these seeds)
prints the same totals every time: 619,609 operations, 26,692 crashes,
18,286,982 messages.

## 2. Mutation check (deterministic)

```bash
python3 scripts/mutants.py build/quorum-sim --budget 150000 --csv mutants.csv   # about 40 minutes
```

`mutants-150000.csv`:

| Injected bug | Caught | First failing seed | Check that fired |
| --- | --- | ---: | --- |
| commit-old-term | yes | 119,967 | leader-completeness |
| skip-up-to-date | yes | 2 | leader-completeness |
| match-last-index | yes | 1,375 | state-machine-safety |
| stale-term-reply | **no** | (150,000 tried) | covered by a staged test |
| no-dedupe | yes | 1 | linearizability |
| stale-read | yes | 1 | linearizability |
| send-before-sync | yes | 25 | commit-without-quorum |
| forget-vote | yes | 80 | election-safety |

The seconds column in the CSV was recorded while other jobs shared the
machine and should be ignored.

## 3. Live cluster (one machine, not portable)

```bash
python3 scripts/run_bench.py build --out benchmarks/baseline.json    # about two minutes
```

`baseline.json` was recorded on 2026-10-08 with nothing else running:

- Intel Xeon, 2 virtual CPUs at 2.1 GHz, Linux, GCC 13.3, `-O2`
- every node and the load generator on the same machine, over loopback
- closed-loop clients: each sends its next request when the last one returns
- 5-second measurement windows; key-value workload, half reads, 64-byte values

What that setup does and does not show:

- It measures the cost of the protocol, the framing, and group commit.
- It does **not** measure a real network. Loopback round trips take
  microseconds.
- Four processes share two cores, so they compete with each other. With more
  cores the numbers should improve; this was not measured.
- This VM's virtual disk completes `fsync` in a fraction of a millisecond. A
  laptop SSD, especially with `--durability full` on macOS, will be slower,
  and throughput at low client counts will drop accordingly.

### Throughput and latency

| State machine | Nodes | Durability | Clients | Throughput /s | p50 ms | p99 ms | p99.9 ms | Commands per fsync |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| key-value | 3 | fsync | 1 | 2,504 | 0.38 | 0.64 | 0.86 | 1.0 |
| key-value | 3 | fsync | 4 | 5,764 | 0.65 | 1.23 | 3.12 | 1.5 |
| key-value | 3 | fsync | 16 | 15,877 | 0.99 | 1.67 | 4.58 | 4.6 |
| key-value | 3 | fsync | 64 | 38,130 | 1.60 | 3.11 | 12.79 | 17.4 |
| key-value | 3 | none | 1 | 12,283 | 0.08 | 0.14 | 0.25 | 1.0 |
| key-value | 3 | none | 16 | 62,204 | 0.25 | 0.44 | 2.04 | 5.6 |
| key-value | 3 | none | 64 | 92,897 | 0.65 | 1.44 | 5.00 | 19.6 |
| key-value | 1 | fsync | 16 | 43,249 | 0.38 | 0.69 | 1.70 | 10.2 |
| key-value | 5 | fsync | 16 | 12,374 | 1.26 | 2.26 | 6.96 | 5.1 |
| matching engine | 3 | fsync | 16 | 15,425 | 1.02 | 1.74 | 5.29 | 4.7 |

Reading it:

- **Group commit.** From 1 to 64 clients throughput rose 15 times while
  commands per fsync rose 17 times. The log is doing roughly the same number of
  fsyncs per second and carrying more in each.
- **The price of durability.** Turning fsync off raised single-client
  throughput from 2,504 to 12,283 per second. With one client every command
  waits for its own fsync on the leader and on a follower.
- **The price of replication.** One node with fsync did 43,249 per second at 16
  clients; three nodes did 15,877. On this machine much of that gap is three
  processes competing for two cores.
- **The state machine is not the bottleneck.** The matching engine and the
  key-value store run at the same rate.

"Commands per fsync" counts the leader's fsyncs only. With durability `none`
it counts sync calls that would have been fsyncs.

### Failover

A three-node cluster under 16 checking clients for 8 seconds. The leader is
killed with SIGKILL partway through and restarted one second later. "Stall" is
the longest gap in which no client anywhere got an answer.

| Trial | Operations | Longest stall | History |
| ---: | ---: | ---: | --- |
| 1 | 120,718 | 376 ms | linearizable |
| 2 | 127,548 | 205 ms | linearizable |
| 3 | 128,327 | 275 ms | linearizable |
| 4 | 122,059 | 329 ms | linearizable |
| 5 | 135,389 | 288 ms | linearizable |
| 6 | 131,909 | 353 ms | linearizable |
| 7 | 123,191 | 281 ms | linearizable |

Median 288 ms. The election timeout is drawn from 200 to 400 ms, so most of
the stall is the survivors waiting to be sure the leader is gone. In all seven
trials no request went unanswered, the three replicas ended with the same
state fingerprint, and the 889,141 recorded operations formed linearizable
histories.
