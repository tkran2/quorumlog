#!/usr/bin/env python3
"""End-to-end tests against real quorum-node processes on localhost.

Each test starts its own three-node cluster on free ports with a private data
directory, injects process failures with signals, and checks what a client can
observe. Standard library only.

    python3 tests/test_cluster.py build
"""

import os
import random
import subprocess
import sys
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import localcluster  # noqa: E402
from localcluster import Cluster, finish  # noqa: E402


def build_path(name):
    return os.path.join(localcluster.BUILD, name)


class KeyValueCluster(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster()
        self.addCleanup(self.cluster.close)
        self.cluster.leader()

    def test_basic_operations(self):
        c = self.cluster
        self.assertEqual(c.cli("get", "missing"), (1, "NOT_FOUND"))
        self.assertEqual(c.cli("put", "color", "red"), (0, "OK"))
        self.assertEqual(c.cli("append", "color", "dish"), (0, "OK"))
        self.assertEqual(c.cli("get", "color"), (0, "OK reddish"))
        self.assertEqual(c.cli("cas", "color", "blue", "green"), (1, "MISMATCH reddish"))
        self.assertEqual(c.cli("cas", "color", "reddish", "green"), (0, "OK"))
        self.assertEqual(c.cli("get", "color"), (0, "OK green"))
        self.assertEqual(c.cli("del", "color"), (0, "OK"))
        self.assertEqual(c.cli("get", "color"), (1, "NOT_FOUND"))
        rows = c.wait_agree()
        self.assertEqual(sum(r["role"] == "leader" for r in rows), 1)

    def test_acknowledged_writes_survive_a_killed_leader(self):
        c = self.cluster
        for i in range(20):
            self.assertEqual(c.cli("put", f"k{i}", f"v{i}"), (0, "OK"))
        first = c.leader()
        c.kill(first)
        second = c.leader(exclude=(first,))
        self.assertNotEqual(first, second)
        for i in range(20):
            self.assertEqual(c.cli("get", f"k{i}"), (0, f"OK v{i}"))
        self.assertEqual(c.cli("put", "after", "failover"), (0, "OK"))
        # The old leader returns, steps into the new term, and catches up.
        c.start(first)
        rows = c.wait_agree()
        self.assertTrue(all(int(r["term"]) >= 2 for r in rows))
        self.assertEqual(c.cli("get", "after"), (0, "OK failover"))

    def test_minority_cannot_commit(self):
        c = self.cluster
        self.assertEqual(c.cli("put", "x", "1"), (0, "OK"))
        leader = c.leader()
        others = [n for n in (1, 2, 3) if n != leader]
        c.kill(others[0])
        c.kill(others[1])
        # One node of three is not a majority. The client gets no answer at all,
        # which is different from "rejected": the outcome is unknown to it.
        code, out = c.cli("put", "x", "2", timeout_ms=1500)
        self.assertEqual(code, 3)
        self.assertTrue(out.startswith("UNKNOWN"))
        c.start(others[0])
        self.assertEqual(c.cli("put", "y", "3", timeout_ms=15000), (0, "OK"))
        code, out = c.cli("get", "x")
        self.assertEqual(code, 0)
        self.assertIn(out, ("OK 1", "OK 2"))  # Either is legal; nothing was promised.
        c.start(others[1])
        c.wait_agree()

    def test_whole_cluster_restart_recovers_from_disk(self):
        c = self.cluster
        for i in range(30):
            self.assertEqual(c.cli("append", "journal", f"{i};"), (0, "OK"))
        before = c.wait_agree()[0]["digest"]
        for node in (1, 2, 3):
            c.kill(node)
        for node in (1, 2, 3):
            c.start(node)
        c.leader()
        expected = "".join(f"{i};" for i in range(30))
        self.assertEqual(c.cli("get", "journal"), (0, "OK " + expected))
        # The read above was itself a logged command, so compare state by value.
        self.assertEqual(len(before), 16)
        c.wait_agree()

    def test_retry_with_same_identity_applies_once(self):
        c = self.cluster
        # Two separate processes present the same client ID and sequence number,
        # as a client would after losing the first answer.
        self.assertEqual(c.cli("append", "once", "A", client_id=4242), (0, "OK"))
        self.assertEqual(c.cli("append", "once", "A", client_id=4242), (0, "OK"))
        self.assertEqual(c.cli("get", "once"), (0, "OK A"))

    def test_history_stays_linearizable_through_kill_and_pause(self):
        c = self.cluster
        bench = c.bench("--clients", 12, "--check", seconds=7)
        time.sleep(1.5)
        first = c.leader()
        c.kill(first)                      # Crash the leader mid-load.
        second = c.leader(exclude=(first,))
        time.sleep(1.0)
        c.start(first)
        time.sleep(1.0)
        c.pause(second)                    # Freeze the new leader: it still believes it leads.
        time.sleep(1.2)
        c.resume(second)
        code, report = finish(bench)
        self.assertEqual(report["history"], "linearizable", report)
        self.assertEqual(report["errors"], 0, report)
        self.assertTrue(report["replicas_agree"], report)
        self.assertEqual(code, 0, report)
        self.assertGreater(report["completed"], 500, report)
        self.assertGreater(report["redirects"] + report["timeouts"], 0, report)


class SnapshotCluster(unittest.TestCase):
    def test_lagging_follower_catches_up_from_a_snapshot(self):
        c = Cluster(extra=["--snapshot-every", "300", "--snapshot-keep", "30"])
        self.addCleanup(c.close)
        leader = c.leader()
        follower = next(n for n in (1, 2, 3) if n != leader)
        c.kill(follower)
        code, report = finish(c.bench("--clients", 8, "--check", seconds=2))
        self.assertEqual(report["history"], "linearizable", report)
        self.assertGreater(report["completed"], 1000, report)
        rows = {r["id"]: r for r in c.status() if "base" in r}
        self.assertGreater(int(rows[str(leader)]["base"]), 0)  # The leader compacted its log.
        # The follower's missing entries no longer exist as log records anywhere.
        c.start(follower)
        rows = {r["id"]: r for r in c.wait_agree()}
        self.assertGreater(int(rows[str(follower)]["base"]), 0)
        self.assertEqual(rows[str(follower)]["digest"], rows[str(leader)]["digest"])


class ExchangeCluster(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster(machine="exchange")
        self.addCleanup(self.cluster.close)
        self.cluster.leader()

    def local(self, path):
        done = subprocess.run([build_path("quorum-cli"), "replay", path, "--local"],
                              capture_output=True, text=True, check=True)
        return done.stdout.strip()

    def test_replicated_session_matches_the_standalone_engine(self):
        session = os.path.join(ROOT, "examples", "session.txt")
        code, out = self.cluster.cli("replay", session)
        self.assertEqual(code, 0, out)
        # The final line is what `matching-engine --file examples/session.txt` prints.
        self.assertEqual(out.splitlines()[-1],
                         "SUMMARY commands=12 active=0 trades=6 rejected=1 "
                         "events=df3ac538e031b5bc state=c41876d9ad53c569")
        self.assertEqual(out, self.local(session))
        self.cluster.wait_agree()

    def test_order_flow_is_exactly_once_across_a_leader_kill(self):
        rng = random.Random(2027)
        path = os.path.join(self.cluster.dir, "flow.txt")
        next_id = 1
        with open(path, "w") as f:
            for _ in range(1500):
                roll = rng.random()
                side = rng.choice(("BUY", "SELL"))
                if roll < 0.6:
                    f.write(f"LIMIT {next_id} {side} {rng.randint(9990, 10010)} {rng.randint(1, 50)}\n")
                    next_id += 1
                elif roll < 0.7:
                    f.write(f"MARKET {next_id} {side} {rng.randint(1, 80)}\n")
                    next_id += 1
                elif roll < 0.9:
                    f.write(f"CANCEL {rng.randint(1, next_id)}\n")
                else:
                    f.write(f"AMEND {rng.randint(1, next_id)} {rng.randint(9990, 10010)} {rng.randint(1, 50)}\n")
        expected = self.local(path)

        c = self.cluster
        replay = subprocess.Popen(
            [build_path("quorum-cli"), "--cluster", c.spec, "--timeout-ms", "20000",
             "replay", path], stdout=subprocess.PIPE, text=True)
        time.sleep(0.4)
        first = c.leader()
        c.kill(first)  # Mid-replay: some command is in flight and will be retried.
        out, _ = replay.communicate(timeout=120)
        self.assertEqual(replay.returncode, 0)
        # Every outcome, every trade, and both fingerprints match one engine
        # that saw each command exactly once, in order.
        self.assertEqual(out.strip(), expected)
        c.start(first)
        rows = c.wait_agree()
        self.assertEqual(len({r["digest"] for r in rows}), 1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        localcluster.set_build(sys.argv.pop(1))
    unittest.main(verbosity=2)
