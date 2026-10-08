#!/usr/bin/env python3
"""A two-minute tour: start three nodes, kill the leader, watch the cluster
keep its promises, then replicate the matching engine.

    python3 scripts/demo.py build
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import localcluster  # noqa: E402
from localcluster import Cluster  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def say(text):
    print(f"\n== {text}")


def show(cluster):
    for row in cluster.status():
        if "role" in row:
            print(f"   node {row['id']}: {row['role']:9} term {row['term']}  applied {row['applied']:>4}"
                  f"  state {row['digest']}")
        else:
            print(f"   node {row.get('id', '?')}: down")


def run(cluster, *args):
    code, out = cluster.cli(*args)
    print(f"   $ quorum-cli {' '.join(str(a) for a in args)}\n     {out}")
    return out


def main():
    if len(sys.argv) > 1:
        localcluster.set_build(sys.argv[1])

    say("Starting three key-value nodes on localhost")
    kv = Cluster()
    try:
        leader = kv.leader()
        show(kv)
        say("Writing through whichever node is the leader")
        run(kv, "put", "account", "100")
        run(kv, "append", "account", "+25")
        run(kv, "get", "account")

        say(f"Killing node {leader}, the leader, with SIGKILL")
        kv.kill(leader)
        started = time.time()
        new_leader = kv.leader(exclude=(leader,))
        print(f"   node {new_leader} took over about {1000 * (time.time() - started):.0f} ms later")
        show(kv)
        say("Everything that was acknowledged is still there")
        run(kv, "get", "account")
        run(kv, "cas", "account", "100+25", "125")

        say(f"Restarting node {leader}: it rejoins as a follower and catches up")
        kv.start(leader)
        kv.wait_agree()
        show(kv)
    finally:
        kv.close()

    say("Now the same log drives a matching engine on three replicas")
    ex = Cluster(machine="exchange")
    try:
        ex.leader()
        session = os.path.join(ROOT, "examples", "session.txt")
        session = os.path.relpath(session)
        out = run(ex, "replay", session, "--quiet")
        print("   A single unreplicated engine, fed the same file, prints:")
        code, local = ex.cli("replay", session, "--local", "--quiet")
        print(f"     {local}")
        print("   identical fingerprints" if out == local else "   MISMATCH")
        ex.wait_agree()
        show(ex)
    finally:
        ex.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
