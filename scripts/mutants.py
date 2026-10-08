#!/usr/bin/env python3
"""Mutation check for the simulator.

A test suite that never fails proves nothing. This script turns on each
deliberately injected bug in turn and reports how many seeded runs the
simulator needs before one of its checks fires.

    python3 scripts/mutants.py build/quorum-sim             # 3,000 seeds per bug
    python3 scripts/mutants.py build/quorum-sim --budget 200000 --csv results/mutants.csv
"""

import argparse
import csv
import re
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("simulator")
    parser.add_argument("--budget", type=int, default=3000, help="seeds to try per bug")
    parser.add_argument("--csv", help="write the table to this file")
    parser.add_argument("--only", help="comma-separated bug names")
    args = parser.parse_args()

    listing = subprocess.run([args.simulator, "--list-bugs"], capture_output=True, text=True, check=True)
    bugs = [line.split("\t") for line in listing.stdout.strip().splitlines()]
    if args.only:
        wanted = set(args.only.split(","))
        bugs = [b for b in bugs if b[0] in wanted]

    rows = []
    for name, description in bugs:
        started = time.time()
        done = subprocess.run([args.simulator, "--bug", name, "--hunt", "--seeds", str(args.budget)],
                              capture_output=True, text=True)
        seconds = time.time() - started
        found = re.search(r"FAIL seed=(\d+) tick=(\d+) ([\w-]+):", done.stdout)
        ran = re.search(r"seeds=(\d+)", done.stdout)
        rows.append({
            "bug": name,
            "description": description,
            "caught": bool(found),
            "seeds_needed": int(found.group(1)) if found else "",
            "check_that_fired": found.group(3) if found else "",
            "seeds_tried": int(ran.group(1)) if ran else args.budget,
            "seconds": round(seconds, 1),
        })
        row = rows[-1]
        outcome = (f"caught at seed {row['seeds_needed']} by {row['check_that_fired']}"
                   if row["caught"] else f"NOT caught in {row['seeds_tried']} seeds")
        print(f"{name:18} {outcome}  ({row['seconds']} s)", flush=True)

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    caught = sum(r["caught"] for r in rows)
    print(f"{caught} of {len(rows)} injected bugs caught within {args.budget} seeds each")
    return 0


if __name__ == "__main__":
    sys.exit(main())
