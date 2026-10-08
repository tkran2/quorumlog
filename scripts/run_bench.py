#!/usr/bin/env python3
"""Measure a local cluster and write results/bench.json plus a Markdown table.

Everything runs on one machine over loopback, so these numbers describe the
software's overhead (protocol, batching, fsync), not a real network.

    python3 scripts/run_bench.py build            # full run, about 3 minutes
    python3 scripts/run_bench.py build --quick    # smoke run for CI
"""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import localcluster  # noqa: E402
from localcluster import Cluster, finish  # noqa: E402


def leader_fsyncs(cluster):
    for row in cluster.status():
        if row.get("role") == "leader":
            return int(row["fsyncs"])
    return 0


def throughput(nodes, durability, clients, seconds, workload="kv", machine="kv"):
    cluster = Cluster(machine=machine, nodes=nodes, extra=["--durability", durability])
    try:
        cluster.leader()
        time.sleep(0.3)
        before = leader_fsyncs(cluster)
        code, report = finish(cluster.bench("--clients", clients, "--workload", workload, seconds=seconds))
        after = leader_fsyncs(cluster)
        if code != 0:
            raise SystemExit(f"benchmark failed: {report}")
        fsyncs = max(after - before, 0)
        return {
            "scenario": "throughput", "machine": machine, "nodes": nodes, "durability": durability,
            "clients": clients, "seconds": report["seconds"], "completed": report["completed"],
            "throughput_per_s": report["throughput_per_s"],
            "p50_us": report["latency_us"]["p50"], "p99_us": report["latency_us"]["p99"],
            "p999_us": report["latency_us"]["p999"], "max_us": report["latency_us"]["max"],
            "leader_fsyncs": fsyncs,
            "commands_per_fsync": round(report["completed"] / fsyncs, 2) if fsyncs else None,
            "replicas_agree": report["replicas_agree"],
        }
    finally:
        cluster.close()


def failover(trial, clients, seconds):
    cluster = Cluster(extra=["--durability", "fsync"])
    try:
        cluster.leader()
        bench = cluster.bench("--clients", clients, "--check", seconds=seconds)
        time.sleep(seconds * 0.4)
        victim = cluster.leader()
        killed_at = time.time()
        cluster.kill(victim)
        cluster.leader(exclude=(victim,))
        elected_after = time.time() - killed_at
        time.sleep(1.0)
        cluster.start(victim)
        code, report = finish(bench)
        return {
            "scenario": "failover", "trial": trial, "clients": clients,
            "completed": report["completed"], "history": report["history"],
            "longest_stall_ms": report["longest_stall_ms"],
            "new_leader_visible_ms": round(1000 * elected_after, 1),
            "timeouts": report["timeouts"], "redirects": report["redirects"],
            "unanswered": report["unanswered"], "replicas_agree": report["replicas_agree"],
            "checker_states": report["checker_states"], "exit_code": code,
        }
    finally:
        cluster.close()


def environment():
    def run(*command):
        try:
            return subprocess.run(command, capture_output=True, text=True).stdout.strip()
        except OSError:
            return ""
    cpu = ""
    if os.path.exists("/proc/cpuinfo"):
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
    else:
        cpu = run("sysctl", "-n", "machdep.cpu.brand_string")
    return {
        "date": time.strftime("%Y-%m-%d"), "system": platform.system(), "machine": platform.machine(),
        "cpu": cpu, "logical_cpus": os.cpu_count(), "python": platform.python_version(),
        "compiler": run(os.environ.get("CXX", "c++"), "--version").splitlines()[0] if run(os.environ.get("CXX", "c++"), "--version") else "",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("build", nargs="?", default="build")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--out", default="results/bench.json")
    args = parser.parse_args()
    localcluster.set_build(args.build)
    seconds = 1.5 if args.quick else 5
    rows = []

    plan = [(3, "fsync", 16)] if args.quick else [
        (3, "fsync", 1), (3, "fsync", 4), (3, "fsync", 16), (3, "fsync", 64),
        (3, "none", 1), (3, "none", 16), (3, "none", 64),
        (1, "fsync", 16), (5, "fsync", 16),
    ]
    for nodes, durability, clients in plan:
        row = throughput(nodes, durability, clients, seconds)
        rows.append(row)
        print(f"kv {nodes} nodes {durability:5} {clients:3} clients: {row['throughput_per_s']:9.0f} ops/s  "
              f"p50 {row['p50_us'] / 1000:6.2f} ms  p99 {row['p99_us'] / 1000:6.2f} ms  "
              f"{row['commands_per_fsync']} commands/fsync", flush=True)

    row = throughput(3, "fsync", 16, seconds, workload="exchange", machine="exchange")
    rows.append(row)
    print(f"exchange 3 nodes fsync 16 clients: {row['throughput_per_s']:9.0f} orders/s  "
          f"p50 {row['p50_us'] / 1000:6.2f} ms  p99 {row['p99_us'] / 1000:6.2f} ms", flush=True)

    trials = 1 if args.quick else 7
    stalls = []
    for trial in range(1, trials + 1):
        row = failover(trial, 16, 4 if args.quick else 8)
        rows.append(row)
        stalls.append(row["longest_stall_ms"])
        print(f"failover trial {trial}: longest stall {row['longest_stall_ms']:.0f} ms, "
              f"{row['completed']} operations, history {row['history']}", flush=True)
    summary = {
        "failover_trials": trials,
        "failover_stall_ms_median": statistics.median(stalls),
        "failover_stall_ms_min": min(stalls), "failover_stall_ms_max": max(stalls),
        "all_histories_linearizable": all(r["history"] == "linearizable" for r in rows if r["scenario"] == "failover"),
    }
    print(f"failover: median stall {summary['failover_stall_ms_median']:.0f} ms "
          f"(min {summary['failover_stall_ms_min']:.0f}, max {summary['failover_stall_ms_max']:.0f}); "
          f"all histories linearizable: {summary['all_histories_linearizable']}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({"environment": environment(), "summary": summary, "rows": rows}, f, indent=2)
    print(f"wrote {args.out}")
    return 0 if summary["all_histories_linearizable"] else 1


if __name__ == "__main__":
    sys.exit(main())
