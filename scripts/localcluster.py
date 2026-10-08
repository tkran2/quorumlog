"""Run a QuorumLog cluster as local processes. Shared by the tests and scripts.

Standard library only. Each Cluster picks free ports and a private temporary
data directory, and can crash (SIGKILL), freeze (SIGSTOP), and restart nodes.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

BUILD = os.path.abspath("build")


def set_build(path):
    global BUILD
    BUILD = os.path.abspath(path)


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            sockets.append(s)
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class Cluster:
    def __init__(self, machine="kv", nodes=3, extra=()):
        self.machine = machine
        self.count = nodes
        self.extra = list(extra)
        self.dir = tempfile.mkdtemp(prefix="quorumlog-cluster-")
        self.ports = free_ports(nodes)
        self.spec = ",".join(f"{i + 1}=127.0.0.1:{p}" for i, p in enumerate(self.ports))
        self.procs = {}
        for node in range(1, nodes + 1):
            self.start(node)

    def start(self, node):
        log = open(os.path.join(self.dir, f"node{node}.log"), "ab")
        self.procs[node] = subprocess.Popen(
            [os.path.join(BUILD, "quorum-node"), "--id", str(node), "--cluster", self.spec,
             "--data", os.path.join(self.dir, f"node{node}"), "--machine", self.machine,
             "--verbose", *self.extra],
            stdout=log, stderr=log)
        log.close()

    def kill(self, node):
        """SIGKILL: no chance to flush or say goodbye."""
        proc = self.procs.pop(node)
        proc.kill()
        proc.wait()

    def pause(self, node):
        self.procs[node].send_signal(signal.SIGSTOP)

    def resume(self, node):
        self.procs[node].send_signal(signal.SIGCONT)

    def close(self):
        for proc in self.procs.values():
            try:
                proc.send_signal(signal.SIGCONT)
                proc.kill()
                proc.wait()
            except ProcessLookupError:
                pass
        shutil.rmtree(self.dir, ignore_errors=True)

    def cli(self, *args, timeout_ms=10000, client_id=None):
        command = [os.path.join(BUILD, "quorum-cli"), "--cluster", self.spec,
                   "--timeout-ms", str(timeout_ms)]
        if client_id is not None:
            command += ["--client-id", str(client_id)]
        done = subprocess.run(command + [str(a) for a in args], capture_output=True, text=True,
                              timeout=timeout_ms / 1000 + 30)
        return done.returncode, done.stdout.strip()

    def leader(self, timeout=15.0, exclude=()):
        deadline = time.time() + timeout
        while time.time() < deadline:
            code, out = self.cli("leader", timeout_ms=1000)
            if code == 0 and int(out) not in exclude:
                return int(out)
            time.sleep(0.05)
        raise AssertionError("no leader elected in time")

    def status(self):
        _, out = self.cli("status")
        rows = []
        for line in out.splitlines():
            rows.append(dict(item.split("=", 1) for item in line.split() if "=" in item))
        return rows

    def wait_agree(self, timeout=20.0):
        """Wait until every node is up and reports the same applied index and digest."""
        deadline = time.time() + timeout
        rows = []
        while time.time() < deadline:
            rows = self.status()
            live = [r for r in rows if "digest" in r]
            if len(live) == self.count and len({(r["applied"], r["digest"]) for r in live}) == 1:
                return live
            time.sleep(0.1)
        raise AssertionError(f"replicas did not converge: {rows}")

    def bench(self, *args, seconds=3):
        command = [os.path.join(BUILD, "quorum-bench"), "--cluster", self.spec,
                   "--seconds", str(seconds), *[str(a) for a in args]]
        return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def finish(proc, timeout=120):
    out, err = proc.communicate(timeout=timeout)
    try:
        return proc.returncode, json.loads(out)
    except json.JSONDecodeError as error:
        raise AssertionError(f"bench did not print JSON: {out!r} {err!r}") from error
