"""Shared harness for the mentat integration tests: builds the binary once,
starts daemons/agents as subprocesses with fake GPUs, and cleans them up."""

import atexit
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RUST = os.path.join(ROOT, "rust")
PYTHON_PKG = os.path.join(ROOT, "python")
BINARY = os.path.join(RUST, "target", "debug", "mentat")

_children = []


def build_binary():
    subprocess.run(["cargo", "build"], cwd=RUST, check=True)
    assert os.path.exists(BINARY), BINARY
    return BINARY


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _reap_all():
    for p in _children:
        if p.poll() is None:
            p.kill()
    for p in _children:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


atexit.register(_reap_all)


class Cluster:
    """One daemon plus N agents, all on localhost with MENTAT_GPUS fakes."""

    def __init__(self):
        build_binary()
        self.tmp = tempfile.mkdtemp(prefix="mentat-test-")
        self.port = free_port()
        self.http_port = free_port()
        self.address = f"127.0.0.1:{self.port}"
        self.head_json = os.path.join(self.tmp, "head.json")
        self.daemon = self._spawn_daemon()
        self._wait_port(self.port)
        self.agents = []

    def _spawn_daemon(self):
        p = subprocess.Popen(
            [
                BINARY,
                "daemon",
                "--port",
                str(self.port),
                "--http-port",
                str(self.http_port),
                "--node-ip",
                "127.0.0.1",
                "--head-json",
                self.head_json,
            ],
            env={**os.environ, "MENTAT_NODE_IP": "127.0.0.1"},
        )
        _children.append(p)
        return p

    def _wait_port(self, port, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                return
            except OSError:
                time.sleep(0.05)
        raise TimeoutError(f"port {port} never opened")

    def start_agent(self, group, gpus=1, container=None, node_ip="127.0.0.1"):
        container = container or f"c{len(self.agents)}"
        env = {
            **os.environ,
            "MENTAT_DAEMON": self.address,
            "MENTAT_GROUP": group,
            "MENTAT_GPUS": str(gpus),
            "MENTAT_NODE_IP": node_ip,
            "CONTAINER_NAME": container,
            "MENTAT_SOCK_DIR": self.tmp,
            "MENTAT_PYTHON": sys.executable,
            "PYTHONPATH": os.pathsep.join([PYTHON_PKG, HERE]),
            # The actor host needs these too; it inherits the agent env.
            "MENTAT_GCS_ADDRESS": self.address,
        }
        p = subprocess.Popen([BINARY, "start", "--block"], env=env)
        _children.append(p)
        self.agents.append(p)
        return p

    def wait_group_gpus(self, group, want, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            snap = self.status_json(group)
            g = snap.get("groups", {}).get(group)
            if g and g.get("gpus_total", 0) >= want:
                return snap
            time.sleep(0.1)
        raise TimeoutError(f"group {group} never reached {want} GPUs: {self.status_json(group)}")

    def status_json(self, group=None):
        url = f"http://127.0.0.1:{self.http_port}/status"
        if group:
            url += f"?group={group}"
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.load(r)

    def metrics(self):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.http_port}/metrics", timeout=5
        ) as r:
            return r.read().decode()

    def cli(self, *args, env_extra=None, check=True):
        env = {**os.environ, **(env_extra or {})}
        return subprocess.run(
            [BINARY, *args], capture_output=True, text=True, env=env, check=check
        )

    def cleanup(self):
        for p in self.agents + [self.daemon]:
            if p.poll() is None:
                p.kill()
        shutil.rmtree(self.tmp, ignore_errors=True)


def fresh_shim(address, group):
    """Import (or re-import) the ray shim bound to this cluster/group.

    Tests call this once per driver identity; module state is process-global,
    so multi-driver scenarios use subprocesses instead.
    """
    os.environ["RAY_ADDRESS"] = address
    os.environ["MENTAT_GROUP"] = group
    sys.path.insert(0, PYTHON_PKG)
    for mod in [m for m in list(sys.modules) if m == "ray" or m.startswith("ray.")]:
        del sys.modules[mod]
    import ray  # noqa: F401

    return ray


def run_ok(fn, name):
    print(f"--- {name}")
    fn()
    print(f"    ok")
