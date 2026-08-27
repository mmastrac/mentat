"""Shared helpers for the mentat integration tests: build the binary once,
start daemons/agents as subprocesses with fake GPUs, clean them up."""

import atexit
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RUST = os.path.join(ROOT, "rust")
PYTHON_PKG = os.path.join(ROOT, "python")
# MENTAT_TEST_BINARY runs the suites against a prebuilt binary (e.g. the one
# out of mentat-artifacts, on a box without cargo).
BINARY = os.environ.get("MENTAT_TEST_BINARY") or os.path.join(
    RUST, "target", "debug", "mentat"
)

_children = []


def build_binary():
    if not os.environ.get("MENTAT_TEST_BINARY"):
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
    """One daemon plus N agents, all on localhost with MENTAT_GPUS fakes.

    daemon_env: extra MENTAT_* variables for the daemon process (the lifecycle
    timeout tests use short windows here)."""

    def __init__(self, daemon_env=None):
        build_binary()
        self.tmp = tempfile.mkdtemp(prefix="mentat-test-")
        self.port = free_port()
        self.http_port = free_port()
        self.address = f"127.0.0.1:{self.port}"
        self.head_json = os.path.join(self.tmp, "head.json")
        self.daemon_env = daemon_env or {}
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
            env={**os.environ, "MENTAT_NODE_IP": "127.0.0.1", **self.daemon_env},
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

    def start_agent(self, group, gpus=1, container=None, node_ip="127.0.0.1",
                    daemon_addr=None, env_extra=None):
        container = container or f"c{len(self.agents)}"
        env = {
            **os.environ,
            # daemon_addr lets the link-loss tests route the agent through a
            # TcpProxy while the driver talks to the daemon directly.
            "MENTAT_DAEMON": daemon_addr or self.address,
            "MENTAT_GROUP": group,
            "MENTAT_GPUS": str(gpus),
            "MENTAT_NODE_IP": node_ip,
            "CONTAINER_NAME": container,
            "MENTAT_SOCK_DIR": self.tmp,
            "MENTAT_PYTHON": sys.executable,
            "PYTHONPATH": os.pathsep.join([PYTHON_PKG, HERE]),
            # The actor host needs these too; it inherits the agent env.
            "MENTAT_GCS_ADDRESS": self.address,
            # Service announcements (MENTAT_OPENAI_API and friends) ride in
            # here, the same way the entrypoints export them before ray start.
            **(env_extra or {}),
        }
        env.pop("CUDA_VISIBLE_DEVICES", None)
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


class Daemon:
    """One mentatd in a mesh test: its own ports, node_ip, peer list, and
    optional MENTAT_* env overrides."""

    def __init__(self, node_ip, peers=(), port=None, env=None):
        build_binary()
        self.tmp = tempfile.mkdtemp(prefix="mentatd-")
        self.node_ip = node_ip
        self.port = port or free_port()
        self.http_port = free_port()
        self.address = f"127.0.0.1:{self.port}"
        self.peers = list(peers)
        self.proc = subprocess.Popen(
            [
                BINARY,
                "daemon",
                "--port",
                str(self.port),
                "--http-port",
                str(self.http_port),
                "--node-ip",
                node_ip,
                "--head-json",
                os.path.join(self.tmp, "head.json"),
                *(
                    ["--peers", ",".join(self.peers)]
                    if self.peers
                    else []
                ),
            ],
            env={**os.environ, **(env or {})},
        )
        _children.append(self.proc)

    def wait_up(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return self
            except OSError:
                time.sleep(0.05)
        raise TimeoutError(f"daemon {self.address} never came up")

    def status_json(self, group=None):
        url = f"http://127.0.0.1:{self.http_port}/status"
        if group:
            url += f"?group={group}"
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.load(r)

    def start_agent(self, group, gpus=1, container="c", tmp=None, env_extra=None):
        env = {
            **os.environ,
            "MENTAT_DAEMON": self.address,
            "MENTAT_GROUP": group,
            "MENTAT_GPUS": str(gpus),
            "MENTAT_NODE_IP": self.node_ip,
            "CONTAINER_NAME": container,
            "MENTAT_SOCK_DIR": tmp or self.tmp,
            "MENTAT_PYTHON": sys.executable,
            "PYTHONPATH": os.pathsep.join([PYTHON_PKG, HERE]),
            "MENTAT_GCS_ADDRESS": self.address,
            **(env_extra or {}),
        }
        env.pop("CUDA_VISIBLE_DEVICES", None)
        p = subprocess.Popen([BINARY, "start", "--block"], env=env)
        _children.append(p)
        return p

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def cleanup(self):
        self.kill()
        shutil.rmtree(self.tmp, ignore_errors=True)


class TcpProxy:
    """A cuttable TCP relay for link-loss tests: point an agent's
    MENTAT_DAEMON at proxy.address, then cut()/pause() to simulate a dropped
    link while both endpoints stay alive. resume() lets the agent's retry
    loop reconnect."""

    def __init__(self, target_addr):
        host, _, port = target_addr.rpartition(":")
        self.target = (host, int(port))
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.address = f"127.0.0.1:{self.listener.getsockname()[1]}"
        self.paused = False
        self.closed = False
        self.conns = []
        self.lock = threading.Lock()
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while not self.closed:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            with self.lock:
                refuse = self.paused or self.closed
            if refuse:
                client.close()
                continue
            try:
                server = socket.create_connection(self.target, timeout=5)
            except OSError:
                client.close()
                continue
            with self.lock:
                self.conns.append((client, server))
            for a, b in ((client, server), (server, client)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def _pipe(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        for s in (src, dst):
            try:
                s.close()
            except OSError:
                pass

    def pause(self):
        with self.lock:
            self.paused = True

    def resume(self):
        with self.lock:
            self.paused = False

    def cut(self):
        with self.lock:
            conns, self.conns = self.conns, []
        for pair in conns:
            for s in pair:
                try:
                    s.close()
                except OSError:
                    pass

    def close(self):
        self.closed = True
        self.cut()
        try:
            self.listener.close()
        except OSError:
            pass


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
