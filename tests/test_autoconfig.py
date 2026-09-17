#!/usr/bin/env python3
"""Assembly with no address anywhere in the configuration.

A container must never need a hardcoded address. The daemon files each agent
and driver under the box that owns the connection's source address, and hands
every actor its node's identity, so the only thing a model image sets is which
group it belongs to.

This suite runs on the real default ports, since the defaults are the thing
being tested. It skips if something else already holds one.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mentat_testlib as tl  # noqa: E402
from mentat_testlib import run_ok  # noqa: E402

# Every port a daemon binds with no flags. A container reaching
# 127.0.0.1:6379 is reaching its own box's daemon, and 6382 is where the
# announcements it hears arrive.
#
# A real cluster may share the LAN. MENTAT_UNIVERSE keeps the two apart: a
# receiver checks `universe` before the key, so each side drops the other's
# announcements silently.
CONTROL, HTTP, ANNOUNCE = 6379, 6380, 6382

#: Every address variable a deployment could set. The point of the suite is
#: that none of them is in the environment.
ADDRESS_VARS = [
    "MENTAT_NODE_IP",
    "MENTAT_DAEMON",
    "MENTAT_DAEMONS",
    "MENTAT_PEERS",
    "MENTAT_GCS_ADDRESS",
    "MENTAT_ANNOUNCE_ADDR",
    "MENTAT_ANNOUNCE_ADDRS",
    "MENTAT_FABRIC_IP",
    "RAY_ADDRESS",
]

state = {}


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def port_free(p):
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", p))
        return True
    except OSError:
        return False
    finally:
        s.close()


def clean_env(**extra):
    """The process environment with every address variable removed."""
    env = {**os.environ, **extra}
    for v in ADDRESS_VARS:
        env.pop(v, None)
    return env


def setup():
    for p in (CONTROL, HTTP, ANNOUNCE):
        if not port_free(p):
            print(f"port {p} is in use; skipping (these are the defaults under test)")
            raise SystemExit(0)
    tl.build_binary()
    tmp = tempfile.mkdtemp(prefix="mentat-auto-")
    # No --node-ip, no --port, no --peers: every flag left at its default.
    log_path = os.path.join(tmp, "daemon.log")
    log_file = open(log_path, "w")
    daemon = subprocess.Popen(
        [tl.BINARY, "daemon", "--head-json", os.path.join(tmp, "head.json")],
        env=clean_env(
            MENTAT_SECRET=tl.TEST_SECRET,
            MENTAT_UNIVERSE=tl.TEST_UNIVERSE,
        ),
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    tl._children.append(daemon)
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", CONTROL), timeout=1).close()
            break
        except OSError:
            time.sleep(0.05)
    else:
        raise TimeoutError("daemon never bound the default port")
    def daemon_log():
        with open(log_path) as f:
            return f.read().splitlines()

    state.update(tmp=tmp, daemon=daemon, daemon_log=daemon_log)


def status(group=None):
    import urllib.request

    url = f"http://127.0.0.1:{HTTP}/status" + (f"?group={group}" if group else "")
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)


def t00_the_signing_vector_matches_the_rust_one():
    """The canonical form, pinned on both sides. secret.rs asserts the same
    payload and signature, so a change to either signer fails here.

    Nested objects sort, and the non-ASCII key stays raw UTF-8.
    """
    env = json.loads(
        tl.sign_announcement({"universe": "k\u00fc", "b": [2, {"d": 4, "c": 3}], "a": 1}, "k")
    )
    assert env["sig"] == (
        "ec381f4b20bc7eb6b1f18c7f15b06a5c7c60aca04b4ffcba2c1910ac6262ed39"
    ), env["sig"]


def t01_a_daemon_with_no_flags_names_itself():
    """`mentatd daemon` with no arguments settles on an identity and a head.

    MENTAT_NODE_IP is what a deployment would otherwise have to set, so the
    route to the world has to stand in for it.
    """
    deadline = time.time() + 20
    while time.time() < deadline:
        snap = status()
        if snap["head_node_id"]:
            break
        time.sleep(0.1)
    else:
        raise TimeoutError(f"no head after the hold-down: {status()}")
    assert snap["node_ip"], snap
    assert snap["node_id"], snap
    # A lone daemon elects itself, so the identity it derived is the head.
    assert snap["head_node_id"] == snap["node_id"], snap
    assert snap["control_addr"].endswith(f":{CONTROL}"), snap
    # Derived from the route to the world, which is what MENTAT_NODE_IP
    # would otherwise have to supply.
    assert snap["node_ip"] != "127.0.0.1", snap
    assert snap["addrs"], snap


def t02_an_agent_with_no_address_registers():
    """A container sets its group and nothing else.

    The daemon files it under the box the connection came from, which is what
    replaces MENTAT_NODE_IP in the model image.
    """
    env = clean_env(
        MENTAT_GROUP="auto",
        MENTAT_GPUS="2",
        MENTAT_MACHINE_PROBE=tl.MACHINE_PROBE,
        MENTAT_SECRET=tl.TEST_SECRET,
        CONTAINER_NAME="cauto",
        MENTAT_SOCK_DIR=state["tmp"],
        MENTAT_PYTHON=sys.executable,
        PYTHONPATH=os.pathsep.join([tl.PYTHON_PKG, HERE]),
    )
    env.pop("CUDA_VISIBLE_DEVICES", None)
    agent = subprocess.Popen([tl.BINARY, "start", "--block"], env=env)
    tl._children.append(agent)
    state["agent"] = agent

    deadline = time.time() + 20
    while time.time() < deadline:
        g = status("auto").get("groups", {}).get("auto")
        if g and g.get("gpus_total", 0) >= 2:
            break
        time.sleep(0.1)
    else:
        raise TimeoutError(f"agent never registered: {status('auto')}")

    snap = status("auto")
    agents = list(snap["groups"]["auto"]["agents"].values())
    assert len(agents) == 1, agents
    a = agents[0]
    # The daemon decided both of these. Neither was in the environment.
    assert a["node_ip"] == snap["node_ip"], (a, snap["node_ip"])
    assert a["node_id"] == snap["node_id"], (a, snap["node_id"])
    assert len(a["machine"]["gpus"]) == 2, a


def t03_a_driver_with_no_address_places_and_calls():
    """The shim reaches the daemon, places a group and runs a method.

    RAY_ADDRESS is unset, so `ray.init` falls through to head.json and then to
    the default port. The actor it spawns is given its node's identity.
    """
    env = clean_env(
        MENTAT_GROUP="auto",
        MENTAT_SECRET=tl.TEST_SECRET,
        PYTHONPATH=os.pathsep.join([tl.PYTHON_PKG, HERE]),
    )
    script = """
import json, ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from fake_worker import FakeWorker

ray.init()
pg = placement_group([{"GPU": 1}])
ray.get(pg.ready(), timeout=60)
a = ray.remote(FakeWorker).options(
    num_gpus=1,
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg, placement_group_bundle_index=0
    ),
).remote(rank=0)
ctx = ray.get(a.runtime_ctx.remote(), timeout=60)
env = ray.get(a.env_dump.remote(), timeout=60)
print(json.dumps({
    "gpus": ctx["gpus"],
    "node_id": ctx["node_id"],
    "actor_node_ip": env.get("MENTAT_NODE_IP", ""),
    "actor_gcs": env.get("MENTAT_GCS_ADDRESS", ""),
}))
"""
    driver = subprocess.run(
        [sys.executable, "-c", script],
        env=env, capture_output=True, text=True, timeout=180,
    )
    assert driver.returncode == 0, driver.stdout + driver.stderr
    out = json.loads(driver.stdout.strip().splitlines()[-1])
    assert out["gpus"], out
    snap = status()
    assert out["node_id"] == snap["node_id"], (out, snap["node_id"])
    # The daemon hands the actor its node's identity. Neither variable was in
    # the driver's environment, so both came off the wire.
    assert out["actor_node_ip"] == snap["node_ip"], out
    assert out["actor_gcs"].endswith(f":{CONTROL}"), out


def t04_two_daemons_mesh_with_no_seed_list():
    """MENTAT_PEERS empty on both. Each finds the other by announcement.

    A node id is the hash of the node's address, so two daemons on one box
    need two addresses to be two nodes. MENTAT_TEST_NET supplies them and
    maps each onto a loopback port, the same way the topology suite builds a
    cluster that a single box has no cabling for.
    """
    from test_topology import TestNet

    tmp = tempfile.mkdtemp(prefix="mentat-mesh-")
    net = TestNet(os.path.join(tmp, "net.json"))
    boxes = {"a": "192.168.9.1", "b": "192.168.9.2"}
    ports = {n: (tl.free_port(), tl.free_port(), free_udp_port()) for n in boxes}
    for name, ip in boxes.items():
        net.addrs[ip] = f"127.0.0.1:{ports[name][0]}"
    net.write()

    procs = {}
    for name, ip in boxes.items():
        ctl, http, udp = ports[name]
        other = next(u for n, (_, _, u) in ports.items() if n != name)
        env = clean_env(
            MENTAT_SECRET=tl.TEST_SECRET,
            MENTAT_UNIVERSE=tl.TEST_UNIVERSE,
            MENTAT_ANNOUNCE_PORT=str(udp),
            MENTAT_ANNOUNCE_INTERVAL_S="1",
            MENTAT_ELECTION_HOLD_DOWN_MS="500",
            MENTAT_TEST_NET=net.path,
        )
        # Set after the strip, which clears every address variable. Loopback
        # does not broadcast, so the peer's listener is addressed directly.
        # The datagram is the one a broadcast would deliver.
        env["MENTAT_ANNOUNCE_ADDR"] = f"127.0.0.1:{other}"
        p = subprocess.Popen(
            [
                tl.BINARY, "daemon",
                "--port", str(ctl),
                "--http-port", str(http),
                "--node-ip", ip,
                "--head-json", os.path.join(tmp, f"head-{name}.json"),
            ],
            env=env,
        )
        tl._children.append(p)
        procs[name] = p

    def peers_of(http_port):
        import urllib.request

        with urllib.request.urlopen(
            f"http://127.0.0.1:{http_port}/status", timeout=3
        ) as r:
            snap = json.load(r)
        return {k for k, v in snap.get("peers", {}).items() if v.get("alive")}

    try:
        deadline = time.time() + 40
        while time.time() < deadline:
            try:
                if peers_of(ports["a"][1]) and peers_of(ports["b"][1]):
                    break
            except OSError:
                pass
            time.sleep(0.5)
        else:
            raise TimeoutError("daemons never found each other without a seed list")
        for name, (_, http, _) in ports.items():
            assert peers_of(http), f"{name} sees no peer"
    finally:
        # These bind ports the later tests want, so they go whatever happened.
        for p in procs.values():
            p.terminate()


def t05_a_foreign_universe_and_a_wrong_key_are_both_refused():
    """The two checks a datagram passes before the listener acts on it.

    A foreign universe belongs to another cluster on the same LAN, which is
    routine, so the drop is silent. A matching universe with a bad signature
    is an intruder, so the drop is logged.
    """
    def signed(universe, key, node):
        return tl.sign_announcement(
            {
                "proto": "0.99",
                "node_id": node,
                "universe": universe,
                "control": "10.9.9.9:6379",
                "http": "10.9.9.9:6380",
                "addrs": ["10.9.9.9"],
                "addr_tags": {},
                "boot_id": "deadbeefdeadbeef",
                "seq": 1,
                "t": int(time.time()),
            },
            key,
        )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    before = len(state["daemon_log"]())
    # Another cluster. Dropped before the key is checked, with no log line.
    sock.sendto(signed("someone-else", tl.TEST_SECRET, "n-other"), ("127.0.0.1", ANNOUNCE))
    # This cluster, wrong key. Refused with one log line naming the source.
    sock.sendto(signed(tl.TEST_UNIVERSE, "not-the-key", "n-intruder"), ("127.0.0.1", ANNOUNCE))
    sock.close()
    time.sleep(2)

    # Neither one becomes a peer.
    peers = status().get("peers", {})
    assert "n-other" not in peers and "n-intruder" not in peers, peers
    added = state["daemon_log"]()[before:]
    rejected = [l for l in added if "announce_rejected" in l]
    assert len(rejected) == 1, added
    assert "someone-else" not in "".join(added), added


def t06_the_router_with_no_seed_list_finds_the_daemon():
    """MENTAT_DAEMONS empty. The only way in is the UDP announcement."""
    tl.build_serve()
    port = tl.free_port()
    env = clean_env(
        SERVE_PORT=str(port),
        POLL_INTERVAL_S="1",
        PROBE_INTERVAL_S="0.5",
        MENTAT_SECRET=tl.TEST_SECRET,
        MENTAT_UNIVERSE=tl.TEST_UNIVERSE,
        ALLOWED_SOURCES="local",
    )
    env["MENTAT_DAEMONS"] = ""
    p = subprocess.Popen([tl.SERVE_BINARY], env=env)
    tl._children.append(p)
    import urllib.request

    deadline = time.time() + 40
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/status.json", timeout=3
            ) as r:
                last = json.load(r)
            if any(d.get("connected") for d in last.get("daemons", {}).values()):
                return
        except OSError:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"router never discovered the daemon: {last}")


def main():
    setup()
    for t in [v for k, v in sorted(globals().items()) if k.startswith("t0")]:
        run_ok(t, t.__name__)
    print("\nautoconfig: all ok")


if __name__ == "__main__":
    main()
