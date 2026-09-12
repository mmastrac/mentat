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

# The ports a daemon binds with no flags, and the announcement port beside
# them. A container reaching 127.0.0.1:6379 is reaching its own box's daemon.
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
    daemon = subprocess.Popen(
        [tl.BINARY, "daemon", "--head-json", os.path.join(tmp, "head.json")],
        env=clean_env(MENTAT_SECRET=tl.TEST_SECRET),
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
    state.update(tmp=tmp, daemon=daemon)


def status(group=None):
    import urllib.request

    url = f"http://127.0.0.1:{HTTP}/status" + (f"?group={group}" if group else "")
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)


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


def t04_the_router_with_no_seed_list_finds_the_daemon():
    """MENTAT_DAEMONS empty. The only way in is the UDP announcement."""
    tl.build_serve()
    port = tl.free_port()
    env = clean_env(
        SERVE_PORT=str(port),
        POLL_INTERVAL_S="1",
        PROBE_INTERVAL_S="0.5",
        MENTAT_SECRET=tl.TEST_SECRET,
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
