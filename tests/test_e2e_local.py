#!/usr/bin/env python3
"""End-to-end test of mentat on one box, no GPUs: daemon + two fake-GPU
agents + the real shim, exercising every audited vLLM behavior. Run with:

    python3 tests/test_e2e_local.py
"""

import json
import os
import signal
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Cluster, fresh_shim, run_ok  # noqa: E402

cluster = Cluster()
cluster.start_agent("g1", gpus=1, container="cA", node_ip="127.0.0.1")
cluster.start_agent("g1", gpus=1, container="cB", node_ip="127.0.0.2")
cluster.wait_group_gpus("g1", 2)

ray = fresh_shim(cluster.address, "g1")
from ray.util.placement_group import placement_group, placement_group_table  # noqa: E402
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy  # noqa: E402
from ray.exceptions import GetTimeoutError, RayActorError  # noqa: E402
from fake_worker import FakeWorker  # noqa: E402

state = {}


def t01_init_and_resources():
    ray.init()
    assert ray.is_initialized()
    res = ray.cluster_resources()
    assert res["GPU"] == 2.0, res
    nodes = ray.nodes()
    ids = {n["NodeID"] for n in nodes}
    assert len(ids) >= 2, nodes
    assert all(n["Alive"] for n in nodes)
    from ray._private.state import available_resources_per_node

    per_node = available_resources_per_node()
    assert sum(v.get("GPU", 0) for v in per_node.values()) == 2.0, per_node
    assert any(k.startswith("node:") for v in per_node.values() for k in v), per_node
    state["driver_node"] = ray.get_runtime_context().get_node_id()
    # Driver-side context: no accelerator, no actor id.
    assert ray.get_runtime_context().get_accelerator_ids()["GPU"] == []
    assert ray.get_runtime_context().get_actor_id() is None


def t02_placement_group():
    pg = placement_group([{"GPU": 1.0}, {"GPU": 1.0}], strategy="PACK")
    done, pending = ray.wait([pg.ready()], timeout=10)
    assert done and not pending
    assert ray.get(pg.ready(), timeout=0) is None
    table = placement_group_table(pg)
    assert set(table["bundles"].keys()) == {0, 1}, table
    assert all(isinstance(k, int) for k in table["bundles_to_node_id"]), table
    assert table["state"] == "CREATED"
    assert table["bundles_to_node_id"][0] == state["driver_node"], (
        "bundle 0 must land on the driver's node",
        table,
        state["driver_node"],
    )
    assert table["bundles_to_node_id"][1] != table["bundles_to_node_id"][0]
    state["pg"] = pg


def t03_spawn_actors():
    pg = state["pg"]
    actors = []
    for rank in range(2):
        a = (
            ray.remote(FakeWorker)
            .options(
                name=f"vllm_Worker_test_TP{rank}",
                num_cpus=0,
                num_gpus=1,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_bundle_index=rank,
                ),
                runtime_env={
                    "env_vars": {"RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES": "1"}
                },
                max_restarts=0,  # unknown-ish option: accepted, ignored
            )
            .remote(rank=rank)
        )
        actors.append(a)
    state["actors"] = actors
    results = ray.get([a.echo.remote("hi") for a in actors])
    assert results == [("echo", 0, "hi"), ("echo", 1, "hi")], results
    state["pids"] = ray.get([a.pid.remote() for a in actors])


def t04_actor_env_and_context():
    a0 = state["actors"][0]
    env = ray.get(a0.env_dump.remote())
    assert env.get("MENTAT_GPU_IDS") == "0", env.get("MENTAT_GPU_IDS")
    assert env.get("RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES") == "1"
    assert "CUDA_VISIBLE_DEVICES" not in env, "mentat must never set CUDA_VISIBLE_DEVICES"
    ctx = ray.get(a0.runtime_ctx.remote())
    assert ctx["gpus"] == ["0"], ctx
    assert ctx["actor_id"], ctx
    assert ctx["is_initialized"] is True
    assert ctx["node_id"] == state["driver_node"], ctx


def t05_get_timeout_semantics():
    a1 = state["actors"][1]
    ref = a1.sleep.remote(2)
    try:
        ray.get(ref, timeout=0)
        raise AssertionError("timeout=0 must raise on a pending ref")
    except GetTimeoutError:
        pass
    try:
        ray.get(ref, timeout=0.2)
        raise AssertionError("short timeout must raise")
    except GetTimeoutError:
        pass
    assert ray.get(ref, timeout=30) == 2


def t06_actor_exception_propagates():
    a0 = state["actors"][0]
    try:
        ray.get(a0.raise_err.remote("boom-123"))
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "boom-123" in str(e)


def t07_run_ref_sentinel_and_sigkill():
    actors = state["actors"]
    run_refs = [a.block_forever.remote() for a in actors]
    ref_to_rank = {r: i for i, r in enumerate(run_refs)}
    done, pending = ray.wait(run_refs, num_returns=1, timeout=1)
    assert not done and len(pending) == 2

    os.kill(state["pids"][0], signal.SIGKILL)
    done, pending = ray.wait(run_refs, num_returns=1, timeout=10)
    assert len(done) == 1, (done, pending)
    # The exact objects come back and still work as dict keys.
    assert ref_to_rank[done[0]] == 0
    try:
        ray.get(done[0])
        raise AssertionError("dead actor's ref must raise")
    except RayActorError as e:
        assert "signal=9" in str(e), str(e)


def t08_ctor_failure_reports():
    pg = state["pg"]
    bad = (
        ray.remote(FakeWorker)
        .options(
            name="vllm_Worker_test_bad",
            num_gpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0
            ),
        )
        .remote(fail_ctor=True)
    )
    try:
        ray.get(bad.echo.remote("x"), timeout=30)
        raise AssertionError("ctor failure must surface")
    except RayActorError as e:
        assert "ctor failed on purpose" in str(e), str(e)


def t09_ray_kill():
    a1 = state["actors"][1]
    marker = a1.block_forever.remote()
    ray.kill(a1)
    done, _ = ray.wait([marker], timeout=10)
    assert done, "kill must resolve outstanding refs"


def t10_duplicate_driver_rejected():
    from ray import _client

    try:
        _client.Connection(
            cluster.address, "another-client", "g1", session=True, kind="driver"
        )
        raise AssertionError("second driver session for g1 must be rejected")
    except _client.MentatError as e:
        assert "already has an active driver" in str(e)


def t11_session_eof_reaps_orphans():
    cluster.start_agent("g2", gpus=1, container="cG2", node_ip="127.0.0.1")
    cluster.wait_group_gpus("g2", 1)
    script = """
import os, sys
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from fake_worker import FakeWorker
ray.init()
pg = placement_group([{"GPU": 1.0}])
ray.wait([pg.ready()], timeout=10)
a = ray.remote(FakeWorker).options(
    name="orphan", num_gpus=1,
    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg,
                                                         placement_group_bundle_index=0),
).remote()
# pid BEFORE block_forever: actor calls are serial, so anything issued after
# a never-returning method would queue forever (same as real ray).
pid = ray.get(a.pid.remote())
a.block_forever.remote()
print("PID", pid, flush=True)
os._exit(0)  # die without any cleanup, like a crashed driver
"""
    env = {
        **os.environ,
        "RAY_ADDRESS": cluster.address,
        "MENTAT_GROUP": "g2",
        "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE]),
    }
    out = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=60
    )
    pid_lines = [l for l in out.stdout.splitlines() if l.startswith("PID ")]
    assert pid_lines, (out.stdout, out.stderr)
    orphan_pid = int(pid_lines[0].split()[1])
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            os.kill(orphan_pid, 0)
            time.sleep(0.2)
        except OSError:
            return  # reaped
    raise AssertionError(f"orphan actor pid {orphan_pid} survived driver death")


def t12_cli_status_and_entrypoint_pipeline():
    r = cluster.cli("status", "--address", cluster.address, "--group", "g1")
    # The literal pipeline from glm53/entrypoint.sh.
    pipeline = (
        "grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1"
    )
    have = subprocess.run(
        ["bash", "-c", pipeline],
        input=r.stdout,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert have == "2", (have, r.stdout)
    # ray-CLI compatibility details.
    r2 = cluster.cli("status", "--address", cluster.address, "--group", "nope")
    assert r2.returncode == 0  # reachable head = exit 0, like `ray status`


def t13_metrics():
    m = cluster.metrics()
    assert 'mentat_gpus_total{group="g1"} 2' in m, m
    assert 'vendor="nvidia"' in m, m
    assert "mentat_actor_exits_total" in m
    sig = [l for l in m.splitlines() if l.startswith('mentat_actor_exits_total{kind="signal"}')]
    assert sig and int(sig[0].split()[-1]) >= 1, sig


def t14_websocket_events():
    s = socket.create_connection(("127.0.0.1", cluster.http_port), timeout=5)
    s.sendall(
        b"GET /events HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
        b"Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        b"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    # Read the 101 response headers.
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += s.recv(1024)
    head, rest = buf.split(b"\r\n\r\n", 1)
    assert b"101" in head.split(b"\r\n")[0], head
    assert b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" in head, head

    def read_ws_frame(pre):
        data = bytearray(pre)

        def need(n):
            while len(data) < n:
                chunk = s.recv(4096)
                if not chunk:
                    raise ConnectionError("ws closed")
                data.extend(chunk)

        need(2)
        opcode = data[0] & 0x0F
        ln = data[1] & 0x7F
        off = 2
        if ln == 126:
            need(4)
            ln = int.from_bytes(data[2:4], "big")
            off = 4
        elif ln == 127:
            need(10)
            ln = int.from_bytes(data[2:10], "big")
            off = 10
        need(off + ln)
        payload = bytes(data[off : off + ln])
        return opcode, payload, bytes(data[off + ln :])

    opcode, payload, rest = read_ws_frame(rest)
    snap = json.loads(payload)
    assert snap["type"] == "snapshot", snap
    assert "g1" in snap["data"]["groups"], snap

    # Trigger an event and expect it on the stream.
    placement_group([{"GPU": 0.0}])
    deadline = time.time() + 10
    seen = None
    s.settimeout(deadline - time.time())
    while time.time() < deadline:
        opcode, payload, rest = read_ws_frame(rest)
        if opcode != 1:
            continue
        evt = json.loads(payload)
        if evt.get("type") == "pg_created":
            seen = evt
            break
    assert seen, "pg_created never arrived on /events"
    s.close()


def _wait_agent(c, group, pred, what, timeout=10):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        snap = c.status_json(group)
        agents = snap["groups"].get(group, {}).get("agents", [])
        if agents:
            last = agents[0]
            if pred(last):
                return last
        time.sleep(0.05)
    raise TimeoutError(f"agent never became {what}: {last}")


def t15_pg_pending_timeout():
    # A placement group with no agents to satisfy it must fail loudly after
    # MENTAT_PG_PENDING_TIMEOUT_MS instead of pending forever.
    c = tl.Cluster(daemon_env={"MENTAT_PG_PENDING_TIMEOUT_MS": "1500"})
    r = fresh_shim(c.address, "gt")
    try:
        from ray.exceptions import RayActorError as RAE
        from ray.util.placement_group import placement_group as make_pg

        r.init()
        pg = make_pg([{"GPU": 1.0}])
        done, pending = r.wait([pg.ready()], timeout=0.5)
        assert not done, "pg with no agents must still be pending before the timeout"
        # The ready ref must complete (as a failure) once the timeout fires.
        done, pending = r.wait([pg.ready()], timeout=10)
        assert done and not pending, (done, pending)
        try:
            r.get(pg.ready())
            raise AssertionError("timed-out pg's ready ref must raise")
        except RAE as e:
            assert "PENDING after" in str(e), str(e)
    finally:
        r.shutdown()
        c.cleanup()


def t16_agent_link_degrade_and_recover():
    # An agent link blip shorter than MENTAT_AGENT_DEAD_AFTER_MS must not kill
    # anything: the agent goes degraded, calls issued during the outage are
    # held, and everything drains when the agent reconnects.
    c = tl.Cluster(
        daemon_env={
            "MENTAT_AGENT_DEGRADED_AFTER_MS": "400",
            "MENTAT_AGENT_DEAD_AFTER_MS": "60000",
        }
    )
    proxy = tl.TcpProxy(c.address)
    r = fresh_shim(c.address, "gdeg")
    try:
        from ray.util.placement_group import placement_group as make_pg
        from ray.util.scheduling_strategies import (
            PlacementGroupSchedulingStrategy as PGS,
        )

        c.start_agent("gdeg", gpus=2, container="cDeg", daemon_addr=proxy.address)
        c.wait_group_gpus("gdeg", 2)
        r.init()
        pg = make_pg([{"GPU": 1.0}, {"GPU": 1.0}])
        assert r.wait([pg.ready()], timeout=10)[0]
        a_call, a_run = [
            r.remote(FakeWorker)
            .options(
                name=f"w{i}",
                num_gpus=1,
                scheduling_strategy=PGS(placement_group=pg, placement_group_bundle_index=i),
            )
            .remote(rank=i)
            for i in range(2)
        ]
        assert r.get(a_call.echo.remote("pre"), timeout=15) == ("echo", 0, "pre")
        run_ref = a_run.block_forever.remote()
        # Let the proxy relay the call before cutting: cut() discards bytes
        # its pipe threads have not pumped yet, and a run call eaten by the
        # proxy is correctly declared lost at reconnect -- a different path
        # than the quiet-blip one this test pins.
        time.sleep(0.3)

        proxy.pause()
        proxy.cut()
        _wait_agent(c, "gdeg", lambda a: not a["alive"], "lost")
        _wait_agent(c, "gdeg", lambda a: a["degraded"], "degraded")
        # The run() sentinel must NOT resolve inside the degrade window.
        done, _ = r.wait([run_ref], timeout=0.3)
        assert not done, "actors must not die during the degrade window"
        # A call issued during the outage is held.
        held = a_call.echo.remote("during")
        done, _ = r.wait([held], timeout=0.3)
        assert not done, "held call must stay pending while the link is down"

        proxy.resume()
        _wait_agent(c, "gdeg", lambda a: a["alive"] and not a["degraded"], "recovered",
                    timeout=20)
        assert r.get(held, timeout=20) == ("echo", 0, "during")
        done, _ = r.wait([run_ref], timeout=0.3)
        assert not done, "run() sentinel must survive the blip"
    finally:
        r.shutdown()
        proxy.close()
        c.cleanup()


def t17_agent_link_giveup():
    # Past MENTAT_AGENT_DEAD_AFTER_MS the daemon gives up: actors are marked
    # dead, and -- the hard rule -- the run() sentinel ref completes so the
    # driver's monitor sees it and restarts.
    c = tl.Cluster(
        daemon_env={
            "MENTAT_AGENT_DEGRADED_AFTER_MS": "400",
            "MENTAT_AGENT_DEAD_AFTER_MS": "2000",
        }
    )
    proxy = tl.TcpProxy(c.address)
    r = fresh_shim(c.address, "ggone")
    try:
        from ray.exceptions import RayActorError as RAE
        from ray.util.placement_group import placement_group as make_pg
        from ray.util.scheduling_strategies import (
            PlacementGroupSchedulingStrategy as PGS,
        )

        c.start_agent("ggone", gpus=1, container="cGone", daemon_addr=proxy.address)
        c.wait_group_gpus("ggone", 1)
        r.init()
        pg = make_pg([{"GPU": 1.0}])
        assert r.wait([pg.ready()], timeout=10)[0]
        a = (
            r.remote(FakeWorker)
            .options(
                name="w0",
                num_gpus=1,
                scheduling_strategy=PGS(placement_group=pg, placement_group_bundle_index=0),
            )
            .remote(rank=0)
        )
        assert r.get(a.echo.remote("pre"), timeout=15) == ("echo", 0, "pre")
        run_ref = a.block_forever.remote()

        proxy.pause()
        proxy.cut()
        done, _ = r.wait([run_ref], timeout=15)
        assert done == [run_ref], "run() ref must complete when the daemon gives up"
        try:
            r.get(run_ref)
            raise AssertionError("dead actor's ref must raise")
        except RAE as e:
            assert "agent link lost" in str(e), str(e)
        snap = c.status_json("ggone")
        states = [a["state"] for a in snap["groups"]["ggone"]["actors"]]
        assert all(s.startswith("dead") for s in states), states
    finally:
        r.shutdown()
        proxy.close()
        c.cleanup()


def main():
    tests = [
        t01_init_and_resources,
        t02_placement_group,
        t03_spawn_actors,
        t04_actor_env_and_context,
        t05_get_timeout_semantics,
        t06_actor_exception_propagates,
        t07_run_ref_sentinel_and_sigkill,
        t08_ctor_failure_reports,
        t09_ray_kill,
        t10_duplicate_driver_rejected,
        t11_session_eof_reaps_orphans,
        t12_cli_status_and_entrypoint_pipeline,
        t13_metrics,
        t14_websocket_events,
        # These three run against their own short-window clusters and rebind
        # the shim, so they stay after everything that uses the main cluster.
        t15_pg_pending_timeout,
        t16_agent_link_degrade_and_recover,
        t17_agent_link_giveup,
    ]
    try:
        for t in tests:
            run_ok(t, t.__name__)
        print(f"\nALL {len(tests)} TESTS PASSED")
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass
        cluster.cleanup()


if __name__ == "__main__":
    main()
