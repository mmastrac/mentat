#!/usr/bin/env python3
"""Mesh behavior across three daemons on one box: workers-first startup,
head election with hold-down, head failure and re-election, event
replication, a serving group surviving a head change, the reachability
matrix, and peer staleness (a wedged daemon going stale -> dead ->
recovered). Run with:

    python3 tests/test_multinode.py

The daemons run with shortened MENTAT_* windows (election hold-down 1.5 s
instead of the 5 s default, fast status pushes, ~3 s peer death) -- which is
itself the test that those knobs work.
"""

import json
import os
import signal
import socket
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Daemon, fresh_shim, free_port, run_ok  # noqa: E402

# An address no interface on this box carries, so every pair naming it must
# read as failed. 10.255.255.1 is not routable from loopback and cannot be
# bound either, so both directions fail fast.
BOGUS = "10.255.255.1"

# Short lifecycle windows for every daemon in the mesh. Status pushes must
# stay several times faster than the stale window (they are the heartbeat).
MESH_ENV = {
    "MENTAT_ELECTION_HOLD_DOWN_MS": "1500",
    "MENTAT_PEER_STATUS_INTERVAL_MS": "300",
    "MENTAT_PEER_STALE_AFTER_MS": "1200",
    "MENTAT_PEER_DEAD_AFTER_MS": "3000",
    "MENTAT_PROBE_INTERVAL_MS": "500",
    "MENTAT_PROBE_TIMEOUT_MS": "500",
    # Every daemon advertises the loopback address it actually answers on
    # plus one address nothing on this box carries. One box has no second
    # fabric to cable, so the unreachable half of the matrix is stated
    # rather than wired.
    "MENTAT_ANNOUNCE_ADDRS": f"127.0.0.1=lan,{BOGUS}=connectx+rdma",
}

state = {}


def wait_for(cond, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(0.3)
    raise TimeoutError(f"timed out waiting for {what}")


def t01_workers_first_then_head():
    # d1 (the eventual head, lowest node ip) starts LAST -- proving nothing
    # depends on head-first ordering anymore.
    p1 = free_port()
    d1_addr = f"127.0.0.1:{p1}"
    d2 = Daemon("127.0.0.2", peers=[d1_addr], env=MESH_ENV).wait_up()
    d3 = Daemon("127.0.0.3", peers=[d1_addr, d2.address], env=MESH_ENV).wait_up()
    state.update(d2=d2, d3=d3, d1_addr=d1_addr, p1=p1)

    # With only d2/d3 up, d2 (lower ip) must win after hold-down.
    d2_id = d2.status_json()["node_id"]
    wait_for(
        lambda: d3.status_json()["head_node_id"] == d2_id
        and d2.status_json()["head_node_id"] == d2_id,
        20,
        "d2 to be elected head of {d2,d3}",
    )


def t02_lowest_id_takes_over():
    d1 = Daemon("127.0.0.1", peers=[state["d2"].address, state["d3"].address],
                port=state["p1"], env=MESH_ENV).wait_up()
    state["d1"] = d1
    d1_id = d1.status_json()["node_id"]
    state["d1_id"] = d1_id
    wait_for(
        lambda: all(
            d.status_json()["head_node_id"] == d1_id
            for d in (d1, state["d2"], state["d3"])
        ),
        25,
        "all three daemons to agree d1 is head",
    )
    # Merged view: every daemon sees two live peers.
    for d in (d1, state["d2"], state["d3"]):
        peers = d.status_json()["peers"]
        assert sum(1 for p in peers.values() if p["alive"]) == 2, peers


def t02b_probes_cover_each_address_pair():
    """Reachability belongs to a (local address x peer address) pair rather
    than to a peer. Both fabrics in the real cluster are numbered out of the
    same subnet, so nothing but a bound-source connection can tell a cabled
    link from an address that merely looks local."""
    d1, d2 = state["d1"], state["d2"]
    d2_id = d2.status_json()["node_id"]

    def pairs():
        p = d1.status_json()["peers"].get(d2_id, {}).get("probes", {})
        # Both rows must have been tried, or the assertions below would pass
        # on a matrix that is merely incomplete.
        if set(p) != {"127.0.0.1", BOGUS}:
            return None
        if any(set(row) != {"127.0.0.1", BOGUS} for row in p.values()):
            return None
        return p

    p = wait_for(pairs, 20, "d1 to probe every address pair to d2")
    assert p["127.0.0.1"]["127.0.0.1"]["ok"], p
    assert p["127.0.0.1"]["127.0.0.1"]["last_ok_ms"] > 0, p
    # Three of the four pairs name an address this box does not carry.
    assert not p["127.0.0.1"][BOGUS]["ok"], p
    assert not p[BOGUS]["127.0.0.1"]["ok"], p
    assert not p[BOGUS][BOGUS]["ok"], p
    assert p["127.0.0.1"][BOGUS]["error"], "a failed pair must say why"
    # The tags ride along, so a consumer can ask which pairs were meant to
    # be a fabric before asking which ones work.
    tags = d1.status_json()["peers"][d2_id]["addr_tags"]
    assert tags[BOGUS] == ["connectx", "rdma"], tags


def t03_group_serves_on_non_head_daemon():
    d2 = state["d2"]
    d2.start_agent("m", gpus=2, container="cm")
    wait_for(
        lambda: d2.status_json("m")["groups"].get("m", {}).get("gpus_total", 0) == 2,
        15,
        "agent registration on d2",
    )
    ray = fresh_shim(d2.address, "m")
    from ray.util.placement_group import placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
    from fake_worker import FakeWorker

    ray.init()
    pg = placement_group([{"GPU": 1.0}, {"GPU": 1.0}])
    done, _ = ray.wait([pg.ready()], timeout=15)
    assert done
    actors = [
        ray.remote(FakeWorker)
        .options(
            name=f"w{r}",
            num_gpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=r
            ),
        )
        .remote(rank=r)
        for r in range(2)
    ]
    assert ray.get([a.echo.remote("pre") for a in actors]) == [
        ("echo", 0, "pre"),
        ("echo", 1, "pre"),
    ]
    state["ray"] = ray
    state["actors"] = actors
    state["run_refs"] = [a.block_forever.remote() for a in actors]


def t04_events_stream_carries_head_change():
    # Subscribe to d3's /events, kill d1, and expect node_leave + head_change
    # to arrive on a DIFFERENT daemon's stream (replication), while the group
    # on d2 keeps serving.
    d3 = state["d3"]
    s = socket.create_connection(("127.0.0.1", d3.http_port), timeout=5)
    s.sendall(
        b"GET /events HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
        b"Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        b"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    buf = b""
    while b"\r\n\r\n" not in buf:
        buf += s.recv(1024)
    _, rest = buf.split(b"\r\n\r\n", 1)

    data = bytearray(rest)

    def read_frame(timeout):
        s.settimeout(timeout)

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
        need(off + ln)
        payload = bytes(data[off : off + ln])
        del data[: off + ln]
        return opcode, payload

    opcode, payload = read_frame(5)
    assert json.loads(payload)["type"] == "snapshot"

    state["d1"].kill()

    deadline = time.time() + 30
    seen = set()
    while time.time() < deadline and not {"node_leave", "head_change"} <= seen:
        try:
            opcode, payload = read_frame(max(1.0, deadline - time.time()))
        except (TimeoutError, socket.timeout):
            continue
        if opcode != 1:
            continue
        evt = json.loads(payload)
        if evt.get("type") in ("node_leave", "head_change"):
            seen.add(evt["type"])
    assert {"node_leave", "head_change"} <= seen, seen
    s.close()

    d2_id = state["d2"].status_json()["node_id"]
    wait_for(
        lambda: state["d2"].status_json()["head_node_id"] == d2_id,
        15,
        "d2 to take over as head",
    )


def t05_group_survived_head_change():
    ray = state["ray"]
    # The serving group on d2 must be completely undisturbed by d1's death.
    # Assert it the way vLLM's monitor does: the run-style refs stay pending
    # (a dead worker would appear in `done` within one poll). No method calls
    # here -- actors are serial, exactly like real Ray, so anything issued
    # after block_forever/run() would queue forever by design.
    for _ in range(3):
        done, _ = ray.wait(state["run_refs"], num_returns=1, timeout=1)
        assert not done, "no worker may die from a head change"
    snap = state["d2"].status_json("m")
    actor_states = [a["state"] for a in snap["groups"]["m"]["actors"]]
    assert actor_states.count("running") == 2, actor_states


def t06_peer_staleness_and_recovery():
    # A wedged (SIGSTOPped) daemon stops pushing status without an EOF. The
    # staleness sweeper must declare it gone after MENTAT_PEER_DEAD_AFTER_MS,
    # the serving group must not care, and a SIGCONT must let the mesh heal.
    d2, d3 = state["d2"], state["d3"]
    d3_id = d3.status_json()["node_id"]

    os.kill(d3.proc.pid, signal.SIGSTOP)
    try:
        wait_for(
            lambda: not d2.status_json()["peers"][d3_id]["alive"],
            15,
            "d2 to declare the wedged d3 dead",
        )
        # A wedged peer daemon leaves the group on d2 alone.
        ray = state["ray"]
        done, _ = ray.wait(state["run_refs"], num_returns=1, timeout=1)
        assert not done, "no worker may die from a wedged peer daemon"
    finally:
        os.kill(d3.proc.pid, signal.SIGCONT)

    # d3's connector re-dials d2 and the link comes back.
    wait_for(
        lambda: d2.status_json()["peers"][d3_id]["alive"],
        20,
        "d2 and the resumed d3 to re-link",
    )


def main():
    tests = [
        t01_workers_first_then_head,
        t02_lowest_id_takes_over,
        t02b_probes_cover_each_address_pair,
        t03_group_serves_on_non_head_daemon,
        t04_events_stream_carries_head_change,
        t05_group_survived_head_change,
        t06_peer_staleness_and_recovery,
    ]
    try:
        for t in tests:
            run_ok(t, t.__name__)
        print(f"\nALL {len(tests)} TESTS PASSED")
    finally:
        try:
            state.get("ray") and state["ray"].shutdown()
        except Exception:
            pass
        for k in ("d1", "d2", "d3"):
            if k in state:
                state[k].cleanup()


if __name__ == "__main__":
    main()
