#!/usr/bin/env python3
"""Fabric islands: reachability probes turned into placement.

A three-daemon mesh whose nodes all tag their one address `rdma`, so probes
confirm a fabric and the island covers all three. Then: a two-bundle group
lands inside it and its ranks are handed the fabric address, a group whose
free GPUs straddle the island boundary stays PENDING and says so, and the
hold-down keeps the island still across a link that drops. Run with:

    python3 tests/test_fabric.py

One box has one loopback address, so the fabric here is loopback. That is
enough: nothing in the derivation cares which addresses they are, only that
both ends are tagged and a probe between them answered.
"""

import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Daemon, fresh_shim, run_ok  # noqa: E402

FABRIC_ENV = {
    "MENTAT_PEER_STATUS_INTERVAL_MS": "200",
    "MENTAT_PEER_STALE_AFTER_MS": "1000",
    "MENTAT_PEER_DEAD_AFTER_MS": "1500",
    "MENTAT_ELECTION_HOLD_DOWN_MS": "500",
    "MENTAT_PROBE_INTERVAL_MS": "300",
    "MENTAT_PROBE_TIMEOUT_MS": "500",
    "MENTAT_ISLAND_HOLD_DOWN_MS": "500",
    # The one address every daemon here answers on, declared a fabric. The
    # probes are what decide whether the declaration holds.
    "MENTAT_ANNOUNCE_ADDRS": "127.0.0.1=connectx+rdma",
}

state = {}


def wait_for(cond, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {what}")


def t01_probes_confirm_the_tagged_fabric():
    d1 = Daemon("127.0.0.1", env=FABRIC_ENV).wait_up()
    d2 = Daemon("127.0.0.2", peers=[d1.address], env=FABRIC_ENV).wait_up()
    d3 = Daemon("127.0.0.3", peers=[d1.address], env=FABRIC_ENV).wait_up()
    state.update(d1=d1, d2=d2, d3=d3)
    ids = {d.status_json()["node_id"] for d in (d1, d2, d3)}

    def island():
        isl = d1.status_json()["islands"]
        return isl[0] if len(isl) == 1 and set(isl[0]["nodes"]) == ids else None

    i = wait_for(island, 25, "d1 to derive one island covering all three nodes")
    # Every member carries the address its probes actually answered on. That
    # is the address a rank binds NCCL to.
    assert set(i["addrs"].values()) == {"127.0.0.1"}, i


def t02_a_two_bundle_group_lands_inside_the_island():
    """The failure this prevents: two ranks placed on boxes with no fabric
    between them rendezvous over NCCL and hang, which reads as a model bug."""
    d1 = state["d1"]
    # Both agents talk to d1 -- one daemon owns a group -- but claim the
    # node identities of two different boxes, which is the real shape.
    for ip in ("127.0.0.1", "127.0.0.2"):
        d1.start_agent("tp2", gpus=1, container=f"c{ip[-1]}",
                       env_extra={"MENTAT_NODE_IP": ip})
    wait_for(
        lambda: d1.status_json("tp2")["groups"].get("tp2", {}).get("gpus_total") == 2,
        20,
        "both tp2 agents to register",
    )

    ray = fresh_shim(d1.address, "tp2")
    from ray.util.placement_group import placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
    from fake_worker import FakeWorker

    ray.init()
    pg = placement_group([{"GPU": 1.0}, {"GPU": 1.0}])
    done, _ = ray.wait([pg.ready()], timeout=20)
    assert done, "a group that fits one fabric must place"
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
    state["ray"] = ray
    state["actors"] = actors
    # Each rank is told the address that carries the fabric it was placed
    # on, so nothing has to be hand-matched per node and per container.
    for a in actors:
        env = ray.get(a.env_dump.remote())
        assert env.get("MENTAT_FABRIC_IP") == "127.0.0.1", env.get("MENTAT_FABRIC_IP")
    pgs = d1.status_json("tp2")["groups"]["tp2"]["placement_groups"]
    assert [p["island_nodes"] for p in pgs] == [3], pgs


def t03_a_group_straddling_the_boundary_waits_and_says_why():
    # One free GPU inside the island and one on a node no daemon claims, so
    # nothing can hold both bundles. Spilling onto the LAN is never the
    # answer, so the group waits.
    d1 = state["d1"]
    for ip in ("127.0.0.1", "127.0.0.9"):
        d1.start_agent("split", gpus=1, container=f"s{ip[-1]}",
                       env_extra={"MENTAT_NODE_IP": ip})
    wait_for(
        lambda: d1.status_json("split")["groups"].get("split", {}).get("gpus_total") == 2,
        20,
        "both split agents to register",
    )

    import subprocess

    driver = """
import os, sys, time
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group
ray.init()
pg = placement_group([{"GPU": 1.0}, {"GPU": 1.0}])
print("PG_REQUESTED", flush=True)
time.sleep(3600)
"""
    p = subprocess.Popen(
        [sys.executable, "-c", driver],
        env={**os.environ, "RAY_ADDRESS": d1.address, "MENTAT_GROUP": "split",
             "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE])},
        stdout=subprocess.PIPE, text=True, bufsize=1,
    )
    tl._children.append(p)
    state["split_driver"] = p
    assert "PG_REQUESTED" in p.stdout.readline()

    def pending_reason():
        pgs = d1.status_json("split")["groups"]["split"]["placement_groups"]
        for pg in pgs:
            if pg["state"] == "PENDING" and pg["pending_reason"]:
                return pg["pending_reason"]
        return None

    why = wait_for(pending_reason, 20, "the split group to report why it cannot place")
    # The message has to name the constraint. "not enough GPUs" is what this
    # used to say, and it is wrong here: there are exactly enough.
    assert "one rdma fabric" in why, why
    assert "'split'" in why, why


def t03b_an_untagged_group_places_beside_a_tagged_one():
    """Opting in is per group. Tagging one pair first is the cautious
    rollout order, and it must not stop a deployment on the untagged pair
    from booting -- which is what a cluster-wide gate would do, since the
    tagged pair alone makes the cluster have an island."""
    d1 = state["d1"]
    # Neither node carries an rdma tag: no daemon claims 127.0.0.8 or
    # 127.0.0.9, so nothing put them on a fabric.
    for ip in ("127.0.0.8", "127.0.0.9"):
        d1.start_agent("untagged", gpus=1, container=f"u{ip[-1]}",
                       env_extra={"MENTAT_NODE_IP": ip})
    wait_for(
        lambda: d1.status_json("untagged")["groups"].get("untagged", {}).get("gpus_total") == 2,
        20,
        "both untagged agents to register",
    )
    # The cluster does have an island, from t01.
    assert d1.status_json()["islands"], "this test is vacuous without one"

    import subprocess

    driver = """
import os, sys, time
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group
ray.init()
pg = placement_group([{"GPU": 1.0}, {"GPU": 1.0}])
done, _ = ray.wait([pg.ready()], timeout=30)
assert done, "an untagged group must place across untagged nodes"
print("PLACED", flush=True)
time.sleep(3600)
"""
    p = subprocess.Popen(
        [sys.executable, "-c", driver],
        env={**os.environ, "RAY_ADDRESS": d1.address, "MENTAT_GROUP": "untagged",
             "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE])},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
    )
    tl._children.append(p)
    state["untagged_driver"] = p
    assert "PLACED" in p.stdout.readline(), "the untagged group never placed"
    pgs = d1.status_json("untagged")["groups"]["untagged"]["placement_groups"]
    assert [pg["state"] for pg in pgs] == ["CREATED"], pgs
    # No island was chosen, so no rank is handed a fabric address it has no
    # fabric for.
    assert [pg["island_nodes"] for pg in pgs] == [None], pgs


def t04_the_hold_down_keeps_a_flapping_link_out_of_placement():
    """A QSFP link that drops and returns must not move the island boundary
    between two consecutive placements, so a change is committed only after
    it has held still."""
    env = {**FABRIC_ENV, "MENTAT_ISLAND_HOLD_DOWN_MS": "10000"}
    e1 = Daemon("127.0.0.1", env=env).wait_up()
    e2 = Daemon("127.0.0.2", peers=[e1.address], env=env).wait_up()
    state.update(e1=e1, e2=e2)
    e2_id = e2.status_json()["node_id"]

    wait_for(
        lambda: len(e1.status_json()["islands"]) == 1
        and len(e1.status_json()["islands"][0]["nodes"]) == 2,
        40,
        "e1 and e2 to form one island (a 10 s hold-down delays the first commit too)",
    )
    e2.kill()
    wait_for(
        lambda: not e1.status_json()["peers"][e2_id]["alive"],
        15,
        "e1 to notice e2 is gone",
    )
    # The peer is gone and the island is unchanged: with a 10 s hold-down,
    # a link that returns inside the window never moved anything.
    deadline = time.time() + 5
    while time.time() < deadline:
        isl = e1.status_json()["islands"]
        assert len(isl) == 1 and len(isl[0]["nodes"]) == 2, isl
        time.sleep(0.5)


def main():
    tests = [
        t01_probes_confirm_the_tagged_fabric,
        t02_a_two_bundle_group_lands_inside_the_island,
        t03_a_group_straddling_the_boundary_waits_and_says_why,
        t03b_an_untagged_group_places_beside_a_tagged_one,
        t04_the_hold_down_keeps_a_flapping_link_out_of_placement,
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
        for k in ("d1", "d2", "d3", "e1", "e2"):
            if k in state:
                state[k].cleanup()


if __name__ == "__main__":
    main()
