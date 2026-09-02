#!/usr/bin/env python3
"""Group semantics: TP=4 in one group, parallel groups sharing nodes
(including the same "model" twice under distinct group names), and
group-scoped resource views. Run with:

    python3 tests/test_groups.py
"""

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Cluster, fresh_shim, run_ok  # noqa: E402

cluster = Cluster()
# TP=4: four single-GPU agents on four "nodes" -- nothing may assume a pair.
# Same container name on every node ON PURPOSE: that is the real deployment
# shape (both ranks' containers are named glm53), and identical agent ids
# once made the registrations replace each other in a loop.
for i in range(4):
    cluster.start_agent("tp4", gpus=1, container="tp4c", node_ip=f"127.0.0.{i + 1}")
cluster.wait_group_gpus("tp4", 4)
# A group of its own for the claim test: the module holds tp4's driver
# session, and a group takes only one.
for i in range(2):
    cluster.start_agent("claimed", gpus=1, container="cc", node_ip=f"127.0.0.{i + 1}")
cluster.wait_group_gpus("claimed", 2)

ray = fresh_shim(cluster.address, "tp4")
from ray.util.placement_group import placement_group, placement_group_table  # noqa: E402
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy  # noqa: E402
from fake_worker import FakeWorker  # noqa: E402

state = {}

DRIVER_SCRIPT = """
import os, sys
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from fake_worker import FakeWorker
ray.init()
assert ray.cluster_resources()["GPU"] == 1.0, ray.cluster_resources()
pg = placement_group([{"GPU": 1.0}])
done, _ = ray.wait([pg.ready()], timeout=15)
assert done
a = ray.remote(FakeWorker).options(
    name="w0", num_gpus=1,
    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg,
                                                         placement_group_bundle_index=0),
).remote(rank=99)
assert ray.get(a.echo.remote("solo"), timeout=30) == ("echo", 99, "solo")
print("DRIVER_OK", flush=True)
"""


def t01_tp4_placement_and_serving():
    ray.init()
    assert ray.cluster_resources()["GPU"] == 4.0
    pg = placement_group([{"GPU": 1.0}] * 4, strategy="PACK")
    done, _ = ray.wait([pg.ready()], timeout=10)
    assert done
    table = placement_group_table(pg)
    assert set(table["bundles"].keys()) == {0, 1, 2, 3}
    nodes = set(table["bundles_to_node_id"].values())
    assert len(nodes) == 4, "each bundle on its own node"
    actors = [
        ray.remote(FakeWorker)
        .options(
            name=f"vllm_Worker_tp4_TP{r}",
            num_gpus=1,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=r
            ),
        )
        .remote(rank=r)
        for r in range(4)
    ]
    assert ray.get([a.echo.remote("x") for a in actors]) == [
        ("echo", r, "x") for r in range(4)
    ]
    state["actors"] = actors
    state["pids"] = ray.get([a.pid.remote() for a in actors])
    # All four GPUs reserved now.
    snap = cluster.status_json("tp4")
    g = snap["groups"]["tp4"]
    assert g["gpus_total"] == 4 and g["gpus_used"] == 4, g


def t02_liveness_at_tp4():
    import signal

    run_refs = [a.block_forever.remote() for a in state["actors"]]
    ref_to_rank = {r: i for i, r in enumerate(run_refs)}
    os.kill(state["pids"][2], signal.SIGKILL)
    done, _ = ray.wait(run_refs, num_returns=1, timeout=10)
    assert len(done) == 1 and ref_to_rank[done[0]] == 2, done


def t03_parallel_groups_share_nodes():
    # Two more "models" on nodes already carrying tp4 agents -- including the
    # same model deployed twice under distinct MENTAT_GROUP values.
    cluster.start_agent("qwen-a", gpus=1, container="qa", node_ip="127.0.0.1")
    cluster.start_agent("qwen-b", gpus=1, container="qb", node_ip="127.0.0.2")
    cluster.wait_group_gpus("qwen-a", 1)
    cluster.wait_group_gpus("qwen-b", 1)

    procs = []
    for group in ("qwen-a", "qwen-b"):
        env = {
            **os.environ,
            "RAY_ADDRESS": cluster.address,
            "MENTAT_GROUP": group,
            "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE]),
        }
        procs.append(
            subprocess.Popen(
                [sys.executable, "-c", DRIVER_SCRIPT],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        )
    for p in procs:
        out, err = p.communicate(timeout=60)
        assert "DRIVER_OK" in out, (out, err)

    # Group scoping: tp4's view was never polluted.
    assert ray.cluster_resources()["GPU"] == 4.0
    snap = cluster.status_json()
    assert set(snap["groups"]) >= {"tp4", "qwen-a", "qwen-b"}, snap["groups"].keys()


def t04_metrics_per_group():
    m = cluster.metrics()
    assert 'mentat_gpus_total{group="tp4"} 4' in m, m
    assert 'mentat_gpus_total{group="qwen-a"} 1' in m, m
    assert 'mentat_gpus_total{group="qwen-b"} 1' in m, m


def t05_a_claim_fences_placement():
    """MENTAT_CLAIM names a placement and placement stays inside it.

    ray.placement_group takes bundles and a strategy, neither of which can
    say which nodes a group may use, so the claim comes from the
    environment. The fence is the point: a group asking for more than its
    claim holds waits rather than spilling onto a node the claim did not
    choose.
    """
    import json

    driver = """
import os, sys, json
sys.path[:0] = os.environ["PYTHONPATH"].split(os.pathsep)
import ray
from ray.util.placement_group import placement_group, placement_group_table
ray.init()
pg = placement_group([{"GPU": 1.0}] * int(os.environ["WANT"]))
if not ray.wait([pg.ready()], timeout=20)[0]:
    print("PLACED null", flush=True)
else:
    t = placement_group_table(pg)
    print("PLACED " + json.dumps(
        sorted(set(t.get("bundles_to_node_id", {}).values()))), flush=True)
"""

    def run(want, extra):
        env = {
            **os.environ,
            "RAY_ADDRESS": cluster.address,
            "MENTAT_GROUP": "claimed",
            "WANT": str(want),
            "PYTHONPATH": os.pathsep.join([tl.PYTHON_PKG, HERE]),
            **extra,
        }
        r = subprocess.run(
            [sys.executable, "-c", driver], env=env,
            capture_output=True, text=True, timeout=120,
        )
        for line in r.stdout.splitlines():
            if line.startswith("PLACED"):
                return json.loads(line[7:])
        raise AssertionError((r.stdout + r.stderr)[-400:])

    free = run(2, {})
    assert free is not None and len(free) == 2, free

    solo = {"sets": [{"name": "solo", "bundles": [1.0], "link": "ip"}]}
    fenced = run(2, {"MENTAT_CLAIM": "fence-test",
                     "MENTAT_CLAIM_SHAPE": json.dumps(solo)})
    assert fenced is None, f"two bundles escaped a one-node claim: {fenced}"

    # The same claim, asked for what it actually holds, places.
    inside = run(1, {"MENTAT_CLAIM": "fence-test",
                     "MENTAT_CLAIM_SHAPE": json.dumps(solo)})
    assert inside is not None and len(inside) == 1, inside


def main():
    tests = [
        t01_tp4_placement_and_serving,
        t02_liveness_at_tp4,
        t03_parallel_groups_share_nodes,
        t04_metrics_per_group,
        t05_a_claim_fences_placement,
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
