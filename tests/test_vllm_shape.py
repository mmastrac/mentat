#!/usr/bin/env python3
"""Replay of vLLM RayExecutorV2's exact ray call sequence, transcribed from
vllm/v1/executor/ray_executor_v2.py and ray_utils.py (vLLM
0.1.dev20051+g487ecf187). If the shim drifts from the audited surface, this
fails on a laptop instead of after a 10-minute weight load. Run with:

    python3 tests/test_vllm_shape.py
"""

import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import mentat_testlib as tl  # noqa: E402
from mentat_testlib import Cluster, fresh_shim, run_ok  # noqa: E402

cluster = Cluster()
cluster.start_agent("glm53", gpus=1, container="head", node_ip="127.0.0.1")
cluster.start_agent("glm53", gpus=1, container="worker", node_ip="127.0.0.2")
cluster.wait_group_gpus("glm53", 2)

ray = fresh_shim(cluster.address, "glm53")
from fake_worker import VllmShapeWorker  # noqa: E402

WORLD_SIZE = 2
state = {}


def t01_initialize_ray_cluster():
    # ray_utils.assert_ray_available / initialize_ray_cluster
    assert not ray.is_initialized()
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"  # ray_utils.py:562
    ray.init(address=None, runtime_env=None)  # ray_utils.py:597 (address=None)
    assert ray.is_initialized()

    # config/parallel.py:950 / arg_utils.py:2043
    from ray.util import get_current_placement_group

    assert get_current_placement_group() is None

    # arg_utils.py:2025-2027
    renv = ray.get_runtime_context().runtime_env
    assert renv.to_dict() == {}

    # ray_utils.py:635
    assert ray.cluster_resources().get("GPU", 0) >= WORLD_SIZE

    # ray_utils.py:656
    from ray._private.state import available_resources_per_node

    per_node = available_resources_per_node()
    assert sum(v.get("GPU", 0) for v in per_node.values()) >= WORLD_SIZE

    # ray_utils.py:673 -- exactly the bundles vLLM builds for TP=2
    from ray.util import placement_group

    pg = placement_group([{"GPU": 1.0}] * WORLD_SIZE, strategy="PACK")
    state["pg"] = pg

    # ray_utils.py:468-471 -- pg readiness via ray.wait on pg.ready()
    ready_ref = pg.ready()
    done, _ = ray.wait([ready_ref], timeout=1800)
    assert done
    # ray_utils.py:489 -- and the timeout=0 poll form
    ray.get(ready_ref, timeout=0)

    # ray_utils.py:305/383/423 -- table reads with INT bundle indices
    from ray.util import placement_group_table

    table = placement_group_table(pg)
    for i in range(WORLD_SIZE):
        assert table["bundles"][i]["GPU"] == 1.0
        assert isinstance(table["bundles_to_node_id"][i], str)
    assert len(pg.bundle_specs) == WORLD_SIZE  # ray_utils.py:436/465/615


def t02_create_workers():
    # ray_executor_v2.py:378-395, kwargs verbatim
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    pg = state["pg"]
    instance_id = "1787840107855336818"
    workers = []
    for rank in range(WORLD_SIZE):
        actor = (
            ray.remote(VllmShapeWorker)
            .options(
                name=f"vllm_Worker_{instance_id}_TP{rank}",
                num_cpus=0,
                num_gpus=1.0,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_bundle_index=rank,
                ),
                runtime_env={
                    "env_vars": {"RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES": "1"}
                },
            )
            .remote(
                vllm_config={"model": "fake", "tp": WORLD_SIZE},
                rank=rank,
                distributed_init_method="tcp://127.0.0.1:29500",
                input_shm_handle=b"shm-handle",
                is_driver_worker=rank == 0,
                is_driver_node=rank == 0,
            )
        )
        workers.append(actor)
    state["workers"] = workers

    # ray_executor_v2.py:127/133 equivalent -- node + physical gpu discovery
    node_gpus = ray.get([w.get_node_and_physical_gpu_ids.remote() for w in workers])
    driver_node = ray.get_runtime_context().get_node_id()
    assert node_gpus[0][0] == driver_node, "rank 0 must sit on the driver node"
    assert all(gpus == [0] for _, gpus in node_gpus), node_gpus

    # env propagation as vLLM does it: plain method args, setdefault semantics
    env_vars = {"VLLM_USE_V1": "1"}
    driver_env_vars = {"NCCL_DEBUG": "WARN", "VLLM_USE_V1": "0"}
    for rank, w in enumerate(workers):
        w.initialize_worker.remote(
            rank, env_vars, driver_env_vars, assigned_physical_gpu_ids=[0]
        )

    # wait_for_init returns the MQ-handle dict through pickle
    inits = ray.get([w.wait_for_init.remote() for w in workers])
    assert all(i["status"] == "READY" for i in inits), inits
    assert inits[1]["handle"] == b"fake-mq-handle-1"


def t03_monitor_loop_and_shutdown():
    # ray_executor_v2.py:486-509 -- the liveness monitor, verbatim shape
    workers = state["workers"]
    run_refs = [w.run.remote() for w in workers]
    ref_to_rank = {r: i for i, r in enumerate(run_refs)}

    for _ in range(2):
        if not ray.is_initialized():
            break
        done, _ = ray.wait(run_refs, num_returns=1, timeout=5.0)
        assert not done, "no worker should die on its own"
        dead_ranks = [ref_to_rank[r] for r in done]
        assert dead_ranks == []

    # shutdown path: ray.kill each worker, then ray.shutdown
    for w in workers:
        ray.kill(w)
    deadline = time.time() + 10
    while time.time() < deadline:
        done, _ = ray.wait(run_refs, num_returns=len(run_refs), timeout=1)
        if len(done) == len(run_refs):
            break
    else:
        raise AssertionError("kills did not resolve the run refs")
    ray.shutdown()  # parallel_state.py:2159
    assert not ray.is_initialized()


def main():
    tests = [t01_initialize_ray_cluster, t02_create_workers, t03_monitor_loop_and_shutdown]
    try:
        for t in tests:
            run_ok(t, t.__name__)
        print(f"\nALL {len(tests)} TESTS PASSED")
    finally:
        cluster.cleanup()


if __name__ == "__main__":
    main()
