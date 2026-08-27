"""A stand-in for vLLM's RayWorkerProc: enough methods to exercise every
actor behavior the executor path relies on, with zero GPU/vLLM dependency.

Must be importable by module path inside the spawned actor host (the tests
put this directory on PYTHONPATH), because stdlib pickle serializes classes
by reference.
"""

import os
import time


class FakeWorker:
    def __init__(self, rank=0, fail_ctor=False):
        if fail_ctor:
            raise RuntimeError("ctor failed on purpose")
        self.rank = rank

    def echo(self, x):
        return ("echo", self.rank, x)

    def pid(self):
        return os.getpid()

    def env_dump(self):
        return dict(os.environ)

    def runtime_ctx(self):
        import ray

        ctx = ray.get_runtime_context()
        return {
            "actor_id": ctx.get_actor_id(),
            "node_id": ctx.get_node_id(),
            "gpus": ctx.get_accelerator_ids()["GPU"],
            "is_initialized": ray.is_initialized(),
        }

    def sleep(self, seconds):
        time.sleep(seconds)
        return seconds

    def raise_err(self, msg):
        raise ValueError(msg)

    def block_forever(self):
        # Mirrors RayWorkerProc.run(): never returns; the ref is a liveness
        # sentinel only.
        while True:
            time.sleep(3600)


class VllmShapeWorker:
    """Method-for-method shape of vLLM's RayWorkerProc as the V2 executor
    drives it (ctor kwargs, the four methods, and their return shapes)."""

    def __init__(self, vllm_config, rank, distributed_init_method,
                 input_shm_handle, is_driver_worker, is_driver_node):
        self.vllm_config = vllm_config
        self.rank = rank
        self.distributed_init_method = distributed_init_method
        self.input_shm_handle = input_shm_handle
        self.is_driver_worker = is_driver_worker
        self.is_driver_node = is_driver_node
        self.env = {}

    def get_node_and_physical_gpu_ids(self):
        import ray

        ctx = ray.get_runtime_context()
        gpus = [int(g) for g in ctx.get_accelerator_ids()["GPU"]]
        return ctx.get_node_id(), gpus

    def initialize_worker(self, local_rank, env_vars, driver_env_vars,
                          assigned_physical_gpu_ids=None):
        for k, v in {**driver_env_vars, **env_vars}.items():
            self.env.setdefault(k, v)
        self.local_rank = local_rank
        self.assigned_physical_gpu_ids = assigned_physical_gpu_ids
        return None

    def wait_for_init(self):
        return {"status": "READY", "handle": b"fake-mq-handle-%d" % self.rank}

    def run(self):
        while True:
            time.sleep(3600)
