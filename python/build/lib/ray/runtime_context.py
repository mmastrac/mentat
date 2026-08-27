"""RuntimeContext: env-derived inside an actor host, hello-derived in the
driver. The five methods below are exactly what vLLM's ray path touches."""

import os

from ray import _client
from ray.runtime_env import RuntimeEnv


class RuntimeContext:
    def get_node_id(self):
        node = os.environ.get("MENTAT_NODE_ID")
        if node:
            return node
        hello = _client.ensure_init()
        return hello["node_id"]

    def get_actor_id(self):
        return os.environ.get("MENTAT_ACTOR_ID") or None

    def get_accelerator_ids(self):
        ids = os.environ.get("MENTAT_GPU_IDS", "")
        gpu = [g for g in ids.split(",") if g != ""]
        return {"GPU": gpu}

    @property
    def gcs_address(self):
        addr = os.environ.get("MENTAT_GCS_ADDRESS")
        if addr:
            return addr
        if _client.GLOBAL.initialized:
            return _client.GLOBAL.hello.get("gcs_address")
        return _client.default_address()

    @property
    def runtime_env(self):
        env = _client.GLOBAL.runtime_env
        if env is None:
            return RuntimeEnv()
        if isinstance(env, RuntimeEnv):
            return env
        return RuntimeEnv(env)


def get_runtime_context():
    return RuntimeContext()
