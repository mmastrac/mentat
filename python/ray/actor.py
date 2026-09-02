"""ActorClass / ActorHandle / ActorMethod: the .options().remote() surface
vLLM's RayExecutorV2 uses to launch RayWorkerProc."""

import os
import sys
import uuid

from ray import _client, cloudpickle
from ray._refs import ObjectRef

# .options() keys mentat understands. Anything else is accepted and ignored
# (with a note when MENTAT_DEBUG is set) -- ray has dozens of scheduling knobs
# that have no meaning without a Ray scheduler.
_KNOWN_OPTIONS = {
    "name",
    "num_cpus",
    "num_gpus",
    "scheduling_strategy",
    "runtime_env",
    "max_concurrency",
    "lifetime",
    "namespace",
    "max_restarts",
    "resources",
}


class ActorClass:
    def __init__(self, cls, options=None):
        self._cls = cls
        self._options = dict(options or {})

    def options(self, **kwargs):
        unknown = set(kwargs) - _KNOWN_OPTIONS
        if unknown and os.environ.get("MENTAT_DEBUG"):
            print(f"mentat: ignoring actor options {sorted(unknown)}", file=sys.stderr)
        merged = dict(self._options)
        merged.update(kwargs)
        return ActorClass(self._cls, merged)

    def remote(self, *args, **kwargs):
        opts = self._options
        name = opts.get("name") or f"actor-{uuid.uuid4().hex[:12]}"

        strategy = opts.get("scheduling_strategy")
        pg_id = None
        bundle_index = 0
        if strategy is not None:
            pg = getattr(strategy, "placement_group", None)
            if pg is None:
                raise _client.MentatError(
                    "mentat: scheduling_strategy must be a "
                    f"PlacementGroupSchedulingStrategy, got {strategy!r}"
                )
            pg_id = pg.id
            bundle_index = getattr(strategy, "placement_group_bundle_index", 0) or 0
        if pg_id is None:
            raise _client.MentatError(
                "mentat: this actor names no placement group. Pass a "
                "PlacementGroupSchedulingStrategy, as vLLM does"
            )

        env_vars = {}
        runtime_env = opts.get("runtime_env")
        if runtime_env:
            env_vars = dict(runtime_env.get("env_vars", {}))

        payload = cloudpickle.dumps((self._cls, args, kwargs))
        resp, _ = _client.get_conn().request(
            {
                "t": "create_actor",
                "name": name,
                "num_gpus": float(opts.get("num_gpus", 0) or 0),
                "pg_id": pg_id,
                "bundle_index": int(bundle_index),
                "env": env_vars,
            },
            payload,
        )
        return ActorHandle(resp["actor_id"], resp["node_id"], resp["gpu_ids"], name)


class ActorMethod:
    __slots__ = ("_actor", "_method")

    def __init__(self, actor, method):
        self._actor = actor
        self._method = method

    def remote(self, *args, **kwargs):
        payload = cloudpickle.dumps((args, kwargs))
        resp, _ = _client.get_conn().request(
            {
                "t": "call",
                "actor_id": self._actor._actor_id,
                "method": self._method,
            },
            payload,
        )
        return ObjectRef(resp["ref_id"])


class ActorHandle:
    def __init__(self, actor_id, node_id, gpu_ids, name):
        self._actor_id = actor_id
        self._node_id = node_id
        self._gpu_ids = gpu_ids
        self._name = name

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)
        return ActorMethod(self, item)

    def __repr__(self):
        return f"ActorHandle({self._name}, {self._actor_id})"
