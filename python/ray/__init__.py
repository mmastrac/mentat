"""mentat's ray-compatible shim.

This package occupies the `ray` import name inside the Spark serving images.
It implements exactly the surface vLLM's RayExecutorV2 path exercises
(audited against vLLM 0.1.dev20051+g487ecf187 / ray 2.57.0) and speaks a
small framed protocol to the mentat daemon. Everything else fails loudly via
__getattr__ rather than silently misbehaving.

Cluster brains (placement, liveness, reaping) live in the Rust daemon; this
module is control-plane only -- after vLLM boots, the only recurring call is
the health monitor's ray.wait every 5 seconds.
"""

import pickle as _pickle

from ray import _client
from ray import cloudpickle  # noqa: F401  (attribute must exist: ray.cloudpickle)
from ray import util  # noqa: F401  (ray.util must resolve without a prior submodule import)
from ray._refs import ObjectRef
from ray.actor import ActorClass, ActorHandle  # noqa: F401
from ray.exceptions import (
    GetTimeoutError,
    RayActorError,
)
from ray.runtime_context import get_runtime_context  # noqa: F401

# vLLM version-checks ray; the wire-compatible lie is deliberate and the
# init banner in _client.py says who we really are.
__version__ = "2.57.0"

__commit__ = "mentat"


def init(address=None, runtime_env=None, **kwargs):
    return _client.ensure_init(address=address, runtime_env=runtime_env)


def is_initialized():
    return _client.GLOBAL.initialized or _client.in_actor()


def shutdown():
    _client.shutdown()


def remote(target=None, **kwargs):
    if target is None:
        # @ray.remote(num_cpus=...) decorator form.
        def wrap(cls):
            return remote(cls, **kwargs)

        return wrap
    if isinstance(target, type):
        return ActorClass(target, kwargs)
    raise AttributeError(
        "mentat: ray.remote on plain functions (tasks) is not implemented -- "
        "vLLM's executor only uses actor classes"
    )


def kill(actor, no_restart=True):
    _client.get_conn().request({"t": "kill_actor", "actor_id": actor._actor_id})


def _load(payload):
    if not payload:
        return None
    return _pickle.loads(payload)


def _get_one(ref, timeout):
    if not isinstance(ref, ObjectRef):
        raise TypeError(f"ray.get expects ObjectRef, got {type(ref)}")
    timeout_ms = None if timeout is None else max(0, int(timeout * 1000))
    resp, payload = _client.get_conn().request(
        {"t": "get", "ref_id": ref._id, "timeout_ms": timeout_ms}
    )
    status = resp["status"]
    if status == "ok":
        return _load(payload)
    if status == "error":
        exc = _load(payload)
        if isinstance(exc, BaseException):
            raise exc
        raise RayActorError(f"actor raised unpicklable exception: {exc!r}")
    if status == "actor_died":
        raise RayActorError(
            f"mentat: actor died: {resp.get('reason', 'unknown reason')} (ref {ref._id})"
        )
    if status == "timeout":
        raise GetTimeoutError(f"mentat: ray.get timed out on {ref._id}")
    raise _client.MentatError(f"mentat: unknown get status {status!r}")


def get(refs, timeout=None):
    if isinstance(refs, list):
        return [_get_one(r, timeout) for r in refs]
    return _get_one(refs, timeout)


def wait(refs, *, num_returns=1, timeout=None, fetch_local=True):
    if not isinstance(refs, list):
        raise TypeError("ray.wait expects a list of ObjectRefs")
    timeout_ms = None if timeout is None else max(0, int(timeout * 1000))
    resp, _ = _client.get_conn().request(
        {
            "t": "wait",
            "ref_ids": [r._id for r in refs],
            "num_returns": num_returns,
            "timeout_ms": timeout_ms,
        }
    )
    ready_ids = set(resp["ready"])
    # Same objects back, partitioned, input order preserved -- vLLM uses the
    # returned refs as dict keys.
    done = [r for r in refs if r._id in ready_ids]
    pending = [r for r in refs if r._id not in ready_ids]
    return done, pending


def nodes():
    resp, _ = _client.get_conn().request({"t": "nodes"})
    return resp["nodes"]


def cluster_resources():
    resp, _ = _client.get_conn().request({"t": "cluster_resources"})
    return resp["resources"]


def available_resources():
    # Sum of the per-node view; not on vLLM's audited path but harmless.
    from ray._private.state import available_resources_per_node

    total = {}
    for res in available_resources_per_node().values():
        for k, v in res.items():
            if not k.startswith("node:"):
                total[k] = total.get(k, 0.0) + v
    return total


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    raise AttributeError(
        f"mentat: ray.{name} is not implemented -- vLLM's audited ray surface "
        "does not use it; if vLLM changed, re-audit before adding it"
    )
