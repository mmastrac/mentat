"""ray.util surface used by vLLM's ray executor path."""

import os
import socket

from ray import _client
from ray.util.placement_group import (  # noqa: F401
    PlacementGroup,
    get_current_placement_group,
    placement_group,
    placement_group_table,
    remove_placement_group,
)


def get_node_ip_address():
    """The IP this node is known by in the cluster.

    VLLM_HOST_IP wins: an operator who set it by hand meant it, and that is
    what makes adopting the automatic choice an opt-in -- a deployment moves
    over by deleting its VLLM_HOST_IP, one deployment at a time.

    MENTAT_FABRIC_IP comes next. The daemon sets it per rank when it placed
    this group on a probed fabric, so it names the address that carries
    NCCL for these particular ranks. MENTAT_NODE_IP is the container's own
    idea of its address, which is right on a single-fabric box and a guess
    anywhere else. The UDP trick is the dev-box fallback."""
    for var in ("VLLM_HOST_IP", "MENTAT_FABRIC_IP", "MENTAT_NODE_IP"):
        v = os.environ.get(var)
        if v:
            return v
    target = _client.GLOBAL.address if _client.GLOBAL.initialized else None
    target = target or os.environ.get("MENTAT_GCS_ADDRESS") or "8.8.8.8:53"
    host, _, port = target.rpartition(":")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((host, int(port)))
            return s.getsockname()[0]
        finally:
            s.close()
    except OSError:
        return "127.0.0.1"


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    raise AttributeError(
        f"mentat: ray.util.{name} is not implemented -- vLLM's audited ray "
        "surface does not use it; if vLLM changed, re-audit before adding it"
    )
