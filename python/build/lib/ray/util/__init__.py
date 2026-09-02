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

    MENTAT_FABRIC_IP wins. The daemon sets it per rank when it placed this
    group on a probed fabric, so it names the address that carries NCCL for
    these particular ranks. MENTAT_NODE_IP comes next: the container's own
    idea of its address, which is right on a single-fabric box and a guess
    anywhere else.

    Only those two are read. An engine's own address setting names what that
    engine binds, chosen for its own reasons, and is routinely the fabric
    address while the node is known by another. An operator pinning this
    answer sets MENTAT_NODE_IP.

    With none of them set, the address that reaches this daemon is the
    answer, since that is the link this container is already talking over.
    Asking the route with no daemon to aim at would answer with whichever
    interface reaches the public internet, which on a multi-homed box is the
    one link that must not carry NCCL, so that case raises instead.
    """
    for var in ("MENTAT_FABRIC_IP", "MENTAT_NODE_IP"):
        v = os.environ.get(var)
        if v:
            return v
    target = _client.GLOBAL.address if _client.GLOBAL.initialized else None
    target = target or os.environ.get("MENTAT_GCS_ADDRESS")
    if not target:
        raise _client.MentatError(
            "mentat: no address for this node. ray.init() has not connected and "
            "MENTAT_GCS_ADDRESS is unset, so there is no daemon to find the route "
            "to. Set MENTAT_NODE_IP to the address this node is known by."
        )
    host, _, port = target.rpartition(":")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((host, int(port)))
            found = s.getsockname()[0]
        finally:
            s.close()
    except OSError as e:
        raise _client.MentatError(
            f"mentat: no route to the daemon at {target} ({e}), so this node's "
            "address cannot be determined. Set MENTAT_NODE_IP. "
            "Answering 127.0.0.1 here would have every other rank dial itself."
        ) from None
    # A daemon on loopback answers loopback, which is this node's address
    # only to itself. Outside an actor that is a driver asking about a
    # single box and is fine. Inside one it is a rank's cluster identity,
    # the address its peers dial, and no peer can reach it there.
    if found.startswith("127.") and _client.in_actor():
        raise _client.MentatError(
            "mentat: this rank reaches its daemon over loopback, so the route "
            "answers 127.0.0.1, which its peers cannot dial. Set MENTAT_NODE_IP "
            "on the container, or place the group on a fabric so the daemon "
            "sets MENTAT_FABRIC_IP per rank."
        )
    return found


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    raise AttributeError(
        f"mentat: ray.util.{name} is not implemented -- vLLM's audited ray "
        "surface does not use it; if vLLM changed, re-audit before adding it"
    )
