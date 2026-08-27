"""Placement groups, mentat-style: bundles of GPUs placed onto a group's
agents by the daemon. The object shape mirrors what vLLM reads."""

from ray import _client
from ray._refs import ObjectRef


class PlacementGroup:
    def __init__(self, pg_id, bundle_specs, ready_ref):
        self.id = pg_id
        self.bundle_specs = list(bundle_specs)
        self._ready_ref = ready_ref

    def ready(self):
        return ObjectRef(self._ready_ref)

    def wait(self, timeout_seconds=None):
        import ray

        done, _ = ray.wait([self.ready()], timeout=timeout_seconds)
        return bool(done)

    def __repr__(self):
        return f"PlacementGroup({self.id}, {self.bundle_specs})"


def placement_group(bundles, strategy="PACK", name="", lifetime=None):
    gpu_bundles = [float(b.get("GPU", 0)) for b in bundles]
    resp, _ = _client.get_conn().request(
        {"t": "create_pg", "bundles": gpu_bundles, "strategy": strategy}
    )
    return PlacementGroup(resp["pg_id"], bundles, resp["ready_ref"])


def placement_group_table(pg):
    resp, _ = _client.get_conn().request({"t": "pg_table", "pg_id": pg.id})
    table = resp["table"]
    # JSON forces string keys; ray's table uses ints and vLLM indexes with
    # ints, so convert here.
    table["bundles"] = {int(k): v for k, v in table.get("bundles", {}).items()}
    table["bundles_to_node_id"] = {
        int(k): v for k, v in table.get("bundles_to_node_id", {}).items()
    }
    return table


def remove_placement_group(pg):
    _client.get_conn().request({"t": "remove_pg", "pg_id": pg.id})


def get_current_placement_group():
    # Only actors scheduled with placement_group_capture_child_tasks would
    # have one; vLLM's driver never does, and the audit confirms only the
    # None path is exercised.
    return None
