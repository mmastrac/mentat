"""Scheduling strategies: PlacementGroupSchedulingStrategy is the only one
mentat acts on; NodeAffinitySchedulingStrategy exists so imports resolve."""


class PlacementGroupSchedulingStrategy:
    def __init__(
        self,
        placement_group,
        placement_group_bundle_index=-1,
        placement_group_capture_child_tasks=False,
    ):
        self.placement_group = placement_group
        self.placement_group_bundle_index = placement_group_bundle_index
        self.placement_group_capture_child_tasks = placement_group_capture_child_tasks


class NodeAffinitySchedulingStrategy:
    def __init__(self, node_id, soft=False):
        self.node_id = node_id
        self.soft = soft
