"""ObjectRef: a string id with ray-shaped identity semantics.

vLLM keeps refs as dict keys across ray.wait() round trips, so hash/eq are on
the id, and ray.wait returns the same objects it was handed.
"""


class ObjectRef:
    __slots__ = ("_id",)

    def __init__(self, ref_id):
        self._id = ref_id

    def hex(self):
        return self._id

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        return isinstance(other, ObjectRef) and other._id == self._id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f"ObjectRef({self._id})"
