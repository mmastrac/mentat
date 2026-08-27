"""Exception hierarchy mirroring the slice of ray.exceptions vLLM touches."""


class RayError(Exception):
    pass


class RayActorError(RayError):
    pass


class ActorDiedError(RayActorError):
    pass


class ActorUnavailableError(RayActorError):
    pass


class RayTaskError(RayError):
    pass


class GetTimeoutError(RayError, TimeoutError):
    pass


class RayChannelError(RayError):
    # Compiled-DAG surface; import-only for us (the V2 executor never uses it).
    pass
