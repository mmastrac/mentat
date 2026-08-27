"""ray.cloudpickle: re-export the real cloudpickle when present (it is a vLLM
dependency, so inside the serving images it always is), else fall back to
stdlib pickle with an inert register_pickle_by_value."""

try:
    from cloudpickle import *  # noqa: F401,F403
    from cloudpickle import dumps, loads, dump, load  # noqa: F401

    try:
        from cloudpickle import register_pickle_by_value  # noqa: F401
    except ImportError:  # very old cloudpickle
        def register_pickle_by_value(module):  # noqa: D103
            pass
except ImportError:
    from pickle import *  # noqa: F401,F403
    from pickle import dumps, loads, dump, load  # noqa: F401

    def register_pickle_by_value(module):
        pass
