"""RuntimeEnv: a dict with ray's to_dict() spelling. mentat only honors
env_vars; anything else a caller stuffs in is carried but inert."""


class RuntimeEnv(dict):
    def to_dict(self):
        return dict(self)

    def env_vars(self):
        return dict(self.get("env_vars", {}))
