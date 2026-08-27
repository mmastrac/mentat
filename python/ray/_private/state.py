"""ray._private.state: vLLM imports available_resources_per_node from here
(with a fallback it never needs because this import succeeds)."""

from ray import _client


def available_resources_per_node():
    resp, _ = _client.get_conn().request({"t": "available_per_node"})
    return resp["nodes"]
