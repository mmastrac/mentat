# mentat

mentat replaces Ray's control plane for vLLM multi-node serving. It has three
parts: a daemon that places actors and watches their liveness, a router that
puts one OpenAI-compatible endpoint in front of every model, and a pure-Python
package that installs as `ray` and implements the surface vLLM's Ray executor
uses.

There is no object store, memory monitor, raylet or dashboard. Per-token work
is unchanged: vLLM's workers exchange data over its own MessageQueue and NCCL,
and after boot the only recurring Ray call is `ray.wait` every 5 seconds.

Registration retries forever, so daemons and containers can start in any
order. Both binaries are static executables.

## Components

| Component | Source | Description | Overhead |
| --- | --- | --- | --- |
| `mentatd` | [rust/](rust/), [crates.io](https://crates.io/crates/mentatd) | Daemon, agent and CLI in one binary | 1.7 MiB on disk, ~2.5 MiB RSS |
| `mentatd-serve` | [serve/](serve/) | Router, its own crate and container | 1.7 MiB on disk, ~3.2 MiB RSS |
| `ray` shim | [python/](python/) | Pure-Python package claiming the `ray` import name | ~2 MiB in a running interpreter |

## Ports

| Port | Owner | Purpose |
| --- | --- | --- |
| 6379/tcp | `mentatd` | Control. The port `RAY_ADDRESS` specifies |
| 6380/tcp | `mentatd` | HTTP: `/status`, `/metrics`, `/events` |
| 6381/tcp | `mentatd-serve` | HTTP: `/v1`, `/mcp`, `/status.json` |
| 6382/udp | both | Daemon announcements |

## Installation

From source:

```
cargo install mentatd
pip wheel --no-deps -w dist ./python
```

The wheel is not on PyPI. The published artifacts image carries both
binaries and the wheel:

```
docker pull mmastrac/mentat-artifacts:0.5.2
```

To build every image locally:

```
VERSION=0.5.2 ./build.sh
```

This produces `mentat-artifacts:<ver>` (both binaries and the wheel, for
`COPY --from`), `mentatd:<ver>`, `mentatd-serve:<ver>` and `mentat:<ver>`
(both binaries in one image).

## Quick start

Run a daemon on each node, on the host network:

```
MENTAT_NODE_IP=10.0.0.1 MENTAT_PEERS=10.0.0.2:6379 mentatd daemon
```

Replace Ray with the shim in the model image:

```dockerfile
COPY --from=mmastrac/mentat-artifacts:0.5.2 /out/mentatd /usr/local/bin/mentatd
COPY --from=mmastrac/mentat-artifacts:0.5.2 /out/mentatd-0.5.2-py3-none-any.whl /tmp/
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
 && pip uninstall -y ray \
 && pip install --no-deps /tmp/mentatd-0.5.2-py3-none-any.whl
```

In the entrypoint, export the daemon address and the group before
`ray start`:

```bash
export VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1
export RAY_ADDRESS=10.0.0.1:6379
export MENTAT_GROUP=mymodel
ray start --address=$RAY_ADDRESS
vllm serve ... --distributed-executor-backend ray -tp 2
```

Inspect the cluster from any node:

```
mentatd status
```

[mentatd.yaml](mentatd.yaml) and [mentatd-serve.yaml](mentatd-serve.yaml)
are compose files for the daemon and the router. Both need
`network_mode: host`.

## Documentation

- [GUIDE.md](GUIDE.md): the `mentatd` manual. Commands, environment,
  migration from Ray, fabrics, HTTP interface.
- [GUIDE-SERVE.md](GUIDE-SERVE.md): the `mentatd-serve` manual. Endpoint
  announcement, routing, environment, HTTP interface.
- [PROTOCOL.md](PROTOCOL.md): the wire protocol.
- [tests/README.md](tests/README.md): the test suites. None need a GPU.
