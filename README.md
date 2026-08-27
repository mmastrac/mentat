# mentat

mentat replaces Ray for vLLM multi-node serving: a Rust daemon for placement,
liveness and reaping, a Rust router for HTTP, and a pure-Python package holding
the `ray` import name.

mentat has no object store, no memory monitor and no dashboard. Registration
retries forever, so containers can start in any order. Each lifecycle decision
is one log line. The driver's exception carries a dead actor's exit code and
signal.

## Components

- `mentatd daemon` — per-node daemon. Control port 6379, HTTP 6380.
- `mentatd start` — agent, one per model container. `ray` symlink alias.
- `mentatd status` / `mentatd stop` — inspect, kill actors.
- `python/ray/` — pure-Python `ray` shim wheel, `pip install --no-deps`. Reports `__version__ == "2.57.0"`.
- `serve/` — `mentat-serve`, separate crate + container. HTTP 6381.

One binary is the daemon, the agent and the CLI. The `ray` symlink keeps
`ray start` and `ray status` working, and `ray --version` names both versions.

`mentatd daemon` takes `--port` (6379), `--http-port` (6380), `--node-ip`,
`--head-json` (`/tmp/mentat/head.json`) and `--peers`. Compose uses the
environment.

The shim reports `2.57.0` because vLLM version-checks Ray, and logs a banner at
`ray.init`. Pure Python is free: `execute_model` runs over vLLM's MessageQueue
and NCCL, zero Ray calls per token.

`mentat-serve` is a separate crate and container. The daemon never touches
inference traffic, so restarting the router leaves models serving.

## Ports

- 6379/tcp — daemon control (`RAY_ADDRESS`)
- 6380/tcp — daemon HTTP
- 6381/tcp — mentat-serve HTTP
- 6382/udp — daemon announcements

## Getting started

Install the binary and build the shim wheel:

```
cargo install mentatd
pip wheel --no-deps -w dist ./python
```

Run a daemon on each box, before any model container:

```
MENTAT_NODE_IP=10.0.0.1 MENTAT_PEERS=10.0.0.2:6379 mentatd daemon
```

In the model image, replace Ray with the shim. The wheel installs as `ray` and
claims the import name:

```
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
 && pip uninstall -y ray \
 && pip install --no-deps /tmp/mentatd-0.1.0-py3-none-any.whl
```

The entrypoint keeps its `ray start` and `ray status` calls. Export first:

```
export VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1
export RAY_ADDRESS=10.0.0.1:6379
export MENTAT_GROUP=glm53
ray start --address=$RAY_ADDRESS
vllm serve ... --distributed-executor-backend ray -tp 2
```

[GUIDE.md](GUIDE.md) covers the vLLM audit, the Ray workarounds you can delete,
and the limits.

## Deploy

1. `VERSION=<ver> ./build.sh` → `mentat-artifacts:<ver>`, `mentatd:<ver>`, `mentat-serve:<ver>`.
2. [mentatd.yaml](mentatd.yaml) → `~/compose/mentatd/` on each box, per-node `.env`. Before any model container.
3. Rebuild the model images; they `COPY --from=mentat-artifacts`.
4. `docker compose up` the models, either node first.
5. Optional: [mentat-serve.yaml](mentat-serve.yaml) → `~/compose/mentat-serve/`. No `.env` required.

Step 2's ordering saves time. A container that starts first retries until a
daemon answers.

Set `MENTAT_NODE_IP` to the address the driver sees. Left empty, the daemon
takes the default route, wrong on a multi-homed box.

Rollback: point `IMAGE` at the previous real-ray image tag.

## Operating

```
mentatd status [--group g] [--json]
mentatd stop [--group g]
curl -s http://<box>:6380/status | jq .
curl -s http://<box>:6380/metrics
websocat ws://<box>:6380/events
curl -s http://<box>:6381/v1/models
curl -s http://<box>:6381/status.json | jq .
```

Run `mentatd status` first. With `--group` it prints the ray-compatible
`N.0/M.0 GPU` line the entrypoints grep for.

`mentatd stop` kills actors and the driver restarts on the dead refs. It runs
immediately, whatever degrade window an agent is inside.

Rank death shows as `event=actor_exit` with pid and signal, and in vLLM's
exception text.

## Daemon HTTP (6380)

- `/metrics` — Prometheus
- `/status` — JSON
- `/events` — WebSocket; snapshot on connect, then `node_join`, `node_leave`, `head_change`, `agent_register`, `agent_lost`, `agent_degraded`, `agent_dead`, `pg_created`, `pg_ready`, `pg_timeout`, `actor_spawning`, `actor_running`, `actor_dead`, `driver_connected`, `driver_disconnected`

`/events` sends a snapshot first, so a late client starts whole. `mentat-serve`
holds the socket open and re-reads `/status` on any event.

## mentat-serve HTTP (6381)

- `/v1` — OpenAI-compatible, routed by model name, streaming passed through
- `/mcp` — merged per-container MCP, tools prefixed `<group>__`, plus native `serve_status`
- `/status.json` — route table and per-group health
- `/v1/models` — currently routable models

`/`, `/healthz` and `/status.json` return the same document.

`/status.json` says why a model is missing from `/v1/models`. Each group
carries `healthy` and a `why_not`: no endpoint, no running actors, unprobed,
probe failed, or probe stale.

Model names come from probing the group's own `/v1/models`.

## Daemon env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `MENTAT_NODE_IP` | default route | This node's cluster identity |
| `MENTAT_PEERS` | empty | Comma-separated peer control addresses |
| `MENTAT_ANNOUNCE_PORT` | 6382 | UDP announce port; 0 disables |
| `MENTAT_ANNOUNCE_ADDR` | empty | Extra unicast announce targets |
| `MENTAT_ANNOUNCE_INTERVAL_S` | 5 | Announce interval |
| `MENTAT_PG_PENDING_TIMEOUT_MS` | 600000 | Placement group PENDING → fail |
| `MENTAT_AGENT_DEGRADED_AFTER_MS` | 30000 | Disconnected agent → `agent_degraded` |
| `MENTAT_AGENT_DEAD_AFTER_MS` | 60000 | Disconnected agent → actors dead |
| `MENTAT_PEER_STALE_AFTER_MS` | 30000 | Silent peer → stale |
| `MENTAT_PEER_DEAD_AFTER_MS` | 60000 | Silent peer → `node_leave` |
| `MENTAT_PEER_STATUS_INTERVAL_MS` | 2000 | Peer status push / heartbeat |
| `MENTAT_ELECTION_HOLD_DOWN_MS` | 5000 | Head candidate stability before `head_change` |
| `MENTAT_SLOW_CALL_WARN_MS` | 15000 | Pending call → one `call_pending_long` |
| `MENTAT_SESSION_REAP_GRACE_MS` | 0 | Driver EOF → actor reap delay |
| `MENTAT_TCP_DEAD_AFTER_MS` | 75000 | TCP keepalive target (Linux only) |
| `MENTAT_GPUS` | detected | GPU count override |

`MENTAT_NODE_IP` and `MENTAT_PEERS` are the two you set.

`MENTAT_PG_PENDING_TIMEOUT_MS` times the rendezvous, from the placement group
request to the agents and GPUs arriving. Ten minutes covers a cold box. Lower
it and the driver restarts into the same wait.

`MENTAT_AGENT_DEGRADED_AFTER_MS` and `MENTAT_AGENT_DEAD_AFTER_MS` both time how
long an agent's link has been EOF. Calls are held until the first. At 30s the
agent is degraded, a warning. At 60s its actors are dead and the container
restarts. The gap allows for short periods of disconnection.

`MENTAT_PEER_STALE_AFTER_MS` and `MENTAT_PEER_DEAD_AFTER_MS` are the mesh
version, timed against status pushes. Keep `MENTAT_PEER_STATUS_INTERVAL_MS`
well under them. Fifteen missed pushes means stale.

`MENTAT_ELECTION_HOLD_DOWN_MS` damps flapping. A candidate must stay best this
long before the designation moves. Raise it if `head_change` streams, and head
changes lag by as much.

`MENTAT_SLOW_CALL_WARN_MS` sets when `call_pending_long` fires. A call pending
15s is queued behind a blocking method or stuck on a wedged worker.

Keep `MENTAT_SESSION_REAP_GRACE_MS` at 0. A restarting vLLM needs the old
actors' names and GPUs freed, so a grace delays recovery.

`MENTAT_TCP_DEAD_AFTER_MS` targets how fast keepalive notices a wedged peer,
via `TCP_KEEPIDLE`, `TCP_KEEPINTVL` and `TCP_KEEPCNT`. Linux only.

`MENTAT_GPUS` lets the tests run without GPUs. Otherwise the agent counts with
`nvidia-smi`.

UDP announcement is how `mentat-serve` finds daemons. `MENTAT_ANNOUNCE_ADDR`
adds unicast targets outside the broadcast domain. `MENTAT_ANNOUNCE_PORT=0`
turns it off.

## Agent / container env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `RAY_ADDRESS` | `head.json`, then localhost | Daemon the driver and agents rendezvous on |
| `MENTAT_GROUP` | — | Group scope for driver and agents |
| `MENTAT_DAEMON` | `RAY_ADDRESS` | Daemon address override |
| `MENTAT_OPENAI_API` | unset | Announced OpenAI endpoint (head rank only) |
| `MENTAT_MCP_API` | unset | Announced MCP endpoint (every rank) |
| `MENTAT_HOST_CONNECT_TIMEOUT_MS` | 60000 | Wait for a spawned actor host to dial back |
| `MENTAT_AGENT_PING_INTERVAL_MS` | 2000 | Agent → daemon ping interval |
| `MENTAT_TCP_DEAD_AFTER_MS` | 75000 | TCP keepalive target (Linux only) |
| `MENTAT_PYTHON` | `python3` | Interpreter used to spawn actors |
| `MENTAT_SOCK_DIR` | `/tmp/mentat` | Actor unix socket directory |
| `MENTAT_DEBUG` | unset | Log ignored `ray` shim kwargs |

`MENTAT_GROUP` scopes placement, `ray.nodes()`, `cluster_resources()` and
`ray status`, so two models on one box never count each other's GPUs. It falls
back to `SERVICE_NAME`, then `default`.

`RAY_ADDRESS` resolves in order: the `--address` flag, the environment,
`head.json`, localhost. The CLI and the agent share it.

The API variables are announced at `ray start` and read once, so export them
first. Head-rank-only `MENTAT_OPENAI_API` is an entrypoint convention. The
agent announces whatever is set, and `mentat-serve` takes the lexically first
of several.

`MENTAT_HOST_CONNECT_TIMEOUT_MS` covers process start only. The actor host
dials back before importing anything heavy, so this times python starting up.
Leave it alone.

`MENTAT_AGENT_PING_INTERVAL_MS` bounds how fast an agent notices a dead daemon.

`MENTAT_PYTHON` and `MENTAT_SOCK_DIR` exist for the tests, which run fake nodes
on one machine. `MENTAT_DEBUG` logs the `ray` kwargs the shim ignored.

Set on each actor process: `MENTAT_ACTOR_ID`, `MENTAT_NODE_ID`,
`MENTAT_GPU_IDS` and `MENTAT_GCS_ADDRESS` from the daemon, plus
`MENTAT_AGENT_PID` from the agent.

## mentat-serve env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `SERVE_PORT` | 6381 | HTTP port |
| `MENTAT_DAEMONS` | `127.0.0.1:6380` | Seed daemon list; empty = UDP only |
| `MENTAT_ANNOUNCE_PORT` | 6382 | UDP listen port; 0 disables |
| `ALLOWED_SOURCES` | `10.100.0.,192.168.1.,127.0.0.1,::1,172.` | Address prefixes accepted for announcements |
| `SERVING_TIMEOUT_S` | 1800 | Upstream request timeout |
| `MENTAT_SECRET` | unimplemented | Reserved: announcement signing |

Unset, `MENTAT_DAEMONS` seeds the local daemon. Set empty, UDP is the only
path. Compose cannot express empty, since `${VAR:-default}` reads it as unset.

`ALLOWED_SOURCES` applies to both the datagram's source and the address it
claims. `172.` covers bridge-networked clients, which keep a `172.x` source.

`SERVING_TIMEOUT_S` caps one upstream request, and a non-streaming answer
arrives when generation ends. A 198K prefill took 144.7s to first byte, so
1800s leaves headroom. Lower it and long generations get cut first.

`mentat-serve` also reads `POLL_INTERVAL_S`, `PROBE_INTERVAL_S`,
`PROBE_TIMEOUT_S`, `PROBE_FRESH_S`, `MCP_TIMEOUT_S`, `TOOLS_TTL_S` and
`DISCOVER_PEERS`. The defaults derive from each other: `PROBE_FRESH_S` is three
probe intervals plus a timeout. Setting one alone makes groups flap.

Unparsable `*_MS` values log `bad_env_ms` and use the default. Defaults live on
`Cfg` in [config.rs](rust/src/config.rs).

## Behavior

- Registration retries forever.
- Head election: lowest node id, `MENTAT_ELECTION_HOLD_DOWN_MS` hold-down.
- Daemons mesh over `MENTAT_PEERS`; replicate events, exchange status.
- Groups scope placement, `ray.nodes()`, `cluster_resources()`, and `ray status`.
- TP=N takes N single-GPU agents or fewer multi-GPU ones.
- A second driver for one group is rejected at `ray.init`.
- Driver and all of a group's agents must reach the same daemon.
- Actors run in their own process group; kills take the tree.
- Actors are serial: a call issued after `run()` queues forever.
- Agent link EOF: calls held, drained on reconnect; degrade at 30s, give up at 60s.
- mentat-serve discovers daemons by UDP announcement, the `MENTAT_DAEMONS` seed, and mesh membership. Each watched daemon is polled on `/status` with `/events` held open.
- Announcements are unsigned. Both datagram source and claimed address must match `ALLOWED_SOURCES`, and everything claimed is re-read over TCP and probed.
- `/v1` routing gate: group has a running actor and its announced endpoint answers `/v1/models`, which is also the source of served model names. `/mcp` merge is ungated.
- Engines are admitted as soon as their API answers, which can be during a model's self-test.
- Shim covers only vLLM's audited surface. Anything else raises `AttributeError` naming the attribute.
- The entrypoints pin `VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1`. The legacy compiled-DAG surface is unimplemented.
- Verified against vLLM `0.1.dev20051+g487ecf187`. After boot the only recurring Ray call is `ray.wait` every 5s.
- `ray start --head`, `--object_store_memory`, `RAY_OBJECT_STORE_MEMORY`, `RAY_memory_monitor_refresh_ms` accepted and ignored.

One daemon owns each group's state and the rest carry replicas. When views
disagree, `mentat-serve` takes the one with more running actors.

Actors get their own process group because vLLM workers fork helpers and a kill
must take the tree. A survivor holds its memory outside `ps` RSS.

Serial actors match real Ray. `run()` never returns for a vLLM worker, so any
call after it queues forever. `call_pending_long` in the log means that
happened.

Announcements are unsigned because the control port already accepts
unauthenticated connections from the same network, so signing datagrams adds no
boundary. Every claim is re-read over TCP anyway. `MENTAT_SECRET` changes that
when the control plane gets authentication.

## Tests

GPU-free, no hardware needed. See [tests/README.md](tests/README.md).
