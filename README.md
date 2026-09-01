# mentat

`mentat` replaces Ray for vLLM and other LLM framework multi-node serving: a Rust
daemon for placement, liveness and reaping, a Rust router for HTTP, and a
pure-Python package that stubs the `ray` package.

`mentat` is barebones and lacks `ray` features intentionally (e.g.: no object store, no
memory monitor, no dashboard). 

`mentat` is friendlier to homelab clusters: Registration retries forever, so
containers can start in any order. Lifecycle events are logged clearly for
easier analysis.

`mentat` is written in Rust and is as bare metal as possible. It takes only a
few _megabytes_ of RAM to manage your cluster.

## Components

| Component | Where | Overhead |
| --- | --- | --- |
| `mentatd` — cluster daemon, agent and CLI in one binary. Control 6379, HTTP 6380. | [rust/](rust/), [crates.io](https://crates.io/crates/mentatd) | 1.7 MiB on disk, ~2.5 MiB RSS |
| `mentatd-serve` — the serving router, its own crate and container. HTTP 6381. | [serve/](serve/) | 1.7 MiB on disk, ~3.2 MiB RSS |
| `ray` shim — pure-Python package claiming the `ray` import name in model containers. `pip install --no-deps`. | [python/ray/](python/ray/) | ~2 MiB added to a running interpreter |

## Commands

- `mentatd daemon` — the per-node daemon.
- `mentatd start` — register the container's GPUs. The `ray` symlink makes this `ray start`.
- `mentatd status` / `mentatd stop` — inspect the cluster, kill actors.
- `mentatd serve` — the router, if `mentatd-serve` is on PATH.

Any name that is not built in runs `mentatd-<name>` from PATH, the way git
finds its subcommands. `mentatd serve` and `mentatd-serve` are the same
binary, so either works.

The `ray` symlink keeps `ray start` and `ray status` working, and
`ray --version` names both versions.

`mentatd daemon` takes `--port` (6379), `--http-port` (6380), `--node-ip`,
`--head-json` (`/tmp/mentat/head.json`) and `--peers`. Compose uses the
environment.

The shim reports `2.57.0` because vLLM version-checks Ray, and logs a banner at
`ray.init`. Pure Python is free: `execute_model` runs over vLLM's MessageQueue
and NCCL, zero Ray calls per token.

The daemon never touches inference traffic, so restarting the router leaves
models serving.

## Ports

- 6379/tcp — daemon control (`RAY_ADDRESS`)
- 6380/tcp — daemon HTTP
- 6381/tcp — mentatd-serve HTTP
- 6382/udp — daemon announcements

## Getting started

Install the binary and build the shim wheel:

```
cargo install mentatd
pip wheel --no-deps -w dist ./python
```

Or take both from the published image:

```
docker pull mmastrac/mentatd:0.4.0
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
 && pip install --no-deps /tmp/mentatd-0.4.0-py3-none-any.whl
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
and the limits. [GUIDE-SERVE.md](GUIDE-SERVE.md) covers serving several models
behind one endpoint. [PROTOCOL.md](PROTOCOL.md) is the wire format.

## Deploy

1. `VERSION=<ver> ./build.sh` → `mentat-artifacts:<ver>`, `mentatd:<ver>`, `mentatd-serve:<ver>`.
2. [mentatd.yaml](mentatd.yaml) → `~/compose/mentatd/` on each box, per-node `.env`. Before any model container.
3. Rebuild the model images; they `COPY --from=mentat-artifacts`.
4. `docker compose up` the models, either node first.
5. Optional: [mentatd-serve.yaml](mentatd-serve.yaml) → `~/compose/mentatd-serve/`. No `.env` required.

Step 2's ordering saves time. A container that starts first retries until a
daemon answers.

Set `MENTAT_NODE_IP` on the daemon to the address the driver sees. Left empty
it takes the default route, wrong on a multi-homed box. A container reaching
that daemon over loopback needs no setting: it claims no identity and the
daemon supplies its own.

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
- `/events` — WebSocket; snapshot on connect, then `node_join`, `node_leave`, `head_change`, `islands_changed`, `agent_register`, `agent_lost`, `agent_degraded`, `agent_dead`, `pg_created`, `pg_ready`, `pg_timeout`, `actor_spawning`, `actor_running`, `actor_dead`, `driver_connected`, `driver_disconnected`

`/events` sends a snapshot first, so a late client starts whole. `mentatd-serve`
holds the socket open and re-reads `/status` on any event.

## mentatd-serve components

- UDP listener ([main.rs](serve/src/main.rs)) — reads announcements on 6382, drops any whose source or claimed address misses `ALLOWED_SOURCES`, and adds the rest to the watch set.
- Watch set — one task per daemon HTTP address. Polls `/status` and holds `/events` open, so a cluster event re-reads immediately instead of waiting out `POLL_INTERVAL_S`.
- Event stream ([ws.rs](serve/src/ws.rs)) — the WebSocket client for `/events`. Coalesces a burst into one re-read, since boot emits many events at once.
- Group table — merges the daemon views into one group per name. Views older than three poll intervals are stale. Overlap resolves toward the daemon with more running actors.
- Endpoint resolution — turns a port-only announcement into one candidate URL per address of the announcing node, own-subnet first, each gated by `ALLOWED_SOURCES`. A verbatim URL is neither re-derived nor gated.
- Prober — probes each candidate group's `/models` and keeps the answer. Candidates are groups with a running actor and an announced OpenAI endpoint. A daemon view change wakes it, so admission does not wait a full interval after boot. With several candidate addresses it sticks to the one that answers, falls through when it stops, and re-tries the preferred one every `PROBE_PROMOTE_S`.
- Proxy ([proxy.rs](serve/src/proxy.rs)) — forwards `/v1` to the group that serves the requested model, streaming passed through.
- MCP merge ([mcp.rs](serve/src/mcp.rs)) — merges every group's management MCP into one tool list, prefixed `<group>__`, with a `tools/list` cache per group. Adds the native `serve_status` tool.
- Status view — the document behind `/`, `/healthz` and `/status.json`.

## mentatd-serve HTTP (6381)

- `/v1` — OpenAI-compatible, routed by model name, streaming passed through
- any other POST — routed by model name too, for root-level endpoints like `/tokenize`
- `/mcp` — merged per-container MCP, tools prefixed `<group>__`, plus native `serve_status`
- `/status.json` — route table, per-group health, and `uptime_s`
- `/v1/models` — currently routable models

`/`, `/healthz` and `/status.json` return the same document.

`/status.json` says why a model is missing from `/v1/models`. Each group
carries `healthy` and a `why_not`: no endpoint, no running actors, unprobed,
probe failed, or probe stale. It also carries `openai` (the address currently
routed to), `openai_candidates` (every address that could serve it, best
first) and `openai_note` (what the container's agent reported about its own
bind). A group serving off its second candidate is how a dropped link looks
from here.

Model names come from probing the group's own `/v1/models`.

## Daemon env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `MENTAT_NODE_IP` | the daemon's, over loopback | This node's cluster identity |
| `MENTAT_PEERS` | empty | Comma-separated peer control addresses |
| `MENTAT_ANNOUNCE_PORT` | 6382 | UDP announce port; 0 disables |
| `MENTAT_ANNOUNCE_ADDR` | empty | Extra unicast announce targets |
| `MENTAT_ANNOUNCE_INTERVAL_S` | 5 | Announce interval |
| `MENTAT_ANNOUNCE_IFACES` | auto | Interfaces to announce on, in preference order, with optional tags |
| `MENTAT_ANNOUNCE_ADDRS` | unset | Addresses to announce instead, same syntax |
| `MENTAT_PROBE_INTERVAL_MS` | 15000 | Reachability probe cadence, per address pair |
| `MENTAT_PROBE_TIMEOUT_MS` | 2000 | Per-probe connect and reply deadline |
| `MENTAT_ISLAND_HOLD_DOWN_MS` | 5000 | Fabric island stability before placement acts on a change |
| `MENTAT_ISLAND_PLACEMENT` | on | `off` places multi-bundle groups without the one-fabric constraint |
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

`MENTAT_PROBE_INTERVAL_MS` paces the reachability probes. One TCP connection
per (own address × peer address) pair, with the source address bound, which
is what makes the answer about cabling rather than about the routing table.
Slow on purpose: it answers "is this cable up", which changes on the
timescale of cables. `MENTAT_PROBE_TIMEOUT_MS` bounds a dropped SYN, which
the kernel would otherwise retry for minutes.

`MENTAT_ISLAND_HOLD_DOWN_MS` is `MENTAT_ELECTION_HOLD_DOWN_MS`'s argument
applied to cables. A placement group cannot be revised after the fact, so a
flapping QSFP link must not move the island boundary between two consecutive
placements.

`MENTAT_ISLAND_PLACEMENT=off` drops the one-fabric constraint on multi-bundle
placement. Groups already opt in one at a time, by carrying an `rdma` tag on
one of their nodes; this is the switch for a cluster whose probes disagree
with its cabling and no time to work out why.

`MENTAT_GPUS` lets the tests run without GPUs. Otherwise the agent counts with
`nvidia-smi`.

UDP announcement is how `mentatd-serve` finds daemons. `MENTAT_ANNOUNCE_ADDR`
adds unicast targets outside the broadcast domain. `MENTAT_ANNOUNCE_PORT=0`
turns it off. `MENTAT_ANNOUNCE_IFACES` names the interfaces explicitly when
the default guess (every up non-loopback interface bar container bridges) is
wrong. Its order is a preference: list the fast link first and a consumer
that can reach both takes it.

Names are fnmatch patterns over `*` and `?`, so one line serves a fleet whose
interface names differ:

```
MENTAT_ANNOUNCE_IFACES=en*f*np*=connectx+rdma,en*=lan
```

A pattern with no wildcard is an exact name — `en` does not match `eno1`,
unlike NCCL's implicit prefix rule. The first entry a name matches decides
its rank and tags; several interfaces matching one entry rank together, in
kernel order. There is no negation: the list is already an allowlist.

Tags travel with the address. `rdma` is the one the daemon reads: it says the
operator cabled this link into a fabric, and probing is what decides whether
that is true. Everything else is carried for consumers.

`MENTAT_ANNOUNCE_ADDRS` takes the same syntax with addresses in place of
names and replaces what the node says it answers on. It is for a node whose
advertisable address is on none of its own interfaces. Broadcast still
follows the interfaces.

A listener watches the address a datagram arrived from. `node_ip` is the
node's cluster identity, so on a multi-homed box it names a subnet the
listener may not route to. [PROTOCOL.md](PROTOCOL.md)
has the selection order.

## Agent / container env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `RAY_ADDRESS` | `head.json`, then localhost | Daemon the driver and agents rendezvous on |
| `MENTAT_GROUP` | — | Group scope for driver and agents |
| `MENTAT_DAEMON` | `RAY_ADDRESS` | Daemon address override |
| `MENTAT_OPENAI_API` | unset | Announced OpenAI endpoint (head rank only) |
| `MENTAT_MCP_API` | unset | Announced MCP endpoint (every rank) |
| `MENTAT_MODEL_PROVIDER` | unset | What serves `MENTAT_OPENAI_API`, e.g. `vllm` |
| `MENTAT_FABRIC_IP` | set by the daemon | This rank's address on the fabric its group was placed on |
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
agent announces whatever is set, and `mentatd-serve` takes the lexically first
of several.

Each takes a whole URL or a port with the host left open:

```
MENTAT_OPENAI_API=http://10.0.0.1:8000/v1   # this address, verbatim
MENTAT_OPENAI_API=http://0.0.0.0:8000/v1    # every address of this node
MENTAT_OPENAI_API=8000/v1                   # the same, said shorter
```

`MENTAT_MODEL_PROVIDER` names the engine behind the OpenAI endpoint, `vllm`
on every current image. The agent lowercases it and announces it alongside the
endpoint, so set it on the rank that sets `MENTAT_OPENAI_API`. It is optional.
Unset, the router reports the provider as unknown.

The port form is the one to prefer. A URL naming one address is reachable
only from that link, and the router resolves the port form against every
address the node announces, so it can serve whichever link it shares. The
URL form stays supported and is the escape hatch when the port form cannot
describe the server. [GUIDE-SERVE.md](GUIDE-SERVE.md) covers both.

The daemon sets `MENTAT_FABRIC_IP` on each spawned rank, rather than the
entrypoint. It names the address that rank answers on inside the fabric its
group was placed on. The shim's `ray.util.get_node_ip_address()` resolves
`VLLM_HOST_IP`, then it, then `MENTAT_NODE_IP`, so a hand-set `VLLM_HOST_IP`
always wins.

`MENTAT_HOST_CONNECT_TIMEOUT_MS` covers process start only. The actor host
dials back before importing anything heavy, so this times python starting up.
Leave it alone.

`MENTAT_AGENT_PING_INTERVAL_MS` bounds how fast an agent notices a dead daemon.

`MENTAT_PYTHON` and `MENTAT_SOCK_DIR` exist for the tests, which run fake nodes
on one machine. `MENTAT_DEBUG` logs the `ray` kwargs the shim ignored.

Set on each actor process: `MENTAT_ACTOR_ID`, `MENTAT_NODE_ID`,
`MENTAT_GPU_IDS`, `MENTAT_GCS_ADDRESS` and, for a group placed on a fabric,
`MENTAT_FABRIC_IP` from the daemon, plus `MENTAT_AGENT_PID` from the agent.

## mentatd-serve env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `SERVE_PORT` | 6381 | HTTP port |
| `MENTAT_DAEMONS` | `127.0.0.1:6380` | Seed daemon list; empty = UDP only |
| `MENTAT_ANNOUNCE_PORT` | 6382 | UDP listen port; 0 disables |
| `ALLOWED_SOURCES` | `10.100.0.,192.168.1.,127.0.0.1,::1,172.` | Address prefixes accepted for announcements |
| `SERVING_TIMEOUT_S` | 1800 | Upstream request timeout |
| `PROBE_PROMOTE_S` | 6 probe intervals | How often a fallen-through group re-tries its preferred address |
| `MENTAT_SECRET` | unset | HMAC key for announcements |
| `MENTAT_SECRET_FILE` | unset | Key from a file, wins over `MENTAT_SECRET` |

A named `MENTAT_SECRET_FILE` that will not read, or reads empty, stops the
process at boot with the reason. A container whose key mount is unreadable
would otherwise sign nothing and refuse every signed announcement its peers
send, which from outside looks like a node that never joined. Leaving both
variables unset, or `MENTAT_SECRET` empty, still runs unsigned.
| `MENTAT_UNIVERSE` | `default` | Cluster name. Foreign universes are dropped silently |

Unset, `MENTAT_DAEMONS` seeds the local daemon. Set empty, UDP is the only
path. Compose cannot express empty, since `${VAR:-default}` reads it as unset.

`ALLOWED_SOURCES` applies to the datagram's source and to any advertised
address before it is watched, which is to say to every address acted on. The
address a node calls its own is not checked, since nothing acts on it. `172.`
covers bridge-networked clients, which keep a `172.x` source. A rejected
source logs `announce_source_not_allowed` once, naming the prefixes in
force.

Set `MENTAT_SECRET` on the daemons and here alike. A keyed router takes signed
announcements only, so a half-applied rollout stops discovery until the seed
list finds the daemons instead. `MENTAT_UNIVERSE` separates clusters sharing a
broadcast domain: a foreign universe is dropped before the key is checked, so
it never logs.

`SERVING_TIMEOUT_S` caps one upstream request, and a non-streaming answer
arrives when generation ends. Lower it and long generations get cut first.

`mentatd-serve` also reads `POLL_INTERVAL_S`, `PROBE_INTERVAL_S`,
`PROBE_TIMEOUT_S`, `PROBE_FRESH_S`, `PROBE_PROMOTE_S`, `MCP_TIMEOUT_S`,
`TOOLS_TTL_S` and `DISCOVER_PEERS`. The defaults derive from each other:
`PROBE_FRESH_S` is three probe intervals plus a timeout, `PROBE_PROMOTE_S` is
six. Setting one alone makes groups flap. A group with several candidate
addresses waits a whole `PROBE_TIMEOUT_S` on each dead one, so
`PROBE_FRESH_S` has to clear a round that walks them all.

Unparsable `*_MS` values log `bad_env_ms` and use the default. Defaults live on
`Cfg` in [config.rs](rust/src/config.rs).

## Behavior

- Registration retries forever.
- Head election: lowest node id, `MENTAT_ELECTION_HOLD_DOWN_MS` hold-down.
- Daemons mesh over `MENTAT_PEERS`; replicate events, exchange status.
- Groups scope placement, `ray.nodes()`, `cluster_resources()`, and `ray status`.
- TP=N takes N single-GPU agents or fewer multi-GPU ones.
- Daemons probe reachability per (own address × peer address) pair, source address bound, and publish the matrix in `/status`.
- Addresses tagged `rdma` on both ends of a probe-ok pair form a fabric island. A placement group of more than one bundle is placed inside one island, after a hold-down. A group none of whose nodes are tagged places as before.
- Each rank of a group placed on an island is spawned with `MENTAT_FABRIC_IP`. `VLLM_HOST_IP` still wins.
- A second driver for one group is rejected at `ray.init`.
- Driver and all of a group's agents must reach the same daemon.
- Actors run in their own process group; kills take the tree.
- Actors are serial: a call issued after `run()` queues forever.
- Agent link EOF: calls held, drained on reconnect; degrade at 30s, give up at 60s.
- mentatd-serve discovers daemons by UDP announcement, the `MENTAT_DAEMONS` seed, and mesh membership. Each watched daemon is polled on `/status` with `/events` held open.
- Announcements are signed when `MENTAT_SECRET` is set. Source and claimed address must match `ALLOWED_SOURCES`, and everything claimed is re-read over TCP either way.
- Service endpoints announce as a URL or as a port with the host left open. The router resolves the port form against the announcing node's addresses and picks by probing.
- `/v1` routing gate: group has a running actor and its announced endpoint answers `/v1/models`, which is also the source of served model names. `/mcp` merge is ungated.
- Engines are admitted as soon as their API answers, which can be during a model's self-test.
- Shim covers only vLLM's audited surface. Anything else raises `AttributeError` naming the attribute.
- The entrypoints pin `VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1`. The legacy compiled-DAG surface is unimplemented.
- Verified against vLLM `0.1.dev20051+g487ecf187`. After boot the only recurring Ray call is `ray.wait` every 5s.
- `ray start --head`, `--object_store_memory`, `RAY_OBJECT_STORE_MEMORY`, `RAY_memory_monitor_refresh_ms` accepted and ignored.

One daemon owns each group's state and the rest carry replicas. When views
disagree, `mentatd-serve` takes the one with more running actors.

Actors get their own process group because vLLM workers fork helpers and a kill
must take the tree. A survivor holds its memory outside `ps` RSS.

Serial actors match real Ray. `run()` never returns for a vLLM worker, so any
call after it queues forever. `call_pending_long` in the log means that
happened.

`MENTAT_ANNOUNCE_IFACES` names the links meant to carry fabric traffic. A
bound-source TCP connection is what confirms one, because the cluster numbers
both of its fabrics out of one subnet. A mistagged link fails its probes,
logs `fabric_addr_unverified`, and stays out of placement.

`MENTAT_SECRET` signs announcements, with a timestamp and per-boot sequence
bounding replay. A keyed listener refuses unsigned ones, so the signature
cannot be stripped. The control port still has no authentication, so claims are
re-read over TCP regardless.

## Tests

GPU-free, no hardware needed. See [tests/README.md](tests/README.md).
