# mentatd

## Name

mentatd: cluster daemon, container agent and command-line client for mentat.

## Synopsis

```
mentatd daemon [--port PORT] [--http-port PORT] [--node-ip ADDR]
               [--head-json PATH] [--peers ADDR,...]
mentatd start [--address ADDR] [--block]
mentatd status [--address ADDR] [--group NAME] [--json]
mentatd stop [--address ADDR] [--group NAME]
mentatd NAME [ARG...]
mentatd --version
```

## Description

One binary fills three roles.

The daemon runs on each node, on the host network. It holds the cluster
state: nodes, agents, placement groups and actors. Daemons form a mesh over
`MENTAT_PEERS`, replicate events, elect a head and probe reachability between
their addresses.

The agent runs inside each model container, started by `mentatd start`. It
registers the container's GPUs with a daemon, spawns actor processes on
request, forwards method calls to them and reports their exit.

The client is the `ray` shim in the driver process, and the `status` and
`stop` commands.

### Groups

A group is one model deployment: a driver and the agents holding its GPUs,
all sharing one `MENTAT_GROUP` value. Placement, `ray.nodes()`,
`ray.cluster_resources()` and `ray status` are scoped to the group, so two
models on one node never count each other's GPUs. Running the same model
twice means two groups. A second driver in one group is rejected at
`ray.init`.

The driver and every agent of a group must reach the same daemon. Rendezvous
follows `RAY_ADDRESS`. The mesh carries observability and head election.

### Placement

A placement group of N single-GPU bundles takes N single-GPU agents or fewer
multi-GPU ones. A placement group that cannot be satisfied stays PENDING and
fails after `MENTAT_PG_PENDING_TIMEOUT_MS`. While it waits, `pending_reason`
in `/status` specifies the constraint.

When nodes carry `rdma` tags, a placement group of more than one bundle is
placed inside one fabric island. See "Fabrics".

### Actors

Each actor runs in its own process group, so a kill takes the whole tree.
Actors are serial, as in Ray. `run()` never returns for a vLLM worker, so a
call issued after it queues forever. `call_pending_long` in the log means one
did.

When an agent's link to the daemon closes, calls are held and drained on
reconnect. After `MENTAT_AGENT_DEGRADED_AFTER_MS` the agent is degraded, which
is a warning. After `MENTAT_AGENT_DEAD_AFTER_MS` its actors are dead, their
`run()` refs resolve, and the driver restarts.

A driver session ending reaps its group's actors and placement groups after
`MENTAT_SESSION_REAP_GRACE_MS`.

### Head election

The head is the lowest node id visible, after `MENTAT_ELECTION_HOLD_DOWN_MS`
of stability. Only the head answers a named placement claim. See "Named
placements".

### Daemon address

`status`, `stop` and the shim resolve the daemon address in this order:

1. The `--address` flag, or the `address` argument to `ray.init`.
2. `RAY_ADDRESS`.
3. The `address` field of `/tmp/mentat/head.json`.
4. `127.0.0.1:6379`.

## Commands

### daemon

Run the cluster daemon. The control port must answer on loopback for agents
in host-network containers and on the cluster subnet for remote agents and
peers, so a containerised daemon needs `network_mode: host`.

- `--port PORT` (default 6379)

  Control port.

- `--http-port PORT` (default 6380)

  HTTP port. See "HTTP interface".

- `--node-ip ADDR` (default `MENTAT_NODE_IP`, else the source address of the
  default route)

  This node's cluster identity. See `MENTAT_NODE_IP`.

- `--head-json PATH` (default `/tmp/mentat/head.json`)

  File the daemon writes its control address to after binding. The client
  reads it when `RAY_ADDRESS` is unset.

- `--peers ADDR,...` (default `MENTAT_PEERS`)

  Control addresses of the other daemons. An entry specifying this daemon is
  skipped.

### start

Register this container's GPUs with a daemon and run the agent. By default
the agent detaches and runs beside the entrypoint with inherited stdio, so
actor output lands in the container log. `/tmp/mentat/agent.json` records
its pid and group. Registration retries until a daemon answers.

The agent reads `MENTAT_GROUP`, `MENTAT_NODE_IP`, `MENTAT_OPENAI_API`,
`MENTAT_MCP_API` and `MENTAT_MODEL_PROVIDER` once at start. Export them
before this command.

- `--address ADDR` (default `127.0.0.1:6379`)

  Daemon to register with. `MENTAT_DAEMON` overrides it.

- `--block`

  Run the agent in the foreground.

- `--head`, `--node-ip-address ADDR`, `--port PORT`

  Accepted for Ray compatibility and ignored.

- `--object-store-memory N`

  Accepted and ignored. Logs `object_store_flag_ignored` once.

### status

Print the cluster state.

- `--address ADDR`

  Daemon to query. See "Daemon address".

- `--group NAME` (default `MENTAT_GROUP`, then `SERVICE_NAME`)

  Scope the output to one group. With a scope the first line is
  `Resources: N.0/M.0 GPU (...)`, the line Ray's `ray status` prints and
  entrypoints grep. With neither the flag nor the variables set, the output
  covers the whole cluster and has no such line.

- `--json`

  Print the `/status` document instead of the text form.

The text form has one line per daemon, peer, fabric island, group and agent.
Under each peer, `reach from <local>: <remote>=ok/<rtt>ms ...` gives the
probe result for each address pair. `fabric N: <addr> ...` lists each
island's members by fabric address.

### stop

Kill actors. Runs immediately, whatever degrade window an agent is inside.
The driver sees the dead refs and restarts.

- `--address ADDR`

  Daemon to send to. See "Daemon address".

- `--group NAME`

  Kill only this group's actors. Unset kills every group's actors. The
  command does not read `MENTAT_GROUP`.

### External subcommands

A name that is not built in runs `mentatd-NAME` with the remaining
arguments, looked up beside the `mentatd` executable and then on `PATH`.
`mentatd serve` runs `mentatd-serve`.

### The ray symlink

Invoked through a symlink named `ray`, the binary accepts the same commands.
`ray start`, `ray status` and `ray stop` are `mentatd start`, `mentatd
status` and `mentatd stop`. `ray --version` prints
`ray, version 2.57.0 (mentatd <ver>)`.

`ray stop` without `--group` kills every actor on the cluster. Remove it from
entrypoints that ran it as cleanup.

## Migrating from Ray

The `ray` CLI and the `ray` import name keep working.
`--distributed-executor-backend ray`, `ray start` and `ray status` behave as
before.

### Audit vLLM

The shim implements the surface vLLM's `RayExecutorV2` uses, audited against
`0.1.dev20051+g487ecf187`. Any other attribute raises `AttributeError` specifying
it, at engine boot. Check the vLLM in the image:

```bash
grep -rn 'ray\.' $(python -c 'import vllm,os;print(os.path.dirname(vllm.__file__))')/v1/executor/
```

The audited surface is `ray.init`, `get`, `wait`, `remote`, `kill`, `nodes`,
`cluster_resources`, `available_resources`, `ray.util.placement_group` and
`ray.util.get_node_ip_address`. A call outside it needs the shim extended
first. Re-run the audit on every base-image change.

`VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1` is required. The legacy executor's
compiled-DAG surface is unimplemented. `@ray.remote` works on actor classes
and raises on a plain function.

### Convert the image

Replace Ray with the shim. The wheel installs as `ray` and claims the import
name:

```dockerfile
COPY --from=mmastrac/mentat-artifacts:0.5.2 /out/mentatd /usr/local/bin/mentatd
COPY --from=mmastrac/mentat-artifacts:0.5.2 /out/mentatd-0.5.2-py3-none-any.whl /tmp/
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
 && pip uninstall -y ray \
 && pip install --no-deps /tmp/mentatd-0.5.2-py3-none-any.whl
```

`--no-deps` keeps pip from resolving Ray's dependencies. The shim has none.
The shim reports `__version__ == "2.57.0"` because vLLM version-checks it.

### Adjust the entrypoint

Export before `ray start`:

```bash
export VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1
export RAY_ADDRESS=10.0.0.1:6379      # the same daemon for driver and every agent
export MENTAT_GROUP=mymodel           # one per model deployment
export VLLM_HOST_IP=10.0.0.1          # this rank's cluster address

ray start --address=$RAY_ADDRESS      # detaches; the agent runs beside vllm
ray status | grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1
vllm serve ... --distributed-executor-backend ray -tp 2
```

`ray status` prints exactly one line matching that regex, scoped to the
group, so a `GPU >= TP` gate keeps working.

`VLLM_HOST_IP` is per rank and per container. A wrong value hangs at NCCL
rendezvous. "Fabrics" covers letting the daemon choose it.

### Workarounds to delete

| Workaround | Why it can go |
|---|---|
| `RAY_OBJECT_STORE_MEMORY`, `--object_store_memory` | There is no object store. Accepted and ignored, logged once as `object_store_flag_ignored`. |
| `RAY_memory_monitor_refresh_ms` | There is no memory monitor. Nothing samples node memory or kills workers. |
| Object store size caps | Same. That memory goes back to weights and KV cache. |
| Head-first startup ordering | Registration retries forever. `ray start --head` is accepted and ignored. |
| `ray stop` between runs | Actors get their own process group and a kill takes the whole tree. `ray stop` now kills actors, see "The ray symlink". |

### Verify

```bash
mentatd status --group mymodel        # the N.0/M.0 GPU line
curl -s http://<node>:6380/status | jq .
websocat ws://<node>:6380/events      # snapshot, then lifecycle events
```

At `ray.init` the container log prints a banner specifying the group and daemon,
ending `-- this is NOT real Ray`. Without it the container is on real Ray.

A rank dying logs `event=actor_exit` with pid and signal in the container
log, and the driver's exception carries the exit code and signal.

### Rollback

Point the image tag back at the Ray-based build. The daemon is inert while
no agent talks to it, so nothing on the host needs undoing.

## Fabrics

This section applies to a cluster with more than one RDMA fabric, for
example two cabled pairs. When both fabrics share a subnet, only a probe can
say which nodes share a cable. A cluster with one fabric can skip it.

### Tagging links

Tag the links on every node, fastest first:

```bash
MENTAT_ANNOUNCE_IFACES=en*f*np*=connectx+rdma,en*=lan
```

Names are patterns over `*` and `?`, so one line serves a fleet whose
interface names differ. The first entry a name matches decides its rank and
tags. See `MENTAT_ANNOUNCE_IFACES`.

`rdma` is the one tag the daemon reads. It claims that the operator cabled
this link into a fabric. Probing decides whether the claim holds.

### Probing

Each daemon opens one TCP connection per (own address × peer address) pair,
with the source address bound, every `MENTAT_PROBE_INTERVAL_MS`. Binding the
source is what makes the result describe the cabling rather than the routing
table. A pair that fails logs `fabric_addr_unverified` once and stays out of
placement.

Compare the result against the patch panel:

```bash
mentatd status          # `reach from <addr>: <addr>=ok/0ms ...` per peer
                        # `fabric 0: <addr> <addr> ...` per island
```

A pair that was cabled and reads `fail` is a cable fault or a tag on the
wrong interface. A pair that reads `ok` on a link nothing was cabled on is a
tag on the wrong interface.

### Islands

An island is a set of nodes that all reach each other over `rdma`-tagged
addresses with a successful probe behind every pair. A change in membership
is committed after `MENTAT_ISLAND_HOLD_DOWN_MS` of stability.

A placement group of more than one bundle is placed inside one island. A
group that fits no island stays PENDING and says why, in `pending_reason`
and again at the pending timeout. Each rank of a group placed on an island is
spawned with `MENTAT_FABRIC_IP` set to its node's address on that island.

### Opting in

The constraint applies per group: a group none of whose nodes carry an
`rdma` tag is placed as before. Tagging one pair leaves a deployment on an
untagged pair unchanged.

Within a tagged pair, a deployment opts in by removing `VLLM_HOST_IP` from
its environment. The shim's `ray.util.get_node_ip_address()` resolves
`VLLM_HOST_IP`, then `MENTAT_FABRIC_IP`, then `MENTAT_NODE_IP`, so a hand-set
`VLLM_HOST_IP` always wins.

`MENTAT_ISLAND_PLACEMENT=off` on a daemon places multi-bundle groups without
the constraint. It is for a cluster whose probes disagree with its cabling.

### Node identity

Islands are derived over node ids, and an agent joins its node by
`MENTAT_NODE_IP`. A container that reaches its daemon over loopback claims no
identity and takes the daemon's own, so it needs no setting. A container
reaching its daemon across the network must set `MENTAT_NODE_IP` to the
daemon's value.

### Named placements

A claim reserves a set of nodes under a name and answers every holder of
that name with the same view, so ranks starting independently agree with no
coordinator between them. A claim ends when its last holder disconnects.

The shim reads `MENTAT_CLAIM` and `MENTAT_CLAIM_SHAPE` at
`ray.util.placement_group`, since Ray's API cannot carry a shape. With
`MENTAT_CLAIM` set, the shim claims the name first and then places inside
the claim. A group asking for more than its claim holds stays PENDING.
[PROTOCOL.md](PROTOCOL.md) defines the shape.

## Environment

An unset or empty variable takes its default. An unparsable `*_MS` value
logs `bad_env_ms` and takes the default. An unrecognised on/off value logs
`bad_env_flag` and takes the default. Every `*_MS` variable is read once at
process start.

### Daemon

- `MENTAT_NODE_IP` (default: the source address of the default route)

  This node's cluster identity. On a multi-homed node set it to the address
  the driver sees itself on, which is the interface that carries
  `VLLM_HOST_IP` inside the model containers. The default is the route to
  the internet, which on a multi-homed node is usually the wrong interface
  and breaks the match between driver and node.

- `MENTAT_PEERS` (default: empty)

  Comma-separated control addresses of the other daemons.

- `MENTAT_ANNOUNCE_PORT` (default 6382)

  UDP port announcements are sent to. `0` turns announcement off and logs
  `announce_off`.

- `MENTAT_ANNOUNCE_INTERVAL_S` (default 5)

  Seconds between announcements.

- `MENTAT_ANNOUNCE_ADDR` (default: empty)

  Comma-separated unicast targets, `host` or `host:port`, for a listener
  outside every broadcast domain this node is on. Broadcast on the selected
  interfaces continues.

- `MENTAT_ANNOUNCE_IFACES` (default: every up non-loopback interface except
  container bridges, in kernel order, untagged)

  Comma-separated list of `name` or `name=tag+tag` entries specifying the
  interfaces to announce on. A name is a pattern over `*` and `?`. A pattern
  with no wildcard is an exact name, so `en` does not match `eno1`. The first
  entry a name matches decides its rank and tags. Interfaces matching one
  entry rank together at that entry's position, in kernel order. List order
  is preference order: list the fast link first and a consumer that can
  reach both takes it. There is no negation.

  Tags travel with the address. `rdma` is the one tag the daemon acts on.
  Every other tag is carried for consumers to read.

- `MENTAT_ANNOUNCE_ADDRS` (default: unset)

  The same syntax with addresses in place of names. Replaces the address
  list the node announces, for a node whose advertisable address is on none
  of its own interfaces. Broadcast still follows the interfaces.

- `MENTAT_SECRET` (default: unset)

  HMAC key for announcements. Set the same key on every daemon and router,
  or on none. A keyed listener refuses unsigned announcements.

- `MENTAT_SECRET_FILE` (default: unset)

  Read the key from this file instead of `MENTAT_SECRET`. A file that cannot
  be read, or reads empty, stops the process at boot with the reason.

- `MENTAT_UNIVERSE` (default `default`)

  Cluster name. An announcement from another universe is dropped before its
  signature is checked, without a log line.

- `MENTAT_PROBE_INTERVAL_MS` (default 15000)

  Interval between reachability probes, per address pair. The answer
  changes on the timescale of cables, so the interval is long.

- `MENTAT_PROBE_TIMEOUT_MS` (default 2000)

  Deadline for one probe's connect and reply. A pair with no route fails at
  once. The deadline bounds a dropped SYN, which the kernel would otherwise
  retry for minutes.

- `MENTAT_ISLAND_HOLD_DOWN_MS` (default 5000)

  How long island membership must hold still before placement acts on a
  change. A placement group cannot be revised after the fact, so a flapping
  link must not move the island boundary between two consecutive placements.

- `MENTAT_ISLAND_PLACEMENT` (default `on`)

  `off` places multi-bundle groups without the one-island constraint.

- `MENTAT_PG_PENDING_TIMEOUT_MS` (default 600000)

  How long a placement group may stay PENDING before it fails and its ready
  ref raises in the driver. It times the whole rendezvous, from the request
  to the agents and GPUs arriving. Ten minutes covers a cold node pulling
  images and mounting weights. A lower value restarts the driver into the
  same wait.

- `MENTAT_AGENT_DEGRADED_AFTER_MS` (default 30000)

  How long an agent's daemon link may be closed before the agent is marked
  degraded. Calls stay held. The event is `agent_degraded`.

- `MENTAT_AGENT_DEAD_AFTER_MS` (default 60000)

  How long an agent's daemon link may be closed before its actors are marked
  dead, which resolves their `run()` refs and restarts the driver. The gap
  between this and the degrade threshold allows for short outages.

- `MENTAT_PEER_STALE_AFTER_MS` (default 30000)

  How long a mesh peer may be silent before it is logged stale.

- `MENTAT_PEER_DEAD_AFTER_MS` (default 60000)

  How long a mesh peer may be silent before `node_leave` fires and its link
  closes. The connector keeps re-dialing it. A dead peer keeps its row in
  `/status` until the same node rejoins under a different node id, which
  happens when its identity address changes.

- `MENTAT_PEER_STATUS_INTERVAL_MS` (default 2000)

  Interval between status pushes to mesh peers. The push is also the
  heartbeat the staleness thresholds count, so keep it several times smaller
  than `MENTAT_PEER_STALE_AFTER_MS`.

- `MENTAT_ELECTION_HOLD_DOWN_MS` (default 5000)

  How long a head candidate must stay best before `head_change` fires. Raise
  it if `head_change` streams. Head changes then lag by as much.

- `MENTAT_SLOW_CALL_WARN_MS` (default 15000)

  A call other than `run()` pending longer than this logs
  `call_pending_long` once. The call is queued behind a blocking method or
  the worker is stuck.

- `MENTAT_SESSION_REAP_GRACE_MS` (default 0)

  Delay between a driver session ending and the reap of its actors and
  placement groups. A restarting vLLM needs the old actors' names and GPUs
  freed, so a grace delays recovery. The dead client is removed at once
  either way, so a new driver session is never blocked by the grace. Raise
  it only to inspect workers after a driver crash.

- `MENTAT_TCP_DEAD_AFTER_MS` (default 75000)

  Target time for TCP keepalive to declare a wedged peer dead, set through
  `TCP_KEEPIDLE`, `TCP_KEEPINTVL` and `TCP_KEEPCNT`. Linux only.

### Agent and container

- `RAY_ADDRESS` (default: `/tmp/mentat/head.json`, then `127.0.0.1:6379`)

  Daemon the driver and the CLI connect to. See "Daemon address".

- `MENTAT_DAEMON` (default: the `--address` flag, then `127.0.0.1:6379`)

  Daemon the agent registers with. Overrides `--address`.

- `MENTAT_GROUP` (default: `SERVICE_NAME`, then `default`)

  The group this container belongs to. Read by the agent, the shim and the
  `status` command.

- `MENTAT_NODE_IP` (default: `VLLM_HOST_IP`, then the local address toward
  the daemon)

  The node this agent belongs to, matched against the daemon's
  `MENTAT_NODE_IP`. A container that reaches its daemon over loopback claims
  no identity and takes the daemon's own.

- `CONTAINER_NAME` (default: the hostname)

  Part of the agent id, which is `<group>@<container>@<node_ip>`.

- `MENTAT_OPENAI_API` (default: unset)

  The OpenAI-compatible endpoint this container announces. Set it on the
  rank that runs the API server. See [GUIDE-SERVE.md](GUIDE-SERVE.md).

- `MENTAT_MCP_API` (default: unset)

  The MCP endpoint this container announces. Set it on every rank.

- `MENTAT_MODEL_PROVIDER` (default: unset)

  The engine behind `MENTAT_OPENAI_API`, for example `vllm`. Lowercased and
  announced with the endpoint. Set it on the same rank.

- `MENTAT_CLAIM` (default: unset)

  Claim this name before placing, and place inside the claim. See "Named
  placements".

- `MENTAT_CLAIM_SHAPE` (default: one `rdma` set covering the requested
  bundles)

  The shape to claim, as JSON. Invalid JSON raises at
  `ray.util.placement_group`.

- `MENTAT_GPUS` (default: the count from `nvidia-smi`)

  GPU count override, for tests on nodes without GPUs.

- `MENTAT_HOST_CONNECT_TIMEOUT_MS` (default 60000)

  How long the agent waits for a spawned actor process to connect to its
  socket. The process connects before importing anything heavy, so this
  times Python starting.

- `MENTAT_AGENT_PING_INTERVAL_MS` (default 2000)

  Interval between agent pings to the daemon. Bounds how fast the agent
  notices a dead daemon.

- `MENTAT_TCP_DEAD_AFTER_MS` (default 75000)

  As for the daemon.

- `MENTAT_PYTHON` (default `python3`)

  Interpreter the agent spawns actors with.

- `MENTAT_SOCK_DIR` (default `/tmp/mentat`)

  Directory for the unix sockets between agent and actor processes.

- `MENTAT_DEBUG` (default: unset)

  Set to log the `ray` keyword arguments the shim ignores.

### Actor process

The agent sets these on each actor process. `MENTAT_ACTOR_ID`,
`MENTAT_NODE_ID`, `MENTAT_GPU_IDS`, `MENTAT_GCS_ADDRESS` and
`MENTAT_AGENT_PID` are always set. `MENTAT_FABRIC_IP` is set when the group
was placed on a fabric island and specifies this rank's address on it.

## Files

- `/tmp/mentat/head.json`

  Written by the daemon after it binds. Carries the control address the
  client falls back to.

- `/tmp/mentat/agent.json`

  Written by `mentatd start`. Carries the detached agent's pid and group.

- `/tmp/mentat/`

  Unix sockets between agent and actor processes. See `MENTAT_SOCK_DIR`.

## HTTP interface

The daemon serves these on `--http-port`:

| Path | Returns |
| --- | --- |
| `/status` | JSON snapshot: node, peers, islands, groups, counters. `?group=NAME` scopes it |
| `/metrics` | Prometheus text |
| `/events` | WebSocket: a snapshot, then one message per event |
| `/healthz` | `ok` |

`/events` sends the snapshot first, so a late client starts whole. Events:
`node_join`, `node_leave`, `head_change`, `islands_changed`,
`agent_register`, `agent_lost`, `agent_degraded`, `agent_dead`,
`pg_created`, `pg_ready`, `pg_timeout`, `actor_spawning`, `actor_running`,
`actor_dead`, `driver_connected`, `driver_disconnected`.

```
curl -s http://<node>:6380/status | jq .
curl -s http://<node>:6380/metrics
websocat ws://<node>:6380/events
```

## Diagnostics

Log lines are `key=value` pairs. Lines to know:

- `actor_exit`, with pid and signal, when a rank dies.
- `call_pending_long` when a call has waited `MENTAT_SLOW_CALL_WARN_MS`.
- `fabric_addr_unverified` when an `rdma`-tagged address has no successful
  probe behind it.
- `object_store_flag_ignored` when `--object-store-memory` was passed.
- `bad_env_ms` and `bad_env_flag` when a variable did not parse.
- `announce_off` when `MENTAT_ANNOUNCE_PORT=0`.

## Limits

- One daemon owns each group. The driver and every agent must reach the
  same one.
- Actors are serial. A call after `run()` never completes.
- The audited surface holds for the vLLM it was audited against. Re-run the
  grep on every base-image change.
- The control port has no authentication. Announcements are signed when
  `MENTAT_SECRET` is set, and every claim in one is re-read over TCP before
  it affects routing.
- Tested on two nodes at TP=1 through TP=4, plus GPU-free suites for the
  lifecycle behaviour.

## See also

[GUIDE-SERVE.md](GUIDE-SERVE.md), [PROTOCOL.md](PROTOCOL.md),
[tests/README.md](tests/README.md).
