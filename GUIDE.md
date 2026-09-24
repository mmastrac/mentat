# mentatd

## Name

mentatd: cluster daemon, container agent and command-line client for mentat.

## Synopsis

```
mentatd daemon [--port PORT] [--http-port PORT] [--node-ip ADDR]
               [--head-json PATH] [--peers ADDR,...]
mentatd start [--address ADDR] [--block]
mentatd status [--address ADDR] [--group NAME] [--json]
mentatd stop [--address ADDR] (--group NAME | --all)
mentatd NAME [ARG...]
mentatd --version
```

## Description

The binary runs as the daemon, the agent or the client.

The daemon runs on each node, on the host network. It holds the cluster
state: nodes, agents, placement groups and actors. Daemons form a mesh over
`MENTAT_PEERS`, replicate events, elect a head and probe reachability
between their addresses.

The agent runs inside each model container. `mentatd start` starts it. The
agent registers the container's GPUs with a daemon, spawns actor processes
on request, forwards method calls to them and reports their exit.

The client is the `ray` shim in the driver process, and the `status` and
`stop` commands.

### Groups

A group is one model deployment: a driver and the agents holding its GPUs,
all sharing one `MENTAT_GROUP` value. Placement, `ray.nodes()`,
`ray.cluster_resources()` and `ray status` are scoped to the group, so two
models on one node never count each other's GPUs. Running the same model
twice makes two groups. A second driver in one group is rejected at
`ray.init`.

Every group is on the head. A daemon that is not the head relays each agent
registration and driver session it receives to the head. A container
connects to the daemon on its own box and still lands on the head with every
other rank. `RAY_ADDRESS` may point at any daemon. Its default of
`127.0.0.1:6379` is correct everywhere.

### Mesh

Each daemon dials the addresses in `MENTAT_PEERS` and every daemon those
peers publish in their status pushes. One entry that reaches any live daemon
joins the whole mesh. A daemon also listens for the announcements other
daemons broadcast, so a daemon with an empty `MENTAT_PEERS` joins by being
on the same broadcast domain. An announcement only produces a dial.
Identity, version and link ownership are settled over TCP, as for a seeded
peer. A daemon dials a peer at its seed address first and then at every
other address the peer announces, on the same port. A pair seeded over a
fabric address stays linked over the LAN while the cable is out. A peer's
address list is refreshed from its status pushes, so a renumbered or newly
cabled link reaches the probes and the islands without a relink. Two daemons
that dial each other at once keep the link the lower node id dialed.

### Placement

A placement group of N single-GPU bundles needs N single-GPU agents or fewer
multi-GPU ones. A placement group that cannot be satisfied stays PENDING and
fails after `MENTAT_PG_PENDING_TIMEOUT_MS`. While it waits, `pending_reason`
in `/status` gives the constraint.

When nodes have `rdma` tags, a placement group of more than one bundle is
placed inside one fabric island. See "Fabrics".

### Actors

Each actor runs in its own process group, so a kill removes the whole tree.
Actors are serial, as in Ray. `run()` never returns for a vLLM worker, so a
call issued after it queues forever. `call_pending_long` in the log reports
such a call.

When an agent's link to the daemon closes, calls are held and drained on
reconnect. After `MENTAT_AGENT_DEGRADED_AFTER_MS` the agent is degraded,
which is a warning. After `MENTAT_AGENT_DEAD_AFTER_MS` its actors are dead,
their `run()` refs resolve, and the driver restarts.

The end of a driver session reaps its actors and placement groups after
`MENTAT_SESSION_REAP_GRACE_MS`. A new driver session for the group kills
every live actor whose owner lacks both a session and a pending reap, such as
one adopted after a daemon restart. An actor holds its GPUs until its process
exits, and a pending placement group places when that exit arrives.

A dead actor keeps its row in `/status`. Its owner's next call on it
returns `RayActorError` with the reason it died. The daemon drops finished
records after `MENTAT_HISTORY_KEEP_MS`, and `history_swept` and
`peer_forgotten` report each drop. A group is the set of agents and actors
that refer to it, so a model removed from a compose file leaves the snapshot
once its rows age out.

### Head election

A settled head stays head while it is alive. A daemon without a head uses
the one its live peers publish. When none is published it uses the lowest
live node id. Two settled heads that meet after a partition resolve to the
lower. Every change waits `MENTAT_ELECTION_HOLD_DOWN_MS` of stability. A
connection that arrives before the first election waits for it.

A head change moves every group. The daemon that stops being head closes its
agent and driver links. The agents re-register through their local daemon's
relay and report the actors they run. The drivers reconnect under their
client ids. The new head adopts both, so the ranks keep running. The old
head logs `groups_moved` and the new head logs `actor_adopted`.

### Daemon address

`status`, `stop` and the shim resolve the daemon address in this order:

1. The `--address` flag, or the `address` argument to `ray.init`.
2. `RAY_ADDRESS`.
3. The `address` field of `/tmp/mentat/head.json`.
4. `127.0.0.1:6379`.

## Commands

### daemon

Run the cluster daemon. The control port must listen on loopback for agents
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

Control addresses of the other daemons. An entry for this daemon is skipped.

### start

Register this container's GPUs with a daemon and run the agent. By default
the agent detaches and runs beside the entrypoint with inherited stdio, so
actor output lands in the container log. `/tmp/mentat/agent.json` records
its pid and group. Registration retries until a daemon replies.

The agent reads `MENTAT_GROUP`, `MENTAT_NODE_IP`, `MENTAT_OPENAI_API`,
`MENTAT_MCP_API` and `MENTAT_MODEL_PROVIDER` once at start, so they must be
exported before this command runs.

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

Scope the output to one group. With a scope the first line is `Resources:
N.0/M.0 GPU (...)`, the line Ray's `ray status` prints and entrypoints grep.
Without the flag or the variables, the output covers the whole cluster and
omits that line.

- `--json`

Print the `/status` document. The default output is the text form.

The text form has one line per daemon, peer, fabric island, group and agent.
Under each peer, `reach from <local>: <remote>=ok/<rtt>ms ...` gives the
probe result for each address pair. `fabric N: <addr> ...` lists each
island's members by fabric address.

### stop

Kill actors. The command runs at once, whatever degrade window an agent is
inside. The driver sees the dead refs and restarts.

A scope is required. With neither flag, or both, the command prints the
groups the daemon knows and exits non-zero.

- `--address ADDR`

Daemon to send to. See "Daemon address".

- `--group NAME`

Kill this group's actors. The command does not read `MENTAT_GROUP`. The box
it runs on is rarely the intended deployment.

- `--all`

Kill every group's actors on that daemon.

### External subcommands

A name that is not built in runs `mentatd-NAME` with the remaining
arguments, looked up beside the `mentatd` executable and then on `PATH`.
`mentatd serve` runs `mentatd-serve`.

### Registering without ray

At runtime a single-rank engine needs nothing from mentat: no placement
group, no actors, no cross-node collectives. It still has to appear in
mentatd-serve. The only way into that listing is an agent registration.

```
python -m ray.register
```

ships in the shim package and performs that registration only: one
connection to the daemon, held open for as long as the endpoint is to be
served, and redialled when the daemon restarts. It reads the same
environment as `mentatd start` (`MENTAT_GROUP`, `MENTAT_OPENAI_API`,
`MENTAT_MCP_API`, `MENTAT_MODEL_PROVIDER`, `CONTAINER_NAME`,
`MENTAT_NODE_IP`, and the daemon address). Each variable has a flag of the
same name. It runs beside the engine:

```
python -m ray.register &
exec vllm serve ...
```

It does not offer GPUs. An agent without GPUs is never chosen for a bundle,
so a placement cannot arrive where nothing can host it. It announces an
endpoint only. A box whose GPUs should be placeable runs `mentatd start`.
That also works for a single-rank engine. mentatd-serve applies its actor
gate only to groups with actor rows, so a group that never requested a
placement is admitted on its endpoint probe alone either way. This module is
for an image with no `ray` shim at all.

`MENTAT_NODE_IP` is best left unset here. An agent without an address of its
own is filed under the daemon's own node when it connected from that box,
and otherwise under the address the daemon saw. Both are correct.

### The ray symlink

The binary accepts the same commands when invoked through a symlink named
`ray`. `ray start`, `ray status` and `ray stop` are `mentatd start`,
`mentatd status` and `mentatd stop`. `ray --version` prints `ray, version
2.57.0 (mentatd <ver>)`.

`ray stop` needs `--group NAME` or `--all`. Under Ray the command stops the
local node's processes. Under mentat it reaches every actor the daemon
knows. An entrypoint that runs it as cleanup fails until it sets a scope.

## Migrating from Ray

The `ray` CLI and the `ray` import name keep working. `--distributed-
executor-backend ray`, `ray start` and `ray status` behave as before.

### Audit vLLM

The shim implements the surface vLLM's `RayExecutorV2` uses, audited against
`0.1.dev20051+g487ecf187`. Any other attribute raises `AttributeError` with
the attribute at engine boot. The audit command for the vLLM in the image:

```bash
grep -rn 'ray\.' $(python -c 'import vllm,os;print(os.path.dirname(vllm.__file__))')/v1/executor/
```

The audited surface is `ray.init`, `get`, `wait`, `remote`, `kill`, `nodes`,
`cluster_resources`, `available_resources`, `ray.util.placement_group` and
`ray.util.get_node_ip_address`. A call outside it needs the shim extended
first. The audit must be re-run on every base-image change.

`VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1` is required. The legacy executor's
compiled-DAG surface is unimplemented. `@ray.remote` works on actor classes
and raises on a plain function.

### Convert the image

The wheel replaces Ray. It installs as `ray` and claims the import name:

```dockerfile
COPY --from=mmastrac/mentat-artifacts:0.13.0 /out/mentatd /usr/local/bin/mentatd
COPY --from=mmastrac/mentat-artifacts:0.13.0 /out/mentatd-0.13.0-py3-none-any.whl /tmp/
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
 && pip uninstall -y ray \
 && pip install --no-deps /tmp/mentatd-0.13.0-py3-none-any.whl
```

`--no-deps` keeps pip from resolving Ray's dependencies. The shim has none.
The shim reports `__version__ == "2.57.0"` because vLLM version-checks it.

### Adjust the entrypoint

The entrypoint exports these before `ray start`:

```bash
export VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1
export MENTAT_GROUP=mymodel           # one per model deployment
ray start                             # detaches. The agent runs beside vllm
ray status | grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1
vllm serve ... --distributed-executor-backend ray -tp 2
```

`ray status` prints exactly one line matching that regex, scoped to the
group, so a `GPU >= TP` gate keeps working.

`MENTAT_NODE_IP` is per rank and per container, and is best left unset. The
daemon files each container under the box it connected from and hands every
rank its node's address. A wrong value hangs at NCCL rendezvous. A value
that gives an address another node is known by is refused at register.

### Workarounds to delete

| Workaround | Why it can go |
|---|---|
| `RAY_OBJECT_STORE_MEMORY`, `--object_store_memory` | There is no object store. Accepted and ignored, logged once as `object_store_flag_ignored`. |
| `RAY_memory_monitor_refresh_ms` | There is no memory monitor. Nothing samples node memory or kills workers. |
| Object store size caps | There is no object store. That memory goes back to weights and KV cache. |
| Head-first startup ordering | Registration retries forever. `ray start --head` is accepted and ignored. |
| `ray stop` between runs | Actors get their own process group and a kill removes the whole tree. `ray stop` kills actors and needs a scope, see "The ray symlink". |

### Verify

```bash
mentatd status --group mymodel        # the N.0/M.0 GPU line
curl -s http://<node>:6380/status | jq .
websocat ws://<node>:6380/events      # snapshot, then lifecycle events
```

At `ray.init` the container log prints a banner with the group and daemon,
ending `-- this is NOT real Ray`. A container without that banner is on real
Ray.

A dying rank logs `event=actor_exit` with pid and signal in the container
log, and the driver's exception reports the exit code and signal.

### Rollback

Rollback is pointing the image tag back at the Ray-based build. The daemon
is inert while no agent is connected to it, so nothing on the host needs
undoing.

## Fabrics

Fabric handling applies to a cluster with more than one RDMA fabric, for
example two cabled pairs. When both fabrics share a subnet, only a probe can
tell which nodes share a cable. A cluster with one fabric needs none of it.

### Tagging links

Every node tags its links, fastest first:

```bash
MENTAT_ANNOUNCE_IFACES=en*f*np*=connectx+rdma,en*=lan
```

Names are patterns over `*` and `?`, so one line serves a fleet whose
interface names differ. The first entry a name matches decides its rank and
tags. See `MENTAT_ANNOUNCE_IFACES`.

`rdma` is the one tag the daemon reads. It means the operator cabled this
link into a fabric. Probing decides whether that holds.

### Probing

Every `MENTAT_PROBE_INTERVAL_MS`, each daemon opens one TCP connection per
address pair (own address, peer address), with the source address bound.
Binding the source makes the result describe the cabling. An unbound probe
describes the routing table. Peers are probed concurrently. A pair that
fails logs `fabric_addr_unverified` once and stays out of placement. Rows
for an address the daemon has lost, or one missing from the peer's current
list, are dropped after the round.

The result, for comparison against the patch panel:

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

A placement group of more than one bundle is placed inside one island. One
that does not fit in any island stays PENDING. `pending_reason` gives the
reason, and the pending timeout repeats it. Each rank of a group placed on
an island is spawned with `MENTAT_FABRIC_IP` set to its node's address on
that island.

### Opting in

The constraint applies per group. A group whose nodes all lack an `rdma`
tag is placed as before. Tagging one pair leaves a deployment on an untagged
pair unchanged.

There is no other opt-in. Each rank of a group placed on an island is
spawned with `MENTAT_FABRIC_IP`. The shim's `ray.util.get_node_ip_address()`
resolves `MENTAT_FABRIC_IP`, then `MENTAT_NODE_IP`, which the daemon also
sets per rank to the node's identity. The shim does not read any engine's
own address variable. That variable gives the address the engine binds,
which the engine chooses for its own reasons.

`MENTAT_ISLAND_PLACEMENT=off` on a daemon places multi-bundle groups without
the constraint. It is for a cluster whose probes disagree with its cabling.

### Node identity

Islands are derived over node ids. An agent joins its node by the box it is
on. A container that reaches its daemon over loopback, or over any address
of the daemon's own box, uses the daemon's identity. A container that
reaches a daemon on another box is filed under whichever box in the mesh
owns the address it connected from, whatever link it came in on. Neither
needs `MENTAT_NODE_IP`. Only a container on a box without a daemon is a node
of its own, identified by its source address. `MENTAT_NODE_IP` on a
container overrides all of this. It is refused when it gives an address of a
box the mesh knows under another name.

Each actor is spawned with `MENTAT_NODE_IP` set to its node's identity, so
the shim's `get_node_ip_address()` returns the same value on every rank with
nothing set in the container. A fabric address, when placement chose one, is
resolved first.

### Claims

A claim reserves a set of nodes under a name and returns the same view to
every holder of that name. Ranks that start independently agree without a
coordinator. A claim ends when its last holder disconnects.

The shim reads `MENTAT_CLAIM` and `MENTAT_CLAIM_SHAPE` at
`ray.util.placement_group`, because Ray's API cannot express a shape. With
`MENTAT_CLAIM` set, the shim claims the name first and then places inside
the claim. A group requesting more than its claim holds stays PENDING.
[PROTOCOL.md](PROTOCOL.md) defines the shape.

## Environment

An unset or empty variable uses its default. An unparsable `*_MS` value
logs `bad_env_ms` and uses the default. An unrecognised on/off value logs
`bad_env_flag` and uses the default. Every `*_MS` variable is read once at
process start.

### Daemon

- `MENTAT_NODE_IP` (default: the source address of the default route)

This node's cluster identity. On a multi-homed node it must be the address
the driver sees itself on, the same one the model containers use. The
default is the route to the internet, which on a multi-homed node is usually
the wrong interface and breaks the match between driver and node. Set but
empty reads as unset. An identity still empty after that stops the daemon.
Every such daemon would share one node id.

- `MENTAT_PEERS` (default: empty)

Comma-separated control addresses of other daemons. One that reaches any
live daemon is enough. The rest of the mesh is learned from it. The seed
should be the address a node identifies itself by, its `MENTAT_NODE_IP`. A
fabric address is renumbered when cables move, and the seed then points at a
box that is not there. `peer_connect_retry` reports that once a minute for
as long as it lasts. A seed is dialed for the life of the process. A daemon
learned from a peer stops being dialed once it has been down for
`MENTAT_HISTORY_KEEP_MS` and no live peer lists it.

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

Comma-separated list of `name` or `name=tag+tag` entries giving the
interfaces to announce on. A name is a pattern over `*` and `?`. A pattern
with no wildcard is an exact name, so `en` does not match `eno1`. The first
entry a name matches decides its rank and tags. Interfaces matching one
entry rank together at that entry's position, in kernel order. List order is
preference order. With the fast link listed first, a consumer that can reach
both uses it. There is no negation.

Tags travel with the address. `rdma` is the one tag the daemon acts on.
Every other tag is stored for consumers to read.

- `MENTAT_ANNOUNCE_ADDRS` (default: unset)

The same syntax with addresses in place of names. Replaces the address list
the node announces, for a node whose advertisable address is on none of its
own interfaces. Broadcast still follows the interfaces.

- `MENTAT_SECRET` (default: unset)

HMAC key for announcements. Every daemon and router in a cluster needs the
same one. A daemon without it logs `announce_off` and announces nothing. A
router without it exits at boot. A half-keyed cluster looks like an empty
one. The key should be mentat's own. The weaker of two services that share a
key discloses it for both.

- `MENTAT_SECRET_FILE` (default: unset)

Read the key from this file. It overrides `MENTAT_SECRET`. A file that
cannot be read, or reads empty, stops the process at boot with the reason.

- `MENTAT_UNIVERSE` (default `default`)

Cluster name. An announcement from another universe is dropped before its
signature is checked, without a log line.

- `MENTAT_PROBE_INTERVAL_MS` (default 15000)

Interval between reachability probes, per address pair. The result changes
on the timescale of cables, so the interval is long.

- `MENTAT_PROBE_TIMEOUT_MS` (default 2000)

Deadline for one probe's connect and reply. A pair with no route fails at
once. The deadline bounds a dropped SYN, which the kernel would otherwise
retry for minutes.

- `MENTAT_ISLAND_HOLD_DOWN_MS` (default 5000)

How long island membership must hold still before placement acts on a
change. A placement group cannot be revised after the fact, so a link that
flaps must not move the island boundary between two consecutive placements.

- `MENTAT_ISLAND_PLACEMENT` (default `on`)

`off` places multi-bundle groups without the one-island constraint.

- `MENTAT_PG_PENDING_TIMEOUT_MS` (default 600000)

How long a placement group may stay PENDING before it fails and its ready
ref raises in the driver. It times the whole rendezvous, from the request to
the agents and GPUs arriving. Ten minutes covers a cold node pulling images
and mounting weights. A lower value restarts the driver into the same wait.

- `MENTAT_AGENT_DEGRADED_AFTER_MS` (default 30000)

How long an agent's daemon link may be closed before the agent is marked
degraded. Calls stay held. The event is `agent_degraded`.

- `MENTAT_AGENT_DEAD_AFTER_MS` (default 60000)

How long an agent's daemon link may be closed before its actors are marked
dead, which resolves their `run()` refs and restarts the driver. The gap
between this and the degrade threshold allows for short outages.

- `MENTAT_HISTORY_KEEP_MS` (default 600000)

How long the daemon keeps a finished record for an operator to read. The
age counts from the event:

- a dead actor or removed placement group, from its end, once its owner is
  gone
- an agent, from when its link dropped
- a mesh peer, from when it was declared dead

- `MENTAT_PEER_STALE_AFTER_MS` (default 30000)

How long a mesh peer may be silent before it is logged stale.

- `MENTAT_PEER_DEAD_AFTER_MS` (default 60000)

How long a mesh peer may be silent before `node_leave` fires and its link
closes. The connector keeps re-dialing it. A dead peer keeps its row in
`/status` for `MENTAT_HISTORY_KEEP_MS`, or until the same box rejoins under
a different node id, which happens when its identity address changes.

- `MENTAT_PEER_STATUS_INTERVAL_MS` (default 2000)

Interval between status pushes to mesh peers. The push is also the heartbeat
the staleness thresholds count, so it must be several times smaller than
`MENTAT_PEER_STALE_AFTER_MS`.

- `MENTAT_ELECTION_HOLD_DOWN_MS` (default 5000)

How long a head candidate must stay best before `head_change` fires. A
higher value stops a stream of `head_change` events. Head changes then lag
by as much.

- `MENTAT_SLOW_CALL_WARN_MS` (default 15000)

A call other than `run()` pending longer than this logs `call_pending_long`
once. The call is queued behind a blocking method or the worker is stuck.

- `MENTAT_SESSION_REAP_GRACE_MS` (default 0)

Delay between a driver session ending and the reap of its actors and
placement groups. The delay runs from the session's latest end. The actors stay up for the grace, and a new driver for the
group waits for the reap to place. A driver that reopens its session inside
the grace keeps its actors. Use a grace to inspect workers after a driver
crash.

- `MENTAT_TCP_DEAD_AFTER_MS` (default 75000)

Target time for TCP keepalive to declare a wedged peer dead, set through
`TCP_KEEPIDLE`, `TCP_KEEPINTVL` and `TCP_KEEPCNT`. Linux only.

### Agent and container

- `RAY_ADDRESS` (default: `/tmp/mentat/head.json`, then `127.0.0.1:6379`)

Daemon the driver and the CLI connect to. Any daemon relays to the head, so
the default is correct on every node. See "Daemon address".

- `MENTAT_DAEMON` (default: the `--address` flag, then `127.0.0.1:6379`)

Daemon the agent registers with. Overrides `--address`.

- `MENTAT_GROUP` (default: `SERVICE_NAME`, then `default`)

The group this container belongs to. Read by the agent, the shim and the
`status` command.

- `MENTAT_NODE_IP` (default: unset)

The node this agent belongs to. Unset, the daemon files the agent under the
box it connected from. See "Node identity". It is needed only for a
container on a box without a daemon.

- `CONTAINER_NAME` (default: the hostname)

Part of the agent id, which is `<group>@<container>@<node_ip>`.

- `MENTAT_OPENAI_API` (default: unset)

The OpenAI-compatible endpoint this container announces. It belongs on the
rank that runs the API server. See [GUIDE-SERVE.md](GUIDE-SERVE.md).

- `MENTAT_MCP_API` (default: unset)

The MCP endpoint this container announces. It belongs on every rank.

- `MENTAT_MODEL_PROVIDER` (default: unset)

The engine behind `MENTAT_OPENAI_API`, for example `vllm`. Lowercased and
announced with the endpoint. It belongs on the same rank.

- `MENTAT_CLAIM` (default: unset)

Claim this name before placing, and place inside the claim. See "Claims".

- `MENTAT_CLAIM_SHAPE` (default: one `rdma` set covering the requested
  bundles)

The shape to claim, as JSON. Invalid JSON raises at
`ray.util.placement_group`.

- `MENTAT_GPUS` (default: what `nvidia-smi` reports)

Reports this many placeholder devices, for tests on nodes without GPUs. The
hardware is not read. `mentatd-probe-machine` reads it.

- `MENTAT_MACHINE` (default: unset)

The whole inventory as JSON, which skips the probe:
`{"memory": <bytes>, "cpus": <n>, "gpus": [{"index": 0, "vendor": "nvidia",
"name": "RTX 6000", "memory": <bytes>, "uma": false}]}`. For a box the probe
cannot describe, and for tests of a machine they are not running on.
Unparseable JSON stops the agent at start.

- `MENTAT_MACHINE_PROBE` (default: `mentatd-probe-machine` beside the binary,
  then on `PATH`)

The program that reports the machine inventory. It prints the object above
on stdout and exits 0. Every vendor detail is in the probe, so editing that
file fixes a part the probe does not know. A probe that is missing,
fails, or prints something else stops the agent at start. An agent that
registers zero devices would otherwise read as a scheduling bug minutes
later in another process.

- `MENTAT_HOST_CONNECT_TIMEOUT_MS` (default 60000)

How long the agent waits for a spawned actor process to connect to its
socket. The process connects before importing anything heavy, so this times
Python starting.

- `MENTAT_AGENT_PING_INTERVAL_MS` (default 2000)

Interval between the agent's `ping` messages to the daemon. It bounds how
fast the agent notices a dead daemon.

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
was placed on a fabric island and gives this rank's address on it.

## Files

- `/tmp/mentat/head.json`

Written by the daemon after it binds. Holds the control address the client
falls back to.

- `/tmp/mentat/agent.json`

Written by `mentatd start`. Holds the detached agent's pid and group.

- `/tmp/mentat/`

Unix sockets between agent and actor processes. See `MENTAT_SOCK_DIR`.

## HTTP interface

The daemon serves these on `--http-port`:

| Path | Returns |
| --- | --- |
| `/status` | JSON snapshot: node, peers, islands, groups, clients, claims, counters. `?group=NAME` scopes it |
| `/metrics` | Prometheus text |
| `/events` | WebSocket: a snapshot, then one message per event |
| `/healthz` | `ok` |

`/events` sends the snapshot first, so a late client starts whole.
PROTOCOL.md holds the event list and the path each one patches.

```
curl -s http://<node>:6380/status | jq .
curl -s http://<node>:6380/metrics
websocat ws://<node>:6380/events
```

## Diagnostics

Log lines are `key=value` pairs. Notable keys:

- `actor_exit`, with pid and signal, when a rank dies.
- `call_pending_long` when a call has waited `MENTAT_SLOW_CALL_WARN_MS`.
- `fabric_addr_unverified` when an `rdma`-tagged address does not have a
  successful probe behind it.
- `object_store_flag_ignored` when `--object-store-memory` was passed.
- `bad_env_ms` and `bad_env_flag` when a variable did not parse.
- `announce_off` when `MENTAT_ANNOUNCE_PORT=0`.
- `peer_discovered`, `peer_link_replaced`, `peer_addrs_changed` and
  `peer_forgotten` when the mesh learns, settles, follows and drops a peer.
- `history_swept` with the counts of actors, placement groups, agents and
  refs dropped.

## Limits

- Every group is on the head, and a head change moves them all. A rank sees
  that as a short reconnect.
- Actors are serial. A call after `run()` never completes.
- The audited surface holds for the vLLM it was audited against. The grep
  must be re-run on every base-image change.
- The control port does not authenticate. Announcements are signed when
  `MENTAT_SECRET` is set, and every claim in one is re-read over TCP before
  it affects routing.
- Tested on two nodes at TP=1 through TP=4, plus GPU-free suites for the
  lifecycle behaviour.

## See also

[GUIDE-SERVE.md](GUIDE-SERVE.md), [PROTOCOL.md](PROTOCOL.md),
[tests/README.md](tests/README.md).
