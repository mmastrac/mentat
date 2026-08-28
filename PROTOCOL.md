# mentat wire protocol

Four link types share one framing and one message enum:

| Link | Transport | Endpoints |
| --- | --- | --- |
| Client | TCP 6379 | Python shim or CLI to daemon |
| Agent | TCP 6379 | Agent to daemon |
| Mesh | TCP 6379 | Daemon to daemon |
| Host | Unix socket | Agent to actor process |

Discovery is a separate UDP datagram (port 6382). HTTP surfaces are 6380
(daemon) and 6381 (router).

## Framing

```
u32le header_len | u32le payload_len | header (JSON) | payload (opaque)
```

Both lengths are capped at 256 MiB; a larger value is a protocol error and
closes the link. EOF at a frame boundary is a clean close.

The payload carries Python pickle bytes end to end. No Rust component
inspects it. Most messages carry an empty payload.

The header is a JSON object:

```json
{"req": 41, "t": "call_actor", ...}
```

`t` selects the message. `req` correlates a response with its request;
unsolicited messages use 0. Remaining fields are the message's own.

Unknown fields are ignored, and fields marked `#[serde(default)]` in
`rust/src/proto.rs` may be absent. Adding an optional field is
backward-compatible in both directions; removing one or changing a type is
not.

## Connection start

The first frame identifies the link type:

| First frame | Link |
| --- | --- |
| `hello` | Client |
| `agent_register` | Agent |
| `peer_hello` | Mesh |
| `host_hello` | Host |

Anything else is answered with `err` and closed.

## Client messages

Request and response, all `req`-correlated. `err` may replace any response.

| Request | Response | Purpose |
| --- | --- | --- |
| `hello` | `hello_ok` | Identify client_id, group, kind (`driver` or `cli`), and whether this is the session connection |
| `nodes` | `nodes_ok` | Node list, group-scoped |
| `cluster_resources` | `resources_ok` | Totals, group-scoped |
| `available_per_node` | `avail_ok` | Free GPUs per node |
| `create_pg` | `create_pg_ok` | Placement group over GPU bundles |
| `pg_table` | `pg_table_ok` | Placement group state |
| `remove_pg` | — | Release |
| `create_actor` | `create_actor_ok` | Payload is a pickled `(cls, args, kwargs)` |
| `call` | `call_ok` | Method call; payload is pickled args. `call_ok` carries a ref id, resolved later by `get` |
| `get` | `get_ok` | Resolve refs; payload is the pickled result |
| `wait` | `wait_ok` | Which refs are ready |
| `kill_actor` | — | Terminate one actor |
| `status` | `status_ok` | Cluster snapshot |
| `stop_all` | — | Kill actors, optionally one group's |

Exactly one client connection per driver sets `session: true`. Its EOF ends
the session and reaps that group's actors.

## Agent messages

| Direction | Message | Purpose |
| --- | --- | --- |
| Agent → daemon | `agent_register` | GPUs, container, pid, announced service endpoints, actors to resume, unacked ref ids |
| Daemon → agent | `agent_register_ok` | Assigned node id |
| Daemon → agent | `spawn` | Start an actor process |
| Agent → daemon | `spawn_result` | Pid or error |
| Daemon → agent | `call_actor` | Forward a method call |
| Agent → daemon | `actor_result` | Result for a ref |
| Agent → daemon | `actor_exit` | Exit code and signal |
| Daemon → agent | `kill` | Terminate an actor |
| Both | `ping` / `pong` | Liveness |

`services` maps a name to a URL (`openai`, `mcp`), taken from `MENTAT_*_API`
in the container environment. The daemon stores and republishes it without
interpreting it.

An agent reconnects with `resume` listing actors still alive, so a link
outage does not orphan them.

## Mesh messages

| Message | Purpose |
| --- | --- |
| `peer_hello` | Dialer identifies itself |
| `peer_hello_ok` | Accepter replies in kind |
| `peer_status` | Periodic snapshot push |
| `peer_event` | One replicated event. The receiver does not re-forward it |

Both hellos carry `node_id`, `node_ip`, `control_addr`, `http_port`, `addrs`
and `addr_tags`. `control_addr`, `http_port`, `addrs` and `addr_tags` default
to empty for daemons that predate them.

Links are keep-first: if a live link to that `node_id` exists, the new one is
refused. Two daemons dialing each other under different addresses would
otherwise churn links.

The head is the lowest node id currently visible, after a hold-down.

## Host messages

Over a unix socket in `MENTAT_SOCK_DIR`, between agent and actor process.

| Message | Purpose |
| --- | --- |
| `host_hello` | Actor process announces itself |
| `ctor` | Construct; payload is pickled `(cls, args, kwargs)` |
| `ctor_ok` / `ctor_err` | Construction result |
| `host_call` | Method call |
| `host_result` | Return value, pickled |

## Announcement datagram

UDP, port 6382, broadcast on every selected interface plus any
`MENTAT_ANNOUNCE_ADDR` unicast target. Interfaces come from
`MENTAT_ANNOUNCE_IFACES`, or every up non-loopback IPv4 interface except
container bridges.

Unsigned (version 1):

```json
{"mentat_announce": 1, "node_id": "...", "control": "10.0.0.1:6379",
 "http": "10.0.0.1:6380", "universe": "default", "addrs": ["10.0.0.1"],
 "addr_tags": {"10.0.0.1": ["connectx"]}}
```

Signed (version 2) wraps the same payload:

```json
{"p": {"mentat_announce": 2, ..., "boot_id": "...", "seq": 41, "t": 1787862155},
 "sig": "<hex>"}
```

`sig` is HMAC-SHA256 over the payload's compact JSON with sorted keys, keyed
by `MENTAT_SECRET` or the contents of `MENTAT_SECRET_FILE`. The verifier
re-serializes the payload it parsed, so every value must survive a JSON round
trip: integers and strings only. An `f64` does not round-trip and will fail
the signature for some values.

`t` is integer seconds and must be within 30 s of the receiver's clock.
`seq` must exceed the last accepted value for the same `boot_id`; a restart
issues a new `boot_id` and restarts `seq`.

Receiver rules, in order:

1. Read `universe` without verifying. If it differs from the receiver's, drop
   silently. Another cluster is not a misconfiguration.
2. With a key configured, require version 2 and a valid signature. Reject
   version 1. Without a key, accept version 1 and drop version 2.
3. Check `t` and `seq`.
4. Check the source address and the advertised address against
   `ALLOWED_SOURCES`.

An announcement is a hint. It adds one address to watch, and every claim in
it is re-read over TCP and probed before it affects routing.

## Address selection

A daemon reports three kinds of address. They are not interchangeable:

| Field | Meaning |
| --- | --- |
| `node_ip` | What the node calls itself. Its cluster identity |
| `link_ip` | The address a mesh link uses: the socket peer address inbound, the dialed address outbound |
| `addrs` | Every address the node answers on, most preferred first |
| `addr_tags` | Operator tags per address. Carried for consumers to read |

`node_ip` is not an address a third party can necessarily reach. On a
multi-homed node it names the subnet the cluster talks on, which a host off
that subnet has no route to.

`addrs` is ordered: the node lists its preferred link first. Only the node
can rank its own links, since a consumer that can reach both sees no
difference between them. The order comes from `MENTAT_ANNOUNCE_IFACES`.

`addr_tags` maps an address to operator-defined tags, e.g.
`{"10.100.0.1": ["connectx", "rdma"]}`. Nothing reads them yet. They exist so
a consumer can route classes of traffic over different links.

A consumer picks one address per node, in this order:

1. The highest-ranked entry in `addrs` on one of its own subnets.
2. The source address of a datagram it received, which is proof of reach.
3. `link_ip`, then the rest of `addrs`, then `node_ip`.

One watch per `node_id`. A node with two links broadcasts on both, and the
datagrams differ only in source address.

## HTTP

Daemon, port 6380:

| Path | Returns |
| --- | --- |
| `/status` | JSON snapshot: node, peers, groups, counters |
| `/metrics` | Prometheus text |
| `/events` | WebSocket: a snapshot, then events |

Events: `node_join`, `node_leave`, `head_change`, `agent_register`,
`agent_lost`, `agent_degraded`, `agent_dead`, `pg_created`, `pg_ready`,
`pg_timeout`, `actor_spawning`, `actor_running`, `actor_dead`,
`driver_connected`, `driver_disconnected`.

Router, port 6381:

| Path | Returns |
| --- | --- |
| `/v1/*` | OpenAI-compatible, routed by request `model`, streamed through |
| `/mcp` | Merged MCP, tools prefixed `<group>__` |
| `/status.json`, `/healthz`, `/` | Route table and per-group health |

A `/v1` request naming a known but ungated model returns 503 with the gate it
failed. An unknown name returns 404. Bodies over 128 MiB are refused.

An idempotent GET to an upstream, meaning the probe and the status poll, is
retried once on a fresh connection when the first attempt fails on an
established one. A server that closed an idle keep-alive connection is
indistinguishable from one that is down, and only the retry separates them. A
refused connection gets no retry, and neither does any POST.

## Versioning

There is no protocol version number. Compatibility rests on optional fields:
a new field is added with a default, and both directions ignore what they do
not recognise. `mentat_announce` is the exception, and it gates signing.

The `ray` shim reports version `2.57.0` because vLLM version-checks it. That
number describes the Ray API being emulated. This protocol has no bearing
on it.
