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

## Named placements

`claim` matches a requested shape against the measured topology and answers
with nodes and links. The name is the reservation: every holder of one name
is answered with the view the first claim produced, so ranks starting
independently agree without a coordinator between them.

```json
{"t": "claim", "name": "myjob", "shape": {
  "sets": [{"name": "tp0", "bundles": 2, "link": "rdma"},
           {"name": "tp1", "bundles": 2, "link": "rdma"}],
  "between": [{"from": "tp0", "to": "tp1", "link": "ip"}]}}
```

`link` is `rdma` (`roce`, `fabric`) or `ip` (`any`). An `rdma` set is placed
inside one fabric island, so every member reaches every other over a tagged,
probe-confirmed address. `bundles` is a count, meaning one GPU per node, or a
list giving GPUs per node.

The answer carries, per member, the node, its host, the address to bind and
the interface that address sits on. Per `between` entry it carries the link a
caller would use, both ends and the round trip observed.

Claiming a name that is held for a different shape is refused. Re-solving
would move nodes under whoever claimed first, so the caller releases the name
or picks another.

Only the head answers a claim. Two daemons solving one name against their own
views could each hand out a placement, and islands are soft-consistent between
daemons by design. A claim sent elsewhere is refused with the head's address.

A claim ends when its last holder goes, so a driver that dies gives its nodes
back with no explicit release. Holders re-send `claim` on reconnect, which is
what rebuilds the table after the head moves.

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
| `claim` | `claim_ok` | Claim a named placement, or read the one that name already holds |
| `release` | `claim_ok` | Give up a hold. The claim ends with its last holder |

Exactly one client connection per driver sets `session: true`. Its EOF ends
the session and reaps that group's actors.

## Agent messages

| Direction | Message | Purpose |
| --- | --- | --- |
| Agent → daemon | `agent_register` | GPUs, container, pid, announced service endpoints, actors to resume, unacked ref ids |
| Agent → daemon | `service_note` | A finding about an already-announced service |
| Daemon → agent | `agent_register_ok` | Assigned node id |
| Daemon → agent | `spawn` | Start an actor process |
| Agent → daemon | `spawn_result` | Pid or error |
| Daemon → agent | `call_actor` | Forward a method call |
| Agent → daemon | `actor_result` | Result for a ref |
| Agent → daemon | `actor_exit` | Exit code and signal |
| Daemon → agent | `kill` | Terminate an actor |
| Both | `ping` / `pong` | Liveness |

A service endpoint is announced in one of two forms, both read from
`MENTAT_*_API` in the container environment.

| Field | Form | Meaning |
| --- | --- | --- |
| `services` | `{"openai": "http://10.0.0.1:8000/v1"}` | One URL, used verbatim |
| `services_ports` | `{"openai": {"port": 8000, "path": "/v1"}}` | Host left open; the consumer resolves it |
| `provider` | `"vllm"` | What serves `openai`, from `MENTAT_MODEL_PROVIDER` |

`MENTAT_OPENAI_API=http://0.0.0.0:8000/v1` and `MENTAT_OPENAI_API=8000/v1`
both produce the second form. Any other value produces the first, unparsed.

The daemon stores and republishes both without interpreting either. Only the
consumer knows which of the node's links it shares, so only the consumer can
resolve a host. See "Address selection".

`service_notes` maps a service name to what the agent noticed about it after
announcing — today, that its server bound one address rather than all of
them. It is carried on `agent_register` and updated in flight by
`service_note`, whose empty `note` clears one. Advisory: it explains a failed
probe, it never causes one.

An agent reconnects with `resume` listing actors still alive, so a link
outage does not orphan them.

## Mesh messages

| Message | Purpose |
| --- | --- |
| `peer_hello` | Dialer identifies itself |
| `peer_hello_ok` | Accepter replies in kind |
| `peer_status` | Periodic snapshot push |
| `peer_event` | One replicated event. The receiver does not re-forward it |
| `probe` | Reachability probe. First frame of its own connection |
| `probe_ok` | The answer, carrying the responder's `node_id` |

Both hellos carry `node_id`, `node_ip`, `control_addr`, `http_port`, `addrs`,
`addr_tags` and `probes`. Every field but `node_id` and `node_ip` defaults to
empty or false for daemons that predate it.

`probes` is a capability bit: true means this daemon answers `probe`. A
daemon never probes a peer whose hello did not set it, so an older peer
receives no frame it would log as unknown.

Links are keep-first: if a live link to that `node_id` exists, the new one is
refused. Two daemons dialing each other under different addresses would
otherwise churn links.

The head is the lowest node id currently visible, after a hold-down.

### Probes

A probe is not sent over the mesh link. It opens its own TCP connection, and
that connection is the answer:

1. The prober binds one of its own addresses, then connects to one of the
   peer's addresses at the peer's control port.
2. It sends `probe` with its `node_id` and the address it bound.
3. The peer answers `probe_ok` with its own `node_id`.
4. The connection closes.

Success requires the reply to carry the expected `node_id`. Both fabrics in a
multi-pair cluster may be numbered out of one subnet, so an address that
answers is not by itself evidence that the intended node answered.

The local bind is the point. Without it the kernel picks a source address by
routing table, and the result reports that table's preference rather than the
cabling. Reachability is therefore a property of an address *pair* rather
than of a remote address.

One probe per (own address × peer address) pair, every
`MENTAT_PROBE_INTERVAL_MS`, bounded by `MENTAT_PROBE_TIMEOUT_MS`. Results are
published per peer under `probes` in `/status` (see "HTTP"). A pair with no
entry has not been tried, which is not the same as a pair that failed.

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

`MENTAT_ANNOUNCE_IFACES` is a comma-separated list of `name` or
`name=tag+tag`. A name is an fnmatch-style pattern over interface names, with
`*` and `?` only: no character classes, no negation, no regex. A pattern with
no wildcard is an exact name, so every configuration written before patterns
existed keeps its meaning — `en` does not match `eno1`.

    MENTAT_ANNOUNCE_IFACES=en*f*np*=connectx+rdma,en*=lan

The first entry a name matches decides its rank and its tags. Interfaces
matching one entry rank together at that entry's position, in kernel order.
List order is preference order.

`MENTAT_ANNOUNCE_ADDRS` takes the same syntax with addresses in place of
names, and replaces what the node says it answers on:

    MENTAT_ANNOUNCE_ADDRS=192.168.1.11=lan,10.100.0.1=connectx+rdma

It is for a node whose advertisable address is on no interface of its own.
It does not change where broadcast goes, which still follows the selected
interfaces.

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
by `MENTAT_SECRET` or the contents of `MENTAT_SECRET_FILE`. A
`MENTAT_SECRET_FILE` that cannot be read is fatal at boot rather than
unsigned. The verifier
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
4. Check the source address against `ALLOWED_SOURCES`, and each advertised
   address before choosing it. The address the announcement calls its own is
   not checked, because nothing acts on it. A rejected source is logged once,
   naming the configured prefixes.

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
| `addr_ifaces` | The interface each address sits on, where one was discovered |

`node_ip` is not an address a third party can necessarily reach. On a
multi-homed node it names the subnet the cluster talks on, which a host off
that subnet has no route to.

`addrs` is ordered: the node lists its preferred link first. Only the node
can rank its own links, since a consumer that can reach both sees no
difference between them. The order comes from `MENTAT_ANNOUNCE_IFACES`.

`addr_tags` maps an address to operator-defined tags, e.g.
`{"10.100.0.1": ["connectx", "rdma"]}`. One tag is interpreted: `rdma` means
the operator cabled this address into a fabric. Placement acts on it once a
probe over that address has succeeded (see "Placement"). The rest are carried
for consumers to read.

`addr_ifaces` maps an address to the interface it was found on, e.g.
`{"10.100.0.1": "enp1s0f0np0"}`. A consumer choosing a link needs the address
to dial and the interface to bind, and only the node knows the second.
Addresses named by `MENTAT_ANNOUNCE_ADDRS` have no interface behind them and
are absent from the map, which reads as unknown.

A tag on its own admits nothing. A link that fails its probes stays out of
placement however it is tagged.

A consumer picks one address per node, in this order:

1. The highest-ranked entry in `addrs` on one of its own subnets.
2. The source address of a datagram it received, which is proof of reach.
3. `link_ip`, then the rest of `addrs`, then `node_ip`.

One watch per `node_id`. A node with two links broadcasts on both, and the
datagrams differ only in source address.

### Resolving a port-announced service

A service announced under `services_ports` has no host. The consumer forms
`http://<host>:<port><path>` once per candidate host, best first:

1. Candidate hosts are the announcing node's `addrs`. An agent is joined to
   its node by matching its `node_ip` against every address that identifies a
   node in the consumer's view — `node_ip`, `link_ip`, and each entry of
   `addrs`. This is why a box's daemon and its containers must agree on
   `MENTAT_NODE_IP`.
2. Every candidate is checked against the consumer's own allowlist
   (`ALLOWED_SOURCES` in `mentatd-serve`). These are addresses the consumer
   derived and will connect to, which is what that list is for. A verbatim
   `services` URL passes no check and is used exactly as written, because the
   operator named a host.
3. Candidates on one of the consumer's own subnets sort first, preserving the
   node's own order within each half. The node ranks its links by speed
   because only it can; the consumer ranks by shared wire because only it
   can.

The consumer then probes candidates in order, keeps whichever answers, falls
through to the next when it stops answering, and periodically re-tries the
higher-ranked ones so a repaired link is taken back automatically.

## Placement

A placement group of more than one bundle is placed inside one fabric island.

An island is a set of nodes that all reach each other over addresses tagged
`rdma`, with a successful probe behind every pair. Each daemon derives islands
for itself, from its own probe table plus the tables its peers publish in
`peer_status`. Soft consistency is sufficient: one daemon decides a given
placement group, the one its driver rendezvoused with.

Derivation, in order:

1. Nodes X and Y are fabric neighbours when some address of X tagged `rdma`
   and some address of Y tagged `rdma` have a probe-ok pair between them.
2. Connected components of that graph are pruned, least-connected node first,
   until every member reaches every other. A group must fit a set whose
   members can all talk; a component alone does not guarantee that.
3. A change in membership is committed only after
   `MENTAT_ISLAND_HOLD_DOWN_MS` of stability, so a flapping cable cannot send
   consecutive placements to different islands.

The graph is over ports — one node's one address — rather than over nodes,
because a rank binds one address and every other rank has to reach that one.
A node with two fabric ports on separate links joins through whichever port
reaches all of the island, or through neither.

Islands are published under `islands` in `/status`, each with the address
every member answers on inside it. Every pair of those addresses answered a
probe.

Placement then:

- A group of one bundle is unconstrained.
- A group is unconstrained unless one of its own alive agents sits on a node
  carrying an `rdma` tag. Opting in is per group, so a tagged pair does not
  constrain a deployment on an untagged one. `MENTAT_ISLAND_PLACEMENT=off`
  disables the constraint for a whole daemon.
- A node belongs to its island, or stands as an island of one. A group whose
  bundles all fit on one node crosses no fabric.
- Candidate islands are those with enough free GPUs in the group. The
  driver's island is tried first, then the smallest sufficient one.
- Nothing spills onto the LAN. A group that fits no island stays PENDING and
  fails at `MENTAT_PG_PENDING_TIMEOUT_MS`, naming the constraint and what the
  best island offered. `pending_reason` in `/status` says the same while
  there is still time to act.

Each rank of a group placed on an island is spawned with `MENTAT_FABRIC_IP`
set to that node's address on that island. The `ray` shim resolves
`get_node_ip_address()` as `VLLM_HOST_IP` → `MENTAT_FABRIC_IP` →
`MENTAT_NODE_IP` → a UDP-socket guess, so a hand-set `VLLM_HOST_IP` always
wins and removing it is how a deployment opts in.

## HTTP

Daemon, port 6380:

| Path | Returns |
| --- | --- |
| `/status` | JSON snapshot: node, peers, islands, groups, counters |
| `/metrics` | Prometheus text |
| `/events` | WebSocket: a snapshot, then events |

Each peer entry carries `probes`: probed pairs keyed local address then
remote address, each `{ok, rtt_ms, last_ok_ms, error}`. Absent means untried.

Events: `node_join`, `node_leave`, `head_change`, `islands_changed`,
`agent_register`, `agent_lost`, `agent_degraded`, `agent_dead`, `pg_created`,
`pg_ready`, `pg_timeout`, `actor_spawning`, `actor_running`, `actor_dead`,
`driver_connected`, `driver_disconnected`.

Router, port 6381:

| Path | Returns |
| --- | --- |
| `/v1/*` | OpenAI-compatible, routed by request `model`, streamed through |
| any other POST | Routed by request `model`, for root-level endpoints such as `/tokenize` |
| `/mcp` | Merged MCP, tools prefixed `<group>__` |
| `/status.json`, `/healthz`, `/` | Route table, per-group health and selected endpoint, and `uptime_s` |

A `/v1` request naming a known but ungated model returns 503 with the gate it
failed. An unknown name returns 404. Bodies over 128 MiB are refused.

Each group carries `openai` (the candidate currently routed to),
`openai_candidates` (every candidate, best first) and `openai_note` (what the
announcing agent reported about its own bind, when it reported anything).

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
