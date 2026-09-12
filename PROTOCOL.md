# mentat wire protocol 0.99

Client (Python shim or CLI), agent and mesh links run on TCP 6379; the host
link, agent to actor process, on a unix socket. All four share one framing
and one message set. Discovery is a UDP datagram on port 6382. HTTP is 6380
(daemon) and 6381 (router). `GUIDE.md` and `GUIDE-SERVE.md` cover the
variables named here.

## Version

The version is `major.minor`; this document describes `0.99`, the 1.0
candidate. The shapes here are 1.0's and the number moves once the spec is
accepted. Both opening
frames of every link set `"proto"`: `hello`/`hello_ok`,
`agent_register`/`agent_register_ok`, `peer_hello`/`peer_hello_ok`,
`probe`/`probe_ok` and `host_hello`/`ctor`. So do the snapshot and the
announcement payload.

A minor bump may add an optional field with a default, a message type, an
event kind, a snapshot key or a metric. A peer sends nothing introduced
after its counterpart's minor, so a `0.100` daemon behaves as `0.99`
toward a `0.99` agent. Any other change is major.

On a major mismatch the accepter returns `err` with its own `proto` and
closes; the dialer closes on a mismatched reply. A mesh peer of another
major is left out of election and is redialed at the normal interval.
An announcement of another major is dropped, logged once per source. An
unknown message type gets `err` and the link stays open. An unparseable
frame closes it.

## Framing

```
u32le header_len | u32le payload_len | header (JSON) | payload (opaque)
```

Each length is capped at 256 MiB; a larger value closes the link. EOF at a
frame boundary is a clean close. The payload is Python pickle bytes, passed
through untouched. Most are empty. The header is a JSON object:

```json
{"req": 41, "t": "call", "actor_id": "a1", "method": "run"}
{"req": 41, "t": "err", "error": "no such actor a1"}
```

`t` selects the message. `req` correlates a response with its request;
unsolicited messages use 0. Request `x` gets `x_ok`, or `ok` when it does
not have a result, or `err`. A message uses its subject's prefix: `pg_`
placement group, `actor_`, `agent_`, `peer_` mesh peer, `host_` actor host,
`claim_`, `ref_` object ref. The rest are bare: `hello` opens a link, and
`nodes`, `resources`, `available` and `status` read cluster state. Memory
and byte counts are integers, in bytes. Ray-shaped responses (`nodes_ok`,
`resources_ok`, `available_ok`, `pg_table_ok`) use floats because Ray does;
every other number is an integer.

## Connection start

The first frame identifies the link: `hello` (client), `agent_register`
(agent), `peer_hello` (mesh), `probe` (one connection per probe) or
`host_hello` (host). Anything else gets `err` and closed.

A non-head daemon relays a `hello` or `agent_register` connection to the
head: it sends the first frame there and pipes bytes both ways until either
side closes. A frame with an empty `node_ip` gets the relaying daemon's
first, so the head files the client under the box it is on. The connection
waits for the first election, and gets `err` after thirty seconds without a
head.

## Groups

A group is one model deployment: a driver and the agents holding its GPUs,
all naming the same `MENTAT_GROUP`. The name is an opaque string. A group
exists while any agent, actor or client names it, and does not need
creating.

Group scoping keeps two models on one node from counting each other's GPUs.
`nodes`, `resources`, `available` and placement report for the sending
client's group, from its `hello`. `status` and `actor_stop` accept a group as
an argument, since an operator reads from outside any one deployment. The
snapshot files agents, actors and placement groups under `groups`.

One driver holds a group: a `hello` with `session: true` is refused when
that group already has a session.

## Ids

The daemon mints an id for every actor and placement group, and a ref for
every result that is not ready yet. Each has a one-letter type prefix, so
a holder can tell what it has and the daemon dispatches without guessing.

| Id | Shape | Minted by |
| --- | --- | --- |
| Actor | `a:<32 hex>` | `actor_create` |
| Placement group | `p:<32 hex>` | `pg_create` |
| Call ref | its actor's id and a counter: `a:<32 hex>:<n>` | `actor_call` |

A ref is a handle to a result that does not exist yet. `actor_call` returns
one at once and the value arrives later, so `ref_get` fetches it and
`ref_wait` reports which of several are ready. A call ref embeds its actor,
which is how one actor's death resolves every ref outstanding against it.

`ref_get` and `ref_wait` accept a call ref or a placement group id. A
placement group resolves once it reaches `CREATED`, so a driver waits on the
id `pg_create` returned and does not need a separate handle. A placement
group that was removed resolves as `actor_died`, with `pending_reason` as
the reason.

Node, agent and client ids are plain strings, and so is a claim name. A
holder never resolves one: they are keys in the snapshot and fields in
`hello` and `agent_register`.

## Client link

| Request | Fields | Response |
| --- | --- | --- |
| `hello` | `proto`, `client_id`, `group`, `session`, `kind`, `node_ip` | `hello_ok`: `proto`, `node_id`, `node_ip`, `control_addr`, `head_node_id` |
| `nodes` | | `nodes_ok`: `nodes`, one Ray-shaped row per node |
| `resources` | | `resources_ok`: `resources`, the row keys summed over the group |
| `available` | | `available_ok`: `nodes`, node id to the row keys, with `GPU` the free device count and `memory` the total, since memory is never reserved |
| `pg_create` | `bundles`, `strategy`, `claim` | `pg_create_ok`: `pg_id` |
| `pg_table` | `pg_id` | `pg_table_ok`: `table` |
| `pg_remove` | `pg_id` | `ok` |
| `actor_create` | `name`, `num_gpus`, `pg_id`, `bundle_index`, `env`. Payload: pickled `(cls, args, kwargs)` | `actor_create_ok`: `actor_id`, `node_id`, `gpu_ids` |
| `actor_call` | `actor_id`, `method`. Payload: pickled `(args, kwargs)` | `actor_call_ok`: `ref_id`, resolved later by `ref_get` (see "Ids") |
| `actor_kill` | `actor_id` | `ok` |
| `ref_get` | `ref_id`, `timeout_ms` | `ref_get_ok`: `status`, `reason`. Payload: the pickled result |
| `ref_wait` | `ref_ids`, `num_returns`, `timeout_ms` | `ref_wait_ok`: `ready` |
| `status` | `group` (optional) | `status_ok`: `snapshot` |
| `actor_stop` | `group`, or `all` | `ok` |
| `claim` | `name`, `shape` | `claim_ok`: `name`, `generation`, `view` |

`kind` is `driver` or `cli`. Exactly one connection per driver sets
`session: true`, and a second in one group is refused. `node_ip` is empty
from the client. The session's EOF starts a reap: after
`MENTAT_SESSION_REAP_GRACE_MS` the daemon kills the driver's actors, removes
its placement groups and drops its claims. The client id is dropped at once,
so a driver restarting inside the grace opens its session without waiting. A
non-head daemon skips the reap, leaving the session to the new head.

A node row, with the daemon's own node always present:

```json
{"NodeID": "...", "NodeManagerAddress": "10.0.0.1", "Alive": true,
 "Resources": {"GPU": 2.0, "CPU": 20.0, "memory": 137438953472.0,
               "object_store_memory": 0.0, "node:10.0.0.1": 1.0}}
```

| Field | Value |
| --- | --- |
| `bundles` | Whole GPUs per bundle |
| `strategy` | Recorded and echoed by `pg_table`. Placement packs, and logs `pg_strategy_ignored` for anything else |
| `num_gpus` | A whole number |
| `claim` | A claim this placement group sits inside, or empty |
| `table` | Ray's: `placement_group_id`, `name`, `strategy`, `state`, `bundles` (index string to `{"GPU": n}`), `bundles_to_node_id`, `stats` |
| `state` | `PENDING`, `CREATED` or `REMOVED` |
| `timeout_ms` | Null blocks, 0 polls |
| `ref_get_ok.status` | `ok`, `error`, `actor_died` or `timeout`, with `reason` filled for `actor_died` |

`actor_stop` kills one group's actors, or every group's with `all`. The
agents stay registered and the driver reconnects, so the group continues. A
request with neither, or both, is refused and lists the groups that exist.

`all` must be spelled because the binary is also installed as `ray`. Under Ray,
`ray stop` stops the local node's processes; here it reaches every group the
daemon knows, so an entrypoint running it as cleanup fails until someone
sets a scope.

## Claims

A claim reserves nodes and links under a name. `claim` matches a shape
against the probed topology and replies with the set it chose. The name
holds the reservation: every holder gets the view the first claim produced,
so ranks starting independently agree without a coordinator.

A name belongs to the claiming client's group, so two groups picking one
name hold two claims. A claim chooses nodes and the placement groups inside
it reserve the GPUs, so two groups may claim one node and compete for its
devices.

```json
{"t": "claim", "name": "myjob", "shape": {
  "sets": [{"name": "tp0", "bundles": 2, "link": "rdma", "vendor": "nvidia"},
           {"name": "tp1", "bundles": [1, 1], "link": "rdma"}],
  "between": [{"from": "tp0", "to": "tp1", "link": "ip"}]}}
```

| Field | Value |
| --- | --- |
| `bundles` | A count of nodes at one GPU each, or a list of GPUs per node |
| `link` | `rdma` or `ip`. An `rdma` set goes inside one fabric island (see "Placement") |
| `vendor` | Pins the set to one GPU vendor. Without it the daemon picks one |

```json
{"t": "claim_ok", "name": "myjob", "generation": 3, "view": {
  "sets": {"tp0": [{"node": "...", "host": "n1", "vendor": "nvidia",
                    "bind": "10.0.0.1", "iface": "enp1s0f0np0",
                    "tags": ["connectx", "rdma"]}]},
  "between": [{"from": {"set": "tp0", "node": "...", "host": "n1",
                        "addr": "192.168.1.11", "iface": "eno1"},
               "to": {"set": "tp1", "node": "...", "host": "n2",
                      "addr": "192.168.1.12", "iface": "eno1"},
               "rtt_ms": 0}]}}
```

| Field | Value |
| --- | --- |
| `bind` | The address the rank binds |
| `iface` | Null for an address from `MENTAT_ANNOUNCE_ADDRS` |
| `rtt_ms` | The round trip a probe observed |

A claim on a name held for a different shape is refused: re-solving would
move nodes under whoever claimed first. Only the head solves a claim, since
two daemons solving one name against their own views could each hand out a
placement, so a claim sent elsewhere is refused with the head's address.

Each session sending `claim` joins the holder set, and the claim ends when
the last holder's session is reaped. Reaping is the only release. The reap
grace applies, so a claim lasts `MENTAT_SESSION_REAP_GRACE_MS` past its
driver, and a restart inside that window under the same name gets the view
it had. A session cut by a head change keeps its claim: the holder re-sends
`claim` to the new head, rebuilding the table there. Only sessions hold a
claim, so a node leaving the mesh does not end one.

`pg_create` with `claim` set places among the nodes the claim chose. A
placement group requesting more than its claim holds stays pending, since
spilling outside it would split ranks that agreed on one view.

## Agent link

| Direction | Message | Fields |
| --- | --- | --- |
| Agent → daemon | `agent_register` | `proto`, `agent_id`, `group`, `node_ip`, `container`, `pid`, `machine`, `services`, `resume`, `unacked_refs` |
| Daemon → agent | `agent_register_ok` | `proto`, `node_id` |
| Daemon → agent | `actor_spawn` | `actor_id`, `name`, `env`, `gpu_ids`, `owner`. Payload: pickled `(cls, args, kwargs)`. `MENTAT_NODE_ID` and `MENTAT_GCS_ADDRESS` reach the process through `env` |
| Agent → daemon | `actor_spawn_result` | `actor_id`, `ok`, `error`, `pid` (0 when the failure came before the fork) |
| Daemon → agent | `actor_dispatch` | `actor_id`, `ref_id`, `method`. Payload: pickled `(args, kwargs)` |
| Agent → daemon | `actor_result` | `ref_id`, `ok`, `error`. Payload: pickled result or exception |
| Agent → daemon | `actor_exit` | `actor_id`, `exit_code`, `signal`, each nullable |
| Daemon → agent | `actor_kill` | `actor_id`. The client message, forwarded unchanged |
| Agent → daemon | `service_note` | `service`, `note`. Empty `note` clears |
| Agent → daemon | `ping` | Sent every `MENTAT_AGENT_PING_INTERVAL_MS`; the daemon replies `pong` on the same `req`. The send fails once the daemon is gone. The daemon learns of a lost agent from EOF and does not send `ping` |

```json
{"t": "agent_register", "proto": "0.99", "agent_id": "g1", "group": "glm",
 "node_ip": "10.0.0.1", "container": "glm-0", "pid": 412,
 "machine": {
   "memory": 137438953472, "cpus": 20,
   "gpus": [{"index": 0, "vendor": "nvidia", "name": "RTX 6000",
             "memory": 51539607552, "uma": false}]},
 "services": {
   "openai": {"host": "", "port": 8000, "path": "/v1", "provider": "vllm",
              "note": ""}},
 "resume": [{"actor_id": "a1", "name": "w0", "gpu_ids": [0], "pid": 9312,
             "owner": "c1", "pending_refs": ["a1:7"]}],
 "unacked_refs": ["a1:8"]}
```

| Field | Value |
| --- | --- |
| `actor_result.error` | Set, with an empty payload, when mentat itself failed. Only a Python failure has an exception to pickle |
| `resume` | Actors alive across a reconnect, each with its `owner`, so a daemon that lost its state rebuilds who owns what |
| `unacked_refs` | Ref ids whose results are buffered agent-side and follow the register. The daemon holds them pending until they arrive |

| Field | Value |
| --- | --- |
| `machine.memory` | Total system memory |
| `machine.gpus` | Every device the agent may bind, in device order |
| `index` | What `spawn.gpu_ids` lists |
| `name` | The vendor's product string |
| `memory` | The device's own memory |
| `uma` | True for a device sharing the system pool (DGX Spark) |

A UMA device's `memory` is that pool, the same figure as the machine's, so
adding the two counts it twice.

`services` maps a service name to where it listens. The consumer forms
`http://<host>:<port><path>`, resolving an empty `host` itself (see "Address
selection").

| Field | Value |
| --- | --- |
| `host`, `port`, `path` | From `MENTAT_<NAME>_API`: `8000/v1` and `http://0.0.0.0:8000/v1` give an empty `host`, `http://10.0.0.1:8000/v1` sets it, any other value is an error at start |
| `provider` | From `MENTAT_MODEL_PROVIDER`. Names what serves the endpoint; the daemon stores and forwards it unread |
| `note` | What the agent found after announcing, such as its server binding one address. A failed probe quotes it |

## Mesh link

| Message | Fields |
| --- | --- |
| `peer_hello` | `proto`, `node_id`, `node_ip`, `control_port`, `http_port`, `addrs`, `addr_tags`, `addr_ifaces` |
| `peer_hello_ok` | The same fields, for the accepter |
| `peer_status` | `snapshot`, pushed every `MENTAT_PEER_STATUS_INTERVAL_MS`. A peer is alive while its pushes arrive |
| `peer_event` | `event`, one replicated event |
| `probe` | `proto`, `node_id`. First frame of its own connection |
| `probe_ok` | `proto`, `node_id` of the responder |

One link per `node_id`: when two exist both ends keep the one dialed by the
lower node id. A daemon dials `MENTAT_PEERS` and every control address its
live peers publish under `peers`. A peer unreachable at its seed address is
dialed at each address it last announced, on the seed's port. `addrs`,
`addr_tags` and `addr_ifaces` come from the hello and refresh on every
status push.

A settled head stays head while alive. A daemon with no head uses the one
its live peers publish in `head_node_id`, or the lowest live node id if none
is published. Two settled heads that meet resolve to the lower. Every change
waits `MENTAT_ELECTION_HOLD_DOWN_MS`. A daemon that stops being head closes
its agent and driver links so both re-register.

### Probes

The prober binds one of its own addresses, connects to one of the peer's at
the peer's control port, sends `probe`, reads `probe_ok` and closes. Success
requires the expected `node_id` in the reply: both fabrics in a multi-pair
cluster may share a subnet, so an address replying does not prove the
intended node did. Binding the local address makes the result describe the
cabling; without it the result reports the routing table's preference.

Each daemon probes every (own address × peer address) pair once per
`MENTAT_PROBE_INTERVAL_MS`, times out at `MENTAT_PROBE_TIMEOUT_MS`, and
probes peers in parallel. Results appear per peer under `probes` in the
snapshot, and an entry exists once the pair has been tried. A row whose
local address this box has lost, or whose remote address the peer stopped
listing, is dropped after the round.

## Host link

Over a unix socket in `MENTAT_SOCK_DIR`, between agent and actor process.
The socket is per actor, so the process identifies itself by connecting.

| Message | Fields |
| --- | --- |
| `host_hello` | `proto`. The process is ready for `ctor`. The shim ships in the model image and the agent in the daemon's, so both ends check the major here |
| `ctor` | `proto`. Payload: pickled `(cls, args, kwargs)` |
| `ctor_ok` / `ctor_err` | `ctor_err` sends `error`, the exception's repr, with the pickled exception |
| `host_call` | `ref_id`, `method`. Payload: pickled `(args, kwargs)` |
| `host_result` | `ref_id`, `ok`. Payload: pickled result or exception |

## Announcement datagram

UDP, port 6382, broadcast on every selected interface plus any
`MENTAT_ANNOUNCE_ADDR` unicast target. The daemon sends and the router
listens. Interface selection, address ranking and tags come from
`MENTAT_ANNOUNCE_IFACES` and `MENTAT_ANNOUNCE_ADDRS`. Every announcement is
signed:

```json
{"p": {"proto": "0.99", "node_id": "...", "universe": "default",
       "control": "10.0.0.1:6379", "http": "10.0.0.1:6380",
       "addrs": ["10.0.0.1"], "addr_tags": {"10.0.0.1": ["connectx", "rdma"]},
       "boot_id": "6d6474919bbe7beb", "seq": 41, "t": 1787862155},
 "sig": "<hex>"}
```

`sig` is HMAC-SHA256 over the payload's compact JSON with sorted keys, keyed
by `MENTAT_SECRET_FILE`'s contents or else `MENTAT_SECRET`. A named file
that cannot be read, or reads empty, is fatal at boot. Without a key the
daemon does not announce and the router does not listen; each logs that at
boot. The verifier re-serializes the payload it parsed, so every value must
survive a JSON round trip: integers and strings only. `t` is integer
seconds, within 30 s of the receiver's clock. `seq` must exceed the last
accepted for the same `boot_id`, and a restart issues a new `boot_id` and
restarts `seq`.

The datagram is at most 1400 bytes, one Ethernet frame. A sender over that
logs `announce_too_large` and sends nothing until it fits. The listener
reads a buffer of at least that size and drops anything longer.

The receiver reads `universe` first, without verifying, and drops a foreign
one without logging, since another cluster on the same broadcast domain is
expected. It then verifies the signature, logging a bad one once per source,
and checks `proto`, `t` and `seq`. The source address, and each advertised
address before it is chosen, must pass `ALLOWED_SOURCES`; a rejection is
logged once. An announcement is a hint. For the router it adds one address to
watch; for a daemon it produces one dial, and `peer_hello` then settles
identity, version and link ownership. Every field is re-read over TCP and
probed before it affects routing, so an empty `MENTAT_PEERS` joins a daemon
by putting it on the same broadcast domain.

## Address selection

| Field | Value |
| --- | --- |
| `node_ip` | What the node calls itself, which fixes the subnet the cluster talks on. A host off that subnet may not reach it |
| `link_ip` | The address a mesh link uses: the socket peer address inbound, the dialed address outbound |
| `addrs` | Every address the node listens on, most preferred first, since only the node can rank its own links |
| `addr_tags` | Each address to its operator tags |
| `addr_ifaces` | Each address to the interface it sits on. Addresses from `MENTAT_ANNOUNCE_ADDRS` are absent |

One tag is interpreted: `rdma` means the operator cabled this address into a
fabric, and placement acts on it once a probe over it has succeeded. A rank
binds an interface and only the node knows which, which `addr_ifaces`
supplies.

A consumer picks one address per node: the highest-ranked entry in `addrs`
on one of its own subnets, then the source address of a datagram it
received, which is proof of reach, then `link_ip`, the rest of `addrs` and
`node_ip`. One watch per `node_id`. A node with two links broadcasts on
both, the datagrams differing only in source address, and the unwatched one
is kept as an alternate for when the watched one stops replying.

A service with an empty `host` resolves against its node's `addrs`. The
agent joins its node by matching its `node_ip` against `node_ip`, `link_ip`
and every entry of `addrs`, so a node's daemon and its containers must agree
on `MENTAT_NODE_IP`. Every candidate passes the consumer's
`ALLOWED_SOURCES`. A service with a `host` is used as written, because the
operator named it. Candidates on the consumer's own subnets sort first,
keeping the node's order within each half. The consumer probes in order,
keeps whichever replies, falls through when it stops, and periodically re-
tries the higher-ranked ones.

## Placement

A placement group reserves whole devices. Placement ignores memory in 0.99,
and a `uma: true` device is one device like any other. The
bundles of one placement group go on GPUs of one vendor because no
collective spans vendors: a claim set's `vendor` pins which, and an
unclaimed placement group uses the first vendor that fits.

A placement group of more than one bundle goes inside one fabric island: a
set of nodes that all reach each other over addresses tagged `rdma`, with a
successful probe behind every pair. Each daemon derives islands from its own
probe table and the tables peers publish in `peer_status`, pruning each
connected component least-connected node first until every member reaches
every other. Soft consistency is enough, since the daemon a driver reached
decides its placement groups. Membership commits after
`MENTAT_ISLAND_HOLD_DOWN_MS` of stability, so a flapping cable cannot send
consecutive placements to different islands. The vertices are addresses,
since a rank binds one address every other rank must reach: a node with two
fabric ports on separate links joins through whichever reaches all of the
island, or through neither.

A placement group of one bundle is unconstrained, and so is one whose group
does not have an alive agent on a node tagged `rdma`.
`MENTAT_ISLAND_PLACEMENT=off` disables the constraint daemon-wide. A node
belongs to its island or stands as an island of one, so a placement group
that fits on a single node stays off the fabric. Candidate islands are those
with enough free GPUs of one vendor: the driver's island first, then the
smallest sufficient one. One that fits nowhere stays `PENDING`, with
`pending_reason` giving the constraint and what the best island offered, and
fails the same way at `MENTAT_PG_PENDING_TIMEOUT_MS`. Each rank is spawned
with `MENTAT_FABRIC_IP` set to its node's address on the island.

## Snapshot

`status_ok`, `peer_status`, `/status` and the first `/events` frame contain
the same object. `?group=` on `/status` and `group` on `status` scope it.

```json
{"proto": "0.99", "node_id": "...", "node_ip": "10.0.0.1", "hostname": "n1",
 "control_addr": "10.0.0.1:6379", "head_node_id": "...", "head_generation": 3,
 "addrs": [...], "addr_tags": {...}, "addr_ifaces": {...},
 "islands": [{"nodes": ["..."], "addrs": {"<node_id>": "10.0.0.1"}}],
 "peers": {"<node_id>": {
   "node_ip": "...", "link_ip": "...", "addrs": [...], "addr_tags": {...},
   "addr_ifaces": {...}, "control_port": 6379, "http_port": 6380,
   "alive": true, "stale": false, "last_seen_ms": 0, "dead_since_ms": null,
   "probes": {"<local>": {"<remote>": {"ok": true, "rtt_ms": 0,
                                       "last_ok_ms": 0, "error": ""}}},
   "groups": {"<group>": {"gpus_total": 2, "gpus_used": 1}}}},
 "clients": {"<client_id>": {"group": "glm", "kind": "driver",
                             "node_id": "...", "session": true}},
 "groups": {"<group>": {
   "claims": {"<name>": {"generation": 3, "holders": ["<client_id>"],
                         "sets": {"tp0": 2, "tp1": 2}}},
   "agents": {"<agent_id>": {
     "node_id": "...", "node_ip": "...", "container": "glm-0", "pid": 412,
     "alive": true, "degraded": false, "gone_since_ms": null,
     "machine": {...}, "gpus_free": [1], "services": {...}}},
   "actors": {"<actor_id>": {
     "name": "w0", "node_id": "...", "gpu_ids": [0], "state": "running",
     "reason": "", "pid": 9312}},
   "placement_groups": {"<pg_id>": {
     "bundles": [1, 1], "strategy": "PACK", "state": "CREATED", "claim": "",
     "pending_reason": null, "island_nodes": 2}},
   "gpus_total": 2, "gpus_used": 1}},
 "counters": {"actors_spawned": 0, "actor_exits_clean": 0, "actor_exits_signal": 0,
              "actor_exits_error": 0, "calls_total": 0, "clients_total": 0,
              "agents_registered": 0, "relayed": 0}}
```

Every collection is keyed by id, which the row itself omits, so an event can
address one row by path.

| Field | Value |
| --- | --- |
| `machine`, `services` | The agent's registration verbatim |
| `gpus_free` | Device indices open to a new bundle |
| `gpus_total`, `gpus_used` | Devices across the group's alive agents |
| actor `state` | `spawning`, `running` or `dead`, with `reason` filled for `dead` |
| `clients` | The connections this daemon serves. `session` marks the one whose EOF reaps the group |
| `claims` | Filled by the head alone, so empty elsewhere |
| `sets` | Each set's node count |

The addresses and interfaces behind `sets` are in the view `claim` returns,
too large to push on an interval.

## Events

An event is a change to the snapshot. `patch` lists paths into it and the
values to store there, so an event borrows its shapes: each value is the row
"Snapshot" defines for that path.

```json
{"type": "actor_running", "seq": 12, "ts_ms": 1787862155000, "node": "...",
 "patch": [{"at": ["groups", "glm", "actors", "a1"],
            "value": {"name": "w0", "node_id": "...", "gpu_ids": [0],
                      "state": "running", "reason": "", "pid": 9312}}]}
```

| Field | Value |
| --- | --- |
| `at` | An array of keys rather than a joined string, since a group name and an id are opaque and either may hold any character |
| `value` | The whole row, so applying one is a replace and a reader holds either the old row or the new. Absent removes the path |
| `why` | Optional free text for a log |

A program reads the row rather than `why`: `gone_since_ms` gives the figure
`agent_degraded` describes in words.

Paths below are written with `/` for reading. Each is the array of its keys.

| Event | Path |
| --- | --- |
| `node_join`, `node_leave` | `peers/<node_id>` |
| `head_change` | `head_node_id` and `head_generation` |
| `islands_changed` | `islands` |
| `agent_register`, `agent_lost`, `agent_degraded`, `agent_dead` | `groups/<group>/agents/<agent_id>` |
| `pg_created`, `pg_ready`, `pg_timeout` | `groups/<group>/placement_groups/<pg_id>` |
| `actor_spawning`, `actor_running`, `actor_dead` | `groups/<group>/actors/<actor_id>` |
| `driver_connected`, `driver_disconnected`, `driver_gone_reaping` | `clients/<client_id>` |
| `claim_solved`, `claim_released` | `groups/<group>/claims/<name>` |

An event that empties a group also patches its `gpus_used`, and one changing
membership patches `gpus_total`, since neither follows from the row alone.

A consumer applies events in `seq` order per originating `node` and re-reads
the snapshot on a gap, since a missed event leaves the view wrong. Counters
move without events, so a consumer reading them re-reads on an interval too.
The first `/events` frame is a snapshot registered under the subscription's
lock, so nothing falls between it and the first event.

Each daemon replicates its own events to every live peer in `peer_event`,
and delivers a peer's event to its own subscribers without re-forwarding it.
A replicated event describes the originating node's snapshot, while a
receiving daemon's snapshot summarises its peers rather than holding their
rows. A consumer therefore applies only events whose `node` is the daemon it
is reading, reading every other node from that node's own stream.

## HTTP

Daemon, port 6380:

| Path | Returns |
| --- | --- |
| `/healthz` | `ok` |
| `/status` | The snapshot. `?group=` scopes it |
| `/metrics` | Prometheus text |
| `/events` | WebSocket: `{"type": "snapshot", "seq": N, "data": {...}}`, then events |

| Metric | Labels | Meaning |
| --- | --- | --- |
| `mentat_build_info` | `version` | Always 1 |
| `mentat_agents` | `group` | Alive agents |
| `mentat_gpus_total`, `mentat_gpus_used` | `group`, `vendor` | Devices on alive agents, and those held by a live bundle |
| `mentat_gpu_memory_bytes` | `group`, `vendor` | Sum of device `memory` on alive agents |
| `mentat_memory_bytes` | `group` | Sum of `machine.memory` on alive agents. A UMA node counts its pool in both |
| `mentat_actors` | `group`, `state` | Actors by state |
| `mentat_actor_exits_total` | `kind` | `clean`, `signal`, `error` |
| `mentat_actors_spawned_total`, `mentat_calls_total`, `mentat_clients_total`, `mentat_agents_registered_total`, `mentat_relayed_total` | | The snapshot's counters |
| `mentat_event_subscribers`, `mentat_peers`, `mentat_is_head`, `mentat_head_generation` | | Gauges |

Router, port 6381:

| Path | Returns |
| --- | --- |
| `GET /v1`, `/v1/models` | The routable models; the router serves this itself |
| `POST /v1/*` | Routed by request `model`, streamed through |
| any other POST | Routed by request `model`, for root-level endpoints such as `/tokenize` |
| `/mcp` | Merged MCP in a flat namespace. `__group` picks the group |
| `/status.json`, `/healthz`, `/` | Route table, per-group health and selected endpoint, `uptime_s` |
| `/stats.json` | Per-model engine and router counters |

A `/v1` request naming a known but ungated model returns 503 with the gate
it failed; an unknown name returns 404. Bodies over 128 MiB are refused.
Each group in `/status.json` has `openai` (the candidate routed to),
`openai_candidates` (every candidate, best first), and `openai_note` and
`provider` from the service entry. The `ray` shim reports Ray version
`2.57.0` because vLLM checks it. That number is the version of the Ray API
the shim emulates, and is unrelated to `proto`.