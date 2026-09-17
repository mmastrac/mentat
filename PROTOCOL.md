# mentat wire protocol 0.99

Client (Python shim or CLI), agent and mesh links run on TCP 6379. The host
link, agent to actor process, runs on a unix socket. All four share one framing
and one message set. Discovery is a UDP datagram on port 6382. HTTP is 6380
(daemon) and 6381 (router). `GUIDE.md` and `GUIDE-SERVE.md` define the
environment variables.

## Version

The version is `major.minor` and matches `[0-9]+\.[0-9]+` exactly. The
major is compared as a number, so leading zeros make no difference. Any
other form is refused. `0.99` is the 1.0 candidate. Its shapes are 1.0's,
and the number changes once the spec is accepted.

Both opening frames of every link set `"proto"`: `hello`/`hello_ok`,
`agent_register`/`agent_register_ok`, `peer_hello`/`peer_hello_ok`,
`probe`/`probe_ok` and `host_hello`/`ctor`. The snapshot and the
announcement payload also set it.

A minor bump may add an optional field with a default, a message type, an
event kind, a snapshot key or a metric. A receiver drops an unknown field in
parsing and applies the patch of an unknown event kind. A sender sends its
own current shape. Nothing records a counterpart's minor. Any other change
is major.

`err` holds `error`, free text for a person, and an optional `code` for a
program to match on. The wording of `error` may change. A receiver treats an
unknown `code` as an absent one. The codes are `no_head`, `proto_mismatch`
(with the refusing side's `proto`), `duplicate_session`, `bad_first_frame`,
`agent_refused`, `unknown_message` and `unknown_ref`. A minor bump may add
one.

`ref_get_ok.status` is a closed set. The shim raises on an unknown value, so
adding one is major. Actor `state` (`spawning`, `running`, `dead`) and
placement group `state` (`PENDING`, `CREATED`, `REMOVED`, Ray's words) are
open sets. A receiver matches the values it wants and passes the rest
through, so a minor bump may add one.

On a major mismatch the accepter returns `err` with its own `proto` and
closes. The dialer closes on a mismatched reply. A mesh peer of another
major is left out of election and redialed at the normal interval. An
announcement of another major is dropped and logged once per source. An
unparseable frame closes the link. An unknown message type keeps the link
open. The client link replies `err` that quotes the type. The agent, mesh
and host links log it and read on. A pushed frame does not belong to a
request.

## Framing

```
u32le header_len | u32le payload_len | header (JSON) | payload (opaque)
```

Each length is capped at 256 MiB. A larger value closes the link. EOF at a
frame boundary is a clean close. The payload is Python pickle bytes and
passes through untouched. Most payloads are empty. The header is a JSON
object:

```json
{"req": 41, "t": "call", "actor_id": "a1", "method": "run"}
{"req": 41, "t": "err", "error": "no such actor a1"}
```

`t` selects the message. `req` correlates a response with its request.
Unsolicited messages use 0, as does an omitted `req`. Request `x` gets
`x_ok`, or `ok` for a request without a result, or `err`. A message uses its
subject's prefix: `pg_` placement group, `actor_`, `agent_`, `peer_` mesh
peer, `host_` actor host, `claim_`, `ref_` object ref. The rest are bare:
`hello` opens a link, and `nodes`, `resources`, `available` and `status`
read cluster state. Memory and byte counts are integers, in bytes.
Ray-shaped responses (`nodes_ok`, `resources_ok`, `available_ok`,
`pg_table_ok`) use floats because Ray does. Every other number is an
integer.

A field written `name?` may be left out. A receiver that does not find one
uses the empty value for its type: `""`, `[]`, `{}`, `false`, `0`, or
`null` where the field is nullable. Every other field is required. A later
minor version may add optional fields. It may not make an existing field
required.

## Connection start

The first frame identifies the link: `hello` (client), `agent_register`
(agent), `peer_hello` (mesh), `probe` (one connection per probe) or
`host_hello` (host). Any other first frame gets `err` and the link closes.

A non-head daemon relays a `hello` or `agent_register` connection to the
head. It sends the first frame there and pipes bytes both ways until either
side closes. When the first frame has an empty `node_ip` and the connection
came from loopback or one of the relaying daemon's own addresses, the relay
fills `node_ip` with its own before forwarding. The head then files the
client under the box it is on. A connection from any other source is a box
that states its own address. A connection that arrives before the first
election waits for it, and gets `err` after thirty seconds without a head.

## Groups

A group is one model deployment: a driver and the agents holding its GPUs,
all with the same `MENTAT_GROUP` value. The name is an opaque string. A
group exists while any agent, actor or client refers to it. Nothing creates
a group.

Group scope keeps two models on one node from counting each other's GPUs.
`nodes`, `resources`, `available` and placement report for the sending
client's group, from its `hello`. `status` and `actor_stop` accept a group as
an argument, for an operator outside any one deployment. The snapshot files
agents, actors and placement groups under `groups`.

One driver holds a group. A `hello` with `session: true` is refused when the
group already has a session.

## Ids

The daemon issues an id for every actor and placement group, and a ref for
every result that is not ready yet. Each has a one-letter type prefix. The
prefix tells a holder what it has and tells the daemon where to dispatch.

| Id | Shape | Issued by |
| --- | --- | --- |
| Actor | `a:<32 hex>` | `actor_create` |
| Placement group | `p:<32 hex>` | `pg_create` |
| Call ref | its actor's id and a counter: `a:<32 hex>:<n>` | `actor_call` |

A ref is a handle to a result that does not exist yet. `actor_call` returns
one at once. `ref_get` fetches the value once it arrives, and `ref_wait`
reports which of several refs are ready. A call ref embeds its actor's id,
so one actor's death resolves every ref outstanding against it.

`ref_get` and `ref_wait` accept a call ref or a placement group id. A
placement group resolves once it reaches `CREATED`. A driver waits on the id
`pg_create` returned, without a separate handle. A removed placement group
resolves as `actor_died`, with `pending_reason` as the reason.

Node, agent and client ids are plain strings, and so is a claim name. They
are keys in the snapshot and fields in `hello` and `agent_register`. A
holder never resolves one.

## Client link

| Request | Fields | Response |
| --- | --- | --- |
| `hello` | `proto`, `client_id`, `group`, `session`, `kind`, `node_ip?` | `hello_ok`: `proto`, `node_id`, `node_ip`, `control_addr`, `head_node_id` |
| `nodes` | | `nodes_ok`: `nodes`, one Ray-shaped row per node |
| `resources` | | `resources_ok`: `resources`, the row keys summed over the group |
| `available` | | `available_ok`: `nodes`, node id to the row keys, with `GPU` the free device count and `memory` the total, since memory is never reserved |
| `pg_create` | `bundles`, `strategy`, `claim?` | `pg_create_ok`: `pg_id` |
| `pg_table` | `pg_id` | `pg_table_ok`: `table` |
| `pg_remove` | `pg_id` | `ok` |
| `actor_create` | `name`, `num_gpus`, `pg_id`, `bundle_index`, `env`. Payload: pickled `(cls, args, kwargs)` | `actor_create_ok`: `actor_id`, `node_id`, `gpu_ids` |
| `actor_call` | `actor_id`, `method`. Payload: pickled `(args, kwargs)` | `actor_call_ok`: `ref_id`. `ref_get` resolves it later (see "Ids") |
| `actor_kill` | `actor_id` | `ok` |
| `ref_get` | `ref_id`, `timeout_ms?` | `ref_get_ok`: `status`, `reason?`. Payload: the pickled result |
| `ref_wait` | `ref_ids`, `num_returns`, `timeout_ms?` | `ref_wait_ok`: `ready` |
| `status` | `group?` | `status_ok`: `snapshot` |
| `actor_stop` | `group?` or `all?`, exactly one | `ok` |
| `claim` | `name`, `shape` | `claim_ok`: `name`, `generation`, `head_node_id`, `view` |

`kind` labels the connection for an operator: `driver` for a shim outside an
actor, `actor` for a shim inside one, `thread` for a shim's extra per-thread
connection, `cli` for a `mentatd` subcommand. The daemon stores it, logs it
and reports it under `clients`. No daemon behaviour depends on the value. A
minor version may add one, and a reader treats an unknown value as text.

Exactly one connection per driver sets `session: true`. A second in one
group is refused. An `actor`, `thread` or `cli` connection sets it false.
`node_ip` is empty from the client. The session's EOF starts a reap. After
`MENTAT_SESSION_REAP_GRACE_MS` the daemon kills the driver's actors, removes
its placement groups and drops its claims. The client id is dropped at once,
so a driver that restarts inside the grace opens its session without
waiting. A non-head daemon skips the reap and leaves the session to the new
head.

A node row. The daemon's own node is always present:

```json
{"NodeID": "...", "NodeManagerAddress": "10.0.0.1", "Alive": true,
 "Resources": {"GPU": 2.0, "CPU": 20.0, "memory": 137438953472.0,
               "object_store_memory": 0.0, "node:10.0.0.1": 1.0}}
```

| Field | Value |
| --- | --- |
| `bundles` | Whole GPUs per bundle. A fraction is refused. A repeat claim under one name must send an equal shape or is refused. Whole numbers compare by value, so `1` and `1.0` are the same shape |
| `strategy` | Recorded. `pg_table` echoes it. Placement packs and logs `pg_strategy_ignored` for any other value |
| `num_gpus` | A whole number |
| `claim` | A claim this placement group sits inside, or empty |
| `table` | Ray's: `placement_group_id`, `name`, `strategy`, `state`, `bundles` (index string to `{"GPU": n}`), `bundles_to_node_id`, `stats` |
| `state` | `PENDING`, `CREATED` or `REMOVED` |
| `timeout_ms?` | Null blocks, 0 polls. Omitted is null, so a caller that leaves it out waits forever |
| `ref_get_ok.status` | `ok`, `error`, `actor_died` or `timeout`, with `reason` filled for `actor_died` |
| An unknown `ref_id` | `ref_get` returns `err`. `ref_wait` counts it ready, since the daemon will never resolve a ref it never held |

`actor_stop` kills one group's actors, or every group's with `all`. The
agents stay registered and the driver reconnects, so the group continues. A
request with neither, or both, is refused and lists the groups that exist.

`all` must be spelled out because the binary is also installed as `ray`.
Under Ray, `ray stop` stops the local node's processes. Under mentat it
reaches every group the daemon knows. An entrypoint that runs it as cleanup
fails until it sets a scope.

## Claims

A claim reserves nodes and links under a name. `claim` matches a shape
against the probed topology and returns the set it chose. The name holds the
reservation. Every holder gets the view the first claim produced, so ranks
that start independently agree without a coordinator.

A name belongs to the claiming client's group. Two groups that pick one name
hold two claims. A claim chooses nodes, and the placement groups inside it
reserve the GPUs. Two groups may claim one node and compete for its devices.

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
{"t": "claim_ok", "name": "myjob", "generation": 3, "head_node_id": "...",
 "view": {
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

A claim on a name held for a different shape is refused. Re-solving would
move nodes under the first claimant. Only the head solves a claim. A claim
sent to any other daemon is refused with the head's address. Two daemons
solving one name against their own views could each hand out a placement.

Each session that sends `claim` joins the holder set. The claim ends when
the last holder's session is reaped. The reap is the only release. The reap
grace applies, so a claim lasts `MENTAT_SESSION_REAP_GRACE_MS` past its
driver, and a restart inside that window under the same name gets the view
it had. A session cut by a head change keeps its claim. The holder re-sends
`claim` to the new head, which rebuilds the table there. Only sessions hold
a claim. A node that leaves the mesh does not end one.

`pg_create` with `claim` set places among the nodes the claim chose. A
placement group that requests more than its claim holds stays pending.
Spilling outside the claim would split ranks that agreed on one view.

## Agent link

| Direction | Message | Fields |
| --- | --- | --- |
| Agent → daemon | `agent_register` | `proto`, `agent_id`, `group`, `node_ip`, `container`, `pid`, `machine`, `services?`, `resume?`, `unacked_refs?` |
| Daemon → agent | `agent_register_ok` | `proto`, `node_id` |
| Daemon → agent | `actor_spawn` | `actor_id`, `name`, `env`, `gpu_ids`, `owner`. Payload: pickled `(cls, args, kwargs)`. The daemon adds the actor's own variables to `env`, which GUIDE.md lists under "Actor process" |
| Agent → daemon | `actor_spawn_result` | `actor_id`, `ok`, `error?`, `pid?` (0 when the failure came before the fork) |
| Daemon → agent | `actor_dispatch` | `actor_id`, `ref_id`, `method`. Payload: pickled `(args, kwargs)` |
| Agent → daemon | `actor_result` | `ref_id`, `ok`, `error?`. Payload: pickled result or exception |
| Agent → daemon | `actor_exit` | `actor_id`, `exit_code?`, `signal?`, each nullable |
| Daemon → agent | `actor_kill` | `actor_id`. The client message, forwarded unchanged |
| Agent → daemon | `service_note` | `service`, `note`. Empty `note` clears |
| Agent → daemon | `ping` | Sent every `MENTAT_AGENT_PING_INTERVAL_MS`. The daemon replies `pong` on the same `req`, which is 0. The send fails once the daemon is gone. The daemon learns of a lost agent from EOF and does not send `ping` |

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
| `actor_result.error?` | Set, with an empty payload, when mentat itself failed. Only a Python failure has an exception to pickle |
| `resume` | Actors alive across a reconnect, each with its `owner?`, so a daemon that lost its state rebuilds ownership |
| `unacked_refs` | Ref ids whose results are buffered agent-side and follow the register. The daemon holds them pending until they arrive |

| Field | Value |
| --- | --- |
| `machine.memory` | Total system memory |
| `machine.gpus` | Every device the agent may bind, in device order |
| `index` | What `spawn.gpu_ids` lists |
| `name` | The vendor's product string |
| `memory` | The device's own memory |
| `uma?` | True for a device sharing the system pool (DGX Spark) |

A UMA device's `memory` is that pool, the same figure as the machine's, so
adding the two counts it twice.

`services` maps a service name to its listen address. The consumer forms
`http://<host>:<port><path>` and resolves an empty `host` itself (see
"Address selection").

| Field | Value |
| --- | --- |
| `host?`, `port`, `path?` | From `MENTAT_<NAME>_API`: `8000/v1` and `http://0.0.0.0:8000/v1` give an empty `host`, `http://10.0.0.1:8000/v1` sets it, any other value is an error at start |
| `provider?` | From `MENTAT_MODEL_PROVIDER`, the label for the engine behind the endpoint. The daemon stores and forwards it unread |
| `note?` | The agent's finding after announcing, such as its server binding one address. A failed probe quotes it |

## Mesh link

| Message | Fields |
| --- | --- |
| `peer_hello` | `proto`, `node_id`, `node_ip`, `control_port`, `http_port`, `addrs`, `addr_tags`, `addr_ifaces` |
| `peer_hello_ok` | The same fields, for the accepter |
| `peer_status` | `snapshot`, pushed every `MENTAT_PEER_STATUS_INTERVAL_MS`. A peer is alive while its pushes arrive |
| `peer_event` | `event`, one replicated event |
| `probe` | `proto`, `node_id`. First frame of its own connection |
| `probe_ok` | `proto`, `node_id` of the responder |

One link per `node_id`. When a second link exists, both ends keep the one
the lower node id dialed. A daemon dials `MENTAT_PEERS` and every control
address its live peers publish under `peers`. A peer unreachable at its seed
address is dialed at each address it last announced, on the seed's port.
`addrs`, `addr_tags` and `addr_ifaces` come from the hello and refresh on
every status push.

### The snapshot as a mesh message

`peer_status` pushes a whole snapshot. The receiver reads these keys from it
as protocol input:

| Key | Read for |
| --- | --- |
| `head_node_id` | Election |
| `addrs`, `addr_tags`, `addr_ifaces` | Refreshing the sending peer's own row |
| `peers/<node_id>/alive`, `node_ip`, `control_port` | Dialing a peer's peers, so one seed address reaches the whole mesh |
| `peers/<node_id>/addrs`, `addr_tags` | Island membership, which decides where an `rdma` claim fits |
| `peers/<node_id>/probes/<local>/<remote>/ok`, `rtt_ms` | The link topology a claim is solved against, and island membership |
| `groups/<group>/gpus_total`, `gpus_used` | The per-group summary in a peer row |

Every other key is output for the HTTP readers. A change to the shape of a
key in the table is a mesh change and a major bump, whatever it does to
`/status`.

A settled head stays head while alive. A daemon without a head uses the one
its live peers publish in `head_node_id`. When none is published it uses the
lowest live node id. Two settled heads that meet resolve to the lower. Every
change waits `MENTAT_ELECTION_HOLD_DOWN_MS`. A daemon that stops being head
closes its agent and driver links so both re-register.

### Probes

The prober binds one of its own addresses, connects to one of the peer's at
the peer's control port, sends `probe`, reads `probe_ok` and closes. Success
requires the expected `node_id` in the reply. Both fabrics in a multi-pair
cluster may share a subnet, so a reply from an address does not prove the
intended node sent it. Binding the local address makes the result describe
the cabling. An unbound probe reports the routing table's preference.

Each daemon probes every (own address × peer address) pair once per
`MENTAT_PROBE_INTERVAL_MS`, times out at `MENTAT_PROBE_TIMEOUT_MS`, and
probes peers in parallel. Results appear per peer under `probes` in the
snapshot. An entry exists once the pair has been tried. A row whose local
address the daemon has lost, or whose remote address the peer stopped
listing, is dropped after the round.

## Host link

The host link runs over a unix socket in `MENTAT_SOCK_DIR`, between agent
and actor process. The socket is per actor, so the process identifies itself
by connecting.

| Message | Fields |
| --- | --- |
| `host_hello` | `proto`. The process is ready for `ctor`. The shim ships in the model image and the agent in the daemon's, so both ends check the major here |
| `ctor` | `proto`. Payload: pickled `(cls, args, kwargs)` |
| `ctor_ok` / `ctor_err` | `ctor_err` sends `error?`, the exception's repr, with the pickled exception |
| `host_call` | `ref_id`, `method`. Payload: pickled `(args, kwargs)` |
| `host_result` | `ref_id`, `ok`. Payload: pickled result or exception |

## Announcement datagram

The announcement is a UDP datagram to port 6382, broadcast on every selected
interface plus any `MENTAT_ANNOUNCE_ADDR` unicast target. The daemon sends.
Both the daemon and the router listen. Interface selection, address ranking
and tags come from `MENTAT_ANNOUNCE_IFACES` and `MENTAT_ANNOUNCE_ADDRS`.
Every announcement is signed:

```json
{"p": {"proto": "0.99", "node_id": "...", "universe": "default",
       "control": "10.0.0.1:6379", "http": "10.0.0.1:6380",
       "addrs": ["10.0.0.1"], "addr_tags": {"10.0.0.1": ["connectx", "rdma"]},
       "boot_id": "6d6474919bbe7beb", "seq": 41, "t": 1787862155},
 "sig": "<hex>"}
```

`sig` is HMAC-SHA256 over the payload in canonical form, keyed by
`MENTAT_SECRET_FILE`'s contents or else `MENTAT_SECRET`. Canonical form is
the payload as JSON with object keys sorted at every depth, no whitespace,
`,` and `:` as separators, and non-ASCII held as UTF-8 bytes. Signing this
payload with the key `k`:

```json
{"a":1,"b":[2,{"c":3,"d":4}],"universe":"kü"}
```

gives `ec381f4b20bc7eb6b1f18c7f15b06a5c7c60aca04b4ffcba2c1910ac6262ed39`.
An implementation that reproduces that hex agrees with this one. A file
given in `MENTAT_SECRET_FILE` that cannot be read, or reads empty, is fatal
at boot. Without a key the daemon does not announce and the router does not
listen. Each logs that at boot. The verifier re-serializes the payload it
parsed, so every value must survive a JSON round trip. Values are integers
and strings only. `t` is integer seconds, within 30 s of the receiver's
clock. `seq` must exceed the last accepted for the same `boot_id`. A restart
issues a new `boot_id` and restarts `seq`.

The datagram is at most 1400 bytes, one Ethernet frame. A sender over that
logs `announce_too_large` and sends nothing until it fits. A listener reads
into a buffer one byte longer, so a datagram at the cap arrives whole. A
read that fills the buffer came from a sender over the cap. The listener
drops it and logs `announce_oversize` once per source. That check runs
before the `universe` read, because a truncated payload parses as nothing.

The receiver reads `universe` next, before verifying. A foreign universe is
dropped without a log line. Another cluster on the same broadcast domain is
expected. A datagram without `universe` reads as `default`, the value a
process uses when `MENTAT_UNIVERSE` is unset. The receiver then verifies the
signature and logs a bad one once per source. It then checks `proto`, `t`
and `seq`. A signed datagram outside the `t` window logs `announce_stale`
once per source. The signature places it in this cluster, and a drifted
clock is worth reporting. The router also requires the source address, and
each advertised address before it is chosen, to pass `ALLOWED_SOURCES`. It
logs a rejection once. The daemon's listener does not apply an allowlist. It
dials the address in a signed datagram.

An announcement is a hint. For the router it adds one address to watch. For
a daemon it produces one dial. `peer_hello` on that dial settles identity,
version and link ownership. Every field is re-read over TCP and
probed before it affects routing. A daemon with an empty `MENTAT_PEERS`
joins by being on the same broadcast domain.

## Address selection

| Field | Value |
| --- | --- |
| `node_ip` | The node's own identity address. It fixes the subnet the cluster uses. A host off that subnet may not reach it |
| `link_ip` | The address a mesh link uses: the socket peer address inbound, the dialed address outbound |
| `addrs` | Every address the node listens on, most preferred first. Only the node can rank its own links |
| `addr_tags` | Each address to its operator tags |
| `addr_ifaces` | Each address to the interface it sits on. Addresses from `MENTAT_ANNOUNCE_ADDRS` are absent |

`rdma` is the one interpreted tag. It means the operator cabled this address
into a fabric. Placement acts on it once a probe over it has succeeded. A
rank binds an interface, and only the node knows which one. `addr_ifaces`
supplies it.

A consumer picks one address per node, in this order: the highest-ranked
entry in `addrs` on one of its own subnets, then the source address of a
datagram it received (proof of reach), then `link_ip`, the rest of `addrs`
and `node_ip`. There is one watch per `node_id`. A node with two links
broadcasts on both with one `seq`. A receiver keeps the first datagram of a
round and drops the rest as replay. The alternates come from `addrs`, so a
node ranks every address it holds. The datagram that arrived does not supply
them.

A service with an empty `host` resolves against its node's `addrs`. The
agent joins its node by matching its `node_ip` against `node_ip`, `link_ip`
and every entry of `addrs`, so a node's daemon and its containers must agree
on `MENTAT_NODE_IP`. Every candidate is checked against the consumer's
`ALLOWED_SOURCES`. A service with a `host` is used as written. The operator
chose it. Candidates on the consumer's own subnets sort first. The node's
order holds within each half. The consumer probes in order, keeps the first
that replies, falls through when it stops replying, and periodically
re-tries the higher-ranked ones.

## Placement

A placement group reserves whole devices. Placement ignores memory in 0.99.
A `uma: true` device is one device like any other. The bundles of one
placement group go on GPUs of one vendor. A collective cannot span vendors.
A claim set's `vendor` pins the vendor. An unclaimed placement group
uses the first vendor that fits.

A placement group of more than one bundle goes inside one fabric island. An
island is a set of nodes that all reach each other over addresses tagged
`rdma`, with a successful probe behind every pair. Each daemon derives
islands from its own probe table and the tables peers publish in
`peer_status`. It prunes each connected component, least-connected node
first, until every member reaches every other. Soft consistency is enough,
because the daemon a driver reached decides its placement groups. Membership
commits after `MENTAT_ISLAND_HOLD_DOWN_MS` of stability, so a cable that
flaps cannot send consecutive placements to different islands. The vertices
are addresses, because a rank binds one address that every other rank must
reach. A node with two fabric ports on separate links joins through
whichever port reaches all of the island, or through neither.

A placement group of one bundle is unconstrained. So is one whose group
lacks an alive agent on a node tagged `rdma`. `MENTAT_ISLAND_PLACEMENT=off`
disables the constraint daemon-wide. A node belongs to its island or is an
island of one, so a placement group that fits on a single node stays off the
fabric. Candidate islands are those with enough free GPUs of one vendor. The
driver's island is tried first, then the smallest sufficient one. A placement
group that fits nowhere stays `PENDING`. `pending_reason` gives the
constraint and what the best island offered. It fails the same way at
`MENTAT_PG_PENDING_TIMEOUT_MS`. Each rank is spawned with `MENTAT_FABRIC_IP`
set to its node's address on the island.

## Snapshot

`status_ok`, `peer_status`, `/status` and the first `/events` frame contain
the same object. `?group=` on `/status` and `group` on `status` scope it.

```json
{"proto": "0.99", "node_id": "...", "node_ip": "10.0.0.1", "hostname": "n1",
 "control_addr": "10.0.0.1:6379", "head_node_id": "...", "head_generation": 3,
 "seq": 41, "boot_id": "6d6474919bbe7beb",
 "addrs": [...], "addr_tags": {...}, "addr_ifaces": {...},
 "islands": [{"nodes": ["..."], "addrs": {"<node_id>": "10.0.0.1"}}],
 "peers": {"<node_id>": {
   "node_ip": "...", "link_ip": "...", "addrs": [...], "addr_tags": {...},
   "addr_ifaces": {...}, "control_port": 6379, "http_port": 6380,
   "alive": true, "stale": false, "last_seen_ms": 0, "dead_since_ms": null,
   "probes": {"<local>": {"<remote>": {"ok": true, "rtt_ms": 0,
                                       "last_ok_ms": null, "error": ""}}},
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

Each collection's key is the id. The row itself omits it. An event addresses
one row by path.

| Field | Value |
| --- | --- |
| `machine`, `services` | The agent's registration verbatim |
| `gpus_free` | Device indices open to a new bundle |
| `gpus_total`, `gpus_used` | Devices across the group's alive agents |
| actor `state` | `spawning`, `running` or `dead`, with `reason` filled for `dead` |
| `clients` | The connections this daemon serves. `session` marks the one whose EOF reaps the group |
| `claims` | Filled on the head only. Empty elsewhere |
| `sets` | Each set's node count |

The addresses and interfaces behind `sets` are in the view `claim` returns.
That view is too large to push on an interval.

## Events

An event is a change to the snapshot. `patch` lists paths into the snapshot
and the values to store there. Each value is the row "Snapshot" defines for
that path.

```json
{"type": "actor_running", "seq": 12, "ts_ms": 1787862155000, "node": "...",
 "patch": [{"at": ["groups", "glm", "actors", "a1"],
            "value": {"name": "w0", "node_id": "...", "gpu_ids": [0],
                      "state": "running", "reason": "", "pid": 9312}}]}
```

| Field | Value |
| --- | --- |
| `at` | An array of keys. A group name and an id are opaque, and either may hold any character |
| `value` | The whole row, so applying one is a replace and a reader holds either the old row or the new. Absent removes the path |
| `why` | Optional free text for a log |

`patch` may be empty. `driver_gone_reaping` sends an empty one.
`driver_disconnected` already removed the client row, and each actor's own
event follows.

A program reads the row. `why` is for a log. `gone_since_ms` gives the
figure `agent_degraded` describes in words.

The paths below are written with `/`. Each is the array of its keys.

| Event | Path |
| --- | --- |
| `node_join`, `node_leave`, `peer_forgotten` | `peers/<node_id>` |
| `head_change` | `head_node_id` and `head_generation`, which counts this daemon's own head changes and compares only against itself |
| `islands_changed` | `islands` |
| `agent_register`, `agent_lost`, `agent_degraded`, `agent_dead`, `service_note` | `groups/<group>/agents/<agent_id>` |
| `pg_created`, `pg_ready`, `pg_pending`, `pg_timeout`, `pg_removed` | `groups/<group>/placement_groups/<pg_id>` |
| `actor_spawning`, `actor_running`, `actor_dead` | `groups/<group>/actors/<actor_id>` |
| `driver_connected`, `driver_disconnected`, `driver_gone_reaping` | `clients/<client_id>` |
| `claim_solved`, `claim_released` | `groups/<group>/claims/<name>` |
| `history_swept`, `head_moved` | Several of the paths above in one patch |

`history_swept` and `head_moved` each hold every row they changed, so a
single event may mix removals and sets across several groups. A group's
`gpus_total` and `gpus_used` are sums over its agent rows. No event patches
them directly. `gpus_free` on an agent row counts the devices no placement
group holds, so `pg_ready` and `pg_removed` include every agent row they
moved.

An entry removes its path when the daemon has dropped the row. Otherwise it
sets a whole row. A row that the daemon keeps in a terminal state is set.
`node_leave` leaves the peer in place with `alive` false and `dead_since_ms`
filled, and `pg_removed` leaves the group `REMOVED`. `peer_forgotten` and
`history_swept` remove those paths once the daemon forgets them. A consumer
that applies events and one that re-reads the snapshot hold the same tables.

A consumer applies events in `seq` order per originating `node`. On a gap it
re-reads the snapshot, because a missed event leaves the view wrong.
Counters move without events, so a consumer that reads them re-reads on an
interval too. The first `/events` frame is a snapshot registered under the
subscription's lock. Nothing falls between it and the first event.

Every snapshot holds the `seq` it reflects. A consumer that re-reads one
resumes the stream from that `seq`. An event at or below it is already in
the view. `seq` counts from 1 per daemon process, so a restart hands out
numbers a consumer has already applied. The snapshot's `boot_id` changes
with the process, which tells a restart from a gap. A consumer holding
events from an older `boot_id` starts again from the new snapshot.

Each daemon replicates its own events to every live peer in `peer_event`.
It delivers a peer's event to its own subscribers without re-forwarding it.
A replicated event describes the originating node's snapshot. A receiving
daemon's snapshot summarises its peers and does not hold their rows. A
consumer therefore applies only events whose `node` is the daemon it is
reading. It reads every other node from that node's own stream.

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
| `GET /v1`, `/v1/models` | The routable models, which the router serves itself |
| `POST /v1/*` | Routed by request `model`, streamed through |
| any other POST | Routed by request `model`, for root-level endpoints such as `/tokenize` |
| `/mcp` | Merged MCP in a flat namespace. `__group` picks the group |
| `/status.json`, `/healthz`, `/` | Route table, per-group health and selected endpoint, `uptime_s` |
| `/stats.json` | Per-model engine and router counters |

A `/v1` request for a known but ungated model returns 503 with the gate it
failed. An unknown model name returns 404. Bodies over 128 MiB are refused.
Each group in `/status.json` has `openai` (the candidate routed to),
`openai_candidates` (every candidate, best first), and `openai_note` and
`provider` from the service entry. The `ray` shim reports Ray version
`2.57.0` because vLLM checks it. That number is the version of the Ray API
the shim emulates. It is unrelated to `proto`.
