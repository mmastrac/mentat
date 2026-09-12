# Protocol 1.0: code changes

Where the code diverges from `PROTOCOL.md` at 1.0, file by file. Line
numbers refer to the tree at commit cbd6b41. Nothing here is implemented
yet.

## Wire summary

| v0 | 1.0 |
| --- | --- |
| No version field | `proto: "1.0"` on every link's opening frame and its reply, in the snapshot and in the announcement |
| `probes: bool` capability bit on the mesh hellos | Removed. Every 1.x daemon answers `probe` |
| `ok0` | `ok` |
| `cluster_resources` / `resources_ok` | `resources` / `resources_ok` |
| `available_per_node` / `avail_ok` | `available` / `available_ok` |
| `release` | Removed. Nothing sent it. A claim ends with its last holder's session |
| `ping` handled on the agent, mesh and daemon-to-agent paths | Agent to daemon only |
| `host_hello.actor_id` | Removed |
| `hello_ok.gcs_address`, `actor_spawn.gcs_address` | `control_addr` |
| `status_ok.data`, `peer_status.data` | `snapshot`, with the schema in "Snapshot" |
| `peer_event {origin, line}` with `line` a JSON string | `peer_event {event}` with the object inline. `origin` is `event.node` |
| `agent_register.gpus: [u32]`, `gpu_vendor`, `cpus` | `machine: {memory, cpus, gpus: [{index, vendor, name, memory, uma}]}` |
| `services` (URL) plus `services_ports` (`{port, path}`) plus `provider` plus `service_notes` | one `services` map of `{host, port, path, provider, note}` |
| `pg_create.bundles: [f64]`, `actor_create.num_gpus: f64` | integers |
| `create_pg`, `remove_pg` | `pg_create`, `pg_remove` |
| `create_actor`, `call`, `kill_actor` | `actor_create`, `actor_call`, `actor_kill` |
| `spawn`, `spawn_result`, `call_actor` | `actor_spawn`, `actor_spawn_result`, `actor_dispatch` |
| `get`, `wait` | `ref_get`, `ref_wait` |
| Bare hex actor and pg ids; ref `<actor>:<n>`; ready ref `pg:<pg_id>:ready` | `a:<hex>`, `p:<hex>`, ref `a:<hex>:<n>`. A pg id is its own readiness handle |
| `create_pg_ok.ready_ref` | dropped. `pg_id` resolves |
| `stop_all`, `group: Option<String>` where `None` meant every group | `actor_stop`, `group: String` plus `all: bool`. Neither or both is refused |
| `st.claims` keyed by name alone, so two groups collide on one name | keyed by `(group, name)`. Snapshot path `groups/<group>/claims/<name>` |
| `kill` (daemon to agent) | folded into `actor_kill`, one message on both links |
| `claim.shape.link` accepts `roce`, `fabric`, `any` | `rdma` or `ip` only |
| `claim.shape.sets[]` | gains optional `vendor` |
| `claim_ok.view.sets[][]` | gains `vendor` |
| `claim_ok.view.between[]` `{from, to, from_node, to_node, local, remote, rtt_ms}` | `{from: {set, node, host, addr, iface}, to: {...}, rtt_ms}` |
| `peer_hello.control_addr` | `control_port` (integer) |
| `nodes_ok.nodes[]`, `pg_table_ok.table` untyped | typed. `Resources` gains `memory` |
| `resources_ok`, `available_ok` | gain `memory` |
| Announcement versions 1 (unsigned) and 2 (signed), `mentat_announce` discriminator | one signed format, `proto` inside the payload, `mentat_announce` removed |
| No announcement size limit. Router reads 2048 bytes | 1400-byte cap; the sender enforces it |
| Signing optional | Key required to announce or listen |
| Snapshot agent `gpus` (count), `gpus_free` (count), `gpu_vendor`, `cpus` | `machine`, `gpus_free` as a list of device indices |
| Snapshot actor `state: "dead (reason)"` | `state: "dead"`, `reason` |
| Snapshot pg `bundles` (count) | list of GPU counts. Gains `claim` |
| Snapshot `gcs_address`, `signing` | `control_addr`. `signing` removed |
| Snapshot peer `control_addr` | `control_port` |
| `mentat_agents{group,vendor}` | `mentat_agents{group}`. `vendor` moves to the GPU metrics. New `mentat_gpu_memory_bytes`, `mentat_memory_bytes` |
| `counters.relayed` does not have a metric | `mentat_relayed_total` |
| Snapshot `agents`, `actors`, `placement_groups` are arrays with an `id` field | maps keyed by id, no repeated key |
| Snapshot does not have client or claim state | `clients` and `claims` maps |
| Event fields are per type and ad hoc | `patch`: paths into the snapshot and the rows to store there |

## Messages with one sender

Some messages have one sender on their link. Nothing else sends them.

- `actor_stop`: the `mentatd stop` CLI (main.rs 327), and `ray stop` through
  the symlink. The shim never sends it.
- `claim`: `placement_group.py` 44, only with `MENTAT_CLAIM` set.
- `service_note`: `watch_service_binds` (agent.rs 318-400), only when a
  server narrows or widens its bind after announcing a port.
- `ping`: the agent's ping loop (agent.rs 583, register.py 132).

## rust/mentatd/src/proto.rs

- Add `pub const PROTO: &str = "1.0"` and a `proto: String` field on
  `Hello`, `HelloOk`, `AgentRegister`, `AgentRegisterOk`, `PeerHello`,
  `PeerHelloOk`, `Probe`, `ProbeOk`, `HostHello`, `Ctor`. Add a helper that
  splits `major.minor` and reports whether a peer's major matches.
- Line 110: rename `Ok0` to `Ok`.
- Lines 57-59: delete `Release`.
- Lines 357-359: `HostHello` loses `actor_id`. The agent matches the frame
  and reads nothing from it (agent.rs 848).
- Lines 44-45: rename `ClusterResources` to `Resources`, `AvailablePerNode`
  to `Available`. Lines 123-128: rename `AvailOk` to `AvailableOk`.
- Line 117: `HelloOk.gcs_address` becomes `control_addr`. Line 224: the same
  on `Spawn`.
- Lines 54, 132: replace `shape: Value` and `view: Value` with typed
  `claim::Shape` and `claim::View` (serde structs, see claim.rs).
- Line 121: `NodesOk.nodes: Vec<Value>` becomes `Vec<NodeRow>`, a struct
  with the Ray field names (`NodeID`, `NodeManagerAddress`, `Alive`,
  `Resources`). Line 139: `PgTableOk.table: Value` becomes a `PgTable`
  struct.
- Lines 161, 347: `StatusOk.data` and `PeerStatus.data` become `snapshot`.
  Keep the type as `Value` in Rust if a struct for the whole snapshot is
  more than the migration wants, but the spec shape is fixed either way.
- Line 62: `CreatePg.bundles: Vec<f64>` becomes `Vec<u32>`. Line 78:
  `CreateActor.num_gpus: f64` becomes `u32`.
- Lines 169-174: replace `gpus`, `gpu_vendor`, `cpus` with `machine:
  Machine`. New structs:

  ```rust
  pub struct Machine { pub memory: u64, pub cpus: u32, pub gpus: Vec<Gpu> }
  pub struct Gpu { pub index: u32, pub vendor: String, pub name: String,
                   pub memory: u64, #[serde(default)] pub uma: bool }
  ```

- Lines 183-205: replace `services`, `services_ports`, `provider`,
  `service_notes` with `services: BTreeMap<String, Service>`. Lines 451-455:
  `ServicePort` becomes `Service { host, port, path, provider, note }` with
  `#[serde(default)]` on every string.
- Lines 281-282, 311-313: `control_addr: String` becomes `control_port: u16`
  on both hellos. Lines 300-301, 323-324: drop `probes`.
- Lines 351-354: `PeerEvent { origin, line }` becomes `PeerEvent { event:
  Value }`.
- Line 381: delete `default_gpu_vendor`.
- Every `#[serde(default)]` whose comment says "for daemons/agents that
  predate the field" loses the default. The remaining defaults are the ones
  the spec marks optional: `hello.node_ip`, `Gpu.uma`, `Service` strings,
  `pg_create.claim`, `actor_spawn_result.error`/`pid`, `actor_result.error`,
  `ctor_err.error`, `get_ok.reason`, `resume[].owner`, `unacked_refs`.
- Add a test that a `Machine` with a `uma` device round-trips, and one that
  a frame with an unknown `t` deserializes to an error the caller can answer
  with `err` (today `serde` rejects it and `read_frame` returns an
  `io::Error`, which closes the link).

## Renames for subject-first naming

A message is named for its subject. Only the `t` string and the Rust variant
change; no field or behaviour moves with them.

| Variant now | 1.0 | `t` now | 1.0 |
| --- | --- | --- | --- |
| `CreatePg`, `CreatePgOk` | `PgCreate`, `PgCreateOk` | `create_pg`, `create_pg_ok` | `pg_create`, `pg_create_ok` |
| `RemovePg` | `PgRemove` | `remove_pg` | `pg_remove` |
| `CreateActor`, `CreateActorOk` | `ActorCreate`, `ActorCreateOk` | `create_actor`, `create_actor_ok` | `actor_create`, `actor_create_ok` |
| `Call`, `CallOk` | `ActorCall`, `ActorCallOk` | `call`, `call_ok` | `actor_call`, `actor_call_ok` |
| `KillActor` | `ActorKill` | `kill_actor` | `actor_kill` |
| `Spawn` | `ActorSpawn` | `spawn` | `actor_spawn` |
| `SpawnResult` | `ActorSpawnResult` | `spawn_result` | `actor_spawn_result` |
| `CallActor` | `ActorDispatch` | `call_actor` | `actor_dispatch` |
| `Kill` | deleted | `kill` | `actor_kill` |
| `Get`, `GetOk` | `RefGet`, `RefGetOk` | `get`, `get_ok` | `ref_get`, `ref_get_ok` |
| `StopAll` | `ActorStop` | `stop_all` | `actor_stop` |
| `Wait`, `WaitOk` | `RefWait`, `RefWaitOk` | `wait`, `wait_ok` | `ref_wait`, `ref_wait_ok` |

`PgTable`/`PgTableOk`, `ActorResult` and `ActorExit` already read this way
and do not move. `StopAll` becomes `ActorStop`: it kills actors, and the
group is the scope it runs over rather than the thing it acts on. A message
with no object to name keeps none: `Hello`, `Nodes`, `Resources`,
`Available`, `Status`.

Two names collided once both links used the prefix. `Kill { actor_id }` and
`KillActor { actor_id }` were one message on both links, so `ActorKill`
serves both and the daemon forwards what it received. `Call` and `CallActor`
differ: the client asks for a call and gets a ref back, while the daemon
dispatches an already-numbered call to the agent that runs it. The second is
`ActorDispatch`.

Call sites: `daemon.rs` match arms and every `(Msg::X, ...)` return,
`agent.rs` (519-676 the register loop, 706-735 spawn results, 857-931 the
host bridge), `main.rs:327`, `python/ray/actor.py:68,91`,
`python/ray/__init__.py`, `python/ray/util/placement_group.py`, and the
`fake_worker.py` and `mentat_testlib.py` helpers under `tests/`.

## rust/mentatd/src/daemon.rs

- Lines 414-460 (first-frame gate): check `proto` major on `hello`,
  `agent_register` and `probe` before anything else. On mismatch answer
  `err` with its own `PROTO` and close. The relay path forwards the frame
  unchanged, so the head performs the check for relayed links. The `other`
  arm's message names `probe` and `host_hello` as accepted first frames.
- Line 486 `head_for`: unchanged.
- Lines 688-693: `HelloOk` gains `proto`, `control_addr` replaces
  `gcs_address`.
- Lines 766-792 `Nodes`: build `NodeRow` values. `node_entry` (1246-1253)
  gains `memory` from `machine.memory`, summed per node over the group's
  agents. The daemon's own row (line 771) reports `memory: 0.0` unless the
  daemon learns its own memory, which is out of scope.
- Lines 793-807 `ClusterResources`: rename. Add `memory`. GPU count is
  `machine.gpus.len()`.
- Lines 808-819 `AvailablePerNode`: rename. Add `memory` (total, since
  memory is never reserved).
- Lines 835-846: delete the `Release` arm. `release` and `release_all`
  (1146, 1159) stay, since `reap_client_resources` calls them. Line 1116:
  the refusal says "Release it or use another name", and a client cannot
  release. Say "use another name".
- Lines 847-886 `CreatePg`: `bundles` is `Vec<u32>`. Drop the
  `ceil().max(1.0)` conversions at 1828, 1978 and in `fit` (1889-1930).
- Lines 887-918 `PgTable`: build a `PgTable` struct. `bundles."i".GPU` stays
  a float.
- Line 927 and the `ActorKill`, `ActorStop` arms: answer `Ok`.
- Lines 1046-1059 `ActorStop`: refuse a request with neither `group` nor
  `all`, and one with both, quoting the groups in `st.actors`. Only `all`
  takes the unfiltered path. The old shape made the wide kill the default
  for a message that left a field out.
- Lines 1262-1400 `actor_create`: `num_gpus: u32`. `ActorSpawn` gains
  `control_addr` in place of `gcs_address`. `MENTAT_GCS_ADDRESS` (1333) is a
  shim-facing environment variable and stays.
- Lines 1769-1888 `placement_scopes` and 1889-1930 `fit`: vendor rule. Free
  devices are grouped by vendor per agent (`free_gpus_of` returns indices,
  the agent's `machine.gpus` maps index to vendor). A scope fits when one
  vendor has enough free devices across its nodes. A claim's `vendor` pins
  it. `no_island_reason` (1945) and `no_fit_reason` (1977) name the vendor
  they counted.
- Lines 1995-2120 `agent_conn`: destructure `machine` and `services`. Line
  2116 event `agent_register` sends `machine` and `services`. Lines
  2429-2436 `ServiceNote`: write into `services[service].note`.
- Wherever `a.gpus.len()` or `a.cpus` is read: `a.machine.gpus.len()`,
  `a.machine.cpus`.
- Line 2442: delete the `Pong` arm. The daemon does not send `ping`, so a
  `pong` falls to the logging arm below it.
- Unknown `t` on an open client or agent link: answer `err`, keep the link.
  Today `read_frame` fails on an unknown variant and the link closes.

## Prefixed ids

Every id a client can hand back takes a type prefix, so `resolve_ref` reads
the id instead of inferring from what it is not.

- `state.rs:493` `random_hex_id`: take a prefix, or add `actor_id()` and
  `pg_id()` wrappers that prepend `a:` and `p:`. Call sites `daemon.rs:854`
  (pg) and `1321` (actor).
- `state.rs:389` `new_ref_id`: the format string already reads
  `{actor}:{r}`, which becomes `a:<hex>:<n>` once the actor id takes its
  prefix. No change here.
- `daemon.rs:881`: delete `ready_ref`. `CreatePgOk` loses the field and
  `pg_id` is the handle.
- `daemon.rs:1408-1412` `resolve_ref`: match `a:` with two colons for a call
  ref and `p:` for a placement group, rather than `strip_prefix("pg:")` then
  `strip_suffix(":ready")`. Today a call ref is recognised by the absence of
  `pg:`, which holds only because `random_hex_id` emits hex and can never
  start with those letters. Nothing states that invariant.
- `python/ray/util/placement_group.py` 12-18, 66: `PlacementGroup` drops
  `_ready_ref`; `ready()` returns `ObjectRef(self.id)`.
- Snapshot keys and event paths include the prefix, since it is part of the
  id everywhere: `groups/<group>/actors/a:<hex>`.

## Claims keyed by group

A claim name is the one caller-chosen id the protocol does not scope. An
actor name is already checked per group (daemon.rs:1276) and every other id
is minted by the daemon, so this is the only place a hierarchy is worth
adding.

- `state.rs:327` `claims: BTreeMap<String, ClaimInfo>` becomes
  `BTreeMap<(String, String), ClaimInfo>`, or a map of group to a map of
  name. `ClaimInfo` does not need a group field once the key holds it.
- `daemon.rs:1113`, `1128`, `1147`, `1780`: look up by the client's group
  and the name. The group comes from the session, as it does for every other
  client message, so no message gains a field.
- `daemon.rs:1160` `release_all` walks one group's claims rather than all.
- `status.rs`: emit `claims` under `groups/<group>` rather than at the top
  level.
- No exclusivity changes. `claim::solve` takes a topology and a request and
  never reads `st.claims` (claim.rs:304), so two claims could always choose
  one node. The placement groups inside a claim reserve the GPUs; the claim
  itself reserves none.

## rust/mentatd/src/state.rs

- Lines 107-109: `AgentInfo.gpus`, `gpu_vendor`, `cpus` become `machine:
  Machine`. Lines 116-127: `services`, `services_ports`, `service_notes`,
  `provider` become `services: BTreeMap<String, Service>`.
- Line 155: `PgInfo.bundles: Vec<f64>` becomes `Vec<u32>`.
- Line 277: drop `PeerInfo.probes`. Line 280: `control_addr: String` becomes
  `control_port: u16`. Every reader that split the port off `control_addr`
  (`head_for` at daemon.rs 498-504, `prober` at mesh.rs 826-828) reads the
  integer.
- Line 196 `ClientInfo`: add `kind: String`. The `driver_connected` event
  already reports it (daemon.rs:683) and nothing stores it, so the `clients`
  row cannot be built without this.
- Lines 398-425 `emit`: take `patch: Vec<Patch>` and optional `why` instead
  of a loose `Value`, and build the envelope from them. Send `PeerEvent {
  event: data }` instead of the serialized line. `event_subs` may keep
  receiving the string form.
- Lines 436-450 `free_gpus_of`: iterate `machine.gpus` by `index`.
- Add `free_gpus_by_vendor(&AgentId) -> BTreeMap<String, Vec<u32>>` for
  placement.

## rust/mentatd/src/status.rs

- Lines 65-81 agent entry: `machine` verbatim, `gpus_free` as the index
  list, `services` verbatim. Drop `gpus`, `gpu_vendor`, `cpus`,
  `services_ports`, `service_notes`, `provider`.
- Lines 95-104 actor entry: `state` is the bare word, `reason` is its own
  field.
- Lines 110-124 pg entry: `bundles` is the list. Add `claim`.
- Lines 126-137 totals: count `machine.gpus.len()`.
- Lines 57-145: build `agents`, `actors` and `placement_groups` as maps
  keyed by id rather than arrays, and drop the `id` field from each row.
  `render` (235-330) iterates values instead of elements.
- Add `clients` from `st.clients` (`group`, `kind`, `node_id`, `session`)
  and `claims` from `st.claims` (`generation`, `holders`, and `sets` as the
  node count per set, read off `view["sets"]`). `claims` is empty on a
  daemon that is not head, matching what `st.claims` already holds.
- Lines 179-193 peer entry: `control_port` replaces `control_addr`.
- Lines 198-230 top level: add `proto`. `control_addr` replaces
  `gcs_address`. Drop `signing`.
- Lines 235-330 `render`: read `machine.gpus` and `gpus_free.len()`. Line
  319 reads `gpu_vendor`, which is now per device. Print the vendor set of
  the agent's devices. The `ray status` grep contract in the module comment
  (one line matching `[0-9.]+/[0-9.]+ GPU`) is unchanged.
- Tests at 390-420 build the old agent shape.

## rust/mentatd/src/http.rs

- Lines 150-170: `mentat_agents` loses the `vendor` label.
  `mentat_gpus_total` and `mentat_gpus_used` gain it, one series per vendor
  present in the group. Add `mentat_gpu_memory_bytes{group,vendor}` and
  `mentat_memory_bytes{group}`.
- Line 205: add `mentat_relayed_total` from `counters.relayed`, beside the
  other counters. `mentat_build_info` (138) is unchanged and enters the
  metric table.
- Line 265: the `/events` snapshot frame is unchanged in shape. The snapshot
  inside it follows status.rs.

## Events as snapshot patches

Every `emit` call site builds a `patch` giving the row it changed and passes
the row as status.rs would render it, so one helper per collection (agent,
actor, pg, peer, client, claim) keeps the two in step. The diagnostic fields
each site passes today (`down_ms`, `waited_ms`, `why`, `reason`, `actors`,
`degrade_window_ms`, `bundles`, `tagged_nodes`) become `why` text or
disappear where the row already holds the figure.

- `island.rs:336` `islands_changed` patches `islands`.
- `mesh.rs:503` `node_join`, `595` and `676` `node_leave` patch
  `peers/<node_id>`. A leave removes the path.
- `mesh.rs:729` `head_change` patches `head_node_id` and `head_generation`.
- `daemon.rs:2112` `agent_register`, `2476` `agent_lost`, `227`
  `agent_degraded`, `248` `agent_dead` patch
  `groups/<group>/agents/<agent_id>`.
- `daemon.rs:874` `pg_created`, `1744` `pg_ready`, `193` `pg_timeout` patch
  `groups/<group>/placement_groups/<pg_id>`.
- `daemon.rs:1359` `actor_spawning`, `2379` `actor_running`, `1583`
  `actor_dead` patch `groups/<group>/actors/<actor_id>`. **`actor_running`
  sends only `actor_id` and `pid` today and cannot name its group.** Look
  the group up from the actor before emitting.
- `daemon.rs:681` `driver_connected`, `1610` `driver_disconnected`, `1658`
  `driver_gone_reaping` patch `clients/<client_id>`. The last two remove it.
- `daemon.rs:1137` `claim_solved` and `1153` `claim_released` patch
  `claims/<name>`. The release removes it.
- A site that changes a group's device totals patches `gpus_total` or
  `gpus_used` in the same event, since neither is derivable from the row.

## rust/mentatd/src/mesh.rs

- Lines 221-233 `PeerHello`: add `proto`, send `control_port`, drop
  `probes`. Lines 244-275: parse the same set from `PeerHelloOk`. On a major
  mismatch log once and return an error so the connector redials at its
  interval.
- Lines 282-287: delete the old-daemon fallback that substitutes the seed
  for an empty `control_addr`.
- Lines 317-380 `accept_peer`: check `proto` first. On mismatch answer `err`
  with `PROTO` and close. Reply with `proto` and `control_port`.
- Lines 419-445 `register_peer` and `PeerIdent` (388-400): drop `probes`,
  send `control_port`.
- Lines 528-560 `PeerStatus { data }`: field is `snapshot`.
- Lines 562-565 `PeerEvent { line, .. }`: the field is `event`, an object.
  `deliver_peer_event` serializes it once for subscribers.
- Lines 566-574: delete the `Ping` and `Pong` arms. Nothing sends a mesh
  ping, and `last_seen_ms` is refreshed from `peer_status`.
- Line 741 `head_candidate`: unchanged, but a peer of another major must not
  be in `st.peers` as alive. Simplest is to never register it.
- Line 824: drop the `p.probes` filter.
- Lines 960-980 probe exchange: `Probe` and `ProbeOk` gain `proto`.

## rust/mentatd/src/announce.rs

- Lines 87-140 sender: refuse to start the announcer without a key and log
  it. Build the payload with `proto` in place of `mentat_announce`, always
  with `boot_id`, `seq`, `t`. Delete the unsigned branch at 134-137.
- After signing, check the envelope length against 1400. Over the cap: log
  `announce_too_large` once per change of payload and skip the round.
- Module comment lines 1-10 describe versions 1 and 2.

## rust/mentatd/src/gpu.rs

- `detect_gpus` returns `Machine`. Query `nvidia-smi --query-
  gpu=index,name,memory.total --format=csv,noheader,nounits` and convert MiB
  to bytes. Read system memory from `/proc/meminfo` (`MemTotal`, kB to
  bytes) and `cpus` from `available_parallelism`.
- UMA detection: on a DGX Spark `nvidia-smi` reports the device and the
  memory it can address is the system pool. Detect it by the product name
  (`GB10`) or by `memory.total` reading as `[N/A]`, and set `uma: true` with
  `memory` equal to the machine total. Pick one and test it against a
  captured `nvidia-smi` line from the box.
- `MENTAT_GPUS=<n>` stays as the test override and yields `n` devices
  `{index: i, vendor: "nvidia", name: "fake", memory: 0, uma: false}`. Add
  `MENTAT_MACHINE=<json>` to inject a whole inventory, which the
  heterogeneous and UMA tests need.

## rust/mentatd/src/agent.rs

- Lines 91-92: `detect_gpus()` returns `Machine`. Drop the separate `cpus`.
- Lines 153-211 `parse_announcement` and 285-316 `announced_services`:
  produce `Service` entries. `http://<host>:<port><path>` with a host that
  is neither empty nor a wildcard sets `host`. A value in neither the
  `port/path` form nor the `http://` URL form is an error at start (line 223
  in register.py has the equivalent message). Line 275 `announced_provider`
  writes `services["openai"].provider`.
- Lines 318-400 `watch_service_binds`: unchanged in behaviour. The note
  lands in the local `services` map too, so a re-register includes it.
- Lines 519-535 `AgentRegister`: send `proto`, `machine`, `services`. Drop
  `gpu_vendor`, `cpus`, `services_ports`, `provider`, `service_notes`. Line
  542: check `proto` on `AgentRegisterOk` and exit with a clear message on a
  major mismatch, since an agent cannot do anything useful against the wrong
  daemon.
- Lines 675-677: delete the `Ping` arm. `Pong` at 674 stays, since it
  answers the agent's own ping.
- Lines 683-730 `spawn_actor`: the actor host opens with `host_hello
  {proto}` and the agent answers `ctor {proto}`. Check the host's major and
  report a spawn failure on mismatch.
- Tests at 1040-1100 cover the two-form parsing and need the single form.

## rust/mentatd/src/claim.rs

- Lines 33-39 `Link::parse`: accept `rdma` and `ip` only.
- Lines 44-49 `SetReq`: `bundles: Vec<u32>`, add `vendor: Option<String>`.
- Lines 83-95 `Topology.free_gpus: BTreeMap<NodeId, f64>` becomes
  `BTreeMap<NodeId, BTreeMap<String, u32>>` (vendor to free count).
  Candidate sets for one `SetReq` come from one vendor.
- Lines 170-173 `Member`: add `vendor`.
- Lines 145-165 `Path` and `to_json`: hold the set names on each end and
  emit the `{from: {set, node, host, addr, iface}, to: {...}, rtt_ms}`
  shape. The solver at 380-400 knows the `BetweenReq` it is solving, so it
  has the names.
- Lines 447-484 `parse`: replace with serde `Shape` and `View` structs, and
  derive `Serialize`/`Deserialize` so proto.rs can hold them typed.
  `bundles` needs a custom deserializer for "count or list".
- Lines 490-560 topology assembly: fill per-vendor free counts from
  `machine.gpus` and `free_gpus_of`.

## rust/mentatd/src/main.rs

- Lines 296-330: `Msg::StatusOk { data }` becomes `{ snapshot }`. `Stop`
  matches `Msg::Ok`.
- Lines 379-395: the CLI `hello` sends `proto` and checks it on the reply.

## rust/mentatd/src/main.rs, the stop scope

- Lines 88-94 `Cmd::Stop`: add `--all`, and require one of `--group` and
  `--all`. Clap spells this as an `ArgGroup` with `required(true)` and
  `multiple(false)`, so the refusal is the parser's rather than a hand-
  rolled check after it.
- Line 327: send `StopAll { group, all }`.
- The same binary answers to `ray`, where Ray's own `ray stop` is node-
  local. A deployment that inherited that line in an entrypoint now fails
  there rather than killing every group.

## rust/common/src/secret.rs

- Line 20: delete `SIGNED_VERSION`.
- Lines 25, 199-208: `CLOCK_SKEW_S`, `fresh` and `now_s` take `u64` seconds.
  The float was only ever cast.
- Lines 179-190 `peek_universe`: the envelope is the only shape, so the
  `unwrap_or(&v)` fallback goes. Test `peek_reads_universe_from_both_shapes`
  goes with it.
- Lines 1-10 module comment: describes the spark-agent envelope, which is
  still the envelope. Drop the migration sentence.
- Test `announcement_survives_the_listener_path` (line 275) builds the
  payload with `mentat_announce`.

## rust/mentatd-serve/src/main.rs

- Lines 1206-1333 `udp_listener`: exit at boot without a key, as the daemon
  does. Delete the unsigned branch (1311-1333) and the `mentat_announce`
  check (1286). Check `proto` major, logging once per source on mismatch.
  Read `t` as `u64`. Size the buffer at 1400 and drop anything that fills
  it.
- Lines 1200-1205 doc comment describes the pre-signing state.
- Lines 987-1010 `watch_daemon`: apply events instead of discarding them.
  Today both branches call `poll_status`, so the stream is a doorbell and
  the payload is dropped at 990. Apply each event's `patch` to the stored
  snapshot, track the last `seq` per originating node, and call
  `poll_status` only on a gap, on connect, or on the slow interval that
  keeps `counters` fresh. Skip an event whose `node` is not the daemon being
  read: it is that node's own row, replicated, and this daemon's snapshot
  summarises its peers rather than holding them.
- Lines 15-16 and 947-948 doc comments describe the poll-on-every-event
  loop.
- Lines 473-520 `endpoint_of`: read one `services` map. An entry with a
  `host` is used as written. An empty `host` resolves as today's
  `services_ports` path. Line 519 `announced:` text describes the port form.
- Lines 524-540 `best_openai`: `provider` comes from
  `services.openai.provider`. Lines 585-603: read `services.openai` only.
- Lines 892-895 `status.json`: `openai_note` is `services.openai.note`.
  Unchanged shape.
- Line 1496: peers publish `http_port` as before. `control_addr` is unused
  here.
- Tests from 2100 onward build agents with `services_ports` and old peer
  shapes (2511 `an_old_daemon_reports_node_ip_alone` tests a fallback the
  spec removes).

## rust/mentatd-serve/src/tokens.rs

- Lines 48-110: the `provider == "vllm"` gate reads the group table's
  `provider`, which main.rs fills from `services.openai.provider`. No wire
  change here.

## rust/mentatd-serve/src/ui.rs and ws.rs

- ui.rs 322, 344: `provider` is router-internal. Unchanged.
- ws.rs: `/events` frames are unchanged. Anything that reads the snapshot
  inside the first frame follows the status.rs changes.

## python/ray/register.py

- Lines 57-97 `services()`: build `{host, port, path, provider, note}`
  entries. `--provider` writes `openai.provider`.
- Lines 99-121: the register dict sends `proto`, `machine` and `services`.
  Drop `gpus`, `gpu_vendor`, `cpus`, `services_ports`, `service_notes`,
  `provider`. `machine` for a register-only container is `{"memory": 0,
  "cpus": os.cpu_count(), "gpus": []}`. Reporting no devices keeps a spawn
  from arriving here (comment at 150).
- Line 173: check `proto` on `agent_register_ok`.

## python/ray/_client.py

- Lines 113-120: `hello` sends `proto`. Check the reply's major and raise
  `MentatError` naming both versions on mismatch.
- Line 26 `_checked`: take the expected `t` and raise `MentatError` on any
  other reply. Today it checks `err` only, so a wrong `*_ok` surfaces as a
  `KeyError` at the caller.

## python/ray, remaining files

- `__init__.py` 128: send `resources`. `_private/state.py` 8: send
  `available`.
- `actor.py` 73: `num_gpus` is `math.ceil` of the option, an integer.
- `util/placement_group.py` 60-61: `gpu_bundles` are integers. Line 48
  default shape: `link` stays `rdma`.
- `_host.py` 90-93: `host_hello` sends `proto` and drops `actor_id`. Line
  100: read `proto` from `ctor` and exit with a message on a major mismatch.
- `runtime_context.py` 33: `gcs_address` reads `hello["control_addr"]`. The
  Ray-facing property name stays.

## tests

- `mentat_testlib.py` 132, 249: `MENTAT_GPUS` still works. Add a helper that
  sets `MENTAT_MACHINE` for a heterogeneous or UMA inventory. Both clusters
  need `MENTAT_SECRET` in every daemon and router environment, since
  announcements are off without one.
- `test_serve.py` 587-589: `services_ports` assertions become one `services`
  map with empty `host`. 320-330 and 553-623 (t01, t08b) read the new shape.
  t10 (720) needs the key.
- `test_topology.py` 368, `test_multinode.py` 114: `probes` in the snapshot
  is unchanged. Any assertion on the claim view's `from_node`, `to_node`,
  `local`, `remote` reads `from.node`, `to.node`, `from.addr`, `to.addr`.
- New tests: a 1.0 daemon refuses a `hello` of major 2 with `err` and
  closes. Placement of a two-bundle group on a node with one nvidia and one
  amd device stays pending. A UMA agent's `gpus_total` is 1 and
  `mentat_memory_bytes` equals `mentat_gpu_memory_bytes` for its group. An
  announcement of 1401 bytes is dropped by the router.

## Docs and config

- `GUIDE.md` 518-530 and `GUIDE-SERVE.md` 52, 406-415: `MENTAT_SECRET` is
  required for announcements. `GUIDE.md` 680: `MENTAT_GPUS` yields fake
  devices, `MENTAT_MACHINE` a full inventory.
- `mentatd.yaml` 80-85, `mentatd-serve.yaml` 36-39: the comment says unset
  leaves announcements unsigned. Unset turns them off.
- `README.md` 106: unchanged.

## Design notes

Version field. `proto` is a `"major.minor"` string on the opening frame in
each direction. The alternative was a bare integer per frame or per link
with capability bits for features. The string holds the minor, and "send
nothing newer than the peer's minor" replaces every capability bit and every
"defaulted for old daemons" comment. A number pair `{"major": 1, "minor":
0}` would save a split at the cost of a nested object on every hello. The
string appears in logs and yaml as written.

UMA placement. Per-device `uma` rather than per-machine, because an APU
beside a discrete card is a real box even if not an inference target, and
the per-device flag costs one boolean. A UMA device reports its pool size as
`memory` so every consumer reads `gpus[].memory` uniformly. The alternative
was omitting `memory` on a UMA device, which avoids a copy of
`machine.memory` but makes every reader branch. Memory does not take part in
placement in 1.0. The alternative, memory-sized bundles, is a minor bump:
add `memory` to a bundle and to the view.

Vendor rule. One vendor per placement group and per claim set. This is the
only placement consequence of a heterogeneous inventory. The alternative was
to leave placement vendor-blind and let a collective fail at NCCL init,
which reports nothing useful.

Signing. A key is required. The alternative that keeps a keyless LAN working
with one format is HMAC over an empty key when none is configured, so a
keyed router still rejects a keyless daemon and a keyless router accepts
one. That is one code path but the signature then means nothing on a keyless
deployment, and reading the log line `verify: off` next to signed datagrams
would mislead. Setting one variable is cheaper than explaining that.

Services. One entry shape with an empty `host` for "resolve me". The
alternative was to keep the URL form for operator-named hosts. A URL whose
host is meaningful is exactly `{host, port, path}` with the host filled, so
the two forms were one type with an unparsed fallback for values the agent
could not read. 1.0 rejects those at start instead. `https` is out of scope.
A `scheme` field is a minor bump.

Response names. `x` is answered by `x_ok`, `ok` or `err`. The alternative
was one `ok` type with the result fields inlined. The shim already parses by
request context: `_checked` tests for `err` and the caller reads fields off
the dict, so a `nodes_ok` answering a `wait` passes unnoticed today. On the
Rust side each response is a serde variant with named fields, so a handler
that builds one has its field names checked at compile time, and the CLI
matches `status_ok` and rejects anything else. A collapsed `ok` would be a
flattened `Value` built with `json!`. A named frame also reads on its own in
a capture. The cost is six bytes per response. Since the names exist,
`_checked` should check them, which is the one-line change above.

Release. Removed. Nothing constructed it, and the session path ends a claim:
`reap_client` runs at the session's EOF and `release_all` drops the client
from every holder set. That path covers a driver that dies without sending
anything, applies the grace, and skips a session cut by a head change. A
second path would need its own answer to a release inside the grace. A minor
bump can add one back.

Ping. Agent to daemon only. The agent's ping makes a dead daemon noticeable
within `MENTAT_AGENT_PING_INTERVAL_MS`, since the send fails. The daemon
learns of a lost agent from the link's EOF, and the mesh refreshes
`last_seen_ms` from `peer_status`, so neither has ever sent one. A handler
for a direction with no sender is untested code. A minor bump can add the
direction when something needs it.

Claim view `between`. Each end is an object with the set name. v0 used
`from`/`to` for host names and `from_node`/`to_node` for ids, with the set
names lost, so a reader with three sets could not tell which `between`
answered which request without recomputing the solve.

Announcement cap. 1400 bytes so a datagram never fragments on a 1500 MTU
path. The sender enforces it, since a listener that cannot read a datagram
can only drop it. The alternative was to move `addrs` and `addr_tags` out of
the datagram and have the router read them from `/status`, which shrinks the
datagram to a fixed size but changes how the router picks the first address
to watch.

Unknown message type. `err` and keep the link, matching what the mesh does
today with `peer_unexpected_msg`. Closing would turn a minor-version bug
into a reconnect loop.

Id prefixes. One letter, `a:` and `p:`, rather than a word or a typed
object. The prefix appears in snapshot keys and event paths where the
collection name already says the type, which is redundant there and the
price of one spelling everywhere. A bare actor id stays unresolvable: giving
`ref_get` a meaning for it would invent a readiness state actors do not
have.

`ref_get_ok.status` for a removed placement group. It resolves as
`actor_died`, which `python/ray/__init__.py:86` maps to `RayActorError`. The
name is wrong for a placement group and 1.0 could rename it, but the shim's
exception mapping is the Ray-facing contract, so it is left alone.