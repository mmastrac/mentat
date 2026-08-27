# mentat

A minimal Ray replacement for vLLM multi-node serving on the GB10 boxes.
Rust daemon + CLI, plus a pure-Python `ray` package that satisfies exactly
the API surface vLLM's `RayExecutorV2` uses. Named after the human computers
of Dune; it does the thinking Ray was hired for, without the baggage.

## Why it exists

Ray was in the glm53/ds4-flash images for one reason: `--distributed-executor-
backend ray` is how TP spans nodes. On unified-memory GB10 it caused real
damage, all observed on 2026-08-27:

- Its memory monitor kills workers when the NODE crosses 95% memory. The
  ~89 GiB of model weights ARE node memory here, so it killed healthy ranks
  seconds after the engine came up -- and logged why only in the raylet event
  log.
- Its object store defaults to ~30% of RAM (~36 GiB on the uncapped worker),
  taken from the pool the weights and KV cache need, for a model that moves
  every tensor over NCCL.
- `ray start --address` does not retry, forcing the head-first stage-file
  ordering barrier.
- A hard kill leaves orphaned workers pinning ~88 GB invisible to `ps` RSS.

mentat has no object store, no memory monitor, no dashboard. Registration
retries forever. Every lifecycle decision is one structured log line in the
container log, and an actor death reason (exit code/signal) arrives inside
the driver's `RayActorError`.

The shim is control-plane only, which is why pure Python costs nothing:
verified against the image's vLLM (`0.1.dev20051+g487ecf187`),
`execute_model` runs over vLLM's own MessageQueue + NCCL with zero Ray calls
per token; after boot the only recurring call is the health monitor's
`ray.wait` every 5 s.

## Pieces

- `mentatd` (`mentat daemon`), one per box, host-level container
  ([mentatd.yaml](mentatd.yaml)). Control port 6379 (what `RAY_ADDRESS`
  points at), HTTP 6380: `/metrics` (Prometheus), `/status` (JSON),
  `/events` (WebSocket: snapshot on connect, then `node_join/leave`,
  `head_change`, `agent_register/lost/degraded/dead`,
  `pg_created/ready/timeout`, `actor_spawning/running/dead`,
  `driver_connected/disconnected`).
  Daemons mesh over `MENTAT_PEERS`, elect a head (lowest node id, 5 s
  hold-down), replicate events, and exchange status so any daemon shows the
  whole cluster.
- `mentat` agent (`ray start ...`, same binary), one per model container.
  Registers the container's GPUs under a group and is the only thing that
  spawns actor processes (they must run the container's Python). Actors get
  their own process group; kills take the whole tree.
- The `ray` shim (`python/ray/`), installed with `pip install --no-deps`,
  claims the `ray` import name. Reports `__version__ == "2.57.0"` because
  vLLM version-checks it; logs a mentat banner at `ray.init`. Anything
  outside the audited surface raises `NotImplementedError` -- silent stubs
  are how this design would rot.
- `mentat-serve` (`serve/`, its own tokio+hyper crate so those dependencies
  stay out of the daemon build), the serving front door in a separate
  container ([mentat-serve.yaml](mentat-serve.yaml)) -- mentatd never touches
  inference traffic. HTTP on 6381: an OpenAI-compatible `/v1` that routes by
  model name to the right group's API with streaming passed through, and one
  `/mcp` merging every container's management MCP (tools prefixed
  `<group>__`, plus a native `serve_status` tool showing the route table).
  Discovery comes from the daemon rather than UDP: seeded by
  `MENTAT_DAEMONS` (the local daemon by default), it follows the mesh's own
  membership to every other daemon, polling each `/status` and holding each
  `/events` WebSocket so any cluster event triggers an immediate re-read.
  This replaces spark-agent's serving proxy.

## Service announcement

Model containers announce their endpoints through the agent registration:
the entrypoints export `MENTAT_OPENAI_API` (head only -- a TP worker serves
nothing) and `MENTAT_MCP_API` (every rank; the status server runs on all of
them) right before `ray start`, the agent carries them on `AgentRegister`,
and the daemon republishes them in `/status` and the `agent_register` event.
Everything is optional and additive: an agent without the env vars registers
exactly as before, and an old daemon ignores the extra field.

Routing is gated on two facts: the group has a running actor, and the
announced endpoint answers a `/v1/models` probe (also where served model
names come from, so `SERVED_NAME` needs no separate announcement). One
deliberate gap: an engine is admitted as soon as its API answers, which on
glm53 is during the self-test window. The `/mcp` merge skips the health
gate: the status server matters most while the engine is loading or wedged.

## Groups

One mentat cluster, many models. Every driver and agent carries
`MENTAT_GROUP` (entrypoints default it to `SERVICE_NAME`), and placement,
`ray.nodes()`, `cluster_resources()`, and `ray status` are all scoped to the
caller's group -- so the entrypoint's `GPU >= TP` gate means "my workers are
here" no matter what else is serving. TP=N takes N single-GPU agents (or
fewer multi-GPU ones); nothing assumes a pair. Running the same model twice =
two compose stacks with distinct `MENTAT_GROUP` values; a second driver for
one group is rejected at `ray.init`.

Rendezvous today: the driver and ALL of a group's agents must talk to the
same daemon -- which the entrypoints already arrange (`RAY_ADDRESS` for the
driver, `ray start --address=$RAY_ADDRESS` for the worker agent, the local
daemon for the head agent, all landing on the head box's mentatd). The mesh
is observability + head designation; moving rendezvous authority onto the
elected head is a later phase.

## Deploy order

1. `./build.sh` on gx10-n3 → `mentat-artifacts:<ver>` + `mentatd:<ver>` +
   `mentat-serve:<ver>`.
2. `mentatd.yaml` to `~/compose/mentatd/` on each box with the per-node
   `.env` from the comments in that file (`MENTAT_NODE_IP` is the CLUSTER
   address -- `10.100.0.x` on the pair). Up it. **Before any converted model
   container.**
3. Rebuild glm53 / ds4-flash (their Dockerfiles `COPY --from=mentat-artifacts`).
4. `docker compose up` the models -- either node first; ordering stopped
   mattering.
5. Optional, any box that should answer clients: `mentat-serve.yaml` to
   `~/compose/mentat-serve/`. No per-node `.env` required: it seeds from the
   local daemon and follows the mesh membership to the rest.

Rollback: point `IMAGE` at the previous (real-ray) image tag. The
entrypoints kept full `ray` CLI compatibility, the neutralized
`RAY_OBJECT_STORE_MEMORY` / `RAY_memory_monitor_refresh_ms` knobs, and the
head-first ordering still documented for exactly that case.

## Operating

```
mentat status [--group g] [--json]   # group view prints the N.0/M.0 GPU line
mentat stop [--group g]              # kill actors: the unstick lever
curl -s http://<box>:6380/status | jq .
curl -s http://<box>:6380/metrics
websocat ws://<box>:6380/events      # snapshot, then live events
curl -s http://<box>:6381/v1/models  # what mentat-serve will route right now
curl -s http://<box>:6381/status.json | jq .   # and why (health per group)
```

If a rank dies, the reason is in the container log (`event=actor_exit`
pid/signal) and in vLLM's own exception text. If the head-box daemon
restarts mid-serve, agents reconnect and the daemon kills the now-ownerless
actors; vLLM exits and the stack restarts clean -- nothing leaks.

An agent link EOF does not kill actors immediately: the daemon holds them
through a degrade window (event `agent_degraded` at 30 s, calls issued
meanwhile are held and drained on reconnect) and gives up at 60 s (event
`agent_dead`, actors marked dead, run() sentinels resolve, the driver
restarts). `mentat stop` stays instant. A placement group that never gets
its agents fails after 10 min instead of pending forever. All of these
windows are env-tunable -- see Tuning.

## Tuning

Every lifecycle window is a MENTAT_* env var in milliseconds, read once at
process start. The daemon reads the cluster-level ones (mentatd's per-node
`.env`), the agent reads the container-level ones (the model container's
env). Defaults sit on the `Cfg` struct in [config.rs](rust/src/config.rs),
where each is read, and an unparsable value logs `bad_env_ms` and uses the
default. The defaults fit the serving pair. Change a knob when a measured
time disagrees with it, and change it to that measurement plus margin.

`MENTAT_PG_PENDING_TIMEOUT_MS` (600000 = 10 min): the clock on "my workers
never showed up". A placement group still waiting for GPUs after this fails,
and the driver gets an error instead of hanging forever. Time your slowest
expected cold start end to end -- image pull, weight mount, container boot,
agent registration -- and keep this above it. Lower it if you would rather
learn about a wedged worker box in two minutes than ten. vLLM's own wait
loop tolerates the full window.

`MENTAT_AGENT_DEGRADED_AFTER_MS` (30000) and `MENTAT_AGENT_DEAD_AFTER_MS`
(60000): what happens when a model container loses its daemon link. Nothing
dies right away. The agent reconnects on its own, and calls made during the
outage are held and delivered when it does. At the degraded mark an
`agent_degraded` event fires, so set that to "how long before I want to
hear about it". At the dead mark the daemon gives up: actors are marked
dead, vLLM's monitor sees its run() refs resolve, and the stack restarts.
Set that to the point where restarting beats waiting. A restart costs the
~9 min model boot, so if your outages are switch reboots that heal in two
minutes, raise it well past two minutes. If a lost link always means a dead
container, lower it and restart sooner. Keep degraded below dead, or the
warning never fires before the give-up.

`MENTAT_PEER_STALE_AFTER_MS` (30000) and `MENTAT_PEER_DEAD_AFTER_MS`
(60000): the same two steps for daemon-to-daemon links, catching a daemon
that wedges while its socket stays open. Serving traffic never crosses the
mesh, so these only decide how fast `mentat status` and the head
designation notice. The heartbeat they judge by is the status push
(`MENTAT_PEER_STATUS_INTERVAL_MS`, 2000). Keep the stale window several
pushes wide, or healthy peers flap.

`MENTAT_ELECTION_HOLD_DOWN_MS` (5000): how long the would-be head must stay
stable before `head_change` commits. Raise it if a flapping link churns the
designation. The tests shorten it. Production has no reason to.

`MENTAT_SLOW_CALL_WARN_MS` (15000): warning only, one `call_pending_long`
log line per slow call. It exists to catch a future vLLM issuing a call
behind a blocking one. Boot-time calls like `wait_for_init` legitimately
pend for minutes and draw the line once per boot -- expected. The same line
at steady state is a bug report. Raise it if the boot-time line bothers
you.

`MENTAT_SESSION_REAP_GRACE_MS` (0): how long a dead vLLM's workers linger
before the daemon kills them. Keep 0. A restarting vLLM needs the old
actors' names and GPUs back, so every second of grace is a second added to
the restart. Set it briefly only to inspect workers after a driver crash.

Container-side:

`MENTAT_HOST_CONNECT_TIMEOUT_MS` (60000): how long the agent waits for a
freshly spawned actor process to dial back. This covers python starting and
importing the small host module only -- the heavy vLLM import happens after
the handshake -- so it fires only when python itself cannot start. Leave it.

`MENTAT_AGENT_PING_INTERVAL_MS` (2000): how often the agent pings the
daemon, which bounds how fast a dead daemon is noticed. Cheap either way.

`MENTAT_TCP_DEAD_AFTER_MS` (75000), read by both sides: the Linux TCP
keepalive target for a peer that wedges while its connection stays open (a
hard power-off, a hung box). The kernel-level backstop behind every window
above. Rarely worth touching.

## Testing

GPU-free, self-contained (daemon + agents as subprocesses, fake GPUs):

```
python3 tests/test_e2e_local.py   # audited behaviors incl. kill -9 liveness,
                                  # pg timeout, degrade window, give-up
python3 tests/test_groups.py      # TP=4, parallel groups, same model twice
python3 tests/test_vllm_shape.py  # call-for-call replay of RayExecutorV2
python3 tests/test_multinode.py   # 3-daemon mesh: election, head death,
                                  # peer staleness (short MENTAT_* windows)
python3 tests/test_serve.py       # mentat-serve: routing, gating, MCP merge
cargo test                        # framing, WS handshake, the status-line grep contract
```

`test_serve.py` builds and runs the real router binary against real
daemon+agents with fake endpoints (`MENTAT_SERVE_TEST_BINARY` skips the
cargo build, like `MENTAT_TEST_BINARY`), and proves pass-through streaming
by timing the gap between SSE frames.

On-hardware gate before touching the serving pair: build the glm53 image on
n3, run the entrypoint's exact `ray start`/`ray status` incantations in it,
then a small model with `vllm serve -tp 1 --distributed-executor-backend ray`
(TP=1 exercises the whole executor path: pg, RayWorkerProc, MQ handle,
monitor loop).

## Sharp edges to keep in mind

- Actors are serial, like real ray: a method call issued after `run()` queues
  forever. vLLM never does this (shutdown is `ray.kill`); the daemon logs
  `call_pending_long` if anything else ever does.
- The shim covers this vLLM's audited surface. On a base-image bump, re-audit
  (`grep -rn 'ray\.' <site-packages>/vllm/v1/executor/`) before rebuilding --
  the Dockerfile layers say the same thing.
- `VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1` is pinned in both entrypoints; the
  legacy executor's compiled-DAG surface is deliberately not implemented.
