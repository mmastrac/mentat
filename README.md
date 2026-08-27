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
  `head_change`, `agent_register/lost`, `pg_created/ready`,
  `actor_spawning/running/dead`, `driver_connected/disconnected`).
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

1. `./build.sh` on gx10-n3 → `mentat-artifacts:<ver>` + `mentatd:<ver>`.
2. `mentatd.yaml` to `~/compose/mentatd/` on each box with the per-node
   `.env` from the comments in that file (`MENTAT_NODE_IP` is the CLUSTER
   address -- `10.100.0.x` on the pair). Up it. **Before any converted model
   container.**
3. Rebuild glm53 / ds4-flash (their Dockerfiles `COPY --from=mentat-artifacts`).
4. `docker compose up` the models -- either node first; ordering stopped
   mattering.

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
```

If a rank dies, the reason is in the container log (`event=actor_exit`
pid/signal) and in vLLM's own exception text. If the head-box daemon
restarts mid-serve, agents reconnect and the daemon kills the now-ownerless
actors; vLLM exits and the stack restarts clean -- nothing leaks.

## Testing

GPU-free, self-contained (daemon + agents as subprocesses, fake GPUs):

```
python3 tests/test_e2e_local.py    # audited behaviors incl. kill -9 liveness
python3 tests/test_groups.py      # TP=4, parallel groups, same model twice
python3 tests/test_vllm_shape.py  # call-for-call replay of RayExecutorV2
python3 tests/test_multinode.py   # 3-daemon mesh: election, head death (~40s)
cargo test                        # framing, WS handshake, the status-line grep contract
```

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
