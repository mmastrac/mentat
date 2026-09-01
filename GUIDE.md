# Replacing Ray with mentatd

This is for a working vLLM multi-node deployment on GB10 boxes that runs on
Ray. It swaps Ray's control plane for a Rust daemon and a pure-Python shim.
Your `vllm serve` line barely changes.

## What changes

The `ray` CLI and the `ray` import name keep working.
`--distributed-executor-backend ray`, `ray start` and `ray status` all behave
as before. The object store, the memory monitor, the raylet and the dashboard
are gone. Per-token work is untouched: `execute_model` already runs over
vLLM's own MessageQueue and NCCL, so the control plane does nothing after boot
except a `ray.wait` every 5 seconds.

## Before you start: audit your vLLM

The shim implements exactly the surface vLLM's `RayExecutorV2` touches, audited
against `0.1.dev20051+g487ecf187`. Anything outside it raises `AttributeError`
with a message naming the attribute. Check yours matches:

```bash
grep -rn 'ray\.' $(python -c 'import vllm,os;print(os.path.dirname(vllm.__file__))')/v1/executor/
```

If that turns up calls beyond `init/get/wait/remote/kill/nodes/cluster_resources/available_resources`,
`ray.util.placement_group`, and `ray.util.get_node_ip_address`, stop and read
the shim first. A missing attribute fails at engine boot.

`VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1` is required, because the legacy
executor's compiled-DAG surface is unimplemented. `@ray.remote` works on actor
classes, and raises on a plain function.

## 1. Get the artifacts

A binary and a wheel.

```bash
cargo install mentatd
```

The wheel is not on PyPI, so build it from the repo:

```bash
git clone https://github.com/mmastrac/mentat && cd mentat
pip wheel --no-deps -w dist ./python
```

Or pull the published images, which carry both binaries and the wheel:

```bash
docker pull mmastrac/mentat-artifacts:0.4.0
```

Or build them yourself, as the compose deployment does:

```bash
VERSION=0.4.0 ./build.sh
```

That produces `mentat-artifacts:<ver>` (binary plus wheel, for `COPY --from`),
`mentatd:<ver>` (the daemon container) and `mentatd-serve:<ver>` (an optional
router).

## 2. Run a daemon on each box

One per node, on the host, before any model container:

```bash
MENTAT_NODE_IP=10.0.0.1 MENTAT_PEERS=10.0.0.2:6379 mentatd daemon
```

`MENTAT_NODE_IP` must be the address the driver will see itself on, the same
interface as your `VLLM_HOST_IP` rather than the LAN address. Left empty it
takes the default route, which is wrong on any multi-homed box and breaks the
driver-node match.

Container form needs `network_mode: host`, because the control port has to
answer both on `127.0.0.1:6379` for local agents and on the cluster subnet for
the remote one. See [mentatd.yaml](mentatd.yaml).

## 3. Convert the model image

Replace real Ray with the shim. The wheel installs as `ray` and claims the
import name:

```dockerfile
COPY --from=mmastrac/mentat-artifacts:0.4.0 /out/mentatd /usr/local/bin/mentatd
COPY --from=mmastrac/mentat-artifacts:0.4.0 /out/mentatd-0.4.0-py3-none-any.whl /tmp/
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
 && pip uninstall -y ray \
 && pip install --no-deps /tmp/mentatd-0.4.0-py3-none-any.whl
```

`--no-deps` keeps pip from resolving Ray's dependencies. The shim has none.

The symlink keeps `ray start` and `ray status` working in your existing
entrypoint. `ray --version` reports `ray, version 2.57.0 (mentatd 0.4.0)`, and
the shim reports `__version__ == "2.57.0"` because vLLM version-checks it.

## 4. Adjust the entrypoint

Export before `ray start`, since the agent reads these once at registration:

```bash
export VLLM_USE_RAY_V2_EXECUTOR_BACKEND=1
export RAY_ADDRESS=10.0.0.1:6379      # same daemon for driver and every agent
export MENTAT_GROUP=glm53             # one per model deployment
export VLLM_HOST_IP=10.0.0.1          # this rank's cluster address

ray start --address=$RAY_ADDRESS      # detaches, agent runs beside vllm
ray status | grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1
vllm serve ... --distributed-executor-backend ray -tp 2
```

`VLLM_HOST_IP` is per rank and per container, which is fine at two nodes on
one fabric and stops being fine past that: it has to be hand-matched to the
box, and a wrong value is a hang at NCCL rendezvous rather than an error.
[Fabrics](#7-fabrics-optional) covers letting the daemon choose it.

`ray start --head` is accepted and ignored. Both nodes can come up in either
order, and a container that starts before its daemon retries until one
answers.

`ray status` prints exactly one line matching that GPU regex, scoped to your
group, so the `GPU >= TP` gate keeps working.

If you serve more than one model on the cluster, give each its own
`MENTAT_GROUP`. Placement, `ray.nodes()`, `cluster_resources()` and
`ray status` are all group-scoped, so two models on one box never count each
other's GPUs. `MENTAT_GROUP` falls back to `SERVICE_NAME`, then `default`.

Groups are also what lets one endpoint front several models at once.
[GUIDE-SERVE.md](GUIDE-SERVE.md) covers merging them: `mentatd-serve` routes
`/v1` by model name to whichever group serves it, and merges every container's
management MCP into one.

## 5. Delete your Ray workarounds

All of these become unnecessary:

| Workaround | Why it can go |
|---|---|
| `RAY_OBJECT_STORE_MEMORY` / `--object_store_memory` | No object store. Accepted and ignored, logged once as `object_store_flag_ignored`. |
| `RAY_memory_monitor_refresh_ms` | No memory monitor. Nothing samples node memory or kills workers at 95%. |
| Object store capped to 4 GiB | Same. That RAM goes back to weights and KV cache. |
| Head-first startup ordering | Registration retries forever. |
| `ray stop` cleanup between runs | Actors get their own process group and kills take the whole tree. |

## 6. Fabrics (optional)

Skip this at two nodes on one fabric. It matters when the cluster has more
than one RDMA fabric — two cabled pairs, say. Both fabrics are then numbered
out of the same subnet, so only a probe can say who can talk to whom, and a
hand-set `VLLM_HOST_IP` becomes one more thing to get wrong per box.

Tag the links on every box, fastest first:

```bash
MENTAT_ANNOUNCE_IFACES=en*f*np*=connectx+rdma,en*=lan
```

One line serves the fleet: names are `*`/`?` patterns, and the first entry a
name matches decides its rank and tags. The daemons then probe every (own
address × peer address) pair with the source address bound, which is the only
thing that separates a cabled pair from two boxes that merely share a subnet.

Check the result against the patch panel before relying on it:

```bash
mentatd status          # `reach from <addr>: <addr>=ok/0ms ...` per peer
                        # `fabric 0: <addr> <addr> ...` per island
```

A pair you cabled that reads `fail` is a cable or a tag on the wrong
interface; the daemon also logs `fabric_addr_unverified` for a tagged address
no probe has ever confirmed. A pair that reads `ok` on a link nothing was
cabled on is the other mistake.

Once the islands match the cabling, placement uses them: a placement group of
more than one bundle is placed inside one island. Nothing spills across
fabrics. A group that fits nowhere stays PENDING and says why, in
`pending_reason` and again at the pending timeout. Each rank is spawned with
`MENTAT_FABRIC_IP` set to its address on the island it landed on.

Migration is per deployment and needs no flag day. A group whose nodes carry
no `rdma` tag is placed exactly as before, so tagging one pair first — the
cautious order — leaves the other pair booting normally. Within a tagged
pair, nothing changes until a container stops exporting `VLLM_HOST_IP`, which
is that deployment opting in: `VLLM_HOST_IP` beats `MENTAT_FABRIC_IP` for
that reason. Once every deployment has opted in, the per-node
`VLLM_HOST_IP` bookkeeping goes away.

`MENTAT_ISLAND_PLACEMENT=off` on a daemon places multi-bundle groups without
the constraint, for a cluster whose probes disagree with its cabling and no
time to work out why.

Islands are derived over node ids, and an agent joins its node by
`MENTAT_NODE_IP`. A container that reaches its daemon over loopback claims
nothing and takes the daemon's own identity, so it needs no setting. Set it
where a container reaches its daemon across the network, and match the
daemon's value — the same requirement `ray.nodes()` already has.

## 7. Verify

```bash
mentatd status --group glm53          # the N.0/M.0 GPU line the gate greps
curl -s http://<box>:6380/status | jq .
websocat ws://<box>:6380/events       # snapshot, then live lifecycle events
```

At `ray.init` the container log prints a banner naming the group and daemon,
ending `-- this is NOT real Ray`. If you do not see it, you are still on real
Ray.

When a rank dies you get `event=actor_exit` with pid and signal in the
container log, and the exit code and signal come back in the driver's exception
rather than only in a raylet event log.

`mentatd stop [--group g]` kills actors immediately. Use it when something
wedges.

## Rollback

Point your image tag back at the previous Ray-based build. Nothing on the host
needs undoing, since the daemon is inert once no agents talk to it. This works
because the entrypoint keeps full `ray` CLI compatibility either way.

## Limits

- One daemon per group: the driver and every agent must reach the same one.
  The mesh gives you observability and head election. Rendezvous follows
  `RAY_ADDRESS`, so set it to one box and leave it.
- Actors are serial, matching Ray. `run()` never returns for a vLLM worker, so
  a call issued after it queues forever. vLLM never does this, and
  `call_pending_long` in the log means something else did.
- The audited surface holds only for the vLLM it was audited against. A
  base-image bump means re-running the grep above.
- The control port has no authentication and accepts connections from anyone
  who can reach it. Announcement datagrams are unsigned hints, re-read over TCP
  before they affect routing.
- Fabric islands are derived from probes between addresses tagged `rdma`.
  Opting in is per group: a group none of whose nodes carry an `rdma` tag
  places exactly as it did before, so tagging one pair cannot strand a
  deployment on the other. `MENTAT_ISLAND_PLACEMENT=off` turns the
  constraint off outright.
- Tested on one pair of boxes at TP=1 through TP=4, with GPU-free suites for
  the lifecycle behaviour. Your hardware is not in that sample.
