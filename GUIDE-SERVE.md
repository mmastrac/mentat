# Serving several models behind one endpoint

`mentatd-serve` puts one OpenAI-compatible endpoint and one MCP endpoint in
front of every model the cluster runs. Clients name a model, the router picks
the group serving it. Adding a second model means starting a second deployment,
not reconfiguring the front of the stack.

It is a separate binary and container from `mentatd`. The daemon never touches
inference traffic, so the router can restart, move or stop while models keep
serving whoever is already talking to them. Overhead is ~3.2 MiB RSS.

## What a group is

A group is one model deployment: a driver and the agents holding its GPUs, all
sharing a `MENTAT_GROUP` value. Placement, `ray.nodes()`, `cluster_resources()`
and `ray status` are scoped to it, so two models on one box never count each
other's GPUs.

Running the same model twice means two deployments with distinct
`MENTAT_GROUP` values. A second driver inside one group is rejected at
`ray.init`.

## Announce the endpoints

Model containers announce through agent registration. The entrypoint exports
these before `ray start`, and the agent reads them once:

```bash
export MENTAT_OPENAI_API=http://10.0.0.1:8000/v1   # rank serving the API
export MENTAT_MCP_API=http://10.0.0.1:9000/mcp     # every rank
ray start --address=$RAY_ADDRESS
```

Both are optional and additive. An agent without them registers as before, and
a daemon that predates the field ignores it.

Only the rank running the API server sets `MENTAT_OPENAI_API`, since only that
rank answers inference. Nothing enforces this: the agent announces whatever is
set, and the router takes the lexically first if several ranks announce. Every
rank sets `MENTAT_MCP_API`, because every rank runs a status server.

## Run the router

```bash
mentatd-serve          # or `mentatd serve`, which finds it on PATH
```

No configuration is required. Daemons feed one watch set from their UDP
announcements on 6382, the `MENTAT_DAEMONS` seed (the local daemon by
default), and the mesh's own membership once any daemon answers. Each watched
daemon is polled on `/status` with its `/events` WebSocket held open, so a
cluster event triggers an immediate re-read.

Container form needs `network_mode: host`, because announced endpoints live on
host addresses that bridge networking cannot reach. See
[mentatd-serve.yaml](mentatd-serve.yaml).

## How a model becomes routable

Two facts admit a group to `/v1`: it has a running actor, and its announced
endpoint answers a `/models` probe. The probe is also where served model names
come from, so whatever a container serves under is what routes to it. Nothing
announces model names.

An engine is admitted as soon as its API answers, which on some models is
during the self-test window.

`/status.json` says why a model is missing. Each group carries a `healthy` flag
and, when false, a `why_not` naming the failed gate: no endpoint, no running
actors, unprobed, probe failed, or probe stale.

A probe that fails on a reused connection is retried once on a fresh one
before the group is marked unhealthy. Servers close idle keep-alive
connections, uvicorn among them, and a probe landing on one gets an error
indistinguishable from a dead endpoint. Without the retry a healthy model
drops out of the route table.

Requests split three ways. A known model routes and streams through, frame by
frame with backpressure, so time to first token survives the hop. A model whose
group exists but is ungated returns 503 with the reason. A name nothing claims
returns 404. Request bodies over 128 MiB are refused.

## The MCP merge

`/mcp` merges every group's management MCP into one endpoint. Tool names are
prefixed `<group>__`, so identical names across containers cannot collide, and
`tools/list` answers are cached per group.

The merge skips the health gate on purpose. A status server matters most while
its engine is loading or wedged, which is exactly when the routing gate would
exclude it.

One native tool comes from the router itself. `serve_status` reports the
watched daemons, each group's health and endpoints, and the model table.

## Check it

```bash
curl -s http://<box>:6381/v1/models          # what routes right now
curl -s http://<box>:6381/status.json | jq . # and why, per group
```

`/`, `/healthz` and `/status.json` return the same document.

## Tuning

`SERVING_TIMEOUT_S` (1800) caps one upstream request, and a non-streaming
answer only arrives when generation ends. Lower it and long generations get cut
before any hung request does.

`ALLOWED_SOURCES` applies to both an announcement's source address and the
address it claims. The default covers the local subnets, loopback and the
docker bridge ranges, since a bridge-networked client keeps a `172.x` source.

`MENTAT_SECRET` must match the daemons'. `MENTAT_UNIVERSE` (default
`default`) separates clusters sharing a broadcast domain, dropping foreign
announcements without logging them.

`MENTAT_DAEMONS` distinguishes unset from set-and-empty. Unset seeds the local
daemon, empty means UDP only. Compose cannot express empty, since
`${VAR:-default}` reads it as unset.

`POLL_INTERVAL_S`, `PROBE_INTERVAL_S`, `PROBE_TIMEOUT_S`, `PROBE_FRESH_S`,
`MCP_TIMEOUT_S`, `TOOLS_TTL_S` and `DISCOVER_PEERS` exist but are left out of
the compose file. The defaults derive from each other: `PROBE_FRESH_S` is three
probe intervals plus a timeout. Setting one alone makes groups flap in and out
of the route table.

## Limits

- `MENTAT_SECRET` signs announcements, and a keyed router refuses unsigned
  ones. The control port still has no authentication, so every claim is
  re-read over TCP before it affects routing.
- The router must reach both every daemon's HTTP port and every announced
  endpoint. On a cluster whose models live on a private subnet, it has to run
  on a box attached to that subnet.
- Admission tracks the probe. A model that answers `/models` while still
  warming up is routable.
- Health is per group. A group with one wedged rank reads healthy while its
  API answers.
