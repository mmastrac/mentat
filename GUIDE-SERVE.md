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
export MENTAT_OPENAI_API=8000/v1   # rank serving the API
export MENTAT_MCP_API=9000/mcp     # every rank
ray start --address=$RAY_ADDRESS
```

Both are optional and additive. An agent without them registers as before, and
a daemon that predates the field ignores it.

### Which form to use

A value takes one of two forms:

| Value | Meaning |
| --- | --- |
| `8000/v1`, or `http://0.0.0.0:8000/v1` | Every address this node answers on |
| `http://10.0.0.1:8000/v1` | That address, and only that address |

Prefer the port form. An endpoint announced on one address is reachable only
from that link, so a router off it can never route to the model however
healthy the model is. The port form leaves the host to the router, which
resolves it against every address the node announces and picks by probing —
so the same container image serves a router on the LAN and one on the fabric,
and a group stays routable when a fabric cable drops.

The port form assumes the API server binds the wildcard address, which is
what `--host 0.0.0.0` does and what vLLM does by default. A server bound to
one address breaks that promise, and the symptom — an endpoint that probes
fine from one box and refuses from another — reads like a network fault. The
agent watches its own `/proc/net/tcp` for the announced port and, if the bind
is narrow, logs `service_bind_narrow` and attaches the finding to the
announcement, so `/status.json` says `bound to 10.100.0.1 only` next to the
failed probe. It only warns. The router's probe stays the only thing that
admits an endpoint.

The URL form stays supported and is not deprecated. It is the escape hatch
for a server the port form cannot describe — a different host, a reverse
proxy, a port published out of a bridge network. A URL is used exactly as
written. `ALLOWED_SOURCES` does not apply to it: that list covers addresses
the router derived for itself.

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
actors, unprobed, probe failed, or probe stale. A probe failure quotes every
candidate address it tried, and appends the agent's own bind finding when
there is one.

### Several addresses for one endpoint

A port announcement resolves to one candidate URL per address of the
announcing node, best first: addresses on a subnet the router is attached to
lead, and the node's own ranking orders each half. Every derived candidate is
checked against `ALLOWED_SOURCES` first.

The prober walks that list and then stays put. Once an address answers it is
kept, so live traffic is never moved by a re-decision. When it stops
answering the router falls through to the next candidate, and every
`PROBE_PROMOTE_S` it re-tries the addresses ranked above the one in use, so a
repaired link is taken back with no operator involved. `/status.json` shows
`openai` (in use) beside `openai_candidates` (all of them), which is how a
dropped cable looks from the router.

A probe that fails on a reused connection is retried once on a fresh one
before the group is marked unhealthy. Servers close idle keep-alive
connections, uvicorn among them, and a probe landing on one gets an error
indistinguishable from a dead endpoint. Without the retry a healthy model
drops out of the route table.

Only the probe and the status poll retry, because only they are idempotent
GETs. A proxied request is sent once: retrying would re-send work the engine may
already be doing, and would double the wait on one that accepts a connection
then stays silent. A client that fails fast can decide for itself.

Any POST carrying a `model` is routed to whoever serves it, so `/v1` and the
root-level endpoints both work: `/v1/chat/completions`, `/tokenize`,
`/detokenize` and whatever else the engine exposes. vLLM's endpoint set moves
between versions, and the router follows the contract it can actually check
rather than a list that would go stale. A body with no `model` is refused,
which is what the management endpoints get.

The announced base ends in `/v1`, and a root-level path is resolved against
it with the `/v1` removed. Appending `/tokenize` to the base would ask for
`/v1/tokenize`, which does not exist.

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

`/`, `/healthz` and `/status.json` return the same document. It carries
`uptime_s`, which is how you tell a router that has been up all along from
one that has been restarting: several of the router's guards are per-process,
so a log line that looks like it repeats every round may be one line per
process. A line stamped earlier than now minus `uptime_s` came from an
earlier one.

## Tuning

`SERVING_TIMEOUT_S` (1800) caps one upstream request, and a non-streaming
answer only arrives when generation ends. Lower it and long generations get cut
before any hung request does.

A router that shares no wire with a fabric should leave that fabric's prefix
out of `ALLOWED_SOURCES`. Candidate addresses sort own-subnet first, and a
router on a third subnet sees no candidate as local, so the node's own
ranking decides and it ranks the fabric first. The router then waits a whole
`PROBE_TIMEOUT_S` per round reaching for an address it can never use. It
recovers by falling through, and leaving the prefix out skips the round trip
entirely.

`ALLOWED_SOURCES` applies to an announcement's source address and to any
address it advertises before that address is watched. It does not apply to
the address a node calls its own, which nothing acts on. The default covers
the local subnets, loopback and the docker bridge ranges, since a
bridge-networked client keeps a `172.x` source. A rejected source logs
`announce_source_not_allowed` once.

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
- The router must reach every daemon's HTTP port, and at least one candidate
  address of every announced endpoint. With the port form that is any link it
  shares with the model's node; with the URL form it is the one address the
  announcement names.
- Admission tracks the probe. A model that answers `/models` while still
  warming up is routable.
- Health is per group. A group with one wedged rank reads healthy while its
  API answers.
