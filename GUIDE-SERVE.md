# mentatd-serve

## Name

mentatd-serve: OpenAI-compatible router and merged MCP endpoint for a mentat
cluster.

## Synopsis

```
mentatd-serve
mentatd-serve --version
mentatd serve
```

## Description

`mentatd-serve` puts one OpenAI-compatible endpoint and one MCP endpoint in
front of every model the cluster runs. A client specifies a model and the router
forwards the request to the group serving it. Adding a model means starting
another deployment. The router needs no change.

It is a separate binary and container from `mentatd`. The daemon never
touches inference traffic, so the router can restart, move or stop while
models keep serving the clients already talking to them.

Configuration is by environment. There are no options other than
`--version`. A containerised router needs `network_mode: host`, because
announced endpoints sit on host addresses that bridge networking cannot
reach.

### Discovery

The router builds a watch set of daemon HTTP addresses from three sources:
UDP announcements on `MENTAT_ANNOUNCE_PORT`, the `MENTAT_DAEMONS` seed list,
and the mesh membership each watched daemon reports. Each watched daemon is
polled on `/status` every `POLL_INTERVAL_S` with its `/events` WebSocket
held open, so a cluster event re-reads at once. A burst of events coalesces
into one re-read.

An announcement is a hint. It adds one address to watch. Every claim in it
is re-read over TCP and probed before it affects routing. The datagram's
source address and every address it advertises must match
`ALLOWED_SOURCES`. With `MENTAT_SECRET` set, unsigned announcements are
refused.

The group table merges every daemon's view into one entry per group name. A
view older than three poll intervals is stale. When two daemons disagree
about a group, the one reporting more running actors wins.

### Admission

A group is routable on `/v1` when a live agent announces an OpenAI endpoint
and that endpoint answers a `/models` probe. The probe is also where model
names come from: whatever the engine lists under `/v1/models` is what routes
to it. Nothing announces model names.

A group whose agents offer GPUs must also have a running actor. Offering GPUs
is what makes a group a placement target, so its engine runs inside actors
mentat spawned and their state says something: an endpoint that outlives every
rank still answers `/models` from a process whose ranks are gone. A group whose
agents offer none had nothing placed -- a single-rank engine registered by
`python -m ray.register` -- so the probe is the whole test.

An engine is admitted as soon as its API answers, which on some models is
during its self-test.

`/status.json` says why a model is missing. Each group carries `healthy`
and, when false, `why_not` naming the failed gate: `no announced OpenAI
endpoint`, `no running actors`, `not probed yet`, `endpoint probe failed`, or
`endpoint probe stale`. A probe failure quotes every candidate address it
tried and appends the agent's own bind finding when there is one.

### Candidate addresses

A port-form announcement (see "Announcing endpoints") resolves to one
candidate URL per address of the announcing node. Candidates on a subnet
the router is attached to sort first. Within each half, the node's own
ranking from `MENTAT_ANNOUNCE_IFACES` orders them. Every candidate is
checked against `ALLOWED_SOURCES`. A URL-form announcement is its own single
candidate. The router uses it as written and skips the allowlist check.

The prober walks the list and keeps the first address that answers. Live
traffic stays on it until it stops answering, then the router falls through
to the next candidate. Every `PROBE_PROMOTE_S` the router re-tries the
addresses ranked above the one in use, so a repaired link is taken back
without operator action. `/status.json` shows `openai` (in use) beside
`openai_candidates` (all of them, best first). A group serving from its
second candidate is how a dropped link looks from the router.

A probe that fails on a reused connection is retried once on a fresh one
before the group is marked unhealthy. Servers close idle keep-alive
connections, and a probe landing on one gets an error indistinguishable from
a dead endpoint. Only the probe and the status poll retry. A proxied request
is sent once, since a retry would re-send work the engine may already be
doing.

### Request handling

Any POST whose body carries `model` is forwarded to the group serving that
model, so `/v1/chat/completions`, `/tokenize`, `/detokenize` and any other
endpoint the engine exposes all work. A body with no `model` is refused
with 400.

The announced base ends in `/v1`. A root-level path such as `/tokenize` is
resolved against the base with the `/v1` removed.

A known model routes and streams through, frame by frame with backpressure.
A model whose group exists but is not admitted returns 503 with the reason.
A name nothing serves returns 404. Bodies over 128 MiB are refused. One
upstream request may run for `SERVING_TIMEOUT_S`.

## Announcing endpoints

Model containers announce endpoints through agent registration. The
entrypoint exports these before `ray start`, and the agent reads them once:

```bash
export MENTAT_OPENAI_API=8000/v1      # the rank serving the API
export MENTAT_MCP_API=9000/mcp        # every rank
export MENTAT_MODEL_PROVIDER=vllm     # what serves the API
ray start --address=$RAY_ADDRESS
```

All three are optional. An agent without them registers as before.

`MENTAT_OPENAI_API` belongs on the rank running the API server, since only
that rank answers inference. Nothing enforces this. The agent announces
whatever is set, and the router takes the lexically first if several ranks
announce. `MENTAT_MCP_API` belongs on every rank, because every rank runs a
status server. `MENTAT_MODEL_PROVIDER` specifies the engine behind
`MENTAT_OPENAI_API` and belongs on the same rank. `/status.json` reports it
per group, empty when the container did not say. "Counting tokens" needs it.

### Port form and URL form

An endpoint takes one of two forms:

| Value | Meaning |
| --- | --- |
| `8000/v1`, or `http://0.0.0.0:8000/v1` | Every address this node announces |
| `http://10.0.0.1:8000/v1` | That address only |

Prefer the port form. An endpoint announced on one address is reachable
only from that link, so a router off it can never route to the model. The
port form leaves the host to the router, which resolves it against every
address the node announces. The same image then serves a router on the LAN
and one on the fabric, and a group stays routable when a fabric cable drops.

The port form assumes the API server binds the wildcard address, which
`--host 0.0.0.0` does and vLLM does by default. The agent watches its own
`/proc/net/tcp` for the announced port. If the server bound a single address, the
agent logs `service_bind_narrow` and attaches the finding to the
announcement, so `/status.json` says `bound to 10.0.0.1 only` beside the
failed probe. The finding is advisory. The probe alone admits an endpoint.

The URL form is for a server the port form cannot describe: a different
host, a reverse proxy, a port published out of a bridge network. A URL is
used exactly as written. `ALLOWED_SOURCES` does not apply to it, since that
list covers addresses the router derived for itself.

## HTTP interface

| Method and path | Returns |
| --- | --- |
| `GET /v1/models`, `GET /v1` | The models routable now, each entry as its engine listed it |
| `POST /v1/*` | Forwarded to the group serving the request's `model`, streaming passed through |
| `POST /v1/responses/input_tokens` | A prompt token count. See "Counting tokens" |
| `POST /mcp` | The merged MCP endpoint. See "The MCP merge" |
| any other `POST` | Forwarded by the request's `model`, for root-level engine endpoints such as `/tokenize` |
| `GET /`, `/healthz`, `/status.json` | Route table, per-group health and endpoints, `uptime_s` |
| `GET /stats.json` | Per-model engine and router counters, for the status page |

`GET /` from a browser (an `Accept` header that asks for HTML) returns the
status page instead of the document.

```bash
curl -s http://<node>:6381/v1/models          # what routes right now
curl -s http://<node>:6381/status.json | jq . # and why, per group
```

The status document carries `uptime_s`. Several of the router's guards are
per-process, for example the once-only log of a rejected source, so a log
line that seems to repeat may be one line per process. A line stamped
earlier than now minus `uptime_s` came from an earlier process.

### The status page

`http://<node>:6381/` in a browser is a live table of what the router is
carrying and what each engine is doing with it. The page polls
`/stats.json`.

The engine publishes queue depth, KV usage, token totals and latency
histograms on `/metrics`, so `running`, `waiting`, `kv`, the token counts
and the mean TTFT, queue and inter-token columns come from the engine
serving that model. The router adds `proxied`, the number of requests it is
carrying for that model right now.

Clicking a model lists those requests one per row: body size, time waiting
with no first byte, time to first byte once it arrives, and bytes returned.
A long wait with no first byte while the engine reports nothing running is
an engine that took the request and stopped.

A group that fails its probe keeps its row, dimmed, with the reason in place
of the numbers.

### Counting tokens

`POST /v1/responses/input_tokens` answers how many prompt tokens an input
would cost, in OpenAI's shape:

```bash
curl -s http://<node>:6381/v1/responses/input_tokens \
  -d '{"model":"mymodel","input":"hello world"}' -H 'content-type: application/json'
# {"object":"response.input_tokens","input_tokens":14}
```

The router owns this route. vLLM has no such endpoint, and the path lands on
its `/v1/responses/{response_id}` pattern for a 405.

The serving engine counts the text. The router sends it to that group's
`/tokenize` as a chat request, so the chat template is included.
`instructions` becomes a leading system message and `tools` are passed
through, because the template renders both and the engine then prices
them. Text-only counts match the engine.

Media is estimated at flat rates: 4000 tokens per image and 40000 per
video, whatever the resolution or length. The true cost depends on tiling
and the model's patch size, which the router cannot know without fetching
the media and running the engine's preprocessor. An attachment that is
neither, such as a PDF, contributes only the text that accompanies it.

The route needs `MENTAT_MODEL_PROVIDER=vllm` on the container. A group that
announced no provider, or one the router does not know, gets a 400 naming
the group.

### The MCP merge

`/mcp` merges every group's management MCP into one endpoint. Tool names
are prefixed `<group>__`, so identical names across containers cannot
collide. `tools/list` answers are cached per group for `TOOLS_TTL_S`.

The merge skips the admission gate. A status server matters most while its
engine is loading or wedged, which is when the gate would exclude it.

One native tool, `serve_status`, reports the watched daemons, each group's
health and endpoints, and the model table.

## Environment

An unset or empty variable takes its default. A `*_S` value must be a
positive number. Anything else takes the default.

- `SERVE_PORT` (default 6381)

  HTTP port.

- `MENTAT_DAEMONS` (default `127.0.0.1:6380`)

  Comma-separated daemon HTTP addresses to seed the watch set. Unset seeds
  the local daemon. Set and empty seeds nothing, leaving UDP as the only
  path in. Compose cannot express empty, since `${VAR:-default}` reads it as
  unset.

- `MENTAT_ANNOUNCE_PORT` (default 6382)

  UDP port to listen for daemon announcements on. `0` turns the listener
  off.

- `ALLOWED_SOURCES` (default `10.100.0.,192.168.1.,127.0.0.1,::1,172.`)

  Comma-separated address prefixes. An announcement's source address and
  every address it advertises must match one before the router acts on it.
  The address a node calls its own is not checked, since nothing acts on it.
  `172.` covers bridge-networked clients, which keep a `172.x` source. A
  rejected source logs `announce_source_not_allowed` once, with the
  prefixes in force.

  A router that shares no wire with a fabric should leave that fabric's
  prefix out. Otherwise the router ranks the fabric address first, waits
  `PROBE_TIMEOUT_S` on it every round, and falls through.

- `DISCOVER_PEERS` (default `1`)

  `1` adds the mesh peers of every watched daemon to the watch set. Any
  other value disables it.

- `POLL_INTERVAL_S` (default 10)

  Interval between `/status` polls of each watched daemon. A daemon view
  older than three intervals is stale.

- `PROBE_INTERVAL_S` (default 5)

  Interval between endpoint probes.

- `PROBE_TIMEOUT_S` (default 3)

  Deadline for one probe.

- `PROBE_FRESH_S` (default: three probe intervals plus one timeout)

  How long a probe result stays valid. Past it the group reads
  `endpoint probe stale`. The default clears one round that walks every candidate
  address, since each dead one costs a whole `PROBE_TIMEOUT_S`. Setting it
  alone makes groups flap in and out of the route table.

- `PROBE_PROMOTE_S` (default: six probe intervals)

  How often a group serving from a lower-ranked address re-tries the
  addresses ranked above it.

- `SERVING_TIMEOUT_S` (default 1800)

  Deadline for one upstream request. A non-streaming answer arrives when
  generation ends, so this is sized for generation. Lower it and long
  generations are cut before any hung request is.

- `MCP_TIMEOUT_S` (default 180)

  Deadline for one forwarded MCP call and for the tokenize call behind
  `/v1/responses/input_tokens`. Some management tools block for their whole
  sampling window, so it is longer than `PROBE_TIMEOUT_S`.

- `TOOLS_TTL_S` (default 60)

  How long a group's `tools/list` answer is cached.

- `MENTAT_SECRET` (default: unset)

  HMAC key for announcements. Must match the daemons'. A keyed router takes
  signed announcements only, so a half-applied rollout stops discovery until
  the seed list finds the daemons instead.

- `MENTAT_SECRET_FILE` (default: unset)

  Read the key from this file instead of `MENTAT_SECRET`. A file that cannot
  be read, or reads empty, stops the process at boot with the reason.

- `MENTAT_UNIVERSE` (default `default`)

  Cluster name. An announcement from another universe is dropped before its
  signature is checked, without a log line.

## Limits

- The router must reach every daemon's HTTP port and at least one candidate
  address of every announced endpoint. With the port form that is any link
  it shares with the model's node. With the URL form it is the one address
  the announcement specifies.
- Admission tracks the probe. A model that answers `/models` while still
  warming up is routable.
- Health is per group. A group with one wedged rank reads healthy while its
  API answers.
- The control port has no authentication. Signing covers announcements
  only, and every claim in one is re-read over TCP before it affects
  routing.

## See also

[GUIDE.md](GUIDE.md), [PROTOCOL.md](PROTOCOL.md).
