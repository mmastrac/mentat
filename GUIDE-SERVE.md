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
front of every model the cluster runs. A client specifies a model and the
router forwards the request to the group serving it. Adding a model means
starting another deployment. The router does not change.

It is a separate binary and container from `mentatd`. The daemon never
touches inference traffic, so the router can restart, move or stop while
models keep serving the clients already connected to them.

Configuration is by environment. The only option is `--version`. A
containerised router needs `network_mode: host`, because announced
endpoints sit on host addresses that bridge networking cannot reach.

### Discovery

The router builds a watch set of daemons from UDP announcements on
`MENTAT_ANNOUNCE_PORT`, the `MENTAT_DAEMONS` seed list, and the mesh
membership each watched daemon reports. Each watched daemon is polled on
`/status` every `POLL_INTERVAL_S` with its `/events` WebSocket held open, so
a cluster event re-reads at once. A burst of events coalesces into one
re-read.

A daemon is watched on one address however many it is known by. The first
reply identifies the node. A second address that replies as the same node is
kept as an alternate and is not polled. When the polled address stops
replying, the watch moves to an alternate that replies. A watch outside the
seed list, that has replied nothing for `MODEL_TTL_S` and that live daemons
stop listing as a peer, is forgotten. `/status.json` lists each watch with
its `node_id` and `alternates`.

An announcement is a hint. It adds one address to watch. Every claim in it
is re-read over TCP and probed before it affects routing. The datagram's
source address and every address it advertises must match `ALLOWED_SOURCES`.
`MENTAT_SECRET` is required, and unsigned announcements are refused.

The group table merges every daemon's view into one entry per group name. A
view older than three poll intervals is stale. When two daemons disagree
about a group, the one reporting more running actors wins.

### Admission

A group is routable on `/v1` when a live agent announces an OpenAI endpoint
and that endpoint replies to a `/models` probe. The probe also supplies the
model names. Whatever the engine lists under `/v1/models` is the name that
routes to it. Nothing announces model names.

A group with actor rows must also have a running one. For an engine that
runs inside actors mentat spawned, the ranks' state stands in for the
engine's. An endpoint still replying after every rank is gone returns
`/models` from a process whose ranks are gone. A group without rows had
nothing placed, whether it ran `ray start` without requesting a placement or
registered through `python -m ray.register`. For it the probe is the whole
test.

An engine is admitted as soon as its API replies, which on some models is
during its self-test.

`/status.json` gives the reason a model is missing. Each group has `healthy`
and, when false, `why_not` giving the failed gate: `no announced OpenAI
endpoint`, `no running actors`, `not probed yet`, `endpoint probe failed`,
or `endpoint probe stale`. A probe failure quotes every candidate address it
tried and appends the agent's own bind finding when there is one.

### Retirement

A daemon drops the agent and actor rows of a gone container after its
`MENTAT_HISTORY_KEEP_MS`. A group with no rows left is gone from its
snapshots. The router also keeps its own clock per group. The clock starts
when the group is first seen, and every round the group can serve resets it.
After `MODEL_TTL_S` without such a round the group is retired. It leaves
`/v1/models`, `/status.json`, the status page and the routes. `group_retired`
is logged once, with the last reason the group could not serve.

Retirement changes the listing only. The group is still probed every round,
and one successful probe brings it back before the next request.

### Candidate addresses

A port-form announcement (see "Announcing endpoints") resolves to one
candidate URL per address of the announcing node. Candidates on a subnet the
router is attached to sort first. Within each half, the node's own ranking
from `MENTAT_ANNOUNCE_IFACES` orders them. Every candidate is checked
against `ALLOWED_SOURCES`. A URL-form announcement is its own single
candidate. The router uses it as written and skips the allowlist check.

The prober walks the list and keeps the first address that replies. Live
traffic stays on it until it stops replying, then the router falls through
to the next candidate. Every `PROBE_PROMOTE_S` the router re-tries the
addresses ranked above the one in use, so a repaired link is restored
without operator action. `/status.json` shows `openai` (in use) beside
`openai_candidates` (all of them, best first). A group serving from its
second candidate is the router's view of a dropped link.

A probe that fails on a reused connection is retried once on a fresh one
before the group is marked unhealthy. Servers close idle keep-alive
connections, and a probe landing on one gets an error indistinguishable from
a dead endpoint. Only the probe and the status poll retry. A proxied request
is sent once, since a retry would re-send work the engine may already be
doing.

### Request handling

Any POST whose body sets `model` is forwarded to the group serving that
model, so `/v1/chat/completions`, `/tokenize`, `/detokenize` and any other
endpoint the engine exposes all work. A body without `model` is refused with
400.

The router reads the body to find the model name and forwards it byte for
byte, with its `Content-Type` as the client sent it. JSON has the name as a
top-level field. `multipart/form-data` has it as a text field, which is the
form the audio endpoints use: `/v1/audio/transcriptions` posts the upload
beside `model`. The router walks the form's parts, so an upload that happens
to contain `name="model"` routes on the real field. The router never reads
an upload itself. A body in neither form is refused with 400.

The announced base ends in `/v1`. A root-level path such as `/tokenize` is
resolved against the base with the `/v1` removed.

A known model routes and streams through, frame by frame with backpressure.
A request for a model that is not routable when it arrives is held for up to
`MODEL_WAIT_S`, because a model that restarts is missing for a while. A
refused upstream connection is retried inside the same window. After the
window, a model whose group exists but is not admitted returns 503 with the
reason, and a name nothing serves returns 404. Once the upstream has
accepted a request, the router never sends it again. Bodies over 128 MiB are
refused. One upstream request may run for `SERVING_TIMEOUT_S`.

A streaming request whose upstream has not replied within `SSE_KEEPALIVE_S`
gets its headers and an SSE comment line, `: keepalive`, every interval
until the first token. A slow prefill then does not look like an idle
connection to the client or anything between. The status is 200 from the
first comment, so an upstream failure after that arrives as an error event,
`{"error": {...}}`, followed by `[DONE]`. The OpenAI clients raise on it.

## Announcing endpoints

Model containers announce endpoints through agent registration. The
entrypoint exports these before `ray start`, and the agent reads them once:

```bash
export MENTAT_OPENAI_API=8000/v1      # the rank serving the API
export MENTAT_MCP_API=9000/mcp        # every rank
export MENTAT_MODEL_PROVIDER=vllm     # what serves the API
ray start --address=$RAY_ADDRESS
```

Each is optional. An agent without them registers as before.

`MENTAT_OPENAI_API` belongs on the rank running the API server, since only
that rank serves inference. Nothing enforces this. The agent announces
whatever is set, and the router uses the lexically first when several ranks
announce. `MENTAT_MCP_API` belongs on every rank, because every rank runs a
status server. `MENTAT_MODEL_PROVIDER` gives the engine behind
`MENTAT_OPENAI_API` and belongs on the same rank. `/status.json` reports it
per group, empty when the container did not report. "Counting tokens" needs
it.

### Port form and URL form

An endpoint value has one of these forms:

| Value | Meaning |
| --- | --- |
| `8000/v1`, or `http://0.0.0.0:8000/v1` | Every address this node announces |
| `http://10.0.0.1:8000/v1` | That address only |

The port form is preferred. An endpoint announced on one address is
reachable only from that link, so a router off that link cannot route to the
model. The port form leaves the host to the router, which resolves it
against every address the node announces. The same image then serves a
router on the LAN and one on the fabric, and a group stays routable when a
fabric cable drops.

The port form assumes the API server binds the wildcard address, which
`--host 0.0.0.0` does and vLLM does by default. The agent watches its own
`/proc/net/tcp` for the announced port. If the server bound a single
address, the agent logs `service_bind_narrow` and attaches the finding to
the announcement, so `/status.json` reports `bound to 10.0.0.1 only` beside
the failed probe. The finding is advisory. The probe alone admits an
endpoint.

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
| `GET /`, `/healthz`, `/status.json` | Route table, per-group health and endpoints, `uptime_s`, `verify` |
| `GET /stats.json` | Per-model engine and router counters, for the status page |

`GET /` with an `Accept` header that requests HTML, as from a browser,
returns the status page.

```bash
curl -s http://<node>:6381/v1/models          # what routes right now
curl -s http://<node>:6381/status.json | jq . # and why, per group
```

The status document has `uptime_s`. Some of the router's guards are
per-process, for example the once-only log of a rejected source, so a log
line that seems to repeat may be one line per process. A line stamped
earlier than now minus `uptime_s` came from an earlier process.

### The status page

`http://<node>:6381/` in a browser is a live table of what the router routes
and what each engine is doing with it. The page polls `/stats.json`.

The engine publishes queue depth, KV usage, token totals and latency
histograms on `/metrics`, so `running`, `waiting`, `kv`, the token counts
and the mean TTFT, queue and inter-token columns come from the engine
serving that model. The router adds `proxied`, the number of requests in
flight for that model.

A click on a model lists those requests one per row: body size, time
waiting with no first byte, time to first byte once it arrives, and bytes
returned. A long wait with no first byte while the engine reports nothing
running is an engine that accepted the request and stopped.

A group that fails its probe keeps its row, dimmed, with the reason in place
of the numbers.

### Counting tokens

`POST /v1/responses/input_tokens` reports how many prompt tokens an input
would cost, in OpenAI's shape:

```bash
curl -s http://<node>:6381/v1/responses/input_tokens \
  -d '{"model":"mymodel","input":"hello world"}' -H 'content-type: application/json'
# {"object":"response.input_tokens","input_tokens":14}
```

The router owns this route. vLLM does not serve that endpoint, and the path
lands on its `/v1/responses/{response_id}` pattern for a 405.

The serving engine counts the text. The router sends it to that group's
`/tokenize` as a chat request, so the chat template is included.
`instructions` becomes a leading system message and `tools` are passed
through, because the template renders both and the engine then prices them.
Text-only counts match the engine.

Media is estimated at flat rates: 4000 tokens per image and 40000 per video,
whatever the resolution or length. The true cost depends on tiling and the
model's patch size, which the router cannot know without fetching the media
and running the engine's preprocessor. An attachment that is neither, such
as a PDF, contributes only the text that accompanies it.

The route needs `MENTAT_MODEL_PROVIDER=vllm` on the container. A group
without an announced provider, or with one the router does not know, gets a
400 that quotes the group.

### The MCP merge

`/mcp` merges every group's management MCP into a single endpoint with one
flat namespace. Groups run the same status server, so a tool name that
appears in several groups is one tool with several places to run it. It is
listed once, with the first group's description and schema, and gains a
`__group` argument that selects the group to run it in. Groups are ordered
as in `/status.json`, so the first group stays put. The argument is required
when several groups offer the tool and optional when one does. The router
strips it before forwarding the call, so the container sees its own plain
arguments. `tools/list` replies are cached per group for `TOOLS_TTL_S`.

The merge skips the admission gate. A status server matters most while its
engine is loading or wedged, which is when the gate would exclude it.

The native tool `serve_status` reports the watched daemons, each group's
health and endpoints, and the model table. That name is reserved. A group
tool called `serve_status` is dropped from the merge, with a log line.

## Environment

An unset or empty variable uses its default. A `*_S` value must be a
positive number. Anything else uses the default.

- `SERVE_PORT` (default 6381)

HTTP port.

- `MENTAT_DAEMONS` (default `127.0.0.1:6380`)

Comma-separated daemon HTTP addresses to seed the watch set. Unset seeds the
local daemon. Set and empty seeds nothing, and UDP is then the only path in.
Compose cannot express empty, since `${VAR:-default}` reads it as unset.

- `MENTAT_ANNOUNCE_PORT` (default 6382)

UDP port to listen for daemon announcements on. `0` turns the listener off.

- `ALLOWED_SOURCES` (default `local`)

Comma-separated entries, in any mix of these forms:

- `local`: every network this box has an interface on, read from the
  interface list at each check. That covers each fabric and LAN the router
  is cabled to, loopback, and the docker bridge. The docker bridge is how a
  bridge-networked client keeps its `172.x` source and still gets in.
- a CIDR block, `10.100.0.0/22` or `fd00::/8`, or a bare address as a single
  host.
- a literal text prefix, `172.`, for a range that no single block covers.

An announcement's source address and every address it advertises must match
one entry before the router acts on it. The node's own identity address is
not checked, because nothing acts on it. A rejected source logs
`announce_source_not_allowed` once, with the entries in force.

The default admits a fabric only when this box is on it, the same wire test
candidate ranking uses. An entry for a fabric the router cannot reach costs
a `PROBE_TIMEOUT_S` wait every round before the fall-through.

- `DISCOVER_PEERS` (default `1`)

`1` adds the mesh peers of every watched daemon to the watch set. Any other
value disables it.

- `POLL_INTERVAL_S` (default 10)

Interval between `/status` polls of each watched daemon. A daemon view older
than three intervals is stale.

- `PROBE_INTERVAL_S` (default 5)

Interval between endpoint probes.

- `PROBE_TIMEOUT_S` (default 3)

Deadline for one probe.

- `PROBE_FRESH_S` (default: three probe intervals plus one timeout)

How long a probe result stays valid. Past it the group reads `endpoint probe
stale`. The default clears one round that walks every candidate address,
since each dead one costs a whole `PROBE_TIMEOUT_S`. Setting it on its own
makes groups flap in and out of the route table.

- `PROBE_PROMOTE_S` (default: six probe intervals)

How often a group serving from a lower-ranked address re-tries the addresses
ranked above it.

- `MODEL_WAIT_S` (default 60)

How long a request for a model that is not routable is held before it is
refused, and how long a refused upstream connection is retried. See "Request
handling".

- `SSE_KEEPALIVE_S` (default 10)

Interval between `: keepalive` comment lines on a streaming response while
the upstream has not yet replied. `0` turns them off, and the upstream's
status then passes through unchanged.

- `SERVING_TIMEOUT_S` (default 1800)

Deadline for one upstream request. A non-streaming response arrives when
generation ends, so the default is sized for generation. A lower value cuts
long generations before any hung request.

- `MCP_TIMEOUT_S` (default 180)

Deadline for one forwarded MCP call and for the tokenize call behind
`/v1/responses/input_tokens`. Some management tools block for their whole
sampling window, so it is longer than `PROBE_TIMEOUT_S`.

- `TOOLS_TTL_S` (default 60)

How long a group's `tools/list` reply is cached.

- `MODEL_TTL_S` (default 3600)

How long a group stays listed while nothing it announces can serve, and how
long a daemon outside the seed list is watched while it replies to nothing.
See "Retirement" and "Discovery". The default outlasts a reboot, a weight
reload or a fabric outage, so a model does not disappear mid-repair.

- `MENTAT_SECRET` (default: unset)

HMAC key for announcements. Must match the daemons'. A keyed router accepts
signed announcements only, so a half-applied rollout stops discovery until
the seed list finds the daemons. `verify` in `/status.json` reports whether
a key is in force.

- `MENTAT_SECRET_FILE` (default: unset)

Read the key from this file. It overrides `MENTAT_SECRET`. A file that
cannot be read, or reads empty, stops the process at boot with the reason.

- `MENTAT_UNIVERSE` (default `default`)

Cluster name. An announcement from another universe is dropped before its
signature is checked, without a log line.

## Limits

- The router must reach every daemon's HTTP port and at least one candidate
  address of every announced endpoint. With the port form that is any link
  it shares with the model's node. With the URL form it is the one address
  the announcement gives.
- Admission tracks the probe. A model that replies `/models` while still
  warming up is routable.
- Health is per group. A group with one wedged rank reads healthy while its
  API replies.
- The control port does not authenticate. Signing covers announcements only,
  and every claim in one is re-read over TCP before it affects routing.

## See also

[GUIDE.md](GUIDE.md), [PROTOCOL.md](PROTOCOL.md).
