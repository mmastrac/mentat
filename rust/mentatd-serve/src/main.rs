//! mentatd-serve: the cluster's serving front door, in the separate process
//! the mentat design reserved for it -- mentatd never touches inference
//! traffic, and this binary never touches cluster control.
//!
//! Two aggregations over what model containers announce at `ray start`
//! (MENTAT_OPENAI_API / MENTAT_MCP_API, held on AgentRegister):
//!   - one OpenAI-compatible endpoint that routes by model name to the
//!     announcing group's API, streaming passed through untouched;
//!   - one MCP endpoint merging the per-container management MCPs, tools
//!     prefixed `<group>__` so identical tool names cannot collide.
//!
//! Discovery: daemons are found by their UDP announcements (port 6382) and
//! by the seed list in MENTAT_DAEMONS (the local daemon by default), then
//! followed through the mesh's own membership. Each watched daemon is
//! polled for /status, with a /events WebSocket held open so any cluster
//! event triggers an immediate re-read. Routing is gated on health: a group
//! is admitted only while it has a running actor and its announced endpoint
//! replies to a probe.

mod mcp;
mod net;
mod proxy;
mod testnet;
mod tokens;
mod ui;
mod ws;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{json, Value};

use crate::net::{local_nets, on_local_net, Allow, Net};

use mentat_common::logfmt::log;
use mentat_common::secret;

pub type BoxedBody =
    http_body_util::combinators::BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;
pub type HttpClient = Client<HttpConnector, Full<Bytes>>;

/// A pooled client and one that never reuses a connection.
///
/// Keep-alive is worth having: the prober and the status poller hit the same
/// hosts every couple of seconds. It also introduces one failure the pool
/// cannot see. A server may close an idle connection at any time, and if it
/// does so between checkout and send, hyper reports a SendRequest failure
/// that reads exactly like a dead endpoint. uvicorn closes idle connections
/// by default, so a probe interval longer than its keep-alive timeout meets
/// this on every round: the endpoint serves perfectly and the probe fails
/// perfectly.
///
/// `fresh` exists to settle that. One retry over a new connection separates
/// a stale socket from an endpoint that is actually gone.
/// Why a request did not get a response.
pub struct SendError {
    pub msg: String,
    /// The connection was refused or unreachable. Nothing was sent.
    pub connect: bool,
    pub timeout: bool,
}

#[derive(Clone)]
pub struct HttpClients {
    pooled: HttpClient,
    fresh: HttpClient,
}

impl HttpClients {
    fn new() -> Self {
        HttpClients {
            pooled: Client::builder(TokioExecutor::new()).build_http(),
            fresh: Client::builder(TokioExecutor::new())
                .pool_max_idle_per_host(0)
                .build_http(),
        }
    }

    /// Send `req` once, over the pool.
    ///
    /// Anything that is not an idempotent GET goes through here. A retry
    /// would re-send a request the upstream may already be working on, and
    /// on this hardware a partially-done prefill is minutes of compute and
    /// the headroom that keeps the node alive.
    async fn send_once(
        &self,
        req: Request<Full<Bytes>>,
        t: Duration,
    ) -> Result<hyper::Response<hyper::body::Incoming>, String> {
        self.send_once_checked(req, t).await.map_err(|e| e.msg)
    }

    /// `send_once`, keeping whether the failure was a refused connection.
    /// A refused connection never reached the upstream, so it is the one
    /// failure a POST can be retried after.
    pub async fn send_once_checked(
        &self,
        req: Request<Full<Bytes>>,
        t: Duration,
    ) -> Result<hyper::Response<hyper::body::Incoming>, SendError> {
        match tokio::time::timeout(t, self.pooled.request(req)).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(SendError {
                msg: e.to_string(),
                connect: e.is_connect(),
                timeout: false,
            }),
            Err(_) => Err(SendError {
                msg: format!("timeout after {:.1}s", t.as_secs_f64()),
                connect: false,
                timeout: true,
            }),
        }
    }

    /// Send `req`, retrying once on a fresh connection if the first attempt
    /// established a connection and then failed on it.
    ///
    /// Only for idempotent GETs: the probe and the status poll, whose only
    /// consumer is the health gate. They are synthetic, short, and safe to
    /// repeat, which is what makes the retry obviously worth it there and
    /// not elsewhere. A proxied completion may legitimately run for minutes,
    /// and a second timeout window is a user-visible hang with nothing to
    /// show for it -- a client that fails fast can decide for itself, one
    /// inside a doubled timeout can do nothing until it expires.
    ///
    /// `is_connect` is the discriminator. A refused connection never got
    /// anywhere, so a retry would only fail the same way. Anything else got
    /// far enough to have been a live socket, which is the case worth a
    /// second look. The request is rebuilt rather than cloned because it was
    /// consumed.
    async fn send(
        &self,
        build: impl Fn() -> Result<Request<Full<Bytes>>, String>,
        t: Duration,
    ) -> Result<hyper::Response<hyper::body::Incoming>, String> {
        let first = tokio::time::timeout(t, self.pooled.request(build()?))
            .await
            .map_err(|_| format!("timeout after {:.1}s", t.as_secs_f64()))?;
        let e = match first {
            Ok(r) => return Ok(r),
            Err(e) if e.is_connect() => return Err(e.to_string()),
            Err(e) => e,
        };
        match tokio::time::timeout(t, self.fresh.request(build()?)).await {
            Err(_) => Err(format!("timeout after {:.1}s", t.as_secs_f64())),
            Ok(Ok(r)) => Ok(r),
            // Report the retry's error: it is the one with no stale
            // connection behind it.
            Ok(Err(retry)) => Err(format!("{retry} (first attempt: {e})")),
        }
    }
}

pub struct Config {
    pub daemons: Vec<String>,
    pub port: u16,
    pub announce_port: u16,
    pub poll_interval: Duration,
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub probe_fresh: Duration,
    pub probe_promote: Duration,
    pub serving_timeout: Duration,
    /// How long a request waits for its model to become routable, and
    /// retries a refused upstream connection, before it is refused.
    pub model_wait: Duration,
    /// Interval between SSE comment lines sent to a streaming client while
    /// the upstream has not yet replied. Zero turns them off.
    pub sse_keepalive: Duration,
    pub mcp_timeout: Duration,
    pub tools_ttl: Duration,
    /// How long a group stays listed while nothing it announces can serve.
    /// See `group_table`.
    pub model_ttl: Duration,
    pub allowed_sources: Allow,
    pub discover_peers: bool,
}

fn env_str(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// `env_secs` that also accepts `0`, for a knob that can be switched off.
fn env_secs_or_zero(name: &str, default: f64) -> Duration {
    let v = std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|v| *v >= 0.0)
        .unwrap_or(default);
    Duration::from_secs_f64(v)
}

fn env_secs(name: &str, default: f64) -> Duration {
    let v = std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(default);
    Duration::from_secs_f64(v)
}

impl Config {
    fn from_env() -> Config {
        let probe_interval = env_secs("PROBE_INTERVAL_S", 5.0);
        let probe_timeout = env_secs("PROBE_TIMEOUT_S", 3.0);
        Config {
            // Unset means seed with the local daemon. Set-but-empty means
            // no seeds at all, leaving UDP announcements as the only way in.
            daemons: std::env::var("MENTAT_DAEMONS")
                .unwrap_or_else(|_| "127.0.0.1:6380".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            port: env_str("SERVE_PORT", "6381").parse().unwrap_or(6381),
            // The daemons' announcement port; 0 turns the listener off.
            announce_port: env_str("MENTAT_ANNOUNCE_PORT", "6382")
                .parse()
                .unwrap_or(6382),
            poll_interval: env_secs("POLL_INTERVAL_S", 10.0),
            probe_interval,
            probe_timeout,
            // Wide enough that one full probe round (they run concurrently,
            // but a timing-out endpoint still holds its round open for
            // probe_timeout) cannot make a healthy group read as stale.
            probe_fresh: env_secs(
                "PROBE_FRESH_S",
                (probe_interval * 3 + probe_timeout).as_secs_f64(),
            ),
            // How often a group that fell through to a lower-ranked address
            // re-tries the address its node ranked higher. Rare on purpose:
            // the fall-through is already serving, so this only pays for
            // getting back onto the preferred link, and every attempt while
            // the preferred link is down is a wasted connect.
            probe_promote: env_secs("PROBE_PROMOTE_S", (probe_interval * 6).as_secs_f64()),
            // A non-streaming answer arrives only when generation ends, so
            // this is sized for generation rather than for a hung request.
            serving_timeout: env_secs("SERVING_TIMEOUT_S", 1800.0),
            // A model with cached weights restarts in well under a minute.
            model_wait: env_secs("MODEL_WAIT_S", 60.0),
            sse_keepalive: env_secs_or_zero("SSE_KEEPALIVE_S", 10.0),
            // latency_percentiles legitimately blocks for its whole window
            // (up to 120s), so the MCP forward allows more than that.
            mcp_timeout: env_secs("MCP_TIMEOUT_S", 180.0),
            tools_ttl: env_secs("TOOLS_TTL_S", 60.0),
            // An hour sits out a reboot, a weight reload or a fabric
            // outage without a model vanishing mid-repair.
            model_ttl: env_secs("MODEL_TTL_S", 3600.0),
            // Every wire this box is on, which is every wire a fabric,
            // a LAN client, or a bridge-networked one (OpenWebUI keeps its
            // 172.x source) can reach it over. An operator adding a range
            // this box is not attached to writes it out.
            allowed_sources: Allow::parse(&env_str("ALLOWED_SOURCES", "local")),
            discover_peers: env_str("DISCOVER_PEERS", "1") == "1",
        }
    }
}

pub struct DaemonView {
    pub status: Option<Value>,
    pub seen: Option<Instant>,
    pub error: Option<String>,
    /// The daemon's own id, once a poll has replied.
    pub node_id: Option<String>,
    /// Named in MENTAT_DAEMONS. Watched for the life of the process.
    pub seed: bool,
}

impl DaemonView {
    fn empty(seed: bool) -> DaemonView {
        DaemonView {
            status: None,
            seen: None,
            error: None,
            node_id: None,
            seed,
        }
    }
}

/// One node's watch: the address being polled, and the others the watch
/// falls through to when it stops replying. Every discovery source hands
/// over whichever address it saw, and watching each would poll a daemon
/// once per link.
pub struct NodeWatch {
    pub addr: String,
    pub alternates: BTreeSet<String>,
}

pub struct ProbeResult {
    pub ok: bool,
    /// The endpoint's own `/models` entries, verbatim. Holding whole objects
    /// keeps `max_model_len` and the rest correct as vLLM adds fields.
    pub models: Vec<Value>,
    pub seen: Instant,
    pub error: Option<String>,
    /// The candidate this group is currently routed to. Sticky: once an
    /// address replies, the router keeps using it rather than re-deciding
    /// every round, so a flapping preferred link cannot move live traffic
    /// between addresses on every probe.
    pub selected: Option<String>,
    /// When a higher-ranked candidate was last re-tried.
    pub promoted_at: Instant,
}

pub struct Shared {
    pub cfg: Config,
    /// When this process started.
    ///
    /// Several of the router's guards are in-memory and per-process: the
    /// announce log notes a source once, and the watch set records a node
    /// once. A restart re-arms them, so a line that looks like it repeats
    /// every round may be one line per process. Publishing uptime is what
    /// separates the two, and it does so for logs already written: a line
    /// stamped before now minus uptime came from an earlier process.
    pub started: Instant,
    /// Announcements must be signed. Published in `/status.json`.
    pub verify: std::sync::atomic::AtomicBool,
    pub client: HttpClients,
    /// One entry per daemon HTTP address being watched.
    pub daemons: Mutex<HashMap<String, DaemonView>>,
    pub watched: Mutex<HashSet<String>>,
    /// node_id -> the watch that owns it. See `NodeWatch`.
    pub nodes: Mutex<HashMap<String, NodeWatch>>,
    /// group -> latest endpoint probe. Present only for probe candidates
    /// (openai announced, actors running).
    pub probes: Mutex<HashMap<String, ProbeResult>>,
    /// "group url" -> cached tools/list report for the MCP merge.
    pub tools: Mutex<HashMap<String, (Instant, Vec<Value>)>>,
    /// Wakes the prober when a daemon view changes, so admission does not
    /// wait out a full probe interval after boot.
    pub refresh: tokio::sync::Notify,
    /// Requests being held right now, keyed by a per-process id. Rows
    /// leave on drop, so a client hangup clears its own.
    pub inflight: Mutex<BTreeMap<u64, ui::Inflight>>,
    pub next_req: AtomicU64,
    /// group -> its last `/metrics` scrape, shared across pollers.
    pub metrics: Mutex<HashMap<String, (Instant, String)>>,
    /// group -> its serving clock. Past `model_ttl` a group is retired: it
    /// leaves the table, the listing and the routes. Maintained by the
    /// prober, since that is what decides whether a group can serve.
    pub live: Mutex<HashMap<String, Liveness>>,
}

/// One announced service, resolved into the base URLs that could serve it.
#[derive(Clone)]
pub struct Endpoint {
    /// Base URLs to try, best first. A verbatim announcement has exactly
    /// one and it is never re-derived -- naming a host is the operator
    /// choosing which address to use. A port announcement has one per address
    /// of the announcing node that this router is allowed to reach.
    ///
    /// Empty means the announcement resolved to nothing, which is a
    /// different failure from not announcing at all and reads differently
    /// in `why_not`.
    pub candidates: Vec<String>,
    /// What was announced, for messages. Not parsed anywhere.
    pub announced: String,
    /// What the announcing agent noticed about the service afterwards,
    /// typically that its server bound one address rather than all of them.
    pub note: Option<String>,
}

impl Endpoint {
    /// The URL to use with no probe result to go on.
    pub fn best(&self) -> Option<&str> {
        self.candidates.first().map(String::as_str)
    }
}

/// One group as the freshest daemon views describe it.
#[derive(Clone)]
pub struct GroupEntry {
    pub group: String,
    pub daemon: String,
    pub agents_alive: usize,
    pub running: usize,
    pub openai: Option<Endpoint>,
    pub mcp: Option<Endpoint>,
    /// What serves `openai`, as the announcing agent reported it (`vllm`). Read
    /// from the agent whose endpoint won, since it describes the engine
    /// behind that endpoint. Empty when the container did not report.
    pub provider: String,
    /// Whether the group has actor rows, running or dead. An endpoint that
    /// outlives every rank still serves `/models`, and only rank state
    /// catches it. A group with no rows had nothing placed, so the probe
    /// is its whole test.
    pub placed: bool,
}

/// Ranked addresses per node, added to `out` from one daemon's snapshot.
///
/// Keyed by every address that identifies a node -- the name it calls
/// itself, the address a mesh link reached it on, and each address it
/// advertises -- because an agent registers under whichever of them its
/// container was configured with, and that is the only key available to
/// join an agent to its node.
///
/// Both its own daemon and every peer that knows it describe a node, and
/// those descriptions differ in completeness. The longest list wins, since a
/// peer that reports one address has not contradicted a daemon that reports
/// three.
fn collect_node_addrs(snap: &Value, out: &mut HashMap<String, Vec<String>>) {
    let mut record = |ids: Vec<&str>, addrs: Vec<String>| {
        if addrs.is_empty() {
            return;
        }
        for id in ids.into_iter().filter(|s| !s.is_empty()) {
            match out.get(id) {
                Some(prev) if prev.len() >= addrs.len() => {}
                _ => {
                    out.insert(id.to_string(), addrs.clone());
                }
            }
        }
    };
    let listed = |v: &Value| -> Vec<String> {
        v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str())
            .map(str::to_string)
            .collect()
    };

    let own = listed(&snap["addrs"]);
    let own_ip = snap["node_ip"].as_str().unwrap_or_default();
    let mut own_ids: Vec<&str> = vec![own_ip];
    own_ids.extend(own.iter().map(String::as_str));
    record(
        own_ids,
        if own.is_empty() {
            vec![own_ip.to_string()]
        } else {
            own.clone()
        },
    );

    for (_, p) in snap["peers"].as_object().into_iter().flatten() {
        let addrs = listed(&p["addrs"]);
        let ip = p["node_ip"].as_str().unwrap_or_default();
        let mut ids: Vec<&str> = vec![ip, p["link_ip"].as_str().unwrap_or_default()];
        ids.extend(addrs.iter().map(String::as_str));
        record(
            ids,
            if addrs.is_empty() {
                vec![ip.to_string()]
            } else {
                addrs.clone()
            },
        );
    }
}

/// One agent's announcement of `svc`, resolved into base URLs to try.
///
/// A verbatim URL resolves to itself and is left ungated: the operator named
/// a host, and a router that second-guessed it would drop an endpoint that
/// announced fine before. A port announcement is resolved here instead,
/// against the announcing node's own ranked addresses, and ALLOWED_SOURCES
/// gates every address that produces -- those are addresses this process
/// derived and will connect to, which is what that list is for.
///
/// Within the node's ranking, an address on one of this box's own subnets
/// comes first. The node ranks its links by speed because only it can. The
/// router ranks by whether it shares the wire, because only it can. Serving
/// Serving HTTP costs almost nothing to send, so reachable beats fast.
fn endpoint_of(
    agent: &Value,
    svc: &str,
    nodes: &HashMap<String, Vec<String>>,
    allowed: &Allow,
    local: &[Net],
) -> Option<Endpoint> {
    let entry = &agent["services"][svc];
    let note = entry["note"]
        .as_str()
        .filter(|n| !n.is_empty())
        .map(str::to_string);
    let port = entry["port"].as_u64()?;
    let path = entry["path"].as_str().unwrap_or_default();
    // A host the operator named is used as written and passes no check:
    // naming one is the operator choosing which address to use.
    if let Some(host) = entry["host"].as_str().filter(|h| !h.is_empty()) {
        let url = format!("http://{host}:{port}{path}");
        return Some(Endpoint {
            candidates: vec![url.clone()],
            announced: url,
            note,
        });
    }
    let node_ip = agent["node_ip"].as_str().unwrap_or_default();
    let mut hosts: Vec<String> = nodes
        .get(node_ip)
        .cloned()
        .unwrap_or_else(|| vec![node_ip.to_string()]);
    hosts.retain(|h| !h.is_empty() && allowed.permits(h, local));
    // A node may list an address twice across the views it was merged from,
    // and a duplicate candidate would be probed twice and reported twice.
    let mut seen = HashSet::new();
    hosts.retain(|h| seen.insert(h.clone()));
    // Stable, so the node's own order survives inside each half.
    hosts.sort_by_key(|h| !on_local_net(h, local));
    Some(Endpoint {
        candidates: hosts
            .iter()
            .map(|h| format!("http://{h}:{port}{path}"))
            .collect(),
        announced: format!("port {port}{path} on node {node_ip}"),
        note,
    })
}

/// The `openai` endpoint a group routes to, and the provider of the agent
/// that announced it.
///
/// Only the rank running the API server is meant to announce `openai`, and
/// nothing enforces it. Several announcements resolve by best candidate, for
/// determinism. The provider follows that same agent, since it describes the
/// engine behind that endpoint.
fn best_openai(announced: Vec<(Endpoint, String)>) -> (Option<Endpoint>, String) {
    match announced
        .into_iter()
        .min_by(|x, y| x.0.best().cmp(&y.0.best()))
    {
        Some((e, provider)) => (Some(e), provider),
        None => (None, String::new()),
    }
}

/// Every group any watched daemon still describes, retired ones included.
///
/// Only the prober reads this. It probes retired groups too, which is how
/// one that comes back un-retires. Every other caller wants `group_table`.
///
/// A group's agents all register with one daemon (the rendezvous rule), so
/// overlap only happens around a stale view -- resolved toward the daemon
/// with more running actors.
pub fn announced_groups(shared: &Shared) -> BTreeMap<String, GroupEntry> {
    let stale = shared.cfg.poll_interval * 3;
    let local = local_nets();
    let daemons = shared.daemons.lock().unwrap();
    // Every node any watched daemon can describe, so an agent's node_ip
    // resolves to that node's own ranked addresses.
    let mut nodes: HashMap<String, Vec<String>> = HashMap::new();
    for view in daemons.values() {
        if let Some(snap) = view.status.as_ref() {
            collect_node_addrs(snap, &mut nodes);
        }
    }
    let mut out: BTreeMap<String, GroupEntry> = BTreeMap::new();
    for (addr, view) in daemons.iter() {
        let fresh = view.seen.map(|s| s.elapsed() <= stale).unwrap_or(false);
        let Some(snap) = view.status.as_ref().filter(|_| fresh) else {
            continue;
        };
        for (name, g) in snap["groups"].as_object().into_iter().flatten() {
            // Every collection uses the id as its key, so a row is a value
            // here.
            let agents: Vec<&Value> = g["agents"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(_, a)| a)
                .filter(|a| a["alive"].as_bool().unwrap_or(false))
                .collect();
            let actors = g["actors"].as_object();
            let placed = actors.is_some_and(|a| !a.is_empty());
            let running = actors
                .into_iter()
                .flatten()
                .filter(|(_, a)| a["state"].as_str() == Some("running"))
                .count();
            let resolve = |a: &Value, svc: &str| {
                endpoint_of(a, svc, &nodes, &shared.cfg.allowed_sources, &local)
            };
            // Only the rank running the API server announces "openai". If
            // several ever do, the one whose best candidate sorts first
            // wins, for determinism.
            let (openai, provider) = best_openai(
                agents
                    .iter()
                    .filter_map(|a| {
                        resolve(a, "openai").map(|e| {
                            let p = a["services"]["openai"]["provider"]
                                .as_str()
                                .unwrap_or_default();
                            (e, p.to_string())
                        })
                    })
                    .collect(),
            );
            // Every rank announces "mcp" (the status server runs on all of
            // them). Prefer the API node's -- it is the one with throughput
            // to report -- then the same order.
            let mcp = agents
                .iter()
                .filter_map(|a| resolve(a, "mcp").map(|m| (a["services"]["openai"].is_null(), m)))
                .min_by(|x, y| (x.0, x.1.best()).cmp(&(y.0, y.1.best())))
                .map(|(_, m)| m);
            let entry = GroupEntry {
                group: name.clone(),
                daemon: addr.clone(),
                agents_alive: agents.len(),
                running,
                openai,
                mcp,
                provider,
                placed,
            };
            let replace = match out.get(name) {
                None => true,
                Some(prev) => {
                    (entry.running, entry.agents_alive) > (prev.running, prev.agents_alive)
                        || ((entry.running, entry.agents_alive)
                            == (prev.running, prev.agents_alive)
                            && entry.daemon < prev.daemon)
                }
            };
            if replace {
                out.insert(name.clone(), entry);
            }
        }
    }
    out
}

/// One group's serving clock, as the prober keeps it.
pub struct Liveness {
    /// The last round the group could serve, or the round it was first seen
    /// if it never has.
    ok_at: Instant,
    /// The retirement has been logged.
    told: bool,
}

impl Liveness {
    fn new(now: Instant) -> Liveness {
        Liveness {
            ok_at: now,
            told: false,
        }
    }

    fn retired(&self, ttl: Duration) -> bool {
        self.ok_at.elapsed() > ttl
    }

    /// Fold in one probe round. True on the round that retires the group,
    /// which is the one round worth a log line.
    fn round(&mut self, now: Instant, ok: bool, ttl: Duration) -> bool {
        if ok {
            *self = Liveness::new(now);
            return false;
        }
        if self.told || !self.retired(ttl) {
            return false;
        }
        self.told = true;
        true
    }
}

/// The groups a caller may see: announced, minus the ones retired for being
/// unable to serve for `model_ttl`.
///
/// A daemon never declares a group over. It keeps the agent and actor rows of
/// a container that is long gone, so a model dropped from an operator's
/// compose file would otherwise sit in `/v1/models` until that daemon
/// restarted, advertised and unroutable. The clock is the router's own and
/// starts when a group is first seen, so one that never comes up is retired
/// on the same terms as one that stopped.
pub fn group_table(shared: &Shared) -> BTreeMap<String, GroupEntry> {
    let mut out = announced_groups(shared);
    let live = shared.live.lock().unwrap();
    out.retain(|name, _| match live.get(name) {
        Some(l) => !l.retired(shared.cfg.model_ttl),
        // Announced since the prober last ran. Keep it: the group has not
        // had a chance to be judged yet, and the next round gives it one.
        None => true,
    });
    out
}

/// Note which announced groups can serve right now, and retire the ones that
/// have not been able to for `model_ttl`. Called once per probe round, after
/// the probes land, so `health_of` reads this round's results.
fn age_groups(shared: &Shared, announced: &BTreeMap<String, GroupEntry>) {
    let now = Instant::now();
    let mut retired: Vec<(String, String)> = Vec::new();
    {
        let mut live = shared.live.lock().unwrap();
        // A group no daemon mentions any more is gone rather than retired,
        // and keeping its clock would retire it the moment it came back.
        live.retain(|name, _| announced.contains_key(name));
        for (name, e) in announced {
            let l = live
                .entry(name.clone())
                .or_insert_with(|| Liveness::new(now));
            match health_of(shared, e) {
                Ok(_) => {
                    l.round(now, true, shared.cfg.model_ttl);
                }
                Err(why) => {
                    if l.round(now, false, shared.cfg.model_ttl) {
                        retired.push((name.clone(), why));
                    }
                }
            }
        }
    }
    // Once per retirement. A group that comes back and goes again reports it
    // again, because the clock reset when it came back.
    for (group, why) in retired {
        log(
            "group_retired",
            &[
                ("group", group),
                ("after_s", shared.cfg.model_ttl.as_secs().to_string()),
                ("why", why),
            ],
        );
    }
}

/// Ok(model names) when the group may serve traffic; Err(why) otherwise.
pub fn health_of(shared: &Shared, e: &GroupEntry) -> Result<Vec<Value>, String> {
    let Some(ep) = e.openai.as_ref() else {
        return Err("no announced OpenAI endpoint".into());
    };
    if ep.candidates.is_empty() {
        return Err(format!(
            "announced {}, and no address of that node passes ALLOWED_SOURCES",
            ep.announced
        ));
    }
    // An endpoint that outlives every rank still serves /models. A group
    // that never had actors does not have rank state to consult.
    if e.placed && e.running == 0 {
        return Err("no running actors".into());
    }
    let probes = shared.probes.lock().unwrap();
    match probes.get(&e.group) {
        None => Err("not probed yet".into()),
        // The agent's own finding is appended rather than substituted: it
        // explains a probe failure without being the gate. "connection
        // refused" plus "bound to 10.100.0.1 only" is one diagnosis. Either
        // alone is a guess.
        Some(p) if !p.ok => Err(format!(
            "endpoint probe failed: {}{}",
            p.error.as_deref().unwrap_or("unknown"),
            match &ep.note {
                Some(n) => format!(" (agent reports: {n})"),
                None => String::new(),
            }
        )),
        Some(p) if p.seen.elapsed() > shared.cfg.probe_fresh => Err("endpoint probe stale".into()),
        Some(p) => Ok(p.models.clone()),
    }
}

/// The base URL a healthy group's traffic goes to: whichever candidate the
/// prober settled on, falling back to the best-ranked one.
pub fn endpoint_url(shared: &Shared, e: &GroupEntry) -> Option<String> {
    let ep = e.openai.as_ref()?;
    shared
        .probes
        .lock()
        .unwrap()
        .get(&e.group)
        .and_then(|p| p.selected.clone())
        .or_else(|| ep.best().map(str::to_string))
}

/// The `id` of each model entry, for the callers that only name them.
pub fn model_ids(models: &[Value]) -> Vec<String> {
    models
        .iter()
        .filter_map(|m| m["id"].as_str().map(String::from))
        .collect()
}

/// model name -> (group, announced base URL), healthy groups only. Names come
/// from probing the endpoint's /models, so SERVED_NAME does not need announcing.
///
/// Names and routes only. This runs on every proxied request, where cloning
/// each engine's full `/models` object would be paid per request.
/// `model_objects` is the listing's heavier answer.
pub fn model_table(shared: &Shared) -> BTreeMap<String, (String, String)> {
    let mut out = BTreeMap::new();
    for e in group_table(shared).values() {
        if let Ok(models) = health_of(shared, e) {
            let url = endpoint_url(shared, e).unwrap_or_default();
            for m in model_ids(&models) {
                out.entry(m)
                    .or_insert_with(|| (e.group.clone(), url.clone()));
            }
        }
    }
    out
}

/// What `/v1/models` replies: every healthy group's `/models` entries as the
/// engine wrote them, so a client sees the same `max_model_len` and `root` it
/// would reading the engine direct. `owned_by` becomes the serving group,
/// which through a router is the useful owner and what `/status.json`
/// correlates on.
///
/// First writer wins on a duplicate name, matching `model_table`, so the
/// listing cannot advertise an entry that routes elsewhere.
pub fn model_objects(shared: &Shared) -> Vec<Value> {
    let mut out: BTreeMap<String, Value> = BTreeMap::new();
    for e in group_table(shared).values() {
        if let Ok(models) = health_of(shared, e) {
            for m in models {
                let Some(id) = m["id"].as_str().map(String::from) else {
                    continue;
                };
                out.entry(id).or_insert_with(|| listed_model(&m, &e.group));
            }
        }
    }
    out.into_values().collect()
}

/// One `/models` entry as the router republishes it: the engine's own object
/// with `owned_by` set to the serving group.
fn listed_model(m: &Value, group: &str) -> Value {
    let mut m = m.clone();
    if let Some(obj) = m.as_object_mut() {
        obj.insert("owned_by".into(), json!(group));
    }
    m
}

/// Groups that announced an endpoint but are not routable, with the reason.
pub fn not_ready(shared: &Shared) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for e in group_table(shared).values() {
        if let Err(why) = health_of(shared, e) {
            out.insert(e.group.clone(), why);
        }
    }
    out
}

pub fn status_view(shared: &Shared) -> Value {
    let daemons: BTreeMap<String, Value> = shared
        .daemons
        .lock()
        .unwrap()
        .iter()
        .map(|(addr, v)| {
            (
                addr.clone(),
                json!({
                    "connected": v.status.is_some(),
                    "age_s": v.seen.map(|s| s.elapsed().as_secs()),
                    "error": v.error,
                    "node_id": v.node_id,
                    "seed": v.seed,
                    "alternates": v.node_id.as_ref().and_then(|id| {
                        shared.nodes.lock().unwrap().get(id).map(|w| w.alternates.clone())
                    }),
                }),
            )
        })
        .collect();
    let groups: BTreeMap<String, Value> = group_table(shared)
        .values()
        .map(|e| {
            let health = health_of(shared, e);
            (
                e.group.clone(),
                json!({
                    "daemon": e.daemon,
                    "agents_alive": e.agents_alive,
                    "actors_running": e.running,
                    // Which address is in use, and which were available to
                    // fall through to. A group serving off its second
                    // candidate is the visible symptom of a link being down.
                    "openai": endpoint_url(shared, e),
                    "openai_candidates": e.openai.as_ref().map(|x| x.candidates.clone()),
                    "openai_note": e.openai.as_ref().and_then(|x| x.note.clone()),
                    "provider": e.provider,
                    "mcp": e.mcp.as_ref().and_then(|x| x.best()),
                    "healthy": health.is_ok(),
                    // Names only. /v1/models holds the whole entries, and
                    // repeating them here would crowd out the health fields.
                    "models": health.as_ref().ok().map(|m| model_ids(m)),
                    "why_not": health.as_ref().err(),
                }),
            )
        })
        .collect();
    let models: BTreeMap<String, Value> = model_table(shared)
        .into_iter()
        .map(|(m, (g, url))| (m, json!({ "group": g, "url": url })))
        .collect();
    json!({
        "uptime_s": shared.started.elapsed().as_secs(),
        "verify": shared.verify.load(std::sync::atomic::Ordering::Relaxed),
        "daemons": daemons,
        "groups": groups,
        "models": models,
    })
}

// ---------------------------------------------------------------------------
// Daemon watchers and the endpoint prober
// ---------------------------------------------------------------------------

/// Watch a daemon at `addr`, with `others` to fall back to before the
/// first answer, best first.
pub fn ensure_watched(shared: &Arc<Shared>, addr: String, others: Vec<String>) {
    // An address already remembered as another way to reach a watched node
    // does not need a watch of its own.
    if shared
        .nodes
        .lock()
        .unwrap()
        .values()
        .any(|w| w.alternates.contains(&addr))
    {
        return;
    }
    {
        let mut w = shared.watched.lock().unwrap();
        if !w.insert(addr.clone()) {
            return;
        }
    }
    log("daemon_watch", &[("daemon", addr.clone())]);
    let shared = shared.clone();
    tokio::spawn(async move { watch_daemon(shared, addr, others).await });
}

/// Hold one daemon fresh: poll /status, and keep a /events WebSocket open so
/// a cluster event re-reads at once.
///
/// The first answer names the node. If a fresh watch already owns it, this
/// address becomes that watch's alternate and this task ends. When the
/// polled address stops replying the watch moves to an alternate that
/// does, and an unseeded address that replies nothing for `model_ttl` while
/// no live daemon lists it is forgotten.
async fn watch_daemon(shared: Arc<Shared>, mut addr: String, others: Vec<String>) {
    let seed = shared.cfg.daemons.contains(&addr);
    let mut node: Option<String> = None;
    let mut failing_since: Option<Instant> = None;
    // Addresses to try before the node has replied on any, since the
    // best-ranked one may be on a link this box cannot use.
    let mut untried: std::collections::VecDeque<String> =
        others.into_iter().filter(|o| *o != addr).collect();
    shared
        .daemons
        .lock()
        .unwrap()
        .entry(addr.clone())
        .or_insert_with(|| DaemonView::empty(seed));
    loop {
        match poll_status(&shared, &addr).await {
            Some(id) => {
                failing_since = None;
                if node.as_deref() != Some(id.as_str()) {
                    if !claim_node(&shared, &id, &addr) {
                        log(
                            "daemon_watch_merged",
                            &[("daemon", addr.clone()), ("node", id.clone())],
                        );
                        drop_watch(&shared, &addr);
                        return;
                    }
                    if let Some(w) = shared.nodes.lock().unwrap().get_mut(&id) {
                        w.alternates.extend(untried.drain(..));
                    }
                    node = Some(id);
                }
                match ws::EventStream::connect(&testnet::mapped(&addr)).await {
                    Ok(mut es) => {
                        // The stream's own `seq`, so a gap is visible. A
                        // missed event leaves a view that is wrong, and
                        // nothing later reports it.
                        let mut last_seq: Option<u64> = None;
                        let mut last_full = Instant::now();
                        loop {
                            // Counters move without events, so the snapshot
                            // is re-read on the poll interval whatever the
                            // stream is doing. That read is also what
                            // discovers peers.
                            if last_full.elapsed() >= shared.cfg.poll_interval {
                                if poll_status(&shared, &addr).await.is_none() {
                                    break;
                                }
                                last_full = Instant::now();
                                last_seq = anchored_seq(&shared, &addr);
                            }
                            let frame = match es.next(shared.cfg.poll_interval).await {
                                Ok(Some(f)) => f,
                                Ok(None) => continue,
                                Err(e) => {
                                    log(
                                        "daemon_events_lost",
                                        &[("daemon", addr.clone()), ("error", e.to_string())],
                                    );
                                    break;
                                }
                            };
                            let Ok(ev) = serde_json::from_str::<Value>(&frame) else {
                                continue;
                            };
                            match apply_frame(&shared, &addr, node.as_deref(), &ev, &mut last_seq) {
                                Applied::Yes => shared.refresh.notify_one(),
                                Applied::Skip => {}
                                Applied::Resync => {
                                    if poll_status(&shared, &addr).await.is_none() {
                                        break;
                                    }
                                    last_full = Instant::now();
                                    last_seq = anchored_seq(&shared, &addr);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let mut d = shared.daemons.lock().unwrap();
                        let v = d
                            .entry(addr.clone())
                            .or_insert_with(|| DaemonView::empty(seed));
                        v.error = Some(format!("/events: {e}"));
                    }
                }
            }
            None => {
                let since = *failing_since.get_or_insert_with(Instant::now);
                if node.is_none() {
                    if let Some(next) = untried.pop_front() {
                        log(
                            "daemon_watch_moved",
                            &[
                                ("node", String::new()),
                                ("from", addr.clone()),
                                ("to", next.clone()),
                            ],
                        );
                        drop_watch(&shared, &addr);
                        shared.watched.lock().unwrap().insert(next.clone());
                        shared
                            .daemons
                            .lock()
                            .unwrap()
                            .insert(next.clone(), DaemonView::empty(seed));
                        untried.push_back(addr);
                        addr = next;
                        continue;
                    }
                }
                if let Some(id) = &node {
                    if let Some(next) = try_alternates(&shared, id, &addr).await {
                        log(
                            "daemon_watch_moved",
                            &[
                                ("node", id.clone()),
                                ("from", addr.clone()),
                                ("to", next.clone()),
                            ],
                        );
                        move_watch(&shared, id, &addr, &next);
                        addr = next;
                        failing_since = None;
                        continue;
                    }
                }
                if !seed
                    && since.elapsed() > shared.cfg.model_ttl
                    && !published_alive(&shared, node.as_deref(), &addr)
                {
                    log(
                        "daemon_forgotten",
                        &[
                            ("daemon", addr.clone()),
                            ("node", node.clone().unwrap_or_default()),
                            ("after_s", shared.cfg.model_ttl.as_secs().to_string()),
                        ],
                    );
                    drop_watch(&shared, &addr);
                    if let Some(id) = &node {
                        shared.nodes.lock().unwrap().remove(id);
                    }
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Claim a node for this watch. False when another fresh watch holds it, in
/// which case this address becomes one of its alternates.
fn claim_node(shared: &Shared, id: &str, addr: &str) -> bool {
    let mut nodes = shared.nodes.lock().unwrap();
    match nodes.get_mut(id) {
        None => {
            nodes.insert(
                id.to_string(),
                NodeWatch {
                    addr: addr.to_string(),
                    alternates: BTreeSet::new(),
                },
            );
            true
        }
        Some(w) if w.addr == addr => true,
        Some(w) => {
            let stale = shared.cfg.poll_interval * 3;
            let fresh = shared
                .daemons
                .lock()
                .unwrap()
                .get(&w.addr)
                .and_then(|v| v.seen)
                .is_some_and(|s| s.elapsed() <= stale);
            if fresh {
                w.alternates.insert(addr.to_string());
                false
            } else {
                // The holder is stale and this address replies, so this
                // watch replaces it.
                let old = std::mem::replace(&mut w.addr, addr.to_string());
                w.alternates.remove(addr);
                w.alternates.insert(old.clone());
                drop(nodes);
                drop_watch(shared, &old);
                true
            }
        }
    }
}

/// Poll each alternate of a node once. The first that replies as that node
/// is the new address.
async fn try_alternates(shared: &Arc<Shared>, id: &str, current: &str) -> Option<String> {
    let mut alts: Vec<String> = shared
        .nodes
        .lock()
        .unwrap()
        .get(id)
        .map(|w| {
            w.alternates
                .iter()
                .filter(|a| *a != current)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    // Loopback is the box the router runs on rather than the peer, and
    // is right only where router and daemon share one. The set is ordered
    // lexicographically, which puts `127.` ahead of a real address, so the
    // preference `peer_addresses` established is restored here.
    alts.sort_by_key(|a| a.starts_with("127.") || a.starts_with("::1"));
    for alt in alts {
        if poll_status(shared, &alt).await.as_deref() == Some(id) {
            return Some(alt);
        }
    }
    None
}

fn move_watch(shared: &Shared, id: &str, from: &str, to: &str) {
    if let Some(w) = shared.nodes.lock().unwrap().get_mut(id) {
        w.addr = to.to_string();
        w.alternates.remove(to);
        w.alternates.insert(from.to_string());
    }
    let mut d = shared.daemons.lock().unwrap();
    d.remove(from);
    let mut w = shared.watched.lock().unwrap();
    w.remove(from);
    w.insert(to.to_string());
}

fn drop_watch(shared: &Shared, addr: &str) {
    shared.daemons.lock().unwrap().remove(addr);
    shared.watched.lock().unwrap().remove(addr);
}

/// Whether any fresh daemon view lists this node, or this address, as a
/// live peer. Such a daemon is unreachable from here rather than gone.
fn published_alive(shared: &Shared, node: Option<&str>, addr: &str) -> bool {
    let stale = shared.cfg.poll_interval * 3;
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let daemons = shared.daemons.lock().unwrap();
    daemons.values().any(|v| {
        let fresh = v.seen.is_some_and(|s| s.elapsed() <= stale);
        let Some(snap) = v.status.as_ref().filter(|_| fresh) else {
            return false;
        };
        snap["peers"]
            .as_object()
            .into_iter()
            .flatten()
            .any(|(id, p)| {
                p["alive"].as_bool().unwrap_or(false)
                    && (Some(id.as_str()) == node
                        || p["node_ip"].as_str() == Some(host)
                        || p["link_ip"].as_str() == Some(host)
                        || p["addrs"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|a| a.as_str() == Some(host)))
            })
    })
}

/// Listen for the daemons' UDP announcements. An announcement is a hint: it
/// only adds a watch candidate, and everything the daemon claims is then
/// read over TCP and probed like any other. Every datagram is signed, so a
/// listener with no key exits at boot rather than watching an empty
/// cluster. Both the datagram source and the claimed address must pass
/// ALLOWED_SOURCES, as the HTTP side does.
async fn udp_listener(shared: Arc<Shared>) {
    let port = shared.cfg.announce_port;
    if port == 0 {
        return;
    }
    // Shared, because a box that hosts a model and the router runs both with
    // host networking and both want these broadcasts.
    let sock = match mentat_common::udp::bind_shared(port).and_then(|s| {
        s.set_nonblocking(true)?;
        tokio::net::UdpSocket::from_std(s)
    }) {
        Ok(s) => s,
        Err(e) => {
            log(
                "announce_listen_failed",
                &[("port", port.to_string()), ("error", e.to_string())],
            );
            return;
        }
    };
    // Same stop as the daemon's: a router that cannot read the key it was
    // given verifies nothing and drops every signed announcement, so it
    // watches an empty cluster and reports only that each datagram failed.
    let key = match secret::load() {
        Ok(Some(k)) => k,
        Ok(None) => {
            // Every announcement is signed, so a listener with no key can
            // verify nothing and would watch an empty cluster while reporting
            // only that each datagram failed.
            log(
                "announce_listen_off",
                &[("why", "no MENTAT_SECRET or MENTAT_SECRET_FILE".to_string())],
            );
            eprintln!("mentatd-serve: discovery needs MENTAT_SECRET or MENTAT_SECRET_FILE");
            std::process::exit(1);
        }
        Err(why) => {
            log("announce_secret_unusable", &[("error", why.clone())]);
            eprintln!("mentatd-serve: {why}");
            std::process::exit(1);
        }
    };
    log(
        "announce_listen",
        &[
            ("port", port.to_string()),
            ("verify", "required".to_string()),
        ],
    );
    shared
        .verify
        .store(true, std::sync::atomic::Ordering::Relaxed);
    // node_id -> (boot_id, last accepted seq). Bounds replay within a boot.
    let mut seen: HashMap<String, (String, u64)> = HashMap::new();
    // Sources already complained about, so a 5s broadcast cannot flood the
    // log with the same misconfiguration.
    let mut warned: HashSet<String> = HashSet::new();
    // Sources whose advertised address has already been reported as
    // unroutable-looking, so the note lands once rather than every round.
    let mut noted: HashSet<String> = HashSet::new();
    let universe = secret::universe();
    // One Ethernet frame, which is the sender's own cap. Anything longer
    // is not an announcement this build wrote.
    let mut buf = [0u8; 1400];
    loop {
        let Ok((n, src)) = sock.recv_from(&mut buf).await else {
            continue;
        };
        // A datagram that filled the buffer was cut to fit. Verifying the
        // fragment would report a bad signature for an oversized sender.
        if n == buf.len() {
            if warned.insert(src.ip().to_string()) {
                log(
                    "announce_oversize",
                    &[("src", src.ip().to_string()), ("cap", n.to_string())],
                );
            }
            continue;
        }
        // Another cluster on this broadcast domain is not a misconfiguration,
        // so it is dropped before the key is consulted and before anything is
        // logged. A datagram with no universe reads as "default", the same
        // value a router uses when MENTAT_UNIVERSE is unset.
        if secret::claimed_universe(&buf[..n]) != universe {
            continue;
        }
        let Some(v) = secret::verify(&buf[..n], &key) else {
            // A wrong key and a stripped signature look the same from here,
            // and both mean the sender cannot be trusted.
            if warned.insert(src.ip().to_string()) {
                log(
                    "announce_rejected",
                    &[
                        ("src", src.ip().to_string()),
                        ("why", "bad signature or unsigned".to_string()),
                    ],
                );
            }
            continue;
        };
        let offered = v["proto"].as_str().unwrap_or_default();
        if !mentat_common::proto::major_matches(offered) {
            if warned.insert(format!("proto:{}", src.ip())) {
                log(
                    "announce_proto_mismatch",
                    &[
                        ("src", src.ip().to_string()),
                        ("offered", offered.to_string()),
                        ("here", mentat_common::proto::PROTO.to_string()),
                    ],
                );
            }
            continue;
        }
        let Some(t) = v["t"].as_u64() else { continue };
        if !secret::fresh(t as f64, secret::now_s()) {
            // The signature passed, so this is one of ours with a drifted
            // clock. Dropping it silently leaves an empty cluster with no
            // stated cause.
            if warned.insert(format!("t:{}", src.ip())) {
                log(
                    "announce_stale",
                    &[
                        ("src", src.ip().to_string()),
                        ("t", t.to_string()),
                        ("now", (secret::now_s() as u64).to_string()),
                        ("window_s", secret::CLOCK_SKEW_S.to_string()),
                    ],
                );
            }
            continue;
        }
        let node = v["node_id"].as_str().unwrap_or_default().to_string();
        let boot = v["boot_id"].as_str().unwrap_or_default().to_string();
        let seq = v["seq"].as_u64().unwrap_or(0);
        if node.is_empty() || boot.is_empty() {
            continue;
        }
        // A restart resets seq, which the new boot_id distinguishes from a
        // replay. One announcement per interface repeats a seq, and dropping
        // the repeat costs nothing: the address it holds is the same one.
        match seen.get(&node) {
            Some((b, last)) if *b == boot && seq <= *last => continue,
            _ => seen.insert(node, (boot, seq)),
        };
        let Some(http) = v["http"].as_str() else {
            continue;
        };
        let Some((http_ip, http_port)) = http.rsplit_once(':') else {
            continue;
        };
        if http_port.parse::<u16>().map(|p| p == 0).unwrap_or(true) {
            continue;
        }
        // ALLOWED_SOURCES gates what gets acted on, and nothing else. The
        // source address can become the address watched, so it is checked
        // here, and so is any advertised address before it is chosen,
        // further down. The advertised address itself is not, because nothing acts
        // on it any more -- gating a field the router only reads would fail
        // discovery closed over a subnet the operator never thinks about, and
        // report nothing about why.
        let src_ip = src.ip().to_string();
        let local = local_nets();
        if !shared.cfg.allowed_sources.permits(&src_ip, &local) {
            // Named once per source. A dropped announcement is otherwise an
            // empty cluster with no stated cause.
            if warned.insert(src_ip.clone()) {
                log(
                    "announce_source_not_allowed",
                    &[
                        ("src", src_ip.clone()),
                        ("allowed_sources", shared.cfg.allowed_sources.to_string()),
                    ],
                );
            }
            continue;
        }
        let node = v["node_id"].as_str().unwrap_or_default().to_string();
        // The node ranks its own addresses, most preferred first, because
        // only it knows which link is the fast one. Use the best it offers
        // that lands on a subnet we are attached to. Failing that, the
        // source address, which at least held this packet here.
        let ranked: Vec<String> = v["addrs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str())
            .map(str::to_string)
            .collect();
        let pick = announce_address(&ranked, &src_ip, &shared.cfg.allowed_sources, &local);
        if pick != src_ip && noted.insert(src_ip.clone()) {
            log(
                "announce_preferred_addr",
                &[
                    ("src", src_ip.clone()),
                    ("advertised", http.to_string()),
                    ("watching", format!("{pick}:{http_port}")),
                ],
            );
        } else if http_ip != src_ip && noted.insert(src_ip.clone()) {
            log(
                "announce_addr_mismatch",
                &[
                    ("src", src_ip.clone()),
                    ("advertised", http.to_string()),
                    ("watching", format!("{pick}:{http_port}")),
                ],
            );
        }
        // One watch per node. A node with two links broadcasts on both, and
        // the datagrams differ only in source address. A watched node
        // learns the other address as an alternate.
        let target = format!("{pick}:{http_port}");
        let watched = !node.is_empty()
            && shared
                .nodes
                .lock()
                .unwrap()
                .get_mut(&node)
                .map(|w| {
                    if w.addr != target {
                        w.alternates.insert(target.clone());
                    }
                    true
                })
                .unwrap_or(false);
        if !watched {
            let others: Vec<String> = ranked
                .iter()
                .filter(|a| shared.cfg.allowed_sources.permits(a, &local))
                .map(|a| format!("{a}:{http_port}"))
                .chain(std::iter::once(format!("{src_ip}:{http_port}")))
                .collect();
            ensure_watched(&shared, target, others);
        }
    }
}

/// Which address to watch for a node that just announced itself.
///
/// The source address is proof: it held this datagram here. An advertised
/// address is only a claim, so it wins only when the node ranked it higher
/// and this box is on its subnet, and only after passing the same allowlist
/// the source did -- otherwise an announcement could name any host and have
/// this process connect to it.
///
/// The address the announcement calls its own is not consulted. Nothing acts
/// on it, so gating it would fail discovery closed over a subnet that decides
/// nothing.
fn announce_address(ranked: &[String], src_ip: &str, allowed: &Allow, local: &[Net]) -> String {
    ranked
        .iter()
        .filter(|a| allowed.permits(a, local))
        .find(|a| on_local_net(a, local))
        .cloned()
        .unwrap_or_else(|| src_ip.to_string())
}

/// The addresses a daemon reports for a peer, best first, and the best one.
///
/// Candidates run in order of evidence: link_ip held the mesh link, addrs
/// is what the peer reports it listens on, node_ip is only the name it calls
/// itself. An address on one of our own subnets beats that order outright,
/// since the pair's cluster identity is a subnet a LAN-only box cannot route
/// to.
fn peer_addresses(p: &Value, local: &[Net]) -> (Option<String>, Vec<String>) {
    let mut cands: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |v: Option<&str>| {
        if let Some(s) = v.filter(|s| !s.is_empty()) {
            if seen.insert(s.to_string()) {
                cands.push(s.to_string());
            }
        }
    };
    push(p["link_ip"].as_str());
    for a in p["addrs"].as_array().into_iter().flatten() {
        push(a.as_str());
    }
    push(p["node_ip"].as_str());
    // Loopback is the reporting box rather than the peer, and is right
    // only when router, reporter and peer share one box. It goes last.
    let lo = |c: &String| c.starts_with("127.") || c == "::1";
    cands.sort_by_key(lo);
    let best = cands
        .iter()
        .find(|c| on_local_net(c, local) && !lo(c))
        .or_else(|| cands.iter().find(|c| !lo(c)))
        .or_else(|| cands.first())
        .cloned();
    (best, cands)
}

/// The event number the daemon's last polled snapshot reflects.
///
/// A snapshot states what the stream has already delivered, so a poll
/// resumes the stream from here. Without it every poll drops the position
/// and the next event costs another poll.
fn anchored_seq(shared: &Arc<Shared>, addr: &str) -> Option<u64> {
    let d = shared.daemons.lock().unwrap();
    d.get(addr)?.status.as_ref()?["seq"].as_u64()
}

/// What one /events frame did to the stored view.
enum Applied {
    /// The view moved. Readers should wake.
    Yes,
    /// Nothing to do: another node's replicated event, or a frame with no
    /// patch in it.
    Skip,
    /// The view cannot be trusted. Re-read the snapshot.
    Resync,
}

/// Fold one /events frame into the stored snapshot.
///
/// The first frame of a stream is a snapshot, which seeds the view without
/// an HTTP read. After that each event holds a `patch`: paths into the
/// snapshot and the rows to store there.
///
/// A daemon replicates its peers' events, and those describe the
/// originating node's snapshot while this daemon's summarises its peers
/// rather than holding their rows. So only the watched daemon's own events
/// are applied, and every other node arrives on its own stream.
fn apply_frame(
    shared: &Arc<Shared>,
    addr: &str,
    node: Option<&str>,
    ev: &Value,
    last_seq: &mut Option<u64>,
) -> Applied {
    let mut d = shared.daemons.lock().unwrap();
    let Some(view) = d.get_mut(addr) else {
        return Applied::Skip;
    };
    if ev["type"].as_str() == Some("snapshot") {
        view.status = Some(ev["data"].clone());
        view.seen = Some(Instant::now());
        view.error = None;
        *last_seq = ev["seq"].as_u64();
        return Applied::Yes;
    }
    if ev["node"].as_str() != node {
        return Applied::Skip;
    }
    let Some(seq) = ev["seq"].as_u64() else {
        return Applied::Resync;
    };
    match *last_seq {
        Some(prev) if seq == prev + 1 => {}
        // The snapshot anchored past this one, so the change is already in
        // the view. A stream with a backlog delivers these after a poll.
        Some(prev) if seq <= prev => return Applied::Skip,
        Some(prev) => {
            log(
                "daemon_events_gap",
                &[
                    ("daemon", addr.to_string()),
                    ("expected", (prev + 1).to_string()),
                    ("got", seq.to_string()),
                ],
            );
            return Applied::Resync;
        }
        // No snapshot frame yet, so there is no view to patch.
        None => return Applied::Resync,
    }
    let Some(snapshot) = view.status.as_mut() else {
        return Applied::Resync;
    };
    if !apply_patch(snapshot, ev) {
        return Applied::Resync;
    }
    *last_seq = Some(seq);
    view.seen = Some(Instant::now());
    Applied::Yes
}

/// Store `value` at `at` in the snapshot, or remove that path when `value`
/// is absent. Returns false when the path does not lead through objects.
///
/// Intermediate objects are created, since the first event for a collection
/// addresses a row under a key the snapshot has never held.
fn patch_at(root: &mut Value, at: &[Value], value: Option<&Value>) -> bool {
    let keys: Vec<&str> = at.iter().filter_map(Value::as_str).collect();
    if keys.len() != at.len() {
        return false;
    }
    let Some((leaf, parents)) = keys.split_last() else {
        return false;
    };
    let mut cur = root;
    for key in parents {
        let Some(obj) = cur.as_object_mut() else {
            return false;
        };
        cur = obj
            .entry((*key).to_string())
            .or_insert_with(|| Value::Object(Default::default()));
    }
    let Some(obj) = cur.as_object_mut() else {
        return false;
    };
    match value {
        Some(v) => obj.insert((*leaf).to_string(), v.clone()),
        None => obj.remove(*leaf),
    };
    true
}

/// Apply one event's `patch` to a stored snapshot. Returns false when the
/// caller has to re-read instead.
fn apply_patch(snapshot: &mut Value, event: &Value) -> bool {
    let Some(patch) = event["patch"].as_array() else {
        return false;
    };
    // Every entry lands, or none does. A half-applied event leaves a view
    // that is wrong with nothing to report it.
    let mut staged = snapshot.clone();
    for entry in patch {
        let Some(at) = entry["at"].as_array() else {
            return false;
        };
        let value = entry.get("value");
        if !patch_at(&mut staged, at, value) {
            return false;
        }
    }
    *snapshot = staged;
    true
}

/// One /status read. Returns the daemon's node id on success.
async fn poll_status(shared: &Arc<Shared>, addr: &str) -> Option<String> {
    let url = format!("http://{}/status", testnet::mapped(addr));
    match http_get_json(&shared.client, &url, Duration::from_secs(5)).await {
        Ok(snap) => {
            let node_id = snap["node_id"].as_str().map(str::to_string);
            if shared.cfg.discover_peers {
                // Membership follows the mesh: every peer entry holds its
                // HTTP address (PeerHello inbound, PeerHelloOk outbound), so
                // one seed daemon reveals the rest.
                let local = local_nets();
                for (id, p) in snap["peers"].as_object().into_iter().flatten() {
                    let Some(port) = p["http_port"].as_u64() else {
                        continue;
                    };
                    if port == 0 || !p["alive"].as_bool().unwrap_or(false) {
                        continue;
                    }
                    // A node already watched learns this as another
                    // address. An unwatched one gets a watch.
                    let (best, all) = peer_addresses(p, &local);
                    let mut nodes = shared.nodes.lock().unwrap();
                    match nodes.get_mut(id) {
                        Some(w) => {
                            for a in all {
                                let a = format!("{a}:{port}");
                                if a != w.addr {
                                    w.alternates.insert(a);
                                }
                            }
                        }
                        None => {
                            drop(nodes);
                            if let Some(ip) = best {
                                ensure_watched(
                                    shared,
                                    format!("{ip}:{port}"),
                                    all.iter().map(|a| format!("{a}:{port}")).collect(),
                                );
                            }
                        }
                    }
                }
            }
            let mut d = shared.daemons.lock().unwrap();
            let seed = d.get(addr).map(|v| v.seed).unwrap_or(false);
            d.insert(
                addr.to_string(),
                DaemonView {
                    status: Some(snap),
                    seen: Some(Instant::now()),
                    error: None,
                    node_id: node_id.clone(),
                    seed,
                },
            );
            drop(d);
            shared.refresh.notify_one();
            node_id
        }
        Err(e) => {
            // Only a watched address keeps a row. A failed alternate would
            // otherwise list a daemon nobody polls.
            if let Some(v) = shared.daemons.lock().unwrap().get_mut(addr) {
                v.error = Some(e);
            }
            None
        }
    }
}

/// Probe every candidate group's announced endpoint. The probe is what turns
/// an announcement into a routable fact, and its /models answer is where the
/// served model names come from. Probes run concurrently so one wedged
/// endpoint cannot age the others' results past freshness.
///
/// A group announced by port has several candidate addresses, and the probe
/// decides among them the same way it decides anything else: by trying. The
/// selection is sticky, falls through on failure, and is re-raised to the
/// node's preferred address when that address replies again -- so a dropped
/// cable moves serving onto the LAN and a reconnected one moves it back,
/// without either transition needing an operator.
async fn prober(shared: Arc<Shared>) {
    loop {
        let table = announced_groups(&shared);
        {
            let mut probes = shared.probes.lock().unwrap();
            probes.retain(|k, _| table.get(k).map(|e| e.openai.is_some()).unwrap_or(false));
        }
        let mut set = tokio::task::JoinSet::new();
        for e in table.values() {
            let Some(ep) = e.openai.clone().filter(|x| !x.candidates.is_empty()) else {
                continue;
            };
            let group = e.group.clone();
            let client = shared.client.clone();
            let t = shared.cfg.probe_timeout;
            // Sticky selection and the promotion clock, read before the
            // round so the probe itself runs unlocked.
            let (sticky, promote) = {
                let probes = shared.probes.lock().unwrap();
                match probes.get(&group) {
                    Some(p) => (
                        p.selected.clone(),
                        p.promoted_at.elapsed() >= shared.cfg.probe_promote,
                    ),
                    None => (None, true),
                }
            };
            set.spawn(async move {
                let r = probe_candidates(&client, &ep.candidates, sticky, promote, t).await;
                (group, r)
            });
        }
        while let Some(Ok((group, (tried_top, res)))) = set.join_next().await {
            let now = Instant::now();
            let prev = {
                let probes = shared.probes.lock().unwrap();
                probes
                    .get(&group)
                    .map(|p| (p.ok, p.selected.clone(), p.promoted_at))
            };
            let (was_ok, was_sel, was_promoted) = match prev {
                Some((a, b, c)) => (Some(a), b, c),
                None => (None, None, now),
            };
            let pr = match res {
                Ok((url, mut models)) => {
                    if models.is_empty() {
                        // An endpoint that replies but lists nothing still
                        // serves, so fall back to the group name. Only `id`
                        // is known here, and the listing fills the rest in.
                        models.push(json!({"id": group.clone()}));
                    }
                    ProbeResult {
                        ok: true,
                        models,
                        seen: now,
                        error: None,
                        selected: Some(url),
                        promoted_at: if tried_top { now } else { was_promoted },
                    }
                }
                Err(e) => ProbeResult {
                    ok: false,
                    models: Vec::new(),
                    seen: now,
                    error: Some(e),
                    // Nothing replied, so there is nothing to be routed to;
                    // clearing it means recovery starts from the top of the
                    // ranking rather than from the last thing that worked.
                    selected: None,
                    promoted_at: if tried_top { now } else { was_promoted },
                },
            };
            if was_ok != Some(pr.ok) {
                log(
                    "group_probe",
                    &[
                        ("group", group.clone()),
                        ("ok", pr.ok.to_string()),
                        ("models", format!("{:?}", model_ids(&pr.models))),
                        ("error", pr.error.clone().unwrap_or_default()),
                    ],
                );
            }
            // Which address serves a group is worth a line every time it
            // moves: it is how a dropped fabric link shows up here.
            if pr.ok && pr.selected != was_sel {
                log(
                    "group_endpoint",
                    &[
                        ("group", group.clone()),
                        ("url", pr.selected.clone().unwrap_or_default()),
                        ("previous", was_sel.unwrap_or_default()),
                    ],
                );
            }
            shared.probes.lock().unwrap().insert(group, pr);
        }
        age_groups(&shared, &table);
        tokio::select! {
            _ = tokio::time::sleep(shared.cfg.probe_interval) => {}
            _ = shared.refresh.notified() => {}
        }
    }
}

/// Probe candidates until one replies.
///
/// Order is the whole behaviour. With a sticky selection the router probes
/// that address first and only walks the list when it fails, so a working
/// route is never abandoned for a re-decision. Every `PROBE_PROMOTE_S` the
/// order is inverted for one round: candidates ranked above the sticky one
/// go first, and a success there restores the route to the node's
/// preferred link.
///
/// Returns whether the top-ranked candidate was tried this round (which is
/// what resets the promotion clock) and either the replying URL with the
/// endpoint's own model entries, or every candidate's error.
async fn probe_candidates(
    client: &HttpClients,
    candidates: &[String],
    sticky: Option<String>,
    promote: bool,
    t: Duration,
) -> (bool, Result<(String, Vec<Value>), String>) {
    let at = sticky
        .as_ref()
        .and_then(|s| candidates.iter().position(|c| c == s));
    // A promotion round walks plain rank order, which puts the candidates
    // above the sticky one ahead of it. Every other round leads with the
    // sticky one and keeps rank order behind it.
    let order: Vec<&String> = match at.filter(|_| !promote) {
        Some(i) => std::iter::once(&candidates[i])
            .chain(
                candidates
                    .iter()
                    .enumerate()
                    .filter_map(|(j, c)| (j != i).then_some(c)),
            )
            .collect(),
        None => candidates.iter().collect(),
    };
    let tried_top = order.first().copied() == candidates.first();

    let mut errors: Vec<String> = Vec::new();
    for base in order {
        let url = format!("{}/models", base.trim_end_matches('/'));
        match http_get_json(client, &url, t).await {
            Ok(v) => {
                // An entry with no string `id` cannot be named or routed to.
                let models: Vec<Value> = v["data"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|m| m["id"].as_str().is_some())
                    .cloned()
                    .collect();
                return (tried_top, Ok((base.clone(), models)));
            }
            Err(e) => errors.push(format!("{base}: {e}")),
        }
    }
    (tried_top, Err(errors.join("; ")))
}

// ---------------------------------------------------------------------------
// HTTP plumbing shared by the modules
// ---------------------------------------------------------------------------

pub fn full_body(bytes: impl Into<Bytes>) -> BoxedBody {
    Full::new(bytes.into()).map_err(|e| match e {}).boxed()
}

pub fn json_response(status: StatusCode, v: &Value) -> Response<BoxedBody> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(full_body(v.to_string()))
        .expect("static response")
}

pub async fn http_get_json(client: &HttpClients, url: &str, t: Duration) -> Result<Value, String> {
    http_json(
        client,
        || {
            Request::builder()
                .method(Method::GET)
                .uri(url)
                .body(Full::new(Bytes::new()))
                .map_err(|e| e.to_string())
        },
        t,
    )
    .await
}

pub async fn http_post_json(
    client: &HttpClients,
    url: &str,
    body: &Value,
    t: Duration,
) -> Result<Value, String> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .map_err(|e| e.to_string())?;
    read_json(client.send_once(req, t).await?, t).await
}

async fn http_json(
    client: &HttpClients,
    build: impl Fn() -> Result<Request<Full<Bytes>>, String>,
    t: Duration,
) -> Result<Value, String> {
    read_json(client.send(build, t).await?, t).await
}

async fn read_json(
    resp: hyper::Response<hyper::body::Incoming>,
    t: Duration,
) -> Result<Value, String> {
    let status = resp.status();
    let body = tokio::time::timeout(t, resp.into_body().collect())
        .await
        .map_err(|_| "timeout reading body".to_string())?
        .map_err(|e| e.to_string())?
        .to_bytes();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    serde_json::from_slice(&body).map_err(|e| e.to_string())
}

async fn handle(
    shared: Arc<Shared>,
    peer_ip: String,
    req: Request<hyper::body::Incoming>,
) -> Response<BoxedBody> {
    if !shared.cfg.allowed_sources.permits_now(&peer_ip) {
        return json_response(
            StatusCode::FORBIDDEN,
            &json!({"error": "source address is not in ALLOWED_SOURCES"}),
        );
    }
    let path = {
        let p = req.uri().path().trim_end_matches('/');
        if p.is_empty() { "/" } else { p }.to_string()
    };
    match (req.method().clone(), path.as_str()) {
        // A browser requesting `/` gets the page. Everything else, curl and
        // the status pollers included, gets the document it always got.
        (Method::GET, "/") if ui::wants_html(req.headers()) => ui::page(),
        (Method::GET, "/" | "/healthz" | "/status.json") => {
            json_response(StatusCode::OK, &status_view(&shared))
        }
        (Method::GET, "/stats.json") => json_response(StatusCode::OK, &ui::stats(&shared).await),
        (Method::GET, "/v1" | "/v1/models") => json_response(
            StatusCode::OK,
            &json!({"object": "list", "data": model_objects(&shared)}),
        ),
        (Method::POST, "/mcp") => mcp::handle(&shared, req).await,
        // Owned rather than proxied: vLLM does not serve that endpoint, and the path
        // lands on its /v1/responses/{response_id} pattern for a 405.
        (Method::POST, "/v1/responses/input_tokens") => tokens::count(&shared, req).await,
        // The model in the body routes anything else posted. vLLM's
        // endpoint set moves between versions and a pinned list would rot,
        // so the contract is the one the router actually implements: a body
        // naming a model goes to whoever serves it. A body without one is
        // rejected there.
        (Method::POST, _) => proxy::forward(&shared, req).await,
        _ => json_response(StatusCode::NOT_FOUND, &json!({"error": "not found"})),
    }
}

#[tokio::main]
async fn main() {
    mentat_common::logfmt::set_program("mentatd-serve");
    // Just enough CLI for the Docker build's smoke test. Everything real is
    // configured by environment, like the daemon's compose file.
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("mentatd-serve {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    let cfg = Config::from_env();
    let shared = Arc::new(Shared {
        started: Instant::now(),
        verify: std::sync::atomic::AtomicBool::new(false),
        client: HttpClients::new(),
        daemons: Mutex::new(HashMap::new()),
        nodes: Mutex::new(HashMap::new()),
        watched: Mutex::new(HashSet::new()),
        probes: Mutex::new(HashMap::new()),
        live: Mutex::new(HashMap::new()),
        tools: Mutex::new(HashMap::new()),
        refresh: tokio::sync::Notify::new(),
        inflight: Mutex::new(BTreeMap::new()),
        next_req: AtomicU64::new(1),
        metrics: Mutex::new(HashMap::new()),
        cfg,
    });
    log(
        "serve_up",
        &[
            ("port", shared.cfg.port.to_string()),
            ("daemons", shared.cfg.daemons.join(",")),
            ("allowed_sources", shared.cfg.allowed_sources.to_string()),
        ],
    );
    for d in shared.cfg.daemons.clone() {
        ensure_watched(&shared, d, Vec::new());
    }
    {
        let shared = shared.clone();
        tokio::spawn(async move { prober(shared).await });
    }
    {
        let shared = shared.clone();
        tokio::spawn(async move { udp_listener(shared).await });
    }

    // vLLM listens with 2048, and a shallower queue here refuses first
    // under load. The kernel clamps this to somaxconn.
    let listener = (|| -> std::io::Result<tokio::net::TcpListener> {
        let sock = tokio::net::TcpSocket::new_v4()?;
        sock.set_reuseaddr(true)?;
        sock.bind(std::net::SocketAddr::from(([0, 0, 0, 0], shared.cfg.port)))?;
        sock.listen(4096)
    })()
    .unwrap_or_else(|e| panic!("bind 0.0.0.0:{}: {e}", shared.cfg.port));
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let peer_ip = peer.ip().to_string();
            let svc = service_fn(move |req| {
                let shared = shared.clone();
                let peer_ip = peer_ip.clone();
                async move { Ok::<_, std::convert::Infallible>(handle(shared, peer_ip, req).await) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 10.0.0.0/24 and 192.168.1.0/24, as a two-homed box would report them.
    fn subnets() -> Vec<Net> {
        vec![
            "10.0.0.0/24".parse().unwrap(),
            "192.168.1.0/24".parse().unwrap(),
        ]
    }

    /// A server that serves one request per connection and then abandons the
    /// socket without closing it politely, which is the state an idle-timed-out
    /// keep-alive connection is in when a client still holds it: the client
    /// believes the connection is usable and the server will not serve it.
    async fn serves_once_per_connection() -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    if sock.read(&mut buf).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let body = b"{\"data\":[]}";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.flush().await;
                    let _ = sock.write_all(body).await;
                    let _ = sock.flush().await;
                    // A second request on this connection goes unanswered.
                    let _ = sock.read(&mut buf).await;
                });
            }
        });
        addr
    }

    fn get(url: &str) -> Request<Full<Bytes>> {
        Request::builder()
            .method(Method::GET)
            .uri(url)
            .body(Full::new(Bytes::new()))
            .unwrap()
    }

    /// Proof the case below is real: with no retry, reusing the pooled
    /// connection fails, and reports something other than a connect error.
    /// This is the failure that drops a healthy group out of the route table.
    #[tokio::test]
    async fn without_a_retry_a_reused_connection_fails() {
        let addr = serves_once_per_connection().await;
        let url = format!("http://{addr}/v1/models");
        let pooled: HttpClient = Client::builder(TokioExecutor::new()).build_http();

        let first = pooled.request(get(&url)).await;
        assert!(first.is_ok(), "first request: {first:?}");
        let _ = first.unwrap().into_body().collect().await;
        // hyper returns a connection to the pool asynchronously once the
        // body is done. Without this wait the next call may open a new
        // connection, so the reuse under test would not happen and the
        // assertion below would hold for the wrong reason.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let second = pooled.request(get(&url)).await;
        let e = second.expect_err("reusing the dead connection should fail");
        assert!(
            !e.is_connect(),
            "the endpoint is up, so this must not read as a connect failure: {e}"
        );
    }

    #[tokio::test]
    async fn a_stale_pooled_connection_retries_instead_of_failing() {
        let addr = serves_once_per_connection().await;
        let url = format!("http://{addr}/v1/models");
        let t = Duration::from_secs(5);
        let clients = HttpClients::new();

        assert!(
            http_get_json(&clients, &url, t).await.is_ok(),
            "first probe"
        );
        // hyper returns a connection to the pool asynchronously once the
        // body is done. Without this wait the next call may open a new
        // connection, so the reuse under test would not happen and the
        // assertion below would hold for the wrong reason.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let second = http_get_json(&clients, &url, t).await;
        assert!(
            second.is_ok(),
            "probe over a stale pooled connection: {second:?}"
        );
    }

    /// The reported trap: ALLOWED_SOURCES lists where packets come from, and
    /// the node calls itself something on another subnet. Gating on that
    /// identity field, which nothing acts on, closes discovery over a subnet
    /// the operator had no reason to list. The source address decides.
    #[test]
    fn an_unlisted_identity_subnet_does_not_block_discovery() {
        let allowed = Allow::parse("192.168.1.0/24");
        let subnets = subnets();
        // Nothing advertised is both allowed and local, so the source stands.
        assert_eq!(
            announce_address(&["10.100.0.2".into()], "192.168.1.77", &allowed, &subnets),
            "192.168.1.77"
        );
    }

    /// An advertised address still passes the allowlist before it is used,
    /// since this one does get connected to.
    #[test]
    fn an_advertised_candidate_is_still_gated() {
        let subnets = subnets();
        let allowed = Allow::parse("10.0.0.0/24");
        assert_eq!(
            announce_address(&["10.0.0.7".into()], "10.0.0.1", &allowed, &subnets),
            "10.0.0.7",
            "allowed and local, so it is preferred"
        );
        assert_eq!(
            announce_address(&["192.168.1.13".into()], "10.0.0.1", &allowed, &subnets),
            "10.0.0.1",
            "local but not allowed, so the source stands"
        );
    }

    /// A POST is not replayed. Re-sending one would duplicate work the
    /// upstream may already be doing: an MCP tools/call has side effects, and
    /// a completion is minutes of compute on this hardware.
    #[tokio::test]
    async fn a_post_is_not_retried() {
        let addr = serves_once_per_connection().await;
        let url = format!("http://{addr}/mcp");
        let t = Duration::from_secs(5);
        let clients = HttpClients::new();
        let body = serde_json::json!({"jsonrpc": "2.0", "method": "tools/list"});

        assert!(
            http_post_json(&clients, &url, &body, t).await.is_ok(),
            "first post"
        );
        // hyper returns a connection to the pool asynchronously once the
        // body is done. Without this wait the next call may open a new
        // connection, so the reuse under test would not happen and the
        // assertion below would hold for the wrong reason.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let second = http_post_json(&clients, &url, &body, t).await;
        assert!(
            second.is_err(),
            "a POST over a stale connection fails rather than replaying: {second:?}"
        );
    }

    /// The retry must not make everything look healthy. An endpoint that is
    /// actually gone still fails, so the health gate still closes.
    #[tokio::test]
    async fn a_dead_endpoint_still_fails() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let clients = HttpClients::new();
        let r = http_get_json(
            &clients,
            &format!("http://{addr}/v1/models"),
            Duration::from_secs(2),
        )
        .await;
        assert!(r.is_err(), "a closed port must still fail");
    }

    #[test]
    fn subnet_membership() {
        let s = subnets();
        assert!(on_local_net("10.0.0.7", &s));
        assert!(on_local_net("192.168.1.13", &s));
        assert!(!on_local_net("10.100.0.1", &s));
        assert!(!on_local_net("not-an-ip", &s));
    }

    /// The reported failure: the peer calls itself by an address on a subnet
    /// this box cannot route to, and the reachable one is only in addrs.
    #[test]
    fn a_reachable_addr_beats_an_unroutable_identity() {
        let p = serde_json::json!({
            "node_ip": "10.100.0.1",
            "link_ip": "10.100.0.1",
            "addrs": ["10.100.0.1", "192.168.1.11"],
        });
        assert_eq!(
            peer_addresses(&p, &subnets()).0.as_deref(),
            Some("192.168.1.11")
        );
    }

    /// With nothing on a local subnet, the link address still leads: it
    /// held a connection, and a routed network does not need a local wire.
    #[test]
    fn link_ip_leads_when_nothing_is_local() {
        let p = serde_json::json!({
            "node_ip": "10.100.0.1",
            "link_ip": "172.16.4.4",
            "addrs": ["172.16.9.9"],
        });
        assert_eq!(
            peer_addresses(&p, &subnets()).0.as_deref(),
            Some("172.16.4.4")
        );
    }

    /// The reported gap: a client reading /v1/models through the router got
    /// three fields where the engine serves eight, so max_model_len -- which
    /// differs between two deployments of the same family -- was undiscoverable
    /// and had to be hardcoded.
    #[test]
    fn a_listed_model_keeps_every_field_the_engine_sent() {
        let engine = json!({
            "id": "glm53",
            "object": "model",
            "created": 1788281389,
            "owned_by": "vllm",
            "root": "/models/glm-5.3-flash-nvfp4",
            "parent": null,
            "max_model_len": 262144,
            "permission": [{"id": "modelperm-9631122d1ff25211"}],
        });
        let listed = listed_model(&engine, "ga");
        assert_eq!(listed["max_model_len"], 262144);
        assert_eq!(listed["root"], "/models/glm-5.3-flash-nvfp4");
        assert_eq!(listed["permission"][0]["id"], "modelperm-9631122d1ff25211");
        assert!(listed["parent"].is_null());
        assert_eq!(
            listed.as_object().unwrap().len(),
            engine.as_object().unwrap().len(),
            "no field added or dropped"
        );
        assert_eq!(listed["owned_by"], "ga", "the one rewrite");
    }

    /// The bare entry a probe synthesises when an endpoint lists nothing, and
    /// an entry holding fields this router predates. Both reach the listing
    /// intact.
    #[test]
    fn a_sparse_or_unfamiliar_entry_survives() {
        let bare = listed_model(&json!({"id": "gb"}), "gb");
        assert_eq!(bare["id"], "gb");
        assert_eq!(bare["owned_by"], "gb");

        let future = listed_model(&json!({"id": "m", "some_new_field": [1, 2]}), "ga");
        assert_eq!(future["some_new_field"], json!([1, 2]));
    }

    /// An entry with no `id` cannot be routed to, so it never reaches the
    /// route table.
    #[test]
    fn model_ids_skips_what_it_cannot_name() {
        let models = vec![
            json!({"id": "a"}),
            json!({"no_id": true}),
            json!({"id": "b"}),
        ];
        assert_eq!(model_ids(&models), vec!["a", "b"]);
    }

    fn endpoint(url: &str) -> Endpoint {
        Endpoint {
            candidates: vec![url.to_string()],
            announced: url.to_string(),
            note: None,
        }
    }

    /// The provider names the engine behind the endpoint, so it has to come
    /// from the rank whose endpoint won rather than from whichever agent the
    /// snapshot happened to list first.
    #[test]
    fn the_provider_follows_the_winning_endpoint() {
        let (ep, provider) = best_openai(vec![
            (endpoint("http://b:8000/v1"), "sglang".into()),
            (endpoint("http://a:8000/v1"), "vllm".into()),
        ]);
        assert_eq!(ep.unwrap().best(), Some("http://a:8000/v1"));
        assert_eq!(provider, "vllm");
    }

    /// An agent predating the field announces no provider, which reads as
    /// unknown rather than as a group with no endpoint.
    #[test]
    fn an_unannounced_provider_is_empty() {
        let (ep, provider) = best_openai(vec![(endpoint("http://a:8000/v1"), String::new())]);
        assert!(ep.is_some());
        assert_eq!(provider, "");

        let (none, provider) = best_openai(Vec::new());
        assert!(none.is_none());
        assert_eq!(provider, "");
    }

    /// The reported shape: a group whose container is long gone stays in
    /// every daemon snapshot, so the router has to be the one that calls it
    /// over.
    #[test]
    fn a_group_that_never_serves_is_retired_once_the_ttl_passes() {
        let ttl = Duration::from_secs(3600);
        let mut l = Liveness::new(Instant::now() - Duration::from_secs(3599));
        assert!(!l.round(Instant::now(), false, ttl), "still inside the ttl");
        assert!(!l.retired(ttl));

        l.ok_at = Instant::now() - Duration::from_secs(3601);
        assert!(l.round(Instant::now(), false, ttl), "the retiring round");
        assert!(l.retired(ttl));
        assert!(
            !l.round(Instant::now(), false, ttl),
            "retirement is said once per outage"
        );
    }

    /// One good round is the whole recovery: a model brought back after a
    /// day down is servable on the next probe.
    #[test]
    fn serving_again_un_retires_a_group() {
        let ttl = Duration::from_secs(3600);
        let mut l = Liveness::new(Instant::now() - Duration::from_secs(86_400));
        assert!(l.round(Instant::now(), false, ttl));
        assert!(l.retired(ttl));

        assert!(!l.round(Instant::now(), true, ttl));
        assert!(!l.retired(ttl), "back the round it answers");

        // And going down again is a new outage, so it reports it again.
        l.ok_at = Instant::now() - Duration::from_secs(3601);
        assert!(l.round(Instant::now(), false, ttl));
    }

    /// An advertised address is a claim. Without the allowlist check on the
    /// candidates, an announcement naming any host would have this process
    /// connect to it.
    #[test]
    fn an_advertised_address_is_still_a_claim() {
        let allowed = Allow::parse("127.0.0.0/8");
        assert!(allowed.permits("127.0.0.1", &[]));
        assert!(!allowed.permits("192.168.1.109", &[]));
        assert!(!allowed.permits("203.0.113.7", &[]));
    }

    /// A port announcement is resolved against the announcing node's own
    /// ranked addresses. The reported failure it exists for: the endpoint
    /// was announced on the fabric address, and a router off that fabric
    /// could never reach it however healthy the model was.
    #[test]
    fn a_port_announcement_resolves_to_every_address_of_its_node() {
        let snap = serde_json::json!({
            "node_ip": "10.100.0.1",
            "addrs": ["10.100.0.1", "192.168.1.11"],
            "peers": {},
        });
        let mut nodes = HashMap::new();
        collect_node_addrs(&snap, &mut nodes);
        let agent = serde_json::json!({
            "node_ip": "10.100.0.1",
            "services": {"openai": {"host": "", "port": 8000, "path": "/v1"}},
        });
        let allowed = Allow::parse("10.0.0.0/8, 192.168.1.0/24");
        let ep = endpoint_of(&agent, "openai", &nodes, &allowed, &subnets()).unwrap();
        assert_eq!(
            ep.candidates,
            vec![
                // Local subnet first: this router shares the LAN wire and
                // not the fabric, whatever the node's own ranking prefers.
                "http://192.168.1.11:8000/v1",
                "http://10.100.0.1:8000/v1",
            ]
        );
    }

    /// A verbatim URL is the escape hatch, so it must survive untouched --
    /// including past ALLOWED_SOURCES, which covers addresses this process
    /// derived rather than one the operator wrote down.
    #[test]
    fn a_verbatim_url_is_neither_re_derived_nor_gated() {
        let agent = serde_json::json!({
            "node_ip": "10.100.0.1",
            "services": {"openai": {"host": "203.0.113.7", "port": 8000, "path": "/v1"}},
        });
        let ep = endpoint_of(
            &agent,
            "openai",
            &HashMap::new(),
            &Allow::parse("10.0.0.0/8"),
            &subnets(),
        )
        .unwrap();
        assert_eq!(ep.candidates, vec!["http://203.0.113.7:8000/v1"]);
    }

    /// A derived address is a claim this process would connect to, so the
    /// allowlist does apply to it. Everything filtered out leaves a resolved
    /// endpoint with nothing to try, which health_of reports as its own
    /// failure rather than as "nothing announced".
    #[test]
    fn derived_addresses_are_gated_and_may_leave_nothing() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "10.100.0.1".to_string(),
            vec!["10.100.0.1".to_string(), "192.168.1.11".to_string()],
        );
        let agent = serde_json::json!({
            "node_ip": "10.100.0.1",
            "services": {"openai": {"host": "", "port": 8000, "path": "/v1"}},
        });
        let ep = endpoint_of(
            &agent,
            "openai",
            &nodes,
            &Allow::parse("172.16.0.0/12"),
            &subnets(),
        )
        .unwrap();
        assert!(ep.candidates.is_empty(), "{:?}", ep.candidates);
        assert!(ep.announced.contains("port 8000/v1"), "{}", ep.announced);
    }

    /// An agent registered under an address the node does not list first
    /// must still find its node. Agents register with whatever MENTAT_NODE_IP
    /// their container was given, which is routinely the fabric address.
    #[test]
    fn an_agent_joins_its_node_by_any_of_its_addresses() {
        let snap = serde_json::json!({
            "node_ip": "192.168.1.13",
            "addrs": ["192.168.1.13"],
            "peers": {"n1": {
                "node_ip": "10.100.0.1",
                "link_ip": "192.168.1.11",
                "addrs": ["192.168.1.11", "10.100.0.1"],
            }},
        });
        let mut nodes = HashMap::new();
        collect_node_addrs(&snap, &mut nodes);
        for key in ["10.100.0.1", "192.168.1.11"] {
            assert_eq!(
                nodes.get(key).map(Vec::len),
                Some(2),
                "{key} must resolve to the peer's whole address list"
            );
        }
    }

    /// A peer that reports one address has not contradicted a daemon that
    /// reports three, so the longer description wins.
    #[test]
    fn the_fuller_description_of_a_node_wins() {
        let mut nodes = HashMap::new();
        collect_node_addrs(
            &serde_json::json!({
                "node_ip": "10.100.0.1", "addrs": [], "peers": {}
            }),
            &mut nodes,
        );
        collect_node_addrs(
            &serde_json::json!({
                "node_ip": "10.100.0.1",
                "addrs": ["10.100.0.1", "192.168.1.11"],
                "peers": {},
            }),
            &mut nodes,
        );
        assert_eq!(nodes["10.100.0.1"].len(), 2);
    }

    /// A working route is not re-decided every round. Without stickiness the
    /// router would move live traffic back to the preferred address the
    /// instant it replied to a probe, mid-generation.
    #[tokio::test]
    async fn a_working_selection_is_probed_first_and_kept() {
        let (top, low) = (
            models_endpoint("model-x").await,
            models_endpoint("model-x").await,
        );
        let c = vec![format!("http://{top}/v1"), format!("http://{low}/v1")];
        let clients = HttpClients::new();
        let (tried_top, r) = probe_candidates(
            &clients,
            &c,
            Some(c[1].clone()),
            false,
            Duration::from_secs(2),
        )
        .await;
        assert!(!tried_top, "the sticky candidate is not the top-ranked one");
        assert_eq!(r.unwrap().0, c[1], "the sticky candidate must be kept");
    }

    /// The failure this whole path exists for: the preferred address stops
    /// replying and the group keeps serving on the next one.
    #[tokio::test]
    async fn a_dead_top_candidate_falls_through() {
        let up = models_endpoint("model-x").await;
        let dead = dead_addr().await;
        let c = vec![format!("http://{dead}/v1"), format!("http://{up}/v1")];
        let clients = HttpClients::new();
        let (_, r) = probe_candidates(&clients, &c, None, false, Duration::from_secs(2)).await;
        let (url, models) = r.expect("the second candidate answers");
        assert_eq!(url, c[1]);
        assert_eq!(model_ids(&models), vec!["model-x"]);
    }

    /// And back again once the cable is in: a promotion round tries the
    /// higher-ranked candidate first, so recovery does not need an operator.
    #[tokio::test]
    async fn a_promotion_round_takes_the_preferred_address_back() {
        let top = models_endpoint("model-x").await;
        let low = models_endpoint("model-x").await;
        let c = vec![format!("http://{top}/v1"), format!("http://{low}/v1")];
        let clients = HttpClients::new();
        let (tried_top, r) = probe_candidates(
            &clients,
            &c,
            Some(c[1].clone()),
            true,
            Duration::from_secs(2),
        )
        .await;
        assert!(tried_top);
        assert_eq!(r.unwrap().0, c[0], "the preferred address answers again");
    }

    /// Every candidate down must fail, and report which ones. A fall-through
    /// list that swallowed the reasons would report one dead address as the
    /// whole story.
    #[tokio::test]
    async fn every_candidate_down_reports_every_candidate() {
        let (d1, d2) = (dead_addr().await, dead_addr().await);
        let c = vec![format!("http://{d1}/v1"), format!("http://{d2}/v1")];
        let clients = HttpClients::new();
        let (_, r) = probe_candidates(&clients, &c, None, false, Duration::from_secs(2)).await;
        let e = r.expect_err("nothing answers");
        assert!(
            e.contains(&d1.to_string()) && e.contains(&d2.to_string()),
            "{e}"
        );
    }

    /// A single-model /v1/models endpoint, for the candidate tests above.
    async fn models_endpoint(model: &str) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = format!("{{\"data\":[{{\"id\":\"{model}\"}}]}}");
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    while sock.read(&mut buf).await.unwrap_or(0) > 0 {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\n\r\n",
                            body.len()
                        );
                        let _ = sock.write_all(head.as_bytes()).await;
                        let _ = sock.write_all(body.as_bytes()).await;
                        let _ = sock.flush().await;
                    }
                });
            }
        });
        addr
    }

    /// An address nothing listens on: bound, read, and dropped.
    async fn dead_addr() -> std::net::SocketAddr {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    }

    fn snap() -> Value {
        json!({
            "node_id": "n1",
            "groups": {"glm": {"actors": {"a:1": {"state": "spawning"}}}},
        })
    }

    /// The event's row replaces the one the snapshot held, so a reader has
    /// either the old row or the new one.
    #[test]
    fn a_patch_replaces_one_row() {
        let mut v = snap();
        let ev = json!({"patch": [
            {"at": ["groups", "glm", "actors", "a:1"], "value": {"state": "running"}},
        ]});
        assert!(apply_patch(&mut v, &ev));
        assert_eq!(v["groups"]["glm"]["actors"]["a:1"]["state"], "running");
    }

    /// An entry with no `value` removes the path. The daemon sends one when
    /// it drops the row itself: `peer_forgotten`, `claim_released`,
    /// `driver_disconnected`.
    #[test]
    fn a_patch_with_no_value_removes_the_path() {
        let mut v = snap();
        let ev = json!({"patch": [{"at": ["groups", "glm", "actors", "a:1"]}]});
        assert!(apply_patch(&mut v, &ev));
        assert!(v["groups"]["glm"]["actors"]["a:1"].is_null());
    }

    /// A minor bump may add an event kind. Applying reads `patch` alone, so
    /// an older router folds in a kind released after it.
    #[test]
    fn an_unknown_event_kind_still_applies_its_patch() {
        let mut v = snap();
        let ev = json!({"type": "something_from_a_later_minor", "patch": [
            {"at": ["groups", "glm", "actors", "a:1"], "value": {"state": "running"}},
        ]});
        assert!(apply_patch(&mut v, &ev));
        assert_eq!(v["groups"]["glm"]["actors"]["a:1"]["state"], "running");
    }

    /// The first event for a collection uses a key the snapshot has never
    /// held, so the objects along the way are created.
    #[test]
    fn a_patch_creates_the_objects_it_walks_through() {
        let mut v = snap();
        let ev = json!({"patch": [
            {"at": ["groups", "new", "agents", "g1"], "value": {"alive": true}},
        ]});
        assert!(apply_patch(&mut v, &ev));
        assert_eq!(v["groups"]["new"]["agents"]["g1"]["alive"], true);
    }

    /// Every entry lands or none does. A half-applied event leaves a view
    /// that is wrong with nothing to report it.
    #[test]
    fn a_patch_that_cannot_finish_changes_nothing() {
        let mut v = snap();
        let before = v.clone();
        let ev = json!({"patch": [
            {"at": ["islands"], "value": []},
            {"at": ["node_id", "deeper"], "value": 1},
        ]});
        assert!(!apply_patch(&mut v, &ev));
        assert_eq!(v, before);
    }

    #[test]
    fn a_frame_with_no_patch_is_refused() {
        let mut v = snap();
        assert!(!apply_patch(&mut v, &json!({"type": "actor_running"})));
    }

    #[test]
    fn an_old_daemon_reports_node_ip_alone() {
        let p = serde_json::json!({"node_ip": "192.168.1.11"});
        assert_eq!(
            peer_addresses(&p, &subnets()).0.as_deref(),
            Some("192.168.1.11")
        );
        assert_eq!(peer_addresses(&serde_json::json!({}), &subnets()).0, None);
    }

    /// A daemon on this box reaches its peers from loopback, and the peer
    /// reports that as the link address. It is this box, and watching
    /// it would poll the local daemon under the peer's name.
    #[test]
    fn a_loopback_link_address_is_not_the_peer() {
        let p = serde_json::json!({
            "node_ip": "192.168.1.77",
            "link_ip": "127.0.0.1",
            "addrs": ["10.100.0.2", "192.168.1.77"],
        });
        let (best, all) = peer_addresses(&p, &subnets());
        assert_eq!(best.as_deref(), Some("192.168.1.77"));
        assert_eq!(all.last().map(String::as_str), Some("127.0.0.1"), "{all:?}");
    }
}
