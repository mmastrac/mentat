//! mentatd-serve: the cluster's serving front door, in the separate process
//! the mentat design reserved for it -- mentatd never touches inference
//! traffic, and this binary never touches cluster control.
//!
//! Two aggregations over what model containers announce at `ray start`
//! (MENTAT_OPENAI_API / MENTAT_MCP_API, carried on AgentRegister):
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
//! answers a probe.

mod logfmt;
mod mcp;
mod proxy;
mod secret;
mod tokens;
mod ws;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
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

use logfmt::log;

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
/// `fresh` exists to answer that. One retry over a new connection separates
/// a stale socket from an endpoint that is actually gone.
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
        tokio::time::timeout(t, self.pooled.request(req))
            .await
            .map_err(|_| format!("timeout after {:.1}s", t.as_secs_f64()))?
            .map_err(|e| e.to_string())
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
    pub mcp_timeout: Duration,
    pub tools_ttl: Duration,
    pub allowed_sources: Vec<String>,
    pub discover_peers: bool,
}

fn env_str(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
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
            // Unset means seed with the local daemon; set-but-empty means no
            // seeds at all, leaving UDP announcements as the only way in.
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
            // latency_percentiles legitimately blocks for its whole window
            // (up to 120s), so the MCP forward allows more than that.
            mcp_timeout: env_secs("MCP_TIMEOUT_S", 180.0),
            tools_ttl: env_secs("TOOLS_TTL_S", 60.0),
            // Own subnets plus the docker bridge ranges: a bridge-networked
            // client (OpenWebUI) reaching this host keeps its 172.x source.
            allowed_sources: env_str("ALLOWED_SOURCES", "10.100.0.,192.168.1.,127.0.0.1,::1,172.")
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            discover_peers: env_str("DISCOVER_PEERS", "1") == "1",
        }
    }
}

pub struct DaemonView {
    pub status: Option<Value>,
    pub seen: Option<Instant>,
    pub error: Option<String>,
}

pub struct ProbeResult {
    pub ok: bool,
    /// The endpoint's own `/models` entries, verbatim. Carrying whole objects
    /// keeps `max_model_len` and the rest correct as vLLM adds fields.
    pub models: Vec<Value>,
    pub seen: Instant,
    pub error: Option<String>,
    /// The candidate this group is currently routed to. Sticky: once an
    /// address answers, the router keeps using it rather than re-deciding
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
    pub client: HttpClients,
    /// One entry per daemon HTTP address being watched.
    pub daemons: Mutex<HashMap<String, DaemonView>>,
    pub watched: Mutex<HashSet<String>>,
    /// group -> latest endpoint probe. Present only for probe candidates
    /// (openai announced, actors running).
    pub probes: Mutex<HashMap<String, ProbeResult>>,
    /// "group url" -> cached tools/list answer for the MCP merge.
    pub tools: Mutex<HashMap<String, (Instant, Vec<Value>)>>,
    /// Wakes the prober when a daemon view changes, so admission does not
    /// wait out a full probe interval after boot.
    pub refresh: tokio::sync::Notify,
}

/// One announced service, resolved into the base URLs that could serve it.
#[derive(Clone)]
pub struct Endpoint {
    /// Base URLs to try, best first. A verbatim announcement has exactly
    /// one and it is never re-derived -- naming a host is the operator
    /// saying which address to use. A port announcement has one per address
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
    /// What serves `openai`, as the announcing agent said it (`vllm`). Taken
    /// from the agent whose endpoint won, since it describes the engine
    /// behind that endpoint. Empty when the container did not say.
    pub provider: String,
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
/// HTTP is trivial bandwidth, so reachable beats fast.
fn endpoint_of(
    agent: &Value,
    svc: &str,
    nodes: &HashMap<String, Vec<String>>,
    allowed: &[String],
    subnets: &[(u32, u32)],
) -> Option<Endpoint> {
    let note = agent["service_notes"][svc]
        .as_str()
        .filter(|n| !n.is_empty())
        .map(str::to_string);
    if let Some(url) = agent["services"][svc].as_str().filter(|u| !u.is_empty()) {
        return Some(Endpoint {
            candidates: vec![url.to_string()],
            announced: url.to_string(),
            note,
        });
    }
    let sp = &agent["services_ports"][svc];
    let port = sp["port"].as_u64()?;
    let path = sp["path"].as_str().unwrap_or_default();
    let node_ip = agent["node_ip"].as_str().unwrap_or_default();
    let mut hosts: Vec<String> = nodes
        .get(node_ip)
        .cloned()
        .unwrap_or_else(|| vec![node_ip.to_string()]);
    hosts.retain(|h| !h.is_empty() && prefix_allowed(allowed, h));
    // A node may list an address twice across the views it was merged from,
    // and a duplicate candidate would be probed twice and reported twice.
    let mut seen = HashSet::new();
    hosts.retain(|h| seen.insert(h.clone()));
    // Stable, so the node's own order survives inside each half.
    hosts.sort_by_key(|h| !on_local_subnet(h, subnets));
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

/// Merge the daemon views into one group table. A group's agents all
/// register with one daemon (the rendezvous rule), so overlap only happens
/// around a stale view -- resolved toward the daemon with more running
/// actors.
pub fn group_table(shared: &Shared) -> BTreeMap<String, GroupEntry> {
    let stale = shared.cfg.poll_interval * 3;
    let subnets = local_subnets();
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
            let agents: Vec<&Value> = g["agents"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|a| a["alive"].as_bool().unwrap_or(false))
                .collect();
            let running = g["actors"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|a| a["state"].as_str() == Some("running"))
                .count();
            let resolve = |a: &Value, svc: &str| {
                endpoint_of(a, svc, &nodes, &shared.cfg.allowed_sources, &subnets)
            };
            // Only the rank running the API server announces "openai". If
            // several ever do, the one whose best candidate sorts first
            // wins, for determinism.
            let (openai, provider) = best_openai(
                agents
                    .iter()
                    .filter_map(|a| {
                        resolve(a, "openai")
                            .map(|e| (e, a["provider"].as_str().unwrap_or_default().to_string()))
                    })
                    .collect(),
            );
            // Every rank announces "mcp" (the status server runs on all of
            // them). Prefer the API node's -- it is the one with throughput
            // to report -- then the same order.
            let mcp = agents
                .iter()
                .filter_map(|a| {
                    resolve(a, "mcp").map(|m| {
                        (
                            a["services"]["openai"].is_null()
                                && a["services_ports"]["openai"].is_null(),
                            m,
                        )
                    })
                })
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

/// Ok(model names) when the group may take traffic; Err(why) otherwise.
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
    if e.running == 0 {
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
/// from probing the endpoint's /models, so SERVED_NAME needs no announcing.
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

/// What `/v1/models` answers: every healthy group's `/models` entries as the
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
                    // Names only. /v1/models carries the whole entries, and
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
        "daemons": daemons,
        "groups": groups,
        "models": models,
    })
}

// ---------------------------------------------------------------------------
// Daemon watchers and the endpoint prober
// ---------------------------------------------------------------------------

pub fn ensure_watched(shared: &Arc<Shared>, addr: String) {
    {
        let mut w = shared.watched.lock().unwrap();
        if !w.insert(addr.clone()) {
            return;
        }
    }
    log("daemon_watch", &[("daemon", addr.clone())]);
    let shared = shared.clone();
    tokio::spawn(async move { watch_daemon(shared, addr).await });
}

/// Hold one daemon fresh: poll /status, and keep a /events WebSocket open so
/// any cluster event (an actor dying, an agent registering) triggers an
/// immediate re-read instead of waiting out the poll interval.
async fn watch_daemon(shared: Arc<Shared>, addr: String) {
    loop {
        poll_status(&shared, &addr).await;
        match ws::EventStream::connect(&addr).await {
            Ok(mut es) => loop {
                match es.next(shared.cfg.poll_interval).await {
                    Ok(Some(_event)) => {
                        // Coalesce a burst (boot emits many events at once)
                        // into one re-read.
                        while let Ok(Some(_)) = es.next(Duration::from_millis(200)).await {}
                        poll_status(&shared, &addr).await;
                    }
                    Ok(None) => poll_status(&shared, &addr).await,
                    Err(e) => {
                        log(
                            "daemon_events_lost",
                            &[("daemon", addr.clone()), ("error", e.to_string())],
                        );
                        break;
                    }
                }
            },
            Err(e) => {
                let mut d = shared.daemons.lock().unwrap();
                let v = d.entry(addr.clone()).or_insert(DaemonView {
                    status: None,
                    seen: None,
                    error: None,
                });
                v.error = Some(format!("events: {e}"));
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Listen for the daemons' UDP announcements. An announcement is a hint: it
/// only adds a watch candidate, and everything the daemon claims is then
/// read over TCP and probed like any other -- which is why the datagrams
/// carry no signature today. Both the datagram source and the claimed
/// address must pass the same prefix list as the HTTP side. A future
/// MENTAT_SECRET signs the datagrams under a bumped mentat_announce
/// version, and the check slots in beside the prefix gate below.
async fn udp_listener(shared: Arc<Shared>) {
    let port = shared.cfg.announce_port;
    if port == 0 {
        return;
    }
    let sock = match tokio::net::UdpSocket::bind(("0.0.0.0", port)).await {
        Ok(s) => s,
        Err(e) => {
            log(
                "announce_listen_failed",
                &[("port", port.to_string()), ("error", e.to_string())],
            );
            return;
        }
    };
    let key = secret::load();
    log(
        "announce_listen",
        &[
            ("port", port.to_string()),
            (
                "verify",
                match key {
                    Some(_) => "required".to_string(),
                    None => "off (no MENTAT_SECRET)".to_string(),
                },
            ),
        ],
    );
    // node_id -> (boot_id, last accepted seq). Bounds replay within a boot.
    let mut seen: HashMap<String, (String, u64)> = HashMap::new();
    // Sources already complained about, so a 5s broadcast cannot flood the
    // log with the same misconfiguration.
    let mut warned: HashSet<String> = HashSet::new();
    // Sources whose advertised address has already been reported as
    // unroutable-looking, so the note lands once rather than every round.
    let mut noted: HashSet<String> = HashSet::new();
    // node_id -> the address chosen for it. A dual-homed daemon broadcasts on
    // every interface, so without this the same node is watched once per
    // link.
    let mut chosen: HashMap<String, String> = HashMap::new();
    let universe = secret::universe();
    let mut buf = [0u8; 2048];
    loop {
        let Ok((n, src)) = sock.recv_from(&mut buf).await else {
            continue;
        };
        // Another cluster on this broadcast domain is not a misconfiguration,
        // so it is dropped before the key is consulted and before anything is
        // logged. An announcement with no universe predates the field.
        match secret::peek_universe(&buf[..n]) {
            Some(u) if u != universe => continue,
            _ => {}
        }
        // A key makes signatures mandatory, so a stripped signature or a
        // replayed version-1 datagram cannot downgrade this listener.
        let v = match &key {
            Some(k) => {
                let Some(p) = secret::verify(&buf[..n], k) else {
                    // A wrong key and a stripped signature look the same from
                    // here, and both mean the sender cannot be trusted.
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
                if p["mentat_announce"].as_u64() != Some(secret::SIGNED_VERSION) {
                    continue;
                }
                let Some(t) = p["t"].as_f64() else { continue };
                if !secret::fresh(t, secret::now_s()) {
                    continue;
                }
                let node = p["node_id"].as_str().unwrap_or_default().to_string();
                let boot = p["boot_id"].as_str().unwrap_or_default().to_string();
                let seq = p["seq"].as_u64().unwrap_or(0);
                if node.is_empty() || boot.is_empty() {
                    continue;
                }
                // A restart resets seq, which the new boot_id distinguishes
                // from a replay. One announcement per interface repeats a
                // seq; dropping the repeat costs nothing, since the address
                // it carries is the same one.
                match seen.get(&node) {
                    Some((b, last)) if *b == boot && seq <= *last => continue,
                    _ => seen.insert(node, (boot, seq)),
                };
                p
            }
            None => {
                let Ok(p) = serde_json::from_slice::<Value>(&buf[..n]) else {
                    continue;
                };
                if p["mentat_announce"].as_u64() != Some(1) {
                    // A signed announcement to a listener with no key. Left
                    // unverified it would be a downgrade, so it is dropped --
                    // and said out loud, since the cause is one missing
                    // variable and the symptom is an empty route table.
                    if p.get("sig").is_some() && warned.insert(src.ip().to_string()) {
                        log(
                            "announce_unverifiable",
                            &[
                                ("src", src.ip().to_string()),
                                (
                                    "why",
                                    "signed announcement, no MENTAT_SECRET here".to_string(),
                                ),
                            ],
                        );
                    }
                    continue;
                }
                p
            }
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
        // here; so is any advertised address before it is chosen, further
        // down. The advertised address itself is not, because nothing acts
        // on it any more -- gating a field the router only reads would fail
        // discovery closed over a subnet the operator has no reason to be
        // thinking about, and say nothing about why.
        let src_ip = src.ip().to_string();
        if !source_allowed(&shared.cfg, &src_ip) {
            // Named once per source. A dropped announcement is otherwise an
            // empty cluster with no stated cause.
            if warned.insert(src_ip.clone()) {
                log(
                    "announce_source_not_allowed",
                    &[
                        ("src", src_ip.clone()),
                        ("allowed_sources", shared.cfg.allowed_sources.join(",")),
                    ],
                );
            }
            continue;
        }
        // One watch per node. A node with two links broadcasts on both, and
        // the datagrams differ only in source address.
        let node = v["node_id"].as_str().unwrap_or_default().to_string();
        if !node.is_empty() && chosen.contains_key(&node) {
            continue;
        }
        // The node ranks its own addresses, most preferred first, because
        // only it knows which link is the fast one. Take the best it offers
        // that lands on a subnet we are attached to; failing that, the source
        // address, which at least carried this packet here.
        let ranked: Vec<String> = v["addrs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str())
            .map(str::to_string)
            .collect();
        let pick = announce_address(
            &ranked,
            &src_ip,
            &shared.cfg.allowed_sources,
            &local_subnets(),
        );
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
        if !node.is_empty() {
            chosen.insert(node, pick.clone());
        }
        ensure_watched(&shared, format!("{pick}:{http_port}"));
    }
}

/// This box's IPv4 networks, as (network, mask). An address inside one of
/// these is on a wire we are attached to, which is the closest thing to
/// proof of reachability available without dialling it.
fn local_subnets() -> Vec<(u32, u32)> {
    let Ok(ifaces) = getifaddrs::InterfaceFilter::new().v4().get() else {
        return Vec::new();
    };
    ifaces
        .filter_map(|i| match (i.address.ip_addr(), i.address.netmask()) {
            (Some(IpAddr::V4(a)), Some(IpAddr::V4(m))) => {
                let (a, m) = (u32::from(a), u32::from(m));
                Some((a & m, m))
            }
            _ => None,
        })
        .collect()
}

fn on_local_subnet(ip: &str, subnets: &[(u32, u32)]) -> bool {
    let Ok(v4) = ip.parse::<Ipv4Addr>() else {
        return false;
    };
    let a = u32::from(v4);
    subnets.iter().any(|(net, mask)| a & mask == *net)
}

/// Which address to watch for a node that just announced itself.
///
/// The source address is proof: it carried this datagram here. An advertised
/// address is only a claim, so it wins only when the node ranked it higher
/// and this box is on its subnet, and only after passing the same allowlist
/// the source did -- otherwise an announcement could name any host and have
/// this process connect to it.
///
/// The address the announcement calls its own is not consulted. Nothing acts
/// on it, so gating it would fail discovery closed over a subnet that decides
/// nothing.
fn announce_address(
    ranked: &[String],
    src_ip: &str,
    allowed: &[String],
    subnets: &[(u32, u32)],
) -> String {
    ranked
        .iter()
        .filter(|a| prefix_allowed(allowed, a))
        .find(|a| on_local_subnet(a, subnets))
        .cloned()
        .unwrap_or_else(|| src_ip.to_string())
}

/// Which address to reach a peer on, given what a daemon reports about it.
///
/// Candidates run in order of evidence: link_ip carried the mesh link, addrs
/// is what the peer says it answers on, node_ip is only the name it calls
/// itself. An address on one of our own subnets beats that order outright,
/// since the pair's cluster identity is a subnet a LAN-only box cannot route
/// to. Old daemons report neither link_ip nor addrs, leaving node_ip.
fn peer_address(p: &Value, subnets: &[(u32, u32)]) -> Option<String> {
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
    cands
        .iter()
        .find(|c| on_local_subnet(c, subnets))
        .or_else(|| cands.first())
        .cloned()
}

async fn poll_status(shared: &Arc<Shared>, addr: &str) {
    let url = format!("http://{addr}/status");
    match http_get_json(&shared.client, &url, Duration::from_secs(5)).await {
        Ok(snap) => {
            if shared.cfg.discover_peers {
                // Membership follows the mesh: every peer entry carries its
                // HTTP address (PeerHello inbound, PeerHelloOk outbound), so
                // one seed daemon reveals the rest. http_port 0 means the
                // peer daemon predates the PeerHelloOk address echo.
                let subnets = local_subnets();
                for (_, p) in snap["peers"].as_object().into_iter().flatten() {
                    let Some(port) = p["http_port"].as_u64() else {
                        continue;
                    };
                    if port == 0 {
                        continue;
                    }
                    if let Some(ip) = peer_address(p, &subnets) {
                        ensure_watched(shared, format!("{ip}:{port}"));
                    }
                }
            }
            shared.daemons.lock().unwrap().insert(
                addr.to_string(),
                DaemonView {
                    status: Some(snap),
                    seen: Some(Instant::now()),
                    error: None,
                },
            );
            shared.refresh.notify_one();
        }
        Err(e) => {
            let mut d = shared.daemons.lock().unwrap();
            let v = d.entry(addr.to_string()).or_insert(DaemonView {
                status: None,
                seen: None,
                error: None,
            });
            v.error = Some(e);
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
/// node's preferred address when that address answers again -- so a dropped
/// cable moves serving onto the LAN and a reconnected one moves it back,
/// without either transition needing an operator.
async fn prober(shared: Arc<Shared>) {
    loop {
        let table = group_table(&shared);
        {
            let mut probes = shared.probes.lock().unwrap();
            probes.retain(|k, _| {
                table
                    .get(k)
                    .map(|e| e.openai.is_some() && e.running > 0)
                    .unwrap_or(false)
            });
        }
        let mut set = tokio::task::JoinSet::new();
        for e in table.values() {
            if e.running == 0 {
                continue;
            }
            let Some(ep) = e.openai.clone().filter(|x| !x.candidates.is_empty()) else {
                continue;
            };
            let group = e.group.clone();
            let client = shared.client.clone();
            let t = shared.cfg.probe_timeout;
            // Sticky selection and the promotion clock, read before the
            // round so the probe itself holds no lock.
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
                        // An endpoint that answers but lists nothing still
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
                    // Nothing answered, so there is nothing to be routed to;
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
        tokio::select! {
            _ = tokio::time::sleep(shared.cfg.probe_interval) => {}
            _ = shared.refresh.notified() => {}
        }
    }
}

/// Probe candidates until one answers.
///
/// Order is the whole behaviour. With a sticky selection the router probes
/// that address first and only walks the list when it fails, so a working
/// route is never abandoned for a re-decision. Every `PROBE_PROMOTE_S` the
/// order is inverted for one round: candidates ranked above the sticky one
/// go first, and a success there takes the route back to the node's
/// preferred link.
///
/// Returns whether the top-ranked candidate was tried this round (which is
/// what resets the promotion clock) and either the answering URL with the
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

fn source_allowed(cfg: &Config, addr: &str) -> bool {
    prefix_allowed(&cfg.allowed_sources, addr)
}

fn prefix_allowed(allowed: &[String], addr: &str) -> bool {
    allowed.iter().any(|p| addr.starts_with(p))
}

async fn handle(
    shared: Arc<Shared>,
    peer_ip: String,
    req: Request<hyper::body::Incoming>,
) -> Response<BoxedBody> {
    if !source_allowed(&shared.cfg, &peer_ip) {
        return json_response(
            StatusCode::FORBIDDEN,
            &json!({"error": "source not permitted"}),
        );
    }
    let path = {
        let p = req.uri().path().trim_end_matches('/');
        if p.is_empty() { "/" } else { p }.to_string()
    };
    match (req.method().clone(), path.as_str()) {
        (Method::GET, "/" | "/healthz" | "/status.json") => {
            json_response(StatusCode::OK, &status_view(&shared))
        }
        (Method::GET, "/v1" | "/v1/models") => json_response(
            StatusCode::OK,
            &json!({"object": "list", "data": model_objects(&shared)}),
        ),
        (Method::POST, "/mcp") => mcp::handle(&shared, req).await,
        // Owned rather than proxied: vLLM has no such endpoint, and the path
        // lands on its /v1/responses/{response_id} pattern for a 405.
        (Method::POST, "/v1/responses/input_tokens") => tokens::count(&shared, req).await,
        // Anything else posted is routed by the model in its body. vLLM's
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
    // Just enough CLI for the Docker build's smoke test. Everything real is
    // configured by environment, like the daemon's compose file.
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("mentatd-serve {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    let cfg = Config::from_env();
    let shared = Arc::new(Shared {
        started: Instant::now(),
        client: HttpClients::new(),
        daemons: Mutex::new(HashMap::new()),
        watched: Mutex::new(HashSet::new()),
        probes: Mutex::new(HashMap::new()),
        tools: Mutex::new(HashMap::new()),
        refresh: tokio::sync::Notify::new(),
        cfg,
    });
    log(
        "serve_up",
        &[
            ("port", shared.cfg.port.to_string()),
            ("daemons", shared.cfg.daemons.join(",")),
        ],
    );
    for d in shared.cfg.daemons.clone() {
        ensure_watched(&shared, d);
    }
    {
        let shared = shared.clone();
        tokio::spawn(async move { prober(shared).await });
    }
    {
        let shared = shared.clone();
        tokio::spawn(async move { udp_listener(shared).await });
    }

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", shared.cfg.port))
        .await
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
    fn subnets() -> Vec<(u32, u32)> {
        let mask = u32::from(Ipv4Addr::new(255, 255, 255, 0));
        vec![
            (u32::from(Ipv4Addr::new(10, 0, 0, 0)), mask),
            (u32::from(Ipv4Addr::new(192, 168, 1, 0)), mask),
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
    /// connection fails, and not as a connect error. This is the failure that
    /// drops a healthy group out of the route table.
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

    /// The reported trap: ALLOWED_SOURCES lists where packets come from, the
    /// node calls itself something on another subnet, and discovery used to
    /// die on a field nothing acts on. The source still carries the day.
    #[test]
    fn an_unlisted_identity_subnet_does_not_block_discovery() {
        let allowed = vec!["192.168.1.".to_string()];
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
        let allowed = vec!["10.0.0.".to_string()];
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
        assert!(on_local_subnet("10.0.0.7", &s));
        assert!(on_local_subnet("192.168.1.13", &s));
        assert!(!on_local_subnet("10.100.0.1", &s));
        assert!(!on_local_subnet("not-an-ip", &s));
    }

    /// The reported failure: the peer calls itself by an address on a subnet
    /// this box has no route to, and the reachable one is only in addrs.
    #[test]
    fn a_reachable_addr_beats_an_unroutable_identity() {
        let p = serde_json::json!({
            "node_ip": "10.100.0.1",
            "link_ip": "10.100.0.1",
            "addrs": ["10.100.0.1", "192.168.1.11"],
        });
        assert_eq!(
            peer_address(&p, &subnets()).as_deref(),
            Some("192.168.1.11")
        );
    }

    /// With nothing on a local subnet, the link address still leads: it
    /// carried a connection, and a routed network needs no local wire.
    #[test]
    fn link_ip_leads_when_nothing_is_local() {
        let p = serde_json::json!({
            "node_ip": "10.100.0.1",
            "link_ip": "172.16.4.4",
            "addrs": ["172.16.9.9"],
        });
        assert_eq!(peer_address(&p, &subnets()).as_deref(), Some("172.16.4.4"));
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
    /// an entry carrying fields this router predates. Both reach the listing
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

    /// An advertised address is a claim. Without the allowlist check on the
    /// candidates, an announcement naming any host would have this process
    /// connect to it.
    #[test]
    fn an_advertised_address_is_still_a_claim() {
        let allowed = vec!["127.".to_string()];
        assert!(prefix_allowed(&allowed, "127.0.0.1"));
        assert!(!prefix_allowed(&allowed, "192.168.1.109"));
        assert!(!prefix_allowed(&allowed, "203.0.113.7"));
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
            "services": {},
            "services_ports": {"openai": {"port": 8000, "path": "/v1"}},
        });
        let allowed = vec!["10.".to_string(), "192.168.1.".to_string()];
        let ep = endpoint_of(&agent, "openai", &nodes, &allowed, &subnets()).unwrap();
        assert_eq!(
            ep.candidates,
            vec![
                // Local subnet first: this router shares the LAN wire and
                // not the fabric, whatever the node's own ranking says.
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
            "services": {"openai": "http://203.0.113.7:8000/v1"},
            "services_ports": {},
        });
        let ep = endpoint_of(
            &agent,
            "openai",
            &HashMap::new(),
            &["10.".to_string()],
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
            "services": {},
            "services_ports": {"openai": {"port": 8000, "path": "/v1"}},
        });
        let ep = endpoint_of(&agent, "openai", &nodes, &["172.".to_string()], &subnets()).unwrap();
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
    /// instant it answered a probe, mid-generation.
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
    /// answering and the group keeps serving on the next one.
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
    /// higher-ranked candidate first, so recovery needs no operator.
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

    /// Every candidate down must fail, and say which ones. A fall-through
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

    /// A server that answers /v1/models with one model, for the candidate
    /// tests above.
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

    #[test]
    fn an_old_daemon_reports_node_ip_alone() {
        let p = serde_json::json!({"node_ip": "192.168.1.11"});
        assert_eq!(
            peer_address(&p, &subnets()).as_deref(),
            Some("192.168.1.11")
        );
        assert_eq!(peer_address(&serde_json::json!({}), &subnets()), None);
    }
}
