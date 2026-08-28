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

    /// Send `req`, retrying once on a fresh connection if the first attempt
    /// established a connection and then failed on it.
    ///
    /// `is_connect` is the discriminator. A refused connection never got
    /// anywhere, so a retry would only fail the same way. Anything else got
    /// far enough to have been a live socket, which is the case worth a
    /// second look. The request is rebuilt rather than cloned because it was
    /// consumed, and it is safe to send twice: a SendRequest failure means
    /// the server never saw the first one.
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
    pub models: Vec<String>,
    pub seen: Instant,
    pub error: Option<String>,
}

pub struct Shared {
    pub cfg: Config,
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

/// One group as the freshest daemon views describe it.
#[derive(Clone)]
pub struct GroupEntry {
    pub group: String,
    pub daemon: String,
    pub agents_alive: usize,
    pub running: usize,
    pub openai: Option<String>,
    pub mcp: Option<String>,
}

/// Merge the daemon views into one group table. A group's agents all
/// register with one daemon (the rendezvous rule), so overlap only happens
/// around a stale view -- resolved toward the daemon with more running
/// actors.
pub fn group_table(shared: &Shared) -> BTreeMap<String, GroupEntry> {
    let stale = shared.cfg.poll_interval * 3;
    let daemons = shared.daemons.lock().unwrap();
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
            // Only the rank running the API server announces "openai". If
            // several ever do, the lexically first wins, for determinism.
            let openai = agents
                .iter()
                .filter_map(|a| a["services"]["openai"].as_str())
                .min()
                .map(str::to_string);
            // Every rank announces "mcp" (the status server runs on all of
            // them). Prefer the API node's -- it is the one with throughput
            // to report -- then lexical order.
            let mcp = agents
                .iter()
                .filter_map(|a| {
                    a["services"]["mcp"]
                        .as_str()
                        .map(|m| (a["services"]["openai"].is_null(), m))
                })
                .min()
                .map(|(_, m)| m.to_string());
            let entry = GroupEntry {
                group: name.clone(),
                daemon: addr.clone(),
                agents_alive: agents.len(),
                running,
                openai,
                mcp,
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
pub fn health_of(shared: &Shared, e: &GroupEntry) -> Result<Vec<String>, String> {
    if e.openai.is_none() {
        return Err("no announced OpenAI endpoint".into());
    }
    if e.running == 0 {
        return Err("no running actors".into());
    }
    let probes = shared.probes.lock().unwrap();
    match probes.get(&e.group) {
        None => Err("not probed yet".into()),
        Some(p) if !p.ok => Err(format!(
            "endpoint probe failed: {}",
            p.error.as_deref().unwrap_or("unknown")
        )),
        Some(p) if p.seen.elapsed() > shared.cfg.probe_fresh => Err("endpoint probe stale".into()),
        Some(p) => Ok(p.models.clone()),
    }
}

/// model name -> (group, announced base URL), healthy groups only. Names come
/// from probing the endpoint's /models, so SERVED_NAME needs no announcing.
pub fn model_table(shared: &Shared) -> BTreeMap<String, (String, String)> {
    let mut out = BTreeMap::new();
    for e in group_table(shared).values() {
        if let Ok(models) = health_of(shared, e) {
            let url = e.openai.clone().unwrap_or_default();
            for m in models {
                out.entry(m)
                    .or_insert_with(|| (e.group.clone(), url.clone()));
            }
        }
    }
    out
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
                    "openai": e.openai,
                    "mcp": e.mcp,
                    "healthy": health.is_ok(),
                    "models": health.as_ref().ok(),
                    "why_not": health.as_ref().err(),
                }),
            )
        })
        .collect();
    let models: BTreeMap<String, Value> = model_table(shared)
        .into_iter()
        .map(|(m, (g, url))| (m, json!({ "group": g, "url": url })))
        .collect();
    json!({ "daemons": daemons, "groups": groups, "models": models })
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
        let src_ip = src.ip().to_string();
        if !source_allowed(&shared.cfg, &src_ip) || !source_allowed(&shared.cfg, http_ip) {
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
        // An advertised address is a claim, so it passes the same allowlist
        // the source did. Without this an announcement could name any host
        // and have this process connect to it.
        let subnets = local_subnets();
        let pick = ranked
            .iter()
            .filter(|a| source_allowed(&shared.cfg, a))
            .find(|a| on_local_subnet(a, &subnets))
            .cloned()
            .unwrap_or_else(|| src_ip.clone());
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
            let Some(base) = e.openai.clone() else {
                continue;
            };
            let group = e.group.clone();
            let client = shared.client.clone();
            let t = shared.cfg.probe_timeout;
            set.spawn(async move {
                let url = format!("{}/models", base.trim_end_matches('/'));
                (group, http_get_json(&client, &url, t).await)
            });
        }
        while let Some(Ok((group, res))) = set.join_next().await {
            let pr = match res {
                Ok(v) => {
                    let mut models: Vec<String> = v["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|m| m["id"].as_str().map(String::from))
                        .collect();
                    if models.is_empty() {
                        // An endpoint that answers but lists nothing still
                        // serves, so fall back to the group name.
                        models.push(group.clone());
                    }
                    ProbeResult {
                        ok: true,
                        models,
                        seen: Instant::now(),
                        error: None,
                    }
                }
                Err(e) => ProbeResult {
                    ok: false,
                    models: Vec::new(),
                    seen: Instant::now(),
                    error: Some(e),
                },
            };
            let mut probes = shared.probes.lock().unwrap();
            if probes.get(&group).map(|p| p.ok) != Some(pr.ok) {
                log(
                    "group_probe",
                    &[
                        ("group", group.clone()),
                        ("ok", pr.ok.to_string()),
                        ("models", format!("{:?}", pr.models)),
                        ("error", pr.error.clone().unwrap_or_default()),
                    ],
                );
            }
            probes.insert(group, pr);
        }
        tokio::select! {
            _ = tokio::time::sleep(shared.cfg.probe_interval) => {}
            _ = shared.refresh.notified() => {}
        }
    }
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
    http_json(
        client,
        || {
            Request::builder()
                .method(Method::POST)
                .uri(url)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(body.to_string())))
                .map_err(|e| e.to_string())
        },
        t,
    )
    .await
}

async fn http_json(
    client: &HttpClients,
    build: impl Fn() -> Result<Request<Full<Bytes>>, String>,
    t: Duration,
) -> Result<Value, String> {
    let resp = client.send(build, t).await?;
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
        (Method::GET, "/v1" | "/v1/models") => {
            let data: Vec<Value> = model_table(&shared)
                .iter()
                .map(|(m, (g, _))| json!({"id": m, "object": "model", "owned_by": g}))
                .collect();
            json_response(StatusCode::OK, &json!({"object": "list", "data": data}))
        }
        (Method::POST, "/mcp") => mcp::handle(&shared, req).await,
        (Method::POST, p) if p.starts_with("/v1/") => proxy::forward(&shared, req).await,
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
        drop(first.unwrap().into_body());
        let second = pooled.request(get(&url)).await;
        let e = second
            .err()
            .expect("reusing the dead connection should fail");
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
        let second = http_get_json(&clients, &url, t).await;
        assert!(
            second.is_ok(),
            "probe over a stale pooled connection: {second:?}"
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
