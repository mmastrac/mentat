//! mentatd: the cluster daemon. Accepts client (Python shim / CLI) and agent
//! connections on the control port. In this phase there is one daemon and it
//! is its own head. The mesh and election layer slots in above these
//! handlers.

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::cfg;
use crate::proto::{read_frame, Frame, Msg};
use crate::state::{
    local_ip_toward, node_id_for, write_json_file, ActorInfo, ActorState, AgentInfo,
    BundleAssignment, ClaimInfo, ClientInfo, FrameWriter, Patch, PgInfo, PgState, RefInfo,
    RefState, Shared, SharedRef, State,
};
use mentat_common::logfmt::log;

pub struct DaemonOpts {
    pub port: u16,
    pub http_port: u16,
    pub node_ip: String,
    pub head_json: String,
    /// Control addresses of the other mentatd instances (static seed list).
    pub peers: Vec<String>,
}

/// This node's cluster identity.
///
/// A set-but-empty MENTAT_NODE_IP reads as unset. `${MENTAT_NODE_IP:-}` in a
/// compose file sets the variable to nothing, and reading that literally gave
/// every daemon deployed from the shipped file the same identity, since a
/// node id is the hash of this string and they all hashed "mentat:". Peers
/// skip a peer bearing their own id, so such a fleet never meshed.
pub fn default_node_ip() -> String {
    if let Ok(ip) = std::env::var("MENTAT_NODE_IP") {
        let ip = ip.trim();
        if !ip.is_empty() {
            return ip.to_string();
        }
    }
    // The address we'd use to reach the world. Loopback on dev boxes.
    local_ip_toward("8.8.8.8:53").unwrap_or_else(|| "127.0.0.1".to_string())
}

pub fn run(opts: DaemonOpts) -> std::io::Result<()> {
    // A daemon with no identity hashes to the same node id as every other
    // one, and a peer bearing your own id reads as yourself and skipped.
    if opts.node_ip.trim().is_empty() {
        let why = "node ip is empty: set MENTAT_NODE_IP or --node-ip to the \
                   address this node is known by";
        log("daemon_no_node_ip", &[("error", why.to_string())]);
        eprintln!("mentatd: {why}");
        std::process::exit(1);
    }
    let hostname = hostname();
    let control_addr = format!("{}:{}", opts.node_ip, opts.port);
    crate::testnet::set_node_ip(&opts.node_ip);
    let shared: SharedRef = Arc::new(Shared {
        st: std::sync::Mutex::new(State::new(
            opts.node_ip.clone(),
            hostname.clone(),
            control_addr.clone(),
        )),
        cv: std::sync::Condvar::new(),
    });
    shared.st.lock().unwrap().http_port = opts.http_port;

    // Bind before writing head.json so a reader never races an unbound port.
    let listener = TcpListener::bind(("0.0.0.0", opts.port))?;
    let _ = write_json_file(
        &opts.head_json,
        &json!({ "address": control_addr, "node_ip": opts.node_ip, "pid": std::process::id() }),
    );
    log(
        "daemon_up",
        &[
            ("addr", control_addr.clone()),
            ("node_id", shared.st.lock().unwrap().node_id.clone()),
            ("hostname", hostname),
        ],
    );

    crate::http::serve(shared.clone(), opts.http_port);
    crate::mesh::start(shared.clone(), opts.peers, opts.port, opts.http_port);
    crate::island::start(shared.clone());
    crate::announce::start(shared.clone(), opts.port, opts.http_port);

    // Lifecycle sweeper: slow-call warnings, the pending-pg timeout, the
    // agent degrade/give-up windows and the dead-actor sweep. A single
    // thread ticking every 200 ms, cheap enough that the short windows the
    // tests use still fire on time.
    {
        let shared = shared.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(200));
            sweep_lifecycle(&shared);
        });
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let shared = shared.clone();
                std::thread::spawn(move || conn_entry(shared, s));
            }
            Err(e) => log("accept_error", &[("error", e.to_string())]),
        }
    }
    Ok(())
}

/// One tick of the lifecycle sweeper.
fn sweep_lifecycle(shared: &SharedRef) {
    let now = crate::state::now_ms_u64();
    let mut st = shared.st.lock().unwrap();

    // Slow-call warnings: vLLM's run() ref legitimately never resolves, but a
    // NORMAL method call sitting pending for long means it queued behind a
    // blocking method (actors are serial, like real ray). That is the
    // likeliest way a future vLLM change breaks silently, so make it loud.
    let warn_after = cfg().slow_call_warn_ms;
    let mut warn: Vec<(String, String, u64)> = Vec::new();
    for (rid, r) in st.refs.iter_mut() {
        if matches!(r.state, RefState::Pending)
            && r.method != "run"
            && !r.warned
            && now.saturating_sub(r.created_ms) > warn_after
        {
            r.warned = true;
            warn.push((rid.clone(), r.method.clone(), now - r.created_ms));
        }
    }
    for (rid, method, age) in warn {
        log(
            "call_pending_long",
            &[
                ("ref", rid),
                ("method", method),
                ("age_ms", age.to_string()),
                (
                    "hint",
                    "queued behind a blocking method, or the worker is stuck".to_string(),
                ),
            ],
        );
    }

    // Pending-pg timeout: a placement group that never gets its agents must
    // fail loudly instead of leaving the driver waiting forever.
    let pg_timeout = cfg().pg_pending_timeout_ms;
    let timed_out: Vec<(String, String, u64, Option<String>)> = st
        .pgs
        .values()
        .filter(|p| p.state == PgState::Pending)
        .filter(|p| now.saturating_sub(p.created_ms) > pg_timeout)
        .map(|p| {
            (
                p.id.clone(),
                p.group.clone(),
                now - p.created_ms,
                p.pending_reason.clone(),
            )
        })
        .collect();
    for (pg_id, group, age, why) in timed_out {
        // The last placement attempt recorded what it could not find. Report
        // that rather than the old blanket guess about GPU counts: at four
        // nodes on two fabrics, "not enough GPUs" is usually wrong and
        // "not enough on one fabric" is usually right.
        let why =
            why.unwrap_or_else(|| format!("group '{group}' never had enough registered GPUs"));
        if let Some(pg) = st.pgs.get_mut(&pg_id) {
            pg.state = PgState::Removed;
            pg.removed_ms = Some(now);
            pg.fail_reason = Some(format!(
                "placement group still PENDING after {age}ms: {why} \
                 (MENTAT_PG_PENDING_TIMEOUT_MS)"
            ));
        }
        log(
            "pg_pending_timeout",
            &[
                ("pg", pg_id.clone()),
                ("group", group.clone()),
                ("age_ms", age.to_string()),
                ("why", why.clone()),
            ],
        );
        let row = st.pgs.get(&pg_id).map(crate::status::pg_row);
        if let Some(row) = row {
            st.emit_patch(
                "pg_timeout",
                vec![Patch::set(
                    &["groups", &group, "placement_groups", &pg_id],
                    row,
                )],
                &format!("waited {age} ms: {why}"),
            );
        }
        shared.cv.notify_all();
    }

    // Agent degrade / give-up windows.
    let degraded_after = cfg().agent_degraded_after_ms;
    let dead_after = cfg().agent_dead_after_ms;
    let mut degrade: Vec<(String, String, u64)> = Vec::new();
    let mut give_up: Vec<(String, String, u64)> = Vec::new();
    for a in st.agents.values_mut() {
        let Some(lost_at) = a.lost_at_ms else {
            continue;
        };
        let down = now.saturating_sub(lost_at);
        if down >= dead_after {
            a.lost_at_ms = None;
            give_up.push((a.id.clone(), a.group.clone(), down));
        } else if down >= degraded_after && !a.degraded {
            a.degraded = true;
            degrade.push((a.id.clone(), a.group.clone(), down));
        }
    }
    for (agent, group, down) in degrade {
        log(
            "agent_degraded",
            &[
                ("agent", agent.clone()),
                ("group", group.clone()),
                ("down_ms", down.to_string()),
            ],
        );
        let row = st
            .agents
            .get(&agent)
            .map(|a| crate::status::agent_row(&st, a));
        if let Some(row) = row {
            st.emit_patch(
                "agent_degraded",
                vec![Patch::set(&["groups", &group, "agents", &agent], row)],
                &format!("down {down} ms"),
            );
        }
    }
    for (agent, group, down) in give_up {
        let orphaned: Vec<String> = st
            .actors
            .values()
            .filter(|ac| ac.agent == agent && !matches!(ac.state, ActorState::Dead { .. }))
            .map(|ac| ac.id.clone())
            .collect();
        log(
            "agent_gave_up",
            &[
                ("agent", agent.clone()),
                ("group", group.clone()),
                ("down_ms", down.to_string()),
                ("actors", orphaned.len().to_string()),
            ],
        );
        let row = st
            .agents
            .get(&agent)
            .map(|a| crate::status::agent_row(&st, a));
        if let Some(row) = row {
            st.emit_patch(
                "agent_dead",
                vec![Patch::set(&["groups", &group, "agents", &agent], row)],
                &format!("down {down} ms, {} actors orphaned", orphaned.len()),
            );
        }
        for id in orphaned {
            mark_actor_dead(
                &mut st,
                &shared.cv,
                &id,
                &format!("agent link lost for {down}ms, past MENTAT_AGENT_DEAD_AFTER_MS"),
            );
        }
    }

    sweep_history(&mut st);
}

/// Drop the rows nobody can act on, once they are MENTAT_HISTORY_KEEP_MS
/// old: dead actors and removed placement groups whose owner is gone, and
/// agents past the give-up threshold.
///
/// A dead actor's row is what turns its owner's next call into
/// RayActorError with the reason, and a removed group's fail reason is read
/// by its owner's ready ref. Nobody else reads either. The age floor covers
/// a daemon restart, which rebuilds the tables from the agents before any
/// driver reconnects.
///
/// Record why a placement group cannot be placed, reporting a reason that
/// differs from the one already on the row.
///
/// `try_place` runs on every state change, so an unconditional event would
/// repeat one reason for as long as the group waits.
fn set_pending_reason(st: &mut State, pg_id: &str, why: String) {
    let same = st
        .pgs
        .get(pg_id)
        .is_some_and(|pg| pg.pending_reason.as_deref() == Some(why.as_str()));
    if same {
        return;
    }
    let Some(group) = st.pgs.get_mut(pg_id).map(|pg| {
        pg.pending_reason = Some(why);
        pg.group.clone()
    }) else {
        return;
    };
    let row = st.pgs.get(pg_id).map(crate::status::pg_row);
    if let Some(row) = row {
        st.emit_patch(
            "pg_pending",
            vec![Patch::set(
                &["groups", &group, "placement_groups", pg_id],
                row,
            )],
            "",
        );
    }
}

/// The row patch for one agent. Its `gpus_free` changes when a placement
/// group or a live actor on it starts or stops holding GPUs.
fn agent_patch(st: &State, agent_id: &str) -> Option<Patch> {
    let a = st.agents.get(agent_id)?;
    Some(Patch::set(
        &["groups", &a.group, "agents", agent_id],
        crate::status::agent_row(st, a),
    ))
}

/// Row patches for each agent a placement group reserves GPUs on.
fn agent_patches(st: &State, pg_id: &str) -> Vec<Patch> {
    let Some(pg) = st.pgs.get(pg_id) else {
        return Vec::new();
    };
    let mut agents: Vec<&str> = pg
        .assignment
        .iter()
        .flatten()
        .map(|b| b.agent.as_str())
        .collect();
    agents.sort();
    agents.dedup();
    agents
        .into_iter()
        .filter_map(|id| agent_patch(st, id))
        .collect()
}

/// A group is whatever agents and actors mention it, so its last row going
/// removes it from every snapshot.
fn sweep_history(st: &mut State) {
    let (now, keep) = (crate::state::now_ms_u64(), cfg().history_keep_ms);
    let aged = |at: u64| now.saturating_sub(at) > keep;

    let gone_actors: Vec<String> = st
        .actors
        .values()
        .filter(|a| !st.clients.contains_key(&a.owner))
        .filter(|a| match &a.state {
            ActorState::Dead { at_ms, .. } => aged(*at_ms),
            _ => false,
        })
        .map(|a| a.id.clone())
        .collect();
    let gone_pgs: Vec<String> = st
        .pgs
        .values()
        .filter(|p| p.state == PgState::Removed && !st.clients.contains_key(&p.owner))
        .filter(|p| p.removed_ms.is_some_and(aged))
        .map(|p| p.id.clone())
        .collect();
    // An agent goes only after every actor it hosted, since a dead actor's
    // row gives its agent.
    let gone_agents: Vec<String> = st
        .agents
        .values()
        .filter(|a| !a.alive && a.gone_since_ms.is_some_and(aged))
        .filter(|a| {
            !st.actors
                .values()
                .any(|ac| ac.agent == a.id && !gone_actors.contains(&ac.id))
        })
        .map(|a| a.id.clone())
        .collect();
    if gone_actors.is_empty() && gone_pgs.is_empty() && gone_agents.is_empty() {
        return;
    }
    let before = st.refs.len();
    // Collect the paths first. Each row holds the group its path needs.
    let mut gone: Vec<Patch> = Vec::new();
    let mut touched_groups: Vec<String> = Vec::new();
    for id in &gone_actors {
        if let Some(a) = st.actors.get(id) {
            gone.push(Patch::remove(&["groups", &a.group, "actors", id]));
            touched_groups.push(a.group.clone());
        }
    }
    for id in &gone_pgs {
        if let Some(p) = st.pgs.get(id) {
            gone.push(Patch::remove(&["groups", &p.group, "placement_groups", id]));
        }
    }
    for id in &gone_agents {
        if let Some(a) = st.agents.get(id) {
            gone.push(Patch::remove(&["groups", &a.group, "agents", id]));
            touched_groups.push(a.group.clone());
        }
    }
    for id in &gone_actors {
        st.actors.remove(id);
    }
    for id in &gone_pgs {
        st.pgs.remove(id);
    }
    for id in &gone_agents {
        st.agents.remove(id);
        st.misfiled_warned.remove(id);
    }
    st.refs
        .retain(|_, r| r.actor.as_ref().is_none_or(|a| st.actors.contains_key(a)));
    // A snapshot builds `groups` from the agents and actors that mention
    // one, so a group whose last row went is gone from the snapshot too.
    let mut emptied: Vec<String> = touched_groups
        .into_iter()
        .filter(|g| {
            !st.agents.values().any(|a| &a.group == g) && !st.actors.values().any(|a| &a.group == g)
        })
        .collect();
    emptied.sort();
    emptied.dedup();
    for g in emptied {
        gone.push(Patch::remove(&["groups", &g]));
    }
    st.emit_patch("history_swept", gone, &format!("kept {keep} ms"));
    log(
        "history_swept",
        &[
            ("actors", gone_actors.len().to_string()),
            ("placement_groups", gone_pgs.len().to_string()),
            ("agents", gone_agents.len().to_string()),
            ("refs", (before - st.refs.len()).to_string()),
            ("kept_ms", keep.to_string()),
        ],
    );
}

pub fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn set_keepalive(stream: &TcpStream) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    unsafe {
        let one: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        // Aggressive-ish probing on Linux: a wedged peer is declared dead in
        // ~MENTAT_TCP_DEAD_AFTER_MS (default 75s) instead of the kernel
        // default of >2h. wait_for_init blocks for ~10 minutes legitimately,
        // but that's an idle-with-live-peer case, which keepalive handles
        // correctly. The target splits as idle + 3 probes: 2/5 + 3*(1/5).
        #[cfg(target_os = "linux")]
        {
            let total_s = (cfg().tcp_dead_after_ms / 1000).max(5) as libc::c_int;
            let cnt: libc::c_int = 3;
            let intvl: libc::c_int = (total_s / 5).max(1);
            let idle: libc::c_int = (total_s - cnt * intvl).max(1);
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPINTVL,
                &intvl as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPCNT,
                &cnt as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

fn conn_entry(shared: SharedRef, stream: TcpStream) {
    set_keepalive(&stream);
    let peer_ip = stream
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let writer = FrameWriter::new(match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    });
    let mut reader = BufReader::new(stream);

    let first = match read_frame(&mut reader) {
        Ok(Some(f)) => f,
        _ => return,
    };
    match first.0.msg {
        Msg::Hello { .. } | Msg::AgentRegister { .. } => match head_for(&shared) {
            Head::Here => match first.0.msg {
                Msg::Hello { .. } => client_conn(shared, reader, writer, peer_ip, first.0),
                _ => agent_conn(shared, reader, writer, peer_ip, first),
            },
            Head::At(addr) => {
                // A client that claimed no box is on this one. The relay's
                // source address may be one this box does not announce, so
                // the head is told outright.
                let mut first = first;
                let mine = crate::announce::all_local_addrs();
                let local = crate::state::is_loopback(&peer_ip) || mine.contains(&peer_ip);
                if local {
                    let my_ip = shared.st.lock().unwrap().node_ip.clone();
                    match &mut first.0.msg {
                        Msg::Hello { node_ip, .. } if node_ip.is_empty() => *node_ip = my_ip,
                        Msg::AgentRegister { node_ip, .. } if node_ip.is_empty() => {
                            *node_ip = my_ip
                        }
                        _ => {}
                    }
                }
                relay(&shared, reader, addr, first)
            }
            Head::None => {
                let _ = writer.send(
                    Msg::refused("no_head", "no head elected yet, retry"),
                    first.0.req,
                    &[],
                );
            }
        },
        Msg::PeerHello { .. } => crate::mesh::accept_peer(shared, reader, writer, peer_ip, first),
        // A probe gets its own connection, since the question is whether
        // this address pair holds traffic. The node id in the reply gives
        // the address belongs to the node the prober meant. The prober owns
        // the result.
        Msg::Probe { .. } => {
            let my_id = shared.st.lock().unwrap().node_id.clone();
            let _ = writer.send(
                Msg::ProbeOk {
                    proto: crate::proto::proto(),
                    node_id: my_id,
                },
                first.0.req,
                &[],
            );
        }
        other => {
            let _ = writer.send(
                Msg::refused(
                    "bad_first_frame",
                    format!(
                        "first frame must be hello, agent_register, peer_hello or probe, got {other:?}"
                    ),
                ),
                first.0.req,
                &[],
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The head, and the relay to it
// ---------------------------------------------------------------------------

enum Head {
    Here,
    /// Addresses to try, best first.
    At(Vec<String>),
    None,
}

/// Where a registration or driver session belongs. Every group lives on
/// the head, so a container points at the daemon on its own box and still
/// lands with every other rank. A connection that arrives before the first
/// election waits here.
fn head_for(shared: &SharedRef) -> Head {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut st = shared.st.lock().unwrap();
    loop {
        if !st.head_node_id.is_empty() {
            if st.head_node_id == st.node_id {
                return Head::Here;
            }
            // The link's own address held a connection, which the
            // control address on a multi-homed peer may not have.
            return match st.peers.get(&st.head_node_id).filter(|p| p.alive) {
                Some(p) => {
                    let port = p.control_port;
                    let mut at = vec![
                        format!("{}:{port}", p.link_ip),
                        format!("{}:{port}", p.node_ip),
                    ];
                    at.extend(p.addrs.iter().map(|a| format!("{a}:{port}")));
                    at.dedup();
                    Head::At(at)
                }
                None => Head::None,
            };
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Head::None;
        }
        st = shared.cv.wait_timeout(st, left).unwrap().0;
    }
}

/// Pipe one connection to the head, first frame included, until either
/// side closes.
fn relay(
    shared: &SharedRef,
    reader: BufReader<TcpStream>,
    head: Vec<String>,
    first: (Frame, Vec<u8>),
) {
    let net = crate::testnet::load();
    let up = head.iter().find_map(|h| {
        let target = net
            .as_ref()
            .and_then(|n| n.real(h.rsplit_once(':').map(|(host, _)| host).unwrap_or(h)))
            .unwrap_or_else(|| h.clone());
        target
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.next())
            .and_then(|a| TcpStream::connect_timeout(&a, Duration::from_secs(5)).ok())
    });
    let Some(mut up) = up else {
        log("relay_failed", &[("head", head.join(","))]);
        return;
    };
    set_keepalive(&up);
    if crate::proto::write_frame(&mut up, &first.0, &first.1).is_err()
        || up.write_all(reader.buffer()).is_err()
    {
        return;
    }
    shared.st.lock().unwrap().counters.relayed += 1;
    let down = reader.into_inner();
    let (Ok(mut down2), Ok(mut up2)) = (down.try_clone(), up.try_clone()) else {
        return;
    };
    let mut down_r = down;
    let mut up_w = up;
    let pump = std::thread::spawn(move || {
        let _ = std::io::copy(&mut down_r, &mut up_w);
        let _ = up_w.shutdown(std::net::Shutdown::Both);
    });
    let _ = std::io::copy(&mut up2, &mut down2);
    let _ = down2.shutdown(std::net::Shutdown::Both);
    let _ = pump.join();
}

/// Called when the head changes. A daemon that has stopped being head
/// drops every group it held. Its agent links close so the agents re-register
/// through the relay, and its driver rows go without a reap, since the
/// actors are alive and the new head adopts them.
pub fn head_moved(st: &mut State, old: &str) {
    if st.head_node_id == st.node_id || (st.agents.is_empty() && st.clients.is_empty()) {
        return;
    }
    let reason = format!("group moved to head {}", st.head_node_id);
    log(
        "groups_moved",
        &[
            ("from", old.to_string()),
            ("to", st.head_node_id.clone()),
            ("agents", st.agents.len().to_string()),
            ("clients", st.clients.len().to_string()),
        ],
    );
    for a in st.agents.values() {
        a.writer.shutdown();
    }
    for (_, w) in st.client_links.drain(..) {
        w.shutdown();
    }
    // The rows go in one event, so a consumer applying it lands where a
    // consumer re-reading /status lands.
    let mut patch: Vec<Patch> = st
        .clients
        .keys()
        .map(|id| Patch::remove(&["clients", id]))
        .chain(
            st.claims
                .keys()
                .map(|(g, name)| Patch::remove(&["groups", g, "claims", name])),
        )
        .collect();
    st.clients.clear();
    st.claims.clear();
    st.refs.clear();
    let now = crate::state::now_ms_u64();
    let mut touched: Vec<(String, String, bool)> = Vec::new();
    for a in st.actors.values_mut() {
        if !matches!(a.state, ActorState::Dead { .. }) {
            a.state = ActorState::Dead {
                reason: reason.clone(),
                at_ms: now,
            };
            touched.push((a.group.clone(), a.id.clone(), true));
        }
    }
    for pg in st.pgs.values_mut() {
        if pg.state != PgState::Removed {
            pg.state = PgState::Removed;
            pg.removed_ms = Some(now);
            touched.push((pg.group.clone(), pg.id.clone(), false));
        }
    }
    // Releasing the groups frees their GPUs, so the agent rows moved too.
    for (_, id, is_actor) in &touched {
        if !is_actor {
            patch.extend(agent_patches(st, id));
        }
    }
    for (group, id, is_actor) in touched {
        let entry = if is_actor {
            st.actors.get(&id).map(|a| {
                (
                    ["groups", &group, "actors", &id],
                    crate::status::actor_row(a),
                )
            })
        } else {
            st.pgs.get(&id).map(|p| {
                (
                    ["groups", &group, "placement_groups", &id],
                    crate::status::pg_row(p),
                )
            })
        };
        if let Some((at, row)) = entry {
            patch.push(Patch::set(&at, row));
        }
    }
    st.emit_patch("head_moved", patch, &reason);
}

// ---------------------------------------------------------------------------
// Client connections
// ---------------------------------------------------------------------------

/// The live actors in `group` that a new driver session for it replaces.
///
/// A group holds one driver session, so an actor whose owner holds none
/// belongs to a driver that is gone. An adopted actor has no reap pending,
/// so killing it here is the only thing that frees its GPUs.
fn orphans_of(st: &State, group: &str, client_id: &str) -> Vec<String> {
    st.actors
        .values()
        .filter(|a| a.group == group && a.owner != client_id)
        .filter(|a| !matches!(a.state, ActorState::Dead { .. }))
        .filter(|a| !st.clients.get(&a.owner).is_some_and(|c| c.has_session))
        .map(|a| a.id.clone())
        .collect()
}

fn client_conn(
    shared: SharedRef,
    mut reader: BufReader<TcpStream>,
    writer: FrameWriter,
    peer_ip: String,
    hello: Frame,
) {
    let (client_id, is_session, orphans) = {
        let Msg::Hello {
            proto,
            client_id,
            group,
            session,
            kind,
            node_ip: claimed,
        } = hello.msg
        else {
            unreachable!()
        };
        if !crate::proto::major_matches(&proto) {
            let _ = writer.send(
                Msg::Err {
                    error: format!("proto {} here, {proto} offered", crate::proto::PROTO),
                    code: "proto_mismatch".into(),
                    head: String::new(),
                    proto: crate::proto::PROTO.to_string(),
                },
                hello.req,
                &[],
            );
            return;
        }
        let mut st = shared.st.lock().unwrap();

        if session {
            let dup = st
                .clients
                .values()
                .any(|c| c.group == group && c.has_session && c.id != client_id);
            if dup {
                let _ = writer.send(
                    Msg::refused(
                        "duplicate_session",
                        format!(
                            "group '{group}' already has an active driver session. \
                             Run a second instance under a different MENTAT_GROUP"
                        ),
                    ),
                    hello.req,
                    &[],
                );
                return;
            }
        }

        let node_id = client_node(
            &st,
            if claimed.is_empty() {
                &peer_ip
            } else {
                &claimed
            },
        );

        let orphans = if session {
            orphans_of(&st, &group, &client_id)
        } else {
            Vec::new()
        };

        let entry = st
            .clients
            .entry(client_id.clone())
            .or_insert_with(|| ClientInfo {
                id: client_id.clone(),
                group: group.clone(),
                kind: kind.clone(),
                node_id: node_id.clone(),
                has_session: false,
            });
        entry.group = group.clone();
        if session {
            entry.has_session = true;
        }
        let node_id = entry.node_id.clone();
        st.counters.clients_total += 1;
        let head = st.head_node_id.clone();
        let node_ip = st.node_ip.clone();
        let gcs = st.control_addr.clone();
        if session {
            let row = st.clients.get(&client_id).map(crate::status::client_row);
            if let Some(row) = row {
                st.emit_patch(
                    "driver_connected",
                    vec![Patch::set(&["clients", &client_id], row)],
                    "",
                );
            }
        }
        st.client_links.push((client_id.clone(), writer.clone()));
        let _ = writer.send(
            Msg::HelloOk {
                proto: crate::proto::proto(),
                node_id,
                node_ip,
                control_addr: gcs,
                head_node_id: head,
            },
            hello.req,
            &[],
        );
        log(
            "client_conn_open",
            &[
                ("client", client_id.clone()),
                ("kind", kind.clone()),
                ("session", session.to_string()),
                ("peer", peer_ip.clone()),
            ],
        );
        (client_id, session, orphans)
    };
    for id in &orphans {
        kill_actor(&shared, id, "a new driver session took the group");
    }

    loop {
        let (frame, payload) = match read_frame(&mut reader) {
            Ok(Some(fp)) => fp,
            Ok(None) => break,
            Err(e) => {
                log(
                    "client_read_error",
                    &[("client", client_id.clone()), ("error", e.to_string())],
                );
                break;
            }
        };
        let req = frame.req;
        let (resp, resp_payload) =
            handle_client_msg(&shared, &client_id, frame.msg, &frame.unknown_t, payload);
        if writer.send(resp, req, &resp_payload).is_err() {
            break;
        }
    }

    log(
        "client_conn_closed",
        &[
            ("client", client_id.clone()),
            ("session", is_session.to_string()),
        ],
    );
    let moved = {
        let mut st = shared.st.lock().unwrap();
        st.client_links
            .retain(|(_, w)| !FrameWriter::same_socket(w, &writer));
        // A session closing on a daemon that is not the head was cut by a
        // head change. Its actors are alive and belong to the new head.
        st.head_node_id != st.node_id
    };
    if is_session && !moved {
        reap_client(&shared, &client_id);
    } else {
        // A driver's worker threads share its session's row, which leaves
        // with the session. A CLI connection's row leaves with it.
        let mut st = shared.st.lock().unwrap();
        if st
            .clients
            .get(&client_id)
            .is_some_and(|c| !c.has_session || moved)
        {
            st.clients.remove(&client_id);
        }
    }
}

fn handle_client_msg(
    shared: &SharedRef,
    client_id: &str,
    msg: Msg,
    unknown_t: &str,
    payload: Vec<u8>,
) -> (Msg, Vec<u8>) {
    match msg {
        Msg::Nodes => {
            let st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            // One entry per distinct node hosting this group's agents, plus
            // the daemon's own node so the driver always finds itself.
            let mut nodes: BTreeMap<String, Value> = BTreeMap::new();
            nodes.insert(
                st.node_id.clone(),
                node_entry(&st.node_id, &st.node_ip, 0.0, 8.0, 0.0),
            );
            for a in st.agents.values().filter(|a| a.alive && a.group == group) {
                let e = nodes.entry(a.node_id.clone()).or_insert_with(|| {
                    node_entry(&a.node_id, &a.node_ip, 0.0, a.machine.cpus as f64, 0.0)
                });
                if let Some(res) = e.get_mut("Resources").and_then(|r| r.as_object_mut()) {
                    let g = res.get("GPU").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    res.insert("GPU".into(), json!(g + a.machine.gpus.len() as f64));
                    res.insert("CPU".into(), json!(a.machine.cpus as f64));
                    res.insert("memory".into(), json!(a.machine.memory as f64));
                }
            }
            (
                Msg::NodesOk {
                    nodes: nodes.into_values().collect(),
                },
                Vec::new(),
            )
        }
        Msg::Resources => {
            let st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            let mut res: BTreeMap<String, f64> = BTreeMap::new();
            let mut gpu = 0.0;
            let mut cpu = 0.0;
            let mut mem = 0.0;
            for a in st.agents.values().filter(|a| a.alive && a.group == group) {
                gpu += a.machine.gpus.len() as f64;
                cpu += a.machine.cpus as f64;
                mem += a.machine.memory as f64;
            }
            res.insert("GPU".into(), gpu);
            res.insert("CPU".into(), cpu);
            res.insert("memory".into(), mem);
            res.insert("object_store_memory".into(), 0.0);
            (Msg::ResourcesOk { resources: res }, Vec::new())
        }
        Msg::Available => {
            let st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            let mut nodes: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
            for a in st.agents.values().filter(|a| a.alive && a.group == group) {
                let free = st.free_gpus_of(&a.id).len() as f64;
                let e = nodes.entry(a.node_id.clone()).or_default();
                *e.entry("GPU".to_string()).or_insert(0.0) += free;
                e.insert("CPU".to_string(), a.machine.cpus as f64);
                // Memory is never reserved, so the free figure is the total.
                e.insert("memory".to_string(), a.machine.memory as f64);
                e.insert(format!("node:{}", a.node_ip), 1.0);
            }
            (Msg::AvailableOk { nodes }, Vec::new())
        }
        Msg::Claim { name, shape } => {
            let mut st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            match claim(&mut st, client_id, &group, &name, &shape) {
                Ok((generation, view)) => (
                    Msg::ClaimOk {
                        name,
                        generation,
                        head_node_id: st.head_node_id.clone(),
                        view,
                    },
                    Vec::new(),
                ),
                Err(refusal) => (refusal, Vec::new()),
            }
        }
        Msg::PgCreate {
            bundles,
            strategy,
            claim,
        } => {
            let mut st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            // Placement packs: it fills one agent before moving to the next,
            // inside one island. A caller requesting a spread gets a pack, so
            // the difference is logged rather than left silent. See "Placement"
            // in PROTOCOL.md.
            if !strategy.is_empty() && !strategy.contains("PACK") {
                log(
                    "pg_strategy_ignored",
                    &[
                        ("group", group.clone()),
                        ("strategy", strategy.clone()),
                        ("using", "PACK".to_string()),
                    ],
                );
            }
            let pg_id = crate::state::new_pg_id();
            let n = bundles.len();
            st.pgs.insert(
                pg_id.clone(),
                PgInfo {
                    id: pg_id.clone(),
                    group: group.clone(),
                    owner: client_id.to_string(),
                    bundles,
                    strategy,
                    claim,
                    assignment: vec![None; n],
                    state: PgState::Pending,
                    created_ms: crate::state::now_ms_u64(),
                    fail_reason: None,
                    island: None,
                    pending_reason: None,
                    removed_ms: None,
                },
            );
            let row = st.pgs.get(&pg_id).map(crate::status::pg_row);
            if let Some(row) = row {
                st.emit_patch(
                    "pg_created",
                    vec![Patch::set(
                        &["groups", &group, "placement_groups", &pg_id],
                        row,
                    )],
                    "",
                );
            }
            try_place(&mut st, &shared.cv);
            (
                // The id is also the handle `ref_get` resolves once the
                // group is CREATED, so there is no separate ready ref.
                Msg::PgCreateOk { pg_id },
                Vec::new(),
            )
        }
        Msg::PgTable { pg_id } => {
            let st = shared.st.lock().unwrap();
            match st.pgs.get(&pg_id) {
                None => err(format!("no such placement group {pg_id}")),
                Some(pg) => {
                    let mut bundles = serde_json::Map::new();
                    let mut b2n = serde_json::Map::new();
                    for (i, spec) in pg.bundles.iter().enumerate() {
                        bundles.insert(i.to_string(), json!({ "GPU": spec }));
                        if let Some(Some(a)) = pg.assignment.get(i) {
                            b2n.insert(i.to_string(), json!(a.node_id));
                        }
                    }
                    let state = match pg.state {
                        PgState::Pending => "PENDING",
                        PgState::Created => "CREATED",
                        PgState::Removed => "REMOVED",
                    };
                    (
                        Msg::PgTableOk {
                            table: json!({
                                "placement_group_id": pg.id,
                                "name": "",
                                "bundles": bundles,
                                "bundles_to_node_id": b2n,
                                "strategy": pg.strategy,
                                "state": state,
                                "stats": {},
                            }),
                        },
                        Vec::new(),
                    )
                }
            }
        }
        Msg::PgRemove { pg_id } => {
            let mut st = shared.st.lock().unwrap();
            let group = st.pgs.get(&pg_id).map(|pg| pg.group.clone());
            if let Some(pg) = st.pgs.get_mut(&pg_id) {
                pg.state = PgState::Removed;
                pg.removed_ms = Some(crate::state::now_ms_u64());
            }
            // The row stays REMOVED until the history sweep drops it, so
            // this sets it rather than removing the path.
            let row = st.pgs.get(&pg_id).map(crate::status::pg_row);
            if let (Some(group), Some(row)) = (group, row) {
                let mut patch = vec![Patch::set(
                    &["groups", &group, "placement_groups", &pg_id],
                    row,
                )];
                patch.extend(agent_patches(&st, &pg_id));
                st.emit_patch("pg_removed", patch, "");
            }
            try_place(&mut st, &shared.cv);
            (Msg::Ok, Vec::new())
        }
        Msg::ActorCreate {
            name,
            num_gpus,
            pg_id,
            bundle_index,
            env,
        } => create_actor(
            shared,
            client_id,
            name,
            num_gpus,
            pg_id,
            bundle_index,
            env,
            payload,
        ),
        Msg::ActorCall { actor_id, method } => {
            let mut st = shared.st.lock().unwrap();
            st.counters.calls_total += 1;
            let ref_id = st.new_ref_id(&actor_id);
            let Some(actor) = st.actors.get(&actor_id) else {
                return err(format!("no such actor {actor_id}"));
            };
            let new_ref = |state: RefState| RefInfo {
                state,
                actor: Some(actor_id.clone()),
                owner: client_id.to_string(),
                method: method.clone(),
                created_ms: crate::state::now_ms_u64(),
                warned: false,
            };
            match &actor.state {
                ActorState::Dead { reason, .. } => {
                    let r = new_ref(RefState::ActorDied {
                        reason: reason.clone(),
                    });
                    st.refs.insert(ref_id.clone(), r);
                }
                _ => {
                    let agent_writer = st
                        .agents
                        .get(&actor.agent)
                        .filter(|a| a.alive)
                        .map(|a| a.writer.clone());
                    let agent_known = st.agents.contains_key(&actor.agent);
                    match agent_writer {
                        // Agent link is down but inside the degrade window
                        // (the actor would be Dead otherwise): hold the call,
                        // drained in order when the agent re-registers.
                        None if agent_known => {
                            let r = new_ref(RefState::Pending);
                            st.refs.insert(ref_id.clone(), r);
                            log(
                                "call_held",
                                &[("ref", ref_id.clone()), ("actor", actor_id.clone())],
                            );
                            if let Some(a) = st.actors.get_mut(&actor_id) {
                                a.queued_calls.push((ref_id.clone(), method, payload));
                            }
                        }
                        None => {
                            let r = new_ref(RefState::ActorDied {
                                reason: format!("agent {} is not registered", actor.agent),
                            });
                            st.refs.insert(ref_id.clone(), r);
                        }
                        Some(w) => {
                            let r = new_ref(RefState::Pending);
                            st.refs.insert(ref_id.clone(), r);
                            let send_res = w.send(
                                Msg::ActorDispatch {
                                    actor_id: actor_id.clone(),
                                    ref_id: ref_id.clone(),
                                    method: method.clone(),
                                },
                                0,
                                &payload,
                            );
                            if send_res.is_err() {
                                // The link is dying under us. The EOF handler
                                // and degrade window handle it from here.
                                log(
                                    "call_held",
                                    &[("ref", ref_id.clone()), ("actor", actor_id.clone())],
                                );
                                if let Some(a) = st.actors.get_mut(&actor_id) {
                                    a.queued_calls.push((ref_id.clone(), method, payload));
                                }
                            }
                        }
                    }
                }
            }
            shared.cv.notify_all();
            (Msg::ActorCallOk { ref_id }, Vec::new())
        }
        Msg::RefGet { ref_id, timeout_ms } => do_get(shared, &ref_id, timeout_ms),
        Msg::RefWait {
            ref_ids,
            num_returns,
            timeout_ms,
        } => do_wait(shared, &ref_ids, num_returns, timeout_ms),
        Msg::ActorKill { actor_id } => {
            kill_actor(shared, &actor_id, "ray.kill");
            (Msg::Ok, Vec::new())
        }
        Msg::Status { group } => {
            let st = shared.st.lock().unwrap();
            (
                Msg::StatusOk {
                    snapshot: crate::status::snapshot(&st, group.as_deref()),
                },
                Vec::new(),
            )
        }
        Msg::ActorStop { group, all } => {
            if group.is_empty() == !all {
                // Neither leaves the scope unsaid and both contradict. The
                // wide form has to be requested: this binary is also installed as
                // `ray`, where an inherited `ray stop` in an entrypoint would
                // otherwise reach every group on the cluster.
                let st = shared.st.lock().unwrap();
                let mut groups: Vec<&str> = st.actors.values().map(|a| a.group.as_str()).collect();
                groups.sort();
                groups.dedup();
                return err(format!(
                    "actor_stop takes a group or all. Groups: {}",
                    if groups.is_empty() {
                        "none".to_string()
                    } else {
                        groups.join(", ")
                    }
                ));
            }
            let ids: Vec<String> = {
                let st = shared.st.lock().unwrap();
                st.actors
                    .values()
                    .filter(|a| all || a.group == group)
                    .filter(|a| !matches!(a.state, ActorState::Dead { .. }))
                    .map(|a| a.id.clone())
                    .collect()
            };
            for id in &ids {
                kill_actor(shared, id, "mentat stop");
            }
            (Msg::Ok, Vec::new())
        }
        // `Msg::Unknown` is a unit variant, so the tag the frame kept is the
        // only thing that reports what was refused.
        Msg::Unknown => refused(
            "unknown_message",
            format!("no such message type {unknown_t:?}"),
        ),
        other => err(format!("unexpected client message: {other:?}")),
    }
}

/// Every node a claim placed, across all its sets.
fn claimed_nodes(view: &Value) -> Vec<String> {
    let mut out: Vec<String> = view["sets"]
        .as_object()
        .into_iter()
        .flatten()
        .flat_map(|(_, members)| members.as_array().into_iter().flatten())
        .filter_map(|m| m["node"].as_str().map(str::to_string))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Solve a claim on `name` the first time, and repeat that view to every
/// later holder.
///
/// The name is the reservation. A second holder of one name is not a second
/// placement, so ranks that claim the same name agree on their nodes without
/// a coordinator. A holder that requests a different shape under a name
/// already held is refused: re-solving would move nodes under whoever
/// claimed first.
///
/// Only the head replies. Two daemons solving the same name against their
/// own views could each hand out a placement, and islands are deliberately
/// soft-consistent between daemons. The error gives the head so a caller can
/// go there.
// The refusal is a `Msg` so it can hold `code` and `head`. `Msg` is a wide
// enum, and one claim per driver makes the size irrelevant here.
#[allow(clippy::result_large_err)]
fn claim(
    st: &mut State,
    client_id: &str,
    group: &str,
    name: &str,
    shape: &Value,
) -> Result<(u64, Value), Msg> {
    if name.trim().is_empty() {
        return Err(Msg::refused("bad_claim_name", "a claim needs a name"));
    }
    if st.head_node_id != st.node_id {
        let addr = st
            .peers
            .values()
            .find(|p| p.node_id == st.head_node_id)
            .map(|p| format!("{}:{}", p.node_ip, p.control_port))
            .unwrap_or_default();
        // The caller retries against the head, so the address goes in a
        // field rather than only in the sentence.
        return Err(Msg::Err {
            error: format!(
                "this node is not the head. Send claims to {} at {addr}",
                st.head_node_id
            ),
            code: "not_head".into(),
            head: addr,
            proto: String::new(),
        });
    }
    let key = (group.to_string(), name.to_string());
    if let Some(c) = st.claims.get_mut(&key) {
        // Both are canonical, so `[1]` and `[1.0]` match here.
        if c.shape != crate::claim::canonical(shape) {
            return Err(Msg::refused(
                "shape_conflict",
                format!("claim {name:?} is held for a different shape. Use another name"),
            ));
        }
        c.holders.insert(client_id.to_string());
        return Ok((c.generation, c.view.clone()));
    }
    let req = crate::claim::parse(shape).map_err(|e| Msg::refused("bad_shape", e))?;
    let topo = crate::claim::topology(st);
    let solution =
        crate::claim::solve(&topo, &req).map_err(|e| Msg::refused("unsolvable_claim", e))?;
    st.claim_generation += 1;
    let generation = st.claim_generation;
    let view = solution.to_json(&topo);
    st.claims.insert(
        key,
        ClaimInfo {
            shape: crate::claim::canonical(shape),
            view: view.clone(),
            generation,
            holders: [client_id.to_string()].into_iter().collect(),
        },
    );
    let row = {
        let c = &st.claims[&(group.to_string(), name.to_string())];
        crate::status::claim_row(c)
    };
    st.emit_patch(
        "claim_solved",
        vec![Patch::set(&["groups", group, "claims", name], row)],
        "",
    );
    Ok((generation, view))
}

/// Drop one hold. The claim goes with its last holder, which is how a driver
/// that dies gives its nodes back.
fn release(st: &mut State, client_id: &str, key: &(String, String)) {
    let Some(c) = st.claims.get_mut(key) else {
        return;
    };
    c.holders.remove(client_id);
    let (group, name) = (key.0.clone(), key.1.clone());
    if c.holders.is_empty() {
        st.claims.remove(key);
        st.emit_patch(
            "claim_released",
            vec![Patch::remove(&["groups", &group, "claims", &name])],
            "",
        );
    } else {
        let row = crate::status::claim_row(&st.claims[key]);
        st.emit_patch(
            "claim_released",
            vec![Patch::set(&["groups", &group, "claims", &name], row)],
            "",
        );
    }
}

/// Drop every claim this client held. This runs where its other resources
/// are reaped, so a disconnect does not need an explicit release.
fn release_all(st: &mut State, client_id: &str) {
    let keys: Vec<(String, String)> = st.claims.keys().cloned().collect();
    for k in keys {
        release(st, client_id, &k);
    }
}

/// Whether an agent claimed an address the mesh files under another node.
///
/// A node id is the hash of an address, so a container told to call itself
/// by its LAN address registers a node distinct from the box it runs on. Its
/// GPUs land on a node no island contains, the group never opts into fabric
/// placement, and the ranks talk over whichever link the address named. That
/// reads as a slow model rather than as a misconfiguration.
///
/// Advisory only, like the agent's own bind finding. A refusal here would
/// stop a deployment that is running, badly, and being told which address to
/// use is the operator's job to correct.
fn misfiled_node(st: &State, node_ip: &str, node_id: &str) -> Option<String> {
    let mut boxes: Vec<(String, String, Vec<String>)> = vec![(
        st.node_id.clone(),
        st.hostname.clone(),
        crate::announce::local_addrs(),
    )];
    for p in st.peers.values().filter(|p| p.alive) {
        boxes.push((p.node_id.clone(), p.node_ip.clone(), p.addrs.clone()));
    }
    misfiled(node_ip, node_id, &boxes)
}

/// The matching half, over what the mesh knows of each box.
fn misfiled(
    node_ip: &str,
    node_id: &str,
    boxes: &[(String, String, Vec<String>)],
) -> Option<String> {
    if node_ip.is_empty() || crate::state::is_loopback(node_ip) {
        return None;
    }
    let (owner_id, owner_name, _) = boxes
        .iter()
        .find(|(_, _, addrs)| addrs.iter().any(|a| a == node_ip))?;
    if owner_id == node_id {
        return None;
    }
    Some(format!(
        "claimed {node_ip}, an address of {owner_name}, which this daemon knows \
         as a different node. Its GPUs would join no island and its group \
         would take no fabric"
    ))
}

/// The node a client connected from: this box for loopback or any address
/// it owns, then the mesh peer or registered agent that announces the
/// address, then a node of the address's own. A driver arriving over the
/// fabric would otherwise be filed away from its own agents, and rank 0
/// would land away from the engine.
fn client_node(st: &State, peer_ip: &str) -> String {
    if crate::state::is_loopback(peer_ip)
        || peer_ip == st.node_ip
        || crate::announce::all_local_addrs()
            .iter()
            .any(|a| a == peer_ip)
    {
        return st.node_id.clone();
    }
    if let Some(p) = st
        .peers
        .values()
        .find(|p| p.node_ip == peer_ip || p.addrs.iter().any(|a| a == peer_ip))
    {
        return p.node_id.clone();
    }
    st.agents
        .values()
        .find(|a| a.node_ip == peer_ip)
        .map(|a| a.node_id.clone())
        .unwrap_or_else(|| node_id_for(peer_ip))
}

fn client_group(st: &State, client_id: &str) -> String {
    st.clients
        .get(client_id)
        .map(|c| c.group.clone())
        .unwrap_or_else(|| "default".to_string())
}

fn node_entry(node_id: &str, ip: &str, gpus: f64, cpus: f64, memory: f64) -> Value {
    json!({
        "NodeID": node_id,
        "NodeManagerAddress": ip,
        "Alive": true,
        "Resources": {
            "GPU": gpus,
            "CPU": cpus,
            "memory": memory,
            "object_store_memory": 0.0,
            format!("node:{ip}"): 1.0,
        },
    })
}

fn err(e: String) -> (Msg, Vec<u8>) {
    (Msg::error(e), Vec::new())
}

/// A refusal a program can match on, for the cases a caller acts on.
fn refused(code: &str, e: String) -> (Msg, Vec<u8>) {
    (Msg::refused(code, e), Vec::new())
}

#[allow(clippy::too_many_arguments)]
fn create_actor(
    shared: &SharedRef,
    client_id: &str,
    name: String,
    num_gpus: u32,
    pg_id: String,
    bundle_index: usize,
    env: BTreeMap<String, String>,
    payload: Vec<u8>,
) -> (Msg, Vec<u8>) {
    let mut st = shared.st.lock().unwrap();
    let group = client_group(&st, client_id);

    let dup = st
        .actors
        .values()
        .any(|a| a.group == group && a.name == name && !matches!(a.state, ActorState::Dead { .. }));
    if dup {
        return err(format!(
            "actor name '{name}' already exists in group '{group}'"
        ));
    }

    let Some(pg) = st.pgs.get(&pg_id) else {
        return err(format!("no such placement group {pg_id}"));
    };
    if pg.state != PgState::Created {
        return err(format!("placement group {pg_id} is not ready"));
    }
    let Some(Some(bundle)) = pg.assignment.get(bundle_index).cloned() else {
        return err(format!(
            "bundle {bundle_index} of placement group {pg_id} is not placed"
        ));
    };
    // The address this rank listens on inside the fabric its group was
    // placed on. Only set when the group spans a fabric. A group on one
    // node has nothing to cross.
    let fabric_ip = pg
        .island
        .as_ref()
        .and_then(|i| i.addr.get(&bundle.node_id))
        .cloned();
    if num_gpus as usize > bundle.gpu_ids.len() {
        return err(format!(
            "actor wants {num_gpus} GPUs but bundle {bundle_index} reserves {}",
            bundle.gpu_ids.len()
        ));
    }

    let Some(agent) = st.agents.get(&bundle.agent).filter(|a| a.alive) else {
        return err(format!(
            "agent {} for bundle {bundle_index} is gone",
            bundle.agent
        ));
    };
    let agent_id = agent.id.clone();
    let agent_writer = agent.writer.clone();
    let agent_node_ip = agent.node_ip.clone();
    let node_id = bundle.node_id.clone();
    let gpu_ids = bundle.gpu_ids.clone();

    let actor_id = crate::state::new_actor_id();
    let mut spawn_env = env;
    spawn_env.insert("MENTAT_ACTOR_ID".into(), actor_id.clone());
    spawn_env.insert("MENTAT_NODE_ID".into(), node_id.clone());
    spawn_env.insert(
        "MENTAT_GPU_IDS".into(),
        gpu_ids
            .iter()
            .map(|g| g.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    spawn_env.insert("MENTAT_GCS_ADDRESS".into(), st.control_addr.clone());
    // The node's identity, so the container does not set a MENTAT_NODE_IP of
    // its own. The shim reads MENTAT_FABRIC_IP first.
    if !agent_node_ip.is_empty() {
        spawn_env.insert("MENTAT_NODE_IP".into(), agent_node_ip);
    }
    if let Some(ip) = fabric_ip {
        spawn_env.insert("MENTAT_FABRIC_IP".into(), ip);
    }

    st.actors.insert(
        actor_id.clone(),
        ActorInfo {
            id: actor_id.clone(),
            name: name.clone(),
            group: group.clone(),
            agent: agent_id.clone(),
            node_id: node_id.clone(),
            gpu_ids: gpu_ids.clone(),
            owner: client_id.to_string(),
            state: ActorState::Spawning,
            pid: None,
            queued_calls: Vec::new(),
        },
    );
    st.counters.actors_spawned += 1;
    let row = st.actors.get(&actor_id).map(crate::status::actor_row);
    if let Some(row) = row {
        st.emit_patch(
            "actor_spawning",
            vec![Patch::set(&["groups", &group, "actors", &actor_id], row)],
            "",
        );
    }

    let send_res = agent_writer.send(
        Msg::ActorSpawn {
            actor_id: actor_id.clone(),
            name,
            env: spawn_env,
            gpu_ids: gpu_ids.clone(),
            owner: client_id.to_string(),
        },
        0,
        &payload,
    );
    if let Err(e) = send_res {
        mark_actor_dead(
            &mut st,
            &shared.cv,
            &actor_id,
            &format!("spawn did not reach agent {agent_id}: {e}"),
        );
        return err(format!("spawn did not reach agent {agent_id}: {e}"));
    }

    (
        Msg::ActorCreateOk {
            actor_id,
            node_id,
            gpu_ids,
        },
        Vec::new(),
    )
}

/// Resolution of one ref id against current state, without blocking.
enum Res {
    Pending,
    Ready { ok: bool, payload: Vec<u8> },
    ActorDied { reason: String },
    Unknown,
}

/// What a ref currently holds.
///
/// Every id holds its type, so this reads the prefix rather than inferring
/// from what the id is not. `p:` is a placement group, which resolves once
/// it reaches CREATED; `a:<hex>:<n>` is a call ref.
fn resolve_ref(st: &State, ref_id: &str) -> Res {
    if ref_id.starts_with("p:") {
        return match st.pgs.get(ref_id) {
            None => Res::Unknown,
            Some(pg) => match pg.state {
                PgState::Created => Res::Ready {
                    ok: true,
                    payload: Vec::new(),
                },
                PgState::Pending => Res::Pending,
                PgState::Removed => Res::ActorDied {
                    reason: pg
                        .fail_reason
                        .clone()
                        .unwrap_or_else(|| "placement group removed".into()),
                },
            },
        };
    }
    match st.refs.get(ref_id) {
        None => Res::Unknown,
        Some(r) => match &r.state {
            RefState::Pending => Res::Pending,
            RefState::Ready { ok, payload } => Res::Ready {
                ok: *ok,
                payload: payload.clone(),
            },
            RefState::ActorDied { reason } => Res::ActorDied {
                reason: reason.clone(),
            },
        },
    }
}

fn do_get(shared: &SharedRef, ref_id: &str, timeout_ms: Option<u64>) -> (Msg, Vec<u8>) {
    let deadline = timeout_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
    let mut st = shared.st.lock().unwrap();
    loop {
        match resolve_ref(&st, ref_id) {
            Res::Ready { ok, payload } => {
                return (
                    Msg::RefGetOk {
                        status: if ok { "ok" } else { "error" }.into(),
                        reason: String::new(),
                    },
                    payload,
                )
            }
            Res::ActorDied { reason } => {
                return (
                    Msg::RefGetOk {
                        status: "actor_died".into(),
                        reason,
                    },
                    Vec::new(),
                )
            }
            Res::Unknown => return refused("unknown_ref", format!("unknown ref {ref_id}")),
            Res::Pending => {}
        }
        match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return (
                        Msg::RefGetOk {
                            status: "timeout".into(),
                            reason: String::new(),
                        },
                        Vec::new(),
                    );
                }
                let (g, _) = shared.cv.wait_timeout(st, d - now).unwrap();
                st = g;
            }
            None => {
                st = shared.cv.wait(st).unwrap();
            }
        }
    }
}

fn do_wait(
    shared: &SharedRef,
    ref_ids: &[String],
    num_returns: usize,
    timeout_ms: Option<u64>,
) -> (Msg, Vec<u8>) {
    let deadline = timeout_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
    let want = num_returns.min(ref_ids.len());
    let mut st = shared.st.lock().unwrap();
    loop {
        let ready: Vec<String> = ref_ids
            .iter()
            .filter(|r| !matches!(resolve_ref(&st, r), Res::Pending))
            .cloned()
            .collect();
        if ready.len() >= want {
            // Cap at num_returns, preserving input order, like ray does.
            let capped: Vec<String> = ready.into_iter().take(num_returns).collect();
            return (Msg::RefWaitOk { ready: capped }, Vec::new());
        }
        match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return (Msg::RefWaitOk { ready }, Vec::new());
                }
                let (g, _) = shared.cv.wait_timeout(st, d - now).unwrap();
                st = g;
            }
            None => {
                st = shared.cv.wait(st).unwrap();
            }
        }
    }
}

fn kill_actor(shared: &SharedRef, actor_id: &str, why: &str) {
    let mut st = shared.st.lock().unwrap();
    let Some(actor) = st.actors.get(actor_id) else {
        return;
    };
    if matches!(actor.state, ActorState::Dead { .. }) {
        return;
    }
    let agent_writer = st
        .agents
        .get(&actor.agent)
        .filter(|a| a.alive)
        .map(|a| a.writer.clone());
    match agent_writer {
        Some(w) => {
            // The authoritative Dead transition happens on ActorExit from the
            // agent, which knows the real exit status.
            let _ = w.send(
                Msg::ActorKill {
                    actor_id: actor_id.to_string(),
                },
                0,
                &[],
            );
            log(
                "kill_sent",
                &[("actor", actor_id.to_string()), ("why", why.to_string())],
            );
        }
        None => {
            mark_actor_dead(
                &mut st,
                &shared.cv,
                actor_id,
                "killed while its agent was down",
            );
        }
    }
}

/// Mark an actor dead and fan the death out to every pending ref that
/// belongs to it. This is the mechanism behind the run()-ref liveness
/// sentinel: the monitor's ray.wait sees the ref complete.
pub fn mark_actor_dead(st: &mut State, cv: &std::sync::Condvar, actor_id: &str, reason: &str) {
    if let Some(actor) = st.actors.get_mut(actor_id) {
        if matches!(actor.state, ActorState::Dead { .. }) {
            return;
        }
        actor.state = ActorState::Dead {
            reason: reason.to_string(),
            at_ms: crate::state::now_ms_u64(),
        };
        // Held calls die with the actor. Their refs resolve in the fan-out
        // below.
        actor.queued_calls.clear();
        let group = actor.group.clone();
        let agent = actor.agent.clone();
        let mut patch: Vec<Patch> = st
            .actors
            .get(actor_id)
            .map(|a| {
                Patch::set(
                    &["groups", &group, "actors", actor_id],
                    crate::status::actor_row(a),
                )
            })
            .into_iter()
            .collect();
        patch.extend(agent_patch(st, &agent));
        st.emit_patch("actor_dead", patch, reason);
    }
    for (rid, r) in st.refs.iter_mut() {
        if r.actor.as_deref() == Some(actor_id) && matches!(r.state, RefState::Pending) {
            log(
                "ref_actor_died",
                &[("ref", rid.clone()), ("reason", reason.to_string())],
            );
            r.state = RefState::ActorDied {
                reason: reason.to_string(),
            };
        }
    }
    // A live actor holds its GPUs, so its death frees them for a group
    // waiting on them.
    try_place(st, cv);
    cv.notify_all();
}

fn reap_client(shared: &SharedRef, client_id: &str) {
    // The client identity goes away immediately even when a reap grace is
    // configured -- a restarting vLLM is a brand-new client that must be able
    // to open its driver session without waiting out the grace.
    let group = {
        let mut st = shared.st.lock().unwrap();
        let group = client_group(&st, client_id);
        st.clients.remove(client_id);
        st.emit_patch(
            "driver_disconnected",
            vec![Patch::remove(&["clients", client_id])],
            "",
        );
        group
    };
    shared.cv.notify_all();

    let grace = cfg().session_reap_grace_ms;
    if grace == 0 {
        reap_client_resources(shared, client_id, &group);
    } else {
        log(
            "session_reap_deferred",
            &[
                ("client", client_id.to_string()),
                ("grace_ms", grace.to_string()),
            ],
        );
        let shared = shared.clone();
        let client_id = client_id.to_string();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(grace));
            reap_client_resources(&shared, &client_id, &group);
        });
    }
}

/// Kill a dead driver's actors, remove its placement groups and refs. Split
/// from reap_client so MENTAT_SESSION_REAP_GRACE_MS can defer just this part.
fn reap_client_resources(shared: &SharedRef, client_id: &str, group: &str) {
    let actor_ids: Vec<String> = {
        let mut st = shared.st.lock().unwrap();
        release_all(&mut st, client_id);
        let ids: Vec<String> = st
            .actors
            .values()
            .filter(|a| a.owner == client_id && !matches!(a.state, ActorState::Dead { .. }))
            .map(|a| a.id.clone())
            .collect();
        let now = crate::state::now_ms_u64();
        let mut removed: Vec<(String, String)> = Vec::new();
        for pg in st.pgs.values_mut() {
            if pg.owner == client_id && pg.state != PgState::Removed {
                pg.state = PgState::Removed;
                pg.removed_ms = Some(now);
                removed.push((pg.group.clone(), pg.id.clone()));
            }
        }
        for (g, pg_id) in removed {
            let row = st.pgs.get(&pg_id).map(crate::status::pg_row);
            if let Some(row) = row {
                let mut patch = vec![Patch::set(&["groups", &g, "placement_groups", &pg_id], row)];
                patch.extend(agent_patches(&st, &pg_id));
                st.emit_patch("pg_removed", patch, "driver session closed");
            }
        }
        if !ids.is_empty() {
            // Nothing to patch: `driver_disconnected` already removed the
            // client row, and each actor's own event follows.
            st.emit_patch(
                "driver_gone_reaping",
                Vec::new(),
                &format!("{} actors in group {group}", ids.len()),
            );
        }
        ids
    };
    for id in &actor_ids {
        kill_actor(shared, id, "driver session closed");
    }
    let mut st = shared.st.lock().unwrap();
    st.refs.retain(|_, r| r.owner != client_id);
    // GPUs just came free. Another group's pending pg may fit now.
    try_place(&mut st, &shared.cv);
    shared.cv.notify_all();
}

// ---------------------------------------------------------------------------
// Placement
// ---------------------------------------------------------------------------

/// Try to complete every pending placement group. All-or-nothing per group:
/// partial reservations are never held, so two pending pgs can't deadlock.
///
/// A group of more than one bundle is placed inside one fabric island. TP
/// ranks talk to each other over NCCL, so ranks split across two fabrics
/// would rendezvous and then hang -- a failure that looks like a model bug
/// and costs a debugging session. Waiting is the better answer: the group
/// stays PENDING and fails loudly at the pending timeout, naming what it
/// could not find.
///
/// The constraint applies only where it means something. A cluster with no
/// derived island places exactly as it did before fabrics existed, which is
/// every untagged deployment and every single-box one. A group that fits on
/// one node does not need a fabric at all, and a node is therefore its own island
/// of one.
pub fn try_place(st: &mut State, cv: &std::sync::Condvar) {
    let pending: Vec<String> = st
        .pgs
        .values()
        .filter(|p| p.state == PgState::Pending)
        .map(|p| p.id.clone())
        .collect();
    for pg_id in pending {
        let (group, owner, bundles, claim) = {
            let pg = &st.pgs[&pg_id];
            (
                pg.group.clone(),
                pg.owner.clone(),
                pg.bundles.clone(),
                pg.claim.clone(),
            )
        };
        let driver_node = st
            .clients
            .get(&owner)
            .map(|c| c.node_id.clone())
            .unwrap_or_default();

        let placed = match placement_scopes(st, &group, &bundles, &driver_node, &claim) {
            Ok(scopes) => scopes.into_iter().find_map(|(island, nodes)| {
                fit(st, &group, &bundles, &driver_node, nodes.as_deref()).map(|a| (island, a))
            }),
            Err(why) => {
                set_pending_reason(st, &pg_id, why);
                continue;
            }
        };
        let Some((island, assignment)) = placed else {
            let why = no_fit_reason(st, &group, &bundles);
            set_pending_reason(st, &pg_id, why);
            continue;
        };

        let n = assignment.len();
        let members = island.as_ref().map(|i| i.nodes.len());
        if let Some(pg) = st.pgs.get_mut(&pg_id) {
            pg.assignment = assignment;
            pg.state = PgState::Created;
            pg.island = island;
            pg.pending_reason = None;
        }
        let row = st.pgs.get(&pg_id).map(crate::status::pg_row);
        if let Some(row) = row {
            let mut patch = vec![Patch::set(
                &["groups", &group, "placement_groups", &pg_id],
                row,
            )];
            patch.extend(agent_patches(st, &pg_id));
            st.emit_patch("pg_ready", patch, "");
        }
        let _ = (n, members);
        cv.notify_all();
    }
}

/// The scopes to try placing a group in, best first.
///
/// `None` in a scope means "anywhere", which is the whole answer for a
/// single-bundle group and for a group that has not opted in. Otherwise the
/// driver's own island comes first -- keeping rank 0 next to the engine --
/// and the rest follow smallest-sufficient-first, so a two-node group does
/// not consume the only four-node fabric.
///
/// Every node also stands as an island of one, since a group whose bundles
/// all land on one node never crosses a fabric.
///
/// Opting in is per group. The operator tags one pair first and boots it,
/// which must leave a group on the untagged pair placing as before -- and a
/// gate testing whether the cluster had any island would strand it instead.
/// The gate tests whether this group's own nodes claim a fabric.
#[allow(clippy::type_complexity)]
fn placement_scopes(
    st: &State,
    group: &str,
    bundles: &[u32],
    driver_node: &str,
    claim: &str,
) -> Result<Vec<(Option<crate::island::Island>, Option<Vec<String>>)>, String> {
    // A claim already settled where this group goes, and it settled for
    // every holder of that name. Re-deriving here could pick different
    // nodes and split ranks that agreed on the claim's view.
    if !claim.is_empty() {
        let Some(c) = st.claims.get(&(group.to_string(), claim.to_string())) else {
            return Err(format!("claim {claim:?} is not held here"));
        };
        let nodes = claimed_nodes(&c.view);
        if nodes.is_empty() {
            return Err(format!("claim {claim:?} names no nodes"));
        }
        // The head solves a claim against the whole mesh. This daemon
        // places among its own agents. Where the two disagree the group
        // would be assigned a node the driver cannot see, and the driver
        // reads the assignment by node id, so it would fail looking up a
        // node it was never told about.
        let served: std::collections::BTreeSet<&str> = st
            .agents
            .values()
            .filter(|a| a.alive && a.group == group)
            .map(|a| a.node_id.as_str())
            .collect();
        if let Some(missing) = nodes.iter().find(|n| !served.contains(n.as_str())) {
            let where_ = st
                .peers
                .values()
                .find(|p| &p.node_id == missing)
                .map(|p| p.node_ip.clone())
                .unwrap_or_else(|| missing.clone());
            let head = st
                .peers
                .values()
                .find(|p| p.node_id == st.head_node_id)
                .map(|p| format!("{}:{}", p.node_ip, p.control_port))
                .unwrap_or_else(|| "this node".to_string());
            return Err(format!(
                "claim {claim:?} placed a bundle on {where_}, which has no agent of \
                 group {group} registered with this daemon. The head solved the claim \
                 against the whole mesh. Point the driver and every agent of the group \
                 at the head, {head}"
            ));
        }
        return Ok(vec![(None, Some(nodes))]);
    }
    let opted_in = cfg().island_placement
        && st
            .agents
            .values()
            .any(|a| a.alive && a.group == group && st.fabrics.tagged.contains(&a.node_id));
    if bundles.len() < 2 || !opted_in {
        return Ok(vec![(None, None)]);
    }
    let need: usize = bundles.iter().map(|b| (*b).max(1) as usize).sum();
    let free_in = |nodes: &[String]| -> usize {
        st.agents
            .values()
            .filter(|a| a.alive && a.group == group && nodes.contains(&a.node_id))
            .map(|a| st.free_gpus_of(&a.id).len())
            .sum()
    };

    let mut scopes: Vec<(usize, bool, Vec<String>, crate::island::Island)> = Vec::new();
    for i in &st.fabrics.islands {
        if free_in(&i.nodes) >= need {
            scopes.push((
                i.nodes.len(),
                !i.nodes.iter().any(|n| n == driver_node),
                i.nodes.clone(),
                i.clone(),
            ));
        }
    }
    // Nodes on no island: each is its own island of one, and only enters
    // the running when it alone can hold the whole group.
    let islanded: Vec<&String> = st.fabrics.islands.iter().flat_map(|i| &i.nodes).collect();
    let mut lone: Vec<String> = st
        .agents
        .values()
        .filter(|a| a.alive && a.group == group)
        .map(|a| a.node_id.clone())
        .filter(|n| !islanded.contains(&n))
        .collect();
    lone.sort();
    lone.dedup();
    for n in lone {
        let nodes = vec![n.clone()];
        if free_in(&nodes) >= need {
            scopes.push((
                1,
                n != driver_node,
                nodes.clone(),
                crate::island::Island {
                    nodes,
                    addr: Default::default(),
                },
            ));
        }
    }
    if scopes.is_empty() {
        return Err(no_island_reason(st, group, bundles.len(), need));
    }
    // Driver's island first, then smallest sufficient.
    scopes.sort_by(|a, b| (a.1, a.0, &a.2).cmp(&(b.1, b.0, &b.2)));
    Ok(scopes
        .into_iter()
        .map(|(_, _, nodes, island)| {
            // A one-node scope does not hold a fabric address to inject.
            let island = (island.nodes.len() > 1).then_some(island);
            (island, Some(nodes))
        })
        .collect())
}

/// First-fit the bundles over one scope's agents, or None if they do not
/// all fit. `nodes` of None means every node in the group.
///
/// Agent order is this group's, alive, driver-node first, then registration
/// order. Bundle 0 lands on the driver's node when it can, which puts TP
/// rank 0 next to the engine for the shm queue.
fn fit(
    st: &State,
    group: &str,
    bundles: &[u32],
    driver_node: &str,
    nodes: Option<&[String]>,
) -> Option<Vec<Option<BundleAssignment>>> {
    let mut candidates: Vec<(bool, u64, String)> = st
        .agents
        .values()
        .filter(|a| a.alive && a.group == group)
        .filter(|a| nodes.map(|ns| ns.contains(&a.node_id)).unwrap_or(true))
        .map(|a| (a.node_id != driver_node, a.seq, a.id.clone()))
        .collect();
    candidates.sort();
    let mut agents: Vec<(String, Vec<u32>)> = candidates
        .into_iter()
        .map(|(_, _, id)| {
            let free = st.free_gpus_of(&id);
            (id, free)
        })
        .collect();

    let mut assignment: Vec<Option<BundleAssignment>> = Vec::with_capacity(bundles.len());
    for spec in bundles {
        let need = (*spec).max(1) as usize;
        let mut placed = None;
        for (agent_id, free) in agents.iter_mut() {
            if free.len() >= need {
                let gpu_ids: Vec<u32> = free.drain(..need).collect();
                placed = Some(BundleAssignment {
                    agent: agent_id.clone(),
                    node_id: st.agents[agent_id].node_id.clone(),
                    gpu_ids,
                });
                break;
            }
        }
        assignment.push(Some(placed?));
    }
    Some(assignment)
}

/// Why no island could hold this group, in the terms an operator can act
/// on: how many nodes it needs on one fabric, and what the best fabric that
/// actually holds part of this group offers.
///
/// Islands with no agent of this group are left out. Naming the cluster's
/// largest fabric when the group has nothing on it sends the reader to the
/// wrong rack.
fn no_island_reason(st: &State, group: &str, bundles: usize, need: usize) -> String {
    let best = st
        .fabrics
        .islands
        .iter()
        .filter_map(|i| {
            let agents = st
                .agents
                .values()
                .filter(|a| a.alive && a.group == group && i.nodes.contains(&a.node_id));
            let (mut free, mut held) = (0usize, 0usize);
            for a in agents {
                free += st.free_gpus_of(&a.id).len();
                held += 1;
            }
            (held > 0).then_some((free, i.nodes.len()))
        })
        .max();
    match best {
        Some((free, nodes)) => format!(
            "{bundles} bundles ({need} GPUs) must share one rdma fabric. The best fabric \
             holding group '{group}' offers {free} free GPUs across {nodes} nodes"
        ),
        None => format!(
            "{bundles} bundles ({need} GPUs) must share one rdma fabric, and no node \
             holding group '{group}' is on one"
        ),
    }
}

/// Why the bundles did not fit, when a scope existed to try. Free GPUs are
/// the usual answer.
fn no_fit_reason(st: &State, group: &str, bundles: &[u32]) -> String {
    let need: usize = bundles.iter().map(|b| (*b).max(1) as usize).sum();
    let free: usize = st
        .agents
        .values()
        .filter(|a| a.alive && a.group == group)
        .map(|a| st.free_gpus_of(&a.id).len())
        .sum();
    format!(
        "{} bundles need {need} GPUs and group '{group}' has {free} free",
        bundles.len()
    )
}

// ---------------------------------------------------------------------------
// Agent connections
// ---------------------------------------------------------------------------

fn agent_conn(
    shared: SharedRef,
    mut reader: BufReader<TcpStream>,
    writer: FrameWriter,
    peer_ip: String,
    first: (Frame, Vec<u8>),
) {
    let Msg::AgentRegister {
        proto,
        agent_id,
        group,
        node_ip,
        machine,
        container,
        pid,
        services,
        resume,
        unacked_refs,
    } = first.0.msg
    else {
        unreachable!()
    };

    if !crate::proto::major_matches(&proto) {
        let _ = writer.send(
            Msg::Err {
                error: format!("proto {} here, {proto} offered", crate::proto::PROTO),
                code: "proto_mismatch".into(),
                head: String::new(),
                proto: crate::proto::PROTO.to_string(),
            },
            first.0.req,
            &[],
        );
        return;
    }

    let (node_ip, node_id) = {
        let st = shared.st.lock().unwrap();
        if node_ip.is_empty() {
            // An agent that claimed nothing is on the box it connected
            // from: this one for loopback or any address this box owns,
            // else the mesh peer that announces the source address, else a
            // node named by the address itself.
            let mine = crate::announce::all_local_addrs();
            if crate::state::is_loopback(&peer_ip) || mine.contains(&peer_ip) {
                (st.node_ip.clone(), st.node_id.clone())
            } else if let Some(p) = st
                .peers
                .values()
                .find(|p| p.node_ip == peer_ip || p.addrs.contains(&peer_ip))
            {
                (p.node_ip.clone(), p.node_id.clone())
            } else {
                (peer_ip.clone(), node_id_for(&peer_ip))
            }
        } else if node_ip == st.node_ip {
            (node_ip, st.node_id.clone())
        } else {
            let id = node_id_for(&node_ip);
            (node_ip, id)
        }
    };
    // A container claiming an address that belongs to a box the mesh files
    // under another node is refused. Registering it would put its GPUs on a
    // node no island reaches, so its group would cross no fabric and its
    // ranks would talk over whichever link the address named, which reads
    // as a slow model rather than as a misconfiguration.
    let misfiled = {
        let mut st = shared.st.lock().unwrap();
        misfiled_node(&st, &node_ip, &node_id).map(|why| {
            let first_time = st.misfiled_warned.insert(agent_id.clone());
            (why, first_time)
        })
    };
    if let Some((why, first_time)) = misfiled {
        if first_time {
            log(
                "agent_node_misfiled",
                &[("agent", agent_id.clone()), ("why", why.clone())],
            );
        }
        // The reason rides on the refusal, and the agent retries in a loop
        // and logs it there, so the operator reading the container's own
        // output sees it however long it has been failing.
        let _ = writer.send(
            Msg::refused("agent_refused", format!("agent {agent_id} {why}")),
            first.0.req,
            &[],
        );
        return;
    }

    {
        let mut st = shared.st.lock().unwrap();
        // Re-registration replaces the previous connection outright.
        if let Some(old) = st.agents.get(&agent_id) {
            old.writer.shutdown();
        }
        let seq = st.seq();
        st.agents.insert(
            agent_id.clone(),
            AgentInfo {
                id: agent_id.clone(),
                group: group.clone(),
                node_id: node_id.clone(),
                node_ip: node_ip.clone(),
                machine: machine.clone(),
                container: container.clone(),
                pid,
                services: services.clone(),
                writer: writer.clone(),
                alive: true,
                lost_at_ms: None,
                degraded: false,
                gone_since_ms: None,
                seq,
            },
        );
        st.counters.agents_registered += 1;
        let row = crate::status::agent_row(&st, &st.agents[&agent_id]);
        st.emit_patch(
            "agent_register",
            vec![Patch::set(&["groups", &group, "agents", &agent_id], row)],
            "",
        );

        // Resumed actors whose owner is gone (or that this daemon already
        // declared dead, e.g. a kill or give-up during the outage) get
        // killed. The rest are re-adopted (matters once the mesh can move
        // the head). The kills are sent after AgentRegisterOk below -- the
        // agent's handshake expects that as the first frame.
        let mut kills: Vec<String> = Vec::new();
        let mut adopt: Vec<ActorInfo> = Vec::new();
        for r in &resume {
            match st.actors.get(&r.actor_id) {
                // Known here: keep it while its owner is around and it has
                // not already been declared dead.
                Some(a) => {
                    if !st.clients.contains_key(&a.owner)
                        || matches!(a.state, ActorState::Dead { .. })
                    {
                        kills.push(r.actor_id.clone());
                        log(
                            "resume_rejected",
                            &[("actor", r.actor_id.clone()), ("agent", agent_id.clone())],
                        );
                    }
                }
                // Not known here. A daemon that restarted has forgotten
                // every actor, and killing them would kill every model on the
                // cluster because its own bookkeeping was lost. The process
                // is alive on the agent, which is the fact that matters, so
                // it is adopted from what the agent reports.
                //
                // The owner may not have reconnected yet, and nothing reaps
                // an actor for want of one. The driver re-sends its hello
                // under the same client id, which is what rejoins the two.
                None => adopt.push(ActorInfo {
                    id: r.actor_id.clone(),
                    name: r.name.clone(),
                    group: group.clone(),
                    agent: agent_id.clone(),
                    node_id: node_id.clone(),
                    gpu_ids: r.gpu_ids.clone(),
                    owner: r.owner.clone(),
                    state: ActorState::Running,
                    pid: (r.pid != 0).then_some(r.pid),
                    queued_calls: Vec::new(),
                }),
            }
        }
        let mut adopted: Vec<Patch> = Vec::new();
        for a in adopt {
            log(
                "actor_adopted",
                &[
                    ("actor", a.id.clone()),
                    ("agent", agent_id.clone()),
                    ("owner", a.owner.clone()),
                ],
            );
            let id = a.id.clone();
            let row = crate::status::actor_row(&a);
            st.actors.insert(id.clone(), a);
            adopted.push(Patch::set(&["groups", &group, "actors", &id], row));
        }
        if !adopted.is_empty() {
            // agent_register above counted these actors' GPUs as free.
            adopted.extend(agent_patch(&st, &agent_id));
            st.emit_patch("actor_adopted", adopted, "");
        }

        // The calls those actors are still working on. The agent holds
        // them (pending_refs) or holds their results to re-send
        // (unacked_refs), and a daemon with none of its own would reply to a
        // driver's get with "no such ref" or leave it waiting for a result
        // it has nowhere to put.
        //
        // A ref id is "<actor>:<n>", so it contains its own actor, and the
        // counter is moved past what was adopted: starting again from one
        // would hand a new call the id of a call still running.
        {
            let reported: Vec<String> = resume
                .iter()
                .flat_map(|r| r.pending_refs.iter())
                .chain(unacked_refs.iter())
                .cloned()
                .collect();
            let mut adopted = 0usize;
            for rid in reported {
                if st.refs.contains_key(&rid) {
                    continue;
                }
                let (actor, seq) = match rid.rsplit_once(':') {
                    Some((a, n)) => (Some(a.to_string()), n.parse::<u64>().ok()),
                    None => (None, None),
                };
                // Only for actors this agent is holding: a ref naming
                // something else is not this agent's to revive.
                let owner = match actor.as_deref().and_then(|a| st.actors.get(a)) {
                    Some(a) if a.agent == agent_id => a.owner.clone(),
                    _ => continue,
                };
                if let Some(n) = seq {
                    st.next_ref = st.next_ref.max(n + 1);
                }
                st.refs.insert(
                    rid,
                    RefInfo {
                        state: RefState::Pending,
                        actor,
                        owner,
                        // The agent reports a call by id and nothing more.
                        method: String::new(),
                        created_ms: crate::state::now_ms_u64(),
                        warned: false,
                    },
                );
                adopted += 1;
            }
            if adopted > 0 {
                log(
                    "refs_adopted",
                    &[("agent", agent_id.clone()), ("count", adopted.to_string())],
                );
            }
        }

        // The resume list is authoritative for what survived on the agent's
        // side. An actor this daemon still thinks is live but the agent no
        // longer holds (agent restarted, or the actor exited during the
        // outage and the exit report was lost) is dead.
        {
            let resumed: std::collections::HashSet<&str> =
                resume.iter().map(|r| r.actor_id.as_str()).collect();
            let missing: Vec<String> = st
                .actors
                .values()
                .filter(|a| {
                    a.agent == agent_id
                        && !matches!(a.state, ActorState::Dead { .. })
                        && !resumed.contains(a.id.as_str())
                })
                .map(|a| a.id.clone())
                .collect();
            for id in missing {
                mark_actor_dead(
                    &mut st,
                    &shared.cv,
                    &id,
                    "agent reconnected without this actor",
                );
            }
        }

        // A pending ref the agent neither holds (pending_refs), has a
        // buffered result for (unacked_refs), nor sits in this daemon's own
        // held-call queue was lost in flight during the outage: fail it so
        // the driver raises instead of hanging forever.
        {
            let known: std::collections::HashSet<&str> = resume
                .iter()
                .flat_map(|r| r.pending_refs.iter())
                .chain(unacked_refs.iter())
                .map(|s| s.as_str())
                .collect();
            let queued: std::collections::HashSet<String> = st
                .actors
                .values()
                .filter(|a| a.agent == agent_id)
                .flat_map(|a| a.queued_calls.iter().map(|(r, _, _)| r.clone()))
                .collect();
            let this_agents_actor = |actor: &Option<String>, st: &State| {
                actor
                    .as_deref()
                    .and_then(|id| st.actors.get(id))
                    .is_some_and(|a| a.agent == agent_id)
            };
            let lost: Vec<String> = st
                .refs
                .iter()
                .filter(|(rid, r)| {
                    matches!(r.state, RefState::Pending)
                        && this_agents_actor(&r.actor, &st)
                        && !known.contains(rid.as_str())
                        && !queued.contains(rid.as_str())
                })
                .map(|(rid, _)| rid.clone())
                .collect();
            for rid in lost {
                log("ref_lost_in_outage", &[("ref", rid.clone())]);
                if let Some(r) = st.refs.get_mut(&rid) {
                    r.state = RefState::ActorDied {
                        reason: "call lost while the agent link was down".into(),
                    };
                }
            }
        }

        // AgentRegisterOk must be the first frame on the link -- the agent's
        // handshake rejects anything else -- so kills and held-call drains
        // follow it.
        let _ = writer.send(
            Msg::AgentRegisterOk {
                proto: crate::proto::proto(),
                node_id: node_id.clone(),
            },
            first.0.req,
            &[],
        );
        for actor_id in kills {
            let _ = writer.send(Msg::ActorKill { actor_id }, 0, &[]);
        }

        // Drain calls held during the outage, in arrival order.
        {
            let drains: Vec<(String, Vec<crate::state::QueuedCall>)> = st
                .actors
                .values_mut()
                .filter(|a| a.agent == agent_id && !matches!(a.state, ActorState::Dead { .. }))
                .filter(|a| !a.queued_calls.is_empty())
                .map(|a| (a.id.clone(), std::mem::take(&mut a.queued_calls)))
                .collect();
            for (actor_id, calls) in drains {
                for (ref_id, method, payload) in calls {
                    log(
                        "held_call_sent",
                        &[("ref", ref_id.clone()), ("actor", actor_id.clone())],
                    );
                    let _ = writer.send(
                        Msg::ActorDispatch {
                            actor_id: actor_id.clone(),
                            ref_id,
                            method,
                        },
                        0,
                        &payload,
                    );
                }
            }
        }
        try_place(&mut st, &shared.cv);
    }
    shared.cv.notify_all();

    loop {
        let (frame, payload) = match read_frame(&mut reader) {
            Ok(Some(fp)) => fp,
            Ok(None) => break,
            Err(e) => {
                log(
                    "agent_read_error",
                    &[("agent", agent_id.clone()), ("error", e.to_string())],
                );
                break;
            }
        };
        match frame.msg {
            Msg::ActorSpawnResult {
                actor_id,
                ok,
                error,
                pid,
            } => {
                let mut st = shared.st.lock().unwrap();
                if let Some(a) = st.actors.get_mut(&actor_id) {
                    if pid != 0 {
                        a.pid = Some(pid);
                    }
                }
                if ok {
                    if let Some(a) = st.actors.get_mut(&actor_id) {
                        if a.state == ActorState::Spawning {
                            a.state = ActorState::Running;
                        }
                    }
                    // The group comes from the actor: a path needs it, and
                    // the event used to hold only the id and pid.
                    let found = st
                        .actors
                        .get(&actor_id)
                        .map(|a| (a.group.clone(), crate::status::actor_row(a)));
                    if let Some((group, row)) = found {
                        st.emit_patch(
                            "actor_running",
                            vec![Patch::set(&["groups", &group, "actors", &actor_id], row)],
                            "",
                        );
                    }
                } else {
                    mark_actor_dead(
                        &mut st,
                        &shared.cv,
                        &actor_id,
                        &format!("spawn failed: {error}"),
                    );
                }
                shared.cv.notify_all();
            }
            Msg::ActorResult { ref_id, ok, error } => {
                let mut st = shared.st.lock().unwrap();
                // First resolution wins: a result re-sent after an outage must
                // not overwrite a ref the driver may already have seen fail.
                if let Some(r) = st
                    .refs
                    .get_mut(&ref_id)
                    .filter(|r| matches!(r.state, RefState::Pending))
                {
                    r.state = if !ok && payload.is_empty() && !error.is_empty() {
                        RefState::ActorDied { reason: error }
                    } else {
                        RefState::Ready { ok, payload }
                    };
                }
                shared.cv.notify_all();
            }
            Msg::ActorExit {
                actor_id,
                exit_code,
                signal,
            } => {
                let mut st = shared.st.lock().unwrap();
                match (exit_code, signal) {
                    (Some(0), _) => st.counters.actor_exits_clean += 1,
                    (_, Some(_)) => st.counters.actor_exits_signal += 1,
                    _ => st.counters.actor_exits_error += 1,
                }
                let reason = format!(
                    "actor process exited (exit_code={} signal={})",
                    exit_code
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "none".into()),
                    signal
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "none".into()),
                );
                mark_actor_dead(&mut st, &shared.cv, &actor_id, &reason);
            }
            Msg::ServiceNote { service, note } => {
                let mut st = shared.st.lock().unwrap();
                let group = st.agents.get_mut(&agent_id).and_then(|a| {
                    let s = a.services.get_mut(&service)?;
                    s.note = note;
                    Some(a.group.clone())
                });
                // The note reaches the router through the agent row, so the
                // row change needs an event like any other.
                let row = st
                    .agents
                    .get(&agent_id)
                    .map(|a| crate::status::agent_row(&st, a));
                if let (Some(group), Some(row)) = (group, row) {
                    st.emit_patch(
                        "service_note",
                        vec![Patch::set(&["groups", &group, "agents", &agent_id], row)],
                        &service,
                    );
                }
            }
            Msg::Ping => {
                let _ = writer.send(Msg::Pong, frame.req, &[]);
            }
            Msg::Pong => {}
            other => log(
                "agent_unexpected_msg",
                &[("agent", agent_id.clone()), ("msg", format!("{other:?}"))],
            ),
        }
    }

    // Agent link lost: its actors are unreachable, but a link blip and a dead
    // container look identical here, so start the degrade window instead of
    // declaring death. The lifecycle sweeper marks the agent degraded after
    // MENTAT_AGENT_DEGRADED_AFTER_MS and gives up (actors dead, run()
    // sentinels resolve, driver restarts) after MENTAT_AGENT_DEAD_AFTER_MS;
    // an agent that re-registers inside the window holds on with nothing
    // lost.
    let mut st = shared.st.lock().unwrap();
    // Only if this reader owned the current registration -- a re-register may
    // already have replaced the entry with a fresh connection.
    let owned = st
        .agents
        .get(&agent_id)
        .map(|a| FrameWriter::same_socket(&a.writer, &writer))
        .unwrap_or(false);
    if owned {
        let group = if let Some(a) = st.agents.get_mut(&agent_id) {
            a.alive = false;
            let now = crate::state::now_ms_u64();
            a.lost_at_ms = Some(now);
            a.gone_since_ms = Some(now);
            a.degraded = false;
            a.group.clone()
        } else {
            String::new()
        };
        let row = st
            .agents
            .get(&agent_id)
            .map(|a| crate::status::agent_row(&st, a));
        if let Some(row) = row {
            st.emit_patch(
                "agent_lost",
                vec![Patch::set(&["groups", &group, "agents", &agent_id], row)],
                &format!("degrade window {} ms", cfg().agent_dead_after_ms),
            );
        }
    }
    shared.cv.notify_all();
}

#[cfg(test)]
mod tests {
    use super::{claim, misfiled, orphans_of, sweep_history};
    use crate::state::{ActorInfo, ActorState, ClientInfo, State};

    /// A second holder spells the shape its own way and joins the claim it
    /// already holds. Going through `claim` covers the comparison the
    /// daemon makes, which `canonical` alone does not.
    #[test]
    fn a_second_holder_may_spell_the_shape_differently() {
        let mut st = State::new("10.0.0.1".into(), "box".into(), "10.0.0.1:6379".into());
        st.head_node_id = st.node_id.clone();

        let ints = serde_json::json!({"sets": [{"name": "s", "bundles": [1]}]});
        let floats = serde_json::json!({"sets": [{"name": "s", "bundles": [1.0]}]});
        let count = serde_json::json!({"sets": [{"name": "s", "bundles": 1}]});

        // Solved already, since solving one needs a cluster to solve it on.
        st.claims.insert(
            ("grp".into(), "fence".into()),
            crate::state::ClaimInfo {
                shape: crate::claim::canonical(&ints),
                view: serde_json::json!({"sets": {}}),
                generation: 7,
                holders: ["c1".to_string()].into_iter().collect(),
            },
        );

        for (who, shape) in [("c2", &floats), ("c3", &count)] {
            let (g, _) = claim(&mut st, who, "grp", "fence", shape)
                .unwrap_or_else(|e| panic!("{who}: {e:?}"));
            assert_eq!(g, 7, "a repeat claim returns the answer already solved");
        }

        // A different request under the same name is still refused.
        let wider = serde_json::json!({"sets": [{"name": "s", "bundles": [2]}]});
        assert!(claim(&mut st, "c4", "grp", "fence", &wider).is_err());
    }

    fn state_with(owner: &str, at_ms: u64) -> State {
        let mut st = State::new("10.0.0.1".into(), "box".into(), "10.0.0.1:6379".into());
        st.actors.insert(
            "a1".into(),
            ActorInfo {
                id: "a1".into(),
                name: "w0".into(),
                group: "g".into(),
                agent: "ag".into(),
                node_id: st.node_id.clone(),
                gpu_ids: vec![0],
                owner: owner.into(),
                state: ActorState::Dead {
                    reason: "exited".into(),
                    at_ms,
                },
                pid: None,
                queued_calls: Vec::new(),
            },
        );
        st
    }

    fn with_driver(mut st: State, client: &str) -> State {
        st.clients.insert(
            client.into(),
            ClientInfo {
                id: client.into(),
                group: "g".into(),
                kind: "driver".into(),
                node_id: st.node_id.clone(),
                has_session: true,
            },
        );
        st
    }

    /// The reported shape: a box that has booted a model dozens of times
    /// holds a row per boot, each from a driver long gone.
    #[test]
    fn a_dead_actor_goes_once_its_driver_has() {
        let mut st = state_with("driver-1", 0);
        sweep_history(&mut st);
        assert!(st.actors.is_empty());
    }

    /// The row is what turns a call on a dead actor into the reason it died,
    /// and only its owner can make that call.
    #[test]
    fn a_dead_actor_stays_while_its_driver_is_connected() {
        let mut st = with_driver(state_with("driver-1", 0), "driver-1");
        sweep_history(&mut st);
        assert_eq!(st.actors.len(), 1);
    }

    /// A restarted daemon rebuilds its tables from the agents before any
    /// driver reconnects, so for a moment every owner looks gone. Without
    /// the age floor that moment would erase the reasons.
    #[test]
    fn a_recent_death_survives_a_daemon_restart() {
        let mut st = state_with("driver-1", crate::state::now_ms_u64());
        sweep_history(&mut st);
        assert_eq!(st.actors.len(), 1);
    }

    /// A driver that crashed while its daemon restarted never re-sends its
    /// hello, so its adopted actor held its GPU until a new driver for the
    /// group waited out MENTAT_PG_PENDING_TIMEOUT_MS.
    #[test]
    fn a_new_session_replaces_actors_whose_owner_has_none() {
        let mut st = state_with("gone", 0);
        st.actors.get_mut("a1").unwrap().state = ActorState::Running;
        assert_eq!(orphans_of(&st, "g", "new"), vec!["a1".to_string()]);

        // The owner reconnecting is the same driver, and keeps its actor.
        assert!(orphans_of(&st, "g", "gone").is_empty());
        // An owner with a live session holds the group.
        let st = with_driver(st, "gone");
        assert!(orphans_of(&st, "g", "new").is_empty());
    }

    /// A live agent with a single GPU. The socket is a loopback pair the
    /// test never reads.
    fn with_agent(mut st: State, id: &str) -> State {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let seq = st.seq();
        st.agents.insert(
            id.into(),
            crate::state::AgentInfo {
                id: id.into(),
                group: "g".into(),
                node_id: st.node_id.clone(),
                node_ip: st.node_ip.clone(),
                machine: crate::proto::Machine {
                    memory: 0,
                    cpus: 1,
                    gpus: vec![crate::proto::Gpu {
                        index: 0,
                        vendor: "nvidia".into(),
                        name: "fake".into(),
                        memory: 0,
                        uma: false,
                    }],
                },
                container: "c".into(),
                pid: 1,
                services: Default::default(),
                writer: crate::state::FrameWriter::new(stream),
                alive: true,
                lost_at_ms: None,
                degraded: false,
                gone_since_ms: None,
                seq,
            },
        );
        st
    }

    /// The old driver's reap sent the kill and ran placement while the
    /// actor was still running, so the new driver's group pended behind a
    /// GPU that came free a moment later. Nothing re-ran placement, and the
    /// group waited out MENTAT_PG_PENDING_TIMEOUT_MS.
    #[test]
    fn an_actor_exit_places_the_group_waiting_on_its_gpu() {
        let mut st = with_agent(state_with("driver-1", 0), "ag");
        st.head_node_id = st.node_id.clone();
        st.actors.get_mut("a1").unwrap().state = ActorState::Running;
        let mut st = with_driver(st, "driver-2");
        st.pgs.insert(
            "p1".into(),
            crate::state::PgInfo {
                id: "p1".into(),
                group: "g".into(),
                owner: "driver-2".into(),
                bundles: vec![1],
                strategy: "PACK".into(),
                assignment: vec![None],
                state: crate::state::PgState::Pending,
                created_ms: crate::state::now_ms_u64(),
                fail_reason: None,
                claim: String::new(),
                island: None,
                pending_reason: None,
                removed_ms: None,
            },
        );
        let cv = std::sync::Condvar::new();
        super::try_place(&mut st, &cv);
        assert_eq!(st.pgs["p1"].state, crate::state::PgState::Pending);

        super::mark_actor_dead(&mut st, &cv, "a1", "actor process exited");
        assert_eq!(st.pgs["p1"].state, crate::state::PgState::Created);
        assert_eq!(
            st.pgs["p1"].assignment[0].as_ref().unwrap().gpu_ids,
            vec![0]
        );
    }

    fn boxes() -> Vec<(String, String, Vec<String>)> {
        vec![
            (
                "id-10.100.0.2".into(),
                "gx10-5818".into(),
                vec!["10.100.0.2".into(), "192.168.1.77".into()],
            ),
            (
                "id-10.100.0.1".into(),
                "gx10-2353".into(),
                vec!["10.100.0.1".into(), "192.168.1.70".into()],
            ),
        ]
    }

    /// The reported shape: a container told to call itself by the LAN
    /// address of the box it runs on. Its GPUs land on a node no island
    /// contains, and the group then stays off the fabric.
    #[test]
    fn a_lan_address_for_a_fabric_box_is_named() {
        let note = misfiled("192.168.1.77", "id-192.168.1.77", &boxes())
            .expect("the LAN address of a known box under another id");
        assert!(note.contains("gx10-5818"), "{note}");
        assert!(note.contains("192.168.1.77"), "{note}");
    }

    /// The address the box is filed under is what it should claim.
    #[test]
    fn the_matching_address_is_quiet() {
        assert_eq!(misfiled("10.100.0.2", "id-10.100.0.2", &boxes()), None);
    }

    /// An address no box in the mesh owns proves nothing about identity: it
    /// may be a node whose daemon has not been seen yet.
    #[test]
    fn an_unknown_address_is_quiet() {
        assert_eq!(misfiled("10.42.0.9", "id-10.42.0.9", &boxes()), None);
    }

    /// Loopback belongs to every box, so it identifies none.
    #[test]
    fn loopback_is_quiet() {
        let b = vec![(
            "id-a".into(),
            "a".into(),
            vec!["127.0.0.1".into(), "10.0.0.1".into()],
        )];
        assert_eq!(misfiled("127.0.0.1", "id-b", &b), None);
        assert_eq!(misfiled("", "id-b", &b), None);
    }
}
