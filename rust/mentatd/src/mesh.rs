//! The daemon mesh: persistent links between mentatd instances, head
//! election, snapshot exchange, and event replication.
//!
//! There is no consensus protocol. State is soft and is rebuilt from the
//! agents and drivers themselves, so the mesh only has to give every daemon
//! the same answer to one question: which daemon is the head. Every group
//! lives there, relayed by the daemon a container reached.

use std::io::BufReader;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::cfg;
use crate::daemon::set_keepalive;
use crate::proto::{read_frame, Frame, Msg};
use crate::state::{is_loopback, now_ms_u64, FrameWriter, PairProbe, PeerInfo, SharedRef};
use mentat_common::logfmt::log;

pub fn start(shared: SharedRef, seeds: Vec<String>, control_port: u16, http_port: u16) {
    for seed in seeds {
        dial(&shared, seed, control_port, http_port, false);
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || discoverer(shared, control_port, http_port));
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || elector(shared));
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || staleness_sweeper(shared));
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || prober(shared));
    }
    std::thread::spawn(move || status_pusher(shared));
}

/// Start a connector for one control address, unless one is already
/// running for it.
fn dial(shared: &SharedRef, target: String, control_port: u16, http_port: u16, discovered: bool) {
    if !shared.st.lock().unwrap().dialing.insert(target.clone()) {
        return;
    }
    if discovered {
        log("peer_discovered", &[("control", target.clone())]);
    }
    let shared = shared.clone();
    std::thread::spawn(move || connector(shared, target, control_port, http_port, discovered));
}

/// Dial every control address the live peers publish in their status
/// pushes. One reachable seed then joins the whole mesh. Nodes that could
/// see only a hub would otherwise each elect themselves head when it went.
fn discoverer(shared: SharedRef, control_port: u16, http_port: u16) {
    loop {
        std::thread::sleep(Duration::from_secs(5));
        let targets: Vec<String> = {
            let st = shared.st.lock().unwrap();
            let mut out = Vec::new();
            for p in st.peers.values().filter(|p| p.alive) {
                for (id, q) in p.last_status["peers"].as_object().into_iter().flatten() {
                    if *id == st.node_id || st.peers.get(id).is_some_and(|x| x.alive) {
                        continue;
                    }
                    if !q["alive"].as_bool().unwrap_or(false) {
                        continue;
                    }
                    if let Some(addr) = q["control_addr"].as_str().filter(|a| !a.is_empty()) {
                        out.push(addr.to_string());
                    }
                }
            }
            out
        };
        for t in targets {
            dial(&shared, t, control_port, http_port, true);
        }
    }
}

/// Keep one link to a peer alive, dialing the seed first and then every
/// other address the peer last announced, on the seed's port. Coverage is
/// judged by the node id learned on first contact, since a peer may be
/// dialed by an address it does not announce. A seed is dialed for the life
/// of the process, and a discovered target until its node is forgotten and
/// no live peer publishes it.
fn connector(shared: SharedRef, seed: String, control_port: u16, http_port: u16, discovered: bool) {
    let mut attempt: u64 = 0;
    let mut known_id: Option<String> = None;
    let mut uncovered_since: Option<Instant> = None;
    loop {
        let covered = {
            let st = shared.st.lock().unwrap();
            match &known_id {
                Some(id) => st.peers.get(id).map(|p| p.alive).unwrap_or(false),
                None => st.peers.values().any(|p| p.alive && p.control_addr == seed),
            }
        };
        if covered {
            uncovered_since = None;
        } else {
            let since = *uncovered_since.get_or_insert_with(Instant::now);
            if discovered && since.elapsed() > Duration::from_millis(cfg().history_keep_ms) {
                let published = {
                    let st = shared.st.lock().unwrap();
                    st.peers.values().filter(|p| p.alive).any(|p| {
                        p.last_status["peers"]
                            .as_object()
                            .into_iter()
                            .flatten()
                            .any(|(_, q)| q["control_addr"].as_str() == Some(seed.as_str()))
                    })
                };
                if !published {
                    log("peer_dial_stopped", &[("control", seed.clone())]);
                    shared.st.lock().unwrap().dialing.remove(&seed);
                    return;
                }
            }
            attempt += 1;
            let mut last_err: Option<std::io::Error> = None;
            for target in dial_targets(&shared, &seed, known_id.as_deref()) {
                match try_connect(&shared, &target, control_port, http_port) {
                    Ok(peer_id) => {
                        if let Some(id) = peer_id {
                            known_id = Some(id);
                        }
                        last_err = None;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            if let Some(e) = last_err {
                if attempt % 20 == 1 {
                    log(
                        "peer_connect_retry",
                        &[
                            ("seed", seed.clone()),
                            ("attempt", attempt.to_string()),
                            ("error", e.to_string()),
                        ],
                    );
                }
            }
        }
        std::thread::sleep(Duration::from_secs(3));
    }
}

/// The seed, then every address its node last announced on the seed's
/// port. The node is found by id, or by the seed among its addresses.
fn dial_targets(shared: &SharedRef, seed: &str, known_id: Option<&str>) -> Vec<String> {
    let mut out = vec![seed.to_string()];
    let (host, port) = match seed.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.to_string()),
        None => return out,
    };
    let st = shared.st.lock().unwrap();
    let peer = match known_id {
        Some(id) => st.peers.get(id),
        None => st
            .peers
            .values()
            .find(|p| p.control_addr == seed || p.addrs.contains(&host)),
    };
    if let Some(p) = peer {
        for a in p.addrs.iter().filter(|a| !is_loopback(a)) {
            let t = format!("{a}:{port}");
            if !out.contains(&t) {
                out.push(t);
            }
        }
    }
    out
}

/// Returns the peer's node id on contact (whether or not this link was kept).
fn try_connect(
    shared: &SharedRef,
    seed: &str,
    control_port: u16,
    http_port: u16,
) -> std::io::Result<Option<String>> {
    // A dropped SYN would hold this connector for the kernel's retry
    // schedule, minutes during which the peer's other addresses go untried.
    let dial = match crate::testnet::load() {
        Some(net) => {
            let host = seed.rsplit_once(':').map(|(h, _)| h).unwrap_or(seed);
            if !net.link_up(&crate::announce::local_addrs(), host) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("{host} is cut (MENTAT_TEST_NET)"),
                ));
            }
            net.real(host).unwrap_or_else(|| seed.to_string())
        }
        None => seed.to_string(),
    };
    let addr = dial
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "no address"))?;
    let stream = TcpStream::connect_timeout(
        &addr,
        Duration::from_millis(cfg().probe_timeout_ms.max(500)),
    )?;
    set_keepalive(&stream);
    let writer = FrameWriter::new(stream.try_clone()?);
    let mut reader = BufReader::new(stream);

    let (my_id, my_ip) = {
        let st = shared.st.lock().unwrap();
        (st.node_id.clone(), st.node_ip.clone())
    };
    writer.send(
        Msg::PeerHello {
            node_id: my_id.clone(),
            node_ip: my_ip.clone(),
            control_addr: format!("{my_ip}:{control_port}"),
            http_port,
            addrs: crate::announce::local_addrs(),
            addr_tags: crate::announce::local_addr_tags(),
            addr_ifaces: crate::announce::local_addr_ifaces(),
            probes: true,
        },
        1,
        &[],
    )?;
    let (frame, _) = read_frame(&mut reader)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "peer closed the connection at hello",
        )
    })?;
    let (
        peer_id,
        peer_ip,
        peer_control,
        peer_http,
        peer_addrs,
        peer_tags,
        peer_ifaces,
        peer_probes,
    ) = match frame.msg {
        Msg::PeerHelloOk {
            node_id,
            node_ip,
            control_addr,
            http_port,
            addrs,
            addr_tags,
            addr_ifaces,
            probes,
        } => (
            node_id,
            node_ip,
            control_addr,
            http_port,
            addrs,
            addr_tags,
            addr_ifaces,
            probes,
        ),
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected peer hello reply: {other:?}"),
            ))
        }
    };
    if peer_id == my_id {
        // The seed list includes ourselves; harmless, just don't peer.
        return Ok(None);
    }
    // An old daemon replies without its addresses; the dialed seed is then
    // the best control address known, and the http port stays unknown.
    let control = if peer_control.is_empty() {
        seed.to_string()
    } else {
        peer_control
    };
    let link_ip = seed
        .rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .unwrap_or_else(|| seed.to_string());
    if !register_peer(
        shared,
        PeerIdent {
            node_id: peer_id.clone(),
            outbound: true,
            node_ip: peer_ip,
            link_ip,
            addrs: peer_addrs,
            addr_tags: peer_tags,
            addr_ifaces: peer_ifaces,
            probes: peer_probes,
            control_addr: control,
            http_port: peer_http,
        },
        writer.clone(),
    ) {
        // An alive link to this node already exists (e.g. it dialed us
        // first); keep that one and drop this socket.
        return Ok(Some(peer_id));
    }
    peer_loop(shared, reader, writer, peer_id.clone());
    Ok(Some(peer_id))
}

/// Inbound side, called from the daemon accept path on a PeerHello frame.
pub fn accept_peer(
    shared: SharedRef,
    reader: BufReader<TcpStream>,
    writer: FrameWriter,
    link_ip: String,
    hello: (Frame, Vec<u8>),
) {
    let Msg::PeerHello {
        node_id,
        node_ip,
        control_addr,
        http_port,
        addrs,
        addr_tags,
        addr_ifaces,
        probes,
    } = hello.0.msg
    else {
        unreachable!()
    };
    let (my_id, my_ip, my_control, my_http) = {
        let st = shared.st.lock().unwrap();
        (
            st.node_id.clone(),
            st.node_ip.clone(),
            st.gcs_address.clone(),
            st.http_port,
        )
    };
    let _ = writer.send(
        Msg::PeerHelloOk {
            node_id: my_id.clone(),
            node_ip: my_ip,
            control_addr: my_control,
            http_port: my_http,
            addrs: crate::announce::local_addrs(),
            addr_tags: crate::announce::local_addr_tags(),
            addr_ifaces: crate::announce::local_addr_ifaces(),
            probes: true,
        },
        hello.0.req,
        &[],
    );
    if node_id == my_id {
        return;
    }
    if !register_peer(
        &shared,
        PeerIdent {
            node_id: node_id.clone(),
            outbound: false,
            node_ip,
            link_ip,
            addrs,
            addr_tags,
            addr_ifaces,
            probes,
            control_addr,
            http_port,
        },
        writer.clone(),
    ) {
        return;
    }
    peer_loop(&shared, reader, writer, node_id);
}

/// What a peer says about itself in its hello, plus what the link observed.
struct PeerIdent {
    node_id: String,
    /// This daemon dialed the link. The other side accepted it.
    outbound: bool,
    node_ip: String,
    link_ip: String,
    addrs: Vec<String>,
    addr_tags: std::collections::BTreeMap<String, Vec<String>>,
    addr_ifaces: std::collections::BTreeMap<String, String>,
    probes: bool,
    control_addr: String,
    http_port: u16,
}

/// Whether two peer entries describe one box.
///
/// A node id is the hash of an address, so a box that comes back calling
/// itself something else joins as a second peer while the first is left
/// behind. What ties the two together is the address list, which both carry
/// in full and which is the same list.
///
/// Loopback is on every box, so an overlap there identifies nothing and is
/// left out. An empty list matches nothing.
fn same_box(arriving: &[String], existing: &[String]) -> bool {
    arriving
        .iter()
        .filter(|a| !is_loopback(a))
        .any(|a| existing.contains(a))
}

/// Record a link to a peer, or refuse it. Returns whether the caller now
/// owns the link.
///
/// When two links to one node exist, the one dialed by the lower node id
/// wins, which both ends decide alike from who dialed and both ids. A
/// losing arrival is refused and a winning one replaces the old link
/// without a node_leave, since the node is still there.
fn register_peer(shared: &SharedRef, p: PeerIdent, writer: FrameWriter) -> bool {
    let PeerIdent {
        node_id,
        outbound,
        node_ip,
        link_ip,
        addrs,
        addr_tags,
        addr_ifaces,
        probes,
        control_addr,
        http_port,
    } = p;
    let mut st = shared.st.lock().unwrap();
    let my_id = st.node_id.clone();
    let dialer = |outbound: bool| if outbound { &my_id } else { &node_id };
    if let Some(old) = st.peers.get(&node_id) {
        if old.alive {
            if dialer(outbound) >= dialer(old.outbound) {
                return false;
            }
            log(
                "peer_link_replaced",
                &[
                    ("peer", node_id.clone()),
                    (
                        "kept",
                        if outbound { "outbound" } else { "inbound" }.to_string(),
                    ),
                ],
            );
            old.writer.shutdown();
        }
    }
    // A dead entry for this same box under an identity it has stopped
    // using is dropped here. Only dead entries go: two live links to one
    // box is a different situation, and the tie-break above settles it.
    let superseded: Vec<String> = st
        .peers
        .values()
        .filter(|q| !q.alive && q.node_id != node_id && same_box(&addrs, &q.addrs))
        .map(|q| q.node_id.clone())
        .collect();
    for old in superseded {
        st.peers.remove(&old);
        log("peer_superseded", &[("peer", old), ("by", node_id.clone())]);
    }

    // A relink keeps the probed pairs and the last snapshot. The pairs
    // describe cabling, which a dropped control link says nothing about,
    // and discarding them would leave placement blind until the next probe
    // round.
    let (probe_pairs, last_status, was_alive) = st
        .peers
        .get(&node_id)
        .map(|p| (p.probe_pairs.clone(), p.last_status.clone(), p.alive))
        .unwrap_or_default();
    st.peers.insert(
        node_id.clone(),
        PeerInfo {
            node_id: node_id.clone(),
            outbound,
            node_ip: node_ip.clone(),
            link_ip,
            addrs,
            addr_tags,
            addr_ifaces,
            probes,
            probe_pairs,
            control_addr,
            http_port,
            writer,
            alive: true,
            last_seen_ms: now_ms_u64(),
            dead_since_ms: 0,
            stale: false,
            last_status,
        },
    );
    if !was_alive {
        st.emit("node_join", json!({ "peer": node_id, "peer_ip": node_ip }));
    }
    shared.cv.notify_all();
    true
}

fn peer_loop(
    shared: &SharedRef,
    mut reader: BufReader<TcpStream>,
    writer: FrameWriter,
    peer_id: String,
) {
    loop {
        let (frame, _payload) = match read_frame(&mut reader) {
            Ok(Some(fp)) => fp,
            Ok(None) => break,
            Err(e) => {
                log(
                    "peer_read_error",
                    &[("peer", peer_id.clone()), ("error", e.to_string())],
                );
                break;
            }
        };
        match frame.msg {
            Msg::PeerStatus { data } => {
                let mut st = shared.st.lock().unwrap();
                if let Some(p) = st.peers.get_mut(&peer_id) {
                    // The push carries the peer's current addresses. The
                    // hello carried the ones it had when the link came up.
                    let addrs = str_list(&data["addrs"]);
                    if !addrs.is_empty() && addrs != p.addrs {
                        log(
                            "peer_addrs_changed",
                            &[
                                ("peer", peer_id.clone()),
                                ("from", p.addrs.join(",")),
                                ("to", addrs.join(",")),
                            ],
                        );
                        p.addrs = addrs;
                    }
                    if let Some(t) = data["addr_tags"].as_object() {
                        p.addr_tags = t.iter().map(|(k, v)| (k.clone(), str_list(v))).collect();
                    }
                    if let Some(t) = data["addr_ifaces"].as_object() {
                        p.addr_ifaces = t
                            .iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect();
                    }
                    p.last_status = data;
                    p.last_seen_ms = now_ms_u64();
                    if p.stale {
                        p.stale = false;
                        log("peer_recovered", &[("peer", peer_id.clone())]);
                    }
                }
            }
            Msg::PeerEvent { line, .. } => {
                let mut st = shared.st.lock().unwrap();
                st.deliver_peer_event(line);
            }
            Msg::Ping => {
                let _ = writer.send(Msg::Pong, frame.req, &[]);
            }
            Msg::Pong => {
                let mut st = shared.st.lock().unwrap();
                if let Some(p) = st.peers.get_mut(&peer_id) {
                    p.last_seen_ms = now_ms_u64();
                }
            }
            other => log(
                "peer_unexpected_msg",
                &[("peer", peer_id.clone()), ("msg", format!("{other:?}"))],
            ),
        }
    }

    let mut st = shared.st.lock().unwrap();
    // Owned + still alive: the staleness sweeper may already have declared
    // this peer gone (and closed the socket under us) -- don't emit twice.
    let owned = st
        .peers
        .get(&peer_id)
        .map(|p| p.alive && FrameWriter::same_socket(&p.writer, &writer))
        .unwrap_or(false);
    if owned {
        if let Some(p) = st.peers.get_mut(&peer_id) {
            p.alive = false;
            p.dead_since_ms = now_ms_u64();
        }
        st.emit(
            "node_leave",
            json!({ "peer": peer_id, "reason": "link closed" }),
        );
    }
    shared.cv.notify_all();
}

fn str_list(v: &serde_json::Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| a.as_str())
        .map(str::to_string)
        .collect()
}

/// The mesh analog of the agent degrade window: a peer that stops sending
/// (status pushes double as heartbeats) is logged stale after
/// MENTAT_PEER_STALE_AFTER_MS and declared gone after
/// MENTAT_PEER_DEAD_AFTER_MS -- covering wedged-but-connected peers that a
/// clean EOF would never report. The connector keeps re-dialing.
fn staleness_sweeper(shared: SharedRef) {
    let stale_after = cfg().peer_stale_after_ms;
    let dead_after = cfg().peer_dead_after_ms;
    let keep = cfg().history_keep_ms;
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let now = now_ms_u64();
        // A link over a cable the pretend network cut is closed here,
        // since nothing else on one box closes it.
        let cut_links: Vec<String> = match crate::testnet::load() {
            Some(net) => {
                let locals = crate::announce::local_addrs();
                let st = shared.st.lock().unwrap();
                st.peers
                    .values()
                    .filter(|p| p.alive && p.outbound && !net.link_up(&locals, &p.link_ip))
                    .map(|p| p.node_id.clone())
                    .collect()
            }
            None => Vec::new(),
        };
        let mut st = shared.st.lock().unwrap();
        for id in cut_links {
            if let Some(p) = st.peers.get(&id) {
                log(
                    "peer_link_cut",
                    &[("peer", id.clone()), ("link_ip", p.link_ip.clone())],
                );
                p.writer.shutdown();
            }
        }
        let mut gone: Vec<(String, u64)> = Vec::new();
        for p in st.peers.values_mut() {
            if !p.alive {
                continue;
            }
            let silent = now.saturating_sub(p.last_seen_ms);
            if silent >= dead_after {
                p.alive = false;
                p.dead_since_ms = now;
                p.writer.shutdown();
                gone.push((p.node_id.clone(), silent));
            } else if silent >= stale_after && !p.stale {
                p.stale = true;
                log(
                    "peer_stale",
                    &[
                        ("peer", p.node_id.clone()),
                        ("silent_ms", silent.to_string()),
                    ],
                );
            }
        }
        let any_gone = !gone.is_empty();
        for (peer, silent) in gone {
            log(
                "peer_dead",
                &[("peer", peer.clone()), ("silent_ms", silent.to_string())],
            );
            st.emit("node_leave", json!({ "peer": peer, "reason": "stale" }));
        }
        // A dead row goes after MENTAT_HISTORY_KEEP_MS. Its seed connector
        // keeps dialing, so the box rejoins when it returns.
        let forget: Vec<String> = st
            .peers
            .values()
            .filter(|p| !p.alive && now.saturating_sub(p.dead_since_ms) > keep)
            .map(|p| p.node_id.clone())
            .collect();
        for id in forget {
            st.peers.remove(&id);
            log(
                "peer_forgotten",
                &[("peer", id), ("kept_ms", keep.to_string())],
            );
        }
        if any_gone {
            shared.cv.notify_all();
        }
    }
}

/// Head election, committed after MENTAT_ELECTION_HOLD_DOWN_MS of
/// stability. A settled head stays head while it is alive, since a change
/// moves every group. A daemon with no head takes the one its live peers
/// publish, else the lowest live id, and two settled heads that meet
/// resolve to the lower.
fn elector(shared: SharedRef) {
    let hold_down = Duration::from_millis(cfg().election_hold_down_ms);
    // Tick at ~1/5th of the hold-down so short test values still commit in a
    // handful of ticks.
    let tick = Duration::from_millis((cfg().election_hold_down_ms / 5).clamp(100, 1000));
    let mut candidate_since: Option<(String, Instant)> = None;
    loop {
        std::thread::sleep(tick);
        let mut st = shared.st.lock().unwrap();
        let candidate = head_candidate(&st);
        if candidate == st.head_node_id {
            candidate_since = None;
            continue;
        }
        let since = match &candidate_since {
            Some((c, t)) if *c == candidate => *t,
            _ => {
                candidate_since = Some((candidate.clone(), Instant::now()));
                continue;
            }
        };
        if since.elapsed() >= hold_down {
            let old = std::mem::replace(&mut st.head_node_id, candidate.clone());
            st.head_generation += 1;
            let generation = st.head_generation;
            st.emit(
                "head_change",
                json!({ "head": candidate, "previous": old, "generation": generation }),
            );
            crate::daemon::head_moved(&mut st, &old);
            candidate_since = None;
            shared.cv.notify_all();
        }
    }
}

/// The head this daemon should follow, by the rule on `elector`.
fn head_candidate(st: &crate::state::State) -> String {
    let alive = |id: &str| id == st.node_id || st.peers.get(id).is_some_and(|p| p.alive);
    let mut claimed: Vec<String> = st
        .peers
        .values()
        .filter(|p| p.alive)
        .filter_map(|p| p.last_status["head_node_id"].as_str())
        .filter(|h| !h.is_empty() && alive(h))
        .map(str::to_string)
        .collect();
    if !st.head_node_id.is_empty() && alive(&st.head_node_id) {
        claimed.push(st.head_node_id.clone());
    }
    if let Some(h) = claimed.into_iter().min() {
        return h;
    }
    let mut ids: Vec<&str> = st
        .peers
        .values()
        .filter(|p| p.alive)
        .map(|p| p.node_id.as_str())
        .collect();
    ids.push(st.node_id.as_str());
    ids.into_iter().min().unwrap_or_default().to_string()
}

/// Push our snapshot to every live peer every MENTAT_PEER_STATUS_INTERVAL_MS
/// (doubles as the heartbeat the reader side timestamps and the staleness
/// sweeper judges by).
fn status_pusher(shared: SharedRef) {
    let interval = Duration::from_millis(cfg().peer_status_interval_ms.max(50));
    loop {
        std::thread::sleep(interval);
        let (snap, writers): (serde_json::Value, Vec<FrameWriter>) = {
            let st = shared.st.lock().unwrap();
            (
                crate::status::snapshot(&st, None),
                st.peers
                    .values()
                    .filter(|p| p.alive)
                    .map(|p| p.writer.clone())
                    .collect(),
            )
        };
        for w in writers {
            let _ = w.send(Msg::PeerStatus { data: snap.clone() }, 0, &[]);
        }
    }
}

/// Probe which of this node's addresses can reach which of each peer's,
/// one TCP connection per pair, every MENTAT_PROBE_INTERVAL_MS.
///
/// Same-subnet numbering across two fabrics means address arithmetic cannot
/// answer this. Two boxes cabled together and two that merely share a subnet
/// look identical from the routing table, so the only honest answer comes
/// from opening the connection.
///
/// The local bind is the whole point. Reaching a peer address over the LAN
/// says nothing about reaching it over the fabric, so a probe that did not
/// pin its source address would report the routing table's preference and
/// call it topology.
///
/// Peers are probed concurrently, one thread each. A fabric-to-LAN pair
/// fails only at the timeout, and five nodes have enough of those to
/// outrun the interval in one line.
///
/// Rows for a lost local address or an unannounced remote one are dropped
/// after the round. A row that is never re-probed keeps its last `ok`.
fn prober(shared: SharedRef) {
    let interval = Duration::from_millis(cfg().probe_interval_ms.max(200));
    let timeout = Duration::from_millis(cfg().probe_timeout_ms.max(50));
    loop {
        std::thread::sleep(interval);
        let my_id = shared.st.lock().unwrap().node_id.clone();
        let locals = crate::announce::local_addrs();
        // Peers worth probing: alive, probe-answering, and with a control
        // port to aim at. Collected before any connecting so the state lock
        // is never held across a network wait.
        let targets: Vec<(String, u16, Vec<String>)> = {
            let st = shared.st.lock().unwrap();
            st.peers
                .values()
                .filter(|p| p.alive && p.probes && !p.addrs.is_empty())
                .filter_map(|p| {
                    let port: u16 = p.control_addr.rsplit_once(':')?.1.parse().ok()?;
                    Some((p.node_id.clone(), port, p.addrs.clone()))
                })
                .collect()
        };
        let workers: Vec<std::thread::JoinHandle<()>> = targets
            .into_iter()
            .map(|(peer_id, port, remotes)| {
                let shared = shared.clone();
                let my_id = my_id.clone();
                let locals = locals.clone();
                std::thread::spawn(move || {
                    probe_peer(&shared, &my_id, &peer_id, port, &locals, &remotes, timeout)
                })
            })
            .collect();
        for w in workers {
            let _ = w.join();
        }
    }
}

/// One round against one peer: every (local, remote) pair, then the prune.
fn probe_peer(
    shared: &SharedRef,
    my_id: &str,
    peer_id: &str,
    port: u16,
    locals: &[String],
    remotes: &[String],
    timeout: Duration,
) {
    for local in locals {
        for remote in remotes {
            let r = probe_pair(my_id, peer_id, local, remote, port, timeout);
            let now = now_ms_u64();
            let mut st = shared.st.lock().unwrap();
            let Some(p) = st.peers.get_mut(peer_id) else {
                return;
            };
            let cell = p
                .probe_pairs
                .entry(local.clone())
                .or_default()
                .entry(remote.clone())
                .or_insert(PairProbe {
                    ok: false,
                    rtt_ms: 0,
                    last_ok_ms: 0,
                    error: String::new(),
                });
            let was = cell.ok;
            match r {
                Ok(rtt) => {
                    cell.ok = true;
                    cell.rtt_ms = rtt.as_millis() as u64;
                    cell.last_ok_ms = now;
                    cell.error.clear();
                }
                Err(e) => {
                    cell.ok = false;
                    cell.error = e.to_string();
                }
            }
            // One line per transition. The table is read from /status, and
            // a 15 s cadence times four pairs would otherwise be the whole
            // log.
            if was != cell.ok {
                log(
                    "probe_pair",
                    &[
                        ("peer", peer_id.to_string()),
                        ("local", local.clone()),
                        ("remote", remote.clone()),
                        ("ok", cell.ok.to_string()),
                        ("rtt_ms", cell.rtt_ms.to_string()),
                        ("error", cell.error.clone()),
                    ],
                );
            }
        }
    }
    let mut st = shared.st.lock().unwrap();
    if let Some(p) = st.peers.get_mut(peer_id) {
        prune_pairs(&mut p.probe_pairs, locals, remotes);
    }
}

/// Drop rows for addresses outside `locals` and `remotes`.
fn prune_pairs(table: &mut crate::state::ProbeTable, locals: &[String], remotes: &[String]) {
    table.retain(|local, _| locals.contains(local));
    for row in table.values_mut() {
        row.retain(|remote, _| remotes.contains(remote));
    }
}

/// One probe: connect from `local` to `remote:port`, exchange the frames,
/// close. Returns the round trip on success.
fn probe_pair(
    my_id: &str,
    peer_id: &str,
    local: &str,
    remote: &str,
    port: u16,
    timeout: Duration,
) -> std::io::Result<Duration> {
    let started = Instant::now();
    let stream = match crate::testnet::load() {
        Some(net) => {
            if !net.reachable(local, remote) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("{local} -> {remote} is cut (MENTAT_TEST_NET)"),
                ));
            }
            let target = net
                .real(remote)
                .unwrap_or_else(|| format!("{remote}:{port}"));
            let addr = target.to_socket_addrs()?.next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "no address")
            })?;
            TcpStream::connect_timeout(&addr, timeout)?
        }
        None => connect_from(local, remote, port, timeout)?,
    };
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let writer = FrameWriter::new(stream.try_clone()?);
    let mut reader = BufReader::new(stream);
    writer.send(
        Msg::Probe {
            node_id: my_id.to_string(),
            local_addr: local.to_string(),
        },
        1,
        &[],
    )?;
    match read_frame(&mut reader)? {
        Some((frame, _)) => match frame.msg {
            // The reply's identity is checked. Both fabrics are numbered
            // out of the same subnet, so an address that answers is not by
            // itself evidence that the intended node answered.
            Msg::ProbeOk { node_id } if node_id == peer_id => Ok(started.elapsed()),
            Msg::ProbeOk { node_id } => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("node {node_id} answered, expected {peer_id}"),
            )),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected probe reply: {other:?}"),
            )),
        },
        None => Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "peer closed the connection before answering the probe",
        )),
    }
}

/// TCP connect with the source address pinned, and with a deadline.
///
/// Neither half comes free from std: TcpStream::connect picks the source
/// address by routing table, and connect_timeout cannot bind one. So the
/// socket is built by hand -- bind, then a non-blocking connect polled to the
/// deadline, because a dropped SYN would otherwise hold this thread for the
/// kernel's retry schedule, minutes past the probe interval.
///
/// IPv4 only, matching what announce selects.
fn connect_from(
    local: &str,
    remote: &str,
    port: u16,
    timeout: Duration,
) -> std::io::Result<TcpStream> {
    use std::os::fd::FromRawFd;

    let bad = |what: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{what} is not an IPv4 address"),
        )
    };
    let local: std::net::Ipv4Addr = local.parse().map_err(|_| bad(local))?;
    let remote: std::net::Ipv4Addr = remote.parse().map_err(|_| bad(remote))?;

    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Owned from here on, so every early return closes it.
        let sock = TcpStream::from_raw_fd(fd);

        let mut addr: libc::sockaddr_in = std::mem::zeroed();
        addr.sin_family = libc::AF_INET as libc::sa_family_t;
        addr.sin_addr.s_addr = u32::from(local).to_be();
        let len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        if libc::bind(fd, &addr as *const _ as *const libc::sockaddr, len) < 0 {
            return Err(std::io::Error::last_os_error());
        }

        sock.set_nonblocking(true)?;
        addr.sin_addr.s_addr = u32::from(remote).to_be();
        addr.sin_port = port.to_be();
        if libc::connect(fd, &addr as *const _ as *const libc::sockaddr, len) < 0 {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(e);
            }
            let mut pfd = libc::pollfd {
                fd,
                events: libc::POLLOUT,
                revents: 0,
            };
            // A signal cutting the wait short is not a failed connect, and
            // recording one would mark a good pair failed for a round.
            loop {
                match libc::poll(&mut pfd, 1, timeout.as_millis() as libc::c_int) {
                    0 => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("connect timed out after {} ms", timeout.as_millis()),
                        ))
                    }
                    n if n < 0 => {
                        let e = std::io::Error::last_os_error();
                        if e.kind() == std::io::ErrorKind::Interrupted {
                            continue;
                        }
                        return Err(e);
                    }
                    _ => break,
                }
            }
            // POLLOUT says the connect finished. SO_ERROR says whether it
            // succeeded.
            let mut err: libc::c_int = 0;
            let mut elen = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
            if libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut err as *mut _ as *mut libc::c_void,
                &mut elen,
            ) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if err != 0 {
                return Err(std::io::Error::from_raw_os_error(err));
            }
        }
        sock.set_nonblocking(false)?;
        Ok(sock)
    }
}

#[cfg(test)]
mod tests {
    use super::same_box;

    fn v(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    /// The reported tombstone: one box registered as 192.168.1.93, came back
    /// identifying as 10.103.0.93, and the first entry stayed dead in every
    /// peer's table for as long as the process lived. Both entries carry the
    /// same address list, which is what says they are one box.
    #[test]
    fn a_renumbered_node_matches_its_own_old_entry() {
        let arriving = v(&["192.168.1.93", "10.103.0.93"]);
        let old = v(&["192.168.1.93", "10.103.0.93"]);
        assert!(same_box(&arriving, &old));
    }

    /// One address in common is enough: an entry recorded before a box had
    /// its fabric address lists only the address it had then.
    #[test]
    fn one_address_in_common_is_enough() {
        assert!(same_box(
            &v(&["192.168.1.93", "10.103.0.93"]),
            &v(&["192.168.1.93"])
        ));
    }

    /// Different boxes keep their entries, which is what stops this from
    /// eating the mesh.
    #[test]
    fn separate_boxes_do_not_match() {
        assert!(!same_box(
            &v(&["192.168.1.93", "10.103.0.93"]),
            &v(&["192.168.1.77", "10.100.0.2"])
        ));
    }

    /// Loopback is on every box, so two nodes that both list it are not
    /// thereby one node.
    #[test]
    fn loopback_alone_identifies_nothing() {
        assert!(!same_box(
            &v(&["127.0.0.1", "10.103.0.93"]),
            &v(&["127.0.0.1"])
        ));
        assert!(!same_box(&v(&["::1"]), &v(&["::1", "10.100.0.2"])));
        // A real address alongside it still matches.
        assert!(same_box(
            &v(&["127.0.0.1", "10.103.0.93"]),
            &v(&["127.0.0.1", "10.103.0.93"])
        ));
    }

    #[test]
    fn an_empty_list_matches_nothing() {
        assert!(!same_box(&v(&[]), &v(&["10.100.0.2"])));
        assert!(!same_box(&v(&["10.100.0.2"]), &v(&[])));
    }
}
