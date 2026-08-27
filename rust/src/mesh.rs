//! The daemon mesh: persistent links between mentatd instances, head
//! election, snapshot exchange, and event replication.
//!
//! Deliberately NOT a consensus protocol: state is soft, every daemon serves
//! its own clients/agents autonomously, and the mesh adds three things --
//! a merged cluster view from any daemon, a replicated /events stream, and a
//! deterministic head designation (lowest node id, with hold-down) published
//! as head_change events. Rendezvous authority still follows RAY_ADDRESS;
//! moving it onto the elected head is a later phase, on purpose.

use std::io::BufReader;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::cfg;
use crate::daemon::set_keepalive;
use crate::logfmt::log;
use crate::proto::{read_frame, Frame, Msg};
use crate::state::{now_ms_u64, FrameWriter, PeerInfo, SharedRef};

pub fn start(shared: SharedRef, seeds: Vec<String>, control_port: u16, http_port: u16) {
    for seed in seeds {
        let shared = shared.clone();
        std::thread::spawn(move || connector(shared, seed, control_port, http_port));
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || elector(shared));
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || staleness_sweeper(shared));
    }
    std::thread::spawn(move || status_pusher(shared));
}

/// Keep one link to a seed alive. A peer may be dialed by a different address
/// than it announces (the pair talks over the QSFP subnet, n3 over the
/// LAN), so "already covered" is judged by the node id learned on first
/// contact, never by comparing address strings.
fn connector(shared: SharedRef, seed: String, control_port: u16, http_port: u16) {
    let mut attempt: u64 = 0;
    let mut known_id: Option<String> = None;
    loop {
        let covered = {
            let st = shared.st.lock().unwrap();
            match &known_id {
                Some(id) => st.peers.get(id).map(|p| p.alive).unwrap_or(false),
                None => st.peers.values().any(|p| p.alive && p.control_addr == seed),
            }
        };
        if !covered {
            attempt += 1;
            match try_connect(&shared, &seed, control_port, http_port) {
                Ok(peer_id) => {
                    if let Some(id) = peer_id {
                        known_id = Some(id);
                    }
                }
                Err(e) => {
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
        }
        std::thread::sleep(Duration::from_secs(3));
    }
}

/// Returns the peer's node id on contact (whether or not this link was kept).
fn try_connect(
    shared: &SharedRef,
    seed: &str,
    control_port: u16,
    http_port: u16,
) -> std::io::Result<Option<String>> {
    let stream = TcpStream::connect(seed)?;
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
        },
        1,
        &[],
    )?;
    let (frame, _) = read_frame(&mut reader)?.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF at peer hello")
    })?;
    let (peer_id, peer_ip, peer_control, peer_http, peer_addrs, peer_tags) = match frame.msg {
        Msg::PeerHelloOk {
            node_id,
            node_ip,
            control_addr,
            http_port,
            addrs,
            addr_tags,
        } => (node_id, node_ip, control_addr, http_port, addrs, addr_tags),
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
            node_ip: peer_ip,
            link_ip,
            addrs: peer_addrs,
            addr_tags: peer_tags,
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
            node_ip,
            link_ip,
            addrs,
            addr_tags,
            control_addr,
            http_port,
        },
        writer.clone(),
    ) {
        return;
    }
    peer_loop(&shared, reader, writer, node_id);
}

/// Keep-first semantics: an alive existing link wins and the new one is
/// refused (returns false). Replacing a healthy link would let two daemons
/// that dial each other under different addresses churn links forever.
/// What a peer says about itself in its hello, plus what the link observed.
struct PeerIdent {
    node_id: String,
    node_ip: String,
    link_ip: String,
    addrs: Vec<String>,
    addr_tags: std::collections::BTreeMap<String, Vec<String>>,
    control_addr: String,
    http_port: u16,
}

fn register_peer(shared: &SharedRef, p: PeerIdent, writer: FrameWriter) -> bool {
    let PeerIdent {
        node_id,
        node_ip,
        link_ip,
        addrs,
        addr_tags,
        control_addr,
        http_port,
    } = p;
    let mut st = shared.st.lock().unwrap();
    if let Some(old) = st.peers.get(&node_id) {
        if old.alive {
            return false;
        }
    }
    st.peers.insert(
        node_id.clone(),
        PeerInfo {
            node_id: node_id.clone(),
            node_ip: node_ip.clone(),
            link_ip,
            addrs,
            addr_tags,
            control_addr,
            http_port,
            writer,
            alive: true,
            last_seen_ms: now_ms_u64(),
            stale: false,
            last_status: serde_json::Value::Null,
        },
    );
    st.emit("node_join", json!({ "peer": node_id, "peer_ip": node_ip }));
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
        }
        st.emit(
            "node_leave",
            json!({ "peer": peer_id, "reason": "link closed" }),
        );
    }
    shared.cv.notify_all();
}

/// The mesh analog of the agent degrade window: a peer that stops sending
/// (status pushes double as heartbeats) is logged stale after
/// MENTAT_PEER_STALE_AFTER_MS and declared gone after
/// MENTAT_PEER_DEAD_AFTER_MS -- covering wedged-but-connected peers that a
/// clean EOF would never report. The connector keeps re-dialing.
fn staleness_sweeper(shared: SharedRef) {
    let stale_after = cfg().peer_stale_after_ms;
    let dead_after = cfg().peer_dead_after_ms;
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let now = now_ms_u64();
        let mut st = shared.st.lock().unwrap();
        let mut gone: Vec<(String, u64)> = Vec::new();
        for p in st.peers.values_mut() {
            if !p.alive {
                continue;
            }
            let silent = now.saturating_sub(p.last_seen_ms);
            if silent >= dead_after {
                p.alive = false;
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
        if any_gone {
            shared.cv.notify_all();
        }
    }
}

/// Deterministic head: lowest node id among self + live peers, committed only
/// after MENTAT_ELECTION_HOLD_DOWN_MS of stability so a flapping link cannot
/// thrash the designation.
fn elector(shared: SharedRef) {
    let hold_down = Duration::from_millis(cfg().election_hold_down_ms);
    // Tick at ~1/5th of the hold-down so short test values still commit in a
    // handful of ticks.
    let tick = Duration::from_millis((cfg().election_hold_down_ms / 5).clamp(100, 1000));
    let mut candidate_since: Option<(String, Instant)> = None;
    loop {
        std::thread::sleep(tick);
        let mut st = shared.st.lock().unwrap();
        let mut ids: Vec<&str> = st
            .peers
            .values()
            .filter(|p| p.alive)
            .map(|p| p.node_id.as_str())
            .collect();
        ids.push(st.node_id.as_str());
        let candidate = ids.iter().min().unwrap().to_string();
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
            candidate_since = None;
        }
    }
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
