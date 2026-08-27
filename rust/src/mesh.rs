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
use std::time::Duration;

use serde_json::json;

use crate::daemon::set_keepalive;
use crate::logfmt::log;
use crate::proto::{read_frame, Frame, Msg};
use crate::state::{now_ms_u64, FrameWriter, PeerInfo, SharedRef};

const HOLD_DOWN_TICKS: u32 = 5; // seconds of stability before a head change

pub fn start(shared: SharedRef, seeds: Vec<String>, control_port: u16, http_port: u16) {
    for seed in seeds {
        let shared = shared.clone();
        std::thread::spawn(move || connector(shared, seed, control_port, http_port));
    }
    {
        let shared = shared.clone();
        std::thread::spawn(move || elector(shared));
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
        },
        1,
        &[],
    )?;
    let (frame, _) = read_frame(&mut reader)?.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF at peer hello")
    })?;
    let (peer_id, peer_ip) = match frame.msg {
        Msg::PeerHelloOk { node_id, node_ip } => (node_id, node_ip),
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
    if !register_peer(
        shared,
        peer_id.clone(),
        peer_ip,
        seed.to_string(),
        0,
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
    hello: (Frame, Vec<u8>),
) {
    let Msg::PeerHello {
        node_id,
        node_ip,
        control_addr,
        http_port,
    } = hello.0.msg
    else {
        unreachable!()
    };
    let (my_id, my_ip) = {
        let st = shared.st.lock().unwrap();
        (st.node_id.clone(), st.node_ip.clone())
    };
    let _ = writer.send(
        Msg::PeerHelloOk {
            node_id: my_id.clone(),
            node_ip: my_ip,
        },
        hello.0.req,
        &[],
    );
    if node_id == my_id {
        return;
    }
    if !register_peer(
        &shared,
        node_id.clone(),
        node_ip,
        control_addr,
        http_port,
        writer.clone(),
    ) {
        return;
    }
    peer_loop(&shared, reader, writer, node_id);
}

/// Keep-first semantics: an alive existing link wins and the new one is
/// refused (returns false). Replacing a healthy link would let two daemons
/// that dial each other under different addresses churn links forever.
fn register_peer(
    shared: &SharedRef,
    node_id: String,
    node_ip: String,
    control_addr: String,
    http_port: u16,
    writer: FrameWriter,
) -> bool {
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
            control_addr,
            http_port,
            writer,
            alive: true,
            last_seen_ms: now_ms_u64(),
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
    let owned = st
        .peers
        .get(&peer_id)
        .map(|p| FrameWriter::same_socket(&p.writer, &writer))
        .unwrap_or(false);
    if owned {
        if let Some(p) = st.peers.get_mut(&peer_id) {
            p.alive = false;
        }
        st.emit("node_leave", json!({ "peer": peer_id }));
    }
    shared.cv.notify_all();
}

/// Deterministic head: lowest node id among self + live peers, committed only
/// after HOLD_DOWN_TICKS seconds of stability so a flapping link cannot
/// thrash the designation.
fn elector(shared: SharedRef) {
    let mut candidate_streak: Option<(String, u32)> = None;
    loop {
        std::thread::sleep(Duration::from_secs(1));
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
            candidate_streak = None;
            continue;
        }
        let streak = match &mut candidate_streak {
            Some((c, n)) if *c == candidate => {
                *n += 1;
                *n
            }
            _ => {
                candidate_streak = Some((candidate.clone(), 1));
                1
            }
        };
        if streak >= HOLD_DOWN_TICKS {
            let old = std::mem::replace(&mut st.head_node_id, candidate.clone());
            st.head_generation += 1;
            let generation = st.head_generation;
            st.emit(
                "head_change",
                json!({ "head": candidate, "previous": old, "generation": generation }),
            );
            candidate_streak = None;
        }
    }
}

/// Push our snapshot to every live peer every 2s (doubles as a keepalive the
/// reader side timestamps).
fn status_pusher(shared: SharedRef) {
    loop {
        std::thread::sleep(Duration::from_secs(2));
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
