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
use crate::state::{is_loopback, now_ms_u64, FrameWriter, PairProbe, PeerInfo, SharedRef};

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
    {
        let shared = shared.clone();
        std::thread::spawn(move || prober(shared));
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

fn register_peer(shared: &SharedRef, p: PeerIdent, writer: FrameWriter) -> bool {
    let PeerIdent {
        node_id,
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
    if let Some(old) = st.peers.get(&node_id) {
        if old.alive {
            return false;
        }
    }
    // A dead entry for this same box under an identity it has stopped
    // using is dropped here. Nothing else removes a peer, so it would sit
    // in /status for the life of the process as a node that never came
    // back. Only dead entries go: two live links to one box is a different
    // situation, and keep-first above already settles it.
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

    // A relink keeps the probed pairs. They describe cabling, which a
    // dropped control link says nothing about, and discarding them would
    // leave placement blind until the next probe round.
    let probe_pairs = st
        .peers
        .get(&node_id)
        .map(|p| p.probe_pairs.clone())
        .unwrap_or_default();
    st.peers.insert(
        node_id.clone(),
        PeerInfo {
            node_id: node_id.clone(),
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
/// Pairs are probed one at a time, so a round costs up to
/// MENTAT_PROBE_TIMEOUT_MS per failing pair and the effective cadence is
/// whichever is longer. A cluster with a dead fabric therefore refreshes its
/// table more slowly than one with a live one, which the question being
/// asked can afford.
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
        for (peer_id, port, remotes) in targets {
            for local in &locals {
                for remote in &remotes {
                    let r = probe_pair(&my_id, &peer_id, local, remote, port, timeout);
                    let now = now_ms_u64();
                    let mut st = shared.st.lock().unwrap();
                    let Some(p) = st.peers.get_mut(&peer_id) else {
                        continue;
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
                    // One line per transition. The table is read from
                    // /status, and a 15 s cadence times four pairs would
                    // otherwise be the whole log.
                    if was != cell.ok {
                        log(
                            "probe_pair",
                            &[
                                ("peer", peer_id.clone()),
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
        }
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
    let stream = connect_from(local, remote, port, timeout)?;
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
