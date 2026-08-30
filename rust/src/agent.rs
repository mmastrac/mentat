//! The agent runs inside each model container. It registers this container's
//! GPUs with the local mentatd under a group name and is the only component
//! that spawns actor processes -- actors must run the container's own Python
//! environment, so spawn stays in here while cluster brains stay in mentatd.
//!
//! The register loop retries forever. That is the fix for the old
//! `ray start --address` behavior that forced head-first ordering.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::BufReader;
use std::net::TcpStream;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::cfg;
use crate::daemon::set_keepalive;
use crate::gpu::detect_gpus;
use crate::logfmt::log;
use crate::proto::{read_frame, Msg, ResumeActor, ServicePort};
use crate::state::{local_ip_toward, FrameWriter, UnixFrameWriter};

pub struct AgentOpts {
    pub daemon_addr: String,
    pub group: String,
}

struct HostActor {
    name: String,
    /// 0 until the process is actually forked. Never pass 0 to kill(2): with
    /// a negated pgid that would target our own process group.
    pid: u32,
    gpu_ids: Vec<u32>,
    host: Option<UnixFrameWriter>,
    pending_refs: HashSet<String>,
    /// Calls that arrived before the host connected, drained in arrival
    /// order at connect time -- actor calls are ordered in real ray.
    queued_calls: Vec<(String, String, Vec<u8>)>,
    /// A kill that arrived before the pid was known; honored right after fork.
    kill_requested: bool,
}

/// What this container announces: whole URLs, and ports whose host the
/// consumer resolves.
struct Services {
    urls: BTreeMap<String, String>,
    ports: BTreeMap<String, ServicePort>,
}

struct AgentShared {
    daemon: Mutex<Option<FrameWriter>>,
    actors: Mutex<HashMap<String, HostActor>>,
    sock_dir: String,
    /// Findings about announced services, carried on every register so a
    /// reconnect does not lose them.
    service_notes: Mutex<BTreeMap<String, String>>,
    /// Daemon-bound messages (results, exits) that could not be delivered
    /// while the daemon link was down. Drained in order right after the next
    /// successful register so nothing from an outage is silently lost.
    unsent: Mutex<Vec<(Msg, Vec<u8>)>>,
}

/// Send to the daemon if the link is up, otherwise buffer for the reconnect.
fn send_daemon(shared: &AgentShared, msg: Msg, payload: &[u8]) {
    let writer = shared.daemon.lock().unwrap().clone();
    let sent = writer
        .map(|w| w.send(msg.clone(), 0, payload).is_ok())
        .unwrap_or(false);
    if !sent {
        shared.unsent.lock().unwrap().push((msg, payload.to_vec()));
    }
}

pub fn run(opts: AgentOpts) -> ! {
    let container = std::env::var("CONTAINER_NAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(crate::daemon::hostname);
    let node_ip = std::env::var("MENTAT_NODE_IP")
        .ok()
        .or_else(|| std::env::var("VLLM_HOST_IP").ok())
        .filter(|s| !s.is_empty())
        .or_else(|| local_ip_toward(&opts.daemon_addr))
        .unwrap_or_default();
    // The node ip is part of the identity: both ranks of a TP pair run a
    // container named the same thing (glm53 on both boxes), and identical
    // agent ids made their registrations replace each other in a loop on
    // first deployment.
    let agent_id = format!("{}@{}@{}", opts.group, container, node_ip);
    let gpus = detect_gpus();
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    let sock_dir = std::env::var("MENTAT_SOCK_DIR").unwrap_or_else(|_| "/tmp/mentat".into());
    let _ = std::fs::create_dir_all(&sock_dir);
    let (urls, ports) = announced_services();
    let services = Services { urls, ports };

    log(
        "agent_start",
        &[
            ("agent", agent_id.clone()),
            ("group", opts.group.clone()),
            ("daemon", opts.daemon_addr.clone()),
            ("node_ip", node_ip.clone()),
            ("gpus", format!("{gpus:?}")),
            ("services", format!("{:?}", services.urls)),
            ("service_ports", format!("{:?}", services.ports)),
        ],
    );

    let shared = Arc::new(AgentShared {
        daemon: Mutex::new(None),
        actors: Mutex::new(HashMap::new()),
        sock_dir,
        service_notes: Mutex::new(BTreeMap::new()),
        unsent: Mutex::new(Vec::new()),
    });
    watch_service_binds(&shared, &services);

    let mut attempt: u64 = 0;
    loop {
        attempt += 1;
        match serve_once(
            &shared, &opts, &agent_id, &container, &node_ip, &gpus, cpus, &services,
        ) {
            Ok(()) => log("agent_daemon_link_closed", &[("agent", agent_id.clone())]),
            Err(e) => {
                if attempt % 10 == 1 {
                    log(
                        "agent_connect_retry",
                        &[
                            ("agent", agent_id.clone()),
                            ("daemon", opts.daemon_addr.clone()),
                            ("attempt", attempt.to_string()),
                            ("error", e.to_string()),
                        ],
                    );
                }
            }
        }
        *shared.daemon.lock().unwrap() = None;
        std::thread::sleep(Duration::from_millis(if attempt < 5 { 1000 } else { 5000 }));
    }
}

/// One MENTAT_*_API value, in either of the two forms it may take.
#[derive(Debug, Clone, PartialEq)]
enum Announcement {
    /// A whole URL, used exactly as written. The operator named a host, so
    /// that host is the answer and nothing re-derives it.
    Url(String),
    /// A port and a path with the host left open, for the consumer to
    /// resolve against this node's addresses.
    Port { port: u16, path: String },
}

/// Read one MENTAT_*_API value.
///
///     http://10.0.0.1:8000/v1   one address, verbatim
///     http://0.0.0.0:8000/v1    every address this node answers on
///     8000/v1                   the same, said shorter
///
/// The wildcard host is what the API server was told to bind, so writing it
/// here says the same thing to the router that `--host 0.0.0.0` says to
/// uvicorn. Anything that parses as neither is passed through verbatim: a
/// value this function does not understand is still the operator's, and
/// refusing it would drop an endpoint that used to announce.
fn parse_announcement(v: &str) -> Announcement {
    let verbatim = || Announcement::Url(v.to_string());
    let split_path = |rest: &str| -> Option<(u16, String)> {
        let (port, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        Some((port.parse().ok()?, path.to_string()))
    };
    if let Some(rest) = v.strip_prefix("http://") {
        let Some(rest) = rest.strip_prefix("0.0.0.0:") else {
            return verbatim();
        };
        return match split_path(rest) {
            Some((port, path)) => Announcement::Port { port, path },
            None => verbatim(),
        };
    }
    if v.contains("://") {
        return verbatim();
    }
    match split_path(v) {
        Some((port, path)) => Announcement::Port { port, path },
        None => verbatim(),
    }
}

/// Service endpoints this container announces, read once at agent start.
/// The entrypoints export these right before `ray start`. An agent without
/// them registers exactly as before.
fn announced_services() -> (BTreeMap<String, String>, BTreeMap<String, ServicePort>) {
    let (mut urls, mut ports) = (BTreeMap::new(), BTreeMap::new());
    for (var, key) in [("MENTAT_OPENAI_API", "openai"), ("MENTAT_MCP_API", "mcp")] {
        let Ok(v) = std::env::var(var) else { continue };
        if v.is_empty() {
            continue;
        }
        match parse_announcement(&v) {
            Announcement::Url(u) => {
                urls.insert(key.to_string(), u);
            }
            Announcement::Port { port, path } => {
                ports.insert(key.to_string(), ServicePort { port, path });
            }
        }
    }
    (urls, ports)
}

/// Watch each port-announced service until its server binds, then say so
/// once if it bound narrowly.
///
/// The port-only form promises the router that every one of this node's
/// addresses reaches the service, which holds only while the API server
/// listens on the wildcard address. A server started with `--host
/// 10.100.0.1` breaks that promise, and the symptom -- an endpoint that
/// probes fine from one box and refuses from another -- reads like a
/// network fault. This turns it into a sentence.
///
/// It only warns. The router's probe stays the only thing that admits an
/// endpoint, so a finding here can explain a failure but never cause one.
/// Verbatim URLs are skipped: naming a host is the operator saying which
/// address to use.
fn watch_service_binds(shared: &Arc<AgentShared>, services: &Services) {
    for (name, sp) in &services.ports {
        let (shared, name, port) = (shared.clone(), name.clone(), sp.port);
        std::thread::spawn(move || {
            // The API server binds minutes after `ray start` returns, so
            // this waits rather than sampling once. Ten minutes covers a
            // cold weight load. Past that the container has a bigger
            // problem than its bind address.
            let give_up = Instant::now() + Duration::from_secs(600);
            let mut ever_listened = false;
            let mut reported: Option<String> = None;
            loop {
                std::thread::sleep(Duration::from_secs(2));
                let bound = listening_addrs_for(port);
                if bound.is_empty() {
                    if !ever_listened && Instant::now() > give_up {
                        log(
                            "service_never_listened",
                            &[("service", name.clone()), ("port", port.to_string())],
                        );
                        return;
                    }
                    // A server that stopped listening says nothing about how
                    // it will bind when it comes back, so the last finding
                    // stands until a new bind contradicts it.
                    continue;
                }
                ever_listened = true;
                // Any wildcard listener keeps the promise, whatever else is
                // also bound.
                let wide = bound.iter().any(|a| a == "0.0.0.0" || a == "::");
                let note = (!wide).then(|| format!("bound to {} only", bound.join(",")));
                if note == reported {
                    continue;
                }
                // The watch outlives the first answer: a server that
                // restarts onto the wildcard address must stop carrying
                // "bound to 10.100.0.1 only" into every later probe failure.
                if let Some(n) = &note {
                    log(
                        "service_bind_narrow",
                        &[
                            ("service", name.clone()),
                            ("port", port.to_string()),
                            ("bound", bound.join(",")),
                            (
                                "hint",
                                "announced as a port, so every address of this node \
                                 was promised. Bind 0.0.0.0 or announce the URL"
                                    .to_string(),
                            ),
                        ],
                    );
                    shared
                        .service_notes
                        .lock()
                        .unwrap()
                        .insert(name.clone(), n.clone());
                } else {
                    log(
                        "service_bind_widened",
                        &[("service", name.clone()), ("port", port.to_string())],
                    );
                    shared.service_notes.lock().unwrap().remove(&name);
                }
                send_daemon(
                    &shared,
                    Msg::ServiceNote {
                        service: name.clone(),
                        note: note.clone().unwrap_or_default(),
                    },
                    &[],
                );
                reported = note;
            }
        });
    }
}

/// Addresses listening on `port`, from the kernel's own table.
///
/// The agent shares the container's network namespace with the API server,
/// which is what makes /proc/net/tcp the right place to ask. Connecting to
/// the port would only answer that something listens.
/// Empty means nothing listens yet, or the table could not be read (the
/// dev boxes are macOS, which has no procfs -- there the check silently
/// does nothing, which is correct for a warning).
fn listening_addrs_for(port: u16) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (path, v6) in [("/proc/net/tcp", false), ("/proc/net/tcp6", true)] {
        if let Ok(table) = std::fs::read_to_string(path) {
            out.extend(listening_addrs(&table, v6, port));
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Parse one /proc/net/tcp table: the local addresses in LISTEN state on
/// `port`.
///
/// The `local_address` column is `HEX:HEX`, the address in host byte order
/// per 32-bit word -- little-endian on every machine this runs on, which is
/// why the bytes come back reversed. `st` is 0A for TCP_LISTEN.
fn listening_addrs(table: &str, v6: bool, port: u16) -> Vec<String> {
    let mut out = Vec::new();
    for line in table.lines().skip(1) {
        let mut f = line.split_whitespace();
        let (_, local, _, st) = match (f.next(), f.next(), f.next(), f.next()) {
            (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
            _ => continue,
        };
        if st != "0A" {
            continue;
        }
        let Some((addr, p)) = local.split_once(':') else {
            continue;
        };
        if u16::from_str_radix(p, 16).ok() != Some(port) {
            continue;
        }
        let Some(a) = parse_proc_addr(addr, v6) else {
            continue;
        };
        out.push(a);
    }
    out
}

/// One hex `local_address` field as an address string.
fn parse_proc_addr(hex: &str, v6: bool) -> Option<String> {
    let want = if v6 { 32 } else { 8 };
    if hex.len() != want {
        return None;
    }
    // Each 32-bit word is little-endian. Reversing its four bytes gives the
    // network order the address is written in.
    let mut bytes = Vec::with_capacity(want / 2);
    for word in hex.as_bytes().chunks(8) {
        let mut w = [0u8; 4];
        for (i, b) in word.chunks(2).enumerate() {
            w[i] = u8::from_str_radix(std::str::from_utf8(b).ok()?, 16).ok()?;
        }
        w.reverse();
        bytes.extend_from_slice(&w);
    }
    if v6 {
        let octets: [u8; 16] = bytes.try_into().ok()?;
        Some(std::net::Ipv6Addr::from(octets).to_string())
    } else {
        let octets: [u8; 4] = bytes.try_into().ok()?;
        Some(std::net::Ipv4Addr::from(octets).to_string())
    }
}

#[allow(clippy::too_many_arguments)]
fn serve_once(
    shared: &Arc<AgentShared>,
    opts: &AgentOpts,
    agent_id: &str,
    container: &str,
    node_ip: &str,
    gpus: &[u32],
    cpus: u32,
    services: &Services,
) -> std::io::Result<()> {
    let stream = TcpStream::connect(&opts.daemon_addr)?;
    set_keepalive(&stream);
    let writer = FrameWriter::new(stream.try_clone()?);
    let mut reader = BufReader::new(stream);

    let resume: Vec<ResumeActor> = {
        let actors = shared.actors.lock().unwrap();
        actors
            .iter()
            .map(|(id, a)| ResumeActor {
                actor_id: id.clone(),
                name: a.name.clone(),
                gpu_ids: a.gpu_ids.clone(),
                pid: a.pid,
                pending_refs: a.pending_refs.iter().cloned().collect(),
            })
            .collect()
    };

    // Refs whose results sit in the unsent buffer: named in the register so
    // the daemon keeps them pending until the drain below delivers them.
    let unacked_refs: Vec<String> = shared
        .unsent
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(m, _)| match m {
            Msg::ActorResult { ref_id, .. } => Some(ref_id.clone()),
            _ => None,
        })
        .collect();

    writer.send(
        Msg::AgentRegister {
            agent_id: agent_id.to_string(),
            group: opts.group.clone(),
            node_ip: node_ip.to_string(),
            gpus: gpus.to_vec(),
            gpu_vendor: crate::proto::default_gpu_vendor(),
            cpus,
            container: container.to_string(),
            pid: std::process::id(),
            services: services.urls.clone(),
            services_ports: services.ports.clone(),
            service_notes: shared.service_notes.lock().unwrap().clone(),
            resume,
            unacked_refs,
        },
        1,
        &[],
    )?;

    let (first, _) = read_frame(&mut reader)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF at register"))?;
    match first.msg {
        Msg::AgentRegisterOk { node_id } => {
            log(
                "agent_registered",
                &[("agent", agent_id.to_string()), ("node_id", node_id)],
            );
        }
        Msg::Err { error } => {
            return Err(std::io::Error::other(error));
        }
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected register reply: {other:?}"),
            ));
        }
    }
    *shared.daemon.lock().unwrap() = Some(writer.clone());

    // Deliver everything buffered during the outage, oldest first, before any
    // new traffic. A failure re-buffers the remainder. The read loop will
    // notice the dead link and retry the whole register.
    {
        let mut unsent = shared.unsent.lock().unwrap();
        let backlog: Vec<(Msg, Vec<u8>)> = unsent.drain(..).collect();
        let mut it = backlog.into_iter();
        for (msg, payload) in it.by_ref() {
            if writer.send(msg.clone(), 0, &payload).is_err() {
                unsent.push((msg, payload));
                unsent.extend(it);
                break;
            }
        }
    }

    // Ping keeps the connection exercised (MENTAT_AGENT_PING_INTERVAL_MS) so
    // a dead daemon is noticed within seconds. A failed send shuts the socket
    // down, which unblocks the read loop below.
    {
        let writer = writer.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(cfg().agent_ping_interval_ms.max(100)));
            if writer.send(Msg::Ping, 0, &[]).is_err() {
                writer.shutdown();
                break;
            }
        });
    }

    loop {
        let (frame, payload) = match read_frame(&mut reader)? {
            Some(fp) => fp,
            None => return Ok(()),
        };
        match frame.msg {
            Msg::Spawn {
                actor_id,
                name,
                env,
                gpu_ids,
                ..
            } => {
                // Insert the entry BEFORE the spawn thread does its slow work:
                // the driver's first .remote() call can arrive within
                // microseconds of CreateActorOk, and an absent map entry would
                // bounce it with "no such actor".
                shared.actors.lock().unwrap().insert(
                    actor_id.clone(),
                    HostActor {
                        name: name.clone(),
                        pid: 0,
                        gpu_ids: gpu_ids.clone(),
                        host: None,
                        pending_refs: HashSet::new(),
                        queued_calls: Vec::new(),
                        kill_requested: false,
                    },
                );
                let shared = shared.clone();
                let writer = writer.clone();
                std::thread::spawn(move || {
                    spawn_actor(shared, writer, actor_id, name, env, gpu_ids, payload)
                });
            }
            Msg::Kill { actor_id } => {
                kill_actor_process(shared, &actor_id);
            }
            Msg::CallActor {
                actor_id,
                ref_id,
                method,
            } => {
                let mut actors = shared.actors.lock().unwrap();
                let sent = match actors.get_mut(&actor_id) {
                    Some(a) => {
                        a.pending_refs.insert(ref_id.clone());
                        match &a.host {
                            Some(h) => h
                                .send(
                                    Msg::HostCall {
                                        ref_id: ref_id.clone(),
                                        method: method.clone(),
                                    },
                                    0,
                                    &payload,
                                )
                                .is_ok(),
                            // Host not connected yet (the handshake thread is
                            // still working): queue, drained in order at
                            // connect time.
                            None => {
                                a.queued_calls
                                    .push((ref_id.clone(), method.clone(), payload));
                                true
                            }
                        }
                    }
                    None => false,
                };
                if !sent {
                    let _ = writer.send(
                        Msg::ActorResult {
                            ref_id,
                            ok: false,
                            error: format!("no such actor {actor_id} on this agent"),
                        },
                        0,
                        &[],
                    );
                }
            }
            Msg::Pong => {}
            Msg::Ping => {
                let _ = writer.send(Msg::Pong, frame.req, &[]);
            }
            other => log("agent_unexpected", &[("msg", format!("{other:?}"))]),
        }
    }
}

fn spawn_actor(
    shared: Arc<AgentShared>,
    writer: FrameWriter,
    actor_id: String,
    name: String,
    env: BTreeMap<String, String>,
    _gpu_ids: Vec<u32>, // already recorded in the map entry by the caller
    payload: Vec<u8>,
) {
    // Short name on purpose: sockaddr_un caps the whole path at ~104 bytes
    // on macOS, and test tmpdirs are long. 12 hex chars of a random 32 still
    // cannot collide within one agent's lifetime.
    let sock_path = format!(
        "{}/a-{}.sock",
        shared.sock_dir,
        &actor_id[..12.min(actor_id.len())]
    );
    let _ = std::fs::remove_file(&sock_path);
    let listener = match UnixListener::bind(&sock_path) {
        Ok(l) => l,
        Err(e) => {
            shared.actors.lock().unwrap().remove(&actor_id);
            let _ = writer.send(
                Msg::SpawnResult {
                    actor_id,
                    ok: false,
                    error: format!("bind {sock_path}: {e}"),
                    pid: 0,
                },
                0,
                &[],
            );
            return;
        }
    };

    let python = std::env::var("MENTAT_PYTHON").unwrap_or_else(|_| "python3".into());
    let mut cmd = std::process::Command::new(&python);
    cmd.args(["-m", "ray._host", "--socket", &sock_path]);
    for (k, v) in &env {
        cmd.env(k, v);
    }
    cmd.env("MENTAT_AGENT_PID", std::process::id().to_string());
    // Own process group: vLLM workers fork compile helpers, and a kill must
    // take the whole tree -- orphaned workers pinning ~90 GB of unified
    // memory is the failure this line exists to prevent.
    cmd.process_group(0);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            shared.actors.lock().unwrap().remove(&actor_id);
            let _ = writer.send(
                Msg::SpawnResult {
                    actor_id,
                    ok: false,
                    error: format!("spawn {python}: {e}"),
                    pid: 0,
                },
                0,
                &[],
            );
            let _ = std::fs::remove_file(&sock_path);
            return;
        }
    };
    let pid = child.id();
    log(
        "actor_spawned",
        &[
            ("actor", actor_id.clone()),
            ("name", name.clone()),
            ("pid", pid.to_string()),
        ],
    );

    {
        let mut actors = shared.actors.lock().unwrap();
        match actors.get_mut(&actor_id) {
            Some(a) => {
                a.pid = pid;
                if a.kill_requested {
                    drop(actors);
                    kill_actor_process(&shared, &actor_id);
                }
            }
            // Entry vanished: a kill raced us and won. Don't leave the child.
            None => unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            },
        }
    }

    // Reaper: one blocking wait per actor process. Sends ActorExit when the
    // process (group leader) dies for any reason.
    {
        let shared = shared.clone();
        let actor_id = actor_id.clone();
        let sock_path = sock_path.clone();
        std::thread::spawn(move || {
            let status = child.wait();
            let (code, signal) = match &status {
                Ok(s) => (s.code(), s.signal()),
                Err(_) => (None, None),
            };
            log(
                "actor_exit",
                &[
                    ("actor", actor_id.clone()),
                    ("pid", pid.to_string()),
                    (
                        "exit_code",
                        code.map(|c| c.to_string()).unwrap_or_else(|| "none".into()),
                    ),
                    (
                        "signal",
                        signal
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "none".into()),
                    ),
                ],
            );
            // Sweep any stragglers left in the process group.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            shared.actors.lock().unwrap().remove(&actor_id);
            let _ = std::fs::remove_file(&sock_path);
            // Buffered if the daemon link is down: a death during an outage
            // must still reach the daemon after the reconnect.
            send_daemon(
                &shared,
                Msg::ActorExit {
                    actor_id,
                    exit_code: code,
                    signal,
                },
                &[],
            );
        });
    }

    // Handshake: the host connects before importing anything heavy, so the
    // default 60 s window (MENTAT_HOST_CONNECT_TIMEOUT_MS) is generous. If
    // the process dies first, the reaper has already reported and we just
    // clean up.
    let host_stream = match accept_with_timeout(
        &listener,
        Duration::from_millis(cfg().host_connect_timeout_ms),
    ) {
        Some(s) => s,
        None => {
            log("actor_host_no_connect", &[("actor", actor_id.clone())]);
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            return;
        }
    };
    let host_writer = UnixFrameWriter::new(match host_stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    });
    let mut host_reader = BufReader::new(host_stream);

    match read_frame(&mut host_reader) {
        Ok(Some((f, _))) if matches!(f.msg, Msg::HostHello { .. }) => {}
        other => {
            log(
                "actor_host_bad_hello",
                &[("actor", actor_id.clone()), ("got", format!("{other:?}"))],
            );
            return;
        }
    }
    if host_writer.send(Msg::Ctor, 0, &payload).is_err() {
        return;
    }
    {
        // Publish the host and drain calls that arrived before it connected,
        // in arrival order (the host executes serially behind the ctor).
        let mut actors = shared.actors.lock().unwrap();
        if let Some(a) = actors.get_mut(&actor_id) {
            a.host = Some(host_writer.clone());
            for (ref_id, method, call_payload) in a.queued_calls.drain(..) {
                if host_writer
                    .send(
                        Msg::HostCall {
                            ref_id: ref_id.clone(),
                            method,
                        },
                        0,
                        &call_payload,
                    )
                    .is_err()
                {
                    send_daemon(
                        &shared,
                        Msg::ActorResult {
                            ref_id,
                            ok: false,
                            error: "actor host socket write failed".into(),
                        },
                        &[],
                    );
                }
            }
        }
    }

    // Relay: host results flow back to the daemon (buffered across a daemon
    // link outage). EOF/err ends the loop and the reaper reports the death.
    while let Ok(Some((frame, payload))) = read_frame(&mut host_reader) {
        match frame.msg {
            Msg::CtorOk => {
                log("actor_ready", &[("actor", actor_id.clone())]);
                send_daemon(
                    &shared,
                    Msg::SpawnResult {
                        actor_id: actor_id.clone(),
                        ok: true,
                        error: String::new(),
                        pid,
                    },
                    &[],
                );
            }
            Msg::CtorErr { error } => {
                log(
                    "actor_ctor_error",
                    &[("actor", actor_id.clone()), ("error", error.clone())],
                );
                send_daemon(
                    &shared,
                    Msg::SpawnResult {
                        actor_id: actor_id.clone(),
                        ok: false,
                        error: format!("constructor raised: {error}"),
                        pid,
                    },
                    &[],
                );
            }
            Msg::HostResult { ref_id, ok } => {
                if let Some(a) = shared.actors.lock().unwrap().get_mut(&actor_id) {
                    a.pending_refs.remove(&ref_id);
                }
                send_daemon(
                    &shared,
                    Msg::ActorResult {
                        ref_id,
                        ok,
                        error: String::new(),
                    },
                    &payload,
                );
            }
            other => log(
                "actor_host_unexpected",
                &[("actor", actor_id.clone()), ("msg", format!("{other:?}"))],
            ),
        }
    }
}

fn accept_with_timeout(listener: &UnixListener, timeout: Duration) -> Option<UnixStream> {
    listener.set_nonblocking(true).ok()?;
    let deadline = Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((s, _)) => {
                s.set_nonblocking(false).ok()?;
                return Some(s);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() > deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return None,
        }
    }
}

fn kill_actor_process(shared: &Arc<AgentShared>, actor_id: &str) {
    let pid = {
        let mut actors = shared.actors.lock().unwrap();
        match actors.get_mut(actor_id) {
            Some(a) if a.pid == 0 => {
                // Fork hasn't happened yet; the spawn thread honors this
                // right after it learns the pid.
                a.kill_requested = true;
                log("actor_kill_deferred", &[("actor", actor_id.to_string())]);
                return;
            }
            Some(a) => a.pid,
            None => {
                log("actor_kill_unknown", &[("actor", actor_id.to_string())]);
                return;
            }
        }
    };
    log(
        "actor_kill",
        &[("actor", actor_id.to_string()), ("pid", pid.to_string())],
    );
    unsafe {
        // Whole process group; the reaper reports the exit.
        libc::kill(-(pid as i32), libc::SIGKILL);
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The form every existing deployment uses. It must keep meaning
    /// exactly one address, because that is the escape hatch for a server
    /// the port form cannot describe.
    #[test]
    fn a_url_with_a_host_is_used_verbatim() {
        for v in [
            "http://10.100.0.1:8000/v1",
            "https://models.example:8443/v1",
            "http://localhost:9000/mcp",
        ] {
            assert_eq!(parse_announcement(v), Announcement::Url(v.to_string()));
        }
    }

    #[test]
    fn a_wildcard_host_and_a_bare_port_mean_the_same_thing() {
        let want = Announcement::Port {
            port: 8000,
            path: "/v1".into(),
        };
        assert_eq!(parse_announcement("http://0.0.0.0:8000/v1"), want);
        assert_eq!(parse_announcement("8000/v1"), want);
        assert_eq!(
            parse_announcement("9000"),
            Announcement::Port {
                port: 9000,
                path: String::new()
            },
        );
    }

    /// A value this parser does not understand is still the operator's.
    /// Refusing it would drop an endpoint that announced fine before.
    #[test]
    fn an_unparsable_value_passes_through() {
        for v in ["not a url", "http://0.0.0.0:notaport/v1", "/v1"] {
            assert_eq!(parse_announcement(v), Announcement::Url(v.to_string()));
        }
    }

    /// A real /proc/net/tcp, trimmed to the columns the parser reads. Row 1
    /// is a wildcard listener on 8000, row 2 is one bound to 10.100.0.1 on
    /// 9000, row 3 is an established connection on 8000 that must not be
    /// mistaken for a bind.
    const TCP4: &str = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:1F40 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1
   1: 0100640A:2328 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 2
   2: 0100640A:1F40 0200640A:C350 01 00000000:00000000 00:00000000 00000000     0        0 3
";

    #[test]
    fn a_wildcard_bind_is_read_as_wildcard() {
        assert_eq!(listening_addrs(TCP4, false, 8000), vec!["0.0.0.0"]);
    }

    /// The finding the warning exists for: the server answers on one address
    /// of a multi-homed node, so the port announcement promised more than
    /// the socket delivers.
    #[test]
    fn a_narrow_bind_is_read_as_its_address() {
        assert_eq!(listening_addrs(TCP4, false, 9000), vec!["10.100.0.1"]);
    }

    /// An established connection on the port is not a listener. Without the
    /// state check every busy port would read as bound.
    #[test]
    fn only_listening_rows_count() {
        assert!(listening_addrs(TCP4, false, 50000).is_empty());
        assert!(listening_addrs("", false, 8000).is_empty());
    }

    #[test]
    fn ipv6_rows_decode_too() {
        let tcp6 = "\
  sl  local_address                         rem_address                        st
   0: 00000000000000000000000000000000:1F40 00000000000000000000000000000000:0000 0A
   1: 0000000000000000FFFF00000100640A:2328 00000000000000000000000000000000:0000 0A
";
        assert_eq!(listening_addrs(tcp6, true, 8000), vec!["::"]);
        assert_eq!(listening_addrs(tcp6, true, 9000), vec!["::ffff:10.100.0.1"]);
    }
}
