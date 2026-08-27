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
use crate::proto::{read_frame, Msg, ResumeActor};
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

struct AgentShared {
    daemon: Mutex<Option<FrameWriter>>,
    actors: Mutex<HashMap<String, HostActor>>,
    sock_dir: String,
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
    let services = announced_services();

    log(
        "agent_start",
        &[
            ("agent", agent_id.clone()),
            ("group", opts.group.clone()),
            ("daemon", opts.daemon_addr.clone()),
            ("node_ip", node_ip.clone()),
            ("gpus", format!("{gpus:?}")),
            ("services", format!("{services:?}")),
        ],
    );

    let shared = Arc::new(AgentShared {
        daemon: Mutex::new(None),
        actors: Mutex::new(HashMap::new()),
        sock_dir,
        unsent: Mutex::new(Vec::new()),
    });

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

/// Service endpoints this container announces, read once at agent start.
/// The entrypoints export these right before `ray start`. An agent without
/// them registers exactly as before.
fn announced_services() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (var, key) in [("MENTAT_OPENAI_API", "openai"), ("MENTAT_MCP_API", "mcp")] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                out.insert(key.to_string(), v);
            }
        }
    }
    out
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
    services: &BTreeMap<String, String>,
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
            services: services.clone(),
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
