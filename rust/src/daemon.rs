//! mentatd: the cluster daemon. Accepts client (Python shim / CLI) and agent
//! connections on the control port. In this phase there is one daemon and it
//! is its own head; the mesh/election layer slots in above these handlers.

use std::collections::BTreeMap;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::cfg;
use crate::logfmt::log;
use crate::proto::{read_frame, Frame, Msg};
use crate::state::{
    local_ip_toward, node_id_for, random_hex_id, write_json_file, ActorInfo, ActorState, AgentInfo,
    BundleAssignment, ClaimInfo, ClientInfo, FrameWriter, PgInfo, PgState, RefInfo, RefState,
    Shared, SharedRef, State,
};

pub struct DaemonOpts {
    pub port: u16,
    pub http_port: u16,
    pub node_ip: String,
    pub head_json: String,
    /// Control addresses of the other mentatd instances (static seed list).
    pub peers: Vec<String>,
}

pub fn default_node_ip() -> String {
    if let Ok(ip) = std::env::var("MENTAT_NODE_IP") {
        return ip;
    }
    // The address we'd use to reach the world; loopback for dev boxes.
    local_ip_toward("8.8.8.8:53").unwrap_or_else(|| "127.0.0.1".to_string())
}

pub fn run(opts: DaemonOpts) -> std::io::Result<()> {
    let hostname = hostname();
    let gcs_address = format!("{}:{}", opts.node_ip, opts.port);
    let shared: SharedRef = Arc::new(Shared {
        st: std::sync::Mutex::new(State::new(
            opts.node_ip.clone(),
            hostname.clone(),
            gcs_address.clone(),
        )),
        cv: std::sync::Condvar::new(),
    });
    shared.st.lock().unwrap().http_port = opts.http_port;

    // Bind before writing head.json so a reader never races an unbound port.
    let listener = TcpListener::bind(("0.0.0.0", opts.port))?;
    let _ = write_json_file(
        &opts.head_json,
        &json!({ "address": gcs_address, "node_ip": opts.node_ip, "pid": std::process::id() }),
    );
    log(
        "daemon_up",
        &[
            ("addr", gcs_address.clone()),
            ("node_id", shared.st.lock().unwrap().node_id.clone()),
            ("hostname", hostname),
        ],
    );

    crate::http::serve(shared.clone(), opts.http_port);
    crate::mesh::start(shared.clone(), opts.peers, opts.port, opts.http_port);
    crate::island::start(shared.clone());
    crate::announce::start(shared.clone());

    // Lifecycle sweeper: slow-call warnings, the pending-pg timeout, and the
    // agent degrade/give-up windows. A single thread ticking every 200 ms,
    // cheap enough that the short windows the tests use still fire on time.
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
        // The last placement attempt recorded what it could not find. Say
        // that rather than the old blanket guess about GPU counts: at four
        // nodes on two fabrics, "not enough GPUs" is usually wrong and
        // "not enough on one fabric" is usually right.
        let why =
            why.unwrap_or_else(|| format!("group '{group}' never had enough registered GPUs"));
        if let Some(pg) = st.pgs.get_mut(&pg_id) {
            pg.state = PgState::Removed;
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
        st.emit(
            "pg_timeout",
            json!({ "group": group, "pg_id": pg_id, "waited_ms": age, "why": why }),
        );
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
        st.emit(
            "agent_degraded",
            json!({ "group": group, "agent": agent, "down_ms": down }),
        );
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
        st.emit(
            "agent_dead",
            json!({ "group": group, "agent": agent, "down_ms": down,
                    "actors": orphaned.len() }),
        );
        for id in orphaned {
            mark_actor_dead(
                &mut st,
                &shared.cv,
                &id,
                &format!("agent link lost for {down}ms (MENTAT_AGENT_DEAD_AFTER_MS); giving up"),
            );
        }
    }
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
        Msg::Hello { .. } => client_conn(shared, reader, writer, peer_ip, first.0),
        Msg::AgentRegister { .. } => agent_conn(shared, reader, writer, peer_ip, first),
        Msg::PeerHello { .. } => crate::mesh::accept_peer(shared, reader, writer, peer_ip, first),
        // A reachability probe. It gets its own connection because the
        // question is about the socket rather than the mesh: answering says
        // this address pair carries traffic, and the node id in the answer
        // says the address belongs to the node the prober meant. Nothing is
        // recorded here -- the prober owns the result.
        Msg::Probe { .. } => {
            let my_id = shared.st.lock().unwrap().node_id.clone();
            let _ = writer.send(Msg::ProbeOk { node_id: my_id }, first.0.req, &[]);
        }
        other => {
            let _ = writer.send(
                Msg::Err {
                    error: format!("expected hello or agent_register, got {other:?}"),
                },
                first.0.req,
                &[],
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Client connections
// ---------------------------------------------------------------------------

fn client_conn(
    shared: SharedRef,
    mut reader: BufReader<TcpStream>,
    writer: FrameWriter,
    peer_ip: String,
    hello: Frame,
) {
    let (client_id, is_session) = {
        let Msg::Hello {
            client_id,
            group,
            session,
            kind,
        } = hello.msg
        else {
            unreachable!()
        };
        let mut st = shared.st.lock().unwrap();

        if session {
            let dup = st
                .clients
                .values()
                .any(|c| c.group == group && c.has_session && c.id != client_id);
            if dup {
                let _ = writer.send(
                    Msg::Err {
                        error: format!(
                            "group '{group}' already has an active driver session; \
                             run a second instance under a distinct MENTAT_GROUP"
                        ),
                    },
                    hello.req,
                    &[],
                );
                return;
            }
        }

        // The client's node: loopback means "same box as this daemon";
        // otherwise match the source address against registered agent nodes,
        // falling back to a node id derived from the source ip itself.
        let node_id = if peer_ip == "127.0.0.1" || peer_ip == "::1" || peer_ip == st.node_ip {
            st.node_id.clone()
        } else {
            st.agents
                .values()
                .find(|a| a.node_ip == peer_ip)
                .map(|a| a.node_id.clone())
                .unwrap_or_else(|| node_id_for(&peer_ip))
        };

        let entry = st
            .clients
            .entry(client_id.clone())
            .or_insert_with(|| ClientInfo {
                id: client_id.clone(),
                group: group.clone(),
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
        let gcs = st.gcs_address.clone();
        if session {
            st.emit(
                "driver_connected",
                json!({ "group": group, "client_id": client_id, "kind": kind }),
            );
        }
        let _ = writer.send(
            Msg::HelloOk {
                node_id,
                node_ip,
                gcs_address: gcs,
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
        (client_id, session)
    };

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
        let (resp, resp_payload) = handle_client_msg(&shared, &client_id, frame.msg, payload);
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
    if is_session {
        reap_client(&shared, &client_id);
    }
}

fn handle_client_msg(
    shared: &SharedRef,
    client_id: &str,
    msg: Msg,
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
                node_entry(&st.node_id, &st.node_ip, 0.0, 8.0),
            );
            for a in st.agents.values().filter(|a| a.alive && a.group == group) {
                let e = nodes
                    .entry(a.node_id.clone())
                    .or_insert_with(|| node_entry(&a.node_id, &a.node_ip, 0.0, a.cpus as f64));
                if let Some(res) = e.get_mut("Resources").and_then(|r| r.as_object_mut()) {
                    let g = res.get("GPU").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    res.insert("GPU".into(), json!(g + a.gpus.len() as f64));
                    res.insert("CPU".into(), json!(a.cpus as f64));
                }
            }
            (
                Msg::NodesOk {
                    nodes: nodes.into_values().collect(),
                },
                Vec::new(),
            )
        }
        Msg::ClusterResources => {
            let st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            let mut res: BTreeMap<String, f64> = BTreeMap::new();
            let mut gpu = 0.0;
            let mut cpu = 0.0;
            for a in st.agents.values().filter(|a| a.alive && a.group == group) {
                gpu += a.gpus.len() as f64;
                cpu += a.cpus as f64;
            }
            res.insert("GPU".into(), gpu);
            res.insert("CPU".into(), cpu);
            res.insert("object_store_memory".into(), 0.0);
            (Msg::ResourcesOk { resources: res }, Vec::new())
        }
        Msg::AvailablePerNode => {
            let st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            let mut nodes: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
            for a in st.agents.values().filter(|a| a.alive && a.group == group) {
                let free = st.free_gpus_of(&a.id).len() as f64;
                let e = nodes.entry(a.node_id.clone()).or_default();
                *e.entry("GPU".to_string()).or_insert(0.0) += free;
                e.insert("CPU".to_string(), a.cpus as f64);
                e.insert(format!("node:{}", a.node_ip), 1.0);
            }
            (Msg::AvailOk { nodes }, Vec::new())
        }
        Msg::Claim { name, shape } => {
            let mut st = shared.st.lock().unwrap();
            match claim(&mut st, client_id, &name, &shape) {
                Ok((generation, view)) => (
                    Msg::ClaimOk {
                        name,
                        generation,
                        view,
                    },
                    Vec::new(),
                ),
                Err(error) => (Msg::Err { error }, Vec::new()),
            }
        }
        Msg::Release { name } => {
            let mut st = shared.st.lock().unwrap();
            release(&mut st, client_id, &name);
            (
                Msg::ClaimOk {
                    name,
                    generation: 0,
                    view: Value::Null,
                },
                Vec::new(),
            )
        }
        Msg::CreatePg { bundles, strategy } => {
            let mut st = shared.st.lock().unwrap();
            let group = client_group(&st, client_id);
            let pg_id = random_hex_id();
            let n = bundles.len();
            st.pgs.insert(
                pg_id.clone(),
                PgInfo {
                    id: pg_id.clone(),
                    group: group.clone(),
                    owner: client_id.to_string(),
                    bundles,
                    strategy,
                    assignment: vec![None; n],
                    state: PgState::Pending,
                    created_ms: crate::state::now_ms_u64(),
                    fail_reason: None,
                    island: None,
                    pending_reason: None,
                },
            );
            st.emit(
                "pg_created",
                json!({ "group": group, "pg_id": pg_id, "bundles": n }),
            );
            try_place(&mut st, &shared.cv);
            (
                Msg::CreatePgOk {
                    ready_ref: format!("pg:{pg_id}:ready"),
                    pg_id,
                },
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
        Msg::RemovePg { pg_id } => {
            let mut st = shared.st.lock().unwrap();
            if let Some(pg) = st.pgs.get_mut(&pg_id) {
                pg.state = PgState::Removed;
            }
            try_place(&mut st, &shared.cv);
            (Msg::Ok0, Vec::new())
        }
        Msg::CreateActor {
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
        Msg::Call { actor_id, method } => {
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
                ActorState::Dead { reason } => {
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
                                reason: "agent connection lost".into(),
                            });
                            st.refs.insert(ref_id.clone(), r);
                        }
                        Some(w) => {
                            let r = new_ref(RefState::Pending);
                            st.refs.insert(ref_id.clone(), r);
                            let send_res = w.send(
                                Msg::CallActor {
                                    actor_id: actor_id.clone(),
                                    ref_id: ref_id.clone(),
                                    method: method.clone(),
                                },
                                0,
                                &payload,
                            );
                            if send_res.is_err() {
                                // The link is dying under us. The EOF handler
                                // and degrade window take it from here.
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
            (Msg::CallOk { ref_id }, Vec::new())
        }
        Msg::Get { ref_id, timeout_ms } => do_get(shared, &ref_id, timeout_ms),
        Msg::Wait {
            ref_ids,
            num_returns,
            timeout_ms,
        } => do_wait(shared, &ref_ids, num_returns, timeout_ms),
        Msg::KillActor { actor_id } => {
            kill_actor(shared, &actor_id, "ray.kill");
            (Msg::Ok0, Vec::new())
        }
        Msg::Status { group } => {
            let st = shared.st.lock().unwrap();
            (
                Msg::StatusOk {
                    data: crate::status::snapshot(&st, group.as_deref()),
                },
                Vec::new(),
            )
        }
        Msg::StopAll { group } => {
            let ids: Vec<String> = {
                let st = shared.st.lock().unwrap();
                st.actors
                    .values()
                    .filter(|a| group.as_deref().is_none_or(|g| a.group == g))
                    .filter(|a| !matches!(a.state, ActorState::Dead { .. }))
                    .map(|a| a.id.clone())
                    .collect()
            };
            for id in &ids {
                kill_actor(shared, id, "mentat stop");
            }
            (Msg::Ok0, Vec::new())
        }
        other => err(format!("unexpected client message: {other:?}")),
    }
}

/// Answer a claim on `name`, solving it the first time and repeating that
/// answer afterwards.
///
/// The name is the reservation. A second holder of one name is not a second
/// placement, so ranks that claim the same name agree on their nodes without
/// a coordinator. A holder that asks for a different shape under a name
/// already taken is refused: re-solving would move nodes under whoever
/// claimed first.
///
/// Only the head answers. Two daemons solving the same name against their
/// own views could each hand out a placement, and islands are deliberately
/// soft-consistent between daemons. The error names the head so a caller can
/// go there.
fn claim(
    st: &mut State,
    client_id: &str,
    name: &str,
    shape: &Value,
) -> Result<(u64, Value), String> {
    if name.trim().is_empty() {
        return Err("a claim needs a name".into());
    }
    if st.head_node_id != st.node_id {
        let addr = st
            .peers
            .values()
            .find(|p| p.node_id == st.head_node_id)
            .map(|p| p.control_addr.clone())
            .unwrap_or_default();
        return Err(format!(
            "claims are answered by the head, which is {} at {addr}",
            st.head_node_id
        ));
    }
    if let Some(c) = st.claims.get_mut(name) {
        if &c.shape != shape {
            return Err(format!(
                "claim {name:?} is held for a different shape; release it or use another name"
            ));
        }
        c.holders.insert(client_id.to_string());
        return Ok((c.generation, c.view.clone()));
    }
    let req = crate::claim::parse(shape)?;
    let topo = crate::claim::topology(st);
    let solution = crate::claim::solve(&topo, &req)?;
    st.claim_generation += 1;
    let generation = st.claim_generation;
    let view = solution.to_json(&topo);
    st.claims.insert(
        name.to_string(),
        ClaimInfo {
            shape: shape.clone(),
            view: view.clone(),
            generation,
            holders: [client_id.to_string()].into_iter().collect(),
        },
    );
    st.emit(
        "claim_solved",
        json!({ "name": name, "generation": generation }),
    );
    Ok((generation, view))
}

/// Drop one hold. The claim goes with its last holder, which is how a driver
/// that dies gives its nodes back.
fn release(st: &mut State, client_id: &str, name: &str) {
    let Some(c) = st.claims.get_mut(name) else {
        return;
    };
    c.holders.remove(client_id);
    if c.holders.is_empty() {
        st.claims.remove(name);
        st.emit("claim_released", json!({ "name": name }));
    }
}

/// Drop every claim this client held. This runs where its other resources
/// are reaped, so a disconnect needs no explicit release.
fn release_all(st: &mut State, client_id: &str) {
    let names: Vec<String> = st.claims.keys().cloned().collect();
    for n in names {
        release(st, client_id, &n);
    }
}

fn client_group(st: &State, client_id: &str) -> String {
    st.clients
        .get(client_id)
        .map(|c| c.group.clone())
        .unwrap_or_else(|| "default".to_string())
}

fn node_entry(node_id: &str, ip: &str, gpus: f64, cpus: f64) -> Value {
    json!({
        "NodeID": node_id,
        "NodeManagerAddress": ip,
        "Alive": true,
        "Resources": { "GPU": gpus, "CPU": cpus, format!("node:{ip}"): 1.0 },
    })
}

fn err(e: String) -> (Msg, Vec<u8>) {
    (Msg::Err { error: e }, Vec::new())
}

#[allow(clippy::too_many_arguments)]
fn create_actor(
    shared: &SharedRef,
    client_id: &str,
    name: String,
    num_gpus: f64,
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
        return err(format!("bundle {bundle_index} of pg {pg_id} is not placed"));
    };
    // The address this rank answers on inside the fabric its group was
    // placed on. Only set when the group spans a fabric. A group on one
    // node has nothing to cross.
    let fabric_ip = pg
        .island
        .as_ref()
        .and_then(|i| i.addr.get(&bundle.node_id))
        .cloned();
    if num_gpus > bundle.gpu_ids.len() as f64 {
        return err(format!(
            "actor wants {num_gpus} GPUs but bundle {bundle_index} reserves {}",
            bundle.gpu_ids.len()
        ));
    }

    let Some(agent) = st.agents.get(&bundle.agent).filter(|a| a.alive) else {
        return err(format!("agent for bundle {bundle_index} is gone"));
    };
    let agent_id = agent.id.clone();
    let agent_writer = agent.writer.clone();
    let node_id = bundle.node_id.clone();
    let gpu_ids = bundle.gpu_ids.clone();

    let actor_id = random_hex_id();
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
    spawn_env.insert("MENTAT_GCS_ADDRESS".into(), st.gcs_address.clone());
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
    st.emit(
        "actor_spawning",
        json!({ "group": group, "actor_id": actor_id, "name": name,
                "agent": agent_id, "node_id": node_id }),
    );

    let gcs = st.gcs_address.clone();
    let send_res = agent_writer.send(
        Msg::Spawn {
            actor_id: actor_id.clone(),
            name,
            env: spawn_env,
            gpu_ids: gpu_ids.clone(),
            node_id: node_id.clone(),
            gcs_address: gcs,
        },
        0,
        &payload,
    );
    if let Err(e) = send_res {
        mark_actor_dead(
            &mut st,
            &shared.cv,
            &actor_id,
            &format!("spawn send failed: {e}"),
        );
        return err(format!("agent send failed: {e}"));
    }

    (
        Msg::CreateActorOk {
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

fn resolve_ref(st: &State, ref_id: &str) -> Res {
    if let Some(rest) = ref_id.strip_prefix("pg:") {
        let pg_id = rest.strip_suffix(":ready").unwrap_or(rest);
        return match st.pgs.get(pg_id) {
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
                    Msg::GetOk {
                        status: if ok { "ok" } else { "error" }.into(),
                        reason: String::new(),
                    },
                    payload,
                )
            }
            Res::ActorDied { reason } => {
                return (
                    Msg::GetOk {
                        status: "actor_died".into(),
                        reason,
                    },
                    Vec::new(),
                )
            }
            Res::Unknown => return err(format!("unknown ref {ref_id}")),
            Res::Pending => {}
        }
        match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return (
                        Msg::GetOk {
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
            return (Msg::WaitOk { ready: capped }, Vec::new());
        }
        match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return (Msg::WaitOk { ready }, Vec::new());
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
                Msg::Kill {
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
            mark_actor_dead(&mut st, &shared.cv, actor_id, "killed with agent gone");
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
        };
        // Held calls die with the actor. Their refs resolve in the fan-out
        // below.
        actor.queued_calls.clear();
        let group = actor.group.clone();
        let name = actor.name.clone();
        st.emit(
            "actor_dead",
            json!({ "group": group, "actor_id": actor_id, "name": name, "reason": reason }),
        );
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
        st.emit(
            "driver_disconnected",
            json!({ "group": group, "client_id": client_id }),
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
        for pg in st.pgs.values_mut() {
            if pg.owner == client_id {
                pg.state = PgState::Removed;
            }
        }
        if !ids.is_empty() {
            st.emit(
                "driver_gone_reaping",
                json!({ "group": group, "client_id": client_id, "actors": ids.len() }),
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
/// one node needs no fabric at all, and a node is therefore its own island
/// of one.
pub fn try_place(st: &mut State, cv: &std::sync::Condvar) {
    let pending: Vec<String> = st
        .pgs
        .values()
        .filter(|p| p.state == PgState::Pending)
        .map(|p| p.id.clone())
        .collect();
    for pg_id in pending {
        let (group, owner, bundles) = {
            let pg = &st.pgs[&pg_id];
            (pg.group.clone(), pg.owner.clone(), pg.bundles.clone())
        };
        let driver_node = st
            .clients
            .get(&owner)
            .map(|c| c.node_id.clone())
            .unwrap_or_default();

        let placed = match placement_scopes(st, &group, &bundles, &driver_node) {
            Ok(scopes) => scopes.into_iter().find_map(|(island, nodes)| {
                fit(st, &group, &bundles, &driver_node, nodes.as_deref()).map(|a| (island, a))
            }),
            Err(why) => {
                if let Some(pg) = st.pgs.get_mut(&pg_id) {
                    pg.pending_reason = Some(why);
                }
                continue;
            }
        };
        let Some((island, assignment)) = placed else {
            let why = no_fit_reason(st, &group, &bundles);
            if let Some(pg) = st.pgs.get_mut(&pg_id) {
                pg.pending_reason = Some(why);
            }
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
        st.emit(
            "pg_ready",
            json!({ "group": group, "pg_id": pg_id, "bundles": n,
                    "island_nodes": members }),
        );
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
/// gate asking whether the cluster had any island would strand it instead.
/// The gate asks whether this group's own nodes claim a fabric.
#[allow(clippy::type_complexity)]
fn placement_scopes(
    st: &State,
    group: &str,
    bundles: &[f64],
    driver_node: &str,
) -> Result<Vec<(Option<crate::island::Island>, Option<Vec<String>>)>, String> {
    let opted_in = cfg().island_placement
        && st
            .agents
            .values()
            .any(|a| a.alive && a.group == group && st.fabrics.tagged.contains(&a.node_id));
    if bundles.len() < 2 || !opted_in {
        return Ok(vec![(None, None)]);
    }
    let need: usize = bundles.iter().map(|b| b.ceil().max(1.0) as usize).sum();
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
            // A one-node scope carries no fabric address to inject.
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
    bundles: &[f64],
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
        let need = spec.ceil().max(1.0) as usize;
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
fn no_fit_reason(st: &State, group: &str, bundles: &[f64]) -> String {
    let need: usize = bundles.iter().map(|b| b.ceil().max(1.0) as usize).sum();
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
        agent_id,
        group,
        node_ip,
        gpus,
        gpu_vendor,
        cpus,
        container,
        pid,
        services,
        services_ports,
        service_notes,
        provider,
        resume,
        unacked_refs,
    } = first.0.msg
    else {
        unreachable!()
    };

    let node_ip = if node_ip.is_empty() {
        if peer_ip == "127.0.0.1" || peer_ip == "::1" {
            shared.st.lock().unwrap().node_ip.clone()
        } else {
            peer_ip.clone()
        }
    } else {
        node_ip
    };
    let node_id = {
        let st = shared.st.lock().unwrap();
        if node_ip == st.node_ip {
            st.node_id.clone()
        } else {
            node_id_for(&node_ip)
        }
    };

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
                gpus: gpus.clone(),
                gpu_vendor: gpu_vendor.clone(),
                cpus,
                container: container.clone(),
                pid,
                services: services.clone(),
                services_ports: services_ports.clone(),
                service_notes,
                provider,
                writer: writer.clone(),
                alive: true,
                lost_at_ms: None,
                degraded: false,
                seq,
            },
        );
        st.counters.agents_registered += 1;
        st.emit(
            "agent_register",
            json!({ "group": group, "agent": agent_id, "node_ip": node_ip,
                    "gpus": gpus.len(), "container": container,
                    "services": services, "services_ports": services_ports }),
        );

        // Resumed actors whose owner is gone (or that this daemon already
        // declared dead, e.g. a kill or give-up during the outage) get
        // killed. The rest are re-adopted (matters once the mesh can move
        // the head). The kills are sent after AgentRegisterOk below -- the
        // agent's handshake expects that as the first frame.
        let mut kills: Vec<String> = Vec::new();
        for r in &resume {
            let keep = st
                .actors
                .get(&r.actor_id)
                .map(|a| {
                    st.clients.contains_key(&a.owner) && !matches!(a.state, ActorState::Dead { .. })
                })
                .unwrap_or(false);
            if !keep {
                kills.push(r.actor_id.clone());
                log(
                    "resume_rejected",
                    &[("actor", r.actor_id.clone()), ("agent", agent_id.clone())],
                );
            }
        }

        // The resume list is authoritative for what survived on the agent's
        // side. An actor this daemon still thinks is live but the agent no
        // longer carries (agent restarted, or the actor exited during the
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

        // A pending ref the agent neither carries (pending_refs), has a
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
                node_id: node_id.clone(),
            },
            first.0.req,
            &[],
        );
        for actor_id in kills {
            let _ = writer.send(Msg::Kill { actor_id }, 0, &[]);
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
                        Msg::CallActor {
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
            Msg::SpawnResult {
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
                    st.emit("actor_running", json!({ "actor_id": actor_id, "pid": pid }));
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
                if let Some(a) = st.agents.get_mut(&agent_id) {
                    if note.is_empty() {
                        a.service_notes.remove(&service);
                    } else {
                        a.service_notes.insert(service, note);
                    }
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
    // an agent that re-registers inside the window carries on with nothing
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
            a.lost_at_ms = Some(crate::state::now_ms_u64());
            a.degraded = false;
            a.group.clone()
        } else {
            String::new()
        };
        st.emit(
            "agent_lost",
            json!({ "agent": agent_id, "group": group,
                    "degrade_window_ms": cfg().agent_dead_after_ms }),
        );
    }
    shared.cv.notify_all();
}
