//! mentatd: the cluster daemon. Accepts client (Python shim / CLI) and agent
//! connections on the control port. In this phase there is one daemon and it
//! is its own head; the mesh/election layer slots in above these handlers.

use std::collections::BTreeMap;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::logfmt::log;
use crate::proto::{read_frame, Frame, Msg};
use crate::state::{
    local_ip_toward, node_id_for, random_hex_id, write_json_file, ActorInfo, ActorState, AgentInfo,
    BundleAssignment, ClientInfo, FrameWriter, PgInfo, PgState, RefInfo, RefState, Shared,
    SharedRef, State,
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

    // Slow-call sweeper: vLLM's run() ref legitimately never resolves, but a
    // NORMAL method call sitting pending for long means it queued behind a
    // blocking method (actors are serial, like real ray). That is the
    // likeliest way a future vLLM change breaks silently, so make it loud.
    {
        let shared = shared.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(15));
            let now = crate::state::now_ms_u64();
            let mut st = shared.st.lock().unwrap();
            let mut warn: Vec<(String, String, u64)> = Vec::new();
            for (rid, r) in st.refs.iter_mut() {
                if matches!(r.state, RefState::Pending)
                    && r.method != "run"
                    && !r.warned
                    && now.saturating_sub(r.created_ms) > 15_000
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
        // ~75s instead of the kernel default of >2h. wait_for_init blocks for
        // ~10 minutes legitimately, but that's an idle-with-live-peer case,
        // which keepalive handles correctly.
        #[cfg(target_os = "linux")]
        {
            let idle: libc::c_int = 30;
            let intvl: libc::c_int = 15;
            let cnt: libc::c_int = 3;
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
        Msg::PeerHello { .. } => crate::mesh::accept_peer(shared, reader, writer, first),
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
                    match agent_writer {
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
                                    actor_id,
                                    ref_id: ref_id.clone(),
                                    method,
                                },
                                0,
                                &payload,
                            );
                            if send_res.is_err() {
                                if let Some(r) = st.refs.get_mut(&ref_id) {
                                    r.state = RefState::ActorDied {
                                        reason: "agent connection lost mid-send".into(),
                                    };
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
                    reason: "placement group removed".into(),
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
    let (actor_ids, group) = {
        let mut st = shared.st.lock().unwrap();
        let group = client_group(&st, client_id);
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
        st.clients.remove(client_id);
        if !ids.is_empty() {
            st.emit(
                "driver_gone_reaping",
                json!({ "group": group, "client_id": client_id, "actors": ids.len() }),
            );
        }
        (ids, group)
    };
    for id in &actor_ids {
        kill_actor(shared, id, "driver session closed");
    }
    let mut st = shared.st.lock().unwrap();
    st.refs.retain(|_, r| r.owner != client_id);
    st.emit(
        "driver_disconnected",
        json!({ "group": group, "client_id": client_id }),
    );
    shared.cv.notify_all();
}

// ---------------------------------------------------------------------------
// Placement
// ---------------------------------------------------------------------------

/// Try to complete every pending placement group. All-or-nothing per group:
/// partial reservations are never held, so two pending pgs can't deadlock.
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

        // Candidate agents: this group's, alive, driver-node first, then
        // registration order. Bundle 0 lands on the driver's node when it
        // can, which puts TP rank 0 next to the engine for the shm queue.
        let mut candidates: Vec<(bool, u64, String)> = st
            .agents
            .values()
            .filter(|a| a.alive && a.group == group)
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
        let mut complete = true;
        for spec in &bundles {
            let need = spec.ceil().max(1.0) as usize;
            let mut placed = None;
            for (agent_id, free) in agents.iter_mut() {
                if free.len() >= need {
                    let gpu_ids: Vec<u32> = free.drain(..need).collect();
                    let node_id = st.agents[agent_id].node_id.clone();
                    placed = Some(BundleAssignment {
                        agent: agent_id.clone(),
                        node_id,
                        gpu_ids,
                    });
                    break;
                }
            }
            match placed {
                Some(b) => assignment.push(Some(b)),
                None => {
                    complete = false;
                    break;
                }
            }
        }

        if complete {
            let n = assignment.len();
            if let Some(pg) = st.pgs.get_mut(&pg_id) {
                pg.assignment = assignment;
                pg.state = PgState::Created;
            }
            st.emit(
                "pg_ready",
                json!({ "group": group, "pg_id": pg_id, "bundles": n }),
            );
            cv.notify_all();
        }
    }
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
        resume,
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
                writer: writer.clone(),
                alive: true,
                seq,
            },
        );
        st.counters.agents_registered += 1;
        st.emit(
            "agent_register",
            json!({ "group": group, "agent": agent_id, "node_ip": node_ip,
                    "gpus": gpus.len(), "container": container }),
        );

        // Resumed actors whose owner is gone get killed; the rest are
        // re-adopted (matters once the mesh can move the head).
        for r in &resume {
            let keep = st
                .actors
                .get(&r.actor_id)
                .map(|a| st.clients.contains_key(&a.owner))
                .unwrap_or(false);
            if !keep {
                let _ = writer.send(
                    Msg::Kill {
                        actor_id: r.actor_id.clone(),
                    },
                    0,
                    &[],
                );
                log(
                    "resume_rejected",
                    &[("actor", r.actor_id.clone()), ("agent", agent_id.clone())],
                );
            }
        }

        let _ = writer.send(
            Msg::AgentRegisterOk {
                node_id: node_id.clone(),
            },
            first.0.req,
            &[],
        );
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
            } => {
                let mut st = shared.st.lock().unwrap();
                if ok {
                    if let Some(a) = st.actors.get_mut(&actor_id) {
                        if a.state == ActorState::Spawning {
                            a.state = ActorState::Running;
                        }
                    }
                    st.emit("actor_running", json!({ "actor_id": actor_id }));
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
                if let Some(r) = st.refs.get_mut(&ref_id) {
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

    // Agent link lost: its actors are unreachable. Declare them dead so
    // pending refs (including run() sentinels) resolve and the driver finds
    // out, rather than hanging forever.
    let mut st = shared.st.lock().unwrap();
    // Only if this reader owned the current registration -- a re-register may
    // already have replaced the entry with a fresh connection.
    let owned = st
        .agents
        .get(&agent_id)
        .map(|a| FrameWriter::same_socket(&a.writer, &writer))
        .unwrap_or(false);
    if owned {
        if let Some(a) = st.agents.get_mut(&agent_id) {
            a.alive = false;
        }
        st.emit("agent_lost", json!({ "agent": agent_id }));
        let orphaned: Vec<String> = st
            .actors
            .values()
            .filter(|ac| ac.agent == agent_id && !matches!(ac.state, ActorState::Dead { .. }))
            .map(|ac| ac.id.clone())
            .collect();
        for id in orphaned {
            mark_actor_dead(&mut st, &shared.cv, &id, "agent connection lost");
        }
    }
    shared.cv.notify_all();
}
