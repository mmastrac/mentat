//! Cluster state held by the daemon, and the shared handle every connection
//! thread works through. All state is soft: it is rebuilt from registrations,
//! never persisted.

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};

use serde_json::{json, Value};

use crate::proto::{write_frame, Frame, Msg};

pub type ClientId = String;
pub type AgentId = String;
pub type ActorId = String;
pub type PgId = String;
pub type RefId = String;
pub type NodeId = String;

/// A cloneable, lock-per-write handle to one peer's socket. Reads happen only
/// on that connection's own reader thread; writes can come from anywhere.
#[derive(Clone)]
pub struct FrameWriter {
    inner: Arc<Mutex<TcpStream>>,
}

impl FrameWriter {
    pub fn new(stream: TcpStream) -> Self {
        FrameWriter {
            inner: Arc::new(Mutex::new(stream)),
        }
    }

    pub fn send(&self, msg: Msg, req: u64, payload: &[u8]) -> std::io::Result<()> {
        let mut s = self.inner.lock().unwrap();
        write_frame(&mut *s, &Frame { req, msg }, payload)
    }

    pub fn shutdown(&self) {
        if let Ok(s) = self.inner.lock() {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    }

    /// True when both writers wrap the same underlying socket.
    pub fn same_socket(a: &FrameWriter, b: &FrameWriter) -> bool {
        Arc::ptr_eq(&a.inner, &b.inner)
    }
}

/// Same idea for unix sockets (agent -> actor host).
#[derive(Clone)]
pub struct UnixFrameWriter {
    inner: Arc<Mutex<std::os::unix::net::UnixStream>>,
}

impl UnixFrameWriter {
    pub fn new(stream: std::os::unix::net::UnixStream) -> Self {
        UnixFrameWriter {
            inner: Arc::new(Mutex::new(stream)),
        }
    }

    pub fn send(&self, msg: Msg, req: u64, payload: &[u8]) -> std::io::Result<()> {
        let mut s = self.inner.lock().unwrap();
        write_frame(&mut *s, &Frame { req, msg }, payload)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ActorState {
    Spawning,
    Running,
    Dead { reason: String },
}

pub struct ActorInfo {
    pub id: ActorId,
    pub name: String,
    pub group: String,
    pub agent: AgentId,
    pub node_id: NodeId,
    pub gpu_ids: Vec<u32>,
    pub owner: ClientId,
    pub state: ActorState,
    pub pid: Option<u32>,
}

pub struct AgentInfo {
    pub id: AgentId,
    pub group: String,
    pub node_id: NodeId,
    pub node_ip: String,
    pub gpus: Vec<u32>,
    pub gpu_vendor: String,
    pub cpus: u32,
    pub container: String,
    pub pid: u32,
    pub writer: FrameWriter,
    pub alive: bool,
    /// Registration order; placement uses it for deterministic bundle order.
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PgState {
    Pending,
    Created,
    Removed,
}

#[derive(Debug, Clone)]
pub struct BundleAssignment {
    pub agent: AgentId,
    pub node_id: NodeId,
    pub gpu_ids: Vec<u32>,
}

pub struct PgInfo {
    pub id: PgId,
    pub group: String,
    pub owner: ClientId,
    pub bundles: Vec<f64>,
    pub strategy: String,
    pub assignment: Vec<Option<BundleAssignment>>,
    pub state: PgState,
}

pub enum RefState {
    Pending,
    Ready { ok: bool, payload: Vec<u8> },
    ActorDied { reason: String },
}

pub struct RefInfo {
    pub state: RefState,
    pub actor: Option<ActorId>,
    pub owner: ClientId,
}

pub struct ClientInfo {
    pub id: ClientId,
    pub group: String,
    pub node_id: NodeId,
    pub has_session: bool,
}

#[derive(Default)]
pub struct Counters {
    pub actors_spawned: u64,
    pub actor_exits_clean: u64,
    pub actor_exits_signal: u64,
    pub actor_exits_error: u64,
    pub calls_total: u64,
    pub clients_total: u64,
    pub agents_registered: u64,
}

pub struct State {
    pub node_id: NodeId,
    pub node_ip: String,
    pub hostname: String,
    pub gcs_address: String,
    pub agents: HashMap<AgentId, AgentInfo>,
    pub actors: HashMap<ActorId, ActorInfo>,
    pub pgs: HashMap<PgId, PgInfo>,
    pub refs: HashMap<RefId, RefInfo>,
    pub clients: HashMap<ClientId, ClientInfo>,
    pub next_seq: u64,
    pub next_ref: u64,
    pub counters: Counters,
    pub next_event_seq: u64,
    /// Live WebSocket subscribers get every new event pushed.
    pub event_subs: Vec<std::sync::mpsc::Sender<String>>,
}

pub struct Shared {
    pub st: Mutex<State>,
    /// One condvar for everything that blocks (get/wait/pg-ready). At this
    /// scale broadcast wakeups are simpler than per-ref parking and cost
    /// nothing measurable.
    pub cv: Condvar,
}

pub type SharedRef = Arc<Shared>;

impl State {
    pub fn new(node_ip: String, hostname: String, gcs_address: String) -> Self {
        State {
            node_id: node_id_for(&node_ip),
            node_ip,
            hostname,
            gcs_address,
            agents: HashMap::new(),
            actors: HashMap::new(),
            pgs: HashMap::new(),
            refs: HashMap::new(),
            clients: HashMap::new(),
            next_seq: 1,
            next_ref: 1,
            counters: Counters::default(),
            next_event_seq: 1,
            event_subs: Vec::new(),
        }
    }

    pub fn seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    pub fn new_ref_id(&mut self, actor: &ActorId) -> RefId {
        let r = self.next_ref;
        self.next_ref += 1;
        format!("{actor}:{r}")
    }

    /// Record an event: ring buffer for late subscribers, push to live ones.
    pub fn emit(&mut self, kind: &str, fields: Value) {
        let seq = self.next_event_seq;
        self.next_event_seq += 1;
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut data = json!({ "type": kind, "seq": seq, "ts_ms": ts_ms as u64 });
        if let (Some(obj), Some(f)) = (data.as_object_mut(), fields.as_object()) {
            for (k, v) in f {
                obj.insert(k.clone(), v.clone());
            }
        }
        let line = data.to_string();
        crate::logfmt::log("event", &[("data", line.clone())]);
        self.event_subs.retain(|tx| tx.send(line.clone()).is_ok());
    }

    /// GPUs of one agent not reserved by any live placement-group bundle.
    pub fn free_gpus_of(&self, agent_id: &AgentId) -> Vec<u32> {
        let Some(agent) = self.agents.get(agent_id) else {
            return Vec::new();
        };
        let mut free: Vec<u32> = agent.gpus.clone();
        for pg in self.pgs.values() {
            if pg.state == PgState::Removed {
                continue;
            }
            for b in pg.assignment.iter().flatten() {
                if &b.agent == agent_id {
                    free.retain(|g| !b.gpu_ids.contains(g));
                }
            }
        }
        free
    }
}

/// Stable node id derived from the node's cluster IP: hex of "mentat:<ip>",
/// zero-padded to ray's 56-hex-char shape. vLLM only ever compares these for
/// equality and uses them as dict keys, so shape is all that matters.
pub fn node_id_for(ip: &str) -> NodeId {
    let mut hex = String::with_capacity(56);
    for b in format!("mentat:{ip}").bytes() {
        hex.push_str(&format!("{b:02x}"));
    }
    hex.truncate(56);
    while hex.len() < 56 {
        hex.push('0');
    }
    hex
}

/// Random 32-hex actor/pg/client ids, ray-shaped.
pub fn random_hex_id() -> String {
    let mut buf = [0u8; 16];
    // /dev/urandom exists on both macOS and Linux; failure here means the OS
    // is broken enough that aborting is correct.
    let mut f = std::fs::File::open("/dev/urandom").expect("open /dev/urandom");
    std::io::Read::read_exact(&mut f, &mut buf).expect("read /dev/urandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Best-effort local IP discovery: the address a UDP socket would use to
/// reach the given destination (no packets are sent).
pub fn local_ip_toward(dest: &str) -> Option<String> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect(dest).ok()?;
    Some(sock.local_addr().ok()?.ip().to_string())
}

/// Write a small JSON file atomically (tmp + rename).
pub fn write_json_file(path: &str, value: &Value) -> std::io::Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = format!("{path}.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(value.to_string().as_bytes())?;
    f.flush()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
