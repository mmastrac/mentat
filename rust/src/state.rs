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
    /// Calls held while the agent link is down (the degrade window), sent in
    /// arrival order when the agent re-registers.
    pub queued_calls: Vec<QueuedCall>,
}

/// A held call: (ref_id, method, payload).
pub type QueuedCall = (RefId, String, Vec<u8>);

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
    /// Announced service endpoints ("openai", "mcp" -> URL). Stored and
    /// republished verbatim for mentatd-serve.
    pub services: std::collections::BTreeMap<String, String>,
    /// The same, for services announced as a port with the host left open.
    /// The daemon does not resolve them: it does not know which links the
    /// router shares with this node, and the router does.
    pub services_ports: std::collections::BTreeMap<String, crate::proto::ServicePort>,
    /// What the agent noticed about a service after announcing it, keyed by
    /// service name. Republished so the router can say why a probe failed.
    pub service_notes: std::collections::BTreeMap<String, String>,
    /// What serves the announced `openai` endpoint (`vllm`). Stored and
    /// republished verbatim, like the endpoints themselves.
    pub provider: String,
    pub writer: FrameWriter,
    pub alive: bool,
    /// When the agent link EOFed (degrade window start). None while
    /// connected, and cleared again once the give-up threshold has fired.
    pub lost_at_ms: Option<u64>,
    /// The degrade event has been emitted for the current outage.
    pub degraded: bool,
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
    pub created_ms: u64,
    /// Why a Removed pg failed (pending timeout etc.); surfaced as the
    /// RayActorError reason when the driver gets the ready ref.
    pub fail_reason: Option<String>,
    /// The claim whose nodes this group was placed inside, empty when it
    /// was placed from the whole cluster.
    pub claim: String,
    /// The fabric island this group was placed inside, when placement had
    /// to pick one. None for a single-bundle group, for a group that fits
    /// on one node, and on a cluster with no derived islands.
    pub island: Option<crate::island::Island>,
    /// Why the last placement attempt did not fit, kept so the pending
    /// timeout can name the constraint rather than guess at it.
    pub pending_reason: Option<String>,
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
    pub method: String,
    pub created_ms: u64,
    /// Set once the slow-call sweeper has complained about this ref.
    pub warned: bool,
}

pub struct ClientInfo {
    pub id: ClientId,
    pub group: String,
    pub node_id: NodeId,
    pub has_session: bool,
}

/// A named placement, held by everyone who claimed it.
///
/// The name is the reservation. Every holder of one name gets the answer the
/// first claim produced, so ranks starting independently agree on their node
/// set without a coordinator between them. A claim ends when its last holder
/// goes, which is what makes a driver that dies give its nodes back.
pub struct ClaimInfo {
    /// The request this was solved for. A second claim describing something
    /// else is a conflict rather than a re-solve.
    pub shape: serde_json::Value,
    /// The answer, returned verbatim to every later holder.
    pub view: serde_json::Value,
    /// Bumped on each solve, so a holder can tell one answer from another
    /// across a head change.
    pub generation: u64,
    pub holders: std::collections::BTreeSet<ClientId>,
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

/// One probed (local address -> peer address) pair.
///
/// Reachability is a property of the pair. The same peer address answers
/// from one of this node's links and refuses from another, which is why the
/// prober binds a local address before connecting and why this table is two
/// levels deep.
#[derive(Clone)]
pub struct PairProbe {
    pub ok: bool,
    /// Round trip of the last successful probe: connect, frame, reply.
    pub rtt_ms: u64,
    /// When the pair last answered. 0 means it never has.
    pub last_ok_ms: u64,
    /// Why the last attempt failed. Empty while ok.
    pub error: String,
}

/// Probed pairs, keyed local address then remote address. An address pair
/// with no entry has not been tried yet. A pair that failed has an entry.
pub type ProbeTable =
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, PairProbe>>;

pub struct PeerInfo {
    pub node_id: NodeId,
    pub node_ip: String,
    /// The address this link actually uses: the socket's peer address
    /// inbound, the address we dialed outbound. node_ip is what the peer
    /// calls itself, which on a multi-homed box is a subnet a third party
    /// may not route to. This one carried a working connection.
    pub link_ip: String,
    /// Every address the peer says it answers on, for a consumer that can
    /// reach none of node_ip or link_ip.
    pub addrs: Vec<String>,
    /// Operator tags per address. Read here for one purpose: an `rdma` tag
    /// on both ends of a probe-ok pair is what makes two nodes fabric
    /// neighbours (see island::islands).
    pub addr_tags: std::collections::BTreeMap<String, Vec<String>>,
    /// The interface each address sits on, where the peer knew one.
    pub addr_ifaces: std::collections::BTreeMap<String, String>,
    /// True when this peer answers probes. False for a daemon that predates
    /// them, which is never probed.
    pub probes: bool,
    /// What probing this peer has found, keyed local then remote address.
    pub probe_pairs: ProbeTable,
    pub control_addr: String,
    pub http_port: u16,
    pub writer: FrameWriter,
    pub alive: bool,
    pub last_seen_ms: u64,
    /// The staleness warning has fired for the current silence.
    pub stale: bool,
    pub last_status: Value,
}

pub struct State {
    pub node_id: NodeId,
    pub node_ip: String,
    pub hostname: String,
    pub gcs_address: String,
    /// This daemon's own HTTP side-port, echoed on PeerHelloOk so the
    /// dialing side records full membership.
    pub http_port: u16,
    /// Mesh view. Key is the peer's node_id.
    pub peers: HashMap<NodeId, PeerInfo>,
    /// Fabric islands and the nodes that claim one, committed after the
    /// island hold-down. A group none of whose nodes are tagged is placed
    /// without the constraint, so a deployment that has not opted in keeps
    /// placing exactly as it did before islands existed.
    pub fabrics: crate::island::Fabrics,
    /// The elected head (lowest node_id among self + live peers, after
    /// hold-down). Starts as self.
    pub head_node_id: NodeId,
    pub head_generation: u64,
    pub agents: HashMap<AgentId, AgentInfo>,
    pub actors: HashMap<ActorId, ActorInfo>,
    pub pgs: HashMap<PgId, PgInfo>,
    pub refs: HashMap<RefId, RefInfo>,
    pub clients: HashMap<ClientId, ClientInfo>,
    /// Named placements, by name. Only the head fills this in.
    pub claims: std::collections::BTreeMap<String, ClaimInfo>,
    pub claim_generation: u64,
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
        let node_id = node_id_for(&node_ip);
        State {
            head_node_id: node_id.clone(),
            head_generation: 0,
            fabrics: crate::island::Fabrics::default(),
            peers: HashMap::new(),
            node_id,
            node_ip,
            hostname,
            gcs_address,
            http_port: 0,
            agents: HashMap::new(),
            actors: HashMap::new(),
            pgs: HashMap::new(),
            refs: HashMap::new(),
            clients: HashMap::new(),
            claims: std::collections::BTreeMap::new(),
            claim_generation: 0,
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

    /// Record a locally-originated event: push to live subscribers and
    /// replicate to mesh peers (who deliver to their subscribers only --
    /// events are never re-forwarded).
    pub fn emit(&mut self, kind: &str, fields: Value) {
        let seq = self.next_event_seq;
        self.next_event_seq += 1;
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut data =
            json!({ "type": kind, "seq": seq, "ts_ms": ts_ms as u64, "node": self.node_id });
        if let (Some(obj), Some(f)) = (data.as_object_mut(), fields.as_object()) {
            for (k, v) in f {
                obj.insert(k.clone(), v.clone());
            }
        }
        let line = data.to_string();
        crate::logfmt::log("event", &[("data", line.clone())]);
        self.event_subs.retain(|tx| tx.send(line.clone()).is_ok());
        let origin = self.node_id.clone();
        for peer in self.peers.values() {
            if peer.alive {
                let _ = peer.writer.send(
                    crate::proto::Msg::PeerEvent {
                        origin: origin.clone(),
                        line: line.clone(),
                    },
                    0,
                    &[],
                );
            }
        }
    }

    /// Deliver a peer's replicated event to local subscribers.
    pub fn deliver_peer_event(&mut self, line: String) {
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
/// node_ip_of reverses it.
/// Whether an address belongs to every box rather than identifying one.
pub fn is_loopback(ip: &str) -> bool {
    ip.parse::<std::net::IpAddr>()
        .map(|a| a.is_loopback())
        .unwrap_or(false)
}

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

/// The IP a node id was built from, or None if it was not built by
/// node_id_for. The inverse exists because the id is the only handle some
/// views carry, and a truncated one identifies nothing: every mentat node id
/// starts with the hex of "mentat:".
pub fn node_ip_of(node_id: &str) -> Option<String> {
    let bytes: Vec<u8> = node_id
        .as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).ok()?, 16).ok())
        .collect::<Option<_>>()?;
    let text = String::from_utf8(bytes.into_iter().take_while(|b| *b != 0).collect()).ok()?;
    text.strip_prefix("mentat:").map(str::to_string)
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

pub fn now_ms_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
