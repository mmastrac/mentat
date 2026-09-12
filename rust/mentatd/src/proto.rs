//! Wire protocol shared by every mentat connection: client (Python shim /
//! CLI), agent, actor host (unix socket), and mesh peer.
//!
//! Frame layout: u32le header_len | u32le payload_len | JSON header | payload.
//! The payload carries pickle bytes end-to-end and is never inspected in
//! Rust -- the Python ends are the only ones that deserialize it.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Hard cap on header+payload. The largest legitimate payload is a pickled
/// vLLM config (a few MB); 256 MB means a corrupt length prefix fails fast
/// instead of allocating the unified pool.
const MAX_FRAME: u32 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// Correlation id. Request/response pairs echo it; unsolicited events use 0.
    #[serde(default)]
    pub req: u64,
    #[serde(flatten)]
    pub msg: Msg,
}

/// The wire version this build speaks, `major.minor`.
///
/// 0.99 is the 1.0 candidate: the shapes are those of the 1.0 spec, and the
/// number moves to 1.0 once the spec is accepted.
pub const PROTO: &str = "0.99";

/// Whether a peer's `proto` shares this build's major.
///
/// A major mismatch means a field changed type or meaning, so the link is
/// refused. A minor difference is compatible in both directions: a peer
/// sends nothing introduced after the minor its counterpart announced.
pub fn major_matches(peer: &str) -> bool {
    fn major(v: &str) -> Option<&str> {
        let (maj, rest) = v.split_once('.')?;
        rest.parse::<u32>().ok()?;
        Some(maj)
    }
    match (major(PROTO), major(peer)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

pub fn proto() -> String {
    PROTO.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    // ---- client -> daemon ----
    Hello {
        proto: String,
        client_id: String,
        group: String,
        /// True for the one connection whose EOF means "this driver is gone".
        session: bool,
        kind: String, // "driver" | "cli"
        /// The box the client is on, filled in by the daemon that relayed
        /// the connection to the head. Empty from the client itself.
        #[serde(default)]
        node_ip: String,
    },
    Nodes,
    Resources,
    Available,
    /// Claim a named placement. Every holder of one name is answered with
    /// the view the first claim produced, so ranks agree without talking to
    /// each other. Re-sent on reconnect: that is what holds the claim.
    ///
    /// The name belongs to the caller's group, so two groups that pick one
    /// name hold two claims.
    Claim {
        name: String,
        /// `{"sets": [...], "between": [...]}`, matched against the measured
        /// topology. A second claim on one name describing something else is
        /// refused rather than re-solved.
        shape: serde_json::Value,
    },
    PgCreate {
        /// Whole GPUs per bundle.
        bundles: Vec<u32>,
        strategy: String,
        /// A claim this group must be placed inside. The claim already
        /// chose nodes, so placement picks among those rather than from
        /// the cluster. Empty places as before.
        #[serde(default)]
        claim: String,
    },
    PgTable {
        pg_id: String,
    },
    PgRemove {
        pg_id: String,
    },
    ActorCreate {
        name: String,
        num_gpus: u32,
        pg_id: String,
        bundle_index: usize,
        env: BTreeMap<String, String>,
        // payload: pickle (cls, args, kwargs)
    },
    ActorCall {
        actor_id: String,
        method: String,
        // payload: pickle (args, kwargs)
    },
    RefGet {
        ref_id: String,
        /// None = block forever, 0 = immediate poll.
        timeout_ms: Option<u64>,
    },
    RefWait {
        ref_ids: Vec<String>,
        num_returns: usize,
        timeout_ms: Option<u64>,
    },
    Status {
        group: Option<String>,
    },
    /// Kill a group's actors, or every group's with `all`. Neither or both
    /// is refused: the binary also answers to `ray`, where an inherited
    /// `ray stop` would otherwise reach the whole cluster.
    ActorStop {
        #[serde(default)]
        group: String,
        #[serde(default)]
        all: bool,
    },

    // ---- daemon -> client responses ----
    Ok,
    Err {
        error: String,
    },
    HelloOk {
        proto: String,
        node_id: String,
        node_ip: String,
        control_addr: String,
        head_node_id: String,
    },
    NodesOk {
        nodes: Vec<Value>,
    },
    ResourcesOk {
        resources: BTreeMap<String, f64>,
    },
    AvailableOk {
        nodes: BTreeMap<String, BTreeMap<String, f64>>,
    },
    ClaimOk {
        name: String,
        generation: u64,
        view: serde_json::Value,
    },
    PgCreateOk {
        /// Also the handle `ref_get` resolves when the group is CREATED.
        pg_id: String,
    },
    PgTableOk {
        table: Value,
    },
    ActorCreateOk {
        actor_id: String,
        node_id: String,
        gpu_ids: Vec<u32>,
    },
    ActorCallOk {
        ref_id: String,
    },
    RefGetOk {
        /// "ok" | "error" | "actor_died" | "timeout"
        status: String,
        /// Human-readable death reason when status == actor_died.
        #[serde(default)]
        reason: String,
        // payload: pickle result or pickled exception
    },
    RefWaitOk {
        ready: Vec<String>,
    },
    StatusOk {
        snapshot: Value,
    },

    // ---- agent <-> daemon ----
    AgentRegister {
        proto: String,
        agent_id: String,
        group: String,
        node_ip: String,
        container: String,
        pid: u32,
        /// What this box is: total memory, cpus, and every GPU the agent
        /// may bind. A count cannot describe a heterogeneous box.
        machine: Machine,
        /// Service endpoints this container announces (e.g. "openai" -> the
        /// vLLM API, "mcp" -> the status server), read from MENTAT_*_API.
        /// Consumed by mentatd-serve; the daemon stores and republishes.
        #[serde(default)]
        services: BTreeMap<String, Service>,
        /// Actors still alive from before a reconnect, so the daemon can
        /// rebuild instead of orphaning them.
        resume: Vec<ResumeActor>,
        /// Ref ids whose results are buffered agent-side from a link outage
        /// and will be re-sent right after this register. The daemon keeps
        /// them pending until those results arrive.
        #[serde(default)]
        unacked_refs: Vec<String>,
    },
    AgentRegisterOk {
        proto: String,
        node_id: String,
    },
    ActorSpawn {
        actor_id: String,
        name: String,
        env: BTreeMap<String, String>,
        gpu_ids: Vec<u32>,
        node_id: String,
        control_addr: String,
        /// The client this actor belongs to, so the agent can name it again
        /// on resume. That is what lets a daemon that lost its state
        /// rebuild who owns what.
        owner: String,
        // payload: pickle (cls, args, kwargs)
    },
    ActorSpawnResult {
        actor_id: String,
        ok: bool,
        #[serde(default)]
        error: String,
        /// The actor process pid, which only the agent knows. 0 when the
        /// failure happened before the fork.
        #[serde(default)]
        pid: u32,
    },
    /// An already-numbered call, dispatched to the agent that runs it.
    /// `ActorCall` is the client asking for one and getting a ref back.
    ActorDispatch {
        actor_id: String,
        ref_id: String,
        method: String,
        // payload: pickle (args, kwargs)
    },
    ActorResult {
        ref_id: String,
        ok: bool,
        /// Set (with empty payload) when the failure originated in mentat
        /// itself rather than in Python -- there is no exception to pickle.
        #[serde(default)]
        error: String,
        // payload: pickle result or pickled exception
    },
    ActorExit {
        actor_id: String,
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    /// Terminate one actor. The client sends it to the daemon and the daemon
    /// forwards it to the agent unchanged.
    ActorKill {
        actor_id: String,
    },
    /// A finding about an already-announced service, sent when the agent
    /// learns it after registering -- the API server binds its socket
    /// minutes after `ray start` returns, so there is nothing to report at
    /// register time. Empty `note` clears one.
    ServiceNote {
        service: String,
        note: String,
    },
    Ping,
    Pong,

    // ---- daemon <-> daemon (mesh) ----
    PeerHello {
        proto: String,
        node_id: String,
        node_ip: String,
        control_port: u16,
        http_port: u16,
        /// Every address the dialing daemon answers on, most preferred
        /// first. node_ip is only what it calls itself, so a third party may
        /// not route there.
        addrs: Vec<String>,
        /// Operator tags per address, for consumers that route classes of
        /// traffic over different links. Carried, never interpreted here.
        addr_tags: BTreeMap<String, Vec<String>>,
        /// The interface each address sits on, where it was discovered from
        /// one. Absent for an address named by MENTAT_ANNOUNCE_ADDRS.
        addr_ifaces: BTreeMap<String, String>,
    },
    PeerHelloOk {
        proto: String,
        node_id: String,
        node_ip: String,
        control_port: u16,
        http_port: u16,
        addrs: Vec<String>,
        addr_tags: BTreeMap<String, Vec<String>>,
        addr_ifaces: BTreeMap<String, String>,
    },
    /// Reachability probe, sent as the FIRST frame of its own short-lived
    /// connection rather than over the mesh link. The point is the socket
    /// underneath it: the prober binds one of its own addresses before
    /// connecting, so an answer proves that one address pair carries
    /// traffic. Nothing about the mesh link would prove that.
    Probe {
        proto: String,
        /// The prober's node id, so a mistargeted probe is visible.
        node_id: String,
        /// The address the prober bound locally. Carried for the answering
        /// daemon's logs. It does not act on it.
        local_addr: String,
    },
    /// The answer, carrying the responder's identity. That is the part worth
    /// having: it says the address reached belongs to the expected node,
    /// rather than to whatever else answers on that port.
    ProbeOk {
        proto: String,
        node_id: String,
    },
    /// Periodic push of a daemon's own snapshot, so every daemon can serve a
    /// merged cluster view without request forwarding.
    PeerStatus {
        snapshot: Value,
    },
    /// A locally-originated event, replicated so any daemon's /events stream
    /// carries the whole cluster. Never re-forwarded. The origin is the
    /// event's own `node`.
    PeerEvent {
        event: Value,
    },

    // ---- actor host (python) <-> agent, over the per-actor unix socket ----
    /// The actor process announcing it is ready for `ctor`. The socket is
    /// per actor, so connecting is the identification.
    HostHello {
        proto: String,
    },
    Ctor {
        proto: String,
        // payload: pickle (cls, args, kwargs)
    },
    CtorOk,
    CtorErr {
        /// repr() of the exception, so the reason survives into Rust logs
        /// and the driver's RayActorError message without unpickling.
        #[serde(default)]
        error: String,
        // payload: pickled exception
    },
    HostCall {
        ref_id: String,
        method: String,
        // payload: pickle (args, kwargs)
    },
    HostResult {
        ref_id: String,
        ok: bool,
        // payload: pickle result or pickled exception
    },
}

/// What a box is: total memory, cpus, and every GPU an agent may bind.
///
/// A count cannot describe a box with two GPU models in it, and memory has
/// to be a figure rather than a tier because a UMA device shares the system
/// pool. Every value is an integer, since a float does not survive the JSON
/// round trip a signature verifier takes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    /// Total system memory in bytes.
    pub memory: u64,
    pub cpus: u32,
    /// Every device the agent may bind, in device order.
    pub gpus: Vec<Gpu>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gpu {
    /// The device index the agent binds on `actor_spawn`.
    pub index: u32,
    pub vendor: String,
    /// The vendor's product string, e.g. "RTX 6000".
    pub name: String,
    /// The device's own memory in bytes. For a UMA device this is the
    /// shared pool, the same figure as the machine's `memory`, so a
    /// consumer adding the two counts it twice.
    pub memory: u64,
    /// True for a device that shares the system pool (DGX Spark).
    #[serde(default)]
    pub uma: bool,
}

/// Where a service listens.
///
/// An empty `host` means the consumer resolves it against the node's
/// addresses, because the container knows its port but not which of its
/// node's links the consumer shares. The consumer forms
/// `http://<host>:<port><path>`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Service {
    #[serde(default)]
    pub host: String,
    pub port: u16,
    /// Empty, or starts with `/`.
    #[serde(default)]
    pub path: String,
    /// What serves the endpoint, from MENTAT_MODEL_PROVIDER. The daemon
    /// stores and forwards it unread.
    #[serde(default)]
    pub provider: String,
    /// What the agent found after announcing, such as its server binding
    /// one address. Advisory: a failed probe quotes it.
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeActor {
    pub actor_id: String,
    pub name: String,
    pub gpu_ids: Vec<u32>,
    pub pid: u32,
    /// The client this actor belongs to, as the daemon named it on spawn.
    #[serde(default)]
    pub owner: String,
    /// Ref ids of calls the agent has relayed but not yet answered
    /// (including the long-lived run() ref).
    pub pending_refs: Vec<String>,
}

pub fn write_frame<W: Write>(w: &mut W, frame: &Frame, payload: &[u8]) -> io::Result<()> {
    let header = serde_json::to_vec(frame)?;
    w.write_all(&(header.len() as u32).to_le_bytes())?;
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&header)?;
    w.write_all(payload)?;
    w.flush()
}

/// Returns Ok(None) on clean EOF at a frame boundary.
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Option<(Frame, Vec<u8>)>> {
    let mut lens = [0u8; 8];
    if !read_exact_or_eof(r, &mut lens)? {
        return Ok(None);
    }
    let hlen = u32::from_le_bytes(lens[0..4].try_into().unwrap());
    let plen = u32::from_le_bytes(lens[4..8].try_into().unwrap());
    if hlen > MAX_FRAME || plen > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame over {MAX_FRAME} bytes: header={hlen} payload={plen}"),
        ));
    }
    let mut header = vec![0u8; hlen as usize];
    r.read_exact(&mut header)?;
    let mut payload = vec![0u8; plen as usize];
    r.read_exact(&mut payload)?;
    let frame: Frame = serde_json::from_slice(&header).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "bad frame header: {e}: {}",
                String::from_utf8_lossy(&header)
            ),
        )
    })?;
    Ok(Some((frame, payload)))
}

/// read_exact, except a clean EOF before the first byte returns Ok(false).
fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(false),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF mid-frame",
                ))
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        let f = Frame {
            req: 7,
            msg: Msg::ActorCall {
                actor_id: "a:01".into(),
                method: "run".into(),
            },
        };
        write_frame(&mut buf, &f, b"PAYLOAD").unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let (g, p) = read_frame(&mut cur).unwrap().unwrap();
        assert_eq!(g.req, 7);
        assert_eq!(p, b"PAYLOAD");
        match g.msg {
            Msg::ActorCall { actor_id, method } => {
                assert_eq!(actor_id, "a:01");
                assert_eq!(method, "run");
            }
            other => panic!("wrong variant: {other:?}"),
        }
        // Clean EOF at the boundary.
        assert!(read_frame(&mut cur).unwrap().is_none());
    }

    #[test]
    fn eof_mid_frame_is_an_error() {
        let mut buf: Vec<u8> = Vec::new();
        let f = Frame {
            req: 1,
            msg: Msg::Ping,
        };
        write_frame(&mut buf, &f, b"").unwrap();
        buf.truncate(buf.len() - 2);
        let mut cur = std::io::Cursor::new(buf);
        // The header is short 2 bytes.
        assert!(read_frame(&mut cur).is_err());
    }

    /// A UMA device repeats the machine total as its own `memory`, and the
    /// figure has to come back as the integer it went out as.
    #[test]
    fn a_uma_machine_round_trips() {
        let m = Machine {
            memory: 137_438_953_472,
            cpus: 20,
            gpus: vec![Gpu {
                index: 0,
                vendor: "nvidia".into(),
                name: "GB10".into(),
                memory: 137_438_953_472,
                uma: true,
            }],
        };
        let back: Machine = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(back, m);
    }

    /// `uma` defaults, so a discrete card needs no entry for it.
    #[test]
    fn uma_defaults_to_false() {
        let g: Gpu = serde_json::from_str(
            r#"{"index":1,"vendor":"nvidia","name":"RTX 6000","memory":51539607552}"#,
        )
        .unwrap();
        assert!(!g.uma);
    }

    /// An unknown `t` is a parse failure today, which closes the link. The
    /// spec answers `err` and keeps it open, so the caller needs to tell
    /// this case from a malformed frame.
    #[test]
    fn an_unknown_message_type_is_reported_as_unknown() {
        let mut buf: Vec<u8> = Vec::new();
        let header = br#"{"req":3,"t":"no_such_message"}"#;
        buf.extend_from_slice(&(header.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(header);
        let mut cur = std::io::Cursor::new(buf);
        match read_frame(&mut cur) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            other => panic!("expected InvalidData, got {other:?}"),
        }
    }

    #[test]
    fn a_major_mismatch_is_refused() {
        assert!(major_matches(PROTO));
        assert!(major_matches("0.1"));
        assert!(!major_matches("1.0"));
        assert!(!major_matches("nonsense"));
        assert!(!major_matches("1"));
    }
}
