//! Wire protocol for every mentat link: client, agent, mesh peer, and actor
//! host.
//!
//! A frame is u32le header_len, u32le payload_len, a flat JSON header, and an
//! opaque payload. The header's `t` names the `Msg` variant. The payload is
//! Python pickle bytes, moved through Rust unread.
//!
//! The connection's first frame chooses the link: `hello` for a client,
//! `agent_register` for an agent, `peer_hello` for a mesh peer, `probe` for a
//! one-shot reachability check. Any other opening frame gets
//! `err`.
//!
//! python/ray/_client.py, _host.py and register.py build these headers by
//! hand, so a field here is a field in four programs.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Cap on each length prefix. A corrupt length would otherwise size an
/// allocation straight off the wire.
const MAX_FRAME: u32 = 256 * 1024 * 1024;

/// One frame header: a correlation id and the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// Correlation id, echoed by the answer. A client numbers its requests
    /// from 1 on each connection. The agent, peer and host links push frames
    /// with 0 and correlate by `actor_id` or `ref_id` instead.
    #[serde(default)]
    pub req: u64,
    #[serde(flatten)]
    pub msg: Msg,
}

/// The wire version this build uses, `major.minor`.
///
/// 0.99 is the 1.0 candidate: the shapes are 1.0's and the number moves when
/// the spec is accepted. rust/mentatd-serve, python/ray/_client.py, _host.py
/// and register.py each hold their own copy of this string.
pub const PROTO: &str = "0.99";

/// Whether a peer's `proto` shares this build's major.
///
/// A major bump changes a field's type or meaning, so the link is refused. A
/// minor difference is compatible both ways, since a peer sends only what the
/// minor its counterpart announced defines. Returns false for a version with
/// no dot, or with a minor that is not a plain number.
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

/// `PROTO` as a `String`, for the message fields that hold one.
pub fn proto() -> String {
    PROTO.to_string()
}

/// Every message on every link.
///
/// Each variant belongs to one link: client, agent, mesh peer, or the actor
/// host's unix socket. A receiver acts on a variant only on that variant's
/// own link.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    // ---- client -> daemon ----
    /// Opens a client link. Returns `hello_ok`.
    Hello {
        proto: String,
        client_id: String,
        group: String,
        /// Claims the group's one driver session. A second client requesting
        /// it is refused, and this connection's EOF reaps the client's actors
        /// and claims.
        session: bool,
        /// "driver" or "cli".
        kind: String,
        /// The box the client is on. Empty leaves the daemon to read it off the
        /// connection, and a relaying daemon fills it in for a local client.
        #[serde(default)]
        node_ip: String,
    },
    /// The group's nodes, shaped as `ray.nodes()`. Returns `nodes_ok`.
    Nodes,
    /// The group's total GPU, CPU and memory. Returns `resources_ok`.
    Resources,
    /// Free GPUs per node, with each node's cpu and memory. Returns
    /// `available_ok`.
    Available,
    /// Claim a named placement. Every holder of one name gets the view the
    /// first claim solved, so ranks agree without talking to each other. Only
    /// the head replies, and a holder keeps the claim by re-sending this on
    /// reconnect.
    Claim {
        /// Names a claim within the caller's group, so two groups that pick
        /// one name hold two claims.
        name: String,
        /// `{"sets": [...], "between": [...]}`, matched against the topology
        /// the probes found. A second claim on one name describing another
        /// shape is refused.
        shape: serde_json::Value,
    },
    /// Create a placement group. Returns `pg_create_ok`.
    PgCreate {
        /// Whole GPUs per bundle.
        bundles: Vec<u32>,
        /// Ray's strategy name, such as "PACK". Recorded, and echoed by
        /// `pg_table`.
        strategy: String,
        /// A claim this group is placed inside, so placement picks among the
        /// nodes that claim already chose. Empty places from the whole
        /// cluster.
        #[serde(default)]
        claim: String,
    },
    /// Read one group's table. Returns `pg_table_ok`.
    PgTable { pg_id: String },
    /// Release a placement group and its GPUs. Returns `ok`.
    PgRemove { pg_id: String },
    /// Create one actor. The payload is a pickled (class, args, kwargs).
    /// Returns `actor_create_ok`.
    ActorCreate {
        name: String,
        num_gpus: u32,
        pg_id: String,
        bundle_index: usize,
        env: BTreeMap<String, String>,
    },
    /// Call one method. The payload is pickled (args, kwargs), and
    /// `actor_call_ok` returns a ref before the call runs.
    ActorCall { actor_id: String, method: String },
    /// Resolve one ref. Returns `ref_get_ok`.
    RefGet {
        ref_id: String,
        /// None blocks until the ref resolves. 0 polls once.
        timeout_ms: Option<u64>,
    },
    /// Wait for `num_returns` of `ref_ids`. Returns `ref_wait_ok`.
    RefWait {
        ref_ids: Vec<String>,
        num_returns: usize,
        timeout_ms: Option<u64>,
    },
    /// Read the cluster snapshot. `group` None covers every group.
    Status { group: Option<String> },
    /// Kill a group's actors, or every group's with `all`. Neither or both is
    /// refused: the binary is also installed as `ray`, where an inherited `ray
    /// stop` would otherwise reach the whole cluster.
    ActorStop {
        #[serde(default)]
        group: String,
        #[serde(default)]
        all: bool,
    },

    // ---- daemon -> client responses ----
    /// A request that returns nothing. The Python shim uses it in place of
    /// any expected response type.
    Ok,
    /// The request failed, or a handshake was refused. `error` reaches the
    /// caller as the message of its exception.
    Err { error: String },
    /// The reply to `hello`. `control_addr` is where this daemon accepts control
    /// connections, and the shim passes it to actors as MENTAT_GCS_ADDRESS.
    HelloOk {
        proto: String,
        node_id: String,
        node_ip: String,
        control_addr: String,
        head_node_id: String,
    },
    /// Ray's node dicts, one per node hosting the group plus the daemon's own.
    NodesOk { nodes: Vec<Value> },
    /// Group totals under Ray's keys: "GPU", "CPU", "memory",
    /// "object_store_memory".
    ResourcesOk { resources: BTreeMap<String, f64> },
    /// Free resources per node id, including "node:<ip>" as Ray reports it.
    AvailableOk {
        nodes: BTreeMap<String, BTreeMap<String, f64>>,
    },
    /// The reply to `claim`. `generation` rises with each new solve, so a rank can
    /// tell one solve from another, and `view` is the solved topology.
    ClaimOk {
        name: String,
        generation: u64,
        view: serde_json::Value,
    },
    PgCreateOk {
        /// Also the ref `ref_get` resolves once the group is CREATED.
        pg_id: String,
    },
    /// Ray's `placement_group_table()` dict.
    PgTableOk { table: Value },
    /// The reply to `actor_create`, sent once the spawn reaches the agent. The actor's
    /// constructor is still running.
    ActorCreateOk {
        actor_id: String,
        node_id: String,
        /// The device indices bound for this actor.
        gpu_ids: Vec<u32>,
    },
    /// The ref that resolves when the call finishes.
    ActorCallOk { ref_id: String },
    /// The reply to `ref_get`. The payload is the pickled value for "ok" and the
    /// pickled exception for "error", and is empty otherwise.
    RefGetOk {
        /// "ok", "error", "actor_died" or "timeout".
        status: String,
        /// Why the actor died. Empty for every other status.
        #[serde(default)]
        reason: String,
    },
    /// The refs that resolved, in the order given and capped at
    /// `num_returns`. A ref that failed counts as resolved.
    RefWaitOk { ready: Vec<String> },
    /// The reply to `status`, with the snapshot `/status` also serves.
    StatusOk { snapshot: Value },

    // ---- agent <-> daemon ----
    /// Opens an agent link. The daemon returns `agent_register_ok` as the
    /// first frame back.
    AgentRegister {
        proto: String,
        /// "group@container@node_ip".
        agent_id: String,
        group: String,
        /// Empty leaves the daemon to read the node off the connection.
        node_ip: String,
        container: String,
        pid: u32,
        /// What this box is: total memory, cpus, and every GPU the agent may
        /// bind.
        machine: Machine,
        /// Endpoints this container announces, from MENTAT_OPENAI_API and
        /// MENTAT_MCP_API. The daemon stores and republishes them for
        /// mentatd-serve.
        #[serde(default)]
        services: BTreeMap<String, Service>,
        /// Actor processes still running from before this register, so a
        /// daemon that lost its state adopts them rather than orphaning them.
        resume: Vec<ResumeActor>,
        /// Refs whose results the agent buffered through a link outage and
        /// re-sends right after this frame. The daemon holds them pending
        /// until those results arrive.
        #[serde(default)]
        unacked_refs: Vec<String>,
    },
    /// The reply to `agent_register`.
    AgentRegisterOk { proto: String, node_id: String },
    /// Start one actor process. The payload is a pickled (class, args,
    /// kwargs).
    ActorSpawn {
        actor_id: String,
        name: String,
        env: BTreeMap<String, String>,
        gpu_ids: Vec<u32>,
        node_id: String,
        control_addr: String,
        /// The `client_id` that owns the actor. The agent reports it again in
        /// `resume`, which is how a restarted daemon learns who owns what.
        owner: String,
    },
    /// Reports the spawn. `ok` is true once the constructor returned.
    ActorSpawnResult {
        actor_id: String,
        ok: bool,
        #[serde(default)]
        error: String,
        /// The actor process pid, which only the agent knows. 0 when the
        /// process never started.
        #[serde(default)]
        pid: u32,
    },
    /// An already-numbered call, handed to the agent that runs it. The
    /// payload is pickled (args, kwargs). `actor_call` is the client requesting
    /// for one and getting a ref back.
    ActorDispatch {
        actor_id: String,
        ref_id: String,
        method: String,
    },
    /// The call finished. The payload is the pickled value, or the pickled
    /// exception when `ok` is false. The first result for a ref wins.
    ActorResult {
        ref_id: String,
        ok: bool,
        /// Set with an empty payload when the call never reached Python. The
        /// daemon records that as the actor dying.
        #[serde(default)]
        error: String,
    },
    /// The actor process is gone. `exit_code` Some(0) is a clean exit, and
    /// `signal` names the signal that killed it.
    ActorExit {
        actor_id: String,
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
    /// Terminate one actor. The client sends it to the daemon and the daemon
    /// forwards it to the agent unchanged.
    ActorKill { actor_id: String },
    /// A finding about a service this agent already announced, such as its
    /// server binding one address. The agent learns it after registering, so
    /// it arrives on its own. An empty `note` clears the last one.
    ServiceNote { service: String, note: String },
    /// Liveness check. The answer echoes `req`.
    Ping,
    /// The reply to `ping`.
    Pong,

    // ---- daemon <-> daemon (mesh) ----
    /// Opens a mesh link. Returns `peer_hello_ok`, which holds the
    /// same fields for the other side.
    PeerHello {
        proto: String,
        node_id: String,
        node_ip: String,
        control_port: u16,
        http_port: u16,
        /// Every address this daemon listens on, in its announce order.
        /// `node_ip` is only what it calls itself, so a third party may not
        /// route there.
        addrs: Vec<String>,
        /// Operator tags per address, for consumers that route classes of
        /// traffic over different links. An address given no tags has no
        /// entry.
        addr_tags: BTreeMap<String, Vec<String>>,
        /// The interface each address sits on. Only an address discovered from
        /// one has an entry, so MENTAT_ANNOUNCE_ADDRS leaves it out.
        addr_ifaces: BTreeMap<String, String>,
    },
    /// The reply to `peer_hello`, with the responder's side of the same fields.
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
    /// Reachability probe, sent as the first frame of its own short-lived
    /// connection rather than over the mesh link. The prober binds one of its
    /// own addresses before connecting, so an answer proves that one address
    /// pair holds traffic. The mesh link proves nothing about any other pair.
    Probe {
        proto: String,
        /// The prober's node id, so a mistargeted probe is visible.
        node_id: String,
        /// The address the prober bound locally.
        local_addr: String,
    },
    /// The reply to `probe`. The prober checks `node_id`, since both fabrics are
    /// numbered out of one subnet and an address that replies is no evidence
    /// of which node replied.
    ProbeOk { proto: String, node_id: String },
    /// A daemon's own snapshot, pushed on an interval, so every daemon serves
    /// a merged cluster view without forwarding requests. The receiver reads
    /// "addrs", "addr_tags" and "addr_ifaces" out of it to follow a peer's
    /// address changes.
    PeerStatus { snapshot: Value },
    /// A locally-originated event, replicated so any daemon's `/events`
    /// stream holds the whole cluster. A received event goes to local
    /// subscribers and stops there. The origin is the event's own `node`.
    PeerEvent { event: Value },

    // ---- actor host (python) <-> agent, over the per-actor unix socket ----
    /// The actor process announcing it is ready for `ctor`. The socket is per
    /// actor, so connecting is the identification.
    HostHello { proto: String },
    /// Construct the actor. The payload is a pickled (class, args, kwargs).
    Ctor { proto: String },
    /// The constructor returned.
    CtorOk,
    /// The constructor raised. The payload is the pickled exception.
    CtorErr {
        /// repr() of the exception, so the reason reaches Rust logs and the
        /// driver's error text without unpickling.
        #[serde(default)]
        error: String,
    },
    /// Run one method. The payload is pickled (args, kwargs).
    HostCall { ref_id: String, method: String },
    /// The method returned. The payload is the pickled value, or the pickled
    /// exception when `ok` is false.
    HostResult { ref_id: String, ok: bool },

    /// A `t` this build does not know.
    ///
    /// A minor bump may add a message type, so the frame parses and the
    /// receiver returns `err` on the same `req` with the link kept. The
    /// unknown message's own fields are lost.
    #[serde(other)]
    Unknown,
}

/// What a box is: total memory, cpus, and every GPU an agent may bind.
///
/// A count cannot describe a box with two GPU models in it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    /// Total system memory in bytes.
    pub memory: u64,
    pub cpus: u32,
    /// Every device the agent may bind, in device order.
    pub gpus: Vec<Gpu>,
}

/// One GPU as the machine probe reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gpu {
    /// The device index the agent binds on `actor_spawn`.
    pub index: u32,
    /// The vendor the probe matched, such as "nvidia".
    pub vendor: String,
    /// The vendor's product string, e.g. "RTX 6000".
    pub name: String,
    /// The device's own memory in bytes. For a UMA device this is the shared
    /// pool, the same figure as the machine's `memory`, so a consumer adding
    /// the two counts it twice.
    pub memory: u64,
    /// The device shares the system memory pool.
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
    /// What the agent found after announcing, such as its server binding one
    /// address. Advisory: a failed probe quotes it.
    #[serde(default)]
    pub note: String,
}

/// One actor process the agent still holds, reported in `agent_register`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeActor {
    pub actor_id: String,
    /// The actor's class name, as `actor_spawn` gave it.
    pub name: String,
    pub gpu_ids: Vec<u32>,
    pub pid: u32,
    /// The client this actor belongs to, as the daemon named it on spawn.
    #[serde(default)]
    pub owner: String,
    /// Ref ids of calls the agent has relayed but not yet replied,
    /// including the long-lived run() ref.
    pub pending_refs: Vec<String>,
}

/// Writes the frame and its payload, then flushes.
pub fn write_frame<W: Write>(w: &mut W, frame: &Frame, payload: &[u8]) -> io::Result<()> {
    let header = serde_json::to_vec(frame)?;
    w.write_all(&(header.len() as u32).to_le_bytes())?;
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&header)?;
    w.write_all(payload)?;
    w.flush()
}

/// Reads the next frame, blocking until it arrives.
///
/// Returns Ok(None) on a clean EOF at a frame boundary. An EOF part way
/// through a frame, a length over `MAX_FRAME`, or a header that is not JSON
/// is an error.
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
        assert!(read_frame(&mut cur).is_err());
    }

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

    #[test]
    fn uma_defaults_to_false() {
        let g: Gpu = serde_json::from_str(
            r#"{"index":1,"vendor":"nvidia","name":"RTX 6000","memory":51539607552}"#,
        )
        .unwrap();
        assert!(!g.uma);
    }

    #[test]
    fn an_unknown_message_type_parses_as_unknown() {
        let mut buf: Vec<u8> = Vec::new();
        let header = br#"{"req":3,"t":"no_such_message","extra":1}"#;
        buf.extend_from_slice(&(header.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(header);
        let mut cur = std::io::Cursor::new(buf);
        let (frame, _) = read_frame(&mut cur).unwrap().unwrap();
        assert_eq!(frame.req, 3);
        assert!(matches!(frame.msg, Msg::Unknown), "{:?}", frame.msg);
    }

    /// A frame that is not JSON still closes the link: nothing can be
    /// replied when the correlation id itself is unreadable.
    #[test]
    fn a_malformed_header_is_still_an_error() {
        let mut buf: Vec<u8> = Vec::new();
        let header = b"{not json";
        buf.extend_from_slice(&(header.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(header);
        let mut cur = std::io::Cursor::new(buf);
        assert!(read_frame(&mut cur).is_err());
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
