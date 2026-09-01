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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    // ---- client -> daemon ----
    Hello {
        client_id: String,
        group: String,
        /// True for the one connection whose EOF means "this driver is gone".
        session: bool,
        kind: String, // "driver" | "cli"
    },
    Nodes,
    ClusterResources,
    AvailablePerNode,
    CreatePg {
        /// GPUs per bundle, e.g. [1.0, 1.0].
        bundles: Vec<f64>,
        strategy: String,
    },
    PgTable {
        pg_id: String,
    },
    RemovePg {
        pg_id: String,
    },
    CreateActor {
        name: String,
        num_gpus: f64,
        pg_id: String,
        bundle_index: usize,
        env: BTreeMap<String, String>,
        // payload: pickle (cls, args, kwargs)
    },
    Call {
        actor_id: String,
        method: String,
        // payload: pickle (args, kwargs)
    },
    Get {
        ref_id: String,
        /// None = block forever, 0 = immediate poll.
        timeout_ms: Option<u64>,
    },
    Wait {
        ref_ids: Vec<String>,
        num_returns: usize,
        timeout_ms: Option<u64>,
    },
    KillActor {
        actor_id: String,
    },
    Status {
        group: Option<String>,
    },
    StopAll {
        group: Option<String>,
    },

    // ---- daemon -> client responses ----
    Ok0,
    Err {
        error: String,
    },
    HelloOk {
        node_id: String,
        node_ip: String,
        gcs_address: String,
        head_node_id: String,
    },
    NodesOk {
        nodes: Vec<Value>,
    },
    ResourcesOk {
        resources: BTreeMap<String, f64>,
    },
    AvailOk {
        nodes: BTreeMap<String, BTreeMap<String, f64>>,
    },
    CreatePgOk {
        pg_id: String,
        ready_ref: String,
    },
    PgTableOk {
        table: Value,
    },
    CreateActorOk {
        actor_id: String,
        node_id: String,
        gpu_ids: Vec<u32>,
    },
    CallOk {
        ref_id: String,
    },
    GetOk {
        /// "ok" | "error" | "actor_died" | "timeout"
        status: String,
        /// Human-readable death reason when status == actor_died.
        #[serde(default)]
        reason: String,
        // payload: pickle result or pickled exception
    },
    WaitOk {
        ready: Vec<String>,
    },
    StatusOk {
        data: Value,
    },

    // ---- agent <-> daemon ----
    AgentRegister {
        agent_id: String,
        group: String,
        node_ip: String,
        gpus: Vec<u32>,
        /// Vendor tag for the GPUs above. Always "nvidia" today; recorded per
        /// agent so the inventory stays honest if that ever changes.
        #[serde(default = "default_gpu_vendor")]
        gpu_vendor: String,
        cpus: u32,
        container: String,
        pid: u32,
        /// Service endpoints this container announces (e.g. "openai" -> the
        /// vLLM API URL, "mcp" -> the status-server's MCP URL), read from
        /// MENTAT_*_API env vars. Consumed by mentatd-serve. The daemon only
        /// stores and republishes them, and the serde default keeps an old
        /// agent and a new daemon (or the reverse) interoperating.
        #[serde(default)]
        services: BTreeMap<String, String>,
        /// Services announced as a port and path rather than a whole URL,
        /// meaning "resolve the host against my node's addresses". The
        /// router does that resolving, because only it knows which of the
        /// node's links it shares. Old daemons ignore the field, and an
        /// agent that only ever announces URLs never sends it.
        #[serde(default)]
        services_ports: BTreeMap<String, ServicePort>,
        /// What serves the announced `openai` endpoint, from
        /// MENTAT_MODEL_PROVIDER -- `vllm` on every current image. The
        /// consumer needs it to know which interface an endpoint speaks,
        /// since two engines answering /v1/chat/completions can still differ
        /// in what else they expose. Empty when the container did not say,
        /// and always empty from an agent that predates the field.
        #[serde(default)]
        provider: String,
        /// What the agent found out about a service after announcing it,
        /// keyed by service name -- today, that the server bound one address
        /// rather than every address. Advisory: it explains a probe failure,
        /// it never causes one. Also carried on a re-register so a daemon
        /// restart does not lose the finding.
        #[serde(default)]
        service_notes: BTreeMap<String, String>,
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
        node_id: String,
    },
    Spawn {
        actor_id: String,
        name: String,
        env: BTreeMap<String, String>,
        gpu_ids: Vec<u32>,
        node_id: String,
        gcs_address: String,
        // payload: pickle (cls, args, kwargs)
    },
    SpawnResult {
        actor_id: String,
        ok: bool,
        #[serde(default)]
        error: String,
        /// The actor process pid, which only the agent knows. 0 when the
        /// failure happened before the fork.
        #[serde(default)]
        pid: u32,
    },
    CallActor {
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
    Kill {
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
        node_id: String,
        node_ip: String,
        control_addr: String,
        http_port: u16,
        /// Every address the dialing daemon answers on, most preferred
        /// first. node_ip is only what it calls itself, so a third party may
        /// not route there. Defaulted for old daemons.
        #[serde(default)]
        addrs: Vec<String>,
        /// Operator tags per address, for consumers that route classes of
        /// traffic over different links. Carried, never interpreted here.
        #[serde(default)]
        addr_tags: BTreeMap<String, Vec<String>>,
        /// The interface each address sits on, where it was discovered from
        /// one. Absent from a daemon that predates the field, and from an
        /// address named by MENTAT_ANNOUNCE_ADDRS.
        #[serde(default)]
        addr_ifaces: BTreeMap<String, String>,
        /// True when this daemon answers `probe` on its control port. A
        /// daemon that predates probing defaults to false and is never
        /// probed, so it never logs a peer_unexpected_msg per pair.
        #[serde(default)]
        probes: bool,
    },
    PeerHelloOk {
        node_id: String,
        node_ip: String,
        /// The accepting daemon's own addresses, so the dialing side records
        /// full membership too -- without these an outbound link stored
        /// http_port 0 and mesh-following consumers (mentatd-serve) could not
        /// reach that daemon's HTTP side. Defaulted for old daemons.
        #[serde(default)]
        control_addr: String,
        #[serde(default)]
        http_port: u16,
        #[serde(default)]
        addrs: Vec<String>,
        #[serde(default)]
        addr_tags: BTreeMap<String, Vec<String>>,
        /// The interface each address sits on, where it was discovered from
        /// one. Absent from a daemon that predates the field, and from an
        /// address named by MENTAT_ANNOUNCE_ADDRS.
        #[serde(default)]
        addr_ifaces: BTreeMap<String, String>,
        #[serde(default)]
        probes: bool,
    },
    /// Reachability probe, sent as the FIRST frame of its own short-lived
    /// connection rather than over the mesh link. The point is the socket
    /// underneath it: the prober binds one of its own addresses before
    /// connecting, so an answer proves that one address pair carries
    /// traffic. Nothing about the mesh link would prove that.
    Probe {
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
        node_id: String,
    },
    /// Periodic push of a daemon's own snapshot, so every daemon can serve a
    /// merged cluster view without request forwarding.
    PeerStatus {
        data: Value,
    },
    /// A locally-originated event, replicated so any daemon's /events stream
    /// carries the whole cluster. Never re-forwarded.
    PeerEvent {
        origin: String,
        line: String,
    },

    // ---- actor host (python) <-> agent, over the per-actor unix socket ----
    HostHello {
        actor_id: String,
    },
    Ctor, // payload: pickle (cls, args, kwargs)
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

pub fn default_gpu_vendor() -> String {
    "nvidia".to_string()
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
            format!("frame too large: header={hlen} payload={plen}"),
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

/// A service announced as a port and a path, with the host left open.
///
/// The announcing container knows which port it serves on. It does not know
/// which of its node's addresses the consumer can reach, and hard-coding one
/// is what makes an endpoint unroutable from off that link. `path` is empty
/// or starts with `/`, and the consumer forms `http://<host>:<port><path>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicePort {
    pub port: u16,
    #[serde(default)]
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeActor {
    pub actor_id: String,
    pub name: String,
    pub gpu_ids: Vec<u32>,
    pub pid: u32,
    /// Ref ids of calls the agent has relayed but not yet answered
    /// (including the long-lived run() ref).
    pub pending_refs: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        let f = Frame {
            req: 7,
            msg: Msg::Call {
                actor_id: "a1".into(),
                method: "run".into(),
            },
        };
        write_frame(&mut buf, &f, b"PAYLOAD").unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let (g, p) = read_frame(&mut cur).unwrap().unwrap();
        assert_eq!(g.req, 7);
        assert_eq!(p, b"PAYLOAD");
        match g.msg {
            Msg::Call { actor_id, method } => {
                assert_eq!(actor_id, "a1");
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
}
