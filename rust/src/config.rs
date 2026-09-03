//! Lifecycle tuning knobs, read once from MENTAT_* environment variables at
//! process start (daemon and agent alike -- matching how the fleet configures
//! everything else). Everything named `*_MS` is a duration in milliseconds.
//! An unset or empty variable means the default, and an unparsable value logs
//! `bad_env_ms` and falls back to the default rather than refusing to start.
//!
//! The defaults are sized for the serving pair: model boot legitimately takes
//! minutes (weights, container pulls), while an agent link blip should heal in
//! seconds. GUIDE.md carries the same table for operators.

use std::sync::OnceLock;

use crate::logfmt::log;

/// The resolved knob values, one field per MENTAT_* variable.
pub struct Cfg {
    /// MENTAT_PG_PENDING_TIMEOUT_MS, default 600_000 (10 min). How long a
    /// placement group may sit PENDING waiting for agents/GPUs before the
    /// daemon fails it loudly (its ready ref raises in the driver). Sized for
    /// the slowest legitimate rendezvous: a cold worker box pulling images
    /// and mounting weights. vLLM's own wait loop tolerates this.
    pub pg_pending_timeout_ms: u64,
    /// MENTAT_AGENT_DEGRADED_AFTER_MS, default 30_000. An agent whose daemon
    /// link EOFs is held in a grace window instead of its actors being marked
    /// dead; after this long still disconnected, the agent is marked degraded
    /// (event + log, calls keep being held).
    pub agent_degraded_after_ms: u64,
    /// MENTAT_AGENT_DEAD_AFTER_MS, default 60_000. The give-up threshold: an
    /// agent disconnected this long has its actors marked dead, which
    /// completes their run() sentinel refs so the driver restarts and the
    /// system returns to the initial state.
    pub agent_dead_after_ms: u64,
    /// MENTAT_ACTOR_KEEP_MS, default 3_600_000. How long a dead actor's row
    /// stays in the table once its owner is gone. The row is what turns a
    /// call on a dead actor into RayActorError with the reason it died, so
    /// it outlives the actor on purpose. Once the owner has gone nobody can
    /// make that call, and an hour is longer than an operator looks.
    pub actor_keep_ms: u64,
    /// MENTAT_PEER_STALE_AFTER_MS, default 30_000. A mesh peer that has not
    /// been heard from (status push or pong) for this long is logged stale --
    /// the mesh analog of the agent degrade window.
    pub peer_stale_after_ms: u64,
    /// MENTAT_PEER_DEAD_AFTER_MS, default 60_000. A silent mesh peer is
    /// declared gone (node_leave, link closed) after this long; the connector
    /// keeps re-dialing it.
    pub peer_dead_after_ms: u64,
    /// MENTAT_ELECTION_HOLD_DOWN_MS, default 5_000. How long a head candidate
    /// must be stable before the designation changes, so a flapping link
    /// cannot thrash head_change events.
    pub election_hold_down_ms: u64,
    /// MENTAT_PROBE_INTERVAL_MS, default 15_000. How often each daemon
    /// re-probes reachability to every live peer, one probe per (own
    /// address x peer address) pair. Slow on purpose: the table answers
    /// "is this cable up", which changes on the timescale of cables.
    pub probe_interval_ms: u64,
    /// MENTAT_PROBE_TIMEOUT_MS, default 2_000. How long one probe waits for
    /// the connect and the reply before the pair is recorded failed. A pair
    /// with no route usually fails immediately. This bounds the case where
    /// the SYN is dropped instead, which the kernel would otherwise retry
    /// for minutes.
    pub probe_timeout_ms: u64,
    /// MENTAT_ISLAND_PLACEMENT, default on. Set to `off` or `0` to place
    /// multi-bundle groups without the one-fabric constraint. The escape
    /// hatch for a cluster whose probes disagree with its cabling under
    /// pressure: it takes one variable and a daemon restart, where the
    /// alternative is untagging every node.
    pub island_placement: bool,
    /// MENTAT_ISLAND_HOLD_DOWN_MS, default 5_000. How long the fabric island
    /// membership must hold still before placement acts on the change --
    /// the election hold-down's argument applied to cables, so a flapping
    /// QSFP link cannot send consecutive placements to different islands.
    pub island_hold_down_ms: u64,
    /// MENTAT_HOST_CONNECT_TIMEOUT_MS, default 60_000. How long the agent
    /// waits for a freshly spawned actor host to connect to its unix socket.
    /// The host connects before importing anything heavy, so this covers
    /// process start only, not vLLM import.
    pub host_connect_timeout_ms: u64,
    /// MENTAT_SLOW_CALL_WARN_MS, default 15_000. A non-run() call pending
    /// longer than this gets one call_pending_long warning (queued behind a
    /// blocking method, or a stuck worker).
    pub slow_call_warn_ms: u64,
    /// MENTAT_AGENT_PING_INTERVAL_MS, default 2_000. How often the agent
    /// pings the daemon so a dead daemon is noticed within seconds.
    pub agent_ping_interval_ms: u64,
    /// MENTAT_PEER_STATUS_INTERVAL_MS, default 2_000. How often each daemon
    /// pushes its snapshot to mesh peers; doubles as the peer heartbeat that
    /// feeds the staleness windows above, so keep it several times smaller
    /// than MENTAT_PEER_STALE_AFTER_MS.
    pub peer_status_interval_ms: u64,
    /// MENTAT_TCP_DEAD_AFTER_MS, default 75_000. Target time for TCP
    /// keepalive to declare a wedged (not closed) peer dead on Linux;
    /// translated into TCP_KEEPIDLE/KEEPINTVL/KEEPCNT in set_keepalive.
    /// (Only read on Linux -- macOS dev builds leave the kernel defaults.)
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub tcp_dead_after_ms: u64,
    /// MENTAT_SESSION_REAP_GRACE_MS, default 0. Delay between a driver
    /// session EOF and the reap of its actors/placement groups. The default
    /// is 0 on purpose: a restarting vLLM is a new client that needs the old
    /// actors' names and GPUs freed, so a grace only delays recovery. The
    /// knob exists for debugging (keep workers up briefly to inspect after a
    /// driver crash); the dead client is removed immediately either way, so a
    /// new driver session is never blocked by the grace.
    pub session_reap_grace_ms: u64,
}

fn env_ms(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => match v.trim().parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                log(
                    "bad_env_ms",
                    &[
                        ("var", name.to_string()),
                        ("value", v),
                        ("default", default.to_string()),
                    ],
                );
                default
            }
        },
        _ => default,
    }
}

/// `off`, `no`, `false` and `0` turn a switch off, and `on`, `yes`, `true`
/// and `1` turn one on. Unset or empty leaves the default. An unrecognised
/// value logs `bad_env_flag` and keeps the default, matching env_ms.
fn env_on(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => match v.trim().to_ascii_lowercase().as_str() {
            "off" | "no" | "false" | "0" => false,
            "on" | "yes" | "true" | "1" => true,
            _ => {
                log(
                    "bad_env_flag",
                    &[
                        ("var", name.to_string()),
                        ("value", v),
                        ("default", default.to_string()),
                    ],
                );
                default
            }
        },
        _ => default,
    }
}

/// The process-wide config, read from the environment on first use.
pub fn cfg() -> &'static Cfg {
    static CFG: OnceLock<Cfg> = OnceLock::new();
    CFG.get_or_init(|| Cfg {
        pg_pending_timeout_ms: env_ms("MENTAT_PG_PENDING_TIMEOUT_MS", 600_000),
        agent_degraded_after_ms: env_ms("MENTAT_AGENT_DEGRADED_AFTER_MS", 30_000),
        agent_dead_after_ms: env_ms("MENTAT_AGENT_DEAD_AFTER_MS", 60_000),
        actor_keep_ms: env_ms("MENTAT_ACTOR_KEEP_MS", 3_600_000),
        peer_stale_after_ms: env_ms("MENTAT_PEER_STALE_AFTER_MS", 30_000),
        peer_dead_after_ms: env_ms("MENTAT_PEER_DEAD_AFTER_MS", 60_000),
        election_hold_down_ms: env_ms("MENTAT_ELECTION_HOLD_DOWN_MS", 5_000),
        probe_interval_ms: env_ms("MENTAT_PROBE_INTERVAL_MS", 15_000),
        probe_timeout_ms: env_ms("MENTAT_PROBE_TIMEOUT_MS", 2_000),
        island_placement: env_on("MENTAT_ISLAND_PLACEMENT", true),
        island_hold_down_ms: env_ms("MENTAT_ISLAND_HOLD_DOWN_MS", 5_000),
        host_connect_timeout_ms: env_ms("MENTAT_HOST_CONNECT_TIMEOUT_MS", 60_000),
        slow_call_warn_ms: env_ms("MENTAT_SLOW_CALL_WARN_MS", 15_000),
        agent_ping_interval_ms: env_ms("MENTAT_AGENT_PING_INTERVAL_MS", 2_000),
        peer_status_interval_ms: env_ms("MENTAT_PEER_STATUS_INTERVAL_MS", 2_000),
        tcp_dead_after_ms: env_ms("MENTAT_TCP_DEAD_AFTER_MS", 75_000),
        session_reap_grace_ms: env_ms("MENTAT_SESSION_REAP_GRACE_MS", 0),
    })
}
