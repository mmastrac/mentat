//! Zero-config discovery: a daemon announces its addresses over UDP, and
//! both the router and the other daemons listen. A cluster on one broadcast
//! domain therefore needs neither a daemon list nor a seed list.
//!
//! Every datagram is HMAC-SHA256 signed and holds a timestamp and a per-boot
//! sequence number, matching spark-agent's mesh discovery so one key serves
//! both. A daemon with no key does not announce at all.
//!
//! Signing raises the floor rather than closing the hole. The control port
//! still accepts unauthenticated connections from the same network, so a
//! listener keeps treating an announcement as a hint -- an address to watch
//! -- and verifies everything it claims over TCP.
//!
//! The two listeners want different things from one. The router adds a watch
//! and reads /status. A daemon dials, and `peer_hello` settles the rest, so a
//! seeded mesh and a discovered one converge on the same links.

use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::time::Duration;

use getifaddrs::InterfaceFlags;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::state::SharedRef;
use mentat_common::logfmt::log;
use mentat_common::secret;

pub const DEFAULT_PORT: u16 = 6382;

pub fn start(shared: SharedRef, control_port: u16, http_port: u16) {
    let port: u16 = std::env::var("MENTAT_ANNOUNCE_PORT")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);
    if port == 0 {
        log(
            "announce_off",
            &[("why", "MENTAT_ANNOUNCE_PORT=0".to_string())],
        );
        return;
    }
    let interval = std::env::var("MENTAT_ANNOUNCE_INTERVAL_S")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or(Duration::from_secs(5));
    // Explicit unicast targets, for tests and for a listener sharing no
    // broadcast domain with this box. "host" or "host:port".
    let extra: Vec<String> = std::env::var("MENTAT_ANNOUNCE_ADDR")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.contains(':') {
                s.to_string()
            } else {
                format!("{s}:{port}")
            }
        })
        .collect();
    // A key that was requested and cannot be had stops the daemon here. It
    // would otherwise sign nothing and refuse every signed announcement its
    // peers send, which from outside is a node that never joined the mesh.
    let key = match secret::load() {
        Ok(Some(k)) => k,
        Ok(None) => {
            // Every announcement is signed, so a daemon with no key has
            // nothing to send. Reporting it at boot beats a router that
            // watches an empty cluster with the reason unstated.
            log(
                "announce_off",
                &[("why", "no MENTAT_SECRET or MENTAT_SECRET_FILE".to_string())],
            );
            return;
        }
        Err(why) => {
            log("announce_secret_unusable", &[("error", why.clone())]);
            eprintln!("mentatd: {why}");
            std::process::exit(1);
        }
    };
    {
        let (shared, key) = (shared.clone(), key.clone());
        std::thread::spawn(move || listen(shared, port, key, control_port, http_port));
    }
    std::thread::spawn(move || run(shared, port, interval, extra, key));
}

/// One Ethernet frame on a 1500-MTU path, so a datagram never fragments.
const MAX_DATAGRAM: usize = 1400;

fn run(shared: SharedRef, port: u16, interval: Duration, extra: Vec<String>, key: Vec<u8>) {
    let sock = match UdpSocket::bind(("0.0.0.0", 0)) {
        Ok(s) => s,
        Err(e) => {
            log("announce_bind_failed", &[("error", e.to_string())]);
            return;
        }
    };
    let _ = sock.set_broadcast(true);
    log(
        "announce_up",
        &[
            ("port", port.to_string()),
            ("interval_s", format!("{:.1}", interval.as_secs_f64())),
            ("extra", extra.join(",")),
        ],
    );
    let boot_id = secret::boot_id();
    let universe = secret::universe();
    let seq = AtomicU64::new(0);
    loop {
        let payload = {
            let st = shared.st.lock().unwrap();
            // `t` and `seq` bound replay: the first against the listener's
            // clock, the second against the last it accepted from this boot.
            // Integer seconds, since an f64 does not survive the JSON round
            // trip a verifier does. See secret::canonical.
            let v = serde_json::json!({
                "proto": crate::proto::PROTO,
                "node_id": st.node_id,
                "control": st.control_addr,
                "http": format!("{}:{}", st.node_ip, st.http_port),
                "universe": universe,
                "addrs": local_addrs(),
                "addr_tags": local_addr_tags(),
                "boot_id": boot_id.clone(),
                "seq": seq.fetch_add(1, Ordering::Relaxed),
                "t": secret::now_s() as u64,
            });
            secret::sign(&v, &key)
        };
        if payload.len() > MAX_DATAGRAM {
            // The sender is the only side that can do anything about it: a
            // listener can only drop what it cannot read.
            log(
                "announce_too_large",
                &[
                    ("bytes", payload.len().to_string()),
                    ("cap", MAX_DATAGRAM.to_string()),
                ],
            );
            std::thread::sleep(interval);
            continue;
        }
        // Re-read the broadcast targets each round: interfaces come and go
        // (the QSFP link on the pair drops with the cable).
        for target in broadcast_targets(port).iter().chain(extra.iter()) {
            let _ = sock.send_to(payload.as_bytes(), target.as_str());
        }
        std::thread::sleep(interval);
    }
}

/// Join the mesh from what other daemons announce.
///
/// A daemon reaches the whole mesh through one `MENTAT_PEERS` entry, since
/// every peer publishes its own table. This covers the case before that: a
/// box with an empty seed list joins by being on the same broadcast domain.
///
/// An announcement is a hint, as it is for the router. It produces one dial,
/// and `peer_hello` then settles identity, version and which side owns the
/// link. Only a holder of the key can put a datagram in front of this, and a
/// foreign `universe` is dropped before the key is consulted.
fn listen(shared: SharedRef, port: u16, key: Vec<u8>, control_port: u16, http_port: u16) {
    let sock = match mentat_common::udp::bind_shared(port) {
        Ok(s) => s,
        Err(e) => {
            log(
                "announce_listen_failed",
                &[("port", port.to_string()), ("error", e.to_string())],
            );
            return;
        }
    };
    log("announce_listen", &[("port", port.to_string())]);
    let universe = secret::universe();
    let my_id = shared.st.lock().unwrap().node_id.clone();
    // node_id -> (boot_id, last accepted seq), which bounds replay within
    // one boot.
    let mut seen: std::collections::HashMap<String, (String, u64)> =
        std::collections::HashMap::new();
    let mut warned: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut buf = [0u8; 1400];
    loop {
        let Ok((n, src)) = sock.recv_from(&mut buf) else {
            continue;
        };
        // Another cluster on this broadcast domain is not a misconfiguration,
        // so it is dropped before the key is consulted and before anything
        // is logged.
        match secret::peek_universe(&buf[..n]) {
            Some(u) if u != universe => continue,
            _ => {}
        }
        let Some(v) = secret::verify(&buf[..n], &key) else {
            if warned.insert(src.ip().to_string()) {
                log(
                    "announce_rejected",
                    &[
                        ("src", src.ip().to_string()),
                        ("why", "bad signature or unsigned".to_string()),
                    ],
                );
            }
            continue;
        };
        let offered = v["proto"].as_str().unwrap_or_default();
        if !crate::proto::major_matches(offered) {
            if warned.insert(format!("proto:{}", src.ip())) {
                log(
                    "announce_proto_mismatch",
                    &[
                        ("src", src.ip().to_string()),
                        ("offered", offered.to_string()),
                        ("here", crate::proto::PROTO.to_string()),
                    ],
                );
            }
            continue;
        }
        let Some(t) = v["t"].as_u64() else { continue };
        if !secret::fresh(t as f64, secret::now_s()) {
            continue;
        }
        let node = v["node_id"].as_str().unwrap_or_default().to_string();
        let boot = v["boot_id"].as_str().unwrap_or_default().to_string();
        let seq = v["seq"].as_u64().unwrap_or(0);
        // Every daemon hears its own broadcasts.
        if node.is_empty() || boot.is_empty() || node == my_id {
            continue;
        }
        match seen.get(&node) {
            Some((b, last)) if *b == boot && seq <= *last => continue,
            _ => seen.insert(node.clone(), (boot, seq)),
        };
        let Some(control) = v["control"].as_str().filter(|c| !c.is_empty()) else {
            continue;
        };
        // A live link already covers this node. Dialing again would race the
        // one that exists, and register_peer would close one of the two.
        if shared
            .st
            .lock()
            .unwrap()
            .peers
            .get(&node)
            .is_some_and(|p| p.alive)
        {
            continue;
        }
        crate::mesh::dial(&shared, control.to_string(), control_port, http_port, true);
    }
}

/// One selected interface: the address it holds and the tags it was given.
pub struct Iface {
    pub iface: getifaddrs::Interface,
    pub tags: Vec<String>,
}

/// One MENTAT_ANNOUNCE_IFACES entry: an interface-name pattern and the tags
/// every address it matches holds.
struct Spec {
    pat: String,
    tags: Vec<String>,
}

/// Parse the "name" or "name=tag+tag" comma-separated list, order preserved.
fn parse_spec(s: &str) -> Vec<Spec> {
    s.split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| {
            let (name, tags) = e.split_once('=').unwrap_or((e, ""));
            Spec {
                pat: name.trim().to_string(),
                tags: tags
                    .split('+')
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(str::to_string)
                    .collect(),
            }
        })
        .collect()
}

/// fnmatch without the character classes: `*` matches any run of characters,
/// `?` matches exactly one, everything else is literal. A pattern with no
/// wildcard is an exact name, which is what every configuration written
/// before patterns existed relies on.
///
/// Deliberately not NCCL's implicit prefix match, where `en` would also
/// select `enp1s0f0np0` and `eno1`. A prefix here must report it: `en*`.
fn glob_match(pat: &str, name: &str) -> bool {
    let (p, n): (Vec<char>, Vec<char>) = (pat.chars().collect(), name.chars().collect());
    let (mut pi, mut ni) = (0usize, 0usize);
    // Where to resume if the current `*` turns out to have consumed too
    // little: the star's own index, and the input position it last tried.
    let (mut star, mut resume) = (None, 0usize);
    while ni < n.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            resume = ni;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            resume += 1;
            ni = resume;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '*')
}

/// Rank and tags for one interface name, or None when no entry matches it.
///
/// First match wins for all three replies. Several interfaces matching one
/// entry share that entry's rank and sort among themselves in kernel order.
fn match_spec(spec: &[Spec], name: &str) -> Option<(usize, Vec<String>)> {
    spec.iter()
        .position(|s| glob_match(&s.pat, name))
        .map(|i| (i, spec[i].tags.clone()))
}

/// Interfaces worth announcing on, most preferred first.
///
/// MENTAT_ANNOUNCE_IFACES names them explicitly and its order is the
/// preference order: put the fast link first and consumers that can reach
/// both will use it. Only this node can rank its own links -- a consumer
/// sees two addresses that both work and cannot tell which is the fast path.
///
/// An entry names an interface, or a `*`/`?` pattern over interface names,
/// so one line can serve every box in a fleet whose interface names differ.
/// The first entry a name matches decides its rank and its tags. Interfaces
/// matching the same entry rank together at that entry's position, in kernel
/// order. There is no negation syntax, because an explicit list is already
/// an allowlist.
///
/// An entry may hold tags, which travel with the address:
///
///     MENTAT_ANNOUNCE_IFACES=en*f*np*=connectx+rdma,en*=lan
///
/// A tag is a claim about cabling, and `rdma` is the one the daemon acts
/// on. mesh::prober is what decides whether the claim holds.
///
/// Unset, every up non-loopback IPv4 interface except the container bridges,
/// where no peer sits and every node would announce to itself, in whatever
/// order the kernel lists them and with no tags.
fn selected_ifaces() -> Vec<Iface> {
    let spec = parse_spec(&std::env::var("MENTAT_ANNOUNCE_IFACES").unwrap_or_default());
    let Ok(ifaces) = getifaddrs::InterfaceFilter::new().v4().get() else {
        return Vec::new();
    };
    let mut out: Vec<(usize, Iface)> = ifaces
        .filter(|i| {
            i.flags.contains(InterfaceFlags::UP) && !i.flags.contains(InterfaceFlags::LOOPBACK)
        })
        .filter_map(|i| {
            if spec.is_empty() {
                let bridge = ["docker", "veth", "br-", "virbr"]
                    .iter()
                    .any(|p| i.name.starts_with(p));
                return (!bridge).then_some((
                    0,
                    Iface {
                        iface: i,
                        tags: Vec::new(),
                    },
                ));
            }
            let (rank, tags) = match_spec(&spec, &i.name)?;
            Some((rank, Iface { iface: i, tags }))
        })
        .collect();
    // Rank by position in the operator's list rather than the kernel's. A
    // stable sort keeps kernel order within one entry.
    out.sort_by_key(|(rank, _)| *rank);
    out.into_iter().map(|(_, i)| i).collect()
}

/// Addresses this node advertises, when the operator names them outright
/// instead of naming interfaces.
///
/// MENTAT_ANNOUNCE_ADDRS uses the same `value=tag+tag` syntax and the same
/// order-is-preference rule as MENTAT_ANNOUNCE_IFACES, with addresses in
/// place of interface names:
///
///     MENTAT_ANNOUNCE_ADDRS=192.168.1.11=lan,10.100.0.1=connectx+rdma
///
/// It exists for the node whose advertisable address is not on any of its
/// own interfaces -- and for the tests, which build topologies a single box
/// is not cabled for. It replaces what this node reports it listens on, and
/// nothing else: broadcast still goes out on the selected interfaces, so a
/// node using this and no MENTAT_ANNOUNCE_ADDR still announces where it
/// always did.
fn announced_override() -> Option<Vec<(String, Vec<String>)>> {
    let raw = crate::testnet::load()
        .and_then(|n| n.announce())
        .unwrap_or_else(|| std::env::var("MENTAT_ANNOUNCE_ADDRS").unwrap_or_default());
    let spec = parse_spec(&raw);
    if spec.is_empty() {
        return None;
    }
    // Checked here rather than at first use. A typo would otherwise be
    // announced to the whole mesh and surface much later as one failing
    // probe per peer, giving the address but not where it came from.
    let (ok, bad): (Vec<_>, Vec<_>) = spec
        .into_iter()
        .partition(|s| s.pat.parse::<std::net::Ipv4Addr>().is_ok());
    for b in &bad {
        log(
            "bad_announce_addr",
            &[
                ("var", "MENTAT_ANNOUNCE_ADDRS".to_string()),
                ("value", b.pat.clone()),
                ("why", "not an IPv4 address".to_string()),
            ],
        );
    }
    (!ok.is_empty()).then(|| ok.into_iter().map(|s| (s.pat, s.tags)).collect())
}

/// One broadcast address per selected interface.
fn broadcast_targets(port: u16) -> Vec<String> {
    selected_ifaces()
        .into_iter()
        .map(|i| i.iface)
        .filter(|i| i.flags.contains(InterfaceFlags::BROADCAST))
        .filter_map(|i| i.address.associated_address())
        .map(|b| format!("{b}:{port}"))
        .collect()
}

/// Every address this node listens on, for consumers that cannot reach the
/// one it calls itself. A listener should still prefer the address a packet
/// actually arrived from. This list is what to fall back to.
/// Every IPv4 address this box listens on, whatever the announce settings
/// select. `local_addrs` is the announced subset and reports a different
/// question: this one is "is that host me".
pub fn all_local_addrs() -> Vec<String> {
    let Ok(ifaces) = getifaddrs::InterfaceFilter::new().v4().get() else {
        return Vec::new();
    };
    ifaces
        .filter(|i| i.flags.contains(InterfaceFlags::UP))
        .filter_map(|i| i.address.ip_addr().map(|a| a.to_string()))
        .collect()
}

pub fn local_addrs() -> Vec<String> {
    if let Some(o) = announced_override() {
        return o.into_iter().map(|(a, _)| a).collect();
    }
    selected_ifaces()
        .into_iter()
        .filter_map(|i| i.iface.address.ip_addr())
        .map(|a| a.to_string())
        .collect()
}

/// The interface each address sits on, for the addresses discovered from
/// one. A placement caller needs which link reaches a node, and the answer is
/// an address and the interface to bind it on.
///
/// MENTAT_ANNOUNCE_ADDRS names addresses without an interface, so those are
/// left out. An address missing from this map has an interface nobody
/// recorded.
pub fn local_addr_ifaces() -> BTreeMap<String, String> {
    if announced_override().is_some() {
        return BTreeMap::new();
    }
    selected_ifaces()
        .into_iter()
        .filter_map(|i| {
            let name = i.iface.name.clone();
            i.iface.address.ip_addr().map(|a| (a.to_string(), name))
        })
        .collect()
}

/// Tags per address, for the addresses that were given any. Empty unless
/// MENTAT_ANNOUNCE_IFACES names tags, so every tag a datagram holds is
/// one an operator set.
pub fn local_addr_tags() -> BTreeMap<String, Vec<String>> {
    if let Some(o) = announced_override() {
        return o.into_iter().filter(|(_, t)| !t.is_empty()).collect();
    }
    selected_ifaces()
        .into_iter()
        .filter(|i| !i.tags.is_empty())
        .filter_map(|i| i.iface.address.ip_addr().map(|a| (a.to_string(), i.tags)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Names as the fleet reports them: two ConnectX ports and a LAN port on
    /// the pair, a differently-named LAN port on the third box.
    fn names() -> Vec<&'static str> {
        vec!["enp1s0f0np0", "enp1s0f1np1", "eno1", "enP2p1s0", "docker0"]
    }

    fn ranked(spec: &str, names: &[&str]) -> Vec<(String, Vec<String>)> {
        let spec = parse_spec(spec);
        let mut out: Vec<(usize, String, Vec<String>)> = names
            .iter()
            .filter_map(|n| match_spec(&spec, n).map(|(rank, tags)| (rank, n.to_string(), tags)))
            .collect();
        out.sort_by_key(|(rank, _, _)| *rank);
        out.into_iter().map(|(_, n, t)| (n, t)).collect()
    }

    /// The trap this guards: NCCL treats `en` as a prefix and would select
    /// every interface here. An entry with no wildcard must stay an exact
    /// name, or every configuration written before patterns existed changes
    /// meaning on upgrade.
    #[test]
    fn a_plain_name_is_not_a_prefix() {
        assert!(glob_match("eno1", "eno1"));
        assert!(!glob_match("en", "eno1"));
        assert_eq!(
            ranked("eno1", &names()),
            vec![("eno1".to_string(), vec![])],
            "only the named interface is selected"
        );
    }

    #[test]
    fn wildcards_match_fnmatch_style() {
        assert!(glob_match("en*", "eno1"));
        assert!(glob_match("en*f*np*", "enp1s0f0np0"));
        assert!(!glob_match("en*f*np*", "eno1"));
        assert!(glob_match("eno?", "eno1"));
        assert!(!glob_match("eno?", "eno10"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("**", "anything"));
        // A trailing star may match nothing at all.
        assert!(glob_match("eno1*", "eno1"));
        // Backtracking: the first star must give characters back so the
        // literal tail can land.
        assert!(glob_match("*np0", "enp1s0f0np0"));
        assert!(!glob_match("*np2", "enp1s0f0np0"));
    }

    /// One line for the whole fleet: the fabric ports rank above the LAN
    /// ports whatever the box calls them, and the docker bridge is left out
    /// by not being named.
    #[test]
    fn one_spec_ranks_and_tags_a_mixed_fleet() {
        assert_eq!(
            ranked("en*f*np*=connectx+rdma,en*=lan", &names()),
            vec![
                (
                    "enp1s0f0np0".to_string(),
                    vec!["connectx".to_string(), "rdma".to_string()]
                ),
                (
                    "enp1s0f1np1".to_string(),
                    vec!["connectx".to_string(), "rdma".to_string()]
                ),
                ("eno1".to_string(), vec!["lan".to_string()]),
                ("enP2p1s0".to_string(), vec!["lan".to_string()]),
            ],
        );
    }

    /// First match wins, so a later entry cannot re-tag or re-rank a name an
    /// earlier one already claimed. Order in the variable is the whole rule.
    #[test]
    fn the_first_matching_entry_decides() {
        assert_eq!(
            ranked("en*=lan,en*f*np*=connectx+rdma", &names()),
            vec![
                ("enp1s0f0np0".to_string(), vec!["lan".to_string()]),
                ("enp1s0f1np1".to_string(), vec!["lan".to_string()]),
                ("eno1".to_string(), vec!["lan".to_string()]),
                ("enP2p1s0".to_string(), vec!["lan".to_string()]),
            ],
            "the broad entry came first, so it swallowed the fabric ports",
        );
    }

    #[test]
    fn an_unmatched_name_is_not_selected() {
        assert!(match_spec(&parse_spec("eno1,en*np*"), "docker0").is_none());
        assert!(match_spec(&parse_spec(""), "eno1").is_none());
    }
}
