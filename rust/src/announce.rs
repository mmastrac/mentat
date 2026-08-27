//! Zero-config discovery: mentatd announces its addresses over UDP so
//! mentatd-serve needs no daemon list.
//!
//! With MENTAT_SECRET set, datagrams are HMAC-SHA256 signed and carry a
//! timestamp and per-boot sequence number, matching spark-agent's mesh
//! discovery so one key serves both. Without it they go out unsigned, as
//! version 1, and a listener holding a key refuses them on version alone.
//!
//! Signing raises the floor rather than closing the hole. The control port
//! still accepts unauthenticated connections from the same network, so a
//! listener keeps treating an announcement as a hint -- an address to watch
//! -- and verifies everything it claims over TCP.

use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::time::Duration;

use getifaddrs::InterfaceFlags;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::logfmt::log;
use crate::secret;
use crate::state::SharedRef;

pub const DEFAULT_PORT: u16 = 6382;

pub fn start(shared: SharedRef) {
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
    let key = secret::load();
    log(
        "announce_signing",
        &[(
            "state",
            match key {
                Some(_) => "on".to_string(),
                None => "off (no MENTAT_SECRET)".to_string(),
            },
        )],
    );
    std::thread::spawn(move || run(shared, port, interval, extra, key));
}

fn run(shared: SharedRef, port: u16, interval: Duration, extra: Vec<String>, key: Option<Vec<u8>>) {
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
            let mut v = serde_json::json!({
                "mentat_announce": 1,
                "node_id": st.node_id,
                "control": st.gcs_address,
                "http": format!("{}:{}", st.node_ip, st.http_port),
                "universe": universe,
                "addrs": local_addrs(),
                "addr_tags": local_addr_tags(),
            });
            if key.is_some() {
                // Version 2 carries what bounds replay: t against the
                // listener's clock, seq against the last one it accepted
                // from this boot.
                v["mentat_announce"] = secret::SIGNED_VERSION.into();
                v["boot_id"] = boot_id.clone().into();
                v["seq"] = seq.fetch_add(1, Ordering::Relaxed).into();
                // Integer seconds: f64 does not survive a JSON round trip,
                // so a float here breaks the signature. See secret::canonical.
                v["t"] = (secret::now_s() as u64).into();
            }
            match &key {
                Some(k) => secret::sign(&v, k),
                None => v.to_string(),
            }
        };
        // Re-read the broadcast targets each round: interfaces come and go
        // (the QSFP link on the pair drops with the cable).
        for target in broadcast_targets(port).iter().chain(extra.iter()) {
            let _ = sock.send_to(payload.as_bytes(), target.as_str());
        }
        std::thread::sleep(interval);
    }
}

/// One selected interface: the address it carries and the tags it was given.
pub struct Iface {
    pub iface: getifaddrs::Interface,
    pub tags: Vec<String>,
}

/// Interfaces worth announcing on, most preferred first.
///
/// MENTAT_ANNOUNCE_IFACES names them explicitly and its order is the
/// preference order: put the fast link first and consumers that can reach
/// both will take it. Only this node can rank its own links -- a consumer
/// sees two addresses that both work and cannot tell which is the fast path.
///
/// An entry may carry tags, which travel with the address and mean nothing
/// here. They exist so a consumer can eventually say which class of traffic
/// belongs on which link:
///
///     MENTAT_ANNOUNCE_IFACES=enp1s0f0np0=connectx+rdma,eno1=lan
///
/// Unset, every up non-loopback IPv4 interface except the container bridges,
/// which carry no peers and would have every node announcing to itself, in
/// whatever order the kernel lists them and with no tags.
fn selected_ifaces() -> Vec<Iface> {
    // "name" or "name=tag+tag", comma separated.
    let spec: Vec<(String, Vec<String>)> = std::env::var("MENTAT_ANNOUNCE_IFACES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|e| {
            let (name, tags) = e.split_once('=').unwrap_or((e, ""));
            let tags = tags
                .split('+')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string)
                .collect();
            (name.trim().to_string(), tags)
        })
        .collect();
    let Ok(ifaces) = getifaddrs::InterfaceFilter::new().v4().get() else {
        return Vec::new();
    };
    let mut out: Vec<Iface> = ifaces
        .filter(|i| {
            i.flags.contains(InterfaceFlags::UP) && !i.flags.contains(InterfaceFlags::LOOPBACK)
        })
        .filter(|i| {
            if spec.is_empty() {
                !["docker", "veth", "br-", "virbr"]
                    .iter()
                    .any(|p| i.name.starts_with(p))
            } else {
                spec.iter().any(|(n, _)| n == &i.name)
            }
        })
        .map(|i| {
            let tags = spec
                .iter()
                .find(|(n, _)| n == &i.name)
                .map(|(_, t)| t.clone())
                .unwrap_or_default();
            Iface { iface: i, tags }
        })
        .collect();
    if !spec.is_empty() {
        // Rank by position in the operator's list rather than the kernel's.
        out.sort_by_key(|i| {
            spec.iter()
                .position(|(n, _)| n == &i.iface.name)
                .unwrap_or(usize::MAX)
        });
    }
    out
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

/// Every address this node answers on, for consumers that cannot reach the
/// one it calls itself. A listener should still prefer the address a packet
/// actually arrived from; this list is what to fall back to.
pub fn local_addrs() -> Vec<String> {
    selected_ifaces()
        .into_iter()
        .filter_map(|i| i.iface.address.ip_addr())
        .map(|a| a.to_string())
        .collect()
}

/// Tags per address, for the addresses that were given any. Empty unless
/// MENTAT_ANNOUNCE_IFACES names tags, so a datagram carries no dead weight.
pub fn local_addr_tags() -> BTreeMap<String, Vec<String>> {
    selected_ifaces()
        .into_iter()
        .filter(|i| !i.tags.is_empty())
        .filter_map(|i| i.iface.address.ip_addr().map(|a| (a.to_string(), i.tags)))
        .collect()
}
