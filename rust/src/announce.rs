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

use std::net::UdpSocket;
use std::time::Duration;

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
            });
            if key.is_some() {
                // Version 2 carries what bounds replay: t against the
                // listener's clock, seq against the last one it accepted
                // from this boot.
                v["mentat_announce"] = secret::SIGNED_VERSION.into();
                v["boot_id"] = boot_id.clone().into();
                v["seq"] = seq.fetch_add(1, Ordering::Relaxed).into();
                v["t"] = secret::now_s().into();
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

/// One broadcast address per up IPv4 interface worth announcing on, from
/// `ip -o -4 addr show`. Docker's own bridges carry no listeners, and lo has
/// no broadcast, so both are skipped -- same reasoning as spark-agent's
/// discovery. Empty where `ip` is absent (macOS dev boxes), which leaves
/// only the explicit MENTAT_ANNOUNCE_ADDR targets.
fn broadcast_targets(port: u16) -> Vec<String> {
    let Ok(out) = std::process::Command::new("ip")
        .args(["-o", "-4", "addr", "show"])
        .output()
    else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut targets = Vec::new();
    for line in text.lines() {
        // "2: eno1    inet 192.168.1.11/24 brd 192.168.1.255 scope ..."
        let f: Vec<&str> = line.split_whitespace().collect();
        let Some(ifname) = f.get(1) else { continue };
        if ["lo", "docker", "veth", "br-", "virbr"]
            .iter()
            .any(|p| ifname.starts_with(p))
        {
            continue;
        }
        if let Some(i) = f.iter().position(|w| *w == "brd") {
            if let Some(b) = f.get(i + 1) {
                targets.push(format!("{b}:{port}"));
            }
        }
    }
    targets
}
