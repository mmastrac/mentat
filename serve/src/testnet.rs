//! The router's half of the pretend network the daemon tests run on.
//!
//! MENTAT_TEST_NET names the same file the daemons read (see
//! `rust/src/testnet.rs`). The router only needs its `addrs` map: a daemon
//! address in the mesh is a pretend one, and the real daemon listens on a
//! loopback port. The host is swapped and the port kept, since every
//! daemon in a test binds every address on its own ports.
//!
//! Unset, addresses are dialed as written.

use std::collections::BTreeMap;

/// `host:port` with the host swapped for its real one, when the file maps
/// it. A host the file lists as `down` is sent to a port nothing listens
/// on, so the dial is refused the way a dead box refuses it.
pub fn mapped(addr: &str) -> String {
    let Some((map, down)) = load() else {
        return addr.to_string();
    };
    let Some((host, port)) = addr.rsplit_once(':') else {
        return addr.to_string();
    };
    if down.iter().any(|d| d == host) {
        return "127.0.0.1:1".to_string();
    }
    match map.get(host) {
        Some(real) => {
            let ip = real.rsplit_once(':').map(|(h, _)| h).unwrap_or(real);
            format!("{ip}:{port}")
        }
        None => addr.to_string(),
    }
}

fn load() -> Option<(BTreeMap<String, String>, Vec<String>)> {
    let path = std::env::var("MENTAT_TEST_NET").ok()?;
    if path.trim().is_empty() {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    let addrs = v["addrs"]
        .as_object()?
        .iter()
        .filter_map(|(k, r)| r.as_str().map(|r| (k.clone(), r.to_string())))
        .collect();
    let down = v["down"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| a.as_str())
        .map(str::to_string)
        .collect();
    Some((addrs, down))
}
