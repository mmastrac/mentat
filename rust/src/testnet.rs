//! A network that exists only in a file, for the tests.
//!
//! One box has one loopback address and no cables, and the questions this
//! daemon answers are about cables: which of a node's addresses reaches
//! which of another's, and what happens to the mesh, the islands and
//! placement when one stops. MENTAT_TEST_NET names a JSON file describing
//! a network to pretend:
//!
//! ```json
//! {"addrs": {"10.100.0.1": "127.0.0.1:41001", "192.168.1.70": "127.0.0.1:41001"},
//!  "cut": [["10.100.0.1", "10.100.0.2"]],
//!  "down": ["10.100.0.3"],
//!  "announce": {"192.168.1.70": "192.168.1.70=lan,10.100.0.1=connectx+rdma"}}
//! ```
//!
//! `addrs` maps every address in the pretend network to the real control
//! address of the daemon that owns it. A dial or probe to a pretend address
//! goes to the real one, without binding a source. `cut` lists address pairs
//! with no cable between them, in either order. `down` lists addresses that
//! answer nobody. `announce` maps a node ip to what that daemon announces
//! in place of MENTAT_ANNOUNCE_ADDRS, so a test can renumber a running
//! daemon.
//!
//! The file is re-read at every use, so a test edits it and the daemons
//! follow: a cut cable fails its probes on the next round and drops the
//! mesh link riding it, and a repaired one comes back the same way.
//!
//! Unset, none of this exists and every hook is a no-op.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde_json::Value;

static NODE_IP: OnceLock<String> = OnceLock::new();

/// Record this daemon's identity, for the per-node `announce` override.
pub fn set_node_ip(ip: &str) {
    let _ = NODE_IP.set(ip.to_string());
}

pub struct TestNet {
    addrs: BTreeMap<String, String>,
    cut: BTreeSet<(String, String)>,
    down: BTreeSet<String>,
    announce: BTreeMap<String, String>,
}

/// The pretend network, or None outside the tests.
pub fn load() -> Option<TestNet> {
    let path = std::env::var("MENTAT_TEST_NET").ok()?;
    if path.trim().is_empty() {
        return None;
    }
    let text = std::fs::read_to_string(&path).ok()?;
    Some(parse(&serde_json::from_str(&text).ok()?))
}

fn parse(v: &Value) -> TestNet {
    let strs = |v: &Value| -> Vec<String> {
        v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str())
            .map(str::to_string)
            .collect()
    };
    TestNet {
        addrs: v["addrs"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(k, r)| r.as_str().map(|r| (k.clone(), r.to_string())))
            .collect(),
        cut: v["cut"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| {
                let p = strs(p);
                (p.len() == 2).then(|| (p[0].clone(), p[1].clone()))
            })
            .collect(),
        down: strs(&v["down"]).into_iter().collect(),
        announce: v["announce"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(k, r)| r.as_str().map(|r| (k.clone(), r.to_string())))
            .collect(),
    }
}

impl TestNet {
    /// The real address behind a pretend one, or None for an address the
    /// file does not know, which is dialed as written.
    pub fn real(&self, host: &str) -> Option<String> {
        self.addrs.get(host).cloned()
    }

    /// Whether a cable runs between two addresses.
    pub fn reachable(&self, local: &str, remote: &str) -> bool {
        !self.down.contains(remote)
            && !self.down.contains(local)
            && !self.cut.contains(&(local.to_string(), remote.to_string()))
            && !self.cut.contains(&(remote.to_string(), local.to_string()))
    }

    /// The local address a kernel would put a link to `remote` on: the one
    /// sharing its /24, else the first. A mesh link is never source-bound,
    /// so this stands in for the routing table.
    pub fn link_local<'a>(&self, locals: &'a [String], remote: &str) -> Option<&'a String> {
        let net = |a: &str| a.rsplit_once('.').map(|(n, _)| n.to_string());
        locals
            .iter()
            .find(|l| net(l) == net(remote))
            .or_else(|| locals.first())
    }

    /// Whether a link from this box to `remote` can exist at all.
    pub fn link_up(&self, locals: &[String], remote: &str) -> bool {
        match self.link_local(locals, remote) {
            Some(l) => self.reachable(l, remote),
            None => !self.down.contains(remote),
        }
    }

    /// This node's announce override, if the file carries one.
    pub fn announce(&self) -> Option<String> {
        NODE_IP.get().and_then(|ip| self.announce.get(ip).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net(json: &str) -> TestNet {
        parse(&serde_json::from_str(json).unwrap())
    }

    #[test]
    fn a_cut_is_symmetric_and_down_covers_every_pair() {
        let n = net(r#"{"cut": [["10.0.0.1","10.0.0.2"]], "down": ["10.0.0.9"]}"#);
        assert!(!n.reachable("10.0.0.1", "10.0.0.2"));
        assert!(!n.reachable("10.0.0.2", "10.0.0.1"));
        assert!(n.reachable("10.0.0.1", "10.0.0.3"));
        assert!(!n.reachable("10.0.0.1", "10.0.0.9"));
        assert!(!n.reachable("10.0.0.9", "10.0.0.1"));
    }

    #[test]
    fn a_link_rides_the_local_on_the_remotes_subnet() {
        let n = net(r#"{"cut": [["10.100.0.1","10.100.0.2"]]}"#);
        let locals = vec!["192.168.1.70".to_string(), "10.100.0.1".to_string()];
        assert_eq!(n.link_local(&locals, "10.100.0.2").unwrap(), "10.100.0.1");
        assert!(!n.link_up(&locals, "10.100.0.2"), "the fabric cable is cut");
        assert!(n.link_up(&locals, "192.168.1.77"), "the LAN is not");
    }
}
