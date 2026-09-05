//! Matching a requested shape against the cluster's measured topology.
//!
//! Placement today answers one question: put these bundles inside one fabric
//! island. It cannot say "a cabled pair here and a cabled pair there, with
//! only IP between them", which is what pipeline parallel over two
//! tensor-parallel pairs needs. A request here names its sets, says which of them need a fabric,
//! and says what has to hold between them.
//!
//! The answer names nodes and, for every link it relied on, the address to
//! dial and the interface it sits on. A caller binding NCCL needs both and
//! only the node knows the second.
//!
//! Every collection walked here is sorted. Two daemons holding the same view
//! must return the same answer, or one name would mean two placements.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use crate::island::Island;
use crate::state::NodeId;

/// What a set needs of the links between its own members.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Link {
    /// An operator-tagged fabric address, probe-confirmed between every pair.
    Rdma,
    /// Anything that answered a probe.
    Any,
}

impl Link {
    pub fn parse(s: &str) -> Result<Link, String> {
        match s {
            "rdma" | "roce" | "fabric" => Ok(Link::Rdma),
            "ip" | "any" | "" => Ok(Link::Any),
            other => Err(format!("unknown link {other:?}, expected rdma or ip")),
        }
    }
}

/// One group of nodes the caller wants placed together.
#[derive(Clone, Debug)]
pub struct SetReq {
    pub name: String,
    /// GPUs per member, one entry per node wanted.
    pub bundles: Vec<f64>,
    pub link: Link,
}

/// A requirement between two sets.
#[derive(Clone, Debug)]
pub struct BetweenReq {
    pub from: String,
    pub to: String,
    pub link: Link,
}

#[derive(Clone, Debug)]
pub struct Request {
    pub sets: Vec<SetReq>,
    pub between: Vec<BetweenReq>,
}

/// One address a node answers on.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Port {
    pub addr: String,
    /// Absent where the address was configured rather than discovered.
    pub iface: Option<String>,
    pub tags: Vec<String>,
}

impl Port {
    fn rdma(&self) -> bool {
        self.tags.iter().any(|t| t == "rdma")
    }
}

/// What the matcher reads. Assembled from the daemon's merged view, and
/// built directly in tests.
#[derive(Clone, Debug, Default)]
pub struct Topology {
    /// Fabric islands, as the island module derived them.
    pub islands: Vec<Island>,
    /// Every address each node answers on.
    pub ports: BTreeMap<NodeId, Vec<Port>>,
    /// Address pairs that answered a probe, with the round trip observed.
    /// Undirected: a one-way link is not something to place on.
    pub links: BTreeMap<(String, String), u64>,
    /// GPUs free on each node right now.
    pub free_gpus: BTreeMap<NodeId, f64>,
    /// Hostname per node, carried through for the answer to be readable.
    pub hosts: BTreeMap<NodeId, String>,
}

impl Topology {
    fn rtt(&self, a: &str, b: &str) -> Option<u64> {
        self.links
            .get(&(a.to_string(), b.to_string()))
            .or_else(|| self.links.get(&(b.to_string(), a.to_string())))
            .copied()
    }

    /// The best link between two nodes meeting `link`, or None.
    ///
    /// Ranked by round trip, then by address, so a tie resolves the same way
    /// on every daemon. Ports are the node's own order otherwise.
    fn path(&self, a: &NodeId, b: &NodeId, link: Link) -> Option<Path> {
        let (pa, pb) = (self.ports.get(a)?, self.ports.get(b)?);
        let mut best: Option<Path> = None;
        for x in pa {
            for y in pb {
                if link == Link::Rdma && !(x.rdma() && y.rdma()) {
                    continue;
                }
                let Some(rtt) = self.rtt(&x.addr, &y.addr) else {
                    continue;
                };
                let cand = Path {
                    from: a.clone(),
                    to: b.clone(),
                    local: x.clone(),
                    remote: y.clone(),
                    rtt_ms: rtt,
                };
                let better = match &best {
                    None => true,
                    Some(b0) => {
                        (cand.rtt_ms, &cand.local.addr, &cand.remote.addr)
                            < (b0.rtt_ms, &b0.local.addr, &b0.remote.addr)
                    }
                };
                if better {
                    best = Some(cand);
                }
            }
        }
        best
    }
}

/// One usable link between two nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Path {
    pub from: NodeId,
    pub to: NodeId,
    pub local: Port,
    pub remote: Port,
    pub rtt_ms: u64,
}

impl Path {
    fn to_json(&self, hosts: &BTreeMap<NodeId, String>) -> Value {
        let name = |n: &NodeId| hosts.get(n).cloned().unwrap_or_else(|| n.clone());
        json!({
            "from": name(&self.from),
            "to": name(&self.to),
            "from_node": self.from,
            "to_node": self.to,
            "local": {"addr": self.local.addr, "iface": self.local.iface},
            "remote": {"addr": self.remote.addr, "iface": self.remote.iface},
            "rtt_ms": self.rtt_ms,
        })
    }
}

/// A member of a placed set, with the link its rank should bind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub node: NodeId,
    pub bind: Port,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Solution {
    pub sets: BTreeMap<String, Vec<Member>>,
    pub between: Vec<Path>,
}

impl Solution {
    pub fn to_json(&self, t: &Topology) -> Value {
        let name = |n: &NodeId| t.hosts.get(n).cloned().unwrap_or_else(|| n.clone());
        let sets: BTreeMap<String, Value> = self
            .sets
            .iter()
            .map(|(k, ms)| {
                let members: Vec<Value> = ms
                    .iter()
                    .map(|m| {
                        json!({
                            "node": m.node,
                            "host": name(&m.node),
                            "bind": m.bind.addr,
                            "iface": m.bind.iface,
                            "tags": m.bind.tags,
                        })
                    })
                    .collect();
                (k.clone(), Value::Array(members))
            })
            .collect();
        json!({
            "sets": sets,
            "between": self.between.iter().map(|p| p.to_json(&t.hosts)).collect::<Vec<_>>(),
        })
    }
}

/// Candidate node groups for a set, best first.
///
/// An `rdma` set may only sit inside an island, since that is the derived
/// answer to "these nodes are cabled together and every pair was probed". An
/// `any` set takes nodes in id order, which is arbitrary but identical on
/// every daemon.
fn candidates(t: &Topology, set: &SetReq, taken: &BTreeSet<NodeId>) -> Vec<Vec<NodeId>> {
    let want = set.bundles.len();
    let fits = |n: &NodeId, i: usize| {
        !taken.contains(n) && t.free_gpus.get(n).copied().unwrap_or(0.0) >= set.bundles[i]
    };
    match set.link {
        Link::Rdma => {
            let mut out = Vec::new();
            for island in &t.islands {
                let avail: Vec<NodeId> = island
                    .nodes
                    .iter()
                    .filter(|n| !taken.contains(*n))
                    .cloned()
                    .collect();
                if avail.len() < want {
                    continue;
                }
                // The first `want` that fit, in island order. Trying every
                // subset would multiply the search for no gain: island
                // members are interchangeable by construction. A member
                // whose GPUs are spoken for is stepped over rather than
                // ending the walk.
                let mut chosen: Vec<NodeId> = Vec::new();
                for n in &avail {
                    if chosen.len() == want {
                        break;
                    }
                    if fits(n, chosen.len()) {
                        chosen.push(n.clone());
                    }
                }
                if chosen.len() == want {
                    out.push(chosen);
                }
            }
            out
        }
        Link::Any => {
            let mut chosen: Vec<NodeId> = Vec::new();
            for n in t.free_gpus.keys() {
                if chosen.len() == want {
                    break;
                }
                if fits(n, chosen.len()) {
                    chosen.push(n.clone());
                }
            }
            if chosen.len() == want {
                vec![chosen]
            } else {
                Vec::new()
            }
        }
    }
}

/// The address a member binds for its own set's traffic.
fn bind_for(t: &Topology, set: &SetReq, node: &NodeId, peers: &[NodeId]) -> Option<Port> {
    let ports = t.ports.get(node)?;
    match set.link {
        // The fabric address that reaches the rest of the set.
        Link::Rdma => ports
            .iter()
            .find(|p| {
                p.rdma()
                    && peers.iter().filter(|q| *q != node).all(|q| {
                        t.ports
                            .get(q)
                            .map(|qp| {
                                qp.iter()
                                    .any(|y| y.rdma() && t.rtt(&p.addr, &y.addr).is_some())
                            })
                            .unwrap_or(false)
                    })
            })
            .cloned(),
        // The node's own first choice.
        Link::Any => ports.first().cloned(),
    }
}

/// Match a request against the topology.
///
/// Sets are placed in the order given, with `rdma` sets first: they have the
/// fewest places to go, and placing a loose set first can take a node the
/// constrained one needed. Each set's candidates are tried in turn and the
/// choice is undone if a later set or a `between` requirement fails.
pub fn solve(t: &Topology, req: &Request) -> Result<Solution, String> {
    for s in &req.sets {
        if s.bundles.is_empty() {
            return Err(format!("set {:?} asks for no nodes", s.name));
        }
    }
    let names: BTreeSet<&str> = req.sets.iter().map(|s| s.name.as_str()).collect();
    if names.len() != req.sets.len() {
        return Err("two sets share a name".into());
    }
    for b in &req.between {
        for n in [&b.from, &b.to] {
            if !names.contains(n.as_str()) {
                return Err(format!("between names {n:?}, which is not a set"));
            }
        }
    }

    let mut order: Vec<&SetReq> = req.sets.iter().collect();
    order.sort_by_key(|s| (s.link != Link::Rdma, s.name.clone()));

    let mut chosen: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
    let mut taken: BTreeSet<NodeId> = BTreeSet::new();
    if !search(t, req, &order, 0, &mut chosen, &mut taken) {
        return Err(why_not(t, req));
    }

    let mut sets: BTreeMap<String, Vec<Member>> = BTreeMap::new();
    for s in &req.sets {
        let nodes = &chosen[&s.name];
        let mut members = Vec::new();
        for n in nodes {
            let bind = bind_for(t, s, n, nodes)
                .ok_or_else(|| format!("{n} has no address for set {:?}", s.name))?;
            members.push(Member {
                node: n.clone(),
                bind,
            });
        }
        sets.insert(s.name.clone(), members);
    }
    let between =
        paths_between(t, req, &chosen).ok_or("no probed link satisfies a between entry")?;
    Ok(Solution { sets, between })
}

fn search(
    t: &Topology,
    req: &Request,
    order: &[&SetReq],
    i: usize,
    chosen: &mut BTreeMap<String, Vec<NodeId>>,
    taken: &mut BTreeSet<NodeId>,
) -> bool {
    if i == order.len() {
        return paths_between(t, req, chosen).is_some();
    }
    let set = order[i];
    for cand in candidates(t, set, taken) {
        for n in &cand {
            taken.insert(n.clone());
        }
        chosen.insert(set.name.clone(), cand.clone());
        if search(t, req, order, i + 1, chosen, taken) {
            return true;
        }
        chosen.remove(&set.name);
        for n in &cand {
            taken.remove(n);
        }
    }
    false
}

/// One path per `between` requirement, or None if any cannot be met.
///
/// A requirement holds when some member of each set can reach some member of
/// the other. The reported path is the one a caller would use.
fn paths_between(
    t: &Topology,
    req: &Request,
    chosen: &BTreeMap<String, Vec<NodeId>>,
) -> Option<Vec<Path>> {
    let mut out = Vec::new();
    for b in &req.between {
        let (from, to) = (chosen.get(&b.from)?, chosen.get(&b.to)?);
        let mut best: Option<Path> = None;
        for a in from {
            for c in to {
                if let Some(p) = t.path(a, c, b.link) {
                    let better = match &best {
                        None => true,
                        Some(b0) => (p.rtt_ms, &p.from, &p.to) < (b0.rtt_ms, &b0.from, &b0.to),
                    };
                    if better {
                        best = Some(p);
                    }
                }
            }
        }
        out.push(best?);
    }
    Some(out)
}

/// Why nothing fit, in the terms the request was written in.
fn why_not(t: &Topology, req: &Request) -> String {
    let mut parts = Vec::new();
    for s in &req.sets {
        let want = s.bundles.len();
        match s.link {
            Link::Rdma => {
                let biggest = t.islands.iter().map(|i| i.nodes.len()).max().unwrap_or(0);
                if biggest < want {
                    parts.push(format!(
                        "set {:?} wants {want} nodes sharing a fabric, largest island has {biggest}",
                        s.name
                    ));
                }
            }
            Link::Any => {
                let have = t.free_gpus.len();
                if have < want {
                    parts.push(format!(
                        "set {:?} wants {want} nodes, cluster has {have}",
                        s.name
                    ));
                }
            }
        }
    }
    if parts.is_empty() {
        parts.push("no assignment satisfies every set and link together".into());
    }
    parts.join(". ")
}

// ---------------------------------------------------------------------------
// The claim table
// ---------------------------------------------------------------------------

/// Read a request out of the JSON a client sent.
pub fn parse(shape: &Value) -> Result<Request, String> {
    let sets = shape["sets"]
        .as_array()
        .ok_or("shape needs a sets array")?
        .iter()
        .map(|s| {
            let name = s["name"].as_str().ok_or("a set needs a name")?.to_string();
            let bundles: Vec<f64> = match &s["bundles"] {
                Value::Array(a) => a.iter().filter_map(|b| b.as_f64()).collect(),
                // A count with no per-node figure means one GPU each, which
                // is what a rank usually wants.
                Value::Number(n) => vec![1.0; n.as_u64().unwrap_or(0) as usize],
                _ => return Err(format!("set {name:?} needs bundles")),
            };
            Ok(SetReq {
                name,
                bundles,
                link: Link::parse(s["link"].as_str().unwrap_or("ip"))?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let between = shape["between"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|b| {
                    Ok(BetweenReq {
                        from: b["from"].as_str().ok_or("between needs from")?.to_string(),
                        to: b["to"].as_str().ok_or("between needs to")?.to_string(),
                        link: Link::parse(b["link"].as_str().unwrap_or("ip"))?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(Request { sets, between })
}

/// The topology as this daemon currently sees it.
///
/// Ports come from the peer table, which carries each node's addresses, the
/// operator's tags and the interface each sits on. Links come from the
/// probes, self to peer and peer to peer alike, so a fabric this daemon is
/// not on still counts.
pub fn topology(st: &crate::state::State) -> Topology {
    let mut t = Topology {
        islands: st.fabrics.islands.clone(),
        ..Default::default()
    };
    let mut record = |node: &NodeId,
                      host: &str,
                      addrs: &[String],
                      tags: &BTreeMap<String, Vec<String>>,
                      ifaces: &BTreeMap<String, String>| {
        let ports: Vec<Port> = addrs
            .iter()
            .map(|a| Port {
                addr: a.clone(),
                iface: ifaces.get(a).cloned(),
                tags: tags.get(a).cloned().unwrap_or_default(),
            })
            .collect();
        if !ports.is_empty() {
            t.ports.insert(node.clone(), ports);
        }
        t.hosts.insert(node.clone(), host.to_string());
    };
    record(
        &st.node_id,
        &st.hostname,
        &crate::announce::local_addrs(),
        &crate::announce::local_addr_tags(),
        &crate::announce::local_addr_ifaces(),
    );
    for p in st.peers.values().filter(|p| p.alive) {
        record(
            &p.node_id,
            &p.node_ip,
            &p.addrs,
            &p.addr_tags,
            &p.addr_ifaces,
        );
        for (local, remotes) in &p.probe_pairs {
            for (remote, r) in remotes {
                if r.ok {
                    t.links.insert((local.clone(), remote.clone()), r.rtt_ms);
                }
            }
        }
        for (_, q) in p.last_status["peers"].as_object().into_iter().flatten() {
            // A dead peer's last probes describe a box that has since gone.
            if !q["alive"].as_bool().unwrap_or(false) {
                continue;
            }
            for (local, remotes) in q["probes"].as_object().into_iter().flatten() {
                for (remote, r) in remotes.as_object().into_iter().flatten() {
                    if r["ok"].as_bool().unwrap_or(false) {
                        t.links.insert(
                            (local.clone(), remote.clone()),
                            r["rtt_ms"].as_u64().unwrap_or(0),
                        );
                    }
                }
            }
        }
    }
    // A node with no agent has no GPUs to give.
    for a in st.agents.values().filter(|a| a.alive) {
        *t.free_gpus.entry(a.node_id.clone()).or_insert(0.0) += st.free_gpus_of(&a.id).len() as f64;
        // An agent can register a node no daemon peers for, which leaves it
        // holding GPUs with no address to bind. What it told us on register
        // is an address, so it stands in. It carries no tag, which limits
        // that node to plain links.
        t.ports.entry(a.node_id.clone()).or_insert_with(|| {
            vec![Port {
                addr: a.node_ip.clone(),
                iface: None,
                tags: Vec::new(),
            }]
        });
        t.hosts
            .entry(a.node_id.clone())
            .or_insert_with(|| a.node_ip.clone());
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    fn port(addr: &str, iface: &str, tags: &[&str]) -> Port {
        Port {
            addr: addr.into(),
            iface: Some(iface.into()),
            tags: tags.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Two cabled pairs on separate fabrics, reaching each other only over
    /// the LAN. The topology this was written for.
    fn two_pairs() -> Topology {
        let mut t = Topology::default();
        let spec = [
            ("nA", "10.100.0.1", "192.168.1.70", "gx10-2353"),
            ("nB", "10.100.0.2", "192.168.1.77", "gx10-5818"),
            ("nC", "10.103.0.36", "192.168.1.36", "gx10-9722"),
            ("nD", "10.103.0.93", "192.168.1.93", "gx10-a7c3"),
        ];
        for (n, fab, lan, host) in spec {
            t.ports.insert(
                n.into(),
                vec![
                    port(fab, "enp1s0f0np0", &["connectx", "rdma"]),
                    port(lan, "eno1", &["lan"]),
                ],
            );
            t.free_gpus.insert(n.into(), 1.0);
            t.hosts.insert(n.into(), host.into());
        }
        // Fabric links inside each pair only.
        t.links
            .insert(("10.100.0.1".into(), "10.100.0.2".into()), 0);
        t.links
            .insert(("10.103.0.36".into(), "10.103.0.93".into()), 0);
        // The LAN reaches everything.
        for a in [
            "192.168.1.70",
            "192.168.1.77",
            "192.168.1.36",
            "192.168.1.93",
        ] {
            for b in [
                "192.168.1.70",
                "192.168.1.77",
                "192.168.1.36",
                "192.168.1.93",
            ] {
                if a != b {
                    t.links.insert((a.into(), b.into()), 1);
                }
            }
        }
        t.islands = vec![
            Island {
                nodes: vec!["nA".into(), "nB".into()],
                addr: [
                    ("nA".into(), "10.100.0.1".into()),
                    ("nB".into(), "10.100.0.2".into()),
                ]
                .into_iter()
                .collect(),
            },
            Island {
                nodes: vec!["nC".into(), "nD".into()],
                addr: [
                    ("nC".into(), "10.103.0.36".into()),
                    ("nD".into(), "10.103.0.93".into()),
                ]
                .into_iter()
                .collect(),
            },
        ];
        t
    }

    fn req(sets: &[(&str, usize, Link)], between: &[(&str, &str, Link)]) -> Request {
        Request {
            sets: sets
                .iter()
                .map(|(n, k, l)| SetReq {
                    name: n.to_string(),
                    bundles: vec![1.0; *k],
                    link: *l,
                })
                .collect(),
            between: between
                .iter()
                .map(|(f, t2, l)| BetweenReq {
                    from: f.to_string(),
                    to: t2.to_string(),
                    link: *l,
                })
                .collect(),
        }
    }

    /// The request this exists for: two cabled pairs, IP between them.
    #[test]
    fn two_fabric_pairs_with_ip_between_them() {
        let t = two_pairs();
        let r = req(
            &[("tp0", 2, Link::Rdma), ("tp1", 2, Link::Rdma)],
            &[("tp0", "tp1", Link::Any)],
        );
        let s = solve(&t, &r).expect("should place");

        let nodes = |k: &str| {
            s.sets[k]
                .iter()
                .map(|m| m.node.clone())
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(nodes("tp0"), ["nA", "nB"].map(String::from).into());
        assert_eq!(nodes("tp1"), ["nC", "nD"].map(String::from).into());

        // Each set binds its fabric, with the interface to bind it on.
        for m in &s.sets["tp0"] {
            assert!(m.bind.tags.contains(&"rdma".to_string()), "{m:?}");
            assert_eq!(m.bind.iface.as_deref(), Some("enp1s0f0np0"));
        }
        // Between them, the LAN.
        assert_eq!(s.between.len(), 1);
        assert!(s.between[0].local.tags.contains(&"lan".to_string()));
        assert_eq!(s.between[0].local.iface.as_deref(), Some("eno1"));
    }

    /// Sets never share a node, or two ranks would collide on one box.
    #[test]
    fn sets_are_disjoint() {
        let t = two_pairs();
        let s = solve(&t, &req(&[("a", 2, Link::Rdma), ("b", 2, Link::Rdma)], &[])).unwrap();
        let a: BTreeSet<_> = s.sets["a"].iter().map(|m| &m.node).collect();
        let b: BTreeSet<_> = s.sets["b"].iter().map(|m| &m.node).collect();
        assert!(a.is_disjoint(&b));
    }

    /// A fabric requirement between two sets on separate fabrics cannot be
    /// met, and saying so beats placing them and hanging in NCCL.
    #[test]
    fn a_fabric_between_separate_fabrics_is_refused() {
        let t = two_pairs();
        let r = req(
            &[("tp0", 2, Link::Rdma), ("tp1", 2, Link::Rdma)],
            &[("tp0", "tp1", Link::Rdma)],
        );
        assert!(solve(&t, &r).is_err());
    }

    /// Three nodes on one fabric when the biggest island holds two.
    #[test]
    fn a_set_larger_than_any_island_is_refused() {
        let t = two_pairs();
        let e = solve(&t, &req(&[("big", 3, Link::Rdma)], &[])).unwrap_err();
        assert!(e.contains("largest island has 2"), "{e}");
    }

    /// A node with its GPUs spoken for cannot take a rank.
    /// A node with its GPUs spoken for cannot take a rank, so the pair goes
    /// to the other fabric rather than being placed short.
    #[test]
    fn a_full_node_sends_the_set_elsewhere() {
        let mut t = two_pairs();
        t.free_gpus.insert("nB".into(), 0.0);
        let s = solve(&t, &req(&[("tp0", 2, Link::Rdma)], &[])).unwrap();
        let got: BTreeSet<_> = s.sets["tp0"].iter().map(|m| m.node.clone()).collect();
        assert_eq!(got, ["nC", "nD"].map(String::from).into());
    }

    /// A loose set placed first can take a node the cabled set needed, so
    /// the constrained set goes first whatever order the caller wrote.
    ///
    /// nE is off both fabrics and sorts first, so a loose set placed before
    /// the cabled one would take nA and leave no pair.
    #[test]
    fn a_constrained_set_is_placed_before_a_loose_one() {
        let mut t = two_pairs();
        for n in ["nC", "nD"] {
            t.free_gpus.remove(n);
            t.ports.remove(n);
        }
        t.islands.retain(|i| i.nodes.contains(&"nA".to_string()));
        t.ports
            .insert("n0".into(), vec![port("192.168.1.50", "eno1", &["lan"])]);
        t.free_gpus.insert("n0".into(), 1.0);
        t.hosts.insert("n0".into(), "gx10-spare".into());

        let s = solve(
            &t,
            &req(&[("loose", 1, Link::Any), ("cabled", 2, Link::Rdma)], &[]),
        )
        .expect("the cabled pair must not be broken up by the loose set");
        let cabled: BTreeSet<_> = s.sets["cabled"].iter().map(|m| m.node.clone()).collect();
        assert_eq!(cabled, ["nA", "nB"].map(String::from).into());
        assert_eq!(s.sets["loose"][0].node, "n0");
    }

    /// The same view answers the same way, or one name would mean two
    /// placements.
    #[test]
    fn the_answer_is_stable() {
        let t = two_pairs();
        let r = req(
            &[("tp0", 2, Link::Rdma), ("tp1", 2, Link::Rdma)],
            &[("tp0", "tp1", Link::Any)],
        );
        let first = solve(&t, &r).unwrap();
        for _ in 0..8 {
            assert_eq!(solve(&t, &r).unwrap(), first);
        }
    }

    #[test]
    fn a_between_naming_no_set_is_refused() {
        let t = two_pairs();
        let r = req(&[("tp0", 2, Link::Rdma)], &[("tp0", "ghost", Link::Any)]);
        assert!(solve(&t, &r).unwrap_err().contains("ghost"));
    }

    #[test]
    fn link_names() {
        assert_eq!(Link::parse("roce"), Ok(Link::Rdma));
        assert_eq!(Link::parse("rdma"), Ok(Link::Rdma));
        assert_eq!(Link::parse("ip"), Ok(Link::Any));
        assert!(Link::parse("carrier pigeon").is_err());
    }
}
