//! Fabric islands: which nodes can actually reach each other over an
//! RDMA-tagged link, derived from the mesh's own probes.
//!
//! Two boxes are on one fabric when the operator tagged an address on each
//! `rdma` AND a probe between those two addresses succeeded. Each half does
//! its own work. The tag picks out the links meant to carry NCCL, which
//! matters because the LAN reaches every box and a bare probe would call
//! that a fabric. The probe confirms the cabling, which matters because the
//! cluster numbers both of its fabrics out of the same subnet, so a cabled
//! pair and two unconnected boxes look alike to address arithmetic.
//!
//! Each daemon computes this for itself, from its own probes plus the
//! probes its peers publish in their status pushes. Soft consistency is
//! fine: one daemon decides a given placement group, the one its driver
//! rendezvoused with, so two daemons disagreeing for a few seconds cannot
//! split a placement.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::cfg;
use crate::logfmt::log;
use crate::state::{NodeId, SharedRef};

/// A set of nodes that are mutually reachable over RDMA-tagged addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Island {
    /// Members, sorted, so two islands compare by value.
    pub nodes: Vec<NodeId>,
    /// The address each member answers on inside this island. Every member
    /// has one, and every pair of them answered a probe. This is what a rank
    /// binds NCCL to.
    pub addr: BTreeMap<NodeId, String>,
}

/// What placement reads: the islands, and which nodes claim a fabric at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fabrics {
    pub islands: Vec<Island>,
    /// Nodes carrying at least one `rdma`-tagged address, whether or not a
    /// probe confirmed it. A group none of whose nodes appear here has not
    /// opted in and is placed without the fabric constraint, so tagging one
    /// pair cannot strand a deployment on the other.
    pub tagged: BTreeSet<NodeId>,
}

/// Everything the derivation reads, gathered from a daemon's merged view.
#[derive(Default, Debug)]
pub struct FabricView {
    /// Node -> its RDMA-tagged addresses, in the node's own rank order.
    pub rdma: BTreeMap<NodeId, Vec<String>>,
    /// Address pairs that answered a probe. Direction is not kept: a
    /// one-way link is not a thing this fabric can have, and both daemons
    /// probe anyway.
    pub ok_pairs: BTreeSet<(String, String)>,
}

impl FabricView {
    fn linked(&self, a: &str, b: &str) -> bool {
        self.ok_pairs.contains(&(a.to_string(), b.to_string()))
            || self.ok_pairs.contains(&(b.to_string(), a.to_string()))
    }

    /// Every address that could carry fabric traffic, as (node, address),
    /// sorted so every daemon walks them in the same order.
    fn ports(&self) -> Vec<(&NodeId, &String)> {
        let mut out: Vec<(&NodeId, &String)> = self
            .rdma
            .iter()
            .flat_map(|(n, addrs)| addrs.iter().map(move |a| (n, a)))
            .collect();
        out.sort();
        out
    }

    /// Whether two ports may carry traffic between their nodes. Two ports of
    /// one node never do: NCCL between ranks crosses boxes, and the prober
    /// never probes a node against itself.
    fn joins(&self, x: (&NodeId, &String), y: (&NodeId, &String)) -> bool {
        x.0 != y.0 && self.linked(x.1, y.1)
    }
}

/// The islands a view implies.
///
/// The graph is over ports -- one node's one address -- rather than over
/// nodes, because a rank binds one address and every other rank has to reach
/// that one. A node with two fabric ports on separate links joins an island
/// through whichever port reaches all of it, or through neither.
///
/// An island is a set every member reaches. A connected component is weaker
/// than that: placement puts a whole placement group inside one island, so a
/// member reaching only some of the others would leave ranks unable to talk.
/// Components are therefore pruned to mutually-connected sets, least-
/// connected port first. On the topology this exists for -- disjoint cabled
/// pairs -- components already are whole and nothing is pruned. The pruning
/// is what keeps a half-cabled mistake from being announced as a fabric.
///
/// Islands of one are dropped. A lone node is not on a fabric, and placement
/// treats a node as its own island anyway.
pub fn islands(v: &FabricView) -> Vec<Island> {
    let ports = v.ports();
    let mut out: Vec<Island> = Vec::new();
    let mut placed: HashSet<(&NodeId, &String)> = HashSet::new();

    for seed in &ports {
        if placed.contains(seed) {
            continue;
        }
        // Connected component by breadth-first walk.
        let mut comp: Vec<(&NodeId, &String)> = vec![*seed];
        let mut queue = vec![*seed];
        while let Some(p) = queue.pop() {
            for q in &ports {
                if !comp.contains(q) && v.joins(p, *q) {
                    comp.push(*q);
                    queue.push(*q);
                }
            }
        }
        for p in &comp {
            placed.insert(*p);
        }
        // Prune to a mutually-connected set: drop the least-connected port
        // until every survivor reaches every other. Ties break on (node,
        // address) so every daemon prunes identically.
        loop {
            let worst = comp
                .iter()
                .map(|p| {
                    let deg = comp.iter().filter(|q| v.joins(*p, **q)).count();
                    (deg, *p)
                })
                .min();
            let Some((deg, port)) = worst else { break };
            if deg + 1 >= comp.len() {
                break;
            }
            log(
                "island_pruned",
                &[
                    ("node", port.0.clone()),
                    ("addr", port.1.clone()),
                    ("reaches", deg.to_string()),
                    ("of", (comp.len() - 1).to_string()),
                    (
                        "why",
                        "a placement group must fit a set every member reaches".to_string(),
                    ),
                ],
            );
            comp.retain(|q| q != &port);
        }
        if comp.len() < 2 {
            continue;
        }
        comp.sort();
        out.push(Island {
            nodes: comp.iter().map(|(n, _)| (*n).clone()).collect(),
            addr: comp
                .iter()
                .map(|(n, a)| ((*n).clone(), (*a).clone()))
                .collect(),
        });
    }
    out.sort_by(|a, b| a.nodes.cmp(&b.nodes));
    out
}

/// The islands plus the nodes that claim a fabric, which is what placement
/// needs: one to place inside, the other to know whether to constrain.
pub fn fabrics(v: &FabricView) -> Fabrics {
    Fabrics {
        islands: islands(v),
        tagged: v.rdma.keys().cloned().collect(),
    }
}

/// Read the daemon's merged view: its own tags and probes, its peers', and
/// what its peers publish about their own peers.
///
/// The third source is what makes a four-node cluster work. This daemon
/// probes its own links only, so without its peers' published tables it
/// could never know whether two other boxes share a fabric.
fn gather(shared: &SharedRef) -> FabricView {
    let mut v = FabricView::default();
    let mut note = |node: &str, addrs: Vec<String>, tags: BTreeMap<String, Vec<String>>| {
        let tagged: Vec<String> = addrs
            .into_iter()
            .filter(|a| {
                tags.get(a)
                    .map(|t| t.iter().any(|x| x == "rdma"))
                    .unwrap_or(false)
            })
            .collect();
        if !tagged.is_empty() {
            v.rdma.insert(node.to_string(), tagged);
        }
    };

    let st = shared.st.lock().unwrap();
    note(
        &st.node_id,
        crate::announce::local_addrs(),
        crate::announce::local_addr_tags(),
    );
    for p in st.peers.values().filter(|p| p.alive) {
        note(&p.node_id, p.addrs.clone(), p.addr_tags.clone());
        for (local, remotes) in &p.probe_pairs {
            for (remote, r) in remotes {
                if r.ok {
                    v.ok_pairs.insert((local.clone(), remote.clone()));
                }
            }
        }
        // What this peer knows about the rest of the mesh.
        for (_, q) in p.last_status["peers"].as_object().into_iter().flatten() {
            let Some(id) = q["node_id"].as_str().or_else(|| q["id"].as_str()) else {
                continue;
            };
            note(id, str_list(&q["addrs"]), tag_map(&q["addr_tags"]));
            for (local, remotes) in q["probes"].as_object().into_iter().flatten() {
                for (remote, r) in remotes.as_object().into_iter().flatten() {
                    if r["ok"].as_bool().unwrap_or(false) {
                        v.ok_pairs.insert((local.clone(), remote.clone()));
                    }
                }
            }
        }
    }
    v
}

fn str_list(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| a.as_str())
        .map(str::to_string)
        .collect()
}

fn tag_map(v: &Value) -> BTreeMap<String, Vec<String>> {
    v.as_object()
        .into_iter()
        .flatten()
        .map(|(k, t)| (k.clone(), str_list(t)))
        .collect()
}

/// Recompute islands and commit a change only once it has held still for
/// MENTAT_ISLAND_HOLD_DOWN_MS.
///
/// The hold-down is the election's argument applied to cables. A QSFP link
/// that flaps would otherwise move the island boundary between two
/// consecutive placements, and a placement group is the one thing here that
/// cannot be revised after the fact.
pub fn start(shared: SharedRef) {
    std::thread::spawn(move || run(shared));
}

fn run(shared: SharedRef) {
    let hold_down = Duration::from_millis(cfg().island_hold_down_ms);
    let tick = Duration::from_millis((cfg().island_hold_down_ms / 5).clamp(100, 1000));
    let mut candidate: Option<(Fabrics, Instant)> = None;
    // Tagged addresses no probe has ever confirmed. Named once each: the
    // tag says the operator meant to cable this, so silence about it is
    // how a wrong tag survives a deployment.
    let mut unverified: HashSet<String> = HashSet::new();
    loop {
        std::thread::sleep(tick);
        let view = gather(&shared);
        let fresh = fabrics(&view);

        let confirmed: HashSet<&String> = view.ok_pairs.iter().flat_map(|(a, b)| [a, b]).collect();
        for (node, addrs) in &view.rdma {
            for a in addrs {
                if confirmed.contains(a) {
                    unverified.remove(a);
                } else if unverified.insert(a.clone()) {
                    log(
                        "fabric_addr_unverified",
                        &[
                            ("node", node.clone()),
                            ("addr", a.clone()),
                            (
                                "why",
                                "tagged rdma, but no probe over it has succeeded -- \
                                 a cable that is out, or a tag on the wrong interface"
                                    .to_string(),
                            ),
                        ],
                    );
                }
            }
        }

        {
            let st = shared.st.lock().unwrap();
            if st.fabrics == fresh {
                candidate = None;
                continue;
            }
        }
        let since = match &candidate {
            Some((c, t)) if *c == fresh => *t,
            _ => {
                candidate = Some((fresh, Instant::now()));
                continue;
            }
        };
        if since.elapsed() < hold_down {
            continue;
        }
        let mut st = shared.st.lock().unwrap();
        st.fabrics = fresh.clone();
        st.emit(
            "islands_changed",
            json!({ "islands": fresh.islands.iter().map(|i| &i.nodes).collect::<Vec<_>>(),
                    "tagged_nodes": fresh.tagged.len() }),
        );
        candidate = None;
        // A placement group that was waiting for a fabric may fit now.
        crate::daemon::try_place(&mut st, &shared.cv);
        shared.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(rdma: &[(&str, &[&str])], ok: &[(&str, &str)]) -> FabricView {
        FabricView {
            rdma: rdma
                .iter()
                .map(|(n, a)| (n.to_string(), a.iter().map(|s| s.to_string()).collect()))
                .collect(),
            ok_pairs: ok
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        }
    }

    /// The cluster this exists for: four boxes, two cabled pairs, every
    /// fabric address out of the same subnet. Only the probes separate them.
    #[test]
    fn two_cabled_pairs_are_two_islands() {
        let v = view(
            &[
                ("n1", &["10.100.0.1"]),
                ("n2", &["10.100.0.2"]),
                ("n3", &["10.100.0.3"]),
                ("n4", &["10.100.0.4"]),
            ],
            &[("10.100.0.1", "10.100.0.2"), ("10.100.0.3", "10.100.0.4")],
        );
        let got = islands(&v);
        assert_eq!(
            got.iter().map(|i| i.nodes.clone()).collect::<Vec<_>>(),
            vec![vec!["n1", "n2"], vec!["n3", "n4"]],
        );
        assert_eq!(got[0].addr["n1"], "10.100.0.1");
        assert_eq!(got[1].addr["n4"], "10.100.0.4");
    }

    /// Same addresses, no probes: subnet arithmetic would call this one
    /// fabric of four. The probes call it nothing.
    #[test]
    fn a_shared_subnet_is_not_a_fabric() {
        let v = view(&[("n1", &["10.100.0.1"]), ("n2", &["10.100.0.2"])], &[]);
        assert!(islands(&v).is_empty());
    }

    /// An untagged link that probes fine is the LAN. Serving rides it;
    /// NCCL does not, so it makes no island.
    #[test]
    fn an_untagged_link_makes_no_island() {
        let v = view(&[], &[("192.168.1.11", "192.168.1.12")]);
        assert!(islands(&v).is_empty());
    }

    /// A half-cabled mistake: n3 reaches n1 but not n2. Announcing all
    /// three would place a TP=3 group whose ranks cannot all talk, so n3 is
    /// pruned out.
    #[test]
    fn a_partly_connected_set_is_pruned_to_one_that_is_whole() {
        let v = view(
            &[
                ("n1", &["10.0.0.1"]),
                ("n2", &["10.0.0.2"]),
                ("n3", &["10.0.0.3"]),
            ],
            &[("10.0.0.1", "10.0.0.2"), ("10.0.0.1", "10.0.0.3")],
        );
        let got = islands(&v);
        assert_eq!(got.len(), 1, "{got:?}");
        // Two survivors of equal standing exist ({n1,n2} and {n1,n3}). The
        // tie-break decides which, and decides it the same way on every
        // daemon. What is asserted here is what matters: the island is
        // whole, and smaller than the component it came from.
        assert_eq!(got[0].nodes.len(), 2, "{got:?}");
        assert!(got[0].nodes.contains(&"n1".to_string()), "{got:?}");
        let (x, y) = (&got[0].nodes[0], &got[0].nodes[1]);
        assert!(v.linked(&got[0].addr[x], &got[0].addr[y]), "{got:?}");
    }

    /// The trap a node graph misses: n1 has two fabric ports, and each
    /// reaches a different peer. Every node pair looks linked, so nothing
    /// prunes, but no single address of n1 reaches both -- and a rank binds
    /// one address. n1 is left out rather than handed an address that only
    /// half its island answers on.
    #[test]
    fn a_node_whose_ports_split_across_links_is_left_out() {
        let v = view(
            &[
                ("n1", &["10.0.0.1", "10.0.1.1"]),
                ("n2", &["10.0.0.2"]),
                ("n3", &["10.0.1.3"]),
            ],
            &[
                ("10.0.0.1", "10.0.0.2"),
                ("10.0.1.1", "10.0.1.3"),
                ("10.0.0.2", "10.0.1.3"),
            ],
        );
        let got = islands(&v);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].nodes, vec!["n2", "n3"], "{got:?}");
    }

    /// Every member of an island carries an address, and every pair of those
    /// addresses answered a probe. Placement hands each rank its entry, so a
    /// gap there is a rank with nothing to bind.
    #[test]
    fn every_member_has_an_address_that_reaches_every_other() {
        let v = view(
            &[
                ("n1", &["10.100.0.1"]),
                ("n2", &["10.100.0.2"]),
                ("n3", &["10.100.0.3"]),
            ],
            &[
                ("10.100.0.1", "10.100.0.2"),
                ("10.100.0.2", "10.100.0.3"),
                ("10.100.0.1", "10.100.0.3"),
            ],
        );
        let got = islands(&v);
        assert_eq!(got[0].nodes.len(), 3);
        for x in &got[0].nodes {
            for y in &got[0].nodes {
                if x != y {
                    assert!(v.linked(&got[0].addr[x], &got[0].addr[y]), "{x} {y}");
                }
            }
        }
    }

    /// Tagged but unprobed nodes still count as opted in, so a group on them
    /// is constrained rather than silently placed across the LAN.
    #[test]
    fn tagging_a_node_opts_it_in_before_any_probe_succeeds() {
        let f = fabrics(&view(&[("n1", &["10.0.0.1"]), ("n2", &["10.0.0.2"])], &[]));
        assert!(f.islands.is_empty());
        assert_eq!(f.tagged.len(), 2);
    }

    /// A node with two fabric ports picks the one that carried a probe, in
    /// its own rank order, so every daemon derives the same address.
    #[test]
    fn the_island_address_is_the_one_that_answered() {
        let v = view(
            &[("n1", &["10.0.0.1", "10.0.1.1"]), ("n2", &["10.0.1.2"])],
            &[("10.0.1.1", "10.0.1.2")],
        );
        let got = islands(&v);
        assert_eq!(got[0].addr["n1"], "10.0.1.1");
    }
}
