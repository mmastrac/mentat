//! Status snapshot (JSON for /status and StatusOk) and the CLI rendering.
//!
//! One contract the entrypoints depend on: in group scope the rendering
//! prints exactly one line matching `[0-9.]+/[0-9.]+ GPU`, because glm53/ds4
//! gate worker readiness on
//!   ray status | grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1
//! and a second matching line would corrupt that pipeline. Every other line
//! spells gpu counts as `gpus=a/b` to stay out of the regex's way.

use serde_json::{json, Value};

use crate::state::{ActorState, PeerInfo, PgState, State};

/// One peer's probed pairs as JSON, keyed local address then remote. A
/// pair the prober has not tried yet has no entry, which readers must not
/// confuse with a pair that failed.
fn probe_table(p: &PeerInfo) -> Value {
    Value::Object(
        p.probe_pairs
            .iter()
            .map(|(local, remotes)| {
                let row: serde_json::Map<String, Value> = remotes
                    .iter()
                    .map(|(remote, r)| {
                        (
                            remote.clone(),
                            json!({
                                "ok": r.ok,
                                "rtt_ms": r.rtt_ms,
                                "last_ok_ms": r.last_ok_ms,
                                "error": r.error,
                            }),
                        )
                    })
                    .collect();
                (local.clone(), Value::Object(row))
            })
            .collect(),
    )
}

/// One agent's row. The event that changes an agent sends this same row,
/// so the two cannot drift.
pub fn agent_row(st: &State, a: &crate::state::AgentInfo) -> Value {
    json!({
        "node_ip": a.node_ip,
        "node_id": a.node_id,
        "container": a.container,
        "alive": a.alive,
        "degraded": a.degraded,
        "gone_since_ms": a.gone_since_ms,
        "machine": a.machine,
        "gpus_free": st.free_gpus_of(&a.id),
        "services": a.services,
        "pid": a.pid,
    })
}

pub fn actor_row(a: &crate::state::ActorInfo) -> Value {
    let (state, reason) = match &a.state {
        ActorState::Spawning => ("spawning", String::new()),
        ActorState::Running => ("running", String::new()),
        ActorState::Dead { reason, .. } => ("dead", reason.clone()),
    };
    json!({
        "name": a.name,
        "node_id": a.node_id,
        "gpu_ids": a.gpu_ids,
        "state": state,
        "reason": reason,
        "pid": a.pid,
    })
}

pub fn pg_row(p: &crate::state::PgInfo) -> Value {
    json!({
        "bundles": p.bundles,
        "strategy": p.strategy,
        "state": match p.state {
            PgState::Pending => "PENDING",
            PgState::Created => "CREATED",
            PgState::Removed => "REMOVED",
        },
        "claim": p.claim,
        // What the last placement attempt could not find. The pending
        // timeout says the same thing minutes later; this says it while
        // there is still time to act.
        "pending_reason": p.pending_reason,
        "island_nodes": p.island.as_ref().map(|i| i.nodes.len()),
    })
}

/// A claim's row: what it holds, not the whole view. The addresses and
/// interfaces behind `sets` come back from `claim`, which is too large to
/// push on every peer_status interval.
pub fn claim_row(c: &crate::state::ClaimInfo) -> Value {
    let sets: serde_json::Map<String, Value> = c.view["sets"]
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), json!(v.as_array().map(|a| a.len()).unwrap_or(0))))
                .collect()
        })
        .unwrap_or_default();
    json!({
        "generation": c.generation,
        "holders": c.holders,
        "sets": sets,
    })
}

pub fn peer_row(p: &PeerInfo) -> Value {
    json!({
        "node_ip": p.node_ip,
        "link_ip": p.link_ip,
        "addrs": p.addrs,
        "addr_tags": p.addr_tags,
        "addr_ifaces": p.addr_ifaces,
        "probes": probe_table(p),
        "control_port": p.control_port,
        "http_port": p.http_port,
        "alive": p.alive,
        "stale": p.stale,
        "last_seen_ms": p.last_seen_ms,
        "dead_since_ms": p.dead_since_ms,
    })
}

pub fn client_row(c: &crate::state::ClientInfo) -> Value {
    json!({
        "group": c.group,
        "kind": c.kind,
        "node_id": c.node_id,
        "session": c.has_session,
    })
}

pub fn snapshot(st: &State, scope: Option<&str>) -> Value {
    let mut groups: Vec<String> = st
        .agents
        .values()
        .map(|a| a.group.clone())
        .chain(st.actors.values().map(|a| a.group.clone()))
        .collect();
    groups.sort();
    groups.dedup();
    if let Some(s) = scope {
        groups.retain(|g| g == s);
        if groups.is_empty() {
            groups.push(s.to_string());
        }
    }

    let mut out_groups = serde_json::Map::new();
    for g in &groups {
        let agents: serde_json::Map<String, Value> = st
            .agents
            .values()
            .filter(|a| &a.group == g)
            .map(|a| (a.id.clone(), agent_row(st, a)))
            .collect();
        let actors: serde_json::Map<String, Value> = st
            .actors
            .values()
            .filter(|a| &a.group == g)
            .map(|a| (a.id.clone(), actor_row(a)))
            .collect();
        let pgs: serde_json::Map<String, Value> = st
            .pgs
            .values()
            .filter(|p| &p.group == g)
            .map(|p| (p.id.clone(), pg_row(p)))
            .collect();

        let total: usize = st
            .agents
            .values()
            .filter(|a| a.alive && &a.group == g)
            .map(|a| a.machine.gpus.len())
            .sum();
        let free: usize = st
            .agents
            .values()
            .filter(|a| a.alive && &a.group == g)
            .map(|a| st.free_gpus_of(&a.id).len())
            .sum();

        let claims: serde_json::Map<String, Value> = st
            .claims
            .iter()
            .filter(|((group, _), _)| group == g)
            .map(|((_, name), c)| (name.clone(), claim_row(c)))
            .collect();
        out_groups.insert(
            g.clone(),
            json!({
                "claims": claims,
                "agents": agents,
                "actors": actors,
                "placement_groups": pgs,
                "gpus_total": total,
                "gpus_used": total - free,
            }),
        );
    }

    let peers: serde_json::Map<String, Value> = st
        .peers
        .values()
        .map(|p| {
            // Only a summary of the peer's groups; the full detail lives on
            // that daemon's own /status.
            let peer_groups: Value = p.last_status["groups"]
                .as_object()
                .map(|gs| {
                    Value::Object(
                        gs.iter()
                            .map(|(name, g)| {
                                (
                                    name.clone(),
                                    json!({
                                        "gpus_total": g["gpus_total"],
                                        "gpus_used": g["gpus_used"],
                                    }),
                                )
                            })
                            .collect(),
                    )
                })
                .unwrap_or(Value::Null);
            (
                p.node_id.clone(),
                json!({
                    "node_ip": p.node_ip,
                    "link_ip": p.link_ip,
                    "addrs": p.addrs,
                    "addr_tags": p.addr_tags,
                    "addr_ifaces": p.addr_ifaces,
                    "probes": probe_table(p),
                    "control_port": p.control_port,
                    "http_port": p.http_port,
                    "alive": p.alive,
                    "stale": p.stale,
                    "last_seen_ms": p.last_seen_ms,
                    "dead_since_ms": p.dead_since_ms,
                    "groups": peer_groups,
                }),
            )
        })
        .collect();

    let clients: serde_json::Map<String, Value> = st
        .clients
        .values()
        .filter(|c| scope.is_none_or(|s| c.group == s))
        .map(|c| (c.id.clone(), client_row(c)))
        .collect();

    json!({
        "proto": crate::proto::PROTO,
        "node_id": st.node_id,
        "node_ip": st.node_ip,
        "addrs": crate::announce::local_addrs(),
        "addr_tags": crate::announce::local_addr_tags(),
        "addr_ifaces": crate::announce::local_addr_ifaces(),
        "hostname": st.hostname,
        "control_addr": st.control_addr,
        "head_node_id": st.head_node_id,
        "head_generation": st.head_generation,
        // Derived from probes rather than configuration: these are the sets a
        // multi-bundle placement group may be placed inside.
        "islands": st.fabrics.islands.iter().map(|i| json!({
            "nodes": i.nodes,
            "addrs": i.addr,
        })).collect::<Vec<_>>(),
        "peers": peers,
        "clients": clients,
        "groups": out_groups,
        "counters": {
            "actors_spawned": st.counters.actors_spawned,
            "actor_exits_clean": st.counters.actor_exits_clean,
            "actor_exits_signal": st.counters.actor_exits_signal,
            "actor_exits_error": st.counters.actor_exits_error,
            "calls_total": st.counters.calls_total,
            "clients_total": st.counters.clients_total,
            "agents_registered": st.counters.agents_registered,
            "relayed": st.counters.relayed,
        },
    })
}

/// Render a snapshot for terminals. `scoped` mirrors whether the query was
/// group-scoped; only then is the ray-compatible GPU line printed.
pub fn render(data: &Value, scoped: bool) -> String {
    let mut out = String::new();
    let empty = serde_json::Map::new();
    let groups = data["groups"].as_object().unwrap_or(&empty);

    if scoped {
        // The one line the entrypoint pipelines depend on.
        let (mut used, mut total) = (0.0, 0.0);
        for g in groups.values() {
            used += g["gpus_used"].as_f64().unwrap_or(0.0);
            total += g["gpus_total"].as_f64().unwrap_or(0.0);
        }
        out.push_str(&format!(
            "Resources: {used:.1}/{total:.1} GPU ({used:.1} reserved in placement groups)\n"
        ));
    }
    let is_head = data["head_node_id"] == data["node_id"];
    out.push_str(&format!(
        "mentat daemon: {} ({}){}\n",
        data["control_addr"].as_str().unwrap_or("?"),
        data["hostname"].as_str().unwrap_or("?"),
        if is_head { " [head]" } else { "" },
    ));
    for (pid, p) in data["peers"].as_object().into_iter().flatten() {
        out.push_str(&format!(
            "peer {}... {} alive={}{}\n",
            &pid[..8.min(pid.len())],
            p["node_ip"].as_str().unwrap_or("?"),
            p["alive"],
            if data["head_node_id"].as_str() == Some(pid) {
                " [head]"
            } else {
                ""
            },
        ));
        // The reachability matrix, one row per local address. This
        // is the line to read against the patch panel: a pair the operator
        // cabled and that reads fail is a cabling or tagging mistake, and a
        // pair that reads ok on a link nothing was cabled on is the other.
        for (local, row) in p["probes"].as_object().into_iter().flatten() {
            let cells: Vec<String> = row
                .as_object()
                .into_iter()
                .flatten()
                .map(|(remote, r)| {
                    if r["ok"].as_bool().unwrap_or(false) {
                        format!("{remote}=ok/{}ms", r["rtt_ms"].as_u64().unwrap_or(0))
                    } else {
                        format!("{remote}=fail")
                    }
                })
                .collect();
            out.push_str(&format!("  reach from {local}: {}\n", cells.join(" ")));
        }
    }

    // Members by fabric address, which is what an operator compares against
    // the cabling. Node ids are useless here: every mentat one starts with
    // the hex of "mentat:", so a truncated id identifies nothing.
    for (i, isl) in data["islands"].as_array().into_iter().flatten().enumerate() {
        let members: Vec<String> = isl["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|n| n.as_str())
            .map(|n| match isl["addrs"][n].as_str() {
                Some(a) => a.to_string(),
                None => crate::state::node_ip_of(n).unwrap_or_else(|| n.to_string()),
            })
            .collect();
        out.push_str(&format!("fabric {i}: {}\n", members.join(" ")));
    }

    for (name, g) in groups {
        out.push_str(&format!(
            "group {name}: gpus={}/{}\n",
            g["gpus_used"].as_f64().unwrap_or(0.0) as u64,
            g["gpus_total"].as_f64().unwrap_or(0.0) as u64,
        ));
        for (id, a) in g["agents"].as_object().into_iter().flatten() {
            let gpus = a["machine"]["gpus"].as_array().map(|v| v.len()).unwrap_or(0);
            let free = a["gpus_free"].as_array().map(|v| v.len()).unwrap_or(0);
            // One line per agent, so the vendor set of its devices rather
            // than one vendor: a box may hold two models.
            let mut vendors: Vec<&str> = a["machine"]["gpus"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|g| g["vendor"].as_str())
                .collect();
            vendors.sort();
            vendors.dedup();
            out.push_str(&format!(
                "  agent {} node={} container={} gpus={}/{} vendor={} alive={}{}\n",
                id,
                a["node_ip"].as_str().unwrap_or("?"),
                a["container"].as_str().unwrap_or("?"),
                gpus - free,
                gpus,
                if vendors.is_empty() {
                    "?".to_string()
                } else {
                    vendors.join("+")
                },
                a["alive"].as_bool().unwrap_or(false),
                if a["degraded"].as_bool().unwrap_or(false) {
                    " degraded=true"
                } else {
                    ""
                },
            ));
        }
        for (id, p) in g["placement_groups"].as_object().into_iter().flatten() {
            out.push_str(&format!(
                "  pg {} bundles={} state={}\n",
                id,
                p["bundles"].as_array().map(|b| b.len()).unwrap_or(0),
                p["state"].as_str().unwrap_or("?"),
            ));
        }
        for (id, a) in g["actors"].as_object().into_iter().flatten() {
            out.push_str(&format!(
                "  actor {} [{}] node={}... pid={} state={}\n",
                a["name"].as_str().unwrap_or("?"),
                id,
                &a["node_id"].as_str().unwrap_or("??????")
                    [..6.min(a["node_id"].as_str().unwrap_or("??????").len())],
                a["pid"].as_u64().unwrap_or(0),
                a["state"].as_str().unwrap_or("?"),
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The entrypoint's literal pipeline, transcribed:
    ///   grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1
    fn entrypoint_gpu_gate(status_output: &str) -> Option<u64> {
        let re_matches: Vec<&str> = status_output
            .lines()
            .filter(|l| find_gpu_pattern(l).is_some())
            .collect();
        assert!(
            re_matches.len() <= 1,
            "more than one line matches the GPU regex: {re_matches:?}"
        );
        let m = find_gpu_pattern(re_matches.first()?)?;
        let denom = m.split('/').nth(1)?;
        denom.split('.').next()?.parse().ok()
    }

    /// Minimal reimplementation of grep -oE '[0-9.]+/[0-9.]+ GPU'.
    fn find_gpu_pattern(line: &str) -> Option<String> {
        let gpu_at = line.find(" GPU")?;
        let before = &line[..gpu_at];
        let start = before
            .rfind(|c: char| !(c.is_ascii_digit() || c == '.' || c == '/'))
            .map(|i| i + 1)
            .unwrap_or(0);
        let cand = &before[start..];
        let mut parts = cand.split('/');
        let (a, b) = (parts.next()?, parts.next()?);
        if a.is_empty() || b.is_empty() || parts.next().is_some() {
            return None;
        }
        Some(cand.to_string())
    }

    #[test]
    fn gpu_line_contract() {
        let data = serde_json::json!({
            "control_addr": "10.100.0.2:6379",
            "hostname": "gx10-n1",
            "groups": {
                "glm53": {
                    "gpus_total": 2.0, "gpus_used": 2.0,
                    "agents": [
                        {"id": "glm53@glm53", "node_ip": "10.100.0.2", "container": "glm53",
                         "gpus": 1, "gpus_free": 0, "gpu_vendor": "nvidia", "alive": true},
                        {"id": "glm53@glm53w", "node_ip": "10.100.0.1", "container": "glm53",
                         "gpus": 1, "gpus_free": 0, "gpu_vendor": "nvidia", "alive": true},
                    ],
                    "placement_groups": [{"id": "abc", "bundles": 2, "state": "CREATED"}],
                    "actors": [{"id": "a1", "name": "vllm_Worker_1_TP0", "node_id": "aabbcc",
                                "pid": 100, "state": "running"}],
                }
            }
        });
        let text = render(&data, true);
        assert_eq!(entrypoint_gpu_gate(&text), Some(2));

        // TP=4-shaped totals must survive the same pipeline.
        let data4 = serde_json::json!({
            "control_addr": "x", "hostname": "y",
            "groups": { "g": { "gpus_total": 4.0, "gpus_used": 0.0,
                                "agents": [], "placement_groups": [], "actors": [] } }
        });
        assert_eq!(entrypoint_gpu_gate(&render(&data4, true)), Some(4));

        // Unscoped output must not accidentally match the pipeline at all.
        assert_eq!(entrypoint_gpu_gate(&render(&data, false)), None);
    }
}
