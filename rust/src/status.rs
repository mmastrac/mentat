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
        let agents: Vec<Value> = st
            .agents
            .values()
            .filter(|a| &a.group == g)
            .map(|a| {
                json!({
                    "id": a.id,
                    "node_ip": a.node_ip,
                    "node_id": a.node_id,
                    "container": a.container,
                    "alive": a.alive,
                    "degraded": a.degraded,
                    "gpus": a.gpus.len(),
                    "gpus_free": st.free_gpus_of(&a.id).len(),
                    "gpu_vendor": a.gpu_vendor,
                    "cpus": a.cpus,
                    "pid": a.pid,
                    "services": a.services,
                    "services_ports": a.services_ports,
                    "service_notes": a.service_notes,
                    "node_note": a.node_note,
                    "provider": a.provider,
                })
            })
            .collect();
        let actors: Vec<Value> = st
            .actors
            .values()
            .filter(|a| &a.group == g)
            .map(|a| {
                let state = match &a.state {
                    ActorState::Spawning => "spawning".to_string(),
                    ActorState::Running => "running".to_string(),
                    ActorState::Dead { reason } => format!("dead ({reason})"),
                };
                json!({
                    "id": a.id,
                    "name": a.name,
                    "node_id": a.node_id,
                    "gpu_ids": a.gpu_ids,
                    "state": state,
                    "pid": a.pid,
                })
            })
            .collect();
        let pgs: Vec<Value> = st
            .pgs
            .values()
            .filter(|p| &p.group == g)
            .map(|p| {
                json!({
                    "id": p.id,
                    "bundles": p.bundles.len(),
                    "strategy": p.strategy,
                    "state": match p.state {
                        PgState::Pending => "PENDING",
                        PgState::Created => "CREATED",
                        PgState::Removed => "REMOVED",
                    },
                    // What the last placement attempt could not find. The
                    // pending timeout says the same thing minutes later;
                    // this says it while there is still time to act.
                    "pending_reason": p.pending_reason,
                    "island_nodes": p.island.as_ref().map(|i| i.nodes.len()),
                })
            })
            .collect();

        let total: usize = st
            .agents
            .values()
            .filter(|a| a.alive && &a.group == g)
            .map(|a| a.gpus.len())
            .sum();
        let free: usize = st
            .agents
            .values()
            .filter(|a| a.alive && &a.group == g)
            .map(|a| st.free_gpus_of(&a.id).len())
            .sum();

        out_groups.insert(
            g.clone(),
            json!({
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
                    "control_addr": p.control_addr,
                    "http_port": p.http_port,
                    "alive": p.alive,
                    "stale": p.stale,
                    "last_seen_ms": p.last_seen_ms,
                    "groups": peer_groups,
                }),
            )
        })
        .collect();

    json!({
        "node_id": st.node_id,
        "node_ip": st.node_ip,
        "addrs": crate::announce::local_addrs(),
        "addr_tags": crate::announce::local_addr_tags(),
        "addr_ifaces": crate::announce::local_addr_ifaces(),
        "hostname": st.hostname,
        "gcs_address": st.gcs_address,
        "head_node_id": st.head_node_id,
        "head_generation": st.head_generation,
        // Derived from probes rather than configuration: these are the sets a
        // multi-bundle placement group may be placed inside.
        "islands": st.fabrics.islands.iter().map(|i| json!({
            "nodes": i.nodes,
            "addrs": i.addr,
        })).collect::<Vec<_>>(),
        "peers": peers,
        "groups": out_groups,
        "counters": {
            "actors_spawned": st.counters.actors_spawned,
            "actor_exits_clean": st.counters.actor_exits_clean,
            "actor_exits_signal": st.counters.actor_exits_signal,
            "actor_exits_error": st.counters.actor_exits_error,
            "calls_total": st.counters.calls_total,
            "clients_total": st.counters.clients_total,
            "agents_registered": st.counters.agents_registered,
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
        data["gcs_address"].as_str().unwrap_or("?"),
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
        for a in g["agents"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "  agent {} node={} container={} gpus={}/{} vendor={} alive={}{}\n",
                a["id"].as_str().unwrap_or("?"),
                a["node_ip"].as_str().unwrap_or("?"),
                a["container"].as_str().unwrap_or("?"),
                a["gpus"].as_u64().unwrap_or(0) - a["gpus_free"].as_u64().unwrap_or(0),
                a["gpus"].as_u64().unwrap_or(0),
                a["gpu_vendor"].as_str().unwrap_or("?"),
                a["alive"].as_bool().unwrap_or(false),
                if a["degraded"].as_bool().unwrap_or(false) {
                    " degraded=true"
                } else {
                    ""
                },
            ));
        }
        for p in g["placement_groups"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "  pg {} bundles={} state={}\n",
                p["id"].as_str().unwrap_or("?"),
                p["bundles"].as_u64().unwrap_or(0),
                p["state"].as_str().unwrap_or("?"),
            ));
        }
        for a in g["actors"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "  actor {} [{}] node={}... pid={} state={}\n",
                a["name"].as_str().unwrap_or("?"),
                a["id"].as_str().unwrap_or("?"),
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
            "gcs_address": "10.100.0.2:6379",
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
            "gcs_address": "x", "hostname": "y",
            "groups": { "g": { "gpus_total": 4.0, "gpus_used": 0.0,
                                "agents": [], "placement_groups": [], "actors": [] } }
        });
        assert_eq!(entrypoint_gpu_gate(&render(&data4, true)), Some(4));

        // Unscoped output must not accidentally match the pipeline at all.
        assert_eq!(entrypoint_gpu_gate(&render(&data, false)), None);
    }
}
