//! Status snapshot (JSON for /status and StatusOk) and the CLI rendering.
//!
//! The rendering carries one load-bearing contract: in group scope it prints
//! EXACTLY ONE line matching `[0-9.]+/[0-9.]+ GPU`, because the glm53/ds4
//! entrypoints gate worker readiness on
//!   ray status | grep -oE '[0-9.]+/[0-9.]+ GPU' | cut -d/ -f2 | cut -d. -f1
//! and a second matching line would corrupt that pipeline. Every other line
//! spells gpu counts as `gpus=a/b` to stay out of the regex's way.

use serde_json::{json, Value};

use crate::state::{ActorState, PgState, State};

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
                    "gpus": a.gpus.len(),
                    "gpus_free": st.free_gpus_of(&a.id).len(),
                    "gpu_vendor": a.gpu_vendor,
                    "cpus": a.cpus,
                    "pid": a.pid,
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

    json!({
        "node_id": st.node_id,
        "node_ip": st.node_ip,
        "hostname": st.hostname,
        "gcs_address": st.gcs_address,
        "head_node_id": st.node_id,
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
    out.push_str(&format!(
        "mentat head: {} ({})\n",
        data["gcs_address"].as_str().unwrap_or("?"),
        data["hostname"].as_str().unwrap_or("?"),
    ));

    for (name, g) in groups {
        out.push_str(&format!(
            "group {name}: gpus={}/{}\n",
            g["gpus_used"].as_f64().unwrap_or(0.0) as u64,
            g["gpus_total"].as_f64().unwrap_or(0.0) as u64,
        ));
        for a in g["agents"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "  agent {} node={} container={} gpus={}/{} vendor={} alive={}\n",
                a["id"].as_str().unwrap_or("?"),
                a["node_ip"].as_str().unwrap_or("?"),
                a["container"].as_str().unwrap_or("?"),
                a["gpus"].as_u64().unwrap_or(0) - a["gpus_free"].as_u64().unwrap_or(0),
                a["gpus"].as_u64().unwrap_or(0),
                a["gpu_vendor"].as_str().unwrap_or("?"),
                a["alive"].as_bool().unwrap_or(false),
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
                &a["node_id"].as_str().unwrap_or("??????")[..6.min(a["node_id"].as_str().unwrap_or("??????").len())],
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
