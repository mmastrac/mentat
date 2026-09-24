//! The merged MCP endpoint: every group's management MCP behind a single
//! URL, in one flat namespace. Groups run the same status server, so a tool
//! name appearing in several of them is one tool with several places to run
//! it. It is listed once and gains a `__group` argument naming where to run
//! it. The engine-health gate does not apply here: the status server exists
//! for when the engine is loading or wedged, so any group with an alive
//! agent announcing "mcp" is listed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::{json, Map, Value};

use crate::{
    full_body, group_table, http_post_json, json_response, status_view, BoxedBody, Shared,
};
use mentat_common::logfmt::log;

/// The argument added to every merged tool to report which group runs it. The
/// leading underscores keep it clear of the tools' own parameter names.
const GROUP_ARG: &str = "__group";

/// The one tool this server serves itself. A group tool of the same name
/// would be unreachable, so the merge drops it and logs.
const NATIVE: &str = "serve_status";

/// Tool calls are small JSON. A bigger body is a client bug.
const MAX_BODY: usize = 16 * 1024 * 1024;

pub async fn handle(shared: &Arc<Shared>, req: Request<Incoming>) -> Response<BoxedBody> {
    let bytes = match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &json!({"error": format!("request body over {MAX_BODY} bytes")}),
            )
        }
    };
    let payload: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"jsonrpc": "2.0", "id": null,
                        "error": {"code": -32700, "message": format!("parse error: {e}")}}),
            )
        }
    };

    // A client may batch requests in a list. Only a request gets an entry.
    if let Value::Array(reqs) = payload {
        let mut out = Vec::new();
        for r in reqs {
            if let Some(resp) = rpc(shared, r).await {
                out.push(resp);
            }
        }
        if out.is_empty() {
            return empty_202();
        }
        return json_response(StatusCode::OK, &Value::Array(out));
    }
    match rpc(shared, payload).await {
        Some(resp) => json_response(StatusCode::OK, &resp),
        None => empty_202(),
    }
}

fn empty_202() -> Response<BoxedBody> {
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(full_body(""))
        .expect("static response")
}

async fn rpc(shared: &Arc<Shared>, req: Value) -> Option<Value> {
    let rid = req["id"].clone();
    let method = req["method"].as_str().unwrap_or("");
    match method {
        "initialize" => {
            let proto = req["params"]["protocolVersion"]
                .as_str()
                .unwrap_or("2025-06-18");
            Some(json!({"jsonrpc": "2.0", "id": rid, "result": {
                "protocolVersion": proto,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": "mentatd-serve", "version": env!("CARGO_PKG_VERSION")},
            }}))
        }
        m if m.starts_with("notifications/") => None,
        "ping" => Some(json!({"jsonrpc": "2.0", "id": rid, "result": {}})),
        "tools/list" => Some(json!({"jsonrpc": "2.0", "id": rid,
                                    "result": {"tools": tool_list(shared).await}})),
        "tools/call" => Some(call(shared, rid, req["params"].clone()).await),
        _ => Some(json!({"jsonrpc": "2.0", "id": rid,
                         "error": {"code": -32601, "message": format!("no method {method:?}")}})),
    }
}

/// One tool name as the groups offering it describe it. The first group's
/// description and schema stand for all of them: the groups run the same
/// status server, so a difference between them is version skew.
struct Merged {
    description: String,
    schema: Value,
    /// Every group offering the tool, in group-table order, so the first
    /// is the same one on every call.
    groups: Vec<String>,
}

/// Every group's tools keyed by tool name, merged across groups.
async fn merge(shared: &Arc<Shared>) -> BTreeMap<String, Merged> {
    let mut out: BTreeMap<String, Merged> = BTreeMap::new();
    for e in group_table(shared).values() {
        // Ungated, so there is no probe to choose among candidates with:
        // the best-ranked one is the answer. See Endpoint::best.
        let Some(url) = e.mcp.as_ref().and_then(|m| m.best()).map(str::to_string) else {
            continue;
        };
        for t in group_tools(shared, &e.group, &url).await {
            let Some(name) = t["name"].as_str() else {
                continue;
            };
            if name == NATIVE {
                log(
                    "mcp_tool_dropped",
                    &[
                        ("group", e.group.clone()),
                        ("tool", name.to_string()),
                        ("why", "name taken by this server".into()),
                    ],
                );
                continue;
            }
            if let Some(m) = out.get_mut(name) {
                m.groups.push(e.group.clone());
                continue;
            }
            out.insert(
                name.to_string(),
                Merged {
                    description: t["description"].as_str().unwrap_or("").to_string(),
                    schema: if t["inputSchema"].is_object() {
                        t["inputSchema"].clone()
                    } else {
                        json!({"type": "object", "properties": {}})
                    },
                    groups: vec![e.group.clone()],
                },
            );
        }
    }
    out
}

async fn tool_list(shared: &Arc<Shared>) -> Vec<Value> {
    let mut out = vec![json!({
        "name": NATIVE,
        "description": "What mentatd-serve can route right now: watched daemons, \
                        each group's health and endpoints, and the model table.",
        "inputSchema": {"type": "object", "properties": {}},
    })];
    for (name, m) in merge(shared).await {
        out.push(json!({
            "name": name,
            "description": m.description,
            "inputSchema": with_group_arg(&m),
        }));
    }
    out
}

/// The tool's own schema plus `__group`. The argument is required when more
/// than one group offers the tool, since picking one for the caller would
/// run a management action somewhere it did not request. With a single
/// group there is nothing to choose and the argument may be left out.
fn with_group_arg(m: &Merged) -> Value {
    let mut schema = m.schema.clone();
    let obj = schema.as_object_mut().expect("schema is an object");
    obj.entry("type").or_insert_with(|| json!("object"));
    {
        // A tool whose `properties` is missing or malformed still gets the
        // argument.
        let props = obj.entry("properties").or_insert_with(|| json!({}));
        if !props.is_object() {
            *props = json!({});
        }
        props.as_object_mut().expect("just set").insert(
            GROUP_ARG.to_string(),
            json!({
                "type": "string",
                "enum": m.groups,
                "description": format!("Which group runs this tool. Offered by: {}.",
                                       m.groups.join(", ")),
            }),
        );
    }
    if m.groups.len() > 1 {
        let mut req = obj
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        req.push(json!(GROUP_ARG));
        obj.insert("required".to_string(), Value::Array(req));
    }
    schema
}

/// One group's tool list, cached briefly. Read rather than assumed -- the
/// containers already differ in what they expose (the Ray tool is
/// conditional). An empty or failed answer is stored for the status page
/// but counts as a miss, so a container that was still booting is retried
/// on the next list instead of a minute later.
async fn group_tools(shared: &Arc<Shared>, group: &str, url: &str) -> Vec<Value> {
    let key = format!("{group} {url}");
    {
        let cache = shared.tools.lock().unwrap();
        if let Some((t, Ok(tools))) = cache.get(&key) {
            if !tools.is_empty() && t.elapsed() <= shared.cfg.tools_ttl {
                return tools.clone();
            }
        }
    }
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    let got = match http_post_json(
        &shared.client,
        url,
        &req,
        std::time::Duration::from_secs(10),
    )
    .await
    {
        Ok(v) => Ok(v["result"]["tools"].as_array().cloned().unwrap_or_default()),
        Err(e) => {
            log(
                "mcp_tools_fetch_failed",
                &[("group", group.to_string()), ("error", e.clone())],
            );
            Err(e)
        }
    };
    let tools = got.clone().unwrap_or_default();
    shared
        .tools
        .lock()
        .unwrap()
        .insert(key, (Instant::now(), got));
    tools
}

/// Each group's MCP endpoint for the status page, named by group. The page
/// reads the tools cache and never waits on a container. A group whose
/// entry is missing, stale or failed gets a background tools/list.
///
/// `tools` is null until a list has answered. `error` is the last failure.
pub fn page_view(shared: &Arc<Shared>) -> Vec<Value> {
    let mut out = Vec::new();
    for e in group_table(shared).values() {
        let Some(url) = e.mcp.as_ref().and_then(|m| m.best()) else {
            continue;
        };
        let key = format!("{} {url}", e.group);
        let cached = shared
            .tools
            .lock()
            .unwrap()
            .get(&key)
            .map(|(t, r)| (t.elapsed(), r.clone()));
        let fresh = matches!(&cached, Some((age, Ok(t)))
                             if !t.is_empty() && *age <= shared.cfg.tools_ttl);
        if !fresh && shared.tools_fetching.lock().unwrap().insert(key.clone()) {
            let shared = shared.clone();
            let group = e.group.clone();
            let url = url.to_string();
            tokio::spawn(async move {
                group_tools(&shared, &group, &url).await;
                shared.tools_fetching.lock().unwrap().remove(&key);
            });
        }
        let (tools, error) = match cached {
            Some((_, Ok(t))) => (
                json!(t
                    .iter()
                    .filter_map(|t| t["name"].as_str())
                    .collect::<Vec<_>>()),
                Value::Null,
            ),
            Some((_, Err(err))) => (Value::Null, json!(err)),
            None => (Value::Null, Value::Null),
        };
        out.push(json!({"group": e.group, "tools": tools, "error": error}));
    }
    out
}

fn tool_err(rid: &Value, msg: String) -> Value {
    // A tool error rides in the result, like the status server's, so the
    // model sees it and can recover.
    json!({"jsonrpc": "2.0", "id": rid,
           "result": {"content": [{"type": "text", "text": msg}], "isError": true}})
}

async fn call(shared: &Arc<Shared>, rid: Value, params: Value) -> Value {
    let name = params["name"].as_str().unwrap_or("");
    if name == NATIVE {
        let text =
            serde_json::to_string_pretty(&status_view(shared)).unwrap_or_else(|e| e.to_string());
        return json!({"jsonrpc": "2.0", "id": rid,
                      "result": {"content": [{"type": "text", "text": text}],
                                 "isError": false}});
    }
    // The same merge the listing was built from, so a name that was listed
    // resolves the same way here. The per-group lists are cached, so this
    // is local work after the first list.
    let merged = merge(shared).await;
    let Some(m) = merged.get(name) else {
        return tool_err(
            &rid,
            format!(
                "no tool {name:?}. Tools right now: {}",
                list_or_none(std::iter::once(NATIVE).chain(merged.keys().map(String::as_str)))
            ),
        );
    };

    // The group's own tool has no `__group` in its schema, so the argument
    // is stripped from what is forwarded.
    let mut args: Map<String, Value> = params["arguments"].as_object().cloned().unwrap_or_default();
    let asked = args.remove(GROUP_ARG);
    let group = match asked.as_ref().and_then(Value::as_str) {
        Some(g) if m.groups.iter().any(|have| have == g) => g.to_string(),
        Some(g) => {
            return tool_err(
                &rid,
                format!(
                    "group {g:?} does not offer {name:?}. Groups that do: {}",
                    list_or_none(m.groups.iter().map(String::as_str))
                ),
            )
        }
        // A non-string `__group` reads the same as none: either way the
        // caller has not named a group.
        None if m.groups.len() == 1 => m.groups[0].clone(),
        None => {
            return tool_err(
                &rid,
                format!(
                    "{name:?} runs in more than one group. Pass {GROUP_ARG:?}: {}",
                    list_or_none(m.groups.iter().map(String::as_str))
                ),
            )
        }
    };

    let table = group_table(shared);
    let Some(url) = table
        .get(&group)
        .and_then(|e| e.mcp.as_ref())
        .and_then(|m| m.best())
        .map(str::to_string)
    else {
        // Reachable only if the group left the table between the merge and
        // now, since the merge lists groups by their MCP endpoint.
        return tool_err(&rid, format!("group {group:?} has no MCP endpoint"));
    };
    let fwd = json!({"jsonrpc": "2.0", "id": rid, "method": "tools/call",
                     "params": {"name": name, "arguments": Value::Object(args)}});
    match http_post_json(&shared.client, &url, &fwd, shared.cfg.mcp_timeout).await {
        Ok(v) if v.get("result").is_some() || v.get("error").is_some() => v,
        Ok(v) => tool_err(
            &rid,
            format!("{group} replied with neither result nor error: {v}"),
        ),
        Err(e) => tool_err(&rid, format!("{group} did not answer: {e}")),
    }
}

/// Names for an error message. An empty list reads as "none", since a
/// caller reading "Groups that do: " cannot tell an empty list from a bug.
fn list_or_none<'a>(names: impl Iterator<Item = &'a str>) -> String {
    let joined = names.collect::<Vec<_>>().join(", ");
    if joined.is_empty() {
        "none".to_string()
    } else {
        joined
    }
}
