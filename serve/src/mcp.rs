//! The merged MCP endpoint: every group's management MCP behind one URL,
//! tool names prefixed `<group>__` so the identical tool sets of two model
//! containers cannot collide. The engine-health gate does not apply here:
//! the status server exists for when the engine is loading or wedged, so
//! any group with an alive agent announcing "mcp" is listed.

use std::sync::Arc;
use std::time::Instant;

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::{json, Value};

use crate::logfmt::log;
use crate::{
    full_body, group_table, http_post_json, json_response, status_view, BoxedBody, Shared,
};

/// Joins group and tool in merged names. Group names are compose service
/// names, which contain no `__`, so the first split is unambiguous.
const SEP: &str = "__";

/// Tool calls are small JSON. A bigger body is a client bug.
const MAX_BODY: usize = 16 * 1024 * 1024;

pub async fn handle(shared: &Arc<Shared>, req: Request<Incoming>) -> Response<BoxedBody> {
    let bytes = match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &json!({"error": format!("body over {MAX_BODY} bytes")}),
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

    // A client may batch requests in a list. Notifications get no entry.
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
                                    "result": {"tools": merged_tools(shared).await}})),
        "tools/call" => Some(call(shared, rid, req["params"].clone()).await),
        _ => Some(json!({"jsonrpc": "2.0", "id": rid,
                         "error": {"code": -32601, "message": format!("no method {method:?}")}})),
    }
}

async fn merged_tools(shared: &Arc<Shared>) -> Vec<Value> {
    let mut out = vec![json!({
        "name": "serve_status",
        "description": "What mentatd-serve can route right now: watched daemons, \
                        each group's health and endpoints, and the model table.",
        "inputSchema": {"type": "object", "properties": {}},
    })];
    for e in group_table(shared).values() {
        // Ungated, so there is no probe to choose among candidates with:
        // the best-ranked one is the answer. See Endpoint::best.
        let Some(url) = e.mcp.as_ref().and_then(|m| m.best()).map(str::to_string) else {
            continue;
        };
        for t in group_tools(shared, &e.group, &url).await {
            let name = t["name"].as_str().unwrap_or("?");
            let schema = if t["inputSchema"].is_object() {
                t["inputSchema"].clone()
            } else {
                json!({"type": "object", "properties": {}})
            };
            out.push(json!({
                "name": format!("{}{SEP}{}", e.group, name),
                "description": format!(
                    "{} (group {})",
                    t["description"].as_str().unwrap_or(""),
                    e.group
                ),
                "inputSchema": schema,
            }));
        }
    }
    out
}

/// One group's tool list, cached briefly. Asked rather than assumed -- the
/// containers already differ in what they expose (the Ray tool is
/// conditional). An empty answer is not cached, so a container that was
/// still booting is retried on the next list instead of a minute later.
async fn group_tools(shared: &Arc<Shared>, group: &str, url: &str) -> Vec<Value> {
    let key = format!("{group} {url}");
    {
        let cache = shared.tools.lock().unwrap();
        if let Some((t, tools)) = cache.get(&key) {
            if t.elapsed() <= shared.cfg.tools_ttl {
                return tools.clone();
            }
        }
    }
    let req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    let tools = match http_post_json(
        &shared.client,
        url,
        &req,
        std::time::Duration::from_secs(10),
    )
    .await
    {
        Ok(v) => v["result"]["tools"].as_array().cloned().unwrap_or_default(),
        Err(e) => {
            log(
                "mcp_tools_fetch_failed",
                &[("group", group.to_string()), ("error", e)],
            );
            Vec::new()
        }
    };
    if !tools.is_empty() {
        shared
            .tools
            .lock()
            .unwrap()
            .insert(key, (Instant::now(), tools.clone()));
    }
    tools
}

fn tool_err(rid: &Value, msg: String) -> Value {
    // A tool error rides in the result, like the status server's, so the
    // model sees it and can recover.
    json!({"jsonrpc": "2.0", "id": rid,
           "result": {"content": [{"type": "text", "text": msg}], "isError": true}})
}

async fn call(shared: &Arc<Shared>, rid: Value, params: Value) -> Value {
    let name = params["name"].as_str().unwrap_or("");
    if name == "serve_status" {
        let text =
            serde_json::to_string_pretty(&status_view(shared)).unwrap_or_else(|e| e.to_string());
        return json!({"jsonrpc": "2.0", "id": rid,
                      "result": {"content": [{"type": "text", "text": text}],
                                 "isError": false}});
    }
    let Some((group, tool)) = name.split_once(SEP) else {
        return tool_err(
            &rid,
            format!("unknown tool {name:?}; merged names are <group>{SEP}<tool>"),
        );
    };
    let table = group_table(shared);
    let Some(url) = table
        .get(group)
        .and_then(|e| e.mcp.as_ref())
        .and_then(|m| m.best())
        .map(str::to_string)
    else {
        return tool_err(
            &rid,
            format!(
                "no group {group:?} with an MCP endpoint; groups: {}",
                table
                    .values()
                    .filter(|e| e.mcp.is_some())
                    .map(|e| e.group.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    };
    // The group prefix is consumed here, so the container sees its own plain
    // tool name. Its internal node-to-node dispatch is untouched.
    let fwd = json!({"jsonrpc": "2.0", "id": rid, "method": "tools/call",
                     "params": {"name": tool, "arguments": params["arguments"]}});
    match http_post_json(&shared.client, &url, &fwd, shared.cfg.mcp_timeout).await {
        Ok(v) if v.get("result").is_some() || v.get("error").is_some() => v,
        Ok(v) => tool_err(&rid, format!("malformed reply from {group}: {v}")),
        Err(e) => tool_err(&rid, format!("{group} did not answer: {e}")),
    }
}
