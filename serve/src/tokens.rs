//! `POST /v1/responses/input_tokens`: how many prompt tokens an input would
//! cost, in OpenAI's shape, answered from the serving engine's own tokenizer.
//!
//! The router owns this route rather than proxying it. vLLM has no such
//! endpoint, and a request for it lands on the `/v1/responses/{response_id}`
//! pattern and comes back 405.
//!
//! Text goes to the group's `/tokenize` as a chat request, so the count
//! includes the chat template. That is most of the answer: "hello world" is
//! 2 tokens as a bare prompt and 14 as a one-message chat. Tools ride along
//! for the same reason -- the template renders them, so the engine counts
//! them exactly.
//!
//! Media is estimated instead. Passing an image part through makes vLLM
//! fetch the URL and fail the whole request, and the engine cannot price an
//! image it has not decoded anyway. The flat rates below stand in.
//!
//! Tools cost about one token more when the caller sends them straight to
//! the engine. serde_json sorts object keys and the chat template renders
//! the schema verbatim, so the router's copy tokenizes a little differently.
//! serde_json's `preserve_order` closes the gap and must not be enabled:
//! `secret::canonical` signs announcements by relying on that same sorting,
//! and turning it on here would reject every signed announcement the
//! daemons send.

use std::sync::Arc;

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::{json, Value};

use crate::{
    group_table, http_post_json, json_response, model_table, not_ready, BoxedBody, Shared,
};

/// Per image and per video, whatever their resolution or length.
///
/// Deliberately crude. The true cost depends on resolution, tiling and the
/// model's own patch size, none of which the router can see without
/// downloading the media and running the engine's preprocessor. A caller
/// sizing a request against a context window needs an answer that is close
/// and cheap, and one image priced within a few thousand tokens moves a
/// 262144-token budget by about 1%.
const IMAGE_TOKENS: u64 = 4_000;
const VIDEO_TOKENS: u64 = 40_000;

/// The only provider whose `/tokenize` this route knows how to drive.
const SUPPORTED: &str = "vllm";

/// An input is prompt-shaped, so it stays far under the proxy's limit.
const MAX_BODY: usize = 32 * 1024 * 1024;

pub async fn count(shared: &Arc<Shared>, req: Request<Incoming>) -> Response<BoxedBody> {
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
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "body is not JSON"}),
            )
        }
    };
    let Some(model) = payload["model"].as_str() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "no model field in request"}),
        );
    };

    let models = model_table(shared);
    let Some((group, base)) = models.get(model) else {
        let pending = not_ready(shared);
        if let Some(why) = pending.get(model) {
            return json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &json!({"error": format!("{model} is not ready: {why}")}),
            );
        }
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({
                "error": format!("no model {model:?} is being served"),
                "available": models.keys().collect::<Vec<_>>(),
            }),
        );
    };

    // Counting means knowing how the engine turns an input into tokens. The
    // router can only claim that for an engine it recognises, and a number
    // produced for one it does not would be a guess a caller cannot tell
    // from a measurement.
    let provider = group_table(shared)
        .get(group)
        .map(|e| e.provider.clone())
        .unwrap_or_default();
    if provider != SUPPORTED {
        let said = if provider.is_empty() {
            "announced no provider".to_string()
        } else {
            format!("announced provider {provider:?}")
        };
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!(
                "cannot count tokens for {model}: group {group} {said}, and only \
                 {SUPPORTED:?} is supported. Set MENTAT_MODEL_PROVIDER on the container."
            )}),
        );
    }

    let (messages, media) = split_input(&payload);
    let text = match messages.is_empty() {
        // Media alone still costs what the media costs. Skipping the call
        // here also keeps `/tokenize` from rejecting an empty message list.
        true => 0,
        false => {
            let mut req = json!({"model": model, "messages": messages});
            if let Some(tools) = payload.get("tools").filter(|t| t.is_array()) {
                req["tools"] = tools.clone();
            }
            let url = format!("{}/tokenize", tokenize_root(base));
            match http_post_json(&shared.client, &url, &req, shared.cfg.mcp_timeout).await {
                Ok(v) => match v["count"].as_u64() {
                    Some(n) => n,
                    None => {
                        return json_response(
                            StatusCode::BAD_GATEWAY,
                            &json!({"error": format!("{group} returned no count: {v}")}),
                        )
                    }
                },
                Err(e) => {
                    return json_response(
                        StatusCode::BAD_GATEWAY,
                        &json!({"error": format!("{group} could not tokenize: {e}")}),
                    )
                }
            }
        }
    };

    json_response(
        StatusCode::OK,
        &json!({"object": "response.input_tokens", "input_tokens": text + media}),
    )
}

/// `/tokenize` is root-level on vLLM while the announced base ends in `/v1`,
/// so the base has to climb out of it. Matches `proxy::upstream_url`.
fn tokenize_root(base: &str) -> String {
    let base = base.trim_end_matches('/');
    base.strip_suffix("/v1").unwrap_or(base).to_string()
}

/// The chat messages to price against the engine, and the token cost of the
/// media taken out of them.
///
/// `instructions` becomes a leading system message, which is where the
/// Responses API puts it, so the template charges for it the same way.
fn split_input(payload: &Value) -> (Vec<Value>, u64) {
    let mut messages = Vec::new();
    let mut media = 0;
    if let Some(s) = payload["instructions"].as_str().filter(|s| !s.is_empty()) {
        messages.push(json!({"role": "system", "content": s}));
    }
    match &payload["input"] {
        Value::String(s) => messages.push(json!({"role": "user", "content": s})),
        Value::Array(items) => {
            for item in items {
                if let Some(s) = item.as_str() {
                    messages.push(json!({"role": "user", "content": s}));
                    continue;
                }
                let (text, cost) = split_content(&item["content"]);
                media += cost;
                messages.push(json!({
                    "role": item["role"].as_str().unwrap_or("user"),
                    "content": text,
                }));
            }
        }
        _ => {}
    }
    (messages, media)
}

/// One message's content, flattened to text with its media priced out.
///
/// Content is a string or a list of parts. A part is spelled the Responses
/// API's way (`input_text`, `input_image`) or chat completions' way (`text`,
/// `image_url`), and clients mix them, so both are read.
///
/// A part of some other type contributes whatever `text` it carries and
/// nothing else, which undercounts an attachment this router cannot price.
fn split_content(content: &Value) -> (String, u64) {
    match content {
        Value::String(s) => (s.clone(), 0),
        Value::Array(parts) => {
            let (mut text, mut media) = (String::new(), 0);
            for p in parts {
                match p["type"].as_str().unwrap_or_default() {
                    "input_image" | "image_url" | "image" => media += IMAGE_TOKENS,
                    "input_video" | "video_url" | "video" => media += VIDEO_TOKENS,
                    _ => {
                        if let Some(s) = p["text"].as_str() {
                            text.push_str(s);
                        }
                    }
                }
            }
            (text, media)
        }
        _ => (String::new(), 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_string_input_is_one_user_message() {
        let (m, media) = split_input(&json!({"model": "m", "input": "hello world"}));
        assert_eq!(m, vec![json!({"role": "user", "content": "hello world"})]);
        assert_eq!(media, 0);
    }

    /// The Responses API carries the system prompt beside the input, and the
    /// template charges for it, so it has to reach the engine as a message.
    #[test]
    fn instructions_lead_as_a_system_message() {
        let (m, _) = split_input(&json!({"instructions": "Be terse.", "input": "hi"}));
        assert_eq!(m[0], json!({"role": "system", "content": "Be terse."}));
        assert_eq!(m[1], json!({"role": "user", "content": "hi"}));
    }

    /// Both part spellings, since clients mix them.
    #[test]
    fn either_spelling_of_a_part_is_read() {
        let responses = json!([
            {"type": "input_text", "text": "a"},
            {"type": "input_image", "image_url": "http://x/a.png"},
        ]);
        let chat = json!([
            {"type": "text", "text": "a"},
            {"type": "image_url", "image_url": {"url": "http://x/a.png"}},
        ]);
        assert_eq!(split_content(&responses), ("a".into(), IMAGE_TOKENS));
        assert_eq!(split_content(&chat), ("a".into(), IMAGE_TOKENS));
    }

    #[test]
    fn media_is_counted_per_part() {
        let (text, media) = split_content(&json!([
            {"type": "input_text", "text": "look: "},
            {"type": "input_image", "image_url": "a"},
            {"type": "input_image", "image_url": "b"},
            {"type": "input_video", "video_url": "c"},
        ]));
        assert_eq!(text, "look: ");
        assert_eq!(media, 2 * IMAGE_TOKENS + VIDEO_TOKENS);
    }

    /// An input of nothing but media still has to cost what the media costs,
    /// and it leaves a message whose content is empty rather than no message,
    /// so the template overhead is still charged.
    #[test]
    fn an_image_only_input_keeps_its_message() {
        let (m, media) = split_input(&json!({
            "input": [{"role": "user", "content": [{"type": "input_image", "image_url": "a"}]}]
        }));
        assert_eq!(m, vec![json!({"role": "user", "content": ""})]);
        assert_eq!(media, IMAGE_TOKENS);
    }

    /// vLLM serves /tokenize at the root while the announced base ends in
    /// /v1, so appending would ask for /v1/tokenize and get a 404.
    #[test]
    fn tokenize_climbs_out_of_v1() {
        assert_eq!(tokenize_root("http://h:8000/v1"), "http://h:8000");
        assert_eq!(tokenize_root("http://h:8000/v1/"), "http://h:8000");
        assert_eq!(tokenize_root("http://h:8000"), "http://h:8000");
    }
}
