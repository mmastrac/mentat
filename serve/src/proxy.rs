//! The OpenAI-compatible route: pick the group by the request's model name,
//! forward, and hand the upstream body back as it streams. hyper forwards
//! the `Incoming` body frame by frame with backpressure, so time-to-first-
//! token is preserved across the hop.

use std::sync::Arc;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{json, Value};

use crate::ui::{Tracked, TrackedBody};
use crate::{json_response, model_table, not_ready, BoxedBody, Shared};

/// Prompt JSON runs megabytes at worst. Refusing bigger bodies keeps a
/// client bug from ballooning this process.
const MAX_BODY: usize = 128 * 1024 * 1024;

/// Where a request path lands on the upstream.
///
/// The announced base ends in `/v1`, since that is what an OpenAI client is
/// handed. vLLM serves some endpoints there and others at the root:
/// `/tokenize` and `/detokenize` are root-level, so concatenating them onto
/// the base would ask for `/v1/tokenize` and get a 404. A root-level path is
/// resolved against the base with its `/v1` removed.
///
/// A base that does not end in `/v1` is used as given, since then it is
/// already the root a caller meant.
fn upstream_url(base: &str, tail: &str) -> String {
    let base = base.trim_end_matches('/');
    match (base.strip_suffix("/v1"), tail.strip_prefix("/v1")) {
        (Some(_), Some(rest)) => format!("{base}{rest}"),
        (Some(root), None) => format!("{root}{tail}"),
        (None, _) => format!("{base}{tail}"),
    }
}

pub async fn forward(shared: &Arc<Shared>, req: Request<Incoming>) -> Response<BoxedBody> {
    let (parts, body) = req.into_parts();
    let bytes = match Limited::new(body, MAX_BODY).collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &json!({"error": format!("request body over {MAX_BODY} bytes")}),
            )
        }
    };
    let parsed: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "body is not JSON"}),
            )
        }
    };
    let Some(model) = parsed["model"].as_str() else {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "no model field in request"}),
        );
    };

    let models = model_table(shared);
    let Some((group, base)) = models.get(model) else {
        let pending = not_ready(shared);
        // A request naming a known-but-unready group (the group name doubles
        // as the served name on every current deployment) is a 503 with the
        // reason. A name nothing claims is a 404.
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
                "not_ready": pending,
            }),
        );
    };

    let tail = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| parts.uri.path());
    let url = upstream_url(base, tail);

    // Rebuilt per attempt rather than cloned, since the retry needs its own.
    let build_up = || {
        let mut up = Request::builder().method(Method::POST).uri(&url).header(
            CONTENT_TYPE,
            parts
                .headers
                .get(CONTENT_TYPE)
                .cloned()
                .unwrap_or_else(|| hyper::header::HeaderValue::from_static("application/json")),
        );
        if let Some(a) = parts.headers.get(ACCEPT) {
            up = up.header(ACCEPT, a.clone());
        }
        up.body(Full::new(bytes.clone())).map_err(|e| e.to_string())
    };

    // The timeout bounds time to response headers. For a stream that is
    // roughly time-to-first-token (vLLM sends headers as the stream opens),
    // and for a non-streaming call it is the whole generation, which is why
    // the default is generation-sized. The streamed body itself is unbounded.
    // A client hangup closes the upstream connection too.
    let up = match build_up() {
        Ok(r) => r,
        Err(e) => {
            return json_response(
                StatusCode::BAD_GATEWAY,
                &json!({"error": format!("building upstream request: {e}")}),
            )
        }
    };
    // Sent once. A retry would re-send work the upstream may already be
    // doing, and would double the wait on an engine that accepts a
    // connection and then never answers, which is a real failure here. A
    // client that fails fast can decide for itself; one inside a doubled
    // timeout can do nothing until it expires.
    let resp = match shared
        .client
        .send_once(up, shared.cfg.serving_timeout)
        .await
    {
        Ok(r) => r,
        Err(e) if e.starts_with("timeout after") => {
            return json_response(
                StatusCode::GATEWAY_TIMEOUT,
                &json!({"error": format!(
                    "{group} gave no response within {:.0}s",
                    shared.cfg.serving_timeout.as_secs_f64()
                )}),
            )
        }
        Err(e) => {
            return json_response(
                StatusCode::BAD_GATEWAY,
                &json!({"error": format!("{group} upstream error: {e}")}),
            )
        }
    };

    // Registered once the upstream has accepted the request, and dropped
    // with the body below, so a client hangup takes its row with it.
    let tracked = Tracked::new(
        shared,
        model,
        group,
        bytes.len(),
        parsed["stream"].as_bool().unwrap_or(false),
    );

    let mut builder = Response::builder().status(resp.status());
    if let Some(ct) = resp.headers().get(CONTENT_TYPE) {
        builder = builder.header(CONTENT_TYPE, ct.clone());
    }
    match builder.body(
        TrackedBody::wrap(
            resp.into_body()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                .boxed(),
            tracked,
        )
        .boxed(),
    ) {
        Ok(r) => r,
        Err(e) => json_response(
            StatusCode::BAD_GATEWAY,
            &json!({"error": format!("relaying upstream response: {e}")}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::upstream_url;

    /// vLLM splits its endpoints: chat and completions live under /v1, while
    /// tokenize and detokenize are at the root. The announced base is the /v1
    /// one, so the root-level paths have to climb out of it.
    #[test]
    fn root_and_v1_endpoints_both_resolve() {
        let base = "http://10.0.0.1:8000/v1";
        assert_eq!(
            upstream_url(base, "/v1/chat/completions"),
            "http://10.0.0.1:8000/v1/chat/completions"
        );
        assert_eq!(
            upstream_url(base, "/tokenize"),
            "http://10.0.0.1:8000/tokenize"
        );
        assert_eq!(
            upstream_url(base, "/detokenize"),
            "http://10.0.0.1:8000/detokenize"
        );
    }

    #[test]
    fn a_query_string_survives() {
        assert_eq!(
            upstream_url("http://h:8000/v1", "/v1/completions?stream=true"),
            "http://h:8000/v1/completions?stream=true"
        );
    }

    #[test]
    fn a_trailing_slash_does_not_double() {
        assert_eq!(
            upstream_url("http://h:8000/v1/", "/tokenize"),
            "http://h:8000/tokenize"
        );
    }

    /// A base announced without /v1 is already the root.
    #[test]
    fn a_base_without_v1_is_used_as_given() {
        assert_eq!(
            upstream_url("http://h:8000", "/v1/chat/completions"),
            "http://h:8000/v1/chat/completions"
        );
        assert_eq!(
            upstream_url("http://h:8000", "/tokenize"),
            "http://h:8000/tokenize"
        );
    }
}
