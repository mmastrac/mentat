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

use crate::{json_response, model_table, not_ready, BoxedBody, Shared};

/// Prompt JSON runs megabytes at worst. Refusing bigger bodies keeps a
/// client bug from ballooning this process.
const MAX_BODY: usize = 128 * 1024 * 1024;

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
    let tail = tail.strip_prefix("/v1").unwrap_or(tail);
    let url = format!("{}{}", base.trim_end_matches('/'), tail);

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
    let up = match up.body(Full::new(bytes)) {
        Ok(r) => r,
        Err(e) => {
            return json_response(
                StatusCode::BAD_GATEWAY,
                &json!({"error": format!("building upstream request: {e}")}),
            )
        }
    };

    // The timeout bounds time to response headers. For a stream that is
    // roughly time-to-first-token (vLLM sends headers as the stream opens),
    // and for a non-streaming call it is the whole generation, which is why
    // the default is generation-sized. The streamed body itself is unbounded.
    // A client hangup closes the upstream connection too.
    let resp =
        match tokio::time::timeout(shared.cfg.serving_timeout, shared.client.request(up)).await {
            Err(_) => {
                return json_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    &json!({"error": format!(
                        "{group} gave no response within {:.0}s",
                        shared.cfg.serving_timeout.as_secs_f64()
                    )}),
                )
            }
            Ok(Err(e)) => {
                return json_response(
                    StatusCode::BAD_GATEWAY,
                    &json!({"error": format!("{group} upstream error: {e}")}),
                )
            }
            Ok(Ok(r)) => r,
        };

    let mut builder = Response::builder().status(resp.status());
    if let Some(ct) = resp.headers().get(CONTENT_TYPE) {
        builder = builder.header(CONTENT_TYPE, ct.clone());
    }
    match builder.body(
        resp.into_body()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .boxed(),
    ) {
        Ok(r) => r,
        Err(e) => json_response(
            StatusCode::BAD_GATEWAY,
            &json!({"error": format!("relaying upstream response: {e}")}),
        ),
    }
}
