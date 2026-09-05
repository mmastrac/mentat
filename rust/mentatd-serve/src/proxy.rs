//! The OpenAI-compatible route: pick the group by the request's model name,
//! forward, and hand the upstream body back as it streams. hyper forwards
//! the `Incoming` body frame by frame with backpressure, so time-to-first-
//! token is preserved across the hop.

use std::collections::BTreeMap;
use std::sync::Arc;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{json, Value};

use crate::ui::{Tracked, TrackedBody};
use crate::{json_response, model_table, not_ready, BoxedBody, Shared};

/// Sized for the largest legitimate body, which is an audio upload rather
/// than a prompt. Refusing bigger ones keeps a client bug from ballooning
/// this process.
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

/// What the router needs from a request body.
#[derive(Debug)]
struct Routed {
    model: String,
    stream: bool,
}

/// Read those two out of a body, whatever form it takes.
///
/// JSON carries both as top-level fields. The audio endpoints are
/// multipart/form-data instead, and spell the model as a text field beside
/// the upload. Either way the body is forwarded byte for byte: this reads it
/// to pick a route and changes nothing.
fn route_by(content_type: Option<&str>, body: &[u8]) -> Result<Routed, String> {
    if let Some(boundary) = boundary_of(content_type.unwrap_or_default()) {
        let fields = form_fields(body, boundary);
        let model = fields
            .get("model")
            .ok_or_else(|| "no model field in the form".to_string())?;
        return Ok(Routed {
            model: model.clone(),
            stream: fields.get("stream").is_some_and(|s| s == "true"),
        });
    }
    let parsed: Value = serde_json::from_slice(body)
        .map_err(|_| "body is neither JSON nor multipart/form-data".to_string())?;
    let model = parsed["model"]
        .as_str()
        .ok_or_else(|| "no model field in request".to_string())?;
    Ok(Routed {
        model: model.to_string(),
        stream: parsed["stream"].as_bool().unwrap_or(false),
    })
}

/// The boundary of a multipart/form-data content type.
///
/// A boundary may contain `=`, so the parameter splits at its first one and
/// the rest is the value. It may not contain `;`, which is what makes
/// splitting the parameters safe.
fn boundary_of(content_type: &str) -> Option<&str> {
    let (kind, params) = content_type.split_once(';')?;
    if !kind.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    params.split(';').find_map(|p| {
        let (k, v) = p.split_once('=')?;
        k.trim()
            .eq_ignore_ascii_case("boundary")
            .then(|| v.trim().trim_matches('"'))
    })
}

/// The text fields of a multipart/form-data body, by name.
///
/// The parts are walked rather than the body searched for a field name,
/// because an upload's bytes can spell anything, `name="model"` included.
/// A part carrying a filename is skipped without its value being touched.
fn form_fields(body: &[u8], boundary: &str) -> BTreeMap<String, String> {
    let delim = format!("--{boundary}");
    let mut out = BTreeMap::new();
    let (mut at, mut start) = (0usize, None);
    while let Some(i) = find(&body[at..], delim.as_bytes()) {
        let i = at + i;
        if let Some(s) = start {
            if let Some((name, value)) = text_part(&body[s..i]) {
                out.insert(name, value);
            }
        }
        at = i + delim.len();
        start = Some(at);
    }
    out
}

/// One part as (field name, value). None for a file part, and for anything
/// whose headers do not parse.
///
/// The part's value ends with the CRLF that belongs to the delimiter line
/// after it, so one is taken off.
fn text_part(part: &[u8]) -> Option<(String, String)> {
    let i = find(part, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&part[..i]).to_ascii_lowercase();
    if head.contains("filename=") {
        return None;
    }
    let name = head.split_once("name=\"")?.1.split_once('"')?.0.to_string();
    let value = &part[i + 4..];
    let value = value.strip_suffix(b"\r\n").unwrap_or(value);
    Some((name, String::from_utf8(value.to_vec()).ok()?))
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
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
    let content_type = parts
        .headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let routed = match route_by(content_type, &bytes) {
        Ok(r) => r,
        Err(e) => return json_response(StatusCode::BAD_REQUEST, &json!({"error": e})),
    };
    let model = routed.model.as_str();

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
                "error": format!("no group serves model {model:?}"),
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
                &json!({"error": format!("cannot build the upstream request: {e}")}),
            )
        }
    };
    // Sent once. A retry would re-send work the upstream may already be
    // doing, and would double the wait on an engine that accepts a
    // connection and then never answers, which is a real failure here. A
    // client that fails fast can decide for itself. One inside a doubled
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
                &json!({"error": format!("{group} did not answer: {e}")}),
            )
        }
    };

    // Registered once the upstream has accepted the request, and dropped
    // with the body below, so a client hangup takes its row with it.
    let tracked = Tracked::new(shared, model, group, bytes.len(), routed.stream);

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
            &json!({"error": format!("cannot relay the upstream response: {e}")}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{boundary_of, route_by, upstream_url};

    /// A transcription as the OpenAI client sends one: the file first, the
    /// model after it.
    fn whisper_body() -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(b"--BOUND\r\n");
        b.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"a.wav\"\r\n",
        );
        b.extend_from_slice(b"Content-Type: audio/wav\r\n\r\n");
        // Bytes that spell a field header, which is what a search of the body
        // rather than a walk of its parts would route on.
        b.extend_from_slice(b"RIFF\x00\x01name=\"model\"\r\n\r\nnot-a-model\x00");
        b.extend_from_slice(b"\r\n--BOUND\r\n");
        b.extend_from_slice(b"Content-Disposition: form-data; name=\"model\"\r\n\r\n");
        b.extend_from_slice(b"whisper\r\n");
        b.extend_from_slice(b"--BOUND--\r\n");
        b
    }

    /// The reported failure: a Whisper upload was refused before it reached
    /// the engine, because the router could only read a model name out of
    /// JSON.
    #[test]
    fn a_multipart_upload_routes_on_its_model_field() {
        let r = route_by(Some("multipart/form-data; boundary=BOUND"), &whisper_body())
            .expect("a form naming a model");
        assert_eq!(r.model, "whisper");
        assert!(!r.stream);
    }

    #[test]
    fn a_json_body_still_routes() {
        let r = route_by(
            Some("application/json"),
            br#"{"model": "glm53", "stream": true}"#,
        )
        .unwrap();
        assert_eq!(r.model, "glm53");
        assert!(r.stream);
    }

    /// Streaming is a form field on the audio endpoints, so the status page
    /// reads it the same way it reads the JSON one.
    #[test]
    fn a_form_can_ask_for_a_stream() {
        let body = b"--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nw\r\n\
                     --B\r\nContent-Disposition: form-data; name=\"stream\"\r\n\r\ntrue\r\n\
                     --B--\r\n";
        let r = route_by(Some("multipart/form-data; boundary=B"), body).unwrap();
        assert_eq!(r.model, "w");
        assert!(r.stream);
    }

    /// A form with no model is the same refusal as JSON with none, and says
    /// which form it read.
    #[test]
    fn a_form_without_a_model_is_named_as_a_form() {
        let body =
            b"--B\r\nContent-Disposition: form-data; name=\"language\"\r\n\r\nen\r\n--B--\r\n";
        let e = route_by(Some("multipart/form-data; boundary=B"), body).unwrap_err();
        assert!(e.contains("form"), "{e}");
    }

    #[test]
    fn a_body_of_neither_form_is_refused() {
        let e = route_by(Some("application/octet-stream"), b"\x00\x01").unwrap_err();
        assert!(e.contains("multipart/form-data"), "{e}");
    }

    /// Clients quote the boundary or leave it bare, and a boundary may
    /// contain `=` of its own.
    #[test]
    fn a_boundary_is_read_however_it_is_spelled() {
        assert_eq!(
            boundary_of("multipart/form-data; boundary=----WebKitFormBoundaryAbC"),
            Some("----WebKitFormBoundaryAbC")
        );
        assert_eq!(
            boundary_of("Multipart/Form-Data; charset=utf-8; BOUNDARY=\"a=b\""),
            Some("a=b")
        );
        assert_eq!(boundary_of("application/json"), None);
        assert_eq!(boundary_of("multipart/form-data"), None);
    }

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
