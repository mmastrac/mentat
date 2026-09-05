//! The status page and the numbers behind it.
//!
//! The engine knows its own queue depth, KV usage and token totals, and
//! publishes them on `/metrics`. It has no endpoint for individual requests.
//! The router knows each request it is carrying right now -- when it arrived,
//! how big it was, whether the first byte has come back -- and knows nothing
//! about what the engine is doing with it. The page shows both side by side.
//!
//! `GET /` answers HTML to a browser and the existing status document to
//! everything else, chosen by `Accept`. Nothing that scripts against `/`,
//! `/healthz` or `/status.json` sees a change.

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use hyper::body::{Body, Bytes, Frame};
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{Response, StatusCode};
use serde_json::{json, Value};

use crate::{
    endpoint_url, full_body, group_table, health_of, model_ids, BoxedBody, HttpClients, Shared,
};

/// A scrape is 60-odd KiB of Prometheus text and the page polls every couple
/// of seconds, so one scrape is shared across whatever asks for it inside
/// this window. Several open tabs then cost what one costs.
const METRICS_TTL_MS: u128 = 1_000;

/// One request the router is carrying, as the router sees it.
///
/// The engine's own view of the same request stays separate from this one.
/// vLLM's metrics are aggregates, so there is no id to join on.
pub struct Inflight {
    pub model: String,
    pub group: String,
    pub started: Instant,
    /// The request body as received. Bytes rather than tokens, since
    /// counting tokens would mean a round trip per request.
    pub prompt_bytes: usize,
    pub stream: bool,
    /// When the upstream's first body byte reached the router. None while
    /// the engine is still queueing or prefilling.
    pub first_byte: Option<Instant>,
    pub out_bytes: u64,
}

/// Registers a request for the life of its response body and removes it on
/// the way out, however that happens.
///
/// The drop is what makes the table honest. A client that hangs up mid-stream
/// takes the response body with it, so this runs then too, and a cancelled
/// request leaves no row behind.
pub struct Tracked {
    shared: Arc<Shared>,
    id: u64,
}

impl Tracked {
    pub fn new(
        shared: &Arc<Shared>,
        model: &str,
        group: &str,
        prompt_bytes: usize,
        stream: bool,
    ) -> Tracked {
        let id = shared.next_req.fetch_add(1, Ordering::Relaxed);
        shared.inflight.lock().unwrap().insert(
            id,
            Inflight {
                model: model.to_string(),
                group: group.to_string(),
                started: Instant::now(),
                prompt_bytes,
                stream,
                first_byte: None,
                out_bytes: 0,
            },
        );
        Tracked {
            shared: shared.clone(),
            id,
        }
    }

    fn saw(&self, n: usize) {
        if let Some(r) = self.shared.inflight.lock().unwrap().get_mut(&self.id) {
            r.first_byte.get_or_insert_with(Instant::now);
            r.out_bytes += n as u64;
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.shared.inflight.lock().unwrap().remove(&self.id);
    }
}

/// The upstream body, counted on its way to the client.
///
/// Frames pass through untouched and one at a time, so the pass-through
/// stays frame-for-frame and time to first token is unaffected.
pub struct TrackedBody {
    inner: BoxedBody,
    tracked: Tracked,
}

impl TrackedBody {
    pub fn wrap(inner: BoxedBody, tracked: Tracked) -> TrackedBody {
        TrackedBody { inner, tracked }
    }
}

impl Body for TrackedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(f))) = &polled {
            if let Some(d) = f.data_ref() {
                this.tracked.saw(d.len());
            }
        }
        polled
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// One Prometheus series that carries a `model_name`, reduced to the parts
/// the page uses.
///
/// A full parser is not needed. Only a handful of names are read, so a line
/// whose name is not one of them is skipped before its labels are looked at.
fn scrape(text: &str, want: &[&str]) -> Vec<(String, String, String, f64)> {
    let mut out = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix("vllm:") else {
            continue;
        };
        let (name, tail) = match rest.find(['{', ' ']) {
            Some(i) => (&rest[..i], &rest[i..]),
            None => continue,
        };
        if !want.contains(&name) {
            continue;
        }
        let (labels, value) = match tail.strip_prefix('{').and_then(|l| l.split_once('}')) {
            Some((l, v)) => (l, v),
            None => ("", tail),
        };
        let Ok(v) = value.trim().parse::<f64>() else {
            continue;
        };
        out.push((
            name.to_string(),
            label(labels, "model_name"),
            label(labels, "finished_reason"),
            v,
        ));
    }
    out
}

/// One label's value out of a Prometheus label list, empty when absent.
fn label(labels: &str, key: &str) -> String {
    for part in labels.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            if k.trim() == key {
                return v.trim().trim_matches('"').to_string();
            }
        }
    }
    String::new()
}

/// The series the page reads. Everything else in a 60 KiB scrape is skipped.
const WANTED: &[&str] = &[
    "num_requests_running",
    "num_requests_waiting",
    "kv_cache_usage_perc",
    "prompt_tokens_total",
    "generation_tokens_total",
    "num_preemptions_total",
    "request_success_total",
    "time_to_first_token_seconds_sum",
    "time_to_first_token_seconds_count",
    "request_queue_time_seconds_sum",
    "request_queue_time_seconds_count",
    "inter_token_latency_seconds_sum",
    "inter_token_latency_seconds_count",
];

/// Engine counters per served model name, from one group's `/metrics`.
fn engine_stats(text: &str) -> std::collections::BTreeMap<String, Value> {
    let mut by_model: std::collections::BTreeMap<String, Value> = Default::default();
    for (name, model, reason, v) in scrape(text, WANTED) {
        let e = by_model.entry(model).or_insert_with(|| json!({}));
        if name == "request_success_total" {
            if v > 0.0 {
                e["finished"][reason] = json!(v);
            }
            continue;
        }
        e[name] = json!(v);
    }
    // A histogram is published as a sum and a count. A mean fits one
    // column and shows whether a queue is backing up.
    for e in by_model.values_mut() {
        for (mean, sum, count) in [
            (
                "ttft_s",
                "time_to_first_token_seconds",
                "time_to_first_token_seconds",
            ),
            (
                "queue_s",
                "request_queue_time_seconds",
                "request_queue_time_seconds",
            ),
            (
                "itl_s",
                "inter_token_latency_seconds",
                "inter_token_latency_seconds",
            ),
        ] {
            let s = e[format!("{sum}_sum")].as_f64().unwrap_or(0.0);
            let c = e[format!("{count}_count")].as_f64().unwrap_or(0.0);
            if c > 0.0 {
                e[mean] = json!(s / c);
            }
        }
    }
    by_model
}

async fn metrics_for(shared: &Arc<Shared>, group: &str, base: &str) -> Option<String> {
    {
        let cache = shared.metrics.lock().unwrap();
        if let Some((at, text)) = cache.get(group) {
            if at.elapsed().as_millis() <= METRICS_TTL_MS {
                return Some(text.clone());
            }
        }
    }
    let root = base.trim_end_matches('/');
    let root = root.strip_suffix("/v1").unwrap_or(root);
    let text = fetch_text(
        &shared.client,
        &format!("{root}/metrics"),
        shared.cfg.probe_timeout,
    )
    .await
    .ok()?;
    shared
        .metrics
        .lock()
        .unwrap()
        .insert(group.to_string(), (Instant::now(), text.clone()));
    Some(text)
}

async fn fetch_text(
    client: &HttpClients,
    url: &str,
    t: std::time::Duration,
) -> Result<String, String> {
    use http_body_util::BodyExt;
    let req = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri(url)
        .body(http_body_util::Full::new(Bytes::new()))
        .map_err(|e| e.to_string())?;
    let resp = client.send_once(req, t).await?;
    let body = tokio::time::timeout(t, resp.into_body().collect())
        .await
        .map_err(|_| "timeout reading metrics".to_string())?
        .map_err(|e| e.to_string())?
        .to_bytes();
    String::from_utf8(body.to_vec()).map_err(|e| e.to_string())
}

/// What the page polls: one row per served model, plus every request the
/// router is carrying.
pub async fn stats(shared: &Arc<Shared>) -> Value {
    let mut rows = Vec::new();
    let mut counts: std::collections::HashMap<String, u64> = Default::default();
    for r in shared.inflight.lock().unwrap().values() {
        *counts.entry(r.model.clone()).or_default() += 1;
    }

    for e in group_table(shared).values() {
        let health = health_of(shared, e);
        let names = health.as_ref().map(|m| model_ids(m)).unwrap_or_default();
        let base = endpoint_url(shared, e).unwrap_or_default();
        let engine = match health.is_ok() {
            false => Default::default(),
            true => match metrics_for(shared, &e.group, &base).await {
                Some(text) => engine_stats(&text),
                None => Default::default(),
            },
        };
        for name in names {
            let m = engine.get(&name).cloned().unwrap_or_else(|| json!({}));
            rows.push(json!({
                "model": name,
                "group": e.group,
                "provider": e.provider,
                "healthy": health.is_ok(),
                "why_not": health.as_ref().err(),
                "inflight": counts.get(&name).copied().unwrap_or(0),
                "running": m["num_requests_running"],
                "waiting": m["num_requests_waiting"],
                "kv": m["kv_cache_usage_perc"],
                "prompt_tokens": m["prompt_tokens_total"],
                "generation_tokens": m["generation_tokens_total"],
                "preemptions": m["num_preemptions_total"],
                "ttft_s": m["ttft_s"],
                "queue_s": m["queue_s"],
                "itl_s": m["itl_s"],
                "finished": m["finished"],
            }));
        }
        // A group with no healthy model still belongs on the page, since
        // "why did it stop serving" is the question being asked.
        if health.is_err() {
            rows.push(json!({
                "model": e.group,
                "group": e.group,
                "provider": e.provider,
                "healthy": false,
                "why_not": health.as_ref().err(),
                "inflight": 0,
            }));
        }
    }

    let requests: Vec<Value> = shared
        .inflight
        .lock()
        .unwrap()
        .iter()
        .map(|(id, r)| {
            json!({
                "id": id,
                "model": r.model,
                "group": r.group,
                "age_s": r.started.elapsed().as_secs_f64(),
                "prompt_bytes": r.prompt_bytes,
                "stream": r.stream,
                "ttfb_s": r.first_byte.map(|t| (t - r.started).as_secs_f64()),
                "out_bytes": r.out_bytes,
            })
        })
        .collect();

    json!({
        "uptime_s": shared.started.elapsed().as_secs(),
        "models": rows,
        "requests": requests,
    })
}

/// True when the caller looks like a browser, so `/` can stay JSON for
/// everything that already reads it.
pub fn wants_html(headers: &hyper::HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|a| a.contains("text/html"))
        .unwrap_or(false)
}

pub fn page() -> Response<BoxedBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/html; charset=utf-8")
        .body(full_body(PAGE))
        .expect("static response")
}

const PAGE: &str = r#"<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>mentatd-serve</title>
<style>
:root { color-scheme: light dark }
body { font: 13px ui-monospace, monospace; margin: 1rem }
table { border-collapse: collapse; margin-bottom: 1rem }
th, td { border: 1px solid; padding: 2px 8px; text-align: right }
th:first-child, td:first-child { text-align: left }
caption { text-align: left; font-weight: bold; padding-bottom: 4px }
tbody tr { cursor: pointer }
tbody tr[aria-selected=true] { font-weight: bold }
a { color: inherit }
.dead { opacity: .6 }
</style>
<h1>mentatd-serve</h1>
<p id="head"></p>
<div id="models"></div>
<div id="focus"></div>
<script>
let sel = new URLSearchParams(location.search).get("model");

const n = (v, d = 0) => (v === null || v === undefined) ? "-" : Number(v).toFixed(d);
const bytes = b => b < 1024 ? b + "B" : b < 1048576 ? (b / 1024).toFixed(1) + "K" : (b / 1048576).toFixed(1) + "M";
const esc = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

function table(caption, cols, rows, opts = {}) {
  if (!rows.length) return "<table><caption>" + caption + "</caption><tr><td>none</td></tr></table>";
  let h = "<table><caption>" + caption + "</caption><thead><tr>" +
    cols.map(c => "<th>" + c + "</th>").join("") + "</tr></thead><tbody>";
  for (const r of rows) {
    const attrs = (opts.key ? ' data-model="' + esc(opts.key(r)) + '"' : "") +
      (opts.selected && opts.selected(r) ? ' aria-selected="true"' : "") +
      (opts.dead && opts.dead(r) ? ' class="dead"' : "");
    h += "<tr" + attrs + ">" + r.cells.map(c => "<td>" + c + "</td>").join("") + "</tr>";
  }
  return h + "</tbody></table>";
}

// Replacing innerHTML drops any text selection inside it, so an unchanged
// table is left alone. A status page is mostly idle, and the numbers people
// want to copy are the ones that are not moving.
function put(id, html) {
  const el = document.getElementById(id);
  if (el.innerHTML !== html) el.innerHTML = html;
}

function render(d) {
  document.getElementById("head").textContent =
    "uptime " + d.uptime_s + "s · " + d.models.length + " models · " +
    d.requests.length + " requests in flight";

  const rows = d.models.map(m => ({
    model: m.model, healthy: m.healthy,
    cells: [
      esc(m.model) + (m.healthy ? "" : " (" + esc(m.why_not || "down") + ")"),
      esc(m.group), m.inflight, n(m.running), n(m.waiting),
      m.kv === undefined || m.kv === null ? "-" : n(m.kv * 100, 1) + "%",
      n(m.prompt_tokens), n(m.generation_tokens),
      n(m.ttft_s, 2), n(m.queue_s, 2),
      m.itl_s === undefined || m.itl_s === null ? "-" : n(m.itl_s * 1000, 1),
      n(m.preemptions),
    ],
  }));
  put("models", table(
    "models (click to focus)",
    ["model", "group", "proxied", "running", "waiting", "kv", "prompt tok", "gen tok", "ttft s", "queue s", "itl ms", "preempt"],
    rows,
    { key: r => r.model, selected: r => r.model === sel, dead: r => !r.healthy }));

  let h = "";
  if (sel) {
    const m = d.models.find(x => x.model === sel);
    const mine = d.requests.filter(r => r.model === sel);
    h += "<h2>" + esc(sel) + " <a href='?'>(clear)</a></h2>";
    if (m) {
      const f = m.finished || {};
      h += table("engine", ["metric", "value"], [
        { cells: ["running", n(m.running)] },
        { cells: ["waiting in queue", n(m.waiting)] },
        { cells: ["kv cache", m.kv == null ? "-" : n(m.kv * 100, 1) + "%"] },
        { cells: ["mean ttft", n(m.ttft_s, 2) + " s"] },
        { cells: ["mean queue wait", n(m.queue_s, 2) + " s"] },
        { cells: ["mean inter-token", m.itl_s == null ? "-" : n(m.itl_s * 1000, 1) + " ms"] },
        { cells: ["prompt tokens", n(m.prompt_tokens)] },
        { cells: ["generation tokens", n(m.generation_tokens)] },
        { cells: ["preemptions", n(m.preemptions)] },
      ].concat(Object.keys(f).map(k => ({ cells: ["finished: " + esc(k), n(f[k])] }))));
    }
    h += table("in flight through the router", ["id", "req", "waiting", "ttfb", "returned", "stream"],
      mine.map(r => ({
        cells: [r.id, bytes(r.prompt_bytes),
          r.ttfb_s == null ? n(r.age_s, 1) + " s" : "-",
          r.ttfb_s == null ? "-" : n(r.ttfb_s, 2) + " s",
          bytes(r.out_bytes), r.stream ? "yes" : "no"],
      })));
  }
  put("focus", h);
}

document.addEventListener("click", e => {
  const tr = e.target.closest("tr[data-model]");
  if (!tr) return;
  sel = tr.dataset.model === sel ? null : tr.dataset.model;
  history.replaceState(null, "", sel ? "?model=" + encodeURIComponent(sel) : location.pathname);
  tick();
});

async function tick() {
  try {
    render(await (await fetch("stats.json")).json());
  } catch (e) {
    document.getElementById("head").textContent = "router unreachable: " + e;
  }
}
tick();
setInterval(tick, 2000);
</script>
"#;

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="glm53-exl3"} 3.0
vllm:num_requests_waiting{engine="0",model_name="glm53-exl3"} 2.0
vllm:kv_cache_usage_perc{engine="0",model_name="glm53-exl3"} 0.5
vllm:prompt_tokens_total{engine="0",model_name="glm53-exl3"} 4.5476058e+07
vllm:generation_tokens_total{engine="0",model_name="glm53-exl3"} 210549.0
vllm:request_success_total{engine="0",finished_reason="stop",model_name="glm53-exl3"} 170.0
vllm:request_success_total{engine="0",finished_reason="abort",model_name="glm53-exl3"} 0.0
vllm:request_queue_time_seconds_sum{engine="0",model_name="glm53-exl3"} 1911.33
vllm:request_queue_time_seconds_count{engine="0",model_name="glm53-exl3"} 231.0
vllm:spec_decode_num_drafts_total{engine="0",model_name="glm53-exl3"} 9.0
python_gc_objects_collected_total{generation="0"} 1234.0
"#;

    #[test]
    fn engine_counters_come_off_a_scrape() {
        let s = engine_stats(SAMPLE);
        let m = &s["glm53-exl3"];
        assert_eq!(m["num_requests_running"], 3.0);
        assert_eq!(m["num_requests_waiting"], 2.0);
        assert_eq!(m["prompt_tokens_total"], 45476058.0);
    }

    /// A histogram arrives as a sum and a count. The page shows the mean.
    #[test]
    fn a_histogram_becomes_its_mean() {
        let s = engine_stats(SAMPLE);
        let q = s["glm53-exl3"]["queue_s"].as_f64().unwrap();
        assert!((q - 1911.33 / 231.0).abs() < 1e-9, "got {q}");
    }

    /// Reasons that never happened would be a column of zeroes.
    #[test]
    fn only_finish_reasons_that_happened_are_kept() {
        let f = &engine_stats(SAMPLE)["glm53-exl3"]["finished"];
        assert_eq!(f["stop"], 170.0);
        assert!(f["abort"].is_null());
    }

    /// A scrape carries hundreds of series this page never shows, and
    /// process-level ones that are not even vllm's.
    #[test]
    fn unasked_series_are_skipped() {
        let s = engine_stats(SAMPLE);
        assert!(s["glm53-exl3"]["spec_decode_num_drafts_total"].is_null());
        assert_eq!(s.len(), 1, "no non-vllm series became a model");
    }

    #[test]
    fn labels_are_read_by_name() {
        let l = r#"engine="0",finished_reason="stop",model_name="glm53-exl3""#;
        assert_eq!(label(l, "model_name"), "glm53-exl3");
        assert_eq!(label(l, "finished_reason"), "stop");
        assert_eq!(label(l, "absent"), "");
    }

    /// curl and the status pollers must keep getting JSON from `/`.
    #[test]
    fn only_a_browser_gets_html() {
        let mut h = hyper::HeaderMap::new();
        assert!(!wants_html(&h), "no Accept at all");
        h.insert(ACCEPT, "*/*".parse().unwrap());
        assert!(!wants_html(&h), "curl");
        h.insert(ACCEPT, "application/json".parse().unwrap());
        assert!(!wants_html(&h));
        h.insert(
            ACCEPT,
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                .parse()
                .unwrap(),
        );
        assert!(wants_html(&h), "browser");
    }
}
