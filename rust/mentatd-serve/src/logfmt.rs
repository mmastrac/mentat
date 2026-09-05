//! Same one-line structured logging as the daemon's logfmt.rs, prefixed
//! `mentatd-serve` so a merged log still says which binary spoke.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn log(event: &str, fields: &[(&str, String)]) {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let secs = ms / 1000;
    let mut line = format!("mentatd-serve ts={}.{:03} event={}", secs, ms % 1000, event);
    for (k, v) in fields {
        if v.contains(' ') || v.contains('"') {
            line.push_str(&format!(" {}={:?}", k, v));
        } else {
            line.push_str(&format!(" {}={}", k, v));
        }
    }
    let mut out = std::io::stderr().lock();
    let _ = writeln!(out, "{line}");
}
