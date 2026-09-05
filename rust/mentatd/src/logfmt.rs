//! One-line structured logging. Everything mentat does that changes state
//! gets exactly one line here -- the whole point is that "why did rank 0 die"
//! is answerable from the container log.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn log(event: &str, fields: &[(&str, String)]) {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let secs = ms / 1000;
    let mut line = format!("mentat ts={}.{:03} event={}", secs, ms % 1000, event);
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
