//! One-line structured logging. Everything mentat does that changes state
//! gets exactly one line here, so "why did rank 0 die" is answerable from
//! the container log.

use std::io::Write;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

static PROGRAM: OnceLock<&'static str> = OnceLock::new();

/// Label every line with `program`, so a merged log says which binary spoke.
/// The default is `mentat`. A second call changes nothing.
pub fn set_program(program: &'static str) {
    let _ = PROGRAM.set(program);
}

pub fn log(event: &str, fields: &[(&str, String)]) {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let secs = ms / 1000;
    let program = PROGRAM.get().copied().unwrap_or("mentat");
    let mut line = format!("{program} ts={}.{:03} event={}", secs, ms % 1000, event);
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
