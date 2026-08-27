//! Announcement signing: HMAC-SHA256 over the canonical payload, keyed from
//! MENTAT_SECRET_FILE or MENTAT_SECRET.
//!
//! The scheme is spark-agent's mesh discovery, so one key serves both while
//! the fleet migrates: envelope `{"p": <payload>, "sig": <hex>}`, signature
//! over the payload's compact JSON with sorted keys. serde_json emits exactly
//! that for a `Value`, since its map is a BTreeMap and `to_string` adds no
//! whitespace. Both sides must serialize identically or every signature
//! fails, which is why the canonical form is one function rather than a
//! convention.
//!
//! `secret.rs` is duplicated in the mentatd-serve crate, like `logfmt.rs`.
//! The two binaries ship separately and share no library.

// The daemon signs and the router verifies, so each uses half of this file.
// Keeping the two copies identical matters more than trimming the unused
// half: a difference in canonical() would break every signature.
#![allow(dead_code)]

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Format version carried in `mentat_announce`. Unsigned announcements are
/// version 1, so a listener holding a key can refuse them by version alone.
pub const SIGNED_VERSION: u64 = 2;

/// A datagram older or newer than this is refused. Wide enough for clock
/// skew between cluster boxes, narrow enough that a captured packet stops
/// being useful quickly.
pub const CLOCK_SKEW_S: f64 = 30.0;

/// The mesh key, from a mounted file or the environment. The file wins, so a
/// secret need not appear in the process environment where `docker inspect`
/// and `/proc/<pid>/environ` expose it. An unreadable or empty source reads
/// as absent.
pub fn load() -> Option<Vec<u8>> {
    if let Ok(path) = std::env::var("MENTAT_SECRET_FILE") {
        let path = path.trim();
        if !path.is_empty() {
            let bytes = std::fs::read(path).ok()?;
            let key = trim_ascii(&bytes);
            return (!key.is_empty()).then(|| key.to_vec());
        }
    }
    let v = std::env::var("MENTAT_SECRET").ok()?;
    let key = trim_ascii(v.as_bytes());
    (!key.is_empty()).then(|| key.to_vec())
}

/// Trailing newlines are what a secret file almost always carries, and a key
/// that differs by one byte between nodes fails every verification with no
/// clue why.
fn trim_ascii(v: &[u8]) -> &[u8] {
    let start = v.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(0);
    let end = v
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(0);
    if start >= end {
        &[]
    } else {
        &v[start..end]
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// The bytes a signature covers.
///
/// The verifier re-serializes a payload it parsed, so every value in it must
/// survive a JSON round trip. Integers and strings do. `f64` does not:
/// serde_json writes 1787862155.6581013 and reads it back as
/// 1787862155.658101, which changes the bytes and fails the signature for
/// some values and not others. Keep floats out of anything signed.
fn canonical(payload: &Value) -> String {
    payload.to_string()
}

/// Wrap `payload` in a signed envelope, ready to send.
pub fn sign(payload: &Value, key: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac takes a key of any length");
    mac.update(canonical(payload).as_bytes());
    let sig = hex(&mac.finalize().into_bytes());
    serde_json::json!({ "p": payload, "sig": sig }).to_string()
}

/// The payload of a correctly signed envelope. Returns None for malformed
/// input and for a bad signature alike, since a caller can act on neither.
pub fn verify(raw: &[u8], key: &[u8]) -> Option<Value> {
    let env: Value = serde_json::from_slice(raw).ok()?;
    let payload = env.get("p")?;
    let want = unhex(env.get("sig")?.as_str()?)?;
    let mut mac = HmacSha256::new_from_slice(key).ok()?;
    mac.update(canonical(payload).as_bytes());
    // Constant-time inside the mac, so a wrong signature leaks no position.
    mac.verify_slice(&want).ok()?;
    Some(payload.clone())
}

/// The cluster this daemon belongs to, from MENTAT_UNIVERSE. Two clusters can
/// share a broadcast domain, and without this each would log the other's
/// announcements as a bad key forever, which teaches an operator to ignore the
/// one line that should mean something.
pub fn universe() -> String {
    std::env::var("MENTAT_UNIVERSE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

/// The universe an announcement claims, read without verifying it.
///
/// Deliberately unauthenticated: a listener needs this to decide whether the
/// datagram is even addressed to it, which it must do before spending a
/// signature check or writing a log line. The copy inside the signed payload
/// is the one that counts, so forging this field buys an attacker nothing
/// beyond silence that they already had by staying quiet.
pub fn peek_universe(raw: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(raw).ok()?;
    let payload = v.get("p").unwrap_or(&v);
    Some(payload.get("universe")?.as_str()?.to_string())
}

/// Whether `t` sits inside the accepted window around now.
pub fn fresh(t: f64, now: f64) -> bool {
    (now - t).abs() <= CLOCK_SKEW_S
}

/// Seconds since the epoch, as the announcement carries them.
pub fn now_s() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// A per-process identifier, so a restarted daemon's sequence numbers can
/// start over without the listener reading them as replay.
pub fn boot_id() -> String {
    // read_exact, never fs::read: /dev/urandom has no end, so reading to EOF
    // never returns.
    let mut buf = [0u8; 8];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        if f.read_exact(&mut buf).is_ok() {
            return hex(&buf);
        }
    }
    // Last resort: unique per process on one box, which is all this needs.
    format!("{:x}{:x}", std::process::id(), now_s() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let key = b"hunter2";
        let payload = serde_json::json!({"b": 2, "a": 1});
        let env = sign(&payload, key);
        assert_eq!(verify(env.as_bytes(), key), Some(payload));
    }

    #[test]
    fn wrong_key_fails() {
        let payload = serde_json::json!({"a": 1});
        let env = sign(&payload, b"one");
        assert_eq!(verify(env.as_bytes(), b"two"), None);
    }

    #[test]
    fn tampered_payload_fails() {
        let env = sign(&serde_json::json!({"http": "10.0.0.1:6380"}), b"k");
        let bad = env.replace("10.0.0.1", "10.0.0.9");
        assert_eq!(verify(bad.as_bytes(), b"k"), None);
    }

    #[test]
    fn unsigned_and_malformed_fail() {
        assert_eq!(verify(b"{\"http\":\"x\"}", b"k"), None);
        assert_eq!(verify(b"not json", b"k"), None);
        assert_eq!(verify(b"{\"p\":{},\"sig\":\"zz\"}", b"k"), None);
    }

    /// Key order in the source JSON must not change the signature, or two
    /// nodes building the same payload differently would reject each other.
    #[test]
    fn canonical_is_key_order_independent() {
        let a: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        assert_eq!(canonical(&a), canonical(&b));
    }

    #[test]
    fn trailing_newline_in_key_is_ignored() {
        assert_eq!(trim_ascii(b"  hunter2\n"), b"hunter2");
        assert_eq!(trim_ascii(b"\n\n"), b"");
    }

    #[test]
    fn peek_reads_universe_from_both_shapes() {
        let signed = sign(&serde_json::json!({"universe": "lab"}), b"k");
        assert_eq!(peek_universe(signed.as_bytes()).as_deref(), Some("lab"));
        assert_eq!(
            peek_universe(br#"{"universe":"lab"}"#).as_deref(),
            Some("lab")
        );
        assert_eq!(peek_universe(b"{}"), None);
    }

    /// A foreign universe is skipped before the key is consulted, so the two
    /// checks must stay independent.
    #[test]
    fn peek_does_not_need_the_key() {
        let signed = sign(&serde_json::json!({"universe": "lab"}), b"secret");
        assert_eq!(peek_universe(signed.as_bytes()).as_deref(), Some("lab"));
        assert_eq!(verify(signed.as_bytes(), b"wrong"), None);
    }

    /// The announcement as announce.rs builds it, through the path a
    /// listener actually takes: parse the envelope, re-serialize the payload,
    /// check the signature. A float field would fail this.
    #[test]
    fn announcement_survives_the_listener_path() {
        let payload = serde_json::json!({
            "mentat_announce": SIGNED_VERSION,
            "node_id": "6d656e7461743a3137322e31382e302e33",
            "control": "172.18.0.3:6379",
            "http": "172.18.0.3:6380",
            "universe": "lab",
            "boot_id": "6d6474919bbe7beb",
            "seq": 159u64,
            "t": 1787862155u64,
        });
        let env = sign(&payload, b"k");
        assert_eq!(verify(env.as_bytes(), b"k"), Some(payload));
    }

    /// Why `t` is an integer. Recorded because the failure is intermittent:
    /// it depends on the value, so a float passes in testing and fails in
    /// production every few seconds.
    #[test]
    fn floats_do_not_survive_the_round_trip() {
        let v = serde_json::json!({"t": 1787862155.6581013f64});
        let once = v.to_string();
        let twice = serde_json::from_str::<Value>(&once).unwrap().to_string();
        assert_ne!(once, twice);
        assert_eq!(verify(sign(&v, b"k").as_bytes(), b"k"), None);
    }

    #[test]
    fn freshness_window() {
        assert!(fresh(1000.0, 1000.0));
        assert!(fresh(1000.0, 1029.0));
        assert!(!fresh(1000.0, 1031.0));
        assert!(!fresh(1000.0, 969.0));
    }
}
