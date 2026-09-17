//! The wire version both binaries use, and the rule for reading a peer's.
//!
//! The daemon and the router check each other's `proto`, so the constant and
//! the comparison live in one place. A second copy would drift, and the
//! symptom is a cluster that refuses links for a version it does in fact
//! understand.

/// The wire version this build uses, `major.minor`.
///
/// 0.99 is the 1.0 candidate: the shapes are 1.0's, and the number moves
/// once the spec is accepted.
pub const PROTO: &str = "0.99";

/// Whether `peer` shares this build's major.
///
/// A major bump changes a field's type or meaning, so a mismatch refuses the
/// link. A minor difference is compatible both ways, since a receiver drops
/// what it has no field for. A version outside `<digits>.<digits>` is
/// refused, which PROTOCOL.md states as the grammar and the shim applies in
/// Python.
pub fn major_matches(peer: &str) -> bool {
    fn major(v: &str) -> Option<&str> {
        let (maj, rest) = v.split_once('.')?;
        let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
        // Leading zeros go, so the comparison is on the number rather than
        // the spelling.
        (digits(maj) && digits(rest)).then(|| maj.trim_start_matches('0'))
    }
    match (major(PROTO), major(peer)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

pub fn proto() -> String {
    PROTO.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_major_mismatch_is_refused() {
        assert!(major_matches(PROTO));
        assert!(major_matches("0.1"));
        assert!(!major_matches("1.0"));
        assert!(!major_matches("nonsense"));
        assert!(!major_matches("1"));
        assert!(!major_matches("0.1.2"), "three parts");
        assert!(!major_matches("0abc.1"), "a major that is not digits");
        assert!(!major_matches("0."), "an empty minor");
        assert!(major_matches("00.1"), "leading zeros are the same number");
    }
}
