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
/// link. A minor difference is compatible both ways, since a receiver keeps
/// the fields it knows and drops the rest. A version outside
/// `<digits>.<digits>` is refused. PROTOCOL.md gives that grammar and the
/// Python shim applies the same rule.
pub fn major_matches(peer: &str) -> bool {
    match (major(PROTO), major(peer)) {
        (Some(a), Some(b)) => a == b || (CUTOVER.contains(&a) && CUTOVER.contains(&b)),
        _ => false,
    }
}

/// The two majors that accept each other while 0.99 becomes 1.0.
///
/// Both ends of every link check the other's version, so a 1.0 build that
/// reads 0.99 covers half a rolling upgrade. The other half is a 0.99 build
/// that reads 1.0, and that half ships before 1.0 exists or the cutover is
/// a flag day on every cluster. Drop the pair in the release after 1.0.
const CUTOVER: [&str; 2] = ["0", "1"];

/// The major of a `major.minor` version, with leading zeros gone, so `00.1`
/// and `0.1` give one answer. None for anything outside that grammar.
fn major(v: &str) -> Option<&str> {
    let (maj, rest) = v.split_once('.')?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(maj) || !digits(rest) {
        return None;
    }
    let trimmed = maj.trim_start_matches('0');
    Some(if trimmed.is_empty() { "0" } else { trimmed })
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
        assert!(!major_matches("nonsense"));
        assert!(!major_matches("1"));
        assert!(!major_matches("0.1.2"), "three parts");
        assert!(!major_matches("0abc.1"), "a major that is not digits");
        assert!(!major_matches("0."), "an empty minor");
        assert!(major_matches("00.1"), "leading zeros are the same number");
        assert!(!major_matches("2.0"), "a major this build knows nothing of");
    }

    /// Every link checks both ways, so 0.99 has to read 1.0 for a cluster to
    /// upgrade in any order. Shipping that only in 1.0 would be too late.
    #[test]
    fn the_cutover_pair_reads_both_ways() {
        assert!(major_matches("1.0"), "0.99 reads a 1.0 peer");
        assert!(major_matches("1.7"), "and any 1.x");
        // The same rule from the other side, which is what 1.0 will run.
        let accepts = |mine: &str, peer: &str| match (major(mine), major(peer)) {
            (Some(a), Some(b)) => a == b || (CUTOVER.contains(&a) && CUTOVER.contains(&b)),
            _ => false,
        };
        assert!(accepts("1.0", "0.99"), "1.0 reads a 0.99 peer");
        assert!(!accepts("2.0", "1.0"), "the pair covers 0 and 1 alone");
    }
}
