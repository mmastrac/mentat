//! What both binaries share. The daemon signs announcements and the router
//! verifies them, and a difference in the canonical form would break every
//! signature, so the form lives in one place. The wire version and the rule
//! for reading a peer's are here for the same reason.

pub mod logfmt;
pub mod proto;
pub mod secret;
