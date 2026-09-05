//! What both binaries share. The daemon signs announcements and the router
//! verifies them, and a difference in the canonical form would break every
//! signature, so the form lives in one place.

pub mod logfmt;
pub mod secret;
