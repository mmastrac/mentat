//! What this box is, as `mentatd-probe-machine` reports it.
//!
//! The vendor knowledge lives in that script rather than here: a box with a
//! part the probe does not know is fixed by editing a file, not by building
//! a binary. This module runs it, reads one JSON object off its stdout, and
//! refuses to guess when it cannot.

use std::process::Command;

use crate::proto::Machine;
use mentat_common::logfmt::log;

/// The probe's name. Found beside this binary first, then on PATH, the same
/// way `mentatd <name>` resolves an external subcommand.
const PROBE: &str = "mentatd-probe-machine";

/// What this box is.
///
/// `MENTAT_MACHINE=<json>` bypasses the probe with a whole inventory, which
/// is how the heterogeneous and UMA tests describe a box they are not
/// running on. `MENTAT_MACHINE_PROBE=<path>` names a probe directly.
///
/// Every failure here exits. An agent that registers a box with no devices
/// reads as a scheduling bug minutes later, in a different process, and the
/// cause is one missing file.
pub fn detect_machine() -> Machine {
    if let Ok(raw) = std::env::var("MENTAT_MACHINE") {
        return parse(&raw, "MENTAT_MACHINE");
    }
    let probe = match probe_path() {
        Some(p) => p,
        None => fatal(format!(
            "no {PROBE} beside this binary or on PATH. Install it, or set \
             MENTAT_MACHINE to the inventory this box has"
        )),
    };
    let out = match Command::new(&probe).output() {
        Ok(o) => o,
        Err(e) => fatal(format!("{} did not run: {e}", probe.display())),
    };
    if !out.status.success() {
        fatal(format!(
            "{} exited {}: {}",
            probe.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let machine = parse(&String::from_utf8_lossy(&out.stdout), &probe.display().to_string());
    log(
        "machine_probed",
        &[
            ("probe", probe.display().to_string()),
            ("gpus", machine.gpus.len().to_string()),
            ("memory", machine.memory.to_string()),
        ],
    );
    machine
}

fn probe_path() -> Option<std::path::PathBuf> {
    if let Some(p) = std::env::var_os("MENTAT_MACHINE_PROBE") {
        return Some(std::path::PathBuf::from(p));
    }
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join(PROBE)));
    let path = std::env::var_os("PATH").unwrap_or_default();
    sibling
        .into_iter()
        .chain(std::env::split_paths(&path).map(|d| d.join(PROBE)))
        .find(|p| p.is_file())
}

fn parse(raw: &str, from: &str) -> Machine {
    match serde_json::from_str::<Machine>(raw.trim()) {
        Ok(m) => m,
        Err(e) => fatal(format!("{from} is not a machine: {e}: {}", raw.trim())),
    }
}

fn fatal(why: String) -> ! {
    log("machine_probe_failed", &[("error", why.clone())]);
    eprintln!("mentatd: {why}");
    std::process::exit(1);
}
