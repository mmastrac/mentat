use std::process::Command;

use crate::proto::Machine;
use mentat_common::logfmt::log;

// The probe script holds the vendor detection. A part it does not know needs
// an edit there, and mentatd stays unchanged.
const PROBE: &str = "mentatd-probe-machine";

pub fn detect_machine() -> Machine {
    // `docker run -e MENTAT_MACHINE` exports the name with no value, which
    // is not a request to bypass the probe.
    if let Some(raw) = std::env::var("MENTAT_MACHINE")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
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
    match std::env::var_os("MENTAT_MACHINE_PROBE").filter(|p| !p.is_empty()) {
        Some(p) => Some(std::path::PathBuf::from(p)),
        None => crate::find_sibling(PROBE),
    }
}

fn parse(raw: &str, from: &str) -> Machine {
    match serde_json::from_str::<Machine>(raw.trim()) {
        Ok(m) => m,
        Err(e) => fatal(format!("{from} is not a machine: {e}: {}", raw.trim())),
    }
}

// Every probe failure stops the agent. An agent that registers no devices
// starts normally and fails minutes later as a scheduling error in another
// process.
fn fatal(why: String) -> ! {
    log("machine_probe_failed", &[("error", why.clone())]);
    eprintln!("mentatd: {why}");
    std::process::exit(1);
}
