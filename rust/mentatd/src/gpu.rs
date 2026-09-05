//! GPU inventory for the agent. Counted, not hardcoded -- costs nothing and
//! keeps TP > 2 / multi-GPU nodes honest.

use std::process::Command;

/// GPU indices this node offers. `MENTAT_GPUS=<n>` overrides for GPU-less
/// testing (macOS, CI); otherwise ask nvidia-smi, which every box that can
/// serve a model already has.
pub fn detect_gpus() -> Vec<u32> {
    if let Ok(n) = std::env::var("MENTAT_GPUS") {
        if let Ok(n) = n.trim().parse::<u32>() {
            return (0..n).collect();
        }
    }
    let out = Command::new("nvidia-smi")
        .args(["--query-gpu=index", "--format=csv,noheader"])
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
            .collect(),
        _ => Vec::new(),
    }
}
