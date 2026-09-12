//! Machine inventory for the agent: memory, cpus, and every GPU it may bind.
//!
//! Measured rather than configured. A count cannot describe a box with two
//! GPU models in it, and a UMA device (DGX Spark) shares the system pool, so
//! the figure a consumer needs is bytes per device.

use std::process::Command;

use crate::proto::{Gpu, Machine};

/// The product-name substring that marks NVIDIA's UMA parts. On those the
/// GPU addresses the system pool rather than its own board memory, so
/// `memory.total` is either the pool or unavailable.
const UMA_NAMES: &[&str] = &["GB10"];

/// What this box is.
///
/// `MENTAT_MACHINE=<json>` injects a whole inventory, for the heterogeneous
/// and UMA tests. `MENTAT_GPUS=<n>` yields n placeholder devices, which is
/// what the GPU-less CI and macOS runs use.
pub fn detect_machine() -> Machine {
    let memory = system_memory();
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);

    if let Ok(raw) = std::env::var("MENTAT_MACHINE") {
        match serde_json::from_str::<Machine>(&raw) {
            Ok(m) => return m,
            Err(e) => {
                // A machine that cannot be read would otherwise register as
                // a box with no devices, which reads as a scheduling bug
                // rather than a typo in one variable.
                eprintln!("mentatd: MENTAT_MACHINE is not a machine: {e}");
                std::process::exit(1);
            }
        }
    }
    if let Ok(n) = std::env::var("MENTAT_GPUS") {
        if let Ok(n) = n.trim().parse::<u32>() {
            return Machine {
                memory,
                cpus,
                gpus: (0..n)
                    .map(|index| Gpu {
                        index,
                        vendor: "nvidia".to_string(),
                        name: "fake".to_string(),
                        memory: 0,
                        uma: false,
                    })
                    .collect(),
            };
        }
    }
    Machine {
        memory,
        cpus,
        gpus: nvidia_gpus(memory),
    }
}

/// `MemTotal` from /proc/meminfo, in bytes. 0 where the file is absent,
/// which is every macOS dev box.
fn system_memory() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    text.lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

fn nvidia_gpus(system_memory: u64) -> Vec<Gpu> {
    let out = Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,name,memory.total",
            "--format=csv,noheader,nounits",
        ])
        .output();
    let text = match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
        _ => return Vec::new(),
    };
    text.lines().filter_map(|l| parse_gpu(l, system_memory)).collect()
}

/// One `index, name, memory.total` row. `memory.total` is MiB, or `[N/A]`
/// on a part with no board memory of its own.
fn parse_gpu(line: &str, system_memory: u64) -> Option<Gpu> {
    let mut f = line.split(',').map(str::trim);
    let index = f.next()?.parse::<u32>().ok()?;
    let name = f.next()?.to_string();
    let mib = f.next().unwrap_or_default();
    let uma = UMA_NAMES.iter().any(|u| name.contains(u)) || mib.parse::<u64>().is_err();
    // A UMA device addresses the system pool, so that is the figure to
    // report. A consumer adding it to the machine total counts it twice,
    // which is why `uma` rides alongside.
    let memory = if uma {
        system_memory
    } else {
        mib.parse::<u64>().ok()? * 1024 * 1024
    };
    Some(Gpu {
        index,
        vendor: "nvidia".to_string(),
        name,
        memory,
        uma,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A discrete card reports MiB, which becomes bytes.
    #[test]
    fn a_discrete_card_converts_mib_to_bytes() {
        let g = parse_gpu("0, NVIDIA RTX 6000 Ada Generation, 49140", 137_438_953_472).unwrap();
        assert_eq!(g.index, 0);
        assert_eq!(g.name, "NVIDIA RTX 6000 Ada Generation");
        assert_eq!(g.memory, 49140 * 1024 * 1024);
        assert!(!g.uma);
    }

    /// A DGX Spark reports the pool it shares with the host.
    #[test]
    fn a_uma_part_reports_the_system_pool() {
        let g = parse_gpu("0, NVIDIA GB10, 131072", 137_438_953_472).unwrap();
        assert!(g.uma);
        assert_eq!(g.memory, 137_438_953_472);
    }

    /// A part with no board memory of its own answers `[N/A]`, which is
    /// the other tell for UMA.
    #[test]
    fn an_unavailable_total_reads_as_uma() {
        let g = parse_gpu("1, Some Future Part, [N/A]", 137_438_953_472).unwrap();
        assert!(g.uma);
        assert_eq!(g.memory, 137_438_953_472);
    }

    /// Two models in one box, which is the case a count cannot describe.
    #[test]
    fn a_heterogeneous_box_keeps_each_device_distinct() {
        let rows = "0, NVIDIA RTX 6000 Ada Generation, 49140\n1, NVIDIA L40S, 46068";
        let gpus: Vec<Gpu> = rows.lines().filter_map(|l| parse_gpu(l, 0)).collect();
        assert_eq!(gpus.len(), 2);
        assert_ne!(gpus[0].name, gpus[1].name);
        assert_ne!(gpus[0].memory, gpus[1].memory);
    }

    #[test]
    fn a_malformed_row_is_skipped() {
        assert!(parse_gpu("not a row", 0).is_none());
        assert!(parse_gpu("", 0).is_none());
    }
}
