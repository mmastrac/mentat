//! This box's own networks, and the allowlist of source addresses the
//! router will act on.
//!
//! Both settle the same question from different sides: which addresses are
//! near enough to be worth connecting to. The interface list is the closest
//! thing to proof of reachability available without dialling an address, so
//! it decides candidate ranking and, by default, the allowlist too.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// One IP network: an address masked to a prefix length, held in the one
/// integer width both families fit into.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Net {
    net: u128,
    mask: u128,
    v6: bool,
}

fn key(addr: IpAddr) -> u128 {
    match addr {
        IpAddr::V4(a) => u32::from(a) as u128,
        IpAddr::V6(a) => u128::from(a),
    }
}

impl Net {
    /// `bits` beyond the family's width is clamped to it.
    fn new(addr: IpAddr, bits: u32) -> Net {
        let v6 = addr.is_ipv6();
        let width = if v6 { 128 } else { 32 };
        let bits = bits.min(width);
        // A shift by the full width is undefined, so /0 is spelled out.
        let mask = if bits == 0 {
            0
        } else {
            (!0u128 >> (128 - bits)) << (width - bits)
        };
        Net {
            net: key(addr) & mask,
            mask,
            v6,
        }
    }

    /// An address with the netmask the interface list reports beside it.
    fn masked(addr: IpAddr, mask: IpAddr) -> Net {
        Net {
            net: key(addr) & key(mask),
            mask: key(mask),
            v6: addr.is_ipv6(),
        }
    }

    fn contains(&self, addr: IpAddr) -> bool {
        self.v6 == addr.is_ipv6() && key(addr) & self.mask == self.net
    }
}

impl std::fmt::Display for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A netmask is contiguous, so its set bits are its prefix length.
        let bits = self.mask.count_ones();
        if self.v6 {
            write!(f, "{}/{bits}", Ipv6Addr::from(self.net))
        } else {
            write!(f, "{}/{bits}", Ipv4Addr::from(self.net as u32))
        }
    }
}

impl std::str::FromStr for Net {
    type Err = ();

    /// `10.0.0.0/22`, `fd00::/8`, or a bare address as a single host.
    fn from_str(s: &str) -> Result<Net, ()> {
        let (addr, bits) = match s.split_once('/') {
            Some((a, b)) => (a, Some(b.trim().parse::<u32>().map_err(|_| ())?)),
            None => (s, None),
        };
        let addr: IpAddr = addr.trim().parse().map_err(|_| ())?;
        let width = if addr.is_ipv6() { 128 } else { 32 };
        let bits = bits.unwrap_or(width);
        if bits > width {
            return Err(());
        }
        Ok(Net::new(addr, bits))
    }
}

/// The networks this box has an interface on. An address inside one of
/// these is on a wire we are attached to.
///
/// Read fresh rather than kept: a docker bridge appears, a fabric link
/// comes up, and a list read at boot would be wrong for the life of the
/// process.
pub fn local_nets() -> Vec<Net> {
    let Ok(ifaces) = getifaddrs::InterfaceFilter::new().v4().v6().get() else {
        return Vec::new();
    };
    ifaces
        .filter_map(|i| match (i.address.ip_addr(), i.address.netmask()) {
            (Some(a), Some(m)) if a.is_ipv4() == m.is_ipv4() => Some(Net::masked(a, m)),
            _ => None,
        })
        .collect()
}

pub fn on_local_net(addr: &str, local: &[Net]) -> bool {
    let Ok(ip) = addr.parse::<IpAddr>() else {
        return false;
    };
    local.iter().any(|n| n.contains(ip))
}

/// ALLOWED_SOURCES: the addresses this router will act on, whether as the
/// source of an announcement, a candidate address derived from one, or the
/// peer of an HTTP request.
pub struct Allow {
    /// The `local` entry: any address on a network this box has an
    /// interface on.
    local: bool,
    nets: Vec<Net>,
    /// Entries that parse as neither, kept as literal text prefixes. `172.`
    /// names a range no single CIDR does, and configs written before the
    /// CIDR form existed are all prefixes.
    prefixes: Vec<String>,
}

impl Allow {
    /// Comma-separated `local`, CIDR blocks, bare addresses, and text
    /// prefixes, in any mix.
    pub fn parse(s: &str) -> Allow {
        let mut allow = Allow {
            local: false,
            nets: Vec::new(),
            prefixes: Vec::new(),
        };
        for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            if entry.eq_ignore_ascii_case("local") {
                allow.local = true;
            } else if let Ok(n) = entry.parse::<Net>() {
                allow.nets.push(n);
            } else {
                allow.prefixes.push(entry.to_string());
            }
        }
        allow
    }

    /// Whether `addr` may be acted on, against a snapshot of this box's own
    /// networks. Callers holding one pass it; `permits_now` reads one.
    pub fn permits(&self, addr: &str, local: &[Net]) -> bool {
        if self.prefixes.iter().any(|p| addr.starts_with(p)) {
            return true;
        }
        let Ok(ip) = addr.parse::<IpAddr>() else {
            // A hostname matches text prefixes and nothing else.
            return false;
        };
        self.nets.iter().any(|n| n.contains(ip))
            || (self.local && local.iter().any(|n| n.contains(ip)))
    }

    /// `permits` for a caller with no interface snapshot in hand. The read
    /// is skipped unless `local` is configured.
    pub fn permits_now(&self, addr: &str) -> bool {
        let local = if self.local { local_nets() } else { Vec::new() };
        self.permits(addr, &local)
    }
}

/// How the entries parsed, for the log lines that name them. A typo lands
/// in `prefix:`, where it matches nothing and reports it.
impl std::fmt::Display for Allow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts: Vec<String> = Vec::new();
        if self.local {
            parts.push("local".to_string());
        }
        parts.extend(self.nets.iter().map(|n| n.to_string()));
        parts.extend(self.prefixes.iter().map(|p| format!("prefix:{p}")));
        write!(f, "{}", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net(s: &str) -> Net {
        s.parse().unwrap()
    }

    /// 10.0.0.0/24 and 192.168.1.0/24, as a two-homed box would report them.
    fn subnets() -> Vec<Net> {
        vec![net("10.0.0.0/24"), net("192.168.1.0/24")]
    }

    #[test]
    fn a_prefix_length_masks_the_address() {
        let n = net("10.4.5.6/22");
        assert!(n.contains("10.4.4.1".parse().unwrap()));
        assert!(n.contains("10.4.7.255".parse().unwrap()));
        assert!(!n.contains("10.4.8.1".parse().unwrap()));
        assert!(!n.contains("10.5.4.1".parse().unwrap()));
    }

    #[test]
    fn the_edge_prefix_lengths_hold() {
        assert!(net("0.0.0.0/0").contains("203.0.113.7".parse().unwrap()));
        assert!(net("10.0.0.1").contains("10.0.0.1".parse().unwrap()));
        assert!(!net("10.0.0.1").contains("10.0.0.2".parse().unwrap()));
        assert!(net("::/0").contains("2001:db8::1".parse().unwrap()));
        assert!(net("fd00::/8").contains("fd12::9".parse().unwrap()));
        assert!(!net("fd00::/8").contains("fe80::1".parse().unwrap()));
    }

    /// The two families share one integer width, so a v4 address must not
    /// fall inside a v6 network that happens to hold the same low bits.
    #[test]
    fn a_network_matches_its_own_family_only() {
        assert!(!net("::/0").contains("10.0.0.1".parse().unwrap()));
        assert!(!net("0.0.0.0/0").contains("::1".parse().unwrap()));
    }

    /// The log line an operator reads back. A typo parses as a prefix,
    /// which matches nothing, and reading `prefix:lcoal` reports why.
    #[test]
    fn the_parse_reads_back() {
        assert_eq!(
            Allow::parse("local, 10.100.0.5/22, 172.").to_string(),
            "local,10.100.0.0/22,prefix:172."
        );
        assert_eq!(Allow::parse("lcoal").to_string(), "prefix:lcoal");
    }

    #[test]
    fn nonsense_is_not_a_network() {
        assert!("10.0.0.0/33".parse::<Net>().is_err());
        assert!("10.0.0.0/x".parse::<Net>().is_err());
        assert!("10.0.0/22".parse::<Net>().is_err());
        assert!("not-an-ip".parse::<Net>().is_err());
    }

    /// The three forms mix. A cluster reached over more than one wire
    /// needs more than one of them.
    #[test]
    fn the_entry_forms_mix() {
        let a = Allow::parse("local, 10.100.0.0/22, 172.");
        let local = subnets();
        assert!(a.permits("10.0.0.7", &local), "local");
        assert!(a.permits("10.100.3.255", &local), "in the /22");
        assert!(!a.permits("10.100.4.1", &local), "past the /22");
        assert!(a.permits("172.17.0.2", &local), "text prefix");
        assert!(!a.permits("203.0.113.7", &local));
    }

    /// Without `local`, the interface list decides nothing: an operator who
    /// wrote the list out meant that list.
    #[test]
    fn local_is_only_consulted_when_configured() {
        let a = Allow::parse("192.168.1.0/24");
        assert!(!a.permits("10.0.0.7", &subnets()));
        assert!(a.permits("192.168.1.13", &subnets()));
    }

    /// The default is `local` alone, so the interface list has to hold at
    /// least loopback for a router to serve its own health check.
    #[test]
    fn the_default_allows_loopback() {
        let a = Allow::parse("local");
        assert!(a.permits_now("127.0.0.1"));
        assert!(!a.permits_now("203.0.113.7"));
    }

    /// The pre-CIDR form still means what it did. `127.` is a prefix.
    /// Matching it as an address would allow nothing.
    #[test]
    fn a_text_prefix_still_matches_as_text() {
        let a = Allow::parse("127.,10.100.0.");
        assert!(a.permits("127.0.0.1", &[]));
        assert!(a.permits("10.100.0.2", &[]));
        assert!(!a.permits("10.100.1.2", &[]));
        // Only a prefix can match a name, and this one does not.
        assert!(!a.permits("some-host", &[]));
    }
}
