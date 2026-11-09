//! Address helpers for the daemon's listener: CIDR ranges and loopback detection.
//!
//! `[server] trusted_proxies` is a list of CIDR ranges (`10.0.0.0/8`, `fd00::/8`, or a bare
//! address meaning a single host). A request whose socket peer falls in one of them may name
//! the real client in `X-Forwarded-For`; anyone else's forwarding headers are ignored. A few
//! lines of bit masking rather than a dependency: the matcher is all the daemon needs.

use std::net::IpAddr;

/// One CIDR range, IPv4 or IPv6, with its host bits cleared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    network: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// Parse `addr/prefix` or a bare address (a single host).
    ///
    /// The error names what was wrong, because it ends up in a configuration report.
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let (address, prefix) = match text.split_once('/') {
            Some((address, prefix)) => (address.trim(), Some(prefix.trim())),
            None => (text, None),
        };
        let address = address.trim_start_matches('[').trim_end_matches(']');
        let address: IpAddr = address
            .parse()
            .map_err(|_| format!("{text:?} is not an IP address or CIDR range"))?;
        let address = address.to_canonical();
        let max = if address.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(prefix) => prefix
                .parse::<u8>()
                .ok()
                .filter(|prefix| *prefix <= max)
                .ok_or_else(|| format!("{text:?} has a prefix length outside 0..={max}"))?,
            None => max,
        };
        Ok(Self {
            network: mask(address, prefix),
            prefix,
        })
    }

    /// Whether `ip` falls inside this range. An IPv4-mapped IPv6 address (`::ffff:10.0.0.1`,
    /// what a dual-stack listener reports for an IPv4 peer) is compared as the IPv4 address.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        ip.is_ipv4() == self.network.is_ipv4() && mask(ip, self.prefix) == self.network
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

/// Clear the host bits of `ip` past `prefix`.
fn mask(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - u32::from(prefix.min(32)))
            };
            IpAddr::V4((bits & mask).into())
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - u32::from(prefix.min(128)))
            };
            IpAddr::V6((bits & mask).into())
        }
    }
}

/// Parse every entry, collecting each bad one into the error.
pub fn parse_cidrs(entries: &[String]) -> Result<Vec<Cidr>, Vec<String>> {
    let mut ranges = Vec::with_capacity(entries.len());
    let mut errors = Vec::new();
    for entry in entries {
        match Cidr::parse(entry) {
            Ok(range) => ranges.push(range),
            Err(error) => errors.push(error),
        }
    }
    if errors.is_empty() {
        Ok(ranges)
    } else {
        Err(errors)
    }
}

/// Whether any range contains `ip`.
pub fn is_trusted(ranges: &[Cidr], ip: IpAddr) -> bool {
    ranges.iter().any(|range| range.contains(ip))
}

/// The key a per-client limiter counts under: the IPv4 address itself, or the IPv6 /64.
///
/// One IPv6 host is routinely handed a whole /64, so keying by the full address would give an
/// attacker 2^64 fresh buckets.
pub fn limiter_key(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => mask(IpAddr::V6(v6), 64),
        v4 => v4,
    }
}

/// Whether a bind value names this machine only: `localhost`, `127.0.0.0/8` or `::1`, parsed
/// as an address rather than matched by prefix (`127.example.com` is not loopback).
pub fn is_loopback_bind(bind: &str) -> bool {
    let bind = bind.trim();
    if bind.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let bare = bind.trim_start_matches('[').trim_end_matches(']');
    bare.parse::<IpAddr>()
        .is_ok_and(|ip| ip.to_canonical().is_loopback())
}

/// A web origin normalised for comparison: `scheme://host:port`, lowercased, with the scheme's
/// default port made explicit, so `https://UI.example` and `https://ui.example:443/` agree.
///
/// `authority` is a Host-header-shaped `host[:port]`; only `http` and `https` are origins the
/// daemon serves. Userinfo, an empty host or a non-numeric port is no origin at all.
pub fn origin(scheme: &str, authority: &str) -> Option<String> {
    let scheme = scheme.trim().to_ascii_lowercase();
    let default_port: u16 = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => return None,
    };
    let authority = authority.trim();
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = match after {
            "" => None,
            _ => Some(after.strip_prefix(':')?),
        };
        (format!("[{}]", host.to_ascii_lowercase()), port)
    } else {
        match authority.split_once(':') {
            Some((host, port)) => (host.to_ascii_lowercase(), Some(port)),
            None => (authority.to_ascii_lowercase(), None),
        }
    };
    if host.is_empty() || host == "[]" {
        return None;
    }
    let port = match port {
        Some(port) => port.parse::<u16>().ok()?,
        None => default_port,
    };
    Some(format!("{scheme}://{host}:{port}"))
}

/// The normalised origin of a URL (`https://ui.example.com/crosstalk` -> `https://ui.example.com:443`).
pub fn url_origin(url: &str) -> Option<String> {
    let (scheme, rest) = url.trim().split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    origin(scheme, authority)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_normalise_case_and_default_ports() {
        assert_eq!(
            url_origin("https://UI.example.com/").as_deref(),
            Some("https://ui.example.com:443")
        );
        assert_eq!(
            url_origin("https://ui.example.com:443"),
            url_origin("https://ui.example.com")
        );
        assert_eq!(
            url_origin("http://[::1]:8730/x").as_deref(),
            Some("http://[::1]:8730")
        );
        assert_ne!(
            url_origin("http://ui.example.com"),
            url_origin("https://ui.example.com")
        );
        assert!(url_origin("ftp://ui.example.com").is_none());
        assert!(url_origin("https://user@ui.example.com").is_none());
        assert!(url_origin("https://ui.example.com:http").is_none());
        assert!(url_origin("ui.example.com").is_none());
        assert!(url_origin("null").is_none());
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().expect("address")
    }

    #[test]
    fn ipv4_ranges_match_their_hosts_only() {
        let range = Cidr::parse("10.1.0.0/16").expect("range");
        assert!(range.contains(ip("10.1.2.3")));
        assert!(!range.contains(ip("10.2.0.1")));
        // Host bits in the configured value are cleared, not rejected.
        assert_eq!(Cidr::parse("10.1.2.3/16").expect("range"), range);
        // A bare address is one host.
        let host = Cidr::parse("192.168.1.5").expect("host");
        assert!(host.contains(ip("192.168.1.5")));
        assert!(!host.contains(ip("192.168.1.6")));
        // /0 is everything of its family.
        assert!(Cidr::parse("0.0.0.0/0")
            .expect("all")
            .contains(ip("8.8.8.8")));
    }

    #[test]
    fn ipv6_ranges_match_and_mapped_ipv4_is_compared_as_ipv4() {
        let range = Cidr::parse("fd00::/8").expect("range");
        assert!(range.contains(ip("fd12:3456::1")));
        assert!(!range.contains(ip("fe80::1")));
        assert!(!range.contains(ip("10.0.0.1")), "families never cross");
        let v4 = Cidr::parse("127.0.0.1/32").expect("loopback");
        assert!(v4.contains(ip("::ffff:127.0.0.1")));
        assert!(Cidr::parse("[::1]").expect("bracketed").contains(ip("::1")));
    }

    #[test]
    fn bad_entries_are_named() {
        assert!(Cidr::parse("10.0.0.0/33").is_err());
        assert!(Cidr::parse("::/129").is_err());
        assert!(Cidr::parse("proxy.internal").is_err());
        assert!(Cidr::parse("").is_err());
        let errors = parse_cidrs(&["10.0.0.0/8".to_string(), "nope".to_string()])
            .expect_err("one bad entry");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("nope"), "{errors:?}");
    }

    #[test]
    fn ipv6_clients_are_keyed_by_their_64() {
        assert_eq!(
            limiter_key(ip("2001:db8:1:2:aaaa::1")),
            limiter_key(ip("2001:db8:1:2:bbbb::9"))
        );
        assert_ne!(
            limiter_key(ip("2001:db8:1:2::1")),
            limiter_key(ip("2001:db8:1:3::1"))
        );
        assert_eq!(limiter_key(ip("10.0.0.1")), ip("10.0.0.1"));
        assert_eq!(limiter_key(ip("::ffff:10.0.0.1")), ip("10.0.0.1"));
    }

    #[test]
    fn loopback_binds_are_parsed_not_prefix_matched() {
        assert!(is_loopback_bind("127.0.0.1"));
        assert!(is_loopback_bind("127.0.0.2"));
        assert!(is_loopback_bind("localhost"));
        assert!(is_loopback_bind("::1"));
        assert!(is_loopback_bind("[::1]"));
        assert!(!is_loopback_bind("127.example.com"));
        assert!(!is_loopback_bind("0.0.0.0"));
        assert!(!is_loopback_bind("::"));
        assert!(!is_loopback_bind("192.168.1.5"));
    }
}
