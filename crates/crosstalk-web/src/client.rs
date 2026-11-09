//! Who sent a request: the client address, and what a trusted reverse proxy says about it.
//!
//! The socket peer is the truth unless it is one of `[server] trusted_proxies`. Only then is
//! `X-Forwarded-For` read (the rightmost address that is not itself a trusted proxy: everything
//! to its left was written by the client and is worth nothing), then `X-Real-IP`; likewise only
//! a trusted proxy's `X-Forwarded-Proto` / `X-Forwarded-Host` count. A request from anyone else
//! that carries those headers is judged by its socket address alone, so a spoofed
//! `X-Forwarded-For` can neither dodge the limiter nor lock somebody else out.

use axum::extract::{ConnectInfo, FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::HeaderMap;
use crosstalk_core::net::{is_trusted, limiter_key, Cidr};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Headers that say a request went through a proxy.
pub const FORWARDING_HEADERS: [&str; 5] = [
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-real-ip",
];

/// What the auth gate worked out about a request's sender; handlers read it as an extractor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientInfo {
    /// The client address (see the module docs).
    pub ip: IpAddr,
    /// The key the limiters count under: the address, or its IPv6 /64.
    pub key: IpAddr,
    /// Whether the socket peer is a trusted proxy.
    pub via_trusted_proxy: bool,
    /// Whether a trusted proxy says the client connected over https.
    pub https: bool,
}

impl ClientInfo {
    /// Inspect a request against the trusted proxy ranges.
    pub fn of(peer: Option<IpAddr>, headers: &HeaderMap, trusted: &[Cidr]) -> Self {
        let via_trusted_proxy = peer.is_some_and(|peer| is_trusted(trusted, peer));
        let ip = client_ip(peer, headers, trusted);
        Self {
            ip,
            key: limiter_key(ip),
            via_trusted_proxy,
            https: via_trusted_proxy && forwarded_proto(headers).as_deref() == Some("https"),
        }
    }

    /// Inspect a whole request.
    pub fn of_request(request: &Request, trusted: &[Cidr]) -> Self {
        Self::of(peer(request.extensions()), request.headers(), trusted)
    }
}

/// The socket peer, when the daemon is served with connection info.
pub fn peer(extensions: &axum::http::Extensions) -> Option<IpAddr> {
    extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
}

/// The client address a request is accounted to.
///
/// Without a socket peer (the test harness, or a daemon served without connection info) every
/// request shares the unspecified address, which only makes the limits stricter.
pub fn client_ip(peer: Option<IpAddr>, headers: &HeaderMap, trusted: &[Cidr]) -> IpAddr {
    let Some(peer) = peer.map(|peer| peer.to_canonical()) else {
        return IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    };
    if !is_trusted(trusted, peer) {
        return peer;
    }

    // Every X-Forwarded-For line, in order, as one hop list.
    let hops: Vec<&str> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|hop| !hop.is_empty())
        .collect();
    if !hops.is_empty() {
        let mut leftmost_proxy = None;
        for hop in hops.iter().rev() {
            // Something unreadable where a trusted proxy should have written an address: the
            // chain cannot be followed further, so the request is the proxy's own.
            let Some(address) = parse_hop(hop) else {
                return leftmost_proxy.unwrap_or(peer);
            };
            if !is_trusted(trusted, address) {
                return address;
            }
            leftmost_proxy = Some(address);
        }
        // Every hop is a trusted proxy: the furthest one is the best name for the client.
        return leftmost_proxy.unwrap_or(peer);
    }

    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_hop)
        .unwrap_or(peer)
}

/// One `X-Forwarded-For` entry: a bare address, `[v6]`, or an address with a port.
fn parse_hop(hop: &str) -> Option<IpAddr> {
    let hop = hop.trim().trim_matches('"');
    if let Ok(address) = hop.parse::<IpAddr>() {
        return Some(address.to_canonical());
    }
    if let Ok(socket) = hop.parse::<SocketAddr>() {
        return Some(socket.ip().to_canonical());
    }
    hop.strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .and_then(|inner| inner.parse::<IpAddr>().ok())
        .map(|address| address.to_canonical())
}

/// The first `X-Forwarded-Proto` value, lowercased: the scheme the outermost proxy saw.
pub fn forwarded_proto(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

/// The first `X-Forwarded-Host` value: the host the outermost proxy was asked for.
pub fn forwarded_host(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-host")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Whether a request carries any proxy header at all.
pub fn has_forwarding_headers(headers: &HeaderMap) -> bool {
    FORWARDING_HEADERS
        .iter()
        .any(|name| headers.contains_key(*name))
}

/// Handlers read the gate's answer; a request that skipped the gate (none do in the router)
/// is judged by its socket address with no proxy trusted.
#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for ClientInfo {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<ClientInfo>()
            .copied()
            .unwrap_or_else(|| ClientInfo::of(peer(&parts.extensions), &parts.headers, &[])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().expect("address")
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                value.parse().expect("value"),
            );
        }
        map
    }

    fn proxies() -> Vec<Cidr> {
        vec![
            Cidr::parse("10.0.0.0/8").expect("range"),
            Cidr::parse("fd00::/8").expect("range"),
        ]
    }

    /// One table row: peer address, request headers, expected client, what the row shows.
    type ClientIpCase = (
        &'static str,
        &'static [(&'static str, &'static str)],
        &'static str,
        &'static str,
    );

    #[test]
    fn client_ip_table() {
        let cases: &[ClientIpCase] = &[
            // An untrusted peer's headers are ignored, whatever they say.
            (
                "203.0.113.9",
                &[("x-forwarded-for", "198.51.100.1")],
                "203.0.113.9",
                "spoofed XFF from the internet",
            ),
            (
                "203.0.113.9",
                &[("x-real-ip", "198.51.100.1")],
                "203.0.113.9",
                "spoofed X-Real-IP",
            ),
            // A trusted proxy names the client.
            (
                "10.0.0.2",
                &[("x-forwarded-for", "198.51.100.1")],
                "198.51.100.1",
                "one proxy",
            ),
            // The client's own claims are to the left; the proxy appended the real peer.
            (
                "10.0.0.2",
                &[("x-forwarded-for", "1.1.1.1, 198.51.100.1")],
                "198.51.100.1",
                "client-supplied hop ignored",
            ),
            // Two trusted proxies in a chain: skip both.
            (
                "10.0.0.2",
                &[("x-forwarded-for", "198.51.100.1, 10.0.0.7")],
                "198.51.100.1",
                "chained proxies",
            ),
            // Several header lines are one list.
            (
                "10.0.0.2",
                &[
                    ("x-forwarded-for", "1.1.1.1"),
                    ("x-forwarded-for", "198.51.100.1"),
                ],
                "198.51.100.1",
                "split header lines",
            ),
            // X-Real-IP when there is no XFF.
            (
                "10.0.0.2",
                &[("x-real-ip", "198.51.100.4")],
                "198.51.100.4",
                "x-real-ip",
            ),
            // Garbage where an address should be: the proxy itself.
            (
                "10.0.0.2",
                &[("x-forwarded-for", "unknown")],
                "10.0.0.2",
                "unparseable hop",
            ),
            // Ports and brackets are tolerated.
            (
                "10.0.0.2",
                &[("x-forwarded-for", "198.51.100.1:5000")],
                "198.51.100.1",
                "with a port",
            ),
            (
                "fd00::2",
                &[("x-forwarded-for", "[2001:db8::1]:443")],
                "2001:db8::1",
                "bracketed v6",
            ),
            // No headers from a trusted proxy: the proxy.
            ("10.0.0.2", &[], "10.0.0.2", "no headers"),
            // An IPv4-mapped peer is the IPv4 address.
            (
                "::ffff:10.0.0.2",
                &[("x-forwarded-for", "198.51.100.1")],
                "198.51.100.1",
                "mapped peer",
            ),
        ];
        for (peer, pairs, expected, label) in cases {
            assert_eq!(
                client_ip(Some(ip(peer)), &headers(pairs), &proxies()),
                ip(expected),
                "{label}"
            );
        }
    }

    #[test]
    fn no_peer_is_the_shared_unspecified_bucket() {
        assert_eq!(
            client_ip(
                None,
                &headers(&[("x-forwarded-for", "1.2.3.4")]),
                &proxies()
            ),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn https_counts_only_from_a_trusted_proxy() {
        let https = headers(&[("x-forwarded-proto", "https")]);
        assert!(ClientInfo::of(Some(ip("10.0.0.2")), &https, &proxies()).https);
        assert!(!ClientInfo::of(Some(ip("203.0.113.9")), &https, &proxies()).https);
        let http = headers(&[("x-forwarded-proto", "http")]);
        assert!(!ClientInfo::of(Some(ip("10.0.0.2")), &http, &proxies()).https);
    }

    #[test]
    fn forwarding_headers_are_detected() {
        assert!(!has_forwarding_headers(&headers(&[("host", "localhost")])));
        for name in FORWARDING_HEADERS {
            assert!(has_forwarding_headers(&headers(&[(name, "x")])), "{name}");
        }
    }
}
