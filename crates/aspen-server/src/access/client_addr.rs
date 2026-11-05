//! Where an HTTP client's address comes from: the TCP peer, or an
//! `X-Forwarded-For` entry written by a trusted proxy (issue #121).
//!
//! The address keys anonymous rate limits and every caller's
//! failed-authentication bucket ([`super::ip_key`]), so a client must not
//! be able to choose it. Behind proxies ([`ClientAddr::Forwarded`]) the
//! header is read only when the TCP peer is one of the trusted proxies;
//! the entry `hops` places from the right is the client's address. Every
//! case where no such address can be read (the peer is not a trusted
//! proxy, the header is too short, the entry is not an address) falls
//! back to the TCP peer, never to a shared unknown key, and is counted in
//! `aspen_client_address_fallbacks_total{reason}`.
//!
//! The header is parsed from its raw bytes, one comma-separated entry at a
//! time, so an entry a client wrote that is not ASCII cannot hide the
//! entries the proxies appended after it. An entry may carry a port
//! (`203.0.113.9:4711`, `[2001:db8::1]:443`) or brackets, as some proxies
//! write them; empty entries are skipped, as HTTP list syntax requires.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::HeaderMap;

use crate::metrics::expo;

/// Where a client's address comes from, for anonymous rate-limit keys and
/// failed-authentication buckets.
#[derive(Debug, Clone, Default)]
pub enum ClientAddr {
    /// The TCP peer (`ConnectInfo<SocketAddr>`), when Aspen faces clients
    /// directly.
    #[default]
    Peer,
    /// The address the proxies in front of Aspen recorded in
    /// `X-Forwarded-For`, read only from a trusted proxy.
    Forwarded(Arc<Forwarded>),
}

/// The settings of [`ClientAddr::Forwarded`], and its fallback counters.
pub struct Forwarded {
    hops: NonZeroUsize,
    proxies: TrustedProxies,
    fallbacks: [AtomicU64; Fallback::ALL.len()],
}

impl fmt::Debug for Forwarded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Forwarded")
            .field("hops", &self.hops)
            .field("proxies", &self.proxies)
            .finish_non_exhaustive()
    }
}

/// Why a request behind [`ClientAddr::Forwarded`] was keyed by its TCP
/// peer instead of a forwarded address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fallback {
    /// The TCP peer is not a trusted proxy: a client that reached Aspen
    /// directly, or a proxy missing from the trusted list.
    UntrustedPeer,
    /// The header has fewer entries than `hops`: a path that skips one of
    /// the proxies, or hops set higher than the proxies in front and a
    /// client that sent no header of its own. A client that sends one
    /// then has its own entry read instead, and does not show here.
    Missing,
    /// The entry `hops` places from the right is not an address.
    Malformed,
}

impl Fallback {
    /// Every reason, in label order.
    pub const ALL: [Self; 3] = [Self::UntrustedPeer, Self::Missing, Self::Malformed];

    /// Its `reason` label.
    pub fn label(self) -> &'static str {
        match self {
            Self::UntrustedPeer => "untrusted_peer",
            Self::Missing => "missing",
            Self::Malformed => "malformed",
        }
    }
}

/// Why client-address settings were refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientAddrError {
    /// Trusted proxies were listed with zero hops, which never reads the
    /// header.
    #[error("trusted proxies are listed but the proxy hop count is 0; set the hop count too")]
    ProxiesWithoutHops,
    /// The trusted-proxy list did not parse.
    #[error("{0}")]
    Proxies(String),
}

impl ClientAddr {
    /// The client address source for `hops` proxies in front (0 means the
    /// TCP peer) whose own addresses are in `proxies` (a list as
    /// [`TrustedProxies::from_str`] reads it); without a list, the
    /// private ranges ([`TrustedProxies::private`]).
    pub fn from_settings(hops: usize, proxies: Option<&str>) -> Result<Self, ClientAddrError> {
        let proxies = proxies
            .map(|p| p.parse::<TrustedProxies>())
            .transpose()
            .map_err(ClientAddrError::Proxies)?;
        match (NonZeroUsize::new(hops), proxies) {
            (None, None) => Ok(Self::Peer),
            (None, Some(_)) => Err(ClientAddrError::ProxiesWithoutHops),
            (Some(hops), proxies) => Ok(Self::forwarded(
                hops,
                proxies.unwrap_or_else(TrustedProxies::private),
            )),
        }
    }

    /// Behind `hops` proxies, trusting the header only from `proxies`.
    pub fn forwarded(hops: NonZeroUsize, proxies: TrustedProxies) -> Self {
        Self::Forwarded(Arc::new(Forwarded {
            hops,
            proxies,
            fallbacks: Default::default(),
        }))
    }

    /// The proxy hop count; 0 for [`ClientAddr::Peer`].
    pub fn hops(&self) -> usize {
        match self {
            Self::Peer => 0,
            Self::Forwarded(f) => f.hops.get(),
        }
    }

    /// The trusted proxies; `None` for [`ClientAddr::Peer`].
    pub fn trusted_proxies(&self) -> Option<&TrustedProxies> {
        match self {
            Self::Peer => None,
            Self::Forwarded(f) => Some(&f.proxies),
        }
    }

    /// The client address of a request from TCP peer `peer` (`None` when
    /// the transport did not say) with these headers. Behind proxies, a
    /// request without a readable forwarded address is the peer, and the
    /// fallback is counted ([`ClientAddr::fallbacks`]). `None` only when
    /// the peer is unknown.
    pub fn client_ip(&self, peer: Option<IpAddr>, headers: &HeaderMap) -> Option<IpAddr> {
        match self {
            Self::Peer => peer,
            Self::Forwarded(f) => {
                let peer = peer?;
                match f.resolve(peer, headers) {
                    Ok(ip) => Some(ip),
                    Err(why) => {
                        f.fallbacks[why as usize].fetch_add(1, Ordering::Relaxed);
                        Some(peer)
                    }
                }
            }
        }
    }

    /// Requests keyed by their TCP peer for `why`, since the process
    /// started; always 0 for [`ClientAddr::Peer`].
    pub fn fallbacks(&self, why: Fallback) -> u64 {
        match self {
            Self::Peer => 0,
            Self::Forwarded(f) => f.fallbacks[why as usize].load(Ordering::Relaxed),
        }
    }

    /// Writes `aspen_client_address_fallbacks_total{reason}`, zeros
    /// included.
    pub(crate) fn render_metrics(&self, out: &mut String) {
        expo::family(
            out,
            "aspen_client_address_fallbacks_total",
            "Requests behind trusted proxies keyed by their TCP peer because no forwarded client address could be read, by reason.",
            expo::Kind::Counter,
            Fallback::ALL
                .iter()
                .map(|&r| (vec![("reason", r.label())], self.fallbacks(r) as f64)),
        );
    }
}

impl Forwarded {
    /// The forwarded client address, or why there is none.
    fn resolve(&self, peer: IpAddr, headers: &HeaderMap) -> Result<IpAddr, Fallback> {
        if !self.proxies.contains(peer) {
            return Err(Fallback::UntrustedPeer);
        }
        // Several header lines are one list, in order; count entries from
        // the right without decoding any but the one that is read.
        let mut seen = 0;
        for line in headers.get_all("x-forwarded-for").iter().rev() {
            for entry in line.as_bytes().rsplit(|&b| b == b',') {
                let entry = entry.trim_ascii();
                if entry.is_empty() {
                    continue;
                }
                seen += 1;
                if seen == self.hops.get() {
                    return parse_entry(entry).ok_or(Fallback::Malformed);
                }
            }
        }
        Err(Fallback::Missing)
    }
}

/// One `X-Forwarded-For` entry: an address, optionally with a port
/// (`1.2.3.4:80`, `[v6]:80`) or in brackets (`[v6]`). An IPv4-mapped IPv6
/// address (`::ffff:1.2.3.4`, as a dual-stack proxy may write it) is
/// read as the IPv4 address it maps, so every caller of
/// [`ClientAddr::client_ip`] sees one form per client; keyed as IPv6, all
/// of them would share one /64.
fn parse_entry(entry: &[u8]) -> Option<IpAddr> {
    let s = std::str::from_utf8(entry).ok()?;
    let ip = if let Ok(ip) = s.parse::<IpAddr>() {
        ip
    } else if let Ok(sa) = s.parse::<SocketAddr>() {
        sa.ip()
    } else {
        let inner = s.strip_prefix('[')?.strip_suffix(']')?;
        IpAddr::V6(inner.parse::<Ipv6Addr>().ok()?)
    };
    Some(ip.to_canonical())
}

/// The networks whose addresses may set a client's address through
/// `X-Forwarded-For`: those of the proxies that connect to Aspen.
#[derive(Clone, PartialEq, Eq)]
pub struct TrustedProxies(Vec<Net>);

/// An IP network: an address with every bit past `prefix` clear.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Net {
    addr: IpAddr,
    prefix: u8,
}

impl Net {
    fn contains(self, ip: IpAddr) -> bool {
        match (self.addr, ip.to_canonical()) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                masked(u32::from(a).into(), 32, self.prefix) == u32::from(n).into()
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => masked(a.into(), 128, self.prefix) == n.into(),
            _ => false,
        }
    }
}

/// `bits` (the low `width` bits of an address) with every bit past
/// `prefix` cleared.
fn masked(bits: u128, width: u8, prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        bits & (u128::MAX << (width - prefix)) & (u128::MAX >> (128 - u32::from(width)))
    }
}

impl TrustedProxies {
    /// Loopback, private (RFC 1918 and RFC 4193), shared (RFC 6598) and
    /// link-local addresses: where load balancers, ingress controllers and
    /// platform front ends usually connect from. A client on the public
    /// internet cannot connect from one of these.
    pub fn private() -> Self {
        let nets: [(IpAddr, u8); 9] = [
            (Ipv4Addr::new(127, 0, 0, 0).into(), 8),
            (Ipv4Addr::new(10, 0, 0, 0).into(), 8),
            (Ipv4Addr::new(172, 16, 0, 0).into(), 12),
            (Ipv4Addr::new(192, 168, 0, 0).into(), 16),
            (Ipv4Addr::new(100, 64, 0, 0).into(), 10),
            (Ipv4Addr::new(169, 254, 0, 0).into(), 16),
            (Ipv6Addr::LOCALHOST.into(), 128),
            (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0).into(), 7),
            (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0).into(), 10),
        ];
        Self(
            nets.into_iter()
                .map(|(addr, prefix)| Net { addr, prefix })
                .collect(),
        )
    }

    /// Whether `ip` (IPv4-mapped IPv6 counts as IPv4) is in one of the
    /// networks.
    pub fn contains(&self, ip: IpAddr) -> bool {
        self.0.iter().any(|n| n.contains(ip))
    }
}

/// Reads a list of networks separated by commas or white space: each an
/// address (`10.0.0.7`, one host) or a CIDR block (`10.0.0.0/16`,
/// `fd00::/8`) with no bits set past its prefix.
impl FromStr for TrustedProxies {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let nets = s
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|t| !t.is_empty())
            .map(|t| {
                let (addr, prefix) = match t.split_once('/') {
                    Some((a, p)) => (a, Some(p)),
                    None => (t, None),
                };
                let addr: IpAddr = addr
                    .parse()
                    .map_err(|_| format!("{t:?} is not an address or CIDR block"))?;
                let width = if addr.is_ipv4() { 32 } else { 128 };
                let prefix = match prefix {
                    None => width,
                    // Digits only: `u8::from_str` also takes a leading `+`.
                    Some(p) if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) => {
                        return Err(format!("{t:?} has a prefix length that is not a number"));
                    }
                    Some(p) => p
                        .parse::<u8>()
                        .ok()
                        .filter(|&p| p <= width)
                        .ok_or_else(|| format!("{t:?} has a prefix length over {width}"))?,
                };
                let bits = match addr {
                    IpAddr::V4(a) => u128::from(u32::from(a)),
                    IpAddr::V6(a) => u128::from(a),
                };
                if masked(bits, width, prefix) != bits {
                    return Err(format!("{t:?} has address bits set past its prefix"));
                }
                Ok(Net { addr, prefix })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if nets.is_empty() {
            return Err("names no address".into());
        }
        Ok(Self(nets))
    }
}

impl fmt::Display for TrustedProxies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, n) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}/{}", n.addr, n.prefix)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TrustedProxies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrustedProxies({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(lines: &[&[u8]]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for l in lines {
            h.append("x-forwarded-for", HeaderValue::from_bytes(l).unwrap());
        }
        h
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn behind(hops: usize) -> ClientAddr {
        ClientAddr::from_settings(hops, None).unwrap()
    }

    #[test]
    fn the_entry_hops_from_the_right_is_read_only_from_a_trusted_peer() {
        let c = behind(1);
        let proxy = Some(ip("10.0.0.2"));
        let h = headers(&[b"6.6.6.6, 1.1.1.1", b"203.0.113.9"]);
        assert_eq!(c.client_ip(proxy, &h), Some(ip("203.0.113.9")));
        assert_eq!(behind(2).client_ip(proxy, &h), Some(ip("1.1.1.1")));
        // A client that reaches Aspen directly is its own address.
        let direct = Some(ip("198.51.100.7"));
        assert_eq!(c.client_ip(direct, &h), direct);
        assert_eq!(c.fallbacks(Fallback::UntrustedPeer), 1);
        // IPv4-mapped peers are matched as IPv4.
        assert_eq!(
            c.client_ip(Some(ip("::ffff:10.0.0.2")), &h),
            Some(ip("203.0.113.9"))
        );
        // No peer at all is unknown, behind proxies or not.
        assert_eq!(c.client_ip(None, &h), None);
        assert_eq!(ClientAddr::Peer.client_ip(None, &h), None);
        assert_eq!(ClientAddr::Peer.client_ip(direct, &h), direct);
    }

    #[test]
    fn entries_are_parsed_one_at_a_time_from_raw_bytes() {
        let c = behind(1);
        let proxy = Some(ip("10.0.0.2"));
        for (lines, want) in [
            (&[&b"\xff, 203.0.113.9"[..]][..], "203.0.113.9"),
            (&[b"\xff", b"203.0.113.9"], "203.0.113.9"),
            (&[b"203.0.113.9:4711"], "203.0.113.9"),
            (&[b"[2001:db8::1]:443"], "2001:db8::1"),
            (&[b"[2001:db8::1]"], "2001:db8::1"),
            (&[b"2001:db8::1"], "2001:db8::1"),
            (&[b"203.0.113.9 ,, "], "203.0.113.9"),
            (&[b"203.0.113.9", b""], "203.0.113.9"),
            // IPv4-mapped entries are their IPv4 address, not a shared /64.
            (&[b"::ffff:203.0.113.9"], "203.0.113.9"),
            (&[b"[::ffff:203.0.113.9]:443"], "203.0.113.9"),
            (&[b"[::ffff:203.0.113.9]"], "203.0.113.9"),
        ] {
            assert_eq!(
                c.client_ip(proxy, &headers(lines)),
                Some(ip(want)),
                "{lines:?}"
            );
        }
        assert_eq!(c.fallbacks(Fallback::Malformed), 0);
        assert_eq!(c.fallbacks(Fallback::Missing), 0);
    }

    #[test]
    fn an_unreadable_entry_falls_back_to_the_peer_and_is_counted() {
        let c = behind(1);
        let proxy = Some(ip("10.0.0.2"));
        for lines in [&[&b"unknown"[..]][..], &[b"1.2.3.4, \xff"], &[b"[1.2.3.4]"]] {
            assert_eq!(c.client_ip(proxy, &headers(lines)), proxy, "{lines:?}");
        }
        assert_eq!(c.fallbacks(Fallback::Malformed), 3);
        for lines in [&[][..], &[&b""[..]], &[b" , "]] {
            assert_eq!(c.client_ip(proxy, &headers(lines)), proxy, "{lines:?}");
        }
        assert_eq!(
            behind(3).client_ip(proxy, &headers(&[b"1.1.1.1, 2.2.2.2"])),
            proxy
        );
        assert_eq!(c.fallbacks(Fallback::Missing), 3);
        let mut out = String::new();
        c.render_metrics(&mut out);
        assert!(
            out.contains("aspen_client_address_fallbacks_total{reason=\"malformed\"} 3\n"),
            "{out}"
        );
        assert!(out.contains("{reason=\"untrusted_peer\"} 0\n"), "{out}");
    }

    #[test]
    fn trusted_proxies_parse_and_match() {
        let p: TrustedProxies = "10.1.0.0/16, 192.0.2.7 fd00::/8".parse().unwrap();
        assert_eq!(p.to_string(), "10.1.0.0/16,192.0.2.7/32,fd00::/8");
        assert!(p.contains(ip("10.1.255.1")));
        assert!(!p.contains(ip("10.2.0.1")));
        assert!(p.contains(ip("192.0.2.7")));
        assert!(!p.contains(ip("192.0.2.8")));
        assert!(p.contains(ip("fd12::1")));
        assert!(p.contains(ip("::ffff:10.1.0.1")));
        assert!(!p.contains(ip("fe80::1")));
        let all: TrustedProxies = "0.0.0.0/0".parse().unwrap();
        assert!(all.contains(ip("8.8.8.8")) && !all.contains(ip("::1")));
        for (bad, why) in [
            ("", "names no address"),
            (" , ", "names no address"),
            ("10.0.0.1/8", "past its prefix"),
            ("10.0.0.0/33", "over 32"),
            ("10.0.0.0/300", "over 32"),
            ("fd00::/129", "over 128"),
            ("host", "not an address"),
            ("10.0.0.0/x", "not a number"),
            ("10.0.0.0/+8", "not a number"),
            ("10.0.0.0/", "not a number"),
            ("10.0.0.0/-8", "not a number"),
        ] {
            let e = bad.parse::<TrustedProxies>().unwrap_err();
            assert!(e.contains(why), "{bad:?}: {e}");
        }
        let private = TrustedProxies::private();
        for a in [
            "127.0.0.1",
            "10.9.8.7",
            "172.31.0.1",
            "192.168.1.1",
            "100.64.0.1",
            "169.254.1.1",
            "::1",
            "fd00::1",
            "fe80::1",
        ] {
            assert!(private.contains(ip(a)), "{a}");
        }
        for a in [
            "8.8.8.8",
            "172.32.0.1",
            "100.128.0.1",
            "198.51.100.7",
            "2001:db8::1",
            "::ffff:8.8.8.8",
        ] {
            assert!(!private.contains(ip(a)), "{a}");
        }
    }

    #[test]
    fn settings_are_checked() {
        assert!(matches!(
            ClientAddr::from_settings(0, None),
            Ok(ClientAddr::Peer)
        ));
        assert_eq!(
            ClientAddr::from_settings(0, Some("10.0.0.0/8")).unwrap_err(),
            ClientAddrError::ProxiesWithoutHops
        );
        assert!(ClientAddr::from_settings(1, Some("nope")).is_err());
        let c = ClientAddr::from_settings(2, Some("10.0.0.0/8")).unwrap();
        assert_eq!(c.hops(), 2);
        assert_eq!(c.trusted_proxies().unwrap().to_string(), "10.0.0.0/8");
        assert_eq!(
            ClientAddr::from_settings(1, None)
                .unwrap()
                .trusted_proxies(),
            Some(&TrustedProxies::private())
        );
    }
}
