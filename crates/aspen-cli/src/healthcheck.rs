//! `aspen healthcheck`: a container health check with no `curl` or `wget`.
//!
//! The runtime image is Debian slim with the `aspen` binary only, so Docker
//! and compose health checks run `aspen healthcheck`, which sends one
//! `GET` over plain HTTP and exits 0 only on a `200` status line.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

/// The URL checked by default: `/healthz` on the port of `ASPEN_LISTEN`,
/// over loopback (a wildcard listen address is not one to connect to).
pub(crate) fn default_url(listen: Option<&str>) -> String {
    let port = listen
        .and_then(|l| l.trim().parse::<SocketAddr>().ok())
        .map_or(7070, |a| a.port());
    format!("http://127.0.0.1:{port}/healthz")
}

/// Splits `http://host[:port]/path` into the address to dial, the `Host`
/// header and the path.
fn split(url: &str) -> Result<(String, String, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("{url}: only http:// URLs are checked"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.is_empty() || authority.contains('@') {
        return Err(format!("{url}: expected http://host[:port]/path"));
    }
    let dial = if authority
        .rsplit_once(':')
        .is_some_and(|(_, p)| p.parse::<u16>().is_ok())
        && !authority.ends_with(']')
    {
        authority.to_owned()
    } else {
        format!("{authority}:80")
    };
    Ok((dial, authority.to_owned(), path.to_owned()))
}

/// Checks `url`; `Ok` only for a `200` answer within `timeout`.
pub(crate) fn check(url: &str, timeout: Duration) -> Result<(), String> {
    let (dial, host, path) = split(url)?;
    let addr = dial
        .to_socket_addrs()
        .map_err(|e| format!("{dial}: {e}"))?
        .next()
        .ok_or_else(|| format!("{dial}: no address"))?;
    let mut stream =
        TcpStream::connect_timeout(&addr, timeout).map_err(|e| format!("{url}: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.set_write_timeout(Some(timeout)))
        .map_err(|e| format!("{url}: {e}"))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: aspen-healthcheck\r\nConnection: close\r\n\r\n"
    )
    .map_err(|e| format!("{url}: {e}"))?;
    let mut head = Vec::new();
    let mut buf = [0u8; 512];
    while !head.windows(2).any(|w| w == b"\r\n") {
        let n = stream.read(&mut buf).map_err(|e| format!("{url}: {e}"))?;
        if n == 0 || head.len() > 4096 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    let line = String::from_utf8_lossy(&head);
    let status = line.lines().next().unwrap_or_default();
    match status.split(' ').nth(1) {
        Some("200") => Ok(()),
        _ => Err(format!("{url}: unhealthy: {status:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_url_uses_the_listen_port_over_loopback() {
        assert_eq!(default_url(None), "http://127.0.0.1:7070/healthz");
        assert_eq!(
            default_url(Some("0.0.0.0:8080")),
            "http://127.0.0.1:8080/healthz"
        );
        assert_eq!(
            default_url(Some("garbage")),
            "http://127.0.0.1:7070/healthz"
        );
    }

    #[test]
    fn urls_split_into_dial_host_and_path() {
        assert_eq!(
            split("http://127.0.0.1:7070/readyz").unwrap(),
            (
                "127.0.0.1:7070".into(),
                "127.0.0.1:7070".into(),
                "/readyz".into()
            )
        );
        assert_eq!(
            split("http://localhost").unwrap(),
            ("localhost:80".into(), "localhost".into(), "/".into())
        );
        assert!(split("https://x/healthz").is_err());
        assert!(split("http://user@x/healthz").is_err());
    }
}
