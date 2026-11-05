//! The upstream: its URL, and the HTTP client that lists its refs, fetches
//! packs from it and pushes to it ([`crate::wire`] has the bytes).

use std::path::Path;
use std::time::Duration;

use aspen_core::id::ObjectId;
use futures_util::StreamExt as _;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use tokio::io::AsyncWriteExt as _;

use crate::RefUpdate;
use crate::credential::{CredentialError, Secret, is_loopback_url};
use crate::wire::{self, ListedRefs, PushAdvertisement, PushReport, V2Caps, WireError};

/// Which forge an upstream is, for the HTTP user name a token goes with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Forge {
    /// github.com (GitHub Enterprise hosts are [`Forge::Other`]; give
    /// their credential `USERNAME=x-access-token`).
    GitHub,
    /// gitlab.com.
    GitLab,
    /// Any other smart-HTTP Git server.
    Other,
}

/// An upstream repository's smart-HTTP URL: `https` (or `http`, which
/// carries no credential unless the host is loopback), with no user
/// information, query or fragment. `github.com/org/repo` is shorthand for
/// `https://github.com/org/repo.git`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamUrl(url::Url);

/// Why an upstream URL is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("bad upstream URL {url:?}: {reason}")]
pub struct BadUrl {
    /// The URL given.
    pub url: String,
    /// What is wrong.
    pub reason: &'static str,
}

impl UpstreamUrl {
    /// Parses and checks a URL.
    pub fn parse(s: &str) -> Result<Self, BadUrl> {
        let bad = |reason| BadUrl {
            url: s.to_owned(),
            reason,
        };
        let full = if s.contains("://") {
            s.to_owned()
        } else {
            let s = s.trim_end_matches('/');
            let with_git = if s.ends_with(".git") {
                s.to_owned()
            } else {
                format!("{s}.git")
            };
            format!("https://{with_git}")
        };
        let mut u = url::Url::parse(&full).map_err(|_| bad("not a URL"))?;
        if !matches!(u.scheme(), "https" | "http") {
            return Err(bad("only https:// (and http://) upstreams are supported"));
        }
        if !u.username().is_empty() || u.password().is_some() {
            return Err(bad(
                "put credentials in a configured credential, not in the URL",
            ));
        }
        if u.query().is_some() || u.fragment().is_some() {
            return Err(bad("no query or fragment"));
        }
        if u.host_str().is_none_or(str::is_empty) {
            return Err(bad("no host"));
        }
        let path = u.path().trim_end_matches('/').to_owned();
        if path.is_empty() {
            return Err(bad("no repository path"));
        }
        u.set_path(&path);
        if u.as_str().len() > 2048 {
            return Err(bad("longer than 2,048 bytes"));
        }
        Ok(Self(u))
    }

    /// The URL text, as stored.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Whether it is `https`.
    pub fn is_https(&self) -> bool {
        self.0.scheme() == "https"
    }

    /// Whether the host is a loopback address.
    pub fn is_loopback(&self) -> bool {
        is_loopback_url(self.0.as_str())
    }

    /// The forge, from the host name.
    pub fn forge(&self) -> Forge {
        match self.0.host_str() {
            Some("github.com") => Forge::GitHub,
            Some("gitlab.com") => Forge::GitLab,
            _ => Forge::Other,
        }
    }

    /// The HTTP user name a token goes with on this host.
    pub fn default_user(&self) -> &'static str {
        match self.forge() {
            Forge::GitHub => "x-access-token",
            Forge::GitLab => "oauth2",
            Forge::Other => "git",
        }
    }

    /// The last path segment without `.git`: the default Aspen repository
    /// name's last segment.
    pub fn repo_path(&self) -> String {
        let p = self.0.path().trim_start_matches('/');
        p.strip_suffix(".git").unwrap_or(p).to_owned()
    }

    fn join(&self, tail: &str) -> String {
        format!("{}/{tail}", self.0.as_str())
    }
}

impl std::fmt::Display for UpstreamUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

/// Why an exchange with the upstream failed.
#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// The credential could not be used.
    #[error(transparent)]
    Credential(#[from] CredentialError),
    /// 401 or 403: the credential is missing, wrong, or lacks access.
    #[error("the upstream refused the credential (HTTP {0}); check the mirror's credential")]
    Auth(u16),
    /// 404: no such repository (or the credential cannot see it).
    #[error("the upstream repository was not found (HTTP 404), or the credential cannot see it")]
    NotFound,
    /// The connection failed, timed out, or the server answered 5xx or
    /// another unexpected status. Retryable.
    #[error("the upstream is unavailable: {0}")]
    Unavailable(String),
    /// The answer did not follow the protocol, or reported an error.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// A local file could not be read or written.
    #[error("local I/O: {0}")]
    Io(String),
    /// The credential's `Authorization` value is not a valid HTTP header
    /// value (a control character in the token, such as a newline).
    #[error(
        "the credential is not a valid HTTP header value; check the token for stray characters"
    )]
    BadAuthorization,
}

impl UpstreamError {
    /// Whether retrying later may succeed without an operator.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::Io(_))
    }
}

/// HTTP timeouts and limits for upstream exchanges.
#[derive(Debug, Clone, Copy)]
pub struct HttpOptions {
    /// Connecting.
    pub connect_timeout: Duration,
    /// Waiting for the next bytes of an answer (a long pack transfer is
    /// fine as long as bytes keep coming).
    pub read_timeout: Duration,
    /// Largest answer that is not a pack (ref listings, reports).
    pub max_listing_bytes: usize,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            max_listing_bytes: 256 << 20,
        }
    }
}

/// A shared HTTP client for upstreams. Redirects are followed only to the
/// same scheme, host and port (a renamed GitHub repository redirects), so
/// a credential never follows a redirect to another host.
pub fn http_client(options: HttpOptions) -> Result<reqwest::Client, String> {
    // reqwest is built without a default crypto provider (A4).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let policy = reqwest::redirect::Policy::custom(|attempt| {
        let same_origin = attempt.previous().first().is_some_and(|first| {
            first.scheme() == attempt.url().scheme()
                && first.host_str() == attempt.url().host_str()
                && first.port_or_known_default() == attempt.url().port_or_known_default()
        });
        if attempt.previous().len() >= 5 || !same_origin {
            attempt.stop()
        } else {
            attempt.follow()
        }
    });
    reqwest::Client::builder()
        .user_agent(wire::AGENT)
        .redirect(policy)
        .connect_timeout(options.connect_timeout)
        .read_timeout(options.read_timeout)
        .build()
        .map_err(|e| e.to_string())
}

/// One upstream with its credential, for the exchanges of one sync pass or
/// one push.
#[derive(Debug, Clone)]
pub struct Upstream {
    http: reqwest::Client,
    url: UpstreamUrl,
    /// The `Authorization` value, marked sensitive (never logged).
    auth: Option<HeaderValue>,
    options: HttpOptions,
}

fn unavailable(e: impl std::fmt::Display) -> UpstreamError {
    UpstreamError::Unavailable(e.to_string())
}

impl Upstream {
    /// An upstream reached with `http`, sending `auth` as the
    /// `Authorization` header when given. A credential that is not a valid
    /// header value is refused here, so no request ever goes out without
    /// the credential the mirror is configured with.
    pub fn new(
        http: reqwest::Client,
        url: UpstreamUrl,
        auth: Option<Secret>,
        options: HttpOptions,
    ) -> Result<Self, UpstreamError> {
        let auth = auth
            .map(|a| {
                let mut v = HeaderValue::from_str(a.expose())
                    .map_err(|_| UpstreamError::BadAuthorization)?;
                v.set_sensitive(true);
                Ok::<_, UpstreamError>(v)
            })
            .transpose()?;
        Ok(Self {
            http,
            url,
            auth,
            options,
        })
    }

    /// The URL.
    pub fn url(&self) -> &UpstreamUrl {
        &self.url
    }

    fn request(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        let r = self.http.request(method, url);
        match &self.auth {
            Some(v) => r.header(AUTHORIZATION, v.clone()),
            None => r,
        }
    }

    async fn send(&self, r: reqwest::RequestBuilder) -> Result<reqwest::Response, UpstreamError> {
        let resp = r.send().await.map_err(unavailable)?;
        match resp.status() {
            s if s.is_success() => Ok(resp),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(UpstreamError::Auth(resp.status().as_u16()))
            }
            StatusCode::NOT_FOUND => Err(UpstreamError::NotFound),
            s => Err(UpstreamError::Unavailable(format!("HTTP {s}"))),
        }
    }

    /// Reads a whole answer that is not a pack, up to the listing limit.
    async fn body(&self, resp: reqwest::Response) -> Result<Vec<u8>, UpstreamError> {
        let mut out = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(unavailable)?;
            if out.len() + chunk.len() > self.options.max_listing_bytes {
                return Err(UpstreamError::Unavailable(format!(
                    "the answer is larger than {} bytes",
                    self.options.max_listing_bytes
                )));
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    async fn v2_caps(&self) -> Result<V2Caps, UpstreamError> {
        let resp = self
            .send(
                self.request(
                    reqwest::Method::GET,
                    self.url.join("info/refs?service=git-upload-pack"),
                )
                .header("Git-Protocol", "version=2"),
            )
            .await?;
        Ok(wire::parse_v2_advertisement(&self.body(resp).await?)?)
    }

    fn upload_pack(&self, body: Vec<u8>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, self.url.join("git-upload-pack"))
            .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
            .header(ACCEPT, "application/x-git-upload-pack-result")
            .header("Git-Protocol", "version=2")
            .body(body)
    }

    /// Lists the upstream's branches and tags, and what `HEAD` points at.
    pub async fn ls_refs(&self) -> Result<ListedRefs, UpstreamError> {
        let caps = self.v2_caps().await?;
        let resp = self
            .send(self.upload_pack(wire::ls_refs_request(caps)))
            .await?;
        Ok(wire::parse_ls_refs(&self.body(resp).await?)?)
    }

    /// Fetches the objects `wants` reach and `haves` do not into a pack
    /// file at `out`. The pack is not thin. Returns its size.
    pub async fn fetch(
        &self,
        wants: &[ObjectId],
        haves: &[ObjectId],
        out: &Path,
        max_pack_bytes: u64,
    ) -> Result<u64, UpstreamError> {
        let caps = self.v2_caps().await?;
        let resp = self
            .send(self.upload_pack(wire::fetch_request(caps, wants, haves)))
            .await?;
        let io = |e: std::io::Error| UpstreamError::Io(e.to_string());
        let mut file = tokio::fs::File::create(out).await.map_err(io)?;
        let mut reader = wire::FetchReader::new(max_pack_bytes);
        let mut stream = resp.bytes_stream();
        let mut pending = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(unavailable)?;
            reader.feed(&chunk, &mut pending)?;
            if !pending.is_empty() {
                file.write_all(&pending).await.map_err(io)?;
                pending.clear();
            }
        }
        reader.finish()?;
        file.sync_all().await.map_err(io)?;
        Ok(reader.pack_bytes())
    }

    /// The upstream's refs and push capabilities.
    pub async fn push_advertisement(&self) -> Result<PushAdvertisement, UpstreamError> {
        let resp = self
            .send(self.request(
                reqwest::Method::GET,
                self.url.join("info/refs?service=git-receive-pack"),
            ))
            .await?;
        Ok(wire::parse_push_advertisement(&self.body(resp).await?)?)
    }

    /// Pushes `updates` with the pack at `pack` (needed unless every update
    /// is a delete), and returns the upstream's report.
    pub async fn push(
        &self,
        advert: &PushAdvertisement,
        updates: &[RefUpdate],
        pack: Option<&Path>,
        atomic: bool,
    ) -> Result<PushReport, UpstreamError> {
        let head = wire::push_request(advert.caps, updates, atomic)?;
        let body = if wire::push_sends_pack(updates) && pack.is_none() {
            // Nothing new to send (a branch at an existing commit): Git
            // still sends a pack, an empty one.
            let mut b = head;
            b.extend_from_slice(&empty_pack());
            reqwest::Body::from(b)
        } else if let Some(pack) = pack.filter(|_| wire::push_sends_pack(updates)) {
            let file = tokio::fs::File::open(pack)
                .await
                .map_err(|e| UpstreamError::Io(e.to_string()))?;
            let rest = tokio_util::io::ReaderStream::new(file);
            let stream = futures_util::stream::once(async move {
                Ok::<_, std::io::Error>(bytes::Bytes::from(head))
            })
            .chain(rest);
            reqwest::Body::wrap_stream(stream)
        } else {
            reqwest::Body::from(head)
        };
        let resp = self
            .send(
                self.request(reqwest::Method::POST, self.url.join("git-receive-pack"))
                    .header(CONTENT_TYPE, "application/x-git-receive-pack-request")
                    .header(ACCEPT, "application/x-git-receive-pack-result")
                    .body(body),
            )
            .await?;
        let body = self.body(resp).await?;
        Ok(wire::parse_push_report(&body, advert.caps.side_band_64k)?)
    }
}

/// A pack with no objects: the 12-byte header and its SHA-1 trailer.
pub(crate) fn empty_pack() -> Vec<u8> {
    let mut p = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
    let digest = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &p);
    p.extend_from_slice(digest.as_ref());
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    #[test]
    fn the_empty_pack_is_gits() {
        let p = empty_pack();
        assert_eq!(p.len(), 32);
        // `git pack-objects --stdout </dev/null | sha1sum` trailer.
        assert_eq!(
            p[12..]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            "029d08823bd8a8eab510ad6ac75c823cfd3ed31e"
        );
    }

    /// A one-shot HTTP server: answers every request with `response` and
    /// reports each request's head.
    async fn server(response: String) -> (String, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
                let _ = s.write_all(response.as_bytes()).await;
                let _ = s.shutdown().await;
            }
        });
        (base, rx)
    }

    #[tokio::test]
    async fn a_credential_never_follows_a_redirect_to_another_origin() {
        let (other, mut other_seen) =
            server("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into())
                .await;
        let (base, mut seen) = server(format!(
            "HTTP/1.1 302 Found\r\nLocation: {other}/elsewhere.git/info/refs?service=git-upload-pack\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
        .await;
        let up = Upstream::new(
            http_client(HttpOptions::default()).unwrap(),
            UpstreamUrl::parse(&format!("{base}/acme.git")).unwrap(),
            Some(Secret::new("Basic c2VjcmV0")),
            HttpOptions::default(),
        )
        .unwrap();
        let err = up.ls_refs().await.unwrap_err();
        assert!(
            matches!(err, UpstreamError::Unavailable(ref m) if m.contains("302")),
            "{err}"
        );
        let first = seen.recv().await.unwrap();
        assert!(first.contains("authorization: Basic c2VjcmV0"), "{first}");
        assert!(
            other_seen.try_recv().is_err(),
            "the other origin must never see the request (or the credential)"
        );
    }

    #[test]
    fn a_credential_that_is_no_header_value_is_refused_not_dropped() {
        let url = UpstreamUrl::parse("https://example.com/acme.git").unwrap();
        let http = http_client(HttpOptions::default()).unwrap();
        for bad in ["Basic abc\n", "Bearer a\rb", "Basic \u{0}"] {
            let r = Upstream::new(
                http.clone(),
                url.clone(),
                Some(Secret::new(bad)),
                HttpOptions::default(),
            );
            assert!(matches!(r, Err(UpstreamError::BadAuthorization)), "{bad:?}");
        }
        let ok = Upstream::new(
            http,
            url,
            Some(Secret::new("Basic abc")),
            HttpOptions::default(),
        )
        .unwrap();
        assert!(
            !format!("{ok:?}").contains("abc"),
            "the value is never in Debug output"
        );
    }

    #[test]
    fn urls_are_normalised_and_checked() {
        let u = UpstreamUrl::parse("github.com/acme/mono").unwrap();
        assert_eq!(u.as_str(), "https://github.com/acme/mono.git");
        assert_eq!(u.forge(), Forge::GitHub);
        assert_eq!(u.repo_path(), "acme/mono");
        let u = UpstreamUrl::parse("https://gitlab.com/g/sub/proj.git/").unwrap();
        assert_eq!(u.as_str(), "https://gitlab.com/g/sub/proj.git");
        assert_eq!(u.forge(), Forge::GitLab);
        assert_eq!(u.repo_path(), "g/sub/proj");
        assert!(
            UpstreamUrl::parse("http://127.0.0.1:8080/r.git")
                .unwrap()
                .is_loopback()
        );
        for bad in [
            "https://user:pw@github.com/a/b.git",
            "https://github.com/a/b.git?x=1",
            "ssh://git@github.com/a/b.git",
            "git@github.com:a/b.git",
            "https://github.com/",
        ] {
            assert!(UpstreamUrl::parse(bad).is_err(), "{bad}");
        }
    }
}
