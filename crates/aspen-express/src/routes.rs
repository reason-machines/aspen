//! Where packfile URIs point: tiers of express endpoints, consistent-hash
//! routing within a tier, and health.
//!
//! The fetch path asks [`Routes::uri`] for each planned pack. A route is a
//! list of **tiers**, tried in order. Each tier is one or more endpoint
//! base URLs that serve express URLs and share one past-end behaviour:
//!
//! - **CDN mode:** one tier whose endpoint is the CDN's base URL, with the
//!   express nodes as the CDN's origin. Declare the edge's past-end
//!   behaviour honestly: most CDNs answer a range at or past the end of a
//!   cached object with `416` themselves (CloudFront does), which git 2.55
//!   and older cannot recover from (S3), so such a tier only serves clients
//!   whose agent is `git/2.56` or later.
//! - **Express mode:** a tier of express nodes reached directly (their own
//!   `/-/express` route or a standalone express process). They answer past
//!   the end with an empty `206`, so they are safe for every client.
//!
//! A typical deployment lists the CDN tier first and the express tier
//! second: new clients download from the edge, older ones from the
//! regional express nodes. A pack for which no tier has a usable endpoint
//! gets no URI here; the fetch path then tries the blob store's own
//! presigned URL and finally stitches the pack into the inline pack, so an
//! express outage costs bytes on the Git connection, never a failed fetch.
//!
//! **Consistent hashing.** Within a tier, endpoints are ranked per pack by
//! rendezvous (highest-random-weight) hashing of the endpoint's base URL
//! and the pack key. Every Aspen server ranks them the same way, so all
//! fetches of one pack go to one node and each pack is cached once per
//! tier; adding or removing a node only moves the packs that node wins or
//! loses. A node marked down is skipped and the next-ranked node serves
//! its packs.
//!
//! **Health.** [`Routes::probe`] fetches `<base>/-/health` from every
//! endpoint (through the CDN, for a CDN tier, so the whole path is
//! checked) and marks an endpoint down after
//! [`ProbeConfig::failures_to_down`] consecutive failures, up again after
//! one success. [`Routes::spawn_prober`] runs it on an interval. Endpoints
//! start up. A URI handed out just before an outage is detected fails that
//! one download; the client's retry gets the next tier or a stitched pack.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aspen_core::blob::{PastEndRange, PresignedUrl};
use sha2::{Digest, Sha256};

use crate::key::PackKey;
use crate::url::{SigningPolicy, UrlConfigError, UrlKeys, check_base, sign};

/// One endpoint: a base URL that serves express URLs.
#[derive(Debug)]
pub struct Endpoint {
    base: String,
    up: AtomicBool,
    failures: AtomicU32,
}

impl Endpoint {
    /// The base URL.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Whether the endpoint is currently believed healthy.
    pub fn is_up(&self) -> bool {
        self.up.load(Ordering::Relaxed)
    }

    /// Records a probe result; returns whether the state changed.
    fn record(&self, ok: bool, failures_to_down: u32) -> bool {
        if ok {
            self.failures.store(0, Ordering::Relaxed);
            !self.up.swap(true, Ordering::Relaxed)
        } else {
            let n = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
            n >= failures_to_down && self.up.swap(false, Ordering::Relaxed)
        }
    }

    /// Sets the endpoint's state directly (an operator override, or a test
    /// standing in for the prober).
    pub fn set_up(&self, up: bool) {
        self.failures.store(0, Ordering::Relaxed);
        self.up.store(up, Ordering::Relaxed);
    }
}

/// A set of endpoints that share a past-end behaviour.
#[derive(Debug)]
pub struct Tier {
    endpoints: Vec<Endpoint>,
    past_end: PastEndRange,
}

impl Tier {
    /// A tier of `bases` (absolute `http(s)` URLs without a trailing `/`,
    /// for example `https://cdn.git.example.com` or
    /// `https://git.example.com/-/express`) answering past the end as
    /// `past_end`.
    pub fn new(bases: &[&str], past_end: PastEndRange) -> Result<Self, UrlConfigError> {
        if bases.is_empty() {
            return Err(UrlConfigError::Base(String::new()));
        }
        let mut endpoints = Vec::with_capacity(bases.len());
        for b in bases {
            check_base(b)?;
            if endpoints.iter().any(|e: &Endpoint| e.base == *b) {
                return Err(UrlConfigError::Base((*b).to_owned()));
            }
            endpoints.push(Endpoint {
                base: (*b).to_owned(),
                up: AtomicBool::new(true),
                failures: AtomicU32::new(0),
            });
        }
        Ok(Self {
            endpoints,
            past_end,
        })
    }

    /// The tier's endpoints, in configuration order.
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// What the tier answers for a range at or past the end.
    pub fn past_end(&self) -> PastEndRange {
        self.past_end
    }

    /// The endpoints ranked for `key`, best first (rendezvous hashing).
    pub fn ranked(&self, key: &PackKey) -> Vec<&Endpoint> {
        let mut v: Vec<(u64, &Endpoint)> = self
            .endpoints
            .iter()
            .map(|e| (weight(&e.base, key), e))
            .collect();
        // Ties (a 64-bit collision) fall back to configuration order, which
        // every server shares.
        v.sort_by_key(|a| std::cmp::Reverse(a.0));
        v.into_iter().map(|(_, e)| e).collect()
    }
}

fn weight(base: &str, key: &PackKey) -> u64 {
    let mut h = Sha256::new();
    h.update(base.as_bytes());
    h.update([0]);
    h.update(key.blob_key().as_str().as_bytes());
    let d = h.finalize();
    u64::from_be_bytes(d[..8].try_into().expect("8 bytes"))
}

/// What the client can use, from its fetch request.
#[derive(Debug, Clone, Copy)]
pub struct Client<'a> {
    /// The URI schemes the client listed (`fetch.uriprotocols`).
    pub schemes: &'a [String],
    /// Whether the client survives a `416` past the end (its agent is
    /// `git/2.56` or later; `aspen_core::fetch::allows_direct_bucket_uris`).
    pub tolerates_416: bool,
}

/// Health probe settings.
#[derive(Debug, Clone, Copy)]
pub struct ProbeConfig {
    /// Time between probes of every endpoint.
    pub interval: Duration,
    /// Time a probe may take before it counts as a failure.
    pub timeout: Duration,
    /// Consecutive failures that mark an endpoint down.
    pub failures_to_down: u32,
}

impl Default for ProbeConfig {
    /// Every 10 s, 3 s timeout, down after 2 failures in a row.
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(3),
            failures_to_down: 2,
        }
    }
}

/// Counters for URI emission, rendered with the server's metrics.
#[derive(Debug, Default)]
struct RouteCounters {
    uris: AtomicU64,
    unavailable: AtomicU64,
    probes_failed: AtomicU64,
}

/// The time URIs are signed at, in Unix seconds.
#[derive(Clone)]
struct Clock(Arc<dyn Fn() -> u64 + Send + Sync>);

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Clock")
    }
}

/// The express routes of one Aspen server: tiers, the signing key and
/// policy. Shared (`Arc`) between the fetch path and the prober.
#[derive(Debug)]
pub struct Routes {
    tiers: Vec<Tier>,
    keys: UrlKeys,
    policy: SigningPolicy,
    probe: ProbeConfig,
    counters: RouteCounters,
    clock: Clock,
}

impl Routes {
    /// Routes over `tiers` (tried in order), signing with `keys`' active
    /// key under `policy`.
    pub fn new(
        tiers: Vec<Tier>,
        keys: UrlKeys,
        policy: SigningPolicy,
        probe: ProbeConfig,
    ) -> Result<Self, UrlConfigError> {
        if tiers.is_empty() {
            return Err(UrlConfigError::Base(String::new()));
        }
        Ok(Self {
            tiers,
            keys,
            policy,
            probe,
            counters: RouteCounters::default(),
            clock: Clock(Arc::new(unix_now)),
        })
    }

    /// Signs with `clock` (Unix seconds) instead of the system clock: for
    /// tests and simulations that need to stay inside one signing window.
    /// Express nodes still check expiries against their own clock.
    #[must_use]
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Clock(Arc::new(clock));
        self
    }

    /// The tiers, in order.
    pub fn tiers(&self) -> &[Tier] {
        &self.tiers
    }

    /// The signing policy.
    pub fn policy(&self) -> SigningPolicy {
        self.policy
    }

    /// The URL for `key` that this client can use, signed at the current
    /// time, or `None` when no tier has one (every eligible endpoint is
    /// down, or none matches the client's schemes or past-end tolerance).
    pub fn uri(&self, key: &PackKey, client: Client<'_>) -> Option<PresignedUrl> {
        self.uri_at(key, client, (self.clock.0)())
    }

    /// [`Routes::uri`] at `now` (Unix seconds).
    pub fn uri_at(&self, key: &PackKey, client: Client<'_>, now: u64) -> Option<PresignedUrl> {
        let mut eligible = false;
        for tier in &self.tiers {
            if tier.past_end == PastEndRange::Status416 && !client.tolerates_416 {
                continue;
            }
            for e in tier.ranked(key) {
                let scheme = e.base.split_once("://").map_or("", |(s, _)| s);
                if !client.schemes.iter().any(|s| s == scheme) {
                    continue;
                }
                eligible = true;
                if !e.is_up() {
                    continue;
                }
                let expires = self.policy.expiry(now);
                self.counters.uris.fetch_add(1, Ordering::Relaxed);
                return Some(PresignedUrl {
                    url: sign(&self.keys, &e.base, key, expires),
                    ttl: Duration::from_secs(expires - now),
                    past_end: tier.past_end,
                });
            }
        }
        if eligible {
            self.counters.unavailable.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Probes every endpoint once with `client` and updates their state.
    pub async fn probe(&self, client: &reqwest::Client) {
        let probes = self.tiers.iter().flat_map(|t| &t.endpoints).map(|e| async {
            let ok = matches!(
                tokio::time::timeout(
                    self.probe.timeout,
                    client.get(format!("{}/-/health", e.base)).send(),
                )
                .await,
                Ok(Ok(r)) if r.status().is_success()
            );
            if !ok {
                self.counters.probes_failed.fetch_add(1, Ordering::Relaxed);
            }
            if e.record(ok, self.probe.failures_to_down.max(1)) {
                eprintln!(
                    "aspen: express endpoint {} is now {}",
                    e.base,
                    if ok { "up" } else { "down" }
                );
            }
        });
        futures_util::future::join_all(probes).await;
    }

    /// Probes every [`ProbeConfig::interval`] until the returned handle is
    /// aborted. Needs a Tokio runtime with timers.
    pub fn spawn_prober(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let routes = Arc::clone(self);
        tokio::spawn(async move {
            let client = probe_client(routes.probe.timeout);
            let mut tick = tokio::time::interval(routes.probe.interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                routes.probe(&client).await;
            }
        })
    }

    /// Appends the routing counters and endpoint states in the Prometheus
    /// text format.
    pub fn render(&self, s: &mut String) {
        use std::fmt::Write;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        crate::metrics::counter(
            s,
            "aspen_express_uris_total",
            "Packfile URIs signed for express or CDN endpoints.",
            get(&self.counters.uris),
        );
        crate::metrics::counter(
            s,
            "aspen_express_unavailable_total",
            "Packs that had an eligible express tier with no healthy endpoint (fell back to the store's URL or the stitched pack).",
            get(&self.counters.unavailable),
        );
        crate::metrics::counter(
            s,
            "aspen_express_probe_failures_total",
            "Failed health probes of express or CDN endpoints.",
            get(&self.counters.probes_failed),
        );
        let name = "aspen_express_endpoint_up";
        let _ = writeln!(
            s,
            "# HELP {name} Whether an express or CDN endpoint passed its last health probes.\n# TYPE {name} gauge"
        );
        for (t, tier) in self.tiers.iter().enumerate() {
            for e in &tier.endpoints {
                let _ = writeln!(
                    s,
                    "{name}{{tier=\"{t}\",endpoint=\"{}\"}} {}",
                    e.base.replace(['"', '\\'], "_"),
                    u8::from(e.is_up())
                );
            }
        }
    }
}

/// The HTTP client the prober uses. Installs `ring` as the process's
/// rustls provider if none is installed (A4 decisions row), so `https`
/// endpoints work.
pub fn probe_client(timeout: Duration) -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("an HTTP client with a crypto provider")
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::url::{UrlKey, verify};
    use aspen_core::id::{PackChecksum, RepoId};

    fn pack(n: u32) -> PackKey {
        let hex = format!("{n:040x}");
        PackKey::new(
            RepoId::new(1).unwrap(),
            PackChecksum::parse_hex(&hex).unwrap(),
            aspen_core::pack::PackGeneration::FIRST,
        )
    }

    fn keys() -> UrlKeys {
        UrlKeys::new(UrlKey::new("k", &[7; 32]).unwrap(), vec![]).unwrap()
    }

    fn routes(tiers: Vec<Tier>) -> Routes {
        Routes::new(
            tiers,
            keys(),
            SigningPolicy::default(),
            ProbeConfig::default(),
        )
        .unwrap()
    }

    fn schemes(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| (*s).to_owned()).collect()
    }

    /// Invariant: rendezvous ranking is a pure function of the endpoint set
    /// and the key (every server agrees), spreads packs over the nodes, and
    /// removing a node moves only the packs that node won.
    #[test]
    fn consistent_hashing_is_stable_and_minimal() {
        let bases = ["http://a", "http://b", "http://c", "http://d"];
        let all = Tier::new(&bases, PastEndRange::Empty206).unwrap();
        let three = Tier::new(&bases[..3], PastEndRange::Empty206).unwrap();
        let mut wins = [0usize; 4];
        for n in 0..2000 {
            let k = pack(n);
            let best = all.ranked(&k)[0].base().to_owned();
            assert_eq!(
                best,
                Tier::new(&bases, PastEndRange::Empty206)
                    .unwrap()
                    .ranked(&k)[0]
                    .base()
            );
            wins[bases.iter().position(|b| *b == best).unwrap()] += 1;
            if best != "http://d" {
                assert_eq!(three.ranked(&k)[0].base(), best, "pack {n} moved");
            }
        }
        for w in wins {
            assert!((350..650).contains(&w), "{wins:?}");
        }
    }

    /// Invariant: a URI goes only to a client that listed its scheme and
    /// can survive its tier's past-end answer; a down endpoint is skipped
    /// for the next-ranked one, and with every eligible endpoint down there
    /// is no URI (the caller falls back).
    #[test]
    fn uris_respect_schemes_past_end_and_health() {
        let r = routes(vec![
            Tier::new(&["https://cdn"], PastEndRange::Status416).unwrap(),
            Tier::new(&["http://x1", "http://x2"], PastEndRange::Empty206).unwrap(),
        ]);
        let k = pack(1);
        let s = schemes(&["http", "https"]);
        let old = Client {
            schemes: &s,
            tolerates_416: false,
        };
        let new = Client {
            schemes: &s,
            tolerates_416: true,
        };
        let u = r.uri_at(&k, new, 100).unwrap();
        assert!(u.url.starts_with("https://cdn/k/"), "{}", u.url);
        assert_eq!(u.past_end, PastEndRange::Status416);
        let u = r.uri_at(&k, old, 100).unwrap();
        assert_eq!(u.past_end, PastEndRange::Empty206);
        let first = r.tiers()[1].ranked(&k)[0].base().to_owned();
        assert!(u.url.starts_with(&format!("{first}/")));
        // The URL verifies for the pack it names.
        let path = &u.url[first.len() + 1..];
        assert_eq!(verify(&keys(), path, 100).unwrap().key, k);
        assert!(u.ttl >= r.policy().ttl() && u.ttl < r.policy().max_lifetime());

        r.tiers()[1].ranked(&k)[0].set_up(false);
        let u = r.uri_at(&k, old, 100).unwrap();
        assert!(!u.url.starts_with(&format!("{first}/")));
        for e in r.tiers()[1].endpoints() {
            e.set_up(false);
        }
        assert!(r.uri_at(&k, old, 100).is_none());
        // The CDN still serves new clients.
        assert!(r.uri_at(&k, new, 100).is_some());
        // Schemes the client did not list are never used (S3).
        let h = schemes(&["https"]);
        assert!(
            r.uri_at(
                &k,
                Client {
                    schemes: &h,
                    tolerates_416: false
                },
                100
            )
            .is_none()
        );
        let mut text = String::new();
        r.render(&mut text);
        assert!(text.contains("aspen_express_unavailable_total 1"), "{text}");
        assert!(
            text.contains("aspen_express_endpoint_up{tier=\"1\",endpoint=\"http://x1\"} 0"),
            "{text}"
        );
    }

    /// Invariant: an endpoint goes down only after the configured number of
    /// consecutive failures, and up again on one success.
    #[test]
    fn health_needs_consecutive_failures() {
        let t = Tier::new(&["http://a"], PastEndRange::Empty206).unwrap();
        let e = &t.endpoints()[0];
        assert!(!e.record(false, 2));
        assert!(e.is_up());
        assert!(!e.record(true, 2));
        assert!(!e.record(false, 2));
        assert!(e.record(false, 2));
        assert!(!e.is_up());
        assert!(e.record(true, 2));
        assert!(e.is_up());
    }
}
