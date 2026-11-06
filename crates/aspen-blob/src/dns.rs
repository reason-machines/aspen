//! An in-process DNS cache for the object store's HTTP client.
//!
//! Without it every new connection to the store resolves the endpoint with
//! `getaddrinfo` on a tokio blocking thread. Under load the HTTP client
//! opens connections faster than it reuses them (bursts of parallel range
//! reads), and the throughput benchmark (bench/throughput) caught all 512
//! blocking threads of a server waiting in `getaddrinfo` for
//! `s3.us-east-1.amazonaws.com`, about 180 lookups a second, so pushes'
//! completion and every other blocking task queued behind DNS and pushes
//! took 30 to 60 s while the CPU idled.
//!
//! [`CachingResolver`] answers from a per-host cache for [`TTL`], lets one
//! lookup run per host while the others wait for it, serves the last answer
//! if a refresh fails, and rotates the addresses on each answer so new
//! connections spread over the endpoint's addresses.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use object_store::client::{DnsError, DnsFuture, DnsResolver};

/// How long an answer is used before it is looked up again.
pub(crate) const TTL: Duration = Duration::from_secs(30);

type Lookup =
    dyn Fn(String) -> DnsFuture + Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe;

#[derive(Debug, Default)]
struct Entry {
    addrs: Vec<IpAddr>,
    at: Option<Instant>,
}

struct Inner {
    lookup: Box<Lookup>,
    hosts: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Entry>>>>,
    turn: AtomicUsize,
}

/// A caching, single-flight [`DnsResolver`].
#[derive(Clone)]
pub(crate) struct CachingResolver(Arc<Inner>);

impl std::fmt::Debug for CachingResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingResolver").finish_non_exhaustive()
    }
}

impl Default for CachingResolver {
    fn default() -> Self {
        Self::with_lookup(|host| {
            Box::pin(async move {
                let addrs = tokio::net::lookup_host((host.as_str(), 443))
                    .await
                    .map_err(|e| Box::new(e) as DnsError)?;
                Ok(addrs.map(|a| a.ip()).collect())
            })
        })
    }
}

impl CachingResolver {
    /// A resolver over `lookup` (the system resolver by default).
    pub(crate) fn with_lookup(
        lookup: impl Fn(String) -> DnsFuture
        + Send
        + Sync
        + std::panic::UnwindSafe
        + std::panic::RefUnwindSafe
        + 'static,
    ) -> Self {
        Self(Arc::new(Inner {
            lookup: Box::new(lookup),
            hosts: std::sync::Mutex::new(HashMap::new()),
            turn: AtomicUsize::new(0),
        }))
    }
}

impl Inner {
    async fn resolve_host(&self, host: String) -> Result<Vec<IpAddr>, DnsError> {
        let slot = self
            .hosts
            .lock()
            .expect("not poisoned")
            .entry(host.clone())
            .or_default()
            .clone();
        // One lookup per host at a time; the others wait and reuse it.
        let mut entry = slot.lock().await;
        let fresh = entry.at.is_some_and(|t| t.elapsed() < TTL);
        if !fresh || entry.addrs.is_empty() {
            match (self.lookup)(host).await {
                Ok(addrs) if !addrs.is_empty() => {
                    entry.addrs = addrs;
                    entry.at = Some(Instant::now());
                }
                _ if !entry.addrs.is_empty() => {
                    // Keep serving the last answer; look again in a second.
                    entry.at = Instant::now().checked_sub(TTL - Duration::from_secs(1));
                }
                Ok(_) => return Err("the name resolved to no address".into()),
                Err(e) => return Err(e),
            }
        }
        let mut addrs = entry.addrs.clone();
        let n = addrs.len();
        addrs.rotate_left(self.turn.fetch_add(1, Ordering::Relaxed) % n);
        Ok(addrs)
    }
}

impl DnsResolver for CachingResolver {
    fn resolve(&self, host: &str) -> DnsFuture {
        let inner = Arc::clone(&self.0);
        let host = host.to_owned();
        Box::pin(async move { inner.resolve_host(host).await })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn counting() -> (CachingResolver, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let r = CachingResolver::with_lookup(move |_host| {
            let c = Arc::clone(&c);
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(vec![
                    "10.0.0.1".parse().unwrap(),
                    "10.0.0.2".parse().unwrap(),
                ])
            })
        });
        (r, calls)
    }

    /// Regression (bench/throughput): every new connection resolved the
    /// endpoint again on a blocking thread. Many concurrent resolutions of
    /// one host must cost one lookup, and later ones none until the TTL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_resolutions_share_one_lookup() {
        let (r, calls) = counting();
        let mut tasks = Vec::new();
        for _ in 0..200 {
            let r = r.clone();
            tasks.push(tokio::spawn(async move { r.resolve("s3.example").await }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap().len(), 2);
        }
        r.resolve("s3.example").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        r.resolve("other.example").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2, "one lookup per host");
    }

    #[tokio::test]
    async fn answers_rotate_over_the_addresses() {
        let (r, _) = counting();
        let a = r.resolve("s3.example").await.unwrap();
        let b = r.resolve("s3.example").await.unwrap();
        assert_ne!(a[0], b[0]);
    }
}
