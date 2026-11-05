//! The express service over HTTP, against a local blob store: what an edge
//! cache and `git http-fetch` see.
//!
//! The packs here are opaque byte strings with a valid pack header and
//! SHA-1 trailer: the express never parses a pack, it only checks that the
//! bytes hash to the checksum in their key. Real Git packs and stock `git`
//! are in `crates/aspen-server/tests/express.rs`.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aspen_blob::LocalBlobStore;
use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PastEndRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::id::{PackChecksum, RepoId};
use aspen_core::pack::PackGeneration;
use aspen_express::url::sign;
use aspen_express::{
    CacheConfig, Client, Express, ExpressConfig, PackKey, ProbeConfig, Refusal, Routes,
    SigningPolicy, Tier, UrlKey, UrlKeys,
};
use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, CONTENT_RANGE, ETAG, HeaderMap};
use sha1::{Digest, Sha1};

fn keys() -> UrlKeys {
    UrlKeys::new(UrlKey::new("k1", &[0x5a; 32]).unwrap(), vec![]).unwrap()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A pack-shaped blob: header, `len` payload bytes from `seed`, trailer.
fn pack_bytes(seed: u8, len: usize) -> (Vec<u8>, PackChecksum) {
    let mut b = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
    b.extend((0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)));
    let sum: [u8; 20] = Sha1::digest(&b).into();
    b.extend_from_slice(&sum);
    (b, PackChecksum::from_bytes(sum))
}

async fn put(store: &LocalBlobStore, dir: &Path, key: &BlobKey, bytes: &[u8]) {
    let f = dir.join(format!("upload-{}", key.as_str().replace('/', "_")));
    std::fs::write(&f, bytes).unwrap();
    store.upload_file(key, &f).await.unwrap();
}

struct Harness {
    _tmp: tempfile::TempDir,
    tmp: std::path::PathBuf,
    store: LocalBlobStore,
    express: Express<LocalBlobStore>,
    base: String,
    http: reqwest::Client,
}

impl Harness {
    async fn start(cache: Option<u64>, wait_for_fill_bytes: u64) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(tmp.path().join("blobs")).unwrap();
        let config = ExpressConfig {
            cache: cache.map(|max_bytes| CacheConfig {
                dir: tmp.path().join("cache"),
                max_bytes,
                max_fills: CacheConfig::DEFAULT_MAX_FILLS,
            }),
            wait_for_fill_bytes,
            read_bytes: 1000,
        };
        let express = Express::new(store.clone(), keys(), config).unwrap();
        let base = serve(express.router()).await;
        Self {
            tmp: tmp.path().to_owned(),
            _tmp: tmp,
            store,
            express,
            base,
            http: aspen_express::routes::probe_client(Duration::from_secs(10)),
        }
    }

    async fn add(&self, repo: i64, bytes: &[u8], checksum: PackChecksum) -> PackKey {
        let key = PackKey::new(RepoId::new(repo).unwrap(), checksum, PackGeneration::FIRST);
        put(&self.store, &self.tmp, key.blob_key(), bytes).await;
        key
    }

    fn url(&self, key: &PackKey) -> String {
        sign(
            &keys(),
            &self.base,
            key,
            SigningPolicy::default().expiry(now()),
        )
    }

    async fn get(&self, url: &str, headers: &[(&str, &str)]) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut req = self.http.get(url);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let r = req.send().await.unwrap();
        (
            r.status(),
            r.headers().clone(),
            r.bytes().await.unwrap().to_vec(),
        )
    }
}

async fn serve(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn h(headers: &HeaderMap, name: impl reqwest::header::AsHeaderName) -> &str {
    headers.get(name).map_or("", |v| v.to_str().unwrap())
}

/// Invariant: an express response is immutable and safe to share. The same
/// URL returns the same bytes (the stored pack, byte for byte) with a
/// strong ETag equal to the pack's checksum, and `Cache-Control` lets a
/// shared cache keep it only until the URL expires (`max-age` never past
/// the expiry, `must-revalidate`); `If-None-Match` gets `304`; ranges get
/// exact `206`s, a range past the end an empty `206` (never `416`), and an
/// `If-Range` that is not the ETag the whole pack. Checked with and without
/// the disk cache, and the cached copy serves identical responses.
#[tokio::test(flavor = "multi_thread")]
async fn responses_are_immutable_and_cacheable() {
    for cache in [None, Some(1 << 20)] {
        let x = Harness::start(cache, 1 << 20).await;
        let (bytes, checksum) = pack_bytes(1, 5000);
        let key = x.add(1, &bytes, checksum).await;
        let url = x.url(&key);
        let expires: u64 = url[x.base.len() + 1..]
            .split('/')
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let etag = format!("\"{}\"", checksum.to_hex());

        // Read before the request: the server's clock is at least this, so
        // its `max-age` (expiry minus its clock) is at most `expires - before`.
        // Reading after would fail whenever a second ticks in between.
        let before = now();
        let (s1, h1, b1) = x.get(&url, &[]).await;
        let (s2, h2, b2) = x.get(&url, &[]).await;
        assert_eq!((s1, s2), (StatusCode::OK, StatusCode::OK));
        assert_eq!(b1, bytes);
        assert_eq!(b2, bytes);
        assert_eq!(h(&h1, ETAG), etag);
        assert_eq!(h(&h2, ETAG), etag);
        let cc = h(&h1, CACHE_CONTROL);
        let max_age: u64 = cc
            .split(", ")
            .find_map(|d| d.strip_prefix("max-age="))
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            max_age > 0 && before + max_age <= expires,
            "{cc} vs {expires}"
        );
        for d in ["public", "immutable", "must-revalidate", "no-transform"] {
            assert!(cc.split(", ").any(|x| x == d), "{cc}");
        }
        assert_eq!(h(&h1, "content-type"), "application/x-git-packed-objects");
        assert_eq!(h(&h1, "accept-ranges"), "bytes");

        let head = x.http.head(&url).send().await.unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(h(head.headers(), "content-length"), bytes.len().to_string());
        assert_eq!(h(head.headers(), ETAG), etag);

        let (s, hd, b) = x.get(&url, &[("if-none-match", &etag)]).await;
        assert_eq!(s, StatusCode::NOT_MODIFIED);
        assert!(b.is_empty());
        assert_eq!(h(&hd, ETAG), etag);

        let size = bytes.len();
        let (s, hd, b) = x.get(&url, &[("range", "bytes=10-19")]).await;
        assert_eq!(s, StatusCode::PARTIAL_CONTENT);
        assert_eq!(h(&hd, CONTENT_RANGE), format!("bytes 10-19/{size}"));
        assert_eq!(b, &bytes[10..20]);
        assert_eq!(h(&hd, ETAG), etag);

        let (s, hd, b) = x.get(&url, &[("range", "bytes=4000-")]).await;
        assert_eq!(s, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            h(&hd, CONTENT_RANGE),
            format!("bytes 4000-{}/{size}", size - 1)
        );
        assert_eq!(b, &bytes[4000..]);

        let past = format!("bytes={size}-");
        let (s, hd, b) = x.get(&url, &[("range", &past)]).await;
        assert_eq!(s, StatusCode::PARTIAL_CONTENT, "never 416");
        assert_eq!(h(&hd, CONTENT_RANGE), format!("bytes */{size}"));
        assert!(b.is_empty());

        let (s, _, b) = x
            .get(&url, &[("range", "bytes=10-19"), ("if-range", &etag)])
            .await;
        assert_eq!((s, b.len()), (StatusCode::PARTIAL_CONTENT, 10));
        let (s, _, b) = x
            .get(&url, &[("range", "bytes=10-19"), ("if-range", "\"other\"")])
            .await;
        assert_eq!((s, b), (StatusCode::OK, bytes.clone()));

        if cache.is_some() {
            // The first request filled the cache; every later one hit it.
            assert_eq!(x.express.metrics().misses(), 1);
            assert!(x.express.metrics().hits() >= 7);
            assert_eq!(x.express.metrics().fills(), 1);
        }
    }
}

/// A local blob store whose `size` takes [`SLOW_STORE_DELAY`]: the store
/// lookup every miss starts with, so each uncached request spends that
/// long between arriving and its response going out.
#[derive(Clone)]
struct SlowStore(LocalBlobStore);

const SLOW_STORE_DELAY: Duration = Duration::from_secs(4);

impl BlobStore for SlowStore {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.0.upload_file(key, path).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        self.0.read_range(key, range).await
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        tokio::time::sleep(SLOW_STORE_DELAY).await;
        self.0.size(key).await
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        self.0.delete(key).await
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        self.0.presign_get(key, ttl).await
    }
}

/// Invariant: `max-age` counts down to the URL's expiry from when the
/// response goes out, not from when the request arrived. A cache keeps a
/// response for `max-age` from when it receives it, so a `max-age` taken
/// before a slow store lookup or cache fill would let a shared cache serve
/// the pack past the URL's expiry by that delay (#68). With a store that
/// takes [`SLOW_STORE_DELAY`] to answer, the clock read after the response
/// plus `max-age` may pass the expiry only by clock granularity and
/// transit, never by the store's delay. Checked with and without the disk
/// cache (a miss waits for the fill there).
#[tokio::test(flavor = "multi_thread")]
async fn max_age_counts_from_the_response_not_the_request() {
    for cache in [None, Some(1 << 20)] {
        let tmp = tempfile::tempdir().unwrap();
        let local = LocalBlobStore::open(tmp.path().join("blobs")).unwrap();
        let (bytes, checksum) = pack_bytes(2, 5000);
        let key = PackKey::new(RepoId::new(1).unwrap(), checksum, PackGeneration::FIRST);
        put(&local, tmp.path(), key.blob_key(), &bytes).await;
        let config = ExpressConfig {
            cache: cache.map(|max_bytes| CacheConfig {
                dir: tmp.path().join("cache"),
                max_bytes,
                max_fills: CacheConfig::DEFAULT_MAX_FILLS,
            }),
            wait_for_fill_bytes: 1 << 20,
            read_bytes: 1000,
        };
        let express = Express::new(SlowStore(local), keys(), config).unwrap();
        let base = serve(express.router()).await;
        let expires = SigningPolicy::default().expiry(now());
        let url = sign(&keys(), &base, &key, expires);

        let r = aspen_express::routes::probe_client(Duration::from_secs(30))
            .get(&url)
            .send()
            .await
            .unwrap();
        let after = now();
        assert_eq!(r.status(), StatusCode::OK);
        let cc = h(r.headers(), CACHE_CONTROL).to_owned();
        assert_eq!(r.bytes().await.unwrap(), bytes);
        let max_age: u64 = cc
            .split(", ")
            .find_map(|d| d.strip_prefix("max-age="))
            .unwrap()
            .parse()
            .unwrap();
        // A `max-age` taken when the request arrived overshoots by at least
        // the store's delay; the slack below that covers whole-second
        // clocks and the time from the server's clock read to `after`.
        assert!(
            after + max_age < expires + SLOW_STORE_DELAY.as_secs(),
            "{cc}: received at {after}, URL expires at {expires}"
        );
    }
}

/// Invariant: a URL opens exactly the pack it was signed for, until it
/// expires. Forged signatures, expired URLs, unknown keys, other blob kinds,
/// and a URL for repository 1's pack rewritten to repository 2's copy of
/// the **same bytes** (same checksum) are all refused with the same `403`,
/// which no cache may store (`no-store`); the express counts each reason.
#[tokio::test(flavor = "multi_thread")]
async fn forged_expired_and_cross_repo_urls_are_refused() {
    let x = Harness::start(Some(1 << 20), 1 << 20).await;
    let (bytes, checksum) = pack_bytes(2, 300);
    let k1 = x.add(1, &bytes, checksum).await;
    let k2 = x.add(2, &bytes, checksum).await;
    let good = x.url(&k1);
    assert_eq!(x.get(&good, &[]).await.0, StatusCode::OK);
    assert_eq!(x.get(&x.url(&k2), &[]).await.0, StatusCode::OK);

    let sig = good[x.base.len() + 1..]
        .split('/')
        .nth(2)
        .unwrap()
        .to_owned();
    let flipped = format!(
        "{}{}",
        if sig.starts_with('0') { '1' } else { '0' },
        &sig[1..]
    );
    let expired = sign(&keys(), &x.base, &k1, now() - 1);
    let other_key = UrlKeys::new(UrlKey::new("k1", &[0x77; 32]).unwrap(), vec![]).unwrap();
    let wrong_secret = sign(
        &other_key,
        &x.base,
        &k1,
        SigningPolicy::default().expiry(now()),
    );
    let cases = [
        (
            good.replace("/repos/1/", "/repos/2/"),
            Refusal::BadSignature,
        ),
        (good.replacen(&sig, &flipped, 1), Refusal::BadSignature),
        (wrong_secret, Refusal::BadSignature),
        (expired, Refusal::Expired),
        (good.replacen("/k1/", "/k9/", 1), Refusal::UnknownKey),
        (good.replace(".pack", ".idx"), Refusal::NotAPack),
        (
            format!("{}/repos/1/packs/{}.pack", x.base, checksum.to_hex()),
            Refusal::Malformed,
        ),
    ];
    let mut first_body = None;
    for (url, reason) in &cases {
        let before = x.express.metrics().refusals(*reason);
        let (s, hd, b) = x.get(url, &[]).await;
        assert_eq!(s, StatusCode::FORBIDDEN, "{url}");
        assert_eq!(h(&hd, CACHE_CONTROL), "no-store", "{url}");
        assert!(hd.get(ETAG).is_none());
        assert_eq!(x.express.metrics().refusals(*reason), before + 1, "{url}");
        assert_eq!(first_body.get_or_insert_with(|| b.clone()), &b);
    }
    // HEAD and ranges are refused the same way.
    let forged = good.replacen(&sig, &flipped, 1);
    let r = x.http.head(&forged).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        x.get(&forged, &[("range", "bytes=0-")]).await.0,
        StatusCode::FORBIDDEN
    );
    // A validly signed URL for a pack that is not stored is a plain 404,
    // also never cached.
    let (missing_bytes, missing) = pack_bytes(3, 10);
    drop(missing_bytes);
    let gone = PackKey::new(RepoId::new(1).unwrap(), missing, PackGeneration::FIRST);
    let (s, hd, _) = x.get(&x.url(&gone), &[]).await;
    assert_eq!(
        (s, h(&hd, CACHE_CONTROL)),
        (StatusCode::NOT_FOUND, "no-store")
    );
    // Health is open and never cached; other methods are refused.
    let (s, hd, _) = x.get(&format!("{}/-/health", x.base), &[]).await;
    assert_eq!((s, h(&hd, CACHE_CONTROL)), (StatusCode::OK, "no-store"));
    let r = x.http.post(&good).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// Invariant: the disk cache only keeps bytes that hash to the checksum in
/// their key. A stored blob that does not is still streamed from the store
/// (the client checks the pack hash itself), but is never cached, and it
/// counts as a corruption alert.
#[tokio::test(flavor = "multi_thread")]
async fn the_cache_keeps_only_packs_that_match_their_key() {
    let x = Harness::start(Some(1 << 20), 1 << 20).await;
    let (mut bytes, checksum) = pack_bytes(4, 2000);
    bytes[100] ^= 1;
    let key = x.add(1, &bytes, checksum).await;
    let url = x.url(&key);
    for _ in 0..2 {
        let (s, _, b) = x.get(&url, &[]).await;
        assert_eq!((s, b == bytes), (StatusCode::OK, true));
    }
    let m = x.express.metrics();
    assert_eq!((m.hits(), m.misses(), m.fills()), (0, 2, 0));
    assert_eq!(m.corrupt_blobs(), 2);
    let mut text = String::new();
    m.render(&mut text);
    assert!(
        text.contains("aspen_alert_express_corrupt_pack_total 2"),
        "{text}"
    );
}

/// Invariant: the cache stays within its byte budget by evicting the least
/// recently used pack; a restarted node keeps (and serves) what is on disk
/// and deletes anything that is not a canonical pack.
#[tokio::test(flavor = "multi_thread")]
async fn the_cache_evicts_least_recently_used_and_survives_restarts() {
    let x = Harness::start(Some(2600), 1 << 20).await;
    let mut packs = Vec::new();
    for seed in 0..3 {
        let (bytes, checksum) = pack_bytes(10 + seed, 1000);
        packs.push((x.add(1, &bytes, checksum).await, bytes));
    }
    let urls: Vec<String> = packs.iter().map(|(k, _)| x.url(k)).collect();
    let get = |i: usize| x.get(&urls[i], &[]);
    assert_eq!(get(0).await.2, packs[0].1);
    assert_eq!(get(1).await.2, packs[1].1);
    assert_eq!(get(0).await.2, packs[0].1); // pack 0 is now more recent than 1
    assert_eq!(get(2).await.2, packs[2].1); // evicts pack 1
    let m = x.express.metrics();
    assert_eq!((m.fills(), m.evictions()), (3, 1));
    let hits = m.hits();
    assert_eq!(get(0).await.2, packs[0].1);
    assert_eq!(m.hits(), hits + 1, "pack 0 stayed");
    let misses = m.misses();
    assert_eq!(get(1).await.2, packs[1].1);
    assert_eq!(m.misses(), misses + 1, "pack 1 was evicted");

    let cache_dir = x.tmp.join("cache");
    std::fs::write(cache_dir.join("data").join("stray"), b"x").unwrap();
    let again = Express::new(
        x.store.clone(),
        keys(),
        ExpressConfig {
            cache: Some(CacheConfig {
                dir: cache_dir.clone(),
                max_bytes: 2600,
                max_fills: CacheConfig::DEFAULT_MAX_FILLS,
            }),
            ..ExpressConfig::default()
        },
    )
    .unwrap();
    assert!(!cache_dir.join("data").join("stray").exists());
    let base = serve(again.router()).await;
    let url = sign(
        &keys(),
        &base,
        &packs[1].0,
        SigningPolicy::default().expiry(now()),
    );
    let r = x.http.get(&url).send().await.unwrap();
    assert_eq!(r.bytes().await.unwrap().to_vec(), packs[1].1);
    assert_eq!(again.metrics().hits(), 1, "served from the cache on disk");
}

/// Invariant: a miss on a pack larger than the wait threshold streams from
/// the store at once while the fill runs in the background; later requests
/// hit the cache.
#[tokio::test(flavor = "multi_thread")]
async fn large_misses_stream_while_the_cache_fills() {
    let x = Harness::start(Some(1 << 20), 100).await;
    let (bytes, checksum) = pack_bytes(20, 50_000);
    let key = x.add(1, &bytes, checksum).await;
    let url = x.url(&key);
    assert_eq!(x.get(&url, &[]).await.2, bytes);
    let m = x.express.metrics();
    for _ in 0..200 {
        if m.fills() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(m.fills(), 1);
    assert_eq!(x.get(&url, &[]).await.2, bytes);
    assert_eq!((m.misses(), m.hits()), (1, 1));
}

/// Invariant: health probes mark an endpoint down after consecutive
/// failures and up after a success, and URIs skip a down endpoint for the
/// next-ranked one. Probes go to `<base>/-/health`, the route the express
/// serves.
#[tokio::test(flavor = "multi_thread")]
async fn probes_track_endpoint_health() {
    let x = Harness::start(None, 0).await;
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", l.local_addr().unwrap())
    };
    let routes = Arc::new(
        Routes::new(
            vec![Tier::new(&[x.base.as_str(), dead.as_str()], PastEndRange::Empty206).unwrap()],
            keys(),
            SigningPolicy::default(),
            ProbeConfig {
                failures_to_down: 1,
                timeout: Duration::from_secs(2),
                ..ProbeConfig::default()
            },
        )
        .unwrap(),
    );
    routes.probe(&x.http).await;
    let ep = |base: &str| {
        routes.tiers()[0]
            .endpoints()
            .iter()
            .find(|e| e.base() == base)
            .unwrap()
            .is_up()
    };
    assert!(ep(&x.base));
    assert!(!ep(&dead));
    let schemes = vec!["http".to_owned()];
    let client = Client {
        schemes: &schemes,
        tolerates_416: false,
    };
    for n in 0..20u8 {
        let (bytes, checksum) = pack_bytes(n, 10);
        let key = x.add(1, &bytes, checksum).await;
        let uri = routes.uri(&key, client).unwrap();
        assert!(uri.url.starts_with(&format!("{}/", x.base)), "{}", uri.url);
        let r = x.http.get(&uri.url).send().await.unwrap();
        assert_eq!(r.bytes().await.unwrap().to_vec(), bytes);
    }
}
