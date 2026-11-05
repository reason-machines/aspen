# Express and CDN mode

Git clients that opt in to packfile URIs download packs over plain HTTPS
instead of receiving them inline on the Git connection:

```bash
git config --global fetch.uriprotocols https
```

Aspen then answers a fetch with an empty inline pack and one URL per stored
pack (`docs/PLAN.md`, fetch path step 3). **Express** is the service behind
those URLs: it serves stored packs through signed, short-lived, immutable
URLs from a regional disk cache, and a CDN can sit in front of it. Clients
that do not opt in, and any pack no URL source can serve, get the stitched
inline pack as before, so turning express on or off never breaks a fetch.

## What a URL looks like

```text
https://cdn.git.example.com/k1/1762181700/6f0c...e2/repos/42/packs/3b18...9a.pack
                           |  |          |         |
                           |  |          |         pack key: repository 42, pack checksum
                           |  |          HMAC-SHA256 signature (64 hex digits)
                           |  expiry, Unix seconds
                           signing key ID
```

- The signature covers the key ID, the expiry and the pack key, so a URL
  opens one pack of one repository until it expires. Rewriting the
  repository, the checksum, the expiry or the file kind gives `403`.
- Signature and expiry are **path segments**. Many CDNs drop the query
  string from their cache key by default; with the signature in the path, a
  forged or expired URL is always a different cache key and reaches the
  origin, which refuses it.
- The expiry is `now + ttl` rounded **up** to a multiple of the window, so
  every fetch of a pack within one window gets the same URL. An edge cache
  keyed on the URL then holds one entry per pack, and a fetch storm (a
  thousand CI jobs cloning one commit) is served from it.
- The URL is the credential: `git http-fetch` sends no `Authorization` to
  packfile URIs. Aspen signs URLs only for a fetch its access guard allowed,
  so a caller without a grant on a repository never gets one. A URL stays
  valid until it expires, at most `ttl + window` (one hour at most, 15
  minutes by default), even if the caller's access is revoked meanwhile.

## What express answers

| Request | Answer |
|---|---|
| `GET` or `HEAD` of a valid URL | `200` with the pack, `ETag: "<checksum>"`, `Cache-Control: public, max-age=<seconds until expiry>, immutable, must-revalidate, no-transform` |
| `If-None-Match` with the ETag | `304` |
| `Range: bytes=a-b` or `bytes=a-` | `206` with an exact `Content-Range` |
| a range starting at or past the end | `206` with an empty body and `Content-Range: bytes */<size>`, never `416` (git 2.55 and older cannot resume after a `416`) |
| `If-Range` that is not the ETag | `200` with the whole pack |
| an invalid, expired or foreign URL | `403 forbidden`, `Cache-Control: no-store`, the same bytes whatever the reason |
| a valid URL whose pack is gone | `404`, `no-store` |
| `/-/health` | `200 ok`, `no-store` |

The bytes behind a pack key never change: the key embeds the pack's SHA-1
trailer, and blobs are write-once. The disk cache only keeps a pack after
checking that its bytes hash to that checksum; a stored blob that does not
is never cached and raises `aspen_alert_express_corrupt_pack_total` (page on
it). So one URL always returns the same bytes, and any cache may share the
response until the URL's expiry.

That check covers bytes served from the disk cache. Bytes that come straight
from the blob store are streamed without checking them first: a miss on a
pack over 64 MiB (see below) or any request to an express run without a
cache. They carry the same `public, max-age` header, so if the store returns
bad bytes once, an edge cache in front can keep them until the URL expires.
Git still rejects such a pack, because it checks the pack against the hash
in the `packfile-uris` line, so the result is failed downloads until the URL
expires, never a wrong clone.

## Running it

Express runs in two places, both reading packs from the same blob store as
the Aspen servers:

- **Inside an Aspen server**, at `/-/express` on the Git listener
  (`Server::with_express_route`). Simplest; good for one region.
- **As a standalone node**, `aspen express`, for a regional pack cache:

  ```bash
  export ASPEN_BLOB_URL=s3://aspen-packs          # the same store the servers use
  export ASPEN_EXPRESS_KEYS="k1:$(cat /run/secrets/express-k1)"   # from your secret manager
  export ASPEN_EXPRESS_CACHE_DIR=/var/cache/aspen-express
  export ASPEN_EXPRESS_CACHE_BYTES=$((200 << 30))
  aspen express --listen 0.0.0.0:7071
  ```

  It serves express URLs at its root, `/-/health`, and its counters at
  `/-/metrics`.

Aspen servers sign URLs for the endpoints in their routes
(`Server::with_express_routes`), built from the environment by
`aspen_express::settings::routes`:

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_EXPRESS_KEYS` | (secret) | `id:hexsecret[,id:hexsecret...]`; each secret at least 32 bytes. The first key signs, every key verifies. Servers and express nodes need the same keys |
| `ASPEN_CDN_URL` | unset | Base URL of a CDN whose origin is express (first tier) |
| `ASPEN_CDN_PAST_END` | `416` | What the CDN answers for a range at or past the end: `416` or `206` |
| `ASPEN_EXPRESS_URLS` | unset | Comma-separated base URLs of express nodes clients reach directly, for example `https://git.example.com/-/express` (next tier) |
| `ASPEN_EXPRESS_TTL_SECS` | `600` | Shortest life of a URL |
| `ASPEN_EXPRESS_WINDOW_SECS` | `300` | Expiries round up to this; TTL + window at most 3600 |
| `ASPEN_EXPRESS_CACHE_DIR` | unset | Disk cache of an express node (no cache when unset) |
| `ASPEN_EXPRESS_CACHE_BYTES` | 10 GiB | Disk cache budget, packs being filled included |
| `ASPEN_EXPRESS_CACHE_FILLS` | `4` | Cache fills that may run at once (at least 1) |
| `ASPEN_EXPRESS_LISTEN` | `0.0.0.0:7071` | Listen address of `aspen express` |

`ServerConfig::packfile_uris` must be on for any URI to be sent.

Generate a key with `openssl rand -hex 32` and keep it in your secret
store. **Rotation:** add the new key after the current one on every express
node (`k1:...,k2:...`), then put it first on the servers (`k2:...,k1:...`),
then drop the old key everywhere once `ttl + window` has passed.

### Tiers, routing and health

Routes are tiers tried in order; each pack takes the first tier that fits
the client:

1. **CDN tier** (`ASPEN_CDN_URL`). Most CDNs answer a range past the end of
   a cached object with `416` themselves (CloudFront does). Leave
   `ASPEN_CDN_PAST_END=416` unless you have checked that your edge passes
   the origin's empty `206` through; with `416`, only clients whose agent
   is `git/2.56` or later get CDN URLs.
2. **Express tier** (`ASPEN_EXPRESS_URLS`): express nodes reached directly,
   safe for every client.
3. If neither tier has a healthy endpoint for the client, the blob store's
   own URL (Aspen's `/-/blobs` for a local store, a presigned bucket URL
   for git 2.56+), and finally the stitched inline pack.

Within a tier, each pack goes to one endpoint chosen by rendezvous
hashing of the endpoint URL and the pack key. Every server picks the same
node for a pack, so each pack is cached once per tier, and adding or
removing a node moves only that node's share.

Servers probe `<endpoint>/-/health` every 10 seconds (through the CDN for
the CDN tier, so the whole path is checked) and stop sending URLs to an
endpoint after two failed probes in a row, until one succeeds
(`Routes::spawn_prober`). A URL handed out just before an outage was
detected fails that one download; Git then fails the fetch, and the retry
gets the next tier or the stitched pack.

### Putting a CDN in front

Point the CDN's origin at the express nodes (a load balancer over
`aspen express`, or the servers' `/-/express`), and configure it to:

- forward `GET` and `HEAD` only, with `Range`, `If-None-Match` and
  `If-Range`; no cookies, no `Authorization`;
- use the full path as the cache key (the query string is never used);
- honour the origin's `Cache-Control`: cache `public` responses for their
  `max-age`, never cache `no-store` ones, and **do not serve stale
  content** (no `stale-if-error` or "serve stale on origin error"
  override): `must-revalidate` is what stops an edge from serving a URL
  after it expires, and revalidating an expired URL is refused. Some CDNs
  serve stale on origin errors regardless (CloudFront does, for the error
  caching TTL); in front of those, refuse URLs past their expiry at the edge
  as well, as the AWS module's viewer function does (the expiry is the
  second path segment, a Unix second);
- not compress responses (packs are already zlib-compressed;
  `no-transform` asks for this too).

The CDN does not verify signatures; the origin does. An edge only ever
holds responses the origin served for a valid URL, keyed by that URL, until
the URL expires.

## Metrics

On the server's `/-/metrics` (its metrics listener,
[OBSERVABILITY.md](OBSERVABILITY.md)) and a standalone node's:

| Metric | Meaning |
|---|---|
| `aspen_express_uris_total` | URLs signed for express or CDN endpoints |
| `aspen_express_unavailable_total` | packs with an eligible tier but no healthy endpoint (fell back) |
| `aspen_express_endpoint_up{tier,endpoint}` | last health state of each endpoint |
| `aspen_express_probe_failures_total` | failed health probes |
| `aspen_express_requests_total` | requests to express URLs |
| `aspen_express_refused_total{reason}` | `403`s by reason: `malformed`, `not_a_pack`, `unknown_key`, `bad_signature`, `expired` |
| `aspen_express_cache_hits_total`, `aspen_express_cache_misses_total` | requests that found the pack cached, or not |
| `aspen_express_bytes_served_total`, `aspen_express_origin_bytes_total` | bytes sent, and bytes read from the blob store |
| `aspen_express_cache_fills_total`, `aspen_express_cache_fill_failures_total`, `aspen_express_cache_evictions_total`, `aspen_express_cache_bytes` | the disk cache |
| `aspen_express_cache_fills_skipped_total` | misses that started no fill (fill cap reached, or the budget held by fills in progress) and were served from the blob store |
| `aspen_alert_express_corrupt_pack_total` | stored packs that do not hash to their key (pages) |

## Limits and known gaps

- A cache miss on a pack larger than 64 MiB streams from the blob store
  while the cache fills in the background, so the first client does not wait
  for the copy and a CDN's origin timeout is not hit. Until the fill
  completes, concurrent misses on that pack also read from the store; a
  CDN's request collapsing absorbs most of them.
- Fills are bounded. At most `ASPEN_EXPRESS_CACHE_FILLS` run at once, and
  each reserves its pack's size against `ASPEN_EXPRESS_CACHE_BYTES` before
  it copies, evicting least recently used packs to make room, so packs being
  filled count against the budget and the cache directory never holds more
  than it (#123). A
  miss that finds the cap reached, or the budget taken by fills in progress,
  starts no fill and streams from the blob store
  (`aspen_express_cache_fills_skipped_total`). A pack larger than the budget
  is never cached.
- One cache directory belongs to one process.
- At startup the cache re-indexes the packs already in its directory and
  trusts each file by its name (the pack key) and its size; it does not hash
  them again. Only verified fills write there, renamed into place after
  `fsync`, so this holds unless something else edits the directory or the
  disk corrupts a file at rest. If you suspect either, empty the cache
  directory: the express refills it from the blob store.
- The CDN configuration has been checked against a local caching proxy
  (`crates/aspen-server/tests/express.rs`), not against a real CDN. The AWS
  Terraform (`deploy/terraform/aws/cdn.tf`) puts CloudFront in front of the
  servers' `/-/express` as described above, with a viewer-request function
  that also refuses expired URLs at the edge, because CloudFront serves an
  expired cached object while the origin answers 5xx or times out, whatever
  `must-revalidate` says. Its behaviour has been tested with mocked
  providers and the function's own tests, not yet against a real
  distribution (`deploy/terraform/aws/public-demo/README.md`, step 7).
