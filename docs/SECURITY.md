# Security model and review

This document describes what Aspen defends against, how each public
surface is protected, and the results of the pre-launch security review
(node D4 in [PLAN.md](PLAN.md)). Read it before you expose an Aspen
instance to the internet.

> **Reporting a vulnerability.** The repository is private and no
> disclosure address has been chosen yet. Until one is published here,
> report issues privately to the maintainers; do not open a public issue.

## 1. Threat model

### Assets

| Asset | Where it lives | Why it matters |
|---|---|---|
| Repository contents | Packs in the object store, metadata in Postgres/SQLite | Private code; integrity of every clone |
| Ref state | `refs` / `ref_versions` tables | Who can move a branch decides what ships |
| Credentials | API tokens (SHA-256 digests only), SSH keys (public keys only), OIDC trust configuration | A stolen credential is an identity |
| Server secrets | SSH host key, `/-/blobs` URL key, express signing keys, mirror tokens and GitHub App keys, `PGPASSWORD` | Impersonating the server, forging download URLs, writing upstream |
| Availability and budget | CPU, memory, disk, database connections, egress | The public demo has a $300/month budget |

### Actors

- **Anonymous internet clients.** They can reach the Git HTTP listener,
  the SSH listener, `/healthz` and `/readyz`, signed blob and express URLs
  they are given, and `place`'s API. They may hold public SSH keys of real
  users (these are often published) and may control many addresses
  (an IPv6 /48 gives 65,536 /64s; botnets).
- **Authenticated callers** with a token, SSH key or OIDC identity, with
  grants on some repositories, who try to reach other repositories, refs
  outside their write patterns, or to exhaust shared resources.
- **Pushers** control the bytes of every pack they send (zip bombs, delta
  cycles, deep chains, duplicate objects, malformed objects).
- **A compromised upstream** in mirror mode, which controls what Aspen
  fetches.
- **Out of scope:** an attacker with access to the database, the object
  store, the host, or the operator's secret store; side channels on shared
  hardware.

### Security goals

1. No request reaches repository data without an access decision for that
   repository, and a hidden repository is indistinguishable from a missing
   one.
2. A caller can write only refs its grants allow, and only through the
   push path's checks.
3. Secrets never reach logs, error messages, URLs, process listings or
   Terraform state.
4. Every input a stranger controls is bounded in bytes, count, memory and
   time before the server commits resources to it.
5. Stored packs keep their invariants (self-contained, base-first, no
   duplicates) whatever a pusher sends.

## 2. Surfaces and their controls

### Authentication (`crates/aspen-auth`)

- **API tokens** are `aspen_<id>_<secret>` with a 256-bit secret; only its
  SHA-256 is stored and digests are compared in constant time
  (`TokenDigest` has no `PartialEq`). A presented but bad credential is
  401, never downgraded to anonymous.
- **OIDC identity tokens** are verified with `ring`: RS256/384/512 and
  ES256/384 only (`none` and HMAC refused), key type and `alg` must fit,
  `crit` refused, then `iss`, `aud`, `exp`, `nbf` (numbers only), `sub`.
  A token is verified only by the issuers whose `iss` it names, so a
  forged token costs one issuer's signature checks (D4).
- **SSH keys**: Ed25519 and ECDSA only (RSA refused, no `rsa` crate),
  looked up by fingerprint after russh verified the client's signature.
  The fingerprint is public, so only the SSH transport builds an SSH-key
  credential; no HTTP header ever becomes one.
- **Failed authentications are limited per client address** (D4):
  `LimitsConfig::auth_failures`, on by default (100 at once, then one a
  second). Every presented credential that fails, over HTTP, over SSH (a
  key offer, and each re-check of an authenticated key before a command,
  issue #243) and at `place`'s kill switch, drains the address's bucket; while it is
  empty, a request that presents a credential is refused (HTTP 429, SSH
  key refused) before the credential costs a database read or a signature
  check. Each credential takes its token before it is checked and gives
  it back when it authenticates (or the store is down), so requests that
  arrive together cannot all pass on one token: however many run at once,
  at most the bucket's level of credentials are checked (issue #198).

### Authorization (`aspen_auth::policy`, `aspen_server::access`)

- One decision per request, by repository name, before the repository is
  looked up: anonymous callers without a grant get 401, signed-in callers
  without a grant 404 (the same as a missing repository), read-only
  callers that push 403. Every push command is checked again against the
  caller's ref patterns (`ng <ref> permission denied`).
- One route parser (`GitRoute`) for HTTP and SSH; handlers take the
  repository only from the `Authorized` extension the guard inserts.
  Several `Authorization` headers are refused (400).
- Tokens and keys hold exactly their own grants plus the anonymous ones.

### Git over HTTP (`aspen_server`, `aspen serve`)

- **Listener** (`aspen_server::listen`, D4): request headers must arrive
  within 30 s of the first byte (`axum::serve` sets no hyper timer, so
  before D4 headers never timed out); HTTP/2 connections are pinged every
  30 s and carry at most 100 streams.
- **Bodies**: fetch bodies are read whole, at most 9 MiB as sent and after
  gunzip (#118; the v2 parser takes 8 MiB); push bodies stream under the
  per-push byte cap and the caller's hourly byte budget. Every Git request
  body fails after 60 s with no bytes (`ServerConfig::body_idle_timeout`,
  D4), and after its first 10 s of waiting once it averages under 1 KiB/s
  (`ServerConfig::body_min_bytes_per_sec`, #118).
- **Concurrent work** (`aspen_server::admission`, #118): fetches, pushes
  and stitches each hold a slot from admission (before the body is read)
  until they end, however they end. Per caller (the rate-limit key, over
  HTTP and SSH together; `LimitsConfig::concurrent`, 16/4/8 by default)
  an extra request is `429`; per instance (`ASPEN_MAX_CONCURRENT_*`,
  32/16/32) it waits in a bounded queue (64, at most 10 s) and is then
  `503`; both with `Retry-After`, and a retryable `ERR` over SSH and for a
  stitch.
- **Protocol limits** (A1, A1b): byte, line and count caps on every
  pkt-line request, negotiation rounds, ref names, push options.
- **Pack ingest** (A2, B1): object count checked against the header before
  the body is read; every declared object and delta size checked against
  the object limit and the inflated total before gix allocates; delta
  depth capped at 4095; duplicate objects refused; prefetch memory shared
  across pushes through a gate.
- **Rate limits** (B3): per-caller token buckets for reads and pushes;
  anonymous callers are keyed by IPv4 address or IPv6 /64. When a limiter's
  table is full, only refilled buckets are dropped; otherwise the new
  caller is refused until one refills, so nobody can reset a drained bucket
  by churning addresses (D4).
- **Health**: `/healthz` touches nothing; `/readyz` answers from a
  database check at most one second old, shared by concurrent probes (D4).
- **Metrics** are on a private listener (`ASPEN_METRICS_LISTEN`, default
  `127.0.0.1:9464`), never the Git listener except in loopback-only
  `aspen dev`.

### Git over SSH (`aspen_server::ssh`, [SSH.md](SSH.md))

- Public-key authentication only; session channels only; one Git command
  per session with Git's own quoting parsed without a shell; only
  `GIT_PROTOCOL` from the environment. The key is identified again for
  every command (and before every protocol v2 command after the first),
  through the same charged check as an offer: a failed re-check drains
  the address's failed-authentication bucket, and while it is empty the
  command is refused without a lookup (issue #243).
- **Admission (D4)**: at most 4,096 connections in total and 10
  unauthenticated connections per address; a connection that has not
  authenticated within 30 s is disconnected, key exchange included.
- **Offers (D4)**: russh does not count an accepted unsigned key offer as
  an attempt, so a client holding only a registered *public* key could
  make one database read per repeated offer. Each offered fingerprint is
  now looked up once per connection, at most 20 distinct keys per
  connection, and refused keys drain the address's failed-authentication
  bucket.
- Commands refused before they reach a repository, and shell greetings,
  cost a request from the subject's bucket (D4).

### Signed URLs (`/-/blobs`, `aspen-express`, presigned bucket URLs)

- HMAC-SHA256 over the key and expiry (domain-separated for express), with
  the signature and expiry in the path for express so a CDN never caches a
  forged URL. Every refusal is the same `403`. TTLs are 15 minutes by
  default and at most 1 hour (`RECOMMENDED_MAX_PRESIGN_TTL`).
- `/-/blobs` responses carry `X-Content-Type-Options: nosniff` and
  `Content-Security-Policy: sandbox` (D4), since a pusher chose the bytes.

### Mirror mode (`aspen-mirror`)

- Credentials are named, read from the environment or files, never stored
  in the database or logged; sent only over HTTPS or to loopback;
  redirects followed only within the same origin. Imported history goes
  through the push path, so every stored-pack invariant holds for it.

### Secrets and configuration (`aspen-cli`, `deploy/`)

- `DATABASE_URL` must not carry a password (`PGPASSWORD` instead), checked
  by `aspen serve`, the admin commands, `aspen mirror` and `aspen doctor
  jobs` (the last two since D4), and hidden in `--help` output.
- Types holding secrets redact themselves in `Debug` (`Secret`,
  `TokenDigest`, `UrlKey`, `LocalUrlSigner`, SSH settings, mirror
  credentials, and since D4 the `/-/blobs` URL key in `ServeConfig`).
- The SSH host key is never generated; it comes from a file or the secret
  store and is never echoed.
- Terraform keeps the database password out of state (write-only
  attributes), CloudFront fronts only packs, and tasks accept traffic only
  from their load balancers. Behind the AWS ALB, `ASPEN_TRUSTED_PROXY_HOPS`
  is 1, so anonymous limits key on the client, not the ALB (D4). The
  header is read only on connections from `ASPEN_TRUSTED_PROXIES` (default:
  private, shared and link-local ranges, where the ALB connects from), so
  a client that reaches a task some other way is keyed by its own address
  (#121).

### Dependencies

- `cargo deny` (required CI) checks RustSec advisories, licenses, bans and
  sources. No advisory is ignored (`deny.toml` `ignore = []`).
- OpenSSL and aws-lc are kept out (rustls on `ring`); the `rsa` crate
  (RUSTSEC-2023-0071) is kept out by refusing RSA SSH keys and not using
  `jsonwebtoken`.

## 3. Review results (D4)

Three independent read-only audits (HTTP surface; SSH and the auth crate;
secrets, deployment and dependencies) plus the obligations earlier nodes
recorded for D4 in the PLAN decisions log.

### Fixed in the D4 pull request, each with a test

| # | Finding | Severity | Test (mutation in `crates/aspen-server/scripts/check-mutations-d4.py`) |
|---|---|---|---|
| 1 | Failed authentication was not rate limited: each bad token cost a database read, each bad JWT a signature check per issuer and key, over HTTP, SSH and `place`'s kill switch (B3 obligation 1, C7 obligation 1) | High | `failed_authentications_are_limited_per_address` in `aspen-server/tests/access.rs` and `ssh_limits.rs`, `auth_failures_are_limited_per_address_and_successes_are_free` (D2, D3, D7) |
| 2 | A JWT was verified by every configured issuer | Medium | `a_forged_token_is_checked_by_the_issuer_it_names_only` (D4) |
| 3 | A repeated unsigned SSH offer of a registered public key cost a database read each time, unbounded per connection | High | `repeated_offers_of_a_registered_key_cost_one_lookup`, `distinct_offers_per_connection_are_capped` (D5, D6) |
| 4 | SSH had no connection caps, no per-address limit and no login deadline (C3 obligation) | High | `connections_are_capped_in_total`, `unauthenticated_connections_per_address_are_capped`, `a_connection_must_authenticate_within_the_deadline` (D8 to D10) |
| 5 | A full limiter table dropped the least recently used half, resetting drained buckets (B3 obligation 2, C7 obligation 4) | Medium | `a_full_table_refuses_new_keys_and_resets_nothing` (D1) |
| 6 | HTTP request headers never timed out (no hyper timer under `axum::serve`) | Medium | `slow_headers_time_out_and_requests_see_their_peer` (D12) |
| 7 | A stalled request body held its request (and for a push a blocking thread) forever (B2 follow-up) | Medium | `an_idle_body_times_out_and_a_slow_one_does_not`, `a_stalled_git_request_body_is_cut_off` (D13, D14) |
| 8 | `/readyz` ran a database query per anonymous request | Medium | `readyz_follows_the_metadata_database` (D15) |
| 9 | The AWS module did not set `ASPEN_TRUSTED_PROXY_HOPS`, so all anonymous callers behind the ALB shared one bucket | High (deployment) | `deploy/terraform/aws/tests/module.tftest.hcl` assert |
| 10 | `aspen mirror` and `aspen doctor jobs` accepted a password in `DATABASE_URL`; `aspen mirror --help` showed its value | Low | `operator_commands_refuse_and_hide_a_password_in_the_database_url` (D16 to D18) |
| 11 | `ServeConfig`'s derived `Debug` would print the `/-/blobs` URL key | Low | `the_local_url_secret_needs_a_file_store_a_public_url_and_32_bytes` (D19) |
| 12 | SSH commands refused before reaching a repository, and shell greetings, cost a key lookup but no request | Low | `refused_commands_and_greetings_draw_on_the_request_bucket` (D11) |
| 13 | `/-/blobs` responses lacked `nosniff` | Low | `the_blob_route_checks_signatures_and_serves_ranges` (D20) |
| 14 | (Found in review of this PR.) With the failure table full of drained buckets, a new address was admitted with a full bucket and its failures could not be charged, so the failure limit stopped working; a push budget admitted a new caller the same way | High | `a_full_table_refuses_unchecked_credentials_from_new_addresses` (D21) |
| 15 | (Found in review of this PR.) Requests without a client address shared one failure bucket, so a few bad credentials could lock out every credential; `place` built its kill-switch key by hand and answered a limited caller 403 | Medium | `an_unknown_address_does_not_share_a_failure_bucket`, `bad_operator_credentials_drain_the_git_failure_bucket` (D22 to D24) |
| 16 | (Found in review of this PR.) IPv4 clients of a `[::]` listener are IPv4-mapped IPv6 addresses; keyed by /64 they would all share one bucket | Medium | `anonymous_ipv6_callers_are_keyed_by_their_slash_64` (D25) |

### Filed as issues (label `security`)

| Issue | Finding | Severity |
|---|---|---|
| #118 | No per-caller or per-instance cap on concurrent fetches, pushes and stitch work; `ls-refs` in memory; push bodies on blocking threads. Fixed: fetches, pushes and stitches are capped per caller (`429`) and per instance (bounded queue, then `503`), both with `Retry-After`; fetch bodies at most 9 MiB; bodies under 1 KiB/s after 10 s fail. `ls-refs` still builds its response in memory, now under a fetch slot | Medium-high |
| #119 | `ref-prefix` lines multiply database work and can exceed Postgres's bind-parameter limit. Fixed: at most 1,024 lines per request; the store drops repeated and covered prefixes, skips ranges behind the page cursor and binds at most 250 ranges per statement (SQLite already failed at about 1,000) | Medium |
| #120 | AWS NLB and Helm SSH Service hide client addresses, so per-address SSH limits would key on the load balancer (D5) | Medium |
| #121 | `X-Forwarded-For` trusted from any peer; ports and short headers fall back to one shared bucket. Fixed: the header is read only from `ASPEN_TRUSTED_PROXIES` peers, entry by entry from raw bytes, ports stripped; an unreadable entry keys by the TCP peer and is counted (`aspen_client_address_fallbacks_total`) | Low-medium |
| #122 | Release and CI workflows use tag-pinned actions and unverified tool downloads | Medium (before publishing) |
| #123 | Express cache fills have no concurrency or disk cap. Fixed: fills are capped and reserve their size against the cache budget before copying (`docs/EXPRESS.md`, limits) | Low |
| #124 | `place`: SSE streams uncapped and unbounded in age; `start_swarm` checks in two steps (C7 obligations 2, 3). Fixed: streams are capped per address (16) and in all (4,096) and end within 10 minutes; a start checks and charges in one step (`demo/place/server/README.md`) | Low-medium |
| #125 | Deployment hardening: no default request rates, images not digest-pinned, secret file modes, metrics NetworkPolicy. Fixed: every deploy path sets read and push rates and `aspen serve` warns without them; default images are digest-pinned (`deploy/scripts/check-image-pins.py`) and the AWS module refuses an unpinned image; secret files other users can read stop startup; the Helm chart has an optional metrics NetworkPolicy (`deploy/README.md`) | Low |
| #198 | The failed-authentication limit overshoots under concurrency: requests from one address that arrive together all pass the check before any failure is charged (raised on #121). Fixed: a credential takes its token before it is checked and gives it back when it authenticates or the store is down (`Limits::charge_credential`); `concurrent_bad_credentials_cannot_overshoot_the_failure_bucket` (mutations D43 to D46) | Low |
| #243 | SSH re-checks of an authenticated key (before each command, and before each protocol v2 command after the first) called the key store directly and were never charged to the failed-authentication bucket, so a connection kept open after its key was revoked could make unbounded uncharged lookups. Fixed: every re-check goes through `Access::identify` with the connection's address; `failed_rechecks_of_a_revoked_key_drain_the_failure_bucket`, `failed_v2_rechecks_drain_the_failure_bucket` (mutations D47 to D49) | Low |

### Accepted risks

- **Signed URLs outlive revocation** by at most their TTL (15 minutes by
  default, 1 hour at most), the same as presigned bucket URLs (C2 row).
  Packfile-URI downloads carry no credential, so per-download checks are
  not possible without a revocation epoch.
- **Limits are per instance.** With N instances a caller gets up to N
  times the configured rates (B3 row).
- **The failed-authentication limit is per address**, so callers behind
  one NAT share it: an attacker failing from that address also blocks
  valid credentials from it until the bucket refills (one a second by
  default). Anonymous requests are never affected. Because a credential
  holds a token while it is checked (#198), no more credentials from one
  address are checked at once than its bucket holds (100 by default):
  callers behind one NAT that present more than that in the same instant
  get 429 for the rest, until checks finish (milliseconds, unless the
  database is slow).
- **A full limiter table refuses new callers** until a tracked bucket
  refills, rather than reset spent buckets: a request from a new caller
  gets 429, and so does a credential from a new address, before it is
  checked. An attacker who drains 100,000 failure buckets (the default
  `max_keys`; two IPv6 /48s hold that many /64s) keeps every new address
  from signing in for as long as it keeps them drained, about 100 failures
  from each address every 100 seconds. Addresses already tracked are
  unaffected, and so are anonymous requests unless request rates are set.
- **Misconfigured proxy hops either lock out every credential or lift
  the failure limit.** The failure limit keys on the client address
  `ASPEN_TRUSTED_PROXY_HOPS` picks out. Set lower than the proxies in
  front, every caller gets the nearest proxy's address, so one caller's
  bad credentials lock out everyone's. Set higher, Aspen reads an entry
  the client wrote: a client that sends its own `X-Forwarded-For` chooses
  a fresh failure bucket per request, so its guessing is unlimited and
  shows on no metric. Only callers that send no header fall back to the
  nearest proxy's address, counted in
  `aspen_client_address_fallbacks_total{reason="missing"}`. No header
  content reaches the unknown address (#121), which only an embedding
  that serves without a peer address produces. The
  SSH per-address limits always key on the TCP peer; the AWS NLB
  (`preserve_client_ip`) and the Helm Service (`externalTrafficPolicy:
  Local`) pass the client's own address through (#120). A load balancer
  that does not hides every client behind its address.
- **The default trusted-proxy list trusts every private address.** With
  hops above 0 and `ASPEN_TRUSTED_PROXIES` unset, `X-Forwarded-For` is
  read from any loopback, private, shared (`100.64.0.0/10`, which
  tailnets use) or link-local peer. Anything that reaches Aspen from one
  of those without going through the proxy (another pod in the cluster
  when the Helm chart's ingress is on, a host on the VPC or a tailnet)
  can write a made-up header and choose its rate-limit and failure
  bucket. Accepted as a default because the proxy's addresses differ per
  cluster; the Helm values and the deploy README say to narrow the list
  to the ingress controller's or load balancer's addresses, or to let
  only the proxy reach the HTTP port. The AWS module allows only the
  ALB's security group to reach it.
- **`/-/metrics` on the Git listener** is possible only with
  `metrics_on_git_listener`, which only loopback-only `aspen dev` sets.

### Not verified

- The deployed public endpoint: nothing is deployed yet (D5). The launch
  gate item "public endpoint security review done; quotas and kill switch
  tested" needs D5's deployment checked against this document.
- The AWS Terraform test runs only in CI; its provider plugins do not run
  in the review's sandbox.
