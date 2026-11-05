# Configuration

`aspen serve` is configured only through environment variables, so every
deployment path (Compose, Helm, Terraform, a systemd unit) sets the same
ones. It checks them at startup and refuses to start, naming the variable,
when one is unusable: a database URL that carries a password, a policy that
does not parse, an SSH listener without a host key. `aspen serve --help`
prints the list with defaults.

This page lists every variable the `aspen` binary reads. A check in CI
(`site/build.py`) compares it with the code, so a new variable cannot ship
undocumented. The [deployment reference](reference/deploy.md#configuration-contract)
shows how each deployment path sets them.

## Server and storage

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_LISTEN` | `0.0.0.0:7070` | Address for Git over HTTP. Plain HTTP/1.1 and cleartext HTTP/2 on one port; put a TLS terminator (load balancer, ingress, Caddy) in front |
| `ASPEN_PUBLIC_URL` | unset | The URL clients use, such as `https://git.example.com`. Needed for signed `/-/blobs` URLs and for `/-/express` on this server |
| `DATABASE_URL` | required | `postgres://aspen@db:5432/aspen?sslmode=require`, or `sqlite:///var/lib/aspen/aspen.db?mode=rwc` for one machine. Must **not** contain the password |
| `PGPASSWORD` | unset | The PostgreSQL password. Kept out of the URL so a secret store can supply it on its own and it never appears in logs or process listings |
| `ASPEN_DB_MAX_CONNECTIONS` | `10` | Database connections per instance. Raise it only while the database has CPU to spare, and keep instances × connections under the database's `max_connections` |
| `ASPEN_DB_ACQUIRE_TIMEOUT_SECS` | `30` | A request waiting for a database connection fails with "internal storage error; retry" only after this many seconds pass with no connection returned to the pool (the database is unreachable, or every connection is stuck). A long queue alone never fails a request: requests take connections in arrival order. `0` waits for ever; at most `31536000` (a year) |
| `ASPEN_BLOB_URL` | required | Where packs are stored: `s3://bucket[/prefix]` for S3 and S3-compatible stores (MinIO, R2, GCS's S3 API), or `file:///path` for a local directory. S3 settings come from the usual `AWS_*` variables (`AWS_REGION`, `AWS_ENDPOINT_URL`, credentials, or the instance's role) |
| `ASPEN_BLOB_URL_SECRET` | unset | With a `file://` store only: a hex key of at least 32 bytes that signs `/-/blobs` download URLs, so clients can fetch packs over packfile URIs from this server. Needs `ASPEN_PUBLIC_URL` |
| `ASPEN_SPOOL_DIR` | `./spool` (`/var/lib/aspen/spool` in the image) | Where pushes are received and completed before upload, where the compaction worker repacks (`compact/`), where exact shallow packs are built (`shallow/`) and where mirror packs pass (`mirror/`). Use a real disk, never tmpfs, with room for about twice the largest push |
| `ASPEN_MIGRATE` | `true` | Apply database migrations at startup. Concurrent instances wait on the database's migration lock. `aspen migrate` does the same by hand |

## Access

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_POLICY_FILE` or `ASPEN_POLICY` | tokens only | The access policy, as a JSON file or inline JSON (not both) |
| `ASPEN_LIMITS_FILE` or `ASPEN_LIMITS` | no request rates (warned at startup) | Rate limits and push quotas, as a JSON file or inline JSON (not both). The compose stack, the Helm chart and the Terraform modules set read and push rates by default |
| `ASPEN_TRUSTED_PROXY_HOPS` | `0` | How many proxies in front of Aspen append to `X-Forwarded-For`. Anonymous callers, and every caller's failed sign-ins, are limited by the address the outermost trusted proxy recorded; `0` uses the TCP peer. A value higher than the real number of proxies lets clients choose their own address |
| `ASPEN_TRUSTED_PROXIES` | private ranges | Addresses or CIDR blocks (`10.0.0.0/16, 192.0.2.7`) the nearest proxy connects from. `X-Forwarded-For` is read only on connections from these, so a client that reaches Aspen directly is limited by its own address. The default trusts loopback, private, shared (`100.64.0.0/10`) and link-local addresses. A request whose forwarded address cannot be read is limited by its TCP peer and counted in `aspen_client_address_fallbacks_total` |

### Grants

API tokens, SSH keys, anonymous callers and OIDC identities all hold
**grants**, written `<permission>:<repositories>[:<refs>]`:

| Grant | Allows |
|---|---|
| `read:acme/app` | Clone and fetch `acme/app` |
| `write:acme/*` | Read and push every repository under `acme/` |
| `write:acme/app:refs/heads/agent-7/*` | Read `acme/app`; push only branches under `agent-7/` |
| `admin:*` | Everything, on every repository |

Reads are always repository-wide; only `write` grants can name refs. A
token or key holds exactly its own grants plus the anonymous ones. Create
them with:

```bash
aspen token create --subject agent:ci-7 --grant write:acme/app   # printed once
aspen token list
aspen token revoke <id>
aspen key add ~/.ssh/id_ed25519.pub --subject user:alice --grant write:acme/*
aspen key list
aspen key revoke SHA256:...
```

These commands read `DATABASE_URL` and `PGPASSWORD` like the server. The
subject is a label for logs and rate limits (`user:alice`, `agent:ci-7`).

### The access policy

Without a policy, every request needs a token or key. A policy can give
everyone some grants, and accept OIDC identity tokens from CI systems or an
identity provider:

<!-- docs-example: policy -->
```json
{
  "anonymous": ["read:public/*"],
  "oidc": [
    {
      "name": "gha",
      "issuer": "https://token.actions.githubusercontent.com",
      "audience": "https://git.example.com",
      "jwks": {
        "keys": [
          {
            "kty": "EC",
            "crv": "P-256",
            "kid": "example",
            "alg": "ES256",
            "use": "sig",
            "x": "UF7c6V6hz4GhiAJfNgXsulwjXLWAnHrS2WY1Z2Ny9vI",
            "y": "rF4cjWa6WUCvIQUGQ2hPUQCSWGcNigiGRrWmQ7eHdS8"
          }
        ]
      },
      "rules": [
        {
          "claims": { "repository": "acme/app", "ref": "refs/heads/main" },
          "grants": ["write:acme/app"]
        }
      ]
    }
  ],
  "subjects": {
    "oidc:gha:repo:acme/tools:ref:refs/heads/main": ["read:acme/*"]
  },
  "leeway_seconds": 60
}
```

- `anonymous`: grants every caller holds, signed in or not.
- `oidc`: accepted issuers. A caller sends the identity token as the HTTP
  password (or as a bearer token). Aspen checks the signature against
  `jwks` (RS256/384/512 and ES256/384; never `none` or HMAC), `iss`
  exactly, `aud` containing `audience`, `exp` and `nbf` with
  `leeway_seconds` of slack. Each rule whose claims all match adds its
  grants; a rule with no claims is refused, since it would match every
  token.
- `jwks` is the issuer's key set document, pasted in: Aspen does not fetch
  it from the issuer yet, so update the policy when the issuer rotates its
  keys. The key above is a placeholder; for GitHub Actions, use the document
  at `https://token.actions.githubusercontent.com/.well-known/jwks`.
- `subjects`: grants for one OIDC identity, by its principal ID,
  `oidc:<issuer name>:<sub claim>`.

### Rate limits and push quotas

<!-- docs-example: limits -->
```json
{
  "read": { "burst": 100, "per_second": 20 },
  "push": { "burst": 20, "per_second": 5 },
  "quota": {
    "max_pack_bytes": 1073741824,
    "max_objects": 1000000,
    "bytes_per_hour": 10737418240
  },
  "concurrent": { "fetches": 16, "pushes": 4, "stitches": 8 },
  "max_keys": 100000
}
```

- `read`, `push`: token buckets per caller (the principal when signed in,
  the client address when anonymous; IPv6 addresses count per /64). A
  caller over its rate gets `429`.
- `quota.max_pack_bytes`, `quota.max_objects`: per push. A push that
  declares a larger body (`Content-Length`) gets `413` before its body is
  read, and one that streams past the cap fails as soon as it crosses it;
  the object count is checked against the pack header before any object
  is read. Nothing of a refused push is stored.
- `quota.bytes_per_hour`: a rolling budget of pushed bytes per caller,
  charged as the push streams in.
- A push body sent gzip-compressed (JGit compresses every push) counts as
  the larger of its bytes as sent and its bytes inflated, for both the cap
  and the budget, so a small compressed body cannot unpack past either.
  The `413` check compares the declared length, which is the compressed
  size.
- `concurrent`: how many requests one caller may have running at once,
  over HTTP and SSH together, keyed like the rates. `fetches` counts
  `git-upload-pack` requests (each HTTP request while the server reads,
  plans and answers it; each SSH session for its whole length),
  `pushes` counts `git-receive-pack` requests and SSH sessions, and
  `stitches` counts inline packs being built and streamed. A request over
  a cap is refused at once: `429` with `Retry-After` over HTTP, a
  retryable `ERR` over SSH and for a stitch. The defaults are 16, 4 and
  8; `null` turns a cap off. The [instance limits](#concurrent-work) bound
  everyone together.
- `max_keys`: most callers each limiter tracks.

Every field is optional. The buckets live in each server's memory, so with
N servers behind a load balancer a caller can get up to N times the
configured rate.

## Concurrent work

These bound how much Git work one instance runs at once, whoever sends it
(issue #118). A request takes a slot before its body is read and gives it
back when it is done, however it ends: answered, failed, or abandoned by a
client that hung up. When every slot of its kind is taken, a request waits
in a queue for up to 10 seconds; with the queue full, or after the wait,
it is refused with `503` and `Retry-After` over HTTP (Git prints
`HTTP 503`), or a retryable `ERR` over SSH and for a stitch. Callers are
also capped one by one (`concurrent` in the [limits](#rate-limits-and-push-quotas)).

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_MAX_CONCURRENT_FETCHES` | `32` | `git-upload-pack` requests (advertisement, `ls-refs`, each `fetch` round up to its pack) and SSH fetch sessions at once. Each can buffer a 9 MiB request and load up to a million ref versions (about 200 MB) to plan a clone |
| `ASPEN_MAX_CONCURRENT_PUSHES` | `16` | `git-receive-pack` requests and SSH push sessions at once. Each reads its pack on a blocking thread (tokio has 512, shared with gzip decoding and local blob reads) and holds delta-base prefetch memory |
| `ASPEN_MAX_CONCURRENT_STITCHES` | `32` | Inline packs being built and streamed at once. Each holds its plan's object IDs while it streams |
| `ASPEN_ADMISSION_QUEUE` | `64` | Requests of each kind that may wait for a slot; `0` refuses as soon as every slot is taken |

Size them to the instance: fetches and stitches by memory, pushes by
memory and disk bandwidth. With N instances behind a load balancer the
service runs up to N times these.

Request bodies are bounded too, for every caller: a fetch request is at
most 9 MiB (the protocol parser's 8 MiB plus framing), and any Git request
body fails once the client has sent nothing for 60 seconds, or, after its
first 10 seconds, once it has arrived slower than 1 KiB/s on average over
the time the server waited for it.

## Push limits

These bound what one push may cost the server, whatever the caller. The
defaults suit a large monorepo.

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_MAX_OBJECT_BYTES` | `536870912` (512 MiB) | Largest single object, inflated |
| `ASPEN_MAX_INFLATED_BYTES` | `8589934592` (8 GiB) | Most one push may inflate to in total; at least `ASPEN_MAX_OBJECT_BYTES` |
| `ASPEN_MAX_PUSH_BYTES` | `8589934592` (8 GiB) | Largest pack per push |
| `ASPEN_MAX_PUSH_OBJECTS` | `10000000` | Most objects per push (checked against the pack header before the body is read) |
| `ASPEN_PREFETCH_MEMORY_BYTES` | `4294967296` (4 GiB) | Memory for delta bases shared by all concurrent pushes. Pushes wait for room, at least 1 GiB each |

Memory: plan for about 140 bytes of heap per object of the largest push you
accept, plus the prefetch memory.

## Fetch delivery

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_PACKFILE_URIS` | `auto` | Hand out packfile URIs to clients that ask (`fetch.uriprotocols`). `auto` turns them on when the store can presign URLs or express routes are configured; `true` or `false` to force |
| `ASPEN_SHALLOW_PACKS` | `true` | Serve `--depth=N` fetches (N up to 64) exact shallow packs, built once per branch version and depth. `false` sends every shallow fetch the full history of its wants |
| `ASPEN_SHALLOW_MAX_REPO_BYTES` | `8589934592` (8 GiB) | The most one repository's exact shallow packs may take in the bucket. A new one evicts the least recently used first; one larger than this on its own is not stored, and its fetch gets the full history |
| `ASPEN_SHALLOW_UNUSED_SECS` | `86400` (24 hours) | How long an exact shallow pack nobody fetches is kept, for current branch tips too; at least `600` |
| `ASPEN_EXPRESS_KEYS` | unset | `id:hexsecret[,id:hexsecret...]`. Signs (first key) and verifies (every key) express and CDN URLs. The same on servers and express nodes; a secret |
| `ASPEN_EXPRESS_URLS` | unset | Comma-separated express nodes clients can reach, such as `https://git.example.com/-/express` |
| `ASPEN_EXPRESS_SERVE` | `auto` | Serve `/-/express` from this server. `auto` does when `ASPEN_EXPRESS_URLS` names `<ASPEN_PUBLIC_URL>/-/express`; `true` for a CDN whose origin is this server |
| `ASPEN_EXPRESS_TTL_SECS`, `ASPEN_EXPRESS_WINDOW_SECS` | `600`, `300` | Shortest life of a signed URL, and the window expiries round up to (together at most an hour) |
| `ASPEN_CDN_URL` | unset | Base URL of a CDN whose origin is express; tried before `ASPEN_EXPRESS_URLS` |
| `ASPEN_CDN_PAST_END` | `416` | What that CDN answers for a range past the end of a pack: `416` (URLs go only to Git 2.56 and later) or `206` (every client) |

The AWS Terraform module sets all of these itself: every task serves
`/-/express`, CloudFront in front of it is the CDN tier, and the module
generates `ASPEN_EXPRESS_KEYS` into Secrets Manager.
[Express and CDN mode](reference/express.md) explains tiers, key rotation
and caching.

## Express nodes

`aspen express` runs a standalone pack cache for a region. It reads
`ASPEN_BLOB_URL` and `ASPEN_EXPRESS_KEYS` (the same values as the servers)
and:

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_EXPRESS_LISTEN` | `0.0.0.0:7071` | Listen address (also `--listen`) |
| `ASPEN_EXPRESS_CACHE_DIR` | unset (no cache) | Disk cache directory |
| `ASPEN_EXPRESS_CACHE_BYTES` | `10737418240` (10 GiB) | Disk cache budget, packs being filled included |
| `ASPEN_EXPRESS_CACHE_FILLS` | `4` | Cache fills that may run at once (at least 1); a miss past it is served from the blob store without filling |

## Git over SSH

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_SSH_LISTEN` | off | Address for Git over SSH, such as `0.0.0.0:2222` |
| `ASPEN_SSH_HOST_KEY_FILE` or `ASPEN_SSH_HOST_KEY` | required with SSH | The host key: an unencrypted OpenSSH Ed25519 or ECDSA private key, as a file or inline from a secret store. Clients pin it, so every instance must use the same key across restarts. Aspen never generates one |

Create a host key with `ssh-keygen -t ed25519 -N '' -f host_key`. More in
[Git over SSH](reference/ssh.md).

Secret files (`ASPEN_SSH_HOST_KEY_FILE` and the mirror credentials'
`*_FILE` variables) must be regular files that other users cannot access:
Aspen refuses to start with one that is not, and warns about one a group other than its own may
read. Use `chmod 600`, or `640` with Aspen's group.

## Mirror mode

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_MIRROR_CREDENTIALS` | unset | Comma-separated names of the credentials mirrors may use, such as `github,gitlab`. Each must be complete at startup |
| `ASPEN_MIRROR_<NAME>_TOKEN` or `ASPEN_MIRROR_<NAME>_TOKEN_FILE` | | An access token for credential `<NAME>` (upper-cased, `-` as `_`) |
| `ASPEN_MIRROR_<NAME>_USERNAME` | per forge | The HTTP user name for the token: `x-access-token` on github.com, `oauth2` on gitlab.com, `git` elsewhere |
| `ASPEN_MIRROR_<NAME>_GITHUB_APP_ID`, `ASPEN_MIRROR_<NAME>_GITHUB_INSTALLATION_ID` | | A GitHub App instead of a token |
| `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY_FILE` or `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY` | | The app's private key (PEM) |
| `ASPEN_MIRROR_<NAME>_GITHUB_API_URL` | `https://api.github.com` | The REST API, for GitHub Enterprise Server |
| `ASPEN_MIRROR_SYNC_SECS` | `60` | Seconds between sync passes of each mirror |
| `ASPEN_MIRROR_WORK_DIR` | `<ASPEN_SPOOL_DIR>/mirror` | Spool for packs to and from upstreams; a real disk |
| `ASPEN_MIRROR_MAX_PACK_BYTES` | `8589934592` (8 GiB) | Largest pack fetched from an upstream |

The one-off commands `aspen import` and `aspen mirror ...` read
`DATABASE_URL`, `ASPEN_BLOB_URL` and the credentials above, and spool in
`ASPEN_WORK_DIR` (default: a directory under the system temporary
directory; also `--work-dir`). See [Mirror mode](reference/mirror.md).

## Logs, metrics and traces

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_METRICS_LISTEN` | `127.0.0.1:9464` | Private listener for Prometheus metrics at `/-/metrics`, or `off`. Must differ from `ASPEN_LISTEN`; keep it off every public load balancer |
| `ASPEN_LOG` | `info` | What is logged, in `tracing` filter syntax (`info,aspen_server=debug`) |
| `ASPEN_LOG_FORMAT` | `text` | `text`, or `json` for one JSON object per line |
| `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | unset (no traces) | Export traces over OTLP/HTTP to this collector. The first gets `/v1/traces` appended; the second is used as is |
| `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `http/protobuf` | Only `http/protobuf` is supported; anything else is refused at startup |
| `OTEL_SERVICE_NAME` | `aspen` | The traces' `service.name` |
| `OTEL_SDK_DISABLED`, `OTEL_TRACES_EXPORTER` | unset | `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` turns export off |

The OpenTelemetry exporter also honours `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_EXPORTER_OTLP_TIMEOUT` and `OTEL_RESOURCE_ATTRIBUTES`. Metric names,
the dashboard and the alert rules are in [Observability](reference/observability.md).

## Endpoints

| Path | Listener | What |
|---|---|---|
| `/<repo>.git/...` | Git | Git smart HTTP (push needs `write`, fetch `read`) |
| `/healthz` | Git | `200` while the process serves HTTP. Use for liveness |
| `/readyz` | Git | `200` once the database answers, `503` otherwise. Use for load balancer readiness |
| `/-/blobs/...` | Git | Signed pack downloads from a `file://` store |
| `/-/express/...` | Git, when served | Signed pack downloads through express |
| `/-/metrics` | metrics | Prometheus metrics |

`aspen healthcheck [--url http://127.0.0.1:7070/readyz]` exits 0 only on a
`200`; the container image has no curl, so container health checks use it.
