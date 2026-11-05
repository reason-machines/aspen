# Deploying Aspen

Every way to run Aspen runs the same binary, `aspen serve`, configured from
the environment (the [contract](#configuration-contract) below):

| Path | What you get | Where |
|---|---|---|
| [On your machine](#on-your-machine-aspen-dev) | `aspen dev`: SQLite and local disk, loopback only | the `aspen` binary ([releases](#releases)) |
| [Single host](#single-host-docker-compose) | Aspen + PostgreSQL + MinIO with `docker compose` | [`compose/`](compose) |
| [Kubernetes](#kubernetes-helm) | Helm chart: `aspen serve`, optional `aspen express` caches; bring PostgreSQL and a bucket | [`helm/aspen/`](helm/aspen) |
| [AWS](#aws-terraform) | ECS Fargate, RDS PostgreSQL, S3 + CloudFront, ALB + NLB, Route53, ACM, budget alarms | [`terraform/aws/`](terraform/aws) |
| [Google Cloud](#google-cloud-terraform) | Cloud Run, Cloud SQL, GCS, HTTPS load balancer + Cloud CDN, budget alarm | [`terraform/gcp/`](terraform/gcp) |
| [Azure](#azure-terraform) | Container Apps, PostgreSQL Flexible Server, Key Vault, budget alarm; an S3-compatible bucket | [`terraform/azure/`](terraform/azure) |
| [Hetzner](#hetzner-terraform) | One server: Aspen + PostgreSQL + MinIO + Caddy (TLS) on a volume | [`terraform/hetzner-minio/`](terraform/hetzner-minio) |

The container image is built from [`docker/Dockerfile`](docker/Dockerfile);
releases publish it as `ghcr.io/reason-machines/aspen:v<version>` for
`linux/amd64` and `linux/arm64` ([Releases](#releases)).

## Configuration contract

Aspen reads its configuration from the environment. Every deployment path
sets the same variables; `aspen serve --help` lists them with their defaults.

| Variable | Example | Meaning |
|---|---|---|
| `ASPEN_LISTEN` | `0.0.0.0:7070` | Address for Git over HTTP |
| `ASPEN_SSH_LISTEN` | `0.0.0.0:2222` | Address for Git over SSH ([docs/SSH.md](../docs/SSH.md)); unset turns SSH off. Needs a host key |
| `ASPEN_SSH_HOST_KEY_FILE` or `ASPEN_SSH_HOST_KEY` | `/etc/aspen/ssh/host_key` | The SSH host key (an unencrypted OpenSSH Ed25519 or ECDSA private key) as a file, or inline from a secret store. Clients pin it, so it must be the same on every instance and across restarts; Aspen never generates one. The file must not be accessible to other users (see "Secret files" below) |
| `ASPEN_METRICS_LISTEN` | `0.0.0.0:9464` | Private listener for `/-/metrics` ([docs/OBSERVABILITY.md](../docs/OBSERVABILITY.md)), or `off`. Default `127.0.0.1:9464`, also in the image; compose and the Helm chart set `0.0.0.0:9464`. Keep it off every public load balancer |
| `ASPEN_LOG`, `ASPEN_LOG_FORMAT`, `OTEL_*` | `info`, `json` | Logs and OTLP traces ([docs/OBSERVABILITY.md](../docs/OBSERVABILITY.md)) |
| `ASPEN_PUBLIC_URL` | `https://git.example.com` | URL clients use; appears in clone URLs |
| `ASPEN_SSH_PUBLIC_HOST` | `ssh.git.example.com` | Host clients use for SSH (AWS only) |
| `DATABASE_URL` | `postgres://aspen@db:5432/aspen?sslmode=require` | PostgreSQL connection, **without** the password |
| `PGPASSWORD` | (secret) | Database password; libpq and sqlx read it when the URL has none |
| `ASPEN_BLOB_URL` | `s3://aspen-packs` | Pack store, as an `object_store` URL |
| `AWS_REGION` | `us-east-1` | Region of the bucket |
| `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP` | `http://minio:9000`, `true` | S3-compatible endpoint (compose/MinIO only) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | (secret) | Static S3 credentials (compose only; on AWS the task role is used) |
| `ASPEN_CDN_URL` | `https://cdn.git.example.com` | Base URL for `packfile-uri` downloads through a CDN whose origin is `aspen-express` ([docs/EXPRESS.md](../docs/EXPRESS.md)) |
| `ASPEN_CDN_PAST_END` | `416` | What that CDN answers for a range past the end, `416` or `206` |
| `ASPEN_EXPRESS_KEYS` | (secret) | `id:hexsecret[,...]` keys that sign and verify express and CDN URLs; the same on servers and express nodes |
| `ASPEN_EXPRESS_URLS` | `https://git.example.com/-/express` | Express nodes clients reach directly (comma-separated) |
| `ASPEN_EXPRESS_TTL_SECS`, `ASPEN_EXPRESS_WINDOW_SECS` | `600`, `300` | Life of a signed URL, and the window expiries round up to |
| `ASPEN_EXPRESS_CACHE_DIR`, `ASPEN_EXPRESS_CACHE_BYTES` | `/var/cache/aspen-express`, `10737418240` | Disk cache of an express node; the budget includes packs being filled |
| `ASPEN_EXPRESS_CACHE_FILLS` | `4` | Cache fills an express node runs at once |
| `ASPEN_BLOB_URL_SECRET` | (secret) | Hex key (at least 32 bytes) that signs `/-/blobs` URLs of a `file://` store (needs `ASPEN_PUBLIC_URL`) |
| `ASPEN_POLICY_FILE` or `ASPEN_POLICY` | `/etc/aspen/policy.json` | Access policy (`aspen_auth::PolicyConfig`: anonymous grants, OIDC issuers, per-subject grants) as a JSON file or inline JSON. Default: every request needs an API token |
| `ASPEN_LIMITS_FILE` or `ASPEN_LIMITS` | `{"push": {"burst": 20, "per_second": 5}}` | Rate limits and push quotas (`aspen_auth::LimitsConfig`). Without it: no request rates or quotas (`aspen serve` logs a warning at startup), failed authentications limited per client address (`auth_failures`: 100, then one a second; `null` turns it off), and concurrent work capped per caller (`concurrent`: 16 fetches, 4 pushes, 8 stitches; `null` turns one off). Every deploy path here sets read and push rates by default (below) |
| `ASPEN_TRUSTED_PROXY_HOPS` | `1` | Proxies in front of Aspen that append to `X-Forwarded-For`; anonymous rate limits and the failed-authentication limit key on the address they record. `0` (default) uses the TCP peer. Set it to exactly the number of proxies: see the warning below |
| `ASPEN_TRUSTED_PROXIES` | `10.0.0.0/16` | Addresses or CIDR blocks, separated by commas, that the nearest proxy connects to Aspen from. `X-Forwarded-For` is read only on connections from these; any other connection is keyed by its TCP peer. Default with hops above 0: loopback, private (`10/8`, `172.16/12`, `192.168/16`, `fc00::/7`), shared (`100.64/10`) and link-local addresses. Setting it with hops `0` refuses the start |
| `ASPEN_DB_MAX_CONNECTIONS` | `10` | Connections in the metadata database pool, per instance. Requests take them in arrival order, so a fetch storm queues instead of failing; raise it only while the database has CPU to spare, and keep instances × connections under the database's `max_connections` |
| `ASPEN_DB_ACQUIRE_TIMEOUT_SECS` | `30` | A request waiting for a metadata connection fails with "internal storage error; retry" only after this long with no connection returned to the pool (the database is unreachable, or every connection is stuck); a long queue alone never fails it. `0` waits for ever; at most `31536000` (a year) |
| `ASPEN_SPOOL_DIR` | `/var/lib/aspen/spool` | Where pushes are spooled and completed: a real disk, never tmpfs, about twice the largest push. The compaction worker every instance runs repacks under `<ASPEN_SPOOL_DIR>/compact` and needs about twice the largest cover's packs there |
| `ASPEN_MAX_OBJECT_BYTES`, `ASPEN_MAX_INFLATED_BYTES` | `536870912`, `8589934592` | Largest object, and most one push may inflate to |
| `ASPEN_MAX_PUSH_BYTES`, `ASPEN_MAX_PUSH_OBJECTS` | `8589934592`, `10000000` | Largest pack and most objects per push |
| `ASPEN_PREFETCH_MEMORY_BYTES` | `4294967296` | Delta-base memory shared by concurrent pushes |
| `ASPEN_MAX_CONCURRENT_FETCHES`, `ASPEN_MAX_CONCURRENT_PUSHES`, `ASPEN_MAX_CONCURRENT_STITCHES` | `32`, `16`, `32` | Fetch requests, push requests and inline packs this instance runs at once, whoever sends them (SSH sessions count as requests). A request beyond them waits in the queue, then gets `503` with `Retry-After` (a retryable `ERR` over SSH and for a stitch). Each caller is also capped by `concurrent` in `ASPEN_LIMITS` (16, 4 and 8 by default; `429`) |
| `ASPEN_ADMISSION_QUEUE` | `64` | Requests of each kind that may wait (at most 10 s) for one of those slots; `0` refuses as soon as every slot is taken |
| `ASPEN_PACKFILE_URIS` | `auto` | `auto` turns packfile URIs on when the store presigns URLs or express routes are set; `true`, `false` |
| `ASPEN_EXPRESS_SERVE` | `auto` | Serve `/-/express` from this server: `auto` when `ASPEN_EXPRESS_URLS` names `<ASPEN_PUBLIC_URL>/-/express`; `true` for a CDN whose origin is this server |
| `ASPEN_MIGRATE` | `true` | Apply schema migrations at startup (concurrent instances serialize on the database's migration lock) |
| `ASPEN_SHALLOW_PACKS` | `true` | Serve `git fetch --depth=N` (N up to 64) from exact shallow packs, built once per ref version and depth under `<ASPEN_SPOOL_DIR>/shallow` and stored beside the other packs, instead of sending the full history ([docs/PLAN.md](../docs/PLAN.md), node C8). `false` sends every shallow fetch the full history of its wants |
| `ASPEN_SHALLOW_MAX_REPO_BYTES` | `8589934592` | The most one repository's exact shallow packs (packs, `.idx` files and entry tables) may take in the bucket. Recording a new one evicts the repository's least recently used first; one larger than this on its own is not stored, and its fetch gets the full history (`aspen_shallow_builds_total{result="over_limit"}`). An evicted pack's blobs stay for the 2-hour grace period, so a fetch streaming it is not cut off (issue #108) |
| `ASPEN_SHALLOW_UNUSED_SECS` | `86400` | Seconds an exact shallow pack is kept after a fetch last used it, whether its version is current or not; at least `600`. Garbage collection then evicts it, and deletes its blobs 2 hours later (issue #108) |
| `ASPEN_MIRROR_CREDENTIALS` | `github,gitlab` | Names of the credentials mirror mode may use ([docs/MIRROR.md](../docs/MIRROR.md)) |
| `ASPEN_MIRROR_<NAME>_TOKEN_FILE` | (secret file) | A token for credential `<NAME>`; or `..._GITHUB_APP_ID`, `..._GITHUB_INSTALLATION_ID` and `..._GITHUB_PRIVATE_KEY_FILE` for a GitHub App. Secret files, checked like the host key |
| `ASPEN_MIRROR_SYNC_SECS` | `60` | Seconds between sync passes of each mirror, which `aspen serve` runs |
| `ASPEN_MIRROR_WORK_DIR`, `ASPEN_MIRROR_MAX_PACK_BYTES` | `/var/lib/aspen/spool/mirror`, `8589934592` | Spool for packs to and from upstreams (default `<ASPEN_SPOOL_DIR>/mirror`, a real disk), and the largest pack fetched from one |

**Request rates.** `ASPEN_LIMITS` limits requests per caller (a token's
subject, or an anonymous client address) and per instance. Every deploy path
in this directory sets it by default to

```json
{"read": {"burst": 100, "per_second": 20}, "push": {"burst": 20, "per_second": 5}}
```

(compose in [`compose/limits.json`](compose/limits.json), Helm in `limits`,
the Terraform modules in `aspen_limits_json`, the AWS module also through
`container_environment`, which overrides it; the public demo passes stricter
ones that way: read 60 then 10/s, push 20 then 2/s, and a push quota). A fleet of
agents sharing one token shares one budget, so raise the rates for it rather
than turning them off. `aspen serve` without read or push rates logs a
warning at startup.

**Secret files.** `ASPEN_SSH_HOST_KEY_FILE` and the mirror credentials'
`*_FILE` variables name secrets (in `aspen serve`, `aspen import` and
`aspen mirror`), so Aspen opens each one once (following symbolic links) and
checks what it opened before reading it: anything but a regular file (a
directory, a FIFO, a device) stops startup, as does a file other users may
access (any permission for "other"), with an error naming the variable and
the problem; and a file a group Aspen does not run in (neither its effective group nor
a supplementary one) may access is read with a warning. Use `chmod 600`, or `640` with Aspen's group
(a Kubernetes Secret mounted with `defaultMode: 0440` and the pod's
`fsGroup`, as the Helm chart does, passes).

**Images.** Every image these files pull by default is pinned to a digest:
the Dockerfile's base images and syntax frontend, compose's PostgreSQL and
MinIO, the Hetzner module's PostgreSQL, MinIO and Caddy, and the AWS
placeholder. `deploy/scripts/check-image-pins.py` (required CI) refuses an
unpinned one. Images you pass in (the Aspen image itself) must be pinned to
a version tag (`:v1.2.3`) or a digest in every Terraform module, AWS
included.

**Set `ASPEN_TRUSTED_PROXY_HOPS` to exactly the number of proxies that
append to `X-Forwarded-For`.** Too low, and every caller has the nearest
proxy's address: one caller's bad credentials then lock out every
credential, and all anonymous callers share one rate limit. Too high, and
Aspen reads an entry the client wrote itself: a client that sends its own
`X-Forwarded-For` chooses the address it is limited by on every request,
so its credential guessing is not limited, and nothing shows it. Only
callers that send no header fall back to the nearest proxy's address
(`reason="missing"` below). Aspen reads the header only on
connections from `ASPEN_TRUSTED_PROXIES`, parses each entry on its own
(an entry may carry a port, and a client's garbage entry cannot hide the
proxies' entries after it), and keys a request whose entry it cannot read
by its TCP peer. Each such request counts in
`aspen_client_address_fallbacks_total{reason}` (`untrusted_peer`,
`missing`, `malformed`); a steady rate means the hop count or the proxy
list is wrong, or something reaches Aspen around its proxy. The default
list trusts every private, shared and link-local address, so anything
that reaches Aspen from one of those without going through the proxy
(another pod in the cluster, a host on the VPC or tailnet) can choose its
address the same way. Narrow `ASPEN_TRUSTED_PROXIES` to the addresses
your proxy connects from (the ingress controller's pod CIDR, the load
balancer's subnets), or allow only the proxy to reach Aspen's HTTP port
(a NetworkPolicy, or a security group as the AWS module uses). SSH limits
always key on the TCP peer, so a load balancer in front of SSH must
preserve client addresses: the AWS module's NLB does
(`preserve_client_ip`), and so does the chart's SSH Service
(`externalTrafficPolicy: Local`). Aspen does not parse proxy protocol. See
[docs/SECURITY.md](../docs/SECURITY.md), accepted risks.

`aspen serve` refuses to start, naming the variable, when a setting is
unusable: a `DATABASE_URL` that carries a password, a URL secret without a
`file://` store, a policy that does not parse. It answers `GET /healthz`
(the process serves HTTP) and `GET /readyz` (`200` once the metadata database
answers a query, `503` otherwise); point liveness checks at the first and
load balancers at the second. `/-/metrics` is only on the metrics listener
(`aspen dev` also serves it beside Git), with the job, queue-lag and
wider-trunk gauges read from the database at most every 30 s. Git over SSH
shares the HTTP server's repositories, access decisions and rate-limit
buckets. SIGTERM stops accepting connections and
finishes the requests in flight. It speaks HTTP/1.1 and cleartext HTTP/2
(h2c) on the same port.

Repositories and API tokens are created with the same binary, against the
same `DATABASE_URL`:

```bash
aspen repo create acme/app                       # trunk refs/heads/main
aspen token create --subject agent:ci-7 --grant write:acme/app   # printed once
aspen token list                                  # never shows a secret
aspen token revoke <id>
aspen key add ~/.ssh/id_ed25519.pub --subject user:alice --grant write:acme/*   # Git over SSH
aspen key list
aspen key revoke SHA256:...
```

Git sends the token as the password: `git push https://agent:<token>@git.example.com/acme/app.git`.

The password is kept out of `DATABASE_URL` so it can come from a secret store
on its own; Terraform never sees it in plain text (see below).

CDN URLs are signed by Aspen with `ASPEN_EXPRESS_KEYS` and verified by
`aspen-express`, the CDN's origin, not by the CDN (decisions log, node C2).
The AWS Terraform below follows that design: CloudFront's origin is the
servers' own `/-/express`, and the module generates the signing key and the
SSH host key into Secrets Manager.

---

## On your machine (`aspen dev`)

```bash
cargo install --locked --git https://github.com/reason-machines/aspen aspen-cli   # or a release archive (below)
aspen dev                                # 127.0.0.1:7070, data in ./.aspen
git push http://127.0.0.1:7070/demo.git main
git clone http://127.0.0.1:7070/demo.git
```

`aspen dev` is `aspen serve` with SQLite and local disk under `--data-dir`,
the `demo` repository (more with `--repo <name>`), and packfile URIs served
from its own `/-/blobs` route. It lets anyone who can reach it push, so it
listens only on a loopback address and refuses any other.
Once a Homebrew tap exists (not yet: publishing waits on the release
gate, see "Releases"), `brew install reason-machines/tap/aspen` installs the
same binary and `brew services start aspen` runs it in the background with its data in
Homebrew's `var/aspen`.

---

## Single host (docker compose)

You need Docker with the compose plugin (`docker compose version`).

```bash
cd deploy/compose
./init-env.sh            # writes .env with random passwords; never overwrites
docker compose up -d
docker compose ps        # postgres, minio and aspen should be "healthy"
curl http://127.0.0.1:7070/healthz    # -> ok
```

What runs:

- **postgres** (`postgres:17-alpine`, pinned to a digest in `compose.yaml`),
  data in the `postgres-data` volume.
- **minio**, data in the `minio-data` volume. Upstream MinIO no longer
  publishes container images, so the default is Chainguard's maintained build
  (`cgr.dev/chainguard/minio:latest-dev`, pinned to a digest in
  `compose.yaml`). Set `MINIO_IMAGE` in `.env` to use your own.
- **minio-init**, a one-shot job that creates the `aspen-packs` bucket and a
  MinIO user `aspen` that can read and write objects in that bucket only. It is
  idempotent and re-applies `ASPEN_S3_SECRET_KEY` on every `up`, so rotating
  the key in `.env` works.
- **aspen**, `aspen serve` built from [`docker/Dockerfile`](docker/Dockerfile)
  on the first `up` (set `ASPEN_IMAGE` to use a published image), with its push
  spool on the `aspen-spool` volume. Its metrics listen on port 9464 inside the
  compose network only; SSH is off until you add a host key (comment in
  `compose.yaml`). Request rates per caller come from
  [`compose/limits.json`](compose/limits.json). Its HTTP port binds to `127.0.0.1`; set
  `ASPEN_BIND_ADDRESS=0.0.0.0` in `.env` to expose it. PostgreSQL and MinIO
  publish no ports, so packfile URIs are off (`ASPEN_PACKFILE_URIS`): clients
  could not reach `http://minio:9000`.

Create a repository and a token, then push:

```bash
docker compose exec aspen aspen repo create acme/app
docker compose exec aspen aspen token create --subject me --grant write:acme/app
git push http://me:<token>@127.0.0.1:7070/acme/app.git main
```

Settings you can put in `.env`: `ASPEN_IMAGE`, `ASPEN_HTTP_PORT`,
`ASPEN_BIND_ADDRESS`, `ASPEN_PUBLIC_URL`, `ASPEN_PACKFILE_URIS`,
`ASPEN_BUCKET`, `MINIO_ROOT_USER`, `MINIO_IMAGE`, `POSTGRES_IMAGE`,
`ASPEN_LOG`.

Stop with `docker compose down`; add `--volumes` to delete all data.

### Smoke test

[`compose/smoke.sh`](compose/smoke.sh) starts an isolated copy of the stack
(own project name, port and throwaway passwords), checks it, and tears it down:
every service healthy, `/healthz` answers, PostgreSQL accepts Aspen's
`DATABASE_URL` and `PGPASSWORD` over TCP (and rejects a wrong password),
Aspen's S3 credentials can write, read and delete objects in the bucket but
cannot create buckets, and Aspen serves Git end to end: a repository and
token from the admin commands, an anonymous push refused, a token push stored
as a pack in MinIO, a clone that passes `git fsck --strict`, the same clone
after Aspen restarts, and a clean exit on SIGTERM. CI runs it on pull
requests that touch `deploy/` and nightly
([`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml)), against
the image it just built (`ASPEN_IMAGE=aspen:ci deploy/compose/smoke.sh`).

### The Aspen image

```bash
docker build -f deploy/docker/Dockerfile -t aspen .    # from the repository root
docker run --rm aspen --version
docker run --rm aspen serve --help                     # the default command is `serve`
```

Multi-stage: compiled with the pinned Rust toolchain, shipped on
`debian:trixie-slim` with CA certificates only, running as uid 10001, with
`ASPEN_LISTEN=0.0.0.0:7070` and `ASPEN_SPOOL_DIR=/var/lib/aspen/spool` (mount
a volume there). There is no curl or wget: health checks run
`aspen healthcheck [--url http://127.0.0.1:7070/readyz]`, which exits 0 only
on a `200`. The Dockerfile's `ARG RUST_VERSION` must equal `channel` in
`rust-toolchain.toml`; required CI runs `deploy/scripts/check-rust-version.sh`
to enforce it, so change both together when you bump the toolchain. Its
`release` target packages binaries built elsewhere (the release workflow),
so multi-arch images need no emulated compile.

---

## AWS (Terraform)

[`terraform/aws`](terraform/aws) is a reusable module;
[`terraform/aws/examples/isolated`](terraform/aws/examples/isolated) is a root
module that uses it. [`terraform/aws/public-demo`](terraform/aws/public-demo)
is how Aspen's own public demo runs on it (an account stack with the budget
alarm, DNS zone and image repositories, and staging and demo environments
with the `place` canvas), with its runbook.

### Isolation comes first

Deploy Aspen into a **dedicated AWS account** (an AWS Organizations member
account works well). At minimum use a dedicated VPC, which the module always
creates; never point it at an account that runs other production systems.

- The example root sets the AWS provider's `allowed_account_ids` to
  `aws_account_id`, so Terraform refuses to run with credentials for any other
  account.
- The module shares nothing with existing infrastructure except the Route53
  hosted zone you pass in, where it adds records for its own hostnames.
- Remote state lives in a bucket in the same dedicated account.

### What it creates

| Component | Default | Notes |
|---|---|---|
| VPC | `10.42.0.0/16`, 2 AZs, public + private subnets | Default security group emptied; free S3 gateway endpoint |
| NAT gateway | off | `nat_gateway = true` adds one NAT gateway and moves tasks to private subnets |
| ECS Fargate | 1 task, 0.25 vCPU / 0.5 GiB, x86_64 | `fargate_spot`, `cpu_architecture = "ARM64"` cut cost further |
| ALB | HTTPS (TLS 1.2+/1.3), HTTP → HTTPS redirect, 600 s idle timeout | Health check `GET /healthz` |
| NLB | TCP 22 → task port 2222, client IP preserved | For Git over SSH (node C3). Tasks see each client's own address, which Aspen's per-address SSH limits key on. Health checks probe `/healthz`, so it is safe before SSH exists |
| RDS PostgreSQL 17 | `db.t4g.micro`, 20 GiB gp3 (autoscaling to 100), single-AZ, encrypted, private, `rds.force_ssl = 1`, 7-day backups, deletion protection | Password generated ephemerally, written with write-only attributes (never in state), stored in Secrets Manager, injected as `PGPASSWORD` |
| S3 pack bucket | Private, SSE-S3, versioned, TLS-only, Block Public Access | Lifecycle: abort incomplete multipart uploads after 1 day; expire old versions after 30 days |
| Express | Every task serves `/-/express` on the Git hostname (`ASPEN_EXPRESS_SERVE=true`) and signs packfile URIs with a key the module generates | Clients that set `fetch.uriprotocols=https` download packs over plain HTTPS ([docs/EXPRESS.md](../docs/EXPRESS.md)) |
| CloudFront | At `cdn.<domain_name>`, origin: the tasks' `/-/express`. Path-only cache key, origin `Cache-Control` honoured (at most 1 hour), GET/HEAD only, no compression | A viewer-request CloudFront Function refuses expired URLs and every path outside `/-/express` at the edge (CloudFront would otherwise serve an expired pack while the origin errors). CDN URLs go to git 2.56+ only (`ASPEN_CDN_PAST_END=416`). Git's smart-HTTP traffic never goes through CloudFront. Standard logs go to their own bucket for `cdn_log_retention_days` (7; `0` turns them off): cache hits never reach the tasks' logs. On a cache miss the request reaches the task through the ALB from a CloudFront edge, so with `ASPEN_TRUSTED_PROXY_HOPS=1` its per-address rate limits key on the edge's address, not the client's; express URLs authorize themselves, so this only makes those limits coarser |
| Secrets Manager | Database password, express signing key (`ASPEN_EXPRESS_KEYS`), Ed25519 SSH host key (`ASPEN_SSH_HOST_KEY`); the host's public key in SSM Parameter Store | All generated by ephemeral resources and written with write-only attributes: never in state. Bump `*_version` to rotate |
| ACM + Route53 | `domain_name`, `ssh.<domain_name>`, `cdn.<domain_name>` | DNS-validated certificates; no domain is hardcoded |
| IAM | Task role: objects in the pack bucket only, plus ECS Exec channels. Execution role: this log group, these secrets, the listed ECR repositories | Trust policies pinned to this account. The bucket has no policy that admits anyone else |
| Security groups | Tasks admit the ALB on the HTTP port and, with SSH on, the NLB; nothing else | `ASPEN_TRUSTED_PROXY_HOPS=1` relies on this: every HTTP request reaches a task through the ALB, which appends the client's address to `X-Forwarded-For`. The ALB connects from the VPC's private addresses, which the default `ASPEN_TRUSTED_PROXIES` trusts; a connection from anywhere else would be keyed by its own address |
| ECS Exec | On (`enable_ecs_exec`); sessions are not logged (`logging = "NONE"` on the cluster) | Session output is where `aspen token create` prints new tokens, so it is not kept. CloudTrail records every `ExecuteCommand` call: who, which task, which command |
| CloudWatch Logs | `/aspen/<name>`, 14-day retention | |
| AWS Budgets | $300/month total; $100/month for CloudFront | Alerts at 80% and 100% of actual and 100% of forecast spend, to email and/or SNS |

### Run it

Prerequisites: Terraform 1.11 or newer, credentials for the dedicated account,
and a public Route53 hosted zone for a domain you already own.

1. **State bucket** (once per account):

   ```bash
   aws s3api create-bucket --bucket <state-bucket> --region us-east-1
   aws s3api put-bucket-versioning --bucket <state-bucket> \
     --versioning-configuration Status=Enabled
   aws s3api put-public-access-block --bucket <state-bucket> \
     --public-access-block-configuration \
     BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
   ```

   (Outside us-east-1, add
   `--create-bucket-configuration LocationConstraint=<region>`.)

2. **Configure and apply:**

   ```bash
   cd deploy/terraform/aws/examples/isolated
   cp backend.hcl.example backend.hcl                 # state bucket, key, region
   cp terraform.tfvars.example terraform.tfvars       # account, domain, emails
   terraform init -backend-config=backend.hcl
   terraform plan -out=aspen.tfplan
   terraform apply aspen.tfplan
   curl "$(terraform output -raw http_url)/healthz"   # -> ok
   ```

   `backend.hcl`, `terraform.tfvars` and state files are ignored by git.
   AWS emails each budget recipient once to confirm the subscription.

3. **Switch to the real image**: copy a release image to ECR (`aws ecr
   create-repository`, then `skopeo copy --all docker://ghcr.io/reason-machines/aspen:v<version> docker://<ecr-repo>:v<version>`),
   set `container_image` and `ecr_repository_arns`, and apply again. Then
   create a repository and a token with ECS Exec
   (`aws ecs execute-command --cluster <name> --task <task> --container aspen --interactive --command "aspen repo create acme/app"`).
   With SSH on, publish the host key clients pin: the
   `ssh_host_public_key_parameter` output names the SSM parameter that holds
   it (`aws ssm get-parameter --name <parameter>`).

### Tearing down

The database has deletion protection and the pack bucket refuses to delete
while it holds objects. To destroy everything, set
`db_deletion_protection = false` (and `bucket_force_destroy = true` if you
want the packs gone), apply, then `terraform destroy`. RDS keeps a final
snapshot named `<name>-final`; delete it by hand when you no longer need it.

### Rotating the database password

Bump `db_password_version` and apply: a new password is generated, set on the
instance and written to Secrets Manager in the same run. Then force a new ECS
deployment so tasks pick it up:
`aws ecs update-service --cluster <name> --service <name> --force-new-deployment`.

### Cost of the default footprint

Approximate us-east-1 list prices, 730 hours a month, idle (no traffic):

| Item | Monthly |
|---|---|
| Fargate, 1 × 0.25 vCPU / 0.5 GiB | ~$9 |
| ALB (hours + minimal LCUs) | ~$18 |
| NLB for SSH (hours + minimal NLCUs) | ~$17 |
| Public IPv4 addresses (2 ALB + 2 NLB + 1 task, $3.65 each) | ~$18 |
| RDS `db.t4g.micro` single-AZ + 20 GiB gp3 | ~$14 |
| Secrets Manager (3 secrets), CloudWatch Logs, S3 at rest (a few GiB) | ~$3 |
| CloudFront (and its function, a few health probes a minute), ACM, Route53 records, Budgets | ~$0 fixed |
| **Total** | **~$79** |

Knobs: `nat_gateway = true` adds about $33 plus $0.045/GB processed;
`enable_ssh = false` saves about $24; `fargate_spot = true` cuts the Fargate
line by about 70%; `db_multi_az = true` doubles the RDS line. The default
task (0.25 vCPU / 0.5 GiB) suits trying it out: size `container_memory` for
real pushes and lower `ASPEN_PREFETCH_MEMORY_BYTES` and the push limits to
fit it (the public demo's environment root shows one sizing). Traffic costs
come on top: CloudFront egress is about $0.085/GB in North America and Europe
after the first 1 TB each month, which is why it has its own budget alarm.

### Checks without an AWS account

See [Checks without a cloud account](#checks-without-a-cloud-account): the
module tests in [`terraform/aws/tests`](terraform/aws/tests) plan the module
against mocked providers and assert the properties above: private, encrypted
database; locked-down bucket with multipart cleanup; the $300 budget with a
required recipient; the CDN in front of `/-/express` with a path-only cache
key and the viewer function that refuses expired URLs (whose own tests are
`node terraform/aws/cdn/viewer-request.test.mjs`); generated secrets written
write-only; no credentials in plain environment variables; and the
placeholder/real-image switch.

---

## Kubernetes (Helm)

[`helm/aspen`](helm/aspen) deploys `aspen serve` (two replicas by default)
and, optionally, standalone `aspen express` cache nodes. It does not run
PostgreSQL or object storage: bring a PostgreSQL 14+ database and an
S3-compatible bucket (S3, GCS through its S3 API, R2, MinIO).

```bash
kubectl create secret generic aspen-db --from-literal=password='<db password>'
helm install aspen deploy/helm/aspen \
  --set database.url='postgres://aspen@db.internal:5432/aspen?sslmode=require' \
  --set database.passwordSecret.name=aspen-db \
  --set blob.url=s3://acme-aspen-packs --set blob.env.AWS_REGION=us-east-1 \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::123456789012:role/aspen \
  --set ingress.enabled=true --set ingress.host=git.example.com --set publicUrl=https://git.example.com
helm test aspen
kubectl exec deploy/aspen -- aspen repo create acme/app
```

What the chart guarantees (checked by [`helm/check-chart.py`](helm/check-chart.py)
in required CI, and end to end on a kind cluster in the deploy workflow by
[`helm/ci/kind-e2e.sh`](helm/ci/kind-e2e.sh)):

- `helm install` fails, naming the value, without `database.url`, with a
  password in it, with SQLite (replicas share one database), without
  `database.passwordSecret`, with a `file://` store, or with express settings
  but no `express.keysSecret`;
- secrets reach pods only through `secretKeyRef`; a secret-looking name
  (`*SECRET*`, `*TOKEN*`, `*PASSWORD*`, `*_KEY`) with a plain value in
  `blob.env` or `extraEnv` fails the install; the policy and limits are a
  ConfigMap whose change rolls the pods;
- mirror mode ([docs/MIRROR.md](../docs/MIRROR.md)): `mirror.credentials`
  names each credential with a `tokenSecret` or a `githubApp` whose private
  key is a Secret, and the chart sets the `ASPEN_MIRROR_*` variables from
  them, secrets through `secretKeyRef` only;
- pods run as uid 10001 with a read-only root file system, no capabilities and
  no service-account token;
- the push spool is node disk (`emptyDir` with a size limit) or a per-pod
  PersistentVolumeClaim (`spool.ephemeralVolume`), never memory;
- liveness probes `/healthz`, readiness `/readyz`; a pod gets 300 s to finish
  long clones and pushes when stopped, and a PodDisruptionBudget keeps one
  replica up;
- `/-/metrics` listens on its own container port (9464, advertised with
  `prometheus.io/*` pod annotations), which neither the Service nor the
  ingress exposes; with `metrics.networkPolicy.enabled`, a NetworkPolicy
  keeps Git open to every source and admits only the peers in
  `metrics.networkPolicy.from` (your Prometheus) to that port, and install
  fails without any (it needs a network plugin that enforces NetworkPolicy).
  The policy names the container ports (`http`, `ssh`, `metrics`), so it
  follows them, and selects the server pods only: express pods have no
  metrics port of their own and serve `/-/metrics` on their public listener
  (tracked in #215);
- `limits` sets read and push request rates per caller by default (remove
  one with `--set limits.read=null`);
- with `ssh.enabled`, Git over SSH listens on 2222 behind its own Service
  (port 22, `LoadBalancer` by default, with `externalTrafficPolicy: Local`
  so Aspen sees client addresses), with the host key mounted from
  `ssh.hostKeySecret`; install fails without one;
- every `ASPEN_*` variable the chart sets is one the binary reads.

Behind an ingress, raise its body-size limit and timeouts (examples in
[`values.yaml`](helm/aspen/values.yaml)). `trustedProxyHops` defaults to 1
with the chart's ingress (the ingress controller) and 0 without it (the TCP
peer); set it to the real count for any other proxy chain, since a value too
high lets clients pick the address their rate limit keys on. Express nodes get their own host (`express.standalone.ingress`),
because express URLs are signed paths under the node's root.

---

## Google Cloud (Terraform)

[`terraform/gcp`](terraform/gcp) with the root
[`examples/project`](terraform/gcp/examples/project): Cloud Run (`aspen serve`,
h2c), Cloud SQL for PostgreSQL 17 on a private IP with TLS required, a GCS
pack bucket, a global HTTPS load balancer with a Google-managed certificate,
Cloud CDN on `/-/express/*` when `express_keys_secret_id` is set, and a
billing budget ($300/month by default) with email alerts. Use a dedicated
project.

- **Image.** Cloud Run cannot pull from `ghcr.io`: create an Artifact Registry
  remote repository for `ghcr.io` and use its path, pinned to a tag or digest.
- **Packs.** Aspen reaches GCS through its S3-compatible API with an HMAC key
  of its own service account (`roles/storage.objectUser` on the bucket only).
  GCS returns the HMAC secret only when the key is created, so it is in
  Terraform state: keep the state bucket private. A native GCS backend that
  would remove the key is #60.
- **Push size.** Cloud Run's only local storage is memory, so the push spool
  counts against the instance's memory. This is the one accepted exception
  to "the spool is a real disk, never tmpfs" (decisions log, node C6): the
  defaults (4 GiB instances, a 2 GiB spool) accept pushes up to 1 GiB, and a
  precondition refuses a cap above half the spool. For larger pushes run the
  Helm chart on GKE, whose spool is a disk.
- **Access.** The service checks no Google identity (`invoker_iam_disabled`):
  the load balancer sends none, so Cloud Run's default invoker check would
  answer 403 to every Git request. Ingress is limited to the load balancer,
  and Aspen's tokens and policy decide access.
- **HTTP/2.** The service port is h2c: Cloud Run caps HTTP/1 request bodies
  at 32 MiB, and a push is one request body.
- **Secrets.** The database password is generated ephemerally and written with
  write-only attributes (never in state); Cloud Run reads it from Secret
  Manager.
- **HTTP only.** Cloud Run serves one HTTP port: no Git over SSH, and the
  metrics listener is not reachable (use the Helm chart on GKE for either).
- **Operator commands** run as the `aspen-admin` Cloud Run job:
  `gcloud run jobs execute aspen-admin --args=repo,create,acme/app --wait`.
  Its output goes to Cloud Logging, so prefer OIDC grants in
  `aspen_policy_json` over API tokens there.

---

## Azure (Terraform)

[`terraform/azure`](terraform/azure) with the root
[`examples/subscription`](terraform/azure/examples/subscription): a resource
group with Container Apps (`aspen serve`, VNet-integrated), PostgreSQL 17
Flexible Server on a private address, Key Vault (RBAC) for secrets, Log
Analytics, a budget ($300/month by default) and, optionally, a custom domain
with its DNS records.

- **Packs.** Aspen's blob layer speaks S3 and the local file system, not
  Azure Blob Storage, so this module takes an S3-compatible bucket (R2, S3,
  MinIO, ...) and its access key. The secret key is an ephemeral variable
  (`TF_VAR_s3_secret_access_key`, needed on every run) written to Key Vault
  with a write-only attribute: it never enters state or a plan. A native
  Azure Blob backend is #60.
- **Spool.** The replica's ephemeral disk (`EmptyDir`, 8 GiB on the
  Consumption profile), so the default push limit is 3 GiB.
- **Operator commands**: `az containerapp exec -g aspen -n aspen --command "aspen repo create acme/app"`.
- **HTTP only.** Container Apps ingress carries HTTP, so Git over SSH and the
  metrics port are not exposed (use the Helm chart on AKS for either).
- **Deletion.** The database carries a `CanNotDelete` lock while
  `deletion_protection` is true; Key Vault has purge protection.

---

## Hetzner (Terraform)

[`terraform/hetzner-minio`](terraform/hetzner-minio) with the root
[`examples/single-host`](terraform/hetzner-minio/examples/single-host): one
Hetzner Cloud server running the compose stack (Aspen, PostgreSQL, MinIO)
behind Caddy, which gets a Let's Encrypt certificate for `domain_name`. All
state lives on a Hetzner volume (protected from deletion by default), so the
server can be rebuilt. The firewall opens 80 and 443, and SSH only to
`admin_cidrs`.

- Secrets (database, MinIO) are generated on the server at first boot into
  the volume; Terraform never sees them.
- `aspen_image` must be pinned to a version tag or digest.
- DNS: give `dns_zone` (a Hetzner DNS zone) for the A/AAAA records, or point
  `domain_name` at the `ipv4`/`ipv6` outputs yourself before first boot.
- Hetzner has no budget API; the cost is the fixed monthly price of the
  server type and volume (shown before you order).
- Operator commands: the `admin_commands` output.
- Git over SSH is off (port 22 is the server's own sshd).

---

## Checks without a cloud account

CI runs these on every pull request (`deploy-config`, part of `ci-ok`), with
no credentials and no backend:

```bash
cd deploy/terraform
terraform fmt -check -recursive
for root in aws/examples/isolated gcp/examples/project azure/examples/subscription hetzner-minio/examples/single-host \
            aws/public-demo/account aws/public-demo/environment; do
  (cd "$root" && terraform init -backend=false && terraform validate)
done
(cd gcp && cp examples/project/.terraform.lock.hcl . && terraform init -backend=false && terraform test)  # and aws, azure, hetzner-minio
(cd aws/public-demo/environment && terraform test)                    # and aws/public-demo/account
node aws/cdn/viewer-request.test.mjs                                   # the AWS CDN's edge function
tflint --init && tflint --recursive --config "$PWD/.tflint.hcl"
cd ../..
helm lint --strict deploy/helm/aspen -f deploy/helm/aspen/ci/minimal-values.yaml
python3 deploy/helm/check-chart.py     # helm, PyYAML; kubeconform if on PATH
python3 deploy/release/test_release.py
```

Each module's `tests/module.tftest.hcl` applies it against mocked providers
and asserts what its section above promises: no secret in a plain
environment variable or in state (write-only attributes), a private database
with TLS, a private bucket with multipart cleanup, liveness on `/healthz` and
readiness on `/readyz`, the spool where it belongs, the budget and its
recipients, and refusals for unpinned images and missing recipients.

---

## Upgrading

New binaries apply their migrations at startup (`ASPEN_MIGRATE`), and most
upgrades can roll one instance at a time. Exceptions are listed here.

- **Schema 0005 (pack generations and garbage collection): no rolling
  upgrade.** Stop every server built before migration 0005 before any
  server with it starts: a pre-0005 server uploads a pack to its
  generation-0 keys before registering it, so a new server's garbage
  collection could delete the blobs of a pack the old server just
  committed. Details in
  [`crates/aspen-meta/SCHEMA.md`](../crates/aspen-meta/SCHEMA.md)
  ("Garbage collection"). Every deployment path here replaces instances
  in a rolling way by default, so take the service down first:
  - **Helm:** the chart has no migration hook (the only hook is the
    `helm test` readiness check); the new pods migrate at startup, and
    both Deployments roll by default. Scale the server Deployment and,
    if `express.standalone.enabled`, the `-express` one to 0 replicas
    (`kubectl scale deployment -l app.kubernetes.io/instance=<release>
    --replicas=0`), wait for their pods to exit, then `helm upgrade`,
    which restores `replicaCount` from the values.
  - **compose** (and the Hetzner module, which runs it): `docker compose
    down` before pulling the new image and starting it again.
  - **AWS module:** the ECS service keeps 100% of its tasks healthy while
    it deploys, so apply `desired_count = 0` first, wait for the tasks to
    stop, then apply the new image with the count restored.
  - **Azure and GCP modules:** Container Apps and Cloud Run start the new
    revision before retiring the old one, and the modules cannot hold
    zero replicas through their variables; stop the running revision
    with the platform's own tools before applying the new image.

---

## Releases

[`.github/workflows/release.yml`](../.github/workflows/release.yml) runs on a
tag `v<version>` (or by hand) and builds, for Linux and macOS on x86_64 and
arm64:

1. the `aspen` binary, which must pass
   [`release/smoke-binary.sh`](release/smoke-binary.sh) on its own platform
   (`aspen dev`, push, clone, `fsck --strict`, SIGTERM), packaged as
   `aspen-<version>-<target>.tar.gz` with a SHA-256 file;
2. the multi-arch image (`linux/amd64`, `linux/arm64`) from those binaries
   (the Dockerfile's `release` target);
3. the Helm chart package and a Homebrew formula
   ([`release/aspen.rb.in`](release/aspen.rb.in), rendered by
   [`release/release.py`](release/release.py)) whose test pushes to and
   clones from `aspen dev`.

Nothing is published unless the repository variable `ASPEN_RELEASE_PUBLISH`
is `true` and the run is a tag push (or a manual run with `publish`); the
publish job also runs in the `release` environment, which can require a
reviewer. Until then every run is a dry run with its artifacts on the run.
Publishing pushes the image to `ghcr.io/reason-machines/aspen`, the chart to
`oci://ghcr.io/reason-machines/charts` and creates the GitHub release with
the archives and the formula.

To cut a release, bump `version` in `Cargo.toml` (`[workspace.package]`) and
`version`/`appVersion` in `helm/aspen/Chart.yaml` in one pull request (CI
checks they agree), merge it, and push the tag.

Without a release, build from source:

```bash
cargo install --locked --git https://github.com/reason-machines/aspen aspen-cli
```

The crates are licensed Apache-2.0 but are not on crates.io: they stay
unpublished until publishing is approved.
