# Self-hosting

Every deployment runs the same `aspen serve` binary, configured from the
[environment](configuration.md). What changes between them is where
PostgreSQL, the bucket and the containers come from.

| Path | What you get | Good for |
|---|---|---|
| [`aspen dev`](getting-started.md#1-a-development-server) | SQLite and local disk, loopback only, anyone may push | Trying Aspen, tests |
| [Docker Compose](reference/deploy.md#single-host-docker-compose) | Aspen, PostgreSQL and MinIO on one host | A team on one machine |
| [Kubernetes (Helm)](reference/deploy.md#kubernetes-helm) | `aspen serve` replicas and optional express caches; bring PostgreSQL and a bucket | Existing clusters, GKE/EKS/AKS |
| [AWS (Terraform)](reference/deploy.md#aws-terraform) | ECS Fargate, RDS PostgreSQL, S3, ALB and NLB, budget alarms | AWS, in a dedicated account |
| [Google Cloud (Terraform)](reference/deploy.md#google-cloud-terraform) | Cloud Run, Cloud SQL, GCS, HTTPS load balancer and Cloud CDN, budget alarm | Google Cloud, HTTP only |
| [Azure (Terraform)](reference/deploy.md#azure-terraform) | Container Apps, PostgreSQL Flexible Server, Key Vault, budget alarm; an S3-compatible bucket | Azure, HTTP only |
| [Hetzner (Terraform)](reference/deploy.md#hetzner-terraform) | One server with Aspen, PostgreSQL, MinIO and Caddy (TLS) on a volume | A small, fixed-price host |

The [deployment reference](reference/deploy.md) has every step, setting and
cost estimate. This page covers what all of them have in common.

## What Aspen needs

- **PostgreSQL 14 or newer.** SQLite works for one process only (`aspen
  dev`, or a single `aspen serve`); the Helm chart refuses it.
- **An S3-compatible bucket**: AWS S3, Cloudflare R2, MinIO, or Google Cloud
  Storage through its S3 API. Azure Blob Storage and native GCS are not
  supported yet. A local directory (`file://`) works for one machine.
- **A local disk for the spool** (`ASPEN_SPOOL_DIR`): pushes are received
  and completed there before upload. Size it for about twice the largest
  push you accept, and do not use tmpfs.
- **Memory**: about 140 bytes per object of the largest push, plus the
  delta-base budget shared by concurrent pushes
  (`ASPEN_PREFETCH_MEMORY_BYTES`, 4 GiB by default).
- **A TLS terminator** in front of port 7070: Aspen speaks plain HTTP/1.1
  and h2c. Allow long requests (clones of large repositories) and large
  request bodies (pushes).

Several `aspen serve` instances can share one database and bucket. Each runs
its own compaction worker and mirror sync passes; they coordinate through
the database, with no leader.

## Before you open it to a team

1. **Access.** Decide who may read anonymously (`anonymous` grants in the
   [policy](configuration.md#the-access-policy)); by default nobody may.
   Give agents and CI their own tokens with narrow grants, or use OIDC so
   no token is stored at all.
2. **Limits.** Set [rate limits and push quotas](configuration.md#rate-limits-and-push-quotas)
   if strangers or runaway agents can reach the server, and set
   `ASPEN_TRUSTED_PROXY_HOPS` to the number of proxies in front of it
   (and `ASPEN_TRUSTED_PROXIES` to the addresses the nearest one connects
   from, if they are not private).
3. **Metrics stay private.** `/-/metrics` is on its own listener
   (`ASPEN_METRICS_LISTEN`); keep it off every public load balancer.
4. **SSH host key** (if you enable SSH): generate it once, keep it in your
   secret store, and give every instance the same one.
5. **Health checks.** Liveness on `/healthz`, readiness on `/readyz`.
6. **Alerts.** Load the shipped [alert rules](operations.md#metrics-and-alerts);
   the paging ones mean stored data needs attention.
7. **Backups.** Back up PostgreSQL (point-in-time recovery where your
   platform offers it) and keep versioning on the bucket. The database and
   the bucket only make sense together: the database names the packs it
   needs. A restore procedure has not been tested yet.

## Secrets

Aspen never needs a secret in a file that Terraform or Git can see:

- The database password comes from `PGPASSWORD`, never from `DATABASE_URL`.
  The Terraform modules generate it with write-only attributes, so it is not
  in Terraform state, and inject it from the platform's secret manager.
- On AWS the bucket is reached with the task's IAM role; elsewhere the S3
  keys come from the secret store.
- Express signing keys, the SSH host key and mirror credentials are
  environment variables (or files) filled from your secret store.

The one exception is Google Cloud: GCS returns the HMAC secret Aspen uses
for its S3 API only when the key is created, so it is in Terraform state.
Keep that state bucket private ([details](reference/deploy.md#google-cloud-terraform)).

## Putting Aspen in front of GitHub or GitLab

Mirror mode needs no migration: run Aspen as above, give it a credential
for the upstream, and import the repository.

```bash
export ASPEN_MIRROR_CREDENTIALS=github
export ASPEN_MIRROR_GITHUB_TOKEN_FILE=/run/secrets/github-token
aspen import github.com/acme/app --credential github
```

By default (`proxy` mode) a push is forwarded to the upstream and accepted
only if the upstream accepts it, so branch protection and required checks
keep working. Fetches are served from Aspen's own packs. See
[Mirror mode](reference/mirror.md) for `async` mode, conflicts and GitHub
App credentials.
