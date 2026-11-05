# Mirror mode

Mirror mode puts Aspen in front of an existing repository on GitHub, GitLab
or any other smart-HTTP Git server (the **upstream**). Clients clone, fetch
and push through Aspen. Fetches are served from Aspen's own packs, so the
upstream no longer carries the read traffic. Pushes reach the upstream in one
of two ways:

| Mode | When a push is accepted | Source of truth |
|---|---|---|
| `proxy` (default) | After the upstream accepts it. Aspen forwards the push and commits only what the upstream took; the client sees the upstream's verdict, including its branch protection | The upstream |
| `async` | At once, as for any Aspen repository. A background job pushes it to the upstream | Aspen |

Nobody has to migrate: teams keep their forge, its pull requests and its
permissions, and point agents and CI at Aspen.

## Set up a mirror

1. Give Aspen a credential for the upstream (next section).
2. Import the repository:

   ```bash
   export DATABASE_URL=postgres://aspen@db:5432/aspen?sslmode=require  # password in PGPASSWORD
   export ASPEN_BLOB_URL=s3://aspen-packs
   export ASPEN_MIRROR_CREDENTIALS=github
   export ASPEN_MIRROR_GITHUB_TOKEN_FILE=/run/secrets/github-token
   aspen import github.com/acme/mono --credential github
   ```

   `aspen import` creates the Aspen repository (named `acme/mono` here; use
   `--name` to choose), makes the upstream's default branch its trunk, and
   copies every branch and tag. Use `--push-through async` for async mode.
   With `--mode copy` the import happens once and the repository is not
   linked to the upstream afterwards.

3. Clients use Aspen's URL as usual: `git clone https://git.example.com/acme/mono.git`.

The server keeps the mirror current: every `ASPEN_MIRROR_SYNC_SECS` (60 by
default) it runs a **sync pass** that imports what changed at the upstream
directly, such as merged pull requests. `aspen serve` runs it (and the
`mirror_push` jobs of async mode) in every server process; `aspen mirror
sync <repo>` runs one pass by hand. The library entry point is
`aspen_mirror::Mirror::run`.

Only branches (`refs/heads/*`) and tags (`refs/tags/*`) are mirrored, and
only those in the repository's writable namespaces. Forge-specific refs such
as GitHub's `refs/pull/*` are not.

## Credentials

A credential has a name, which the mirror stores; the secret stays in the
environment (or a file a secret store fills) and never enters the database
or the logs. List the names in `ASPEN_MIRROR_CREDENTIALS` (comma-separated).
For each name, upper-cased with `-` as `_`:

| Variable | Meaning |
|---|---|
| `ASPEN_MIRROR_<NAME>_TOKEN`, or `..._TOKEN_FILE` | A personal, project or fine-grained access token |
| `ASPEN_MIRROR_<NAME>_USERNAME` | The HTTP user name the token goes with. Defaults: `x-access-token` on github.com, `oauth2` on gitlab.com, `git` elsewhere (set `x-access-token` for GitHub Enterprise Server) |
| `ASPEN_MIRROR_<NAME>_GITHUB_APP_ID` | A GitHub App's ID (or client ID), instead of a token |
| `ASPEN_MIRROR_<NAME>_GITHUB_INSTALLATION_ID` | The app installation to act as |
| `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY_FILE`, or `..._GITHUB_PRIVATE_KEY` | The app's private key, PEM, as GitHub issues it |
| `ASPEN_MIRROR_<NAME>_GITHUB_API_URL` | The REST API; default `https://api.github.com`, or `https://<host>/api/v3` for GitHub Enterprise Server |

The `*_FILE` variables name secret files: `aspen serve`, `aspen import` and
`aspen mirror` refuse one that is not a regular file or that other users may
access, and warn about one a
group other than Aspen's own may (`chmod 600`).

A GitHub App credential mints an installation token (valid for one hour) with
a JWT signed by the app key, caches it, and mints a new one five minutes
before it expires. The app needs **Contents: read and write** on the
repository (read only, for a proxy mirror nobody pushes to).

Aspen sends a credential only over HTTPS (or to a loopback address, for
tests and sidecars), and follows a redirect only to the same scheme, host
and port, so a credential never reaches another host. Every name in
`ASPEN_MIRROR_CREDENTIALS` must be complete, so a typo fails at startup.

## Proxy mode

A push to a proxy mirror runs Aspen's usual checks first (the pack, the
commit check, `-o force`, the caller's ref permissions). Aspen then sends the
accepted updates to the upstream, with Aspen's current value of each ref as
the old value, so the upstream's own compare-and-swap refuses an update if
the ref moved there. Aspen commits only what the upstream accepted:

- The upstream refused (branch protection, a hook): the client gets
  `rejected by the upstream repository this mirror forwards to: <the
  upstream's reason>`, and Aspen stores nothing.
- The upstream's ref is not at Aspen's value (someone pushed to the forge
  directly, and Aspen has not synced yet): the update is not sent, the
  client gets `the upstream repository has moved past this mirror; retry
  once the mirror has synced`, and the mirror worker is asked to sync at
  once. Fetch, rebase and push again.
- The upstream cannot be reached: `the upstream repository could not be
  reached; retry`.

So a proxy mirror never holds a ref value the upstream did not accept. If
the upstream accepted an update and Aspen's own commit then lost a race, the
client is told the push succeeded (it did, at the source of truth) and a
sync brings Aspen up to date.

After the upstream accepts, Aspen records the agreement in its ledger. That
write is best effort: proxy mode never decides anything from the ledger, so
if the database fails at that moment the push still succeeds,
`aspen_mirror_ledger_write_failures_total` counts it, and the next sync pass
records the agreement. If the repository stopped being a mirror while the
push was in flight (`aspen mirror remove`), nothing is forwarded and Aspen
decides the push alone, like any other repository's
(`aspen_mirror_forwarded_total{result="not_mirror"}`).

The upstream's refusal reasons reach the client on one line: control
characters (newlines, terminal escape sequences) become spaces and the text
is cut at 512 bytes.

**Who the upstream sees.** Every forwarded push reaches the forge as the
mirror's credential: the bot user or GitHub App, never the person who
pushed to Aspen. Forge rules keyed on the pushing user (branch protection
that lets only some people push, CODEOWNERS bypass lists, push rulesets by
actor) therefore apply to the bot. Give the bot only the access the mirror
needs, and enforce per-user rules in Aspen: its access control (tokens,
OIDC grants and ref patterns, `docs/PLAN.md` B3) is the per-user gate, and
it runs before anything is forwarded.

## Async mode and conflicts

In async mode Aspen accepts pushes as the primary; each write enqueues a
`mirror_push` job in the push's own transaction, and the worker pushes the
ref to the upstream. Changes made at the upstream directly still flow back
while Aspen has not changed the same ref.

For every ref Aspen keeps a **ledger**: the value both sides last agreed on.
Each sync step compares Aspen's value, the upstream's and the ledger:

| | Aspen = upstream | Only the upstream changed | Only Aspen changed | Both changed |
|---|---|---|---|---|
| proxy | in sync | import | import (Aspen follows) | import |
| async | in sync | import | push | **conflict** |

A **conflict** is recorded and reported, and neither side is overwritten:

- `aspen_alert_mirror_conflicts` on `/-/metrics` counts refs in conflict (page on it);
- `aspen mirror status <repo>` lists them with both values;
- the `mirror_push` job completes, so it does not retry forever.

Resolve one by keeping a side:

```bash
aspen mirror resolve acme/mono refs/heads/main --keep aspen      # push Aspen's value over the upstream's
aspen mirror resolve acme/mono refs/heads/main --keep upstream   # import the upstream's value
```

Resolving only moves the ledger; the sync it runs then pushes or imports. If
either side changed again after the conflict was recorded, the ref is in
conflict again rather than losing the newer change.

A push to the upstream that the upstream refuses (for example branch
protection) fails the job; it is retried with backoff and, after 10
attempts, parked as a dead job (`aspen doctor jobs`, node C5).

## Commands

| Command | Does |
|---|---|
| `aspen import <upstream> [--name N] [--mode mirror\|copy] [--push-through proxy\|async] [--credential C]` | Create a mirror (or a copy) and import everything |
| `aspen mirror status [<repo>]` | All mirrors with mode, upstream, conflicts and last error; or one mirror's details and conflicts |
| `aspen mirror sync <repo>` | Run a sync pass now |
| `aspen mirror resolve <repo> <ref> --keep aspen\|upstream` | Resolve a conflict and sync |
| `aspen mirror remove <repo>` | Stop mirroring; the repository and its refs stay |

They read `DATABASE_URL`, `ASPEN_BLOB_URL`, `ASPEN_WORK_DIR` (spool for
packs in transit, a real disk) and the `ASPEN_MIRROR_*` variables, and apply
pending database migrations.

## How it works

- **Import** fetches, with protocol v2, the objects the upstream's value
  reaches beyond Aspen's trunk and the ref's current value, then pushes them
  into Aspen through the ordinary push path as a branch-scoped push with the
  ref's current value as the old value. Imported history is therefore stored
  exactly like pushed history: completed, self-contained, base-first packs,
  one per write, exact closures, commit index rows. The repository's force
  policy does not apply to imports (the upstream already accepted the
  change); a forced trunk update stays exact under the forced-trunk rule.
- **Push to the upstream** (async mode) builds a pack of what Aspen's value
  reaches beyond the upstream's ref and trunk with the fetch planner and
  stitcher, and sends it with the upstream's value as the old value.
- **Proxy forwarding** sends the push's completed pack. Completion makes
  it self-contained (no delta base outside it), but not closed: it holds
  what the client sent, and the client left out everything reachable from
  the tips Aspen advertised to it (trunk and the `.have` lines of earlier
  trunk tips). The upstream must hold those objects. In proxy mode it does,
  since Aspen's refs are values the upstream accepted; an upstream whose
  history was rewritten or pruned behind Aspen's back refuses the pack as
  incomplete, and the client sees that refusal.
- Settings and the ledger are in the metadata database (migration 0003,
  `crates/aspen-meta/SCHEMA.md`); the code is `crates/aspen-mirror` and
  `crates/aspen-server/src/mirror.rs`.

## Requirements and limits

- The server needs outbound HTTPS to the upstream (on AWS the default
  deployment's tasks have public IPs; with `nat_gateway = true`, through the
  NAT gateway).
- The upstream must speak Git protocol v2 for fetches, as GitHub, GitLab
  and stock Git 2.18 or later behind `git http-backend` do.
- SSH upstreams, Git LFS objects, forwarding lazy partial-clone fetches to
  the upstream, webhook-triggered syncs and leader election between several
  servers are not in v1: every server instance runs its own passes, which is
  safe (each write is a compare-and-swap) but repeats the upstream polling.
- A first import makes one fetch per ref (trunk first, then four at a time),
  so a repository with tens of thousands of tags takes a while to import.
