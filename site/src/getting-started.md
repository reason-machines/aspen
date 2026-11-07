# Run it yourself in 5 minutes

This page takes you from nothing to pushing and cloning through Aspen on
your own machine, then to a server that requires tokens, then to the
single-host stack with PostgreSQL and MinIO.

Once you have the `aspen` binary, steps 1 and 2 take a minute or two.
Getting the binary is the slow part until releases are published: building
from source compiles for several minutes (about five on a 2-core machine).

## 0. Get the binary

There are no published releases yet (see [status](introduction.md)). Build
from source. You need:

- `git`,
- a Rust toolchain from [rustup](https://rustup.rs) (the repository pins the
  version in `rust-toolchain.toml`, and rustup installs it on first use),
- a C compiler (`build-essential` on Debian and Ubuntu, the Xcode command
  line tools on macOS).

```bash
git clone https://github.com/reason-machines/aspen
cd aspen
cargo install --locked --path crates/aspen-cli
aspen --version
```

`cargo install` puts `aspen` in `~/.cargo/bin`. Once releases are published,
the [Releases](reference/deploy.md#releases) section of the deployment
reference lists the archives, the container image and the Homebrew formula.

## 1. A development server

```bash
mkdir aspen-try && cd aspen-try
aspen dev
```

`aspen dev` runs the whole server in one process: SQLite for metadata and a
local directory for packs, both under `./.aspen`. It creates a repository
named `demo`, listens on `127.0.0.1:7070` and lets anyone who can reach it
push, which is why it refuses to listen on anything but a loopback address.
`--repo <name>` (repeatable) creates other repositories instead of `demo`;
add `--repo demo` to keep it too.

In a second terminal, push a repository to it:

```bash
git init -b main hello && cd hello
echo "hello, aspen" > README.md
git add README.md && git commit -m "first commit"
git push http://127.0.0.1:7070/demo.git main
```

Clone it back and check the clone:

```bash
cd ..
git clone http://127.0.0.1:7070/demo.git copy
git -C copy log --oneline
git -C copy fsck --strict
```

Push a branch, then list the refs and make a shallow clone:

```bash
cd hello
git switch -c feature
echo "more" >> README.md && git commit -am "on a branch"
git push http://127.0.0.1:7070/demo.git feature
git ls-remote http://127.0.0.1:7070/demo.git
cd ..
git clone --depth=1 http://127.0.0.1:7070/demo.git shallow
```

The server's metrics are at <http://127.0.0.1:7070/-/metrics>. On a real
server they are on a separate, private listener instead; `aspen dev` serves
them beside Git for convenience.

Stop the server with Ctrl-C. It finishes the requests in flight first. Your
data stays in `./.aspen`, and the next `aspen dev` in the same directory
picks it up.

Before you go further, read [Using Git with Aspen](using-git.md): a force
push or a branch delete needs one extra flag, because a push only sees the
trunk branch.

## 2. Require tokens

`aspen serve` is the production server. Every deployment runs it, configured
from environment variables ([Configuration](configuration.md)). By default
every request needs an API token. You can try it on one machine with SQLite
and a local directory:

```bash
export DATABASE_URL="sqlite://$PWD/aspen.db?mode=rwc"
export ASPEN_BLOB_URL="file://$PWD/packs"
export ASPEN_SPOOL_DIR="$PWD/spool"
export ASPEN_LISTEN=127.0.0.1:7070

aspen repo create acme/app
aspen token create --subject user:me --grant write:acme/app
```

The last command prints a token that starts with `aspen_`. It is shown once
and only its hash is stored, so copy it now. Start the server:

```bash
aspen serve
```

In a second terminal (with the same `ASPEN_LISTEN` if you changed it), push
with the token as the password. The user name is ignored:

```bash
cd hello
git push http://127.0.0.1:7070/acme/app.git main            # asks for credentials: refused
git push http://me:aspen_...@127.0.0.1:7070/acme/app.git main
```

For anything longer-lived than a test, give the token to a
[credential helper](https://git-scm.com/docs/gitcredentials) instead of
putting it in the URL.

To let anyone read without a token, give anonymous callers a grant in the
access policy, then restart the server:

```bash
export ASPEN_POLICY='{"anonymous": ["read:acme/*"]}'
aspen serve
```

Grants, OIDC identity tokens (GitHub Actions, GitLab CI) and rate limits are
on the [configuration](configuration.md#access) page.

## 3. PostgreSQL and object storage on one host

The single-host stack runs Aspen, PostgreSQL and MinIO with Docker Compose.
You need Docker with the compose plugin (`docker compose version`).

```bash
cd deploy/compose          # in your clone of the repository
./init-env.sh              # writes .env with random passwords; never overwrites
docker compose up -d       # builds the Aspen image on the first run
docker compose ps          # postgres, minio and aspen should be "healthy"
curl http://127.0.0.1:7070/healthz

docker compose exec aspen aspen repo create acme/app
docker compose exec aspen aspen token create --subject me --grant write:acme/app
git push http://me:<token>@127.0.0.1:7070/acme/app.git main
```

The first `up` compiles Aspen inside the image build, which takes several
minutes. `docker compose down` stops the stack; add `--volumes` to delete
its data. The [single-host section](reference/deploy.md#single-host-docker-compose)
of the deployment reference describes each service and setting.

## Next

- [Self-hosting](self-hosting.md): pick a deployment path for a team.
- [Operations](operations.md): health checks, metrics, alerts and upgrades.
- [Mirror mode](reference/mirror.md): put Aspen in front of an existing
  GitHub or GitLab repository.
