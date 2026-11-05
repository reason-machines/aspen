# Git over SSH

Aspen serves Git over SSH as well as smart HTTP (plan node C3). The same
repositories, branch-scoped URLs, access rules, rate limits and push quotas
apply to both; only the way a client proves who it is differs.

## Clone URLs

```text
git clone ssh://git@ssh.example.com/acme/mono.git
git clone git@ssh.example.com:acme/mono.git
git push  ssh://git@ssh.example.com/acme/mono.git/-/refs/heads/agent-7 ...
```

The user name is ignored; `git` is the convention. A path is the repository
name, optionally with `.git`, optionally followed by `/-/<ref>`: the
branch-scoped URL that makes stock `git push --delete <branch>` and
`--force-with-lease` work (see `docs/PLAN.md`, push path step 1). Paths
relative to a home directory (`~user/...`) do not exist.

Protocol v2 (`git -c protocol.version=2`, the default since git 2.26), v1
and v0 all work. In v0 and v1 the ref listing shows `HEAD`, trunk and the
branch of a scoped URL, as over HTTP; use v2 (or a scoped URL) to see other
refs. In v2, `ls-remote` through a scoped URL lists exactly what the plain
URL lists: the scope narrows only pushes and the v0/v1 listing.

## Keys

A client authenticates with a public key that an operator registered. A
registered key carries:

| Field | Meaning |
|---|---|
| key | An `ssh-ed25519` or `ecdsa-sha2-nistp256/384/521` public key |
| subject | Who the key acts as (`user:alice`, `agent:ci-7`), for logs and rate limits; the same rule as API token subjects, and never `oidc:...` |
| grants | What the key may do, in the grant syntax of API tokens: `read:acme/*`, `write:acme/mono`, `write:acme/mono:refs/heads/agent-7/*` |
| expiry | Optional |

A key holds exactly its own grants plus what anonymous callers may do; it
never inherits its subject's grants from tokens or OIDC rules. Keys are
stored in the metadata database (`ssh_keys`, migration 0004) by their
SHA-256 fingerprint, the value `ssh-keygen -l -f key.pub` prints. A revoked
key stays revoked: registering the same key again is refused.

**RSA keys are not accepted.** Verifying them needs the `rsa` crate, which
has an open advisory (RUSTSEC-2023-0071) with no fixed release, so Aspen
does not link it. Security-key types (`sk-ssh-ed25519@openssh.com`) are not
accepted yet either. Generate an Ed25519 key with
`ssh-keygen -t ed25519`.

Keys are registered with the `aspen` binary, against the server's
`DATABASE_URL`:

```bash
aspen key add ~/.ssh/id_ed25519.pub --subject user:alice --grant write:acme/*
aspen key list
aspen key revoke SHA256:...
```

The library calls behind them are
`aspen_server::ssh::keys::SqlSshKeys::register` (and `revoke`), with
`aspen_auth::SshPublicKey::parse_openssh` reading an `authorized_keys`-style
line.

## What happens on a connection

0. Admission. A connection is closed as soon as it is accepted when
   `SshConfig::max_connections` (4,096) are already open, or when its
   address (an IPv4 address or an IPv6 /64) already has
   `max_unauthenticated_per_address` (10) connections that have not
   authenticated. A connection that has not authenticated within
   `auth_timeout` (30 s) of being accepted, key exchange included, is
   disconnected. The address is the TCP peer: a load balancer in front
   must preserve client addresses, or every client shares its limits.
   The AWS module's NLB and the Helm chart's SSH Service do
   ([deploy/README.md](../deploy/README.md)); Aspen does not parse proxy
   protocol.
1. Key exchange with the server's host keys.
2. Public-key authentication. Unknown, revoked, expired and RSA keys are
   refused at the SSH layer, so the client sees
   `Permission denied (publickey)`. Passwords, keyboard-interactive and
   OpenSSH certificates are refused. There is no anonymous SSH. Each key
   a client offers is looked up once per connection, at most
   `max_auth_attempts` (20) distinct keys may be offered, and every
   refused key is charged to the address's failed-authentication bucket
   (`LimitsConfig::auth_failures`, the one HTTP uses; default 100, then
   one a second); while it is empty, offers are refused without a lookup.
   An offer takes its token before the lookup and gives it back if the
   key is registered, so connections offering keys at once cannot
   overshoot the bucket.
3. One command per session: `git-upload-pack <path>` or
   `git-receive-pack <path>`. Anything else (a shell command,
   `git-upload-archive`) is refused with a message on stderr. No shell,
   pty, subsystem, X11, agent or port forwarding. `ssh -T git@host` prints
   which subject the key acts as.
4. The key is identified again for every command, so revoking a key or
   changing its grants takes effect on the next command, even on a
   connection that stays open (an OpenSSH control master). A protocol v2
   session runs several commands on one stream; before each one after the
   first, the key is identified and the repository authorized again, as
   each HTTP POST is, so a revocation also ends a session that is already
   running: its next command gets `ERR` and the session exits with
   status 1. Each re-check is charged like an offer in step 2: a failed
   one (a revoked, expired or removed key) drains the address's
   failed-authentication bucket, and while that is empty commands are
   refused (`too many failed authentications`) without a lookup.
5. The access decision is the one HTTP requests get, by repository name:
   a key without a grant on a repository is told `repository not found`,
   exactly as for a repository that does not exist; a read-only key that
   pushes is told `permission denied`; a push to a ref outside a key's
   write patterns gets `ng <ref> permission denied`. Git prints these as
   `fatal: remote error: ...` or as a rejected ref.
6. Rate limits and push quotas are keyed by subject, so a subject's HTTP
   requests and SSH commands draw on the same buckets. In protocol v2 every
   command after the first in a session (each negotiation round of a fetch)
   costs one request, as each HTTP POST does. A command refused before it
   reaches a repository (not Git, a bad path) and a shell greeting cost one
   request too.

## Running the server

`aspen_server::ssh::SshServer` wraps the HTTP server's `Server` and its
`Access`:

```rust,ignore
let auth = AuthService::new(policy, tokens)?.with_ssh_keys(SqlSshKeys(meta.clone()));
let access = Arc::new(Access::new(auth, limits, meta.clone(), ClientAddr::Peer));
let router = server.router(Arc::clone(&access));       // HTTP
let host_key = load_host_key(Path::new("/etc/aspen/ssh_host_ed25519_key"))?;
let ssh = SshServer::new(server, access, SshConfig::new(vec![host_key]))?;
tokio::spawn(ssh.serve(TcpListener::bind("0.0.0.0:2222").await?));
```

Keep the host key stable across restarts and identical on every instance
behind a load balancer: clients pin it in `known_hosts`. It is a secret;
load it from your secret store, never from the image. `aspen serve` refuses
an `ASPEN_SSH_HOST_KEY_FILE` that other users may access and warns about one
a group other than its own may (`chmod 600`; issue #125). `SshConfig` also sets
the inactivity timeout (10 minutes), the minimum time a refused
authentication takes (1 s), authentication attempts per connection (20),
sessions per connection (8), and the admission limits of step 0.

Wiring this into `aspen serve` (listen address `ASPEN_SSH_LISTEN`, the host
key from the secret store) belongs to the release node (C6), together with
the HTTP server.
