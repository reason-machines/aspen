# Using Git with Aspen

Aspen speaks the standard Git protocols, so `git clone`, `fetch`, `pull`
and `push` work as usual. Two things are different, and both follow from
one design choice: **a push sees only the trunk branch.** When a client
starts a push, Aspen tells it about trunk (usually `main`) and nothing
else, so the conversation stays small however many branches the repository
has, and pushes to different branches never touch each other.

The consequences:

- Git cannot tell a branch update from a branch creation, so its push output
  shows `* [new branch]` for a branch that already exists. That is normal.
- `git push --force` to a branch looks exactly like a normal push to the
  server, so Aspen needs you to say "force" in a way it can see.
- `git push --delete <branch>` and `--force-with-lease` need to know the
  branch's current value, which the client was not shown.

Fetching is unaffected: `git clone`, `git fetch` and `git ls-remote` list
every branch and tag (with protocol version 2, the default since Git 2.26).

## URLs

| What | URL |
|---|---|
| A repository | `https://git.example.com/acme/app.git` (the `.git` is optional) |
| Its branch-scoped URL | `https://git.example.com/acme/app.git/-/refs/heads/<branch>` |
| Over SSH | `ssh://git@ssh.example.com/acme/app.git` or `git@ssh.example.com:acme/app.git` |

Repository names can have several segments (`acme/app`, `acme/tools/ci`).
The branch-scoped URL is the same repository; the only difference is that a
push through it is also shown that one branch, which is what makes
`--force-with-lease` and `--delete` work with stock Git. SSH paths take the
same `/-/refs/heads/<branch>` suffix ([Git over SSH](reference/ssh.md)).

## Signing in

Over HTTPS, use an API token as the password. The user name is ignored:

```bash
git clone https://me:aspen_<token>@git.example.com/acme/app.git
```

Prefer a [credential helper](https://git-scm.com/docs/gitcredentials) to a
token in the URL. When a request needs a credential, Aspen answers `401`
and Git asks its helper, as it does for any HTTPS host. A signed-in caller
without access to a repository gets "not found", the same answer as for a
repository that does not exist.

CI systems with OIDC identity tokens (GitHub Actions, GitLab CI) can sign in
with those instead of a stored token; the operator maps their claims to
grants ([configuration](configuration.md#access)). Over SSH, the operator
registers your public key (`aspen key add`); Ed25519 and ECDSA keys work,
RSA keys do not ([why](reference/ssh.md#keys)).

## Pushing

A plain push of new commits works as usual:

```bash
git push origin main
git push origin my-branch          # creates or fast-forwards my-branch
git push --atomic origin main v1.2 # all or nothing
```

### Force pushes

A non-fast-forward update is refused unless the push carries the push
option `force`:

```console
$ git push --force origin my-branch
 ! [remote rejected] my-branch -> my-branch (non-fast-forward (fetch and rebase, or push with -o force))

$ git push --force -o force origin my-branch
 * [new branch]      my-branch -> my-branch
```

`-o force=refs/heads/my-branch` forces only that ref of a multi-ref push.
Force pushes to trunk are refused by default: each repository has a force
policy (`aspen repo create --allow-force`), which is `branches` unless the
operator chose otherwise.

### Leases

To force only if the branch is still where you last saw it, either push
through the branch-scoped URL, where stock `--force-with-lease` works:

```bash
git remote add my-branch https://git.example.com/acme/app.git/-/refs/heads/my-branch
git fetch my-branch
git push --force-with-lease my-branch my-branch
```

or give the expected value as a push option, on the plain URL:

```bash
git push --force -o lease=refs/heads/my-branch:<expected-sha> origin my-branch
```

If the branch moved, the push is refused and nothing changes.

### Deleting a branch

`git push origin --delete my-branch` fails in the client ("remote ref does
not exist"), because the client was not shown the branch. Delete through
the branch-scoped URL, which checks the branch's current value:

```bash
git push https://git.example.com/acme/app.git/-/refs/heads/my-branch --delete my-branch
```

or delete unconditionally with the full ref name and `-o force` (allowed
where force is):

```bash
git push -o force origin --delete refs/heads/my-branch
```

Trunk cannot be deleted.

### Tags

Tags are pushed as usual. A tag never fast-forwards, so replacing an
existing tag needs `git push --force -o force origin <tag>`, or a lease;
without the option Aspen answers `tag already exists (push with -o force to
replace it)`.

### When a push is refused

Aspen puts its reason on the `! [remote rejected]` line and repeats it
after `remote:`, so you see it even with `--quiet`. Reasons that say
"retry" (a concurrent push won a race on the same branch, a storage error)
are safe to retry as they are. Typical refusals:

| Reason | What to do |
|---|---|
| `non-fast-forward (fetch and rebase, or push with -o force)` | Rebase, or force as above |
| `force is not allowed for this ref by the repository's allow_force policy` | Trunk, or a repository whose policy forbids force |
| `deleting a ref this client has not seen needs -o force, ...` | Use the branch-scoped URL or `-o force` |
| `permission denied` | Your token or key cannot write this ref |

## Fetching

Clones and fetches work as usual, over protocol version 0, 1 or 2. Two
details:

- With protocol v0 or v1 (old Git, libgit2, go-git), the initial listing
  shows only `HEAD` and trunk, plus the branch of a branch-scoped URL. Use
  protocol v2 (`git -c protocol.version=2`) or a branch-scoped URL to fetch
  other branches with those clients.
- `git fetch` does not follow new annotated tags automatically
  (`include-tag` is not supported yet). `git clone` and `git fetch --tags`
  get every tag.

### Shallow clones

`git clone --depth=N` and `git fetch --depth=N` with N up to 64 get an exact
shallow pack, built once per branch version and depth and shared by every
later fetch of it (the `actions/checkout` default is depth 1). Other shallow
forms (`--deepen`, `--shallow-since`, `--shallow-exclude`, deeper depths)
work too, but receive the full history of what they ask for along with the
correct shallow boundary, so they cost a full clone's bytes.

### Partial clones

`--filter` (blobless and treeless clones) is not supported: Aspen does not
advertise it, so Git warns and makes a full clone. A lazy fetch from an
existing partial clone, which asks for single blobs, gets an error.

### Downloading packs from object storage

If you set

```bash
git config --global fetch.uriprotocols https
```

Git lets the server send URLs instead of pack bytes (packfile URIs). Aspen
then hands out short-lived signed URLs for its stored packs, from the
bucket, a regional cache or a CDN, depending on how it is deployed. Clients
that do not set it, or older clients the URLs are not safe for, get the
same data inline. Details: [Express and CDN mode](reference/express.md).
