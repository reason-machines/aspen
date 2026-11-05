# Limitations

Aspen is pre-alpha. This page lists what it does not do yet, so you can
decide before you depend on it. The [design plan](reference/plan.md) records
the reason behind each choice.

## Not in the first release

- **Partial clones.** `--filter` is not advertised, so `git clone
  --filter=blob:none` falls back to a full clone with a warning. A lazy
  fetch from an existing partial clone (it asks for single blobs) gets a
  clear error.
- **Exact packs for every shallow form.** `--depth=N` (N up to 64) of a
  branch tip gets an exact shallow pack. `--deepen`, `--shallow-since`,
  `--shallow-exclude`, `--unshallow` and deeper depths are served the full
  history of what they ask for, with a correct shallow boundary.
- **SHA-256 repositories.**
- **A web UI** for browsing code, and **code review or issues.** Use mirror
  mode to keep those on GitHub or GitLab.

## Known gaps

- **Tag following.** `git fetch` does not pick up new annotated tags on its
  own; `git fetch --tags` and `git clone` do. Aspen does not yet record
  what a tag points to, which `include-tag` and `ls-refs` peeling need.
- **Force pushes and deletes** need `-o force`, a lease or the
  branch-scoped URL ([why](using-git.md)). OpenAI's Git fork has a protocol
  extension (`explicit-haves`) that would make plain `--force-with-lease`
  work; Aspen does not support it.
- **SSH keys**: Ed25519 and ECDSA only. RSA and security keys (`sk-*`) are
  refused ([why](reference/ssh.md#keys)).
- **Rate limits are per instance**, kept in memory. There are no
  per-repository storage quotas.
- **OIDC key sets are configured, not fetched.** When an issuer rotates
  its signing keys, update the policy.
- **Object storage backends**: S3 and S3-compatible stores and the local
  file system. Google Cloud Storage works through its S3 API with an HMAC
  key; Azure Blob Storage is not supported, so the Azure module takes an
  S3-compatible bucket.
- **The AWS Terraform module's CDN** (CloudFront in front of
  `/-/express`) has been tested with mocked providers, not yet against a
  real distribution. Its URLs go to Git 2.56 and later only; older clients
  download from the servers' `/-/express`.
- **Mirror mode**: HTTPS upstreams only (no SSH), no Git LFS, and a first
  import fetches each ref separately, which is slow for tens of thousands
  of tags.
- **Storage overhead.** Compaction keeps the packs it merges, and
  splitting multi-ref pushes can store an object more than once (at most
  twice per push). Neither has been measured on a large repository.

## Not verified yet

The [launch gate](reference/plan.md#7-launch-gate-all-true-before-the-demo-is-public)
is the list of checks that must all pass before Aspen is called ready.
Several have not been run yet, among them the differential suite against
Aspen at a million operations, 24-hour chaos and fuzzing runs, the full
client matrix against Aspen, a clean-account run of the Compose and AWS
paths from these docs alone, and published benchmarks on identical
instances.
