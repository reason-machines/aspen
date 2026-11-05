# Contributing to Aspen

Thanks for your interest. Aspen is pre-alpha (see the
[README](README.md#pre-alpha-status)), so expect the code and the plan to
move quickly. Bug reports, interop reports from unusual Git clients, and
small focused pull requests are the most useful contributions right now.

## Before you start

- [docs/PLAN.md](docs/PLAN.md) is the source of truth for scope and
  architecture. Its decisions log records why each choice was made; read the
  relevant rows before changing a rule they describe. A change that alters a
  decision updates the plan and adds a row to the log in the same pull
  request.
- For anything larger than a bug fix, open an issue first so we can agree on
  the shape before you write the code.
- Report security problems privately, as described in
  [SECURITY.md](SECURITY.md), never in a public issue.

## Building and testing

You need a Rust toolchain (the version is pinned in `rust-toolchain.toml`;
rustup installs it on first use), a C compiler and `git` on your `PATH`.

```bash
cargo build --workspace
cargo test --workspace          # unit tests, the interop smoke and most integration tests
cargo xtask check-deps          # crate layering rules (docs/PLAN.md section 2)
cargo xtask check-workflows     # CI actions pinned to commits, downloads pinned to checksums
cargo deny check                # licenses, advisories, sources (needs cargo-deny)
```

Some tests need more than Rust:

- The S3 conformance tests start MinIO in Docker
  (`crates/aspen-blob/scripts/test-minio.sh`); without it they are skipped
  locally and fail in CI.
- PostgreSQL tests start a throwaway cluster from the PostgreSQL binaries on
  your machine.
- The `place` demo (`demo/place`) uses Node.js; see its README.
- The documentation site builds with `site/install-mdbook.sh` and
  `python3 site/build.py`. When you add an environment variable, document it
  in `site/src/configuration.md`, or the build fails.

## Required checks

Every pull request must pass `ci-ok`, the aggregate of the jobs in
[.github/workflows/ci.yml](.github/workflows/ci.yml): `cargo fmt`,
`cargo clippy -D warnings` with rustdoc warnings denied and the layering
check, `cargo test`, the interop smoke against the latest Git release,
`cargo deny`, the deployment configuration checks (Terraform, tflint, Helm,
Compose), the `place` demo's checks and the docs site build. Slow suites (the
client matrix, the long differential run, chaos, fuzzing, benchmarks) run
nightly.

## Pull requests

- Keep each pull request to one change, with tests that would fail without
  it. For a bug fix, say how you reproduced the bug and show the test
  failing before the fix.
- Prefer tests that check an invariant or a user-visible behavior over tests
  of internals. Several crates keep a `scripts/check-mutations.py` that
  breaks the code on purpose to prove the tests notice; extend it when you
  add a rule worth guarding.
- Commit subjects follow the style of the history: a type and an optional
  scope, then a short imperative summary, for example
  `fix(ssh): charge failed re-checks to the failed-authentication bucket`.
- Write docs and comments for a reader who has not seen this conversation:
  say what the code does and why, not how it got there.

## License

Aspen is licensed under the [Apache License 2.0](LICENSE). Unless you say
otherwise, any contribution you submit is licensed under the same terms, as
described in section 5 of the license.
