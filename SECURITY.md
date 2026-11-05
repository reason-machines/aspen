# Security policy

## Reporting a vulnerability

Please do not report security problems in public issues, pull requests or
discussions.

Report them privately with GitHub's private vulnerability reporting on
[reason-machines/aspen](https://github.com/reason-machines/aspen): open
[Report a vulnerability](https://github.com/reason-machines/aspen/security/advisories/new)
(the repository's **Security** tab, then **Report a vulnerability**). Include what you found, how to
reproduce it, and which version or commit you tested. We will acknowledge
your report, keep you informed while we work on a fix, and credit you in the
advisory unless you prefer otherwise.

## Supported versions

Aspen is pre-alpha and has no releases yet. Security fixes land on the
`main` branch only. Once releases exist, this section will say which ones
receive fixes.

## Scope

In scope: the `aspen` server and CLI, the Git protocol implementation, the
access controls (tokens, OIDC, SSH keys, policy, rate limits), the `place`
demo server, and the deployment templates in `deploy/`.

The public demo at <https://place.reasonmachines.com> is a demonstration
service. Its deployment is in progress, so it may not be live yet. Please do
not run load tests or denial-of-service attempts against
it; run Aspen yourself for that (see the [README](README.md#quickstart-5-minutes-once-built)).

## Threat model

[docs/SECURITY.md](docs/SECURITY.md) describes the threat model, the
controls on each public surface, the findings of the security review and
the risks accepted for now.
