#!/usr/bin/env python3
"""Removes each guard mirror mode's tests rely on, one at a time, and shows
that a test fails without it. Run from the repository root:

    python3 crates/aspen-mirror/scripts/check-mutations.py [M1 M2 ...]

Each mutation is an exact text replacement in one source file; the file is
restored afterwards. A mutation whose pattern no longer matches is reported,
so the list fails loudly when the code it targets changes. The schema's own
constraints are covered by crates/aspen-meta/scripts/drop-each-constraint.py
(run it with `sqlite 0003_mirrors.sql`)."""
import subprocess, sys

R = 'crates/aspen-mirror/src/reconcile.rs'
S = 'crates/aspen-mirror/src/service.rs'
C = 'crates/aspen-mirror/src/credential.rs'
U = 'crates/aspen-mirror/src/upstream.rs'
W = 'crates/aspen-mirror/src/wire.rs'
P = 'crates/aspen-server/src/receive/push.rs'
RM = 'crates/aspen-server/src/receive/mod.rs'
MM = 'crates/aspen-meta/src/mirrors.rs'
RF = 'crates/aspen-core/src/refs.rs'
E2E = ["-p", "aspen-server", "--test", "mirror"]

muts = [
 ("M1 async import overwrites a ref Aspen moved", R,
  "        MirrorMode::Async if aspen == synced => Action::Import,",
  "        MirrorMode::Async if true => Action::Import,",
  E2E + ["async_conflicts_are_reported"]),
 ("M2 a conflict is pushed over the upstream's change", R,
  "        MirrorMode::Async => Action::Conflict,",
  "        MirrorMode::Async => Action::Push,",
  ["-p", "aspen-mirror", "--lib", "async_mode_never_overwrites"]),
 ("M3 resolving keeps the wrong side", R,
  "        Side::Aspen => conflict.upstream,",
  "        Side::Aspen => conflict.aspen,",
  E2E + ["async_conflicts_are_reported"]),
 ("M4 proxy push not forwarded (Aspen accepts alone)", P,
  "        if let Some((hook, MirrorMode::Proxy)) = self.mirror.clone() {",
  "        if let Some((hook, MirrorMode::Async)) = self.mirror.clone() {",
  E2E + ["proxy_pushes_land_at_the_upstream"]),
 ("M5 an upstream refusal is committed anyway", P,
  "                    reject(verdicts, RejectReason::UpstreamRejected);",
  "                    let _ = reject;",
  E2E + ["a_push_the_upstream_refuses"]),
 ("M6 updates forwarded although the upstream is ahead", S,
  ".map(|u| (advert.refs.get(&u.name).copied() != u.old).then_some(Forwarded::Ahead))",
  ".map(|u| (false && advert.refs.get(&u.name).copied() != u.old).then_some(Forwarded::Ahead))",
  E2E + ["a_proxy_push_while_the_upstream_is_ahead"]),
 ("M7 imports go through the proxy hook (sent back upstream)", P,
  "        if self.origin == Origin::Client\n",
  "        if true\n",
  E2E + ["a_sync_follows_every_kind_of_change"]),
 ("M8 imports bound by the repository's force policy", RM,
  "            settings: r.settings.clone().with_allow_force(ForcePolicy::All),",
  "            settings: r.settings.clone(),",
  E2E + ["a_sync_follows_every_kind_of_change"]),
 ("M9 async writes enqueue no mirror_push job", P,
  "        matches!(self.mirror, Some((_, MirrorMode::Async))).then(|| VersionJob {",
  "        matches!(self.mirror, Some((_, MirrorMode::Proxy))).then(|| VersionJob {",
  E2E + ["async_pushes_reach_the_upstream"]),
 ("M10 async deletes enqueue no job", P,
  "                RefWrite::from_verdict(v, None, Deps::default(), Closure::Exact).map(|mut w| {\n                    w.jobs.extend(self.mirror_job());",
  "                RefWrite::from_verdict(v, None, Deps::default(), Closure::Exact).map(|mut w| {\n                    w.jobs.extend(self.mirror_job().filter(|_| false));",
  E2E + ["async_pushes_reach_the_upstream"]),
 ("M11 ledger writes ignore the expected value", MM,
  "                if current != expected {",
  "                if false && current != expected {",
  ["-p", "aspen-meta", "--test", "store", "sqlite_store"]),
 ("M12 credentials sent over plain HTTP", C,
  "        if !url.is_https() && !url.is_loopback() {",
  "        if false && !url.is_https() && !url.is_loopback() {",
  ["-p", "aspen-mirror", "--lib", "never_plain_http"]),
 ("M13 redirects followed to another origin", U,
  "        if attempt.previous().len() >= 5 || !same_origin {",
  "        if attempt.previous().len() >= 5 {",
  ["-p", "aspen-mirror", "--lib", "a_credential_never_follows_a_redirect"]),
 ("M14 GitHub App token minted on every use", C,
  "                    .filter(|(_, exp)| unix_now() + RENEW_BEFORE.as_secs() < *exp)",
  "                    .filter(|(_, exp)| unix_now() + RENEW_BEFORE.as_secs() > *exp)",
  E2E + ["github_app_installation_tokens"]),
 ("M15 a truncated fetch answer accepted", W,
  "        if self.state == FetchState::Done && self.buf.is_empty() {",
  "        if true {",
  ["-p", "aspen-mirror", "--lib", "the_fetch_reader_writes_band_one"]),
 ("M16 a push that brings no objects is forwarded without a pack", U,
  "        let body = if wire::push_sends_pack(updates) && pack.is_none() {",
  "        let body = if wire::push_sends_pack(updates) && pack.is_none() && false {",
  E2E + ["proxy_pushes_land_at_the_upstream"]),
 ("M17 an update the upstream accepted reported refused when Aspen lost the race", P,
  "                VerdictKind::Reject(_) if self.forwarded.contains(&v.name) => {",
  "                VerdictKind::Reject(_) if false && self.forwarded.contains(&v.name) => {",
  E2E + ["an_update_the_upstream_accepted_is_ok_even_when_aspen_loses_the_race"]),
 ("M18 a lagging proxy mirror requests no sync", P,
  "                    lagging = true;\n                    (RefStatus::Ok, RefOutcome::Ok)",
  "                    lagging = false;\n                    (RefStatus::Ok, RefOutcome::Ok)",
  E2E + ["an_update_the_upstream_accepted_is_ok_even_when_aspen_loses_the_race"]),
 ("M19 updates Aspen refused are forwarded anyway", P,
  "            .filter(|&i| verdicts[i].kind.writes())",
  "            .filter(|&i| i < usize::MAX)",
  E2E + ["a_client_push_aspen_refuses_is_never_forwarded"]),
 ("M20 a client force of trunk accepted (force policy ignored)", RF,
  "            (true, false) => Err(RejectReason::ForceNotAllowed),",
  "            (true, false) => Ok(()),",
  E2E + ["a_client_push_aspen_refuses_is_never_forwarded"]),
 ("M21 a failed ledger write after upstream acceptance fails the push", S,
  "                    self.record_agreed(repo, &u.name, u.new).await;",
  "                    let e = self.store.ledger_entry(repo.id, &u.name).await?;\n                    self.agree(repo.id, &u.name, e.synced, u.new).await?;",
  E2E + ["an_upstream_acceptance_stands_when_recording_it_in_the_ledger_fails"]),
 ("M22 a repository no longer a mirror reported as accepted", S,
  "            return Ok(vec![Forwarded::NotMirror; updates.len()]);",
  "            return Ok(vec![Forwarded::Accepted; updates.len()]);",
  E2E + ["a_repository_that_stopped_being_a_mirror_forwards_nothing"]),
 ("M23 a NotMirror answer refuses the update", P,
  "                Forwarded::NotMirror => {}",
  "                Forwarded::NotMirror => reject(verdicts, RejectReason::UpstreamUnavailable),",
  E2E + ["an_update_the_upstream_accepted_is_ok_even_when_aspen_loses_the_race"]),
 ("M24 a bad credential header silently dropped (anonymous request)", U,
  "                    .map_err(|_| UpstreamError::BadAuthorization)?;",
  "                    .unwrap_or(HeaderValue::from_static(\"\"));",
  ["-p", "aspen-mirror", "--lib", "a_credential_that_is_no_header_value_is_refused"]),
 ("M25 upstream text passed to clients unsanitized", S,
  "        let c = if c.is_control() { ' ' } else { c };",
  "        let c = c;",
  ["-p", "aspen-mirror", "--lib", "upstream_text_reaches_clients"]),
 ("M26 a mirror create leaves an orphan repository", MM,
  "            insert_mirror_row(tx, &row(repo.id)).await?;\n            Ok(repo)",
  "            let _ = insert_mirror_row(tx, &row(repo.id)).await;\n            Ok(repo)",
  ["-p", "aspen-meta", "--test", "store", "sqlite_store"]),
]

only = sys.argv[1:]
res = []
for name, f, old, new, args in muts:
    if only and not any(name.startswith(o + " ") for o in only):
        continue
    src = open(f).read()
    if src.count(old) != 1:
        res.append((name, "PATTERN NOT FOUND x%d" % src.count(old))); print(res[-1], flush=True); continue
    open(f, 'w').write(src.replace(old, new))
    try:
        r = subprocess.run(["cargo", "test", "--locked"] + args[:-1] + ["--", args[-1]],
                           capture_output=True, text=True, timeout=1500)
        out = r.stdout + r.stderr
        if "error[" in out or "could not compile" in out: verdict = "COMPILE ERROR"
        elif r.returncode != 0 and "FAILED" in out: verdict = "test FAILS (good)"
        elif "running 0 tests" in out and "1 passed" not in out: verdict = "NO TEST RAN"
        else: verdict = "test PASSES (bad)"
    finally:
        open(f, 'w').write(src)
    res.append((name, verdict)); print(res[-1], flush=True)

sys.exit(0 if all(v == 'test FAILS (good)' for _, v in res) else 1)
