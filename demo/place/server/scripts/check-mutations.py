#!/usr/bin/env python3
"""Removes each check C7's tests rely on, one at a time, and shows that a
test fails without it. Run from the repository root:

    python3 demo/place/server/scripts/check-mutations.py [H1 P3 ...]

Each mutation is one or more exact text replacements; the files are
restored afterwards. A mutation whose pattern no longer matches is reported,
so the list fails loudly when the code it targets changes. Exits non-zero
unless every selected mutation makes its test fail."""
import subprocess, sys

PUSH = 'crates/aspen-server/src/receive/push.rs'
ACCESS = 'crates/aspen-server/src/access/mod.rs'
PROTO = 'crates/aspen-proto/src/receive.rs'
HOOK = 'demo/place/server/src/hook.rs'
RULES = 'demo/place/server/src/rules.rs'
STATE = 'demo/place/server/src/state.rs'
PLACE = 'demo/place/server/src/place.rs'
API = 'demo/place/server/src/api.rs'

SRV = ["-p", "aspen-server", "--test", "push_hook"]
E2E = ["-p", "aspen-place", "--test", "place"]

muts = [
 # The generic push hook in receive-pack.
 ("H1 hook refusal ignored", [(PUSH, "                    v.kind = VerdictKind::Reject(RejectReason::PushRule);\n", "")],
  SRV + ["a_refusal_shows_its_reason"]),
 ("H2 hook given no stored tip (as if trusting a zero old value)", [(PUSH, "                stored: self.stored.get(&name).copied().flatten(),", "                stored: None,")],
  SRV + ["the_hook_sees_the_stored_tip"]),
 ("H3 stored tip not re-read after a lost compare-and-swap", [(PUSH, "                            self.stored.insert(\n                                c.name.clone(),\n                                current.as_ref().and_then(|v| v.value.tip()),\n                            );\n                            again", "                            again")],
  SRV + ["after_a_lost_compare_and_swap"]),
 ("H4 committed reports no-op updates too", [(PUSH, "            .filter(|v| v.kind.writes())\n            .map(|v| CommittedUpdate {", "            .map(|v| CommittedUpdate {")],
  SRV + ["a_no_op_in_a_committed_atomic_push"]),
 ("H5 range does not stop at excluded tips", [(PUSH, "                    Ancestry::Descendant => {\n                        is_below = true;", "                    Ancestry::Descendant => {\n                        is_below = false;")],
  SRV + ["the_hook_sees_the_stored_tip"]),
 ("H6 hook detail lines dropped", [(PUSH, "                            for line in details {\n                                report.note(line);\n                            }", "                            let _ = details;")],
  SRV + ["a_refusal_shows_its_reason"]),
 ("H7 hook run after --atomic is applied", [(PUSH, "        self.run_hook(&mut verdicts).await?;\n        if atomic && !self.req.invalid.is_empty() {", "        if atomic && !self.req.invalid.is_empty() {"),
                                            (PUSH, "        Ok(if atomic {\n            apply_atomic(verdicts)\n        } else {\n            verdicts\n        })", "        let mut verdicts = if atomic { apply_atomic(verdicts) } else { verdicts };\n        self.run_hook(&mut verdicts).await?;\n        Ok(verdicts)")],
  SRV + ["a_refusal_fails_an_atomic_push_as_a_whole"]),
 ("H8 IPv6 callers keyed by full address", [(ACCESS, "                let net = std::net::Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0);", "                let net = v6;")],
  ["-p", "aspen-server", "--lib", "anonymous_ipv6_callers"]),
 ("H10 a hook error taken as an acceptance", [(PUSH, "                .await\n                .map_err(UnpackError::Store)?\n            {", "                .await\n                .unwrap_or(HookVerdict::Accept)\n            {")],
  SRV + ["a_hook_error_is_a_retryable_unpack_failure"]),
 ("H11 SSH and HTTP callers reach the hook without their rate-limit key", [(ACCESS, "                caller_key: key.to_owned(),", "                caller_key: String::new(),")],
  ["-p", "aspen-server", "--test", "ssh", "a_push_hook_sees_the_ssh_subject_as_the_caller"]),
 ("H9 report notes written under the rejection lines", [(PROTO, "                for line in &self.notes {\n                    sideband::write_message(out, mode, Band::Progress, &format!(\"aspen: {line}\"));\n                }\n", "")],
  ["-p", "aspen-proto", "--lib", "notes_go_on_band_2"]),
 # The tile rules and the place hook.
 ("P1 range excludes trunk only (counts a branch's earlier pushes)", [(HOOK, "update.trunk.into_iter().chain(update.stored).collect();", "update.trunk.into_iter().collect();")],
  E2E + ["a_branch_is_checked_over_commits_new_to_it"]),
 ("P2 main not protected", [(RULES, "    if name == TRUNK_REF {", "    if false && name == TRUNK_REF {")],
  E2E + ["every_tile_rule_is_enforced"]),
 ("P3 server namespaces open to Git clients", [(RULES, "    if pusher == Pusher::Git && RESERVED_PREFIXES", "    if false && pusher == Pusher::Git && RESERVED_PREFIXES")],
  E2E + ["every_tile_rule_is_enforced"]),
 ("P4 rules skipped for every caller", [(HOOK, "update.caller.key == INTERNAL_KEY {", "update.caller.key != INTERNAL_KEY || true {")],
  E2E + ["every_tile_rule_is_enforced"]),
 ("P5 palette fast path accepts any blob", [(HOOK, "    if let Some(i) = shared.palette_color(&e.id) {", "    if let Some(i) = shared.palette_color(&e.id).or(Some(0)) {")],
  E2E + ["every_tile_rule_is_enforced"]),
 ("P6 deleted tiles not reported", [(HOOK, "                        } else if !n.is_some_and(|n| !dir(n)) {", "                        } else if false {")],
  E2E + ["every_tile_rule_is_enforced"]),
 ("P7 Git push quota not enforced", [(HOOK, ".admit(&quota_key, now_ms());", ".admit(&quota_key, now_ms()).or(Ok::<(), u64>(()));")],
  E2E + ["git_pushes_are_limited_per_address_and_branch"]),
 # The merger bot, expiry and the feed.
 ("P8 merges replay pushes newest first", [(STATE, "        for p in &self.pending {\n            for t in &p.tiles {", "        for p in self.pending.iter().rev() {\n            for t in &p.tiles {")],
  E2E + ["the_merger_replays_pushes_in_seq_order"]),
 ("P9 merge commit not on main's tip", [(PLACE, "            &main.extras,\n            &[main.commit],\n            &message,", "            &main.extras,\n            &[],\n            &message,")],
  E2E + ["a_git_push_paints_a_branch_and_the_merger_fast_forwards_main"]),
 ("P10 unmerged branches expire", [(STATE, ".filter(|(_, b)| b.overlay.is_empty() && (b.deleted", ".filter(|(_, b)| (b.deleted")],
  E2E + ["branches_expire_only_after_their_tiles_merge"]),
 ("P11 a seq from the future is not resynced", [(STATE, "        if after > self.seq || after + 1 < oldest {", "        if after + 1 < oldest {")],
  E2E + ["the_event_stream_lines_up"]),
 # The API, the swarm and the kill switch.
 ("P12 web pushes not limited", [(API, "        .pushes\n        .admit(&session.branch, now);", "        .pushes\n        .admit(&session.branch, now)\n        .or(Ok::<(), u64>(()));")],
  E2E + ["web_strokes_are_real_commits"]),
 ("P13 a session may push to any branch", [(API, "    if branch != session.branch {", "    if false && branch != session.branch {")],
  E2E + ["web_strokes_are_real_commits"]),
 ("P14 strokes not checked before writing", [(API, "    let tiles = match rules::validate_stroke(&tiles, &place.shared.rules) {\n        Ok(t) => t,", "    let tiles = match rules::validate_stroke(&tiles[..tiles.len().min(1)], &place.shared.rules) {\n        Ok(t) => t,")],
  E2E + ["web_strokes_are_real_commits"]),
 ("P15 any signed-in caller is an operator", [(PLACE, "                    .authorize(&self.repo, aspen_auth::Need::Admin)", "                    .authorize(&self.repo, aspen_auth::Need::Read)")],
  E2E + ["the_kill_switch_takes_an_admin_grant"]),
 ("P16 kill switch does not pause ambient agents", [(PLACE, "        if killed {\n            return;\n        }\n", "")],
  E2E + ["the_kill_switch_takes_an_admin_grant"]),
 ("P17 kill switch does not stop the swarm", [(PLACE, "            if engaged && s.swarm.state == crate::state::SwarmState::Running {", "            if false && engaged && s.swarm.state == crate::state::SwarmState::Running {"),
                                             (STATE, "                if self.kill.engaged || self.swarm.ends_ms.is_some_and(|e| now >= e) {", "                if self.swarm.ends_ms.is_some_and(|e| now >= e) {"),
                                             (STATE, "            (SwarmState::Running, Some(start)) if !self.kill.engaged => {", "            (SwarmState::Running, Some(start)) => {")],
  E2E + ["the_kill_switch_takes_an_admin_grant"]),
 ("P18 swarm rate not capped", [(STATE, "self.swarm.credit = (self.swarm.credit + share * cap * elapsed).min(cap.max(1.0));", "self.swarm.credit = self.swarm.credit + share * cap * elapsed * 10.0;")],
  E2E + ["the_swarm_is_one_global_run_capped_by_the_server"]),
 ("P19 swarm starts per address not limited", [(API, "            .swarm_starts\n            .admit(&addr, now_ms());", "            .swarm_starts\n            .admit(&addr, now_ms())\n            .or(Ok::<(), u64>(()));")],
  E2E + ["the_swarm_is_one_global_run_capped_by_the_server"]),
 # Hook bookkeeping stays bounded.
 ("P20 Git pushes leave a sequence number behind", [(HOOK, "let waited_on = caller.key == INTERNAL_KEY;", "let waited_on = !caller.key.is_empty();")],
  E2E + ["git_pushes_and_strokes_leave_nothing_waiting_in_the_hook"]),
 ("P21 accepted updates not capped", [(HOOK, "                    self.git.remove(&oldest);", "                    let _ = oldest;")],
  ["-p", "aspen-place", "--lib", "updates_that_never_commit_are_bounded"]),
 ("P23 the cap evicts the server's own waiting writes too", [(HOOK, "                if let Some(oldest) = oldest {\n                    self.git.remove(&oldest);\n                }", "                if let Some(oldest) = oldest {\n                    self.git.remove(&oldest);\n                    self.server.clear();\n                }")],
  E2E + ["git_checks_that_never_commit_cannot_evict_a_pending_stroke"]),
 ("P22 expiry ignores a branch already gone", [(PLACE, "report.reason() != Some(RejectReason::NoSuchRef)", "report.reason() != Some(RejectReason::StaleOld)")],
  E2E + ["a_branch_deleted_behind_the_services_back_still_expires"]),
]

only = sys.argv[1:]
res = []
for name, edits, args in muts:
    if only and not any(name.startswith(o + " ") for o in only):
        continue
    originals = {}
    missing = None
    for f, old, _ in edits:
        src = originals.setdefault(f, open(f).read())
        if src.count(old) != 1:
            missing = "PATTERN NOT FOUND x%d in %s" % (src.count(old), f)
    if missing:
        res.append((name, missing)); print(res[-1], flush=True); continue
    current = dict(originals)
    for f, old, new in edits:
        current[f] = current[f].replace(old, new)
    for f, text in current.items():
        open(f, 'w').write(text)
    try:
        r = subprocess.run(["cargo", "test", "--locked"] + args[:-1] + ["--", args[-1]],
                           capture_output=True, text=True, timeout=1500)
        out = r.stdout + r.stderr
        if "error[" in out or "could not compile" in out: verdict = "COMPILE ERROR"
        elif r.returncode != 0 and "FAILED" in out: verdict = "test FAILS (good)"
        elif "running 0 tests" in out and "1 passed" not in out: verdict = "NO TEST RAN"
        else: verdict = "test PASSES (bad)"
    finally:
        for f, text in originals.items():
            open(f, 'w').write(text)
    res.append((name, verdict)); print(res[-1], flush=True)

sys.exit(0 if res and all(v == 'test FAILS (good)' for _, v in res) else 1)
