#!/usr/bin/env python3
"""Removes each check B1's tests rely on, one at a time, and shows that a
test fails without it. Run from the repository root:

    python3 crates/aspen-server/scripts/check-mutations.py [M1 M2 ...]

Each mutation is an exact text replacement in one source file; the file is
restored afterwards. A mutation whose pattern no longer matches is reported,
so the list fails loudly when the code it targets changes."""
import subprocess, sys, json
P='crates/aspen-server/src/receive/push.rs'; G='crates/aspen-pack/src/graph.rs'; S='crates/aspen-pack/src/subset.rs'
A='crates/aspen-server/src/receive/ancestry.rs'; GA='crates/aspen-server/src/receive/gate.rs'; SRC='crates/aspen-server/src/receive/source.rs'
M='crates/aspen-server/src/receive/mod.rs'
muts = [
 ("M1 reach ignores held objects", G, "                if held[next] {", "                if false && held[next] {", ["-p","aspen-server","--test","packs","an_all_ref_delta_resend"]),
 ("M2 subset copies a delta whose base is left out", S, "            if base.is_some() && kept_base.is_none() {", "            if false && base.is_some() && kept_base.is_none() {", ["-p","aspen-pack","a_delta_whose_base_is_left_out"]),
 ("M3 no per-write split (every write stores the whole pack)", P, "let whole = spill.is_none() && !keep.is_empty() && keep.iter().all(|k| *k);", "let whole = spill.is_none() && !keep.is_empty();", ["-p","aspen-server","--test","receive","a_multi_ref_push_stores_one_pack_per_write"]),
 ("M4 forced-trunk rule off", P, "                    Ancestry::NotDescendant => true,", "                    Ancestry::NotDescendant => false,", ["-p","aspen-server","--test","receive","a_forced_trunk_update_stays_exact"]),
 ("M5 skipped objects not copied into a forced trunk pack", P, "            if to_read.is_empty() {\n                continue;\n            }", "            if true {\n                continue;\n            }", ["-p","aspen-server","--test","packs","a_forced_trunk_update_copies"]),
 ("M6 orphan forced trunk keeps the old trunk dependency", P, "                        trunk: a.as_ref().map(|v| v.id),", "                        trunk: a.as_ref().map(|v| v.id).or(self.trunk_dep.as_ref().map(|v| v.id)),", ["-p","aspen-server","--test","receive","an_orphan_forced_trunk"]),
 ("M7 prefetch gate does not wait", GA, "        let permit = tokio::time::timeout(self.wait, self.sem.clone().acquire_owned())\n            .await\n            .map_err(|_| busy())?\n            .map_err(|_| busy())?;", "        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.map_err(|_| busy())?;", ["-p","aspen-server","--test","limits","the_gate_shares_one_budget"]),
 ("M8 over-budget prefetch reported as retryable storage error", P, "            Err(PrefetchError::OverBudget { budget }) => {\n                return Err(UnpackError::BasesOverBudget { budget });\n            }\n            Err(PrefetchError::Store(e)) => return Err(UnpackError::Store(e)),\n        };\n        self.timings.prefetch", "            Err(PrefetchError::OverBudget { budget }) => {\n                return Err(UnpackError::Store(StoreError::Unavailable { reason: budget.to_string() }));\n            }\n            Err(PrefetchError::Store(e)) => return Err(UnpackError::Store(e)),\n        };\n        self.timings.prefetch", ["-p","aspen-server","--test","limits","a_push_whose_bases_exceed"]),
 ("M9 ancestry never reads unindexed commits from packs", SRC, "            if unknown.is_empty() {\n                break;\n            }", "            if true {\n                break;\n            }", ["-p","aspen-server","--test","limits","fast_forward_checks_read_unindexed"]),
 ("M10 ancestry stops when b's walk meets a's (wrong stop rule)", A, "        while b_only > 0 {", "        while b_only > 0 && expanded < 2 {", ["-p","aspen-server","--lib","answers_match_reachability"]),
 ("M11 branch at a non-commit accepted", P, "        if name.is_branch() && kind != ObjectKind::Commit {", "        if false && name.is_branch() && kind != ObjectKind::Commit {", ["-p","aspen-server","--test","receive","a_branch_at_a_non_commit"]),
 ("M12 tags not peeled for closure ancestry", P, "                Some(c) => self.ancestry(dt, c, &[d.id]).await?,", "                Some(_) => self.ancestry(dt, tip, &[d.id]).await?,", ["-p","aspen-server","--test","receive","a_multi_ref_push_stores_one_pack_per_write"]),
 ("M13 tip row skipped for a branch at an existing commit", P, "            if matches!(row.first(), Some(CommitLookup::NotIndexed)) {", "            if false && matches!(row.first(), Some(CommitLookup::NotIndexed)) {", ["-p","aspen-server","--test","receive","a_branch_at_an_existing_commit"]),
 ("M14 connectivity check removed", P, "        aspen_pack::scan::check_connectivity(&svc.packs, &self.search, &graph.missing()).await?;", "        let _ = aspen_pack::scan::check_connectivity(&svc.packs, &self.search, &graph.missing()).await;", ["-p","aspen-server","--test","packs","a_missing_referenced_object"]),
 ("M15 lone-flush probe treated as a request", P, "    if req.command_count() == 0 {\n        return Parsed::Empty;\n    }", "    if req.command_count() == 0 {\n        return Parsed::Fatal(\"no commands\".into());\n    }", ["-p","aspen-server","--test","packs","a_lone_flush_probe"]),
 ("M16 no .have lines advertised", M, "            let want = have_tip_seqs(v.seq, r.settings.have_tips());", "            let want = have_tip_seqs(v.seq, 0);", ["-p","aspen-server","--test","receive","the_advertisement_shows"]),
 ("M17 atomic ignored", P, "        Ok(if atomic {\n            apply_atomic(verdicts)", "        Ok(if false {\n            apply_atomic(verdicts)", ["-p","aspen-server","--test","receive","an_atomic_push_is_all_or_nothing"]),
 ("M18 dependency ancestry ignored (every write Exact)", P, "        let closure = Closure::of_new_version(tip, true, answers.iter().map(|(v, a)| (v, *a)))", "        let closure = Closure::of_new_version(tip, true, answers.iter().take(0).map(|(v, a)| (v, *a)))", ["-p","aspen-server","--test","receive","a_branch_created_at_an_old_trunk_commit_is_wider"]),
 ("M19 scoped dependency dropped", P, "                scoped_old: self.scoped_dep.as_ref().map(|v| v.id),\n            };\n            let dep_versions", "                scoped_old: None,\n            };\n            let dep_versions", ["-p","aspen-server","--test","receive","a_scoped_push_depends_on_the_scoped_branch"]),
 ("M20 per-ref permission ignored", P, "        if !self.access.may_update(&command.name) {", "        if false && !self.access.may_update(&command.name) {", ["-p","aspen-server","--test","receive","refs_outside_the_callers_grant"]),
 ("M21 a write inside trunk's pack gets its own pack", P, ".filter(|t| matches!(&self.planned[t.0].contents, Contents::Trunk(m) if m[at]))", ".filter(|t| false && matches!(&self.planned[t.0].contents, Contents::Trunk(m) if m[at]))", ["-p","aspen-server","--test","receive","a_tag_inside_trunks_pack_shares_it_and_is_wider"]),
 ("M22 no own-pack budget (one pack per write, never shared)", P, "                match g.reach_within(at, &self.held, self.own_budget, &mut self.scratch) {", "                match g.reach_within(at, &self.held, usize::MAX, &mut self.scratch) {", ["-p","aspen-server","--test","receive","a_first_push_of_many_tags_stores_each_object_at_most_twice"]),
 ("M23 writes at one tip not placed together", P, "        if let Some(pl) = self.by_tip.get(&tip) {", "        if let Some(pl) = self.by_tip.get(&tip).filter(|_| false) {", ["-p","aspen-server","--test","receive","two_refs_at_one_new_commit_share_one_pack"]),
 ("M24 shared set: any tip counts as spanning", G, "            (Some((&at, _)), None) => Some(at as usize),", "            (Some((&at, _)), _) => Some(at as usize),", ["-p","aspen-pack","--lib","a_shared_set_is_the_union"]),
 ("M25 bounded walk ignores its limit", G, "            over = out.len() > limit;\n        }\n        for &at in &out {", "            over = false;\n        }\n        for &at in &out {", ["-p","aspen-pack","--lib","reach_within_matches_reach"]),
 ("M26 advertised haves not limited to the searched closure", M, "                if newer.parent != Some(old.id) || newer.deps.trunk != Some(old.id) {", "                if newer.parent != Some(old.id) {", ["-p","aspen-server","--test","receive","after_a_forced_trunk_update_skipped_tips_are_not_advertised"]),
 ("M27 forced trunk holds a slot across its ancestry walks", P, "        let mut chosen = None;\n        // Ancestry walks take their own slots: no slot is held here yet.", "        let _early = self.svc.gate.acquire().await.map_err(store)?;\n        let mut chosen = None;", ["-p","aspen-server","--test","packs","a_forced_trunk_update_runs_under_a_one_slot_gate"]),
 ("M28 packs uploaded while planning (refused writes store packs)", P, "        Ok(Plan::Store {\n            pack,\n            deps,\n            closure,\n            commits,\n            jobs,\n        })\n    }", "        if let Some(pl) = pack { self.ensure_stored(pl.pack).await?; }\n        Ok(Plan::Store {\n            pack,\n            deps,\n            closure,\n            commits,\n            jobs,\n        })\n    }", ["-p","aspen-server","--test","receive","an_atomic_push_is_all_or_nothing"]),
 ("M29 own packs copy held delta bases", P, "                (flags_of(&members, None), None)", "                (flags_of(&members, Some(&self.held)), None)", ["-p","aspen-server","--test","receive","branches_over_resent_history_store_at_most_twice_the_pack"]),
 ("M30 the shared pack copies held delta bases", P, "Contents::Shared(set) => (flags_of(&set.members(), None), None)", "Contents::Shared(set) => (flags_of(&set.members(), Some(&self.held)), None)", ["-p","aspen-server","--test","receive","a_shared_pack_over_resent_history_copies_no_held_base"]),
 ("M31 a forced trunk's pack not charged to the own-pack budget", P, "            .saturating_sub(keep.iter().filter(|k| **k).count());", "            .saturating_sub(0 * keep.iter().filter(|k| **k).count());", ["-p","aspen-server","--test","receive","a_forced_trunk_with_refs_on_its_history_stores_at_most_twice_the_pack"]),
 ("M32 one-shared-pack assertion inverted", P, "                        !self\n                            .planned", "                        self\n                            .planned", ["-p","aspen-server","--test","receive","the_spanning_tip_of_the_shared_pack_is_exact"]),
]
only = sys.argv[1:]
res=[]
bad=False
for name, f, old, new, args in muts:
    if only and not any(name.startswith(o+" ") for o in only): continue
    src=open(f).read()
    if src.count(old)!=1:
        res.append((name,"PATTERN NOT FOUND x%d"%src.count(old))); print(res[-1], flush=True); continue
    open(f,'w').write(src.replace(old,new))
    try:
        r=subprocess.run(["cargo","test","--locked"]+args[:-1]+["--",args[-1]],capture_output=True,text=True,timeout=1500)
        out=r.stdout+r.stderr
        if "error[" in out or "could not compile" in out: verdict="COMPILE ERROR"
        elif r.returncode!=0 and "FAILED" in out: verdict="test FAILS (good)"
        elif "running 0 tests" in out and "1 passed" not in out: verdict="NO TEST RAN"
        else: verdict="test PASSES (bad)"
    finally:
        open(f,'w').write(src)
    res.append((name,verdict)); print(res[-1], flush=True)

sys.exit(0 if all(v == 'test FAILS (good)' for _, v in res) else 1)
