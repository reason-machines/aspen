#!/usr/bin/env python3
"""Removes each rule a push's search set (issue #149,
crates/aspen-server/src/receive/closure.rs) relies on, one at a time, and
shows that a test fails without it. Run from the repository root:

    python3 crates/aspen-server/scripts/check-mutations-149.py [R1 R2 ...]

Each mutation is an exact text replacement in one source file; the file is
restored afterwards. A mutation whose pattern no longer matches is reported,
so the list fails loudly when the code it targets changes."""
import subprocess, sys

C = 'crates/aspen-server/src/receive/closure.rs'
T = ["-p", "aspen-server", "--lib"]
muts = [
 ("R1 a full cache drops everything (clear-all eviction)", C,
  "            self.cold = std::mem::take(&mut self.hot);\n            self.hot_weight = 0;",
  "            self.hot.clear();\n            self.cold.clear();\n            self.hot_weight = 0;",
  T + ["a_cache_turnover_does_not_make_a_push_walk_the_history_again"]),
 ("R2 a late absence replaces a compacted block", C,
  "                Some(Block::Compacted(p)) => {\n                    packs.insert(p);\n                }\n                prev => {",
  "                Some(Block::Compacted(_)) if false => {}\n                prev => {",
  T + ["a_late_answer_does_not_forget_a_compacted_cover"]),
 ("R3 a cover's pack row is not checked", C,
  "        self.check_rows(meta, repo, name, level, &found).await?;",
  "",
  T + ["a_cover_whose_pack_has_no_row_is_corrupt"]),
 ("R4 closures are not remembered (every push walks its history)", C,
  "        self.lock().summaries.insert(root, Arc::clone(&acc));",
  "",
  T + ["a_cache_turnover_does_not_make_a_push_walk_the_history_again"]),
 ("R5 an absent block is asked about on every push", C,
  "Some(Block::Absent { asked, wait }) if now < asked + wait => open.push(first),",
  "Some(Block::Absent { asked, wait }) if false && now < asked + wait => open.push(first),",
  T + ["without_compaction_a_push_reads_one_version_and_asks_each_block_once_a_minute"]),
 ("R6 an absent block's wait never grows", C,
  "(wait * 2).min(self.tuning.recheck_max)",
  "wait.min(self.tuning.recheck_max)",
  T + ["without_compaction_a_push_reads_one_version_and_asks_each_block_once_a_minute"]),
 ("R7 a cover of another span stands for the block", C,
  "                && c.range.last().get() == first + (span - 1)\n",
  "",
  T + ["a_cover_of_another_layout_is_not_used"]),
 ("R8 compacted covers are never used", C,
  "                found.insert(first, p);",
  "                let _ = p;",
  T + ["a_push_reads_no_more_metadata_after_4000_trunk_pushes_than_after_300"]),
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

sys.exit(0 if res and all(v == 'test FAILS (good)' for _, v in res) else 1)
