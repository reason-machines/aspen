#!/usr/bin/env python3
"""Breaks each guard of C1's garbage collection (issue #48) one at a time
and shows that a named test fails without it. Run from the repository
root:

    python3 crates/aspen-compact/scripts/check-mutations.py [G1 G2 ...]

Each mutation is one or more exact text replacements in one source file;
the file is restored afterwards. A mutation whose pattern no longer
matches is reported, so the list fails loudly when the code it targets
changes. The aspen-meta store tests also run against Postgres when
ASPEN_TEST_PG_BIN is set."""
import subprocess, sys

MG = 'crates/aspen-meta/src/gc.rs'
MC = 'crates/aspen-meta/src/covers.rs'
P = 'crates/aspen-server/src/receive/push.rs'
B = 'crates/aspen-compact/src/build.rs'
CG = 'crates/aspen-compact/src/gc.rs'
XK = 'crates/aspen-express/src/key.rs'
RACE = ["-p", "aspen-server", "--test", "gc_race"]
STORE = ["-p", "aspen-meta", "--test", "store"]

muts = [
    ("G1 a checksum registered again keeps generation 0 (late deletes hit it)", MG,
     [("        if generation == PackGeneration::FIRST {\n            return Ok(inserted);",
       "        if true || generation == PackGeneration::FIRST {\n            return Ok(inserted);")],
     RACE + ["a_late_blob_delete_never_removes_a_pack_pushed_again"]),
    ("G2 the sweep leaves no tombstone", MG,
     [("            for p in &deleted {", "            for p in deleted.iter().take(0) {")],
     STORE + ["store"]),
    ("G3 push uploads before it registers", P,
     [('''        let record = self
            .svc
            .meta
            .register_pack(&new_pack)
            .await
            .map_err(store)?;
        let keys = record.keys();''',
       '''        let keys = aspen_core::pack::PackKeys::new(
            self.repo.id,
            &new_pack.checksum,
            aspen_core::pack::PackGeneration::FIRST,
        );'''),
      ("        let id = record.id;\n        self.timings.upload",
       "        let id = self.svc.meta.register_pack(&new_pack).await.map_err(store)?.id;\n        self.timings.upload")],
     RACE + ["a_sweep_during_a_push_upload_never_deletes_its_blobs"]),
    ("G4 compactor reuses a stored pack without registering or uploading it", B,
     [("        let record = self.meta.register_pack(&new).await?;\n        let keys = record.keys();\n        self.blobs",
       "        let reused = existing.clone();\n        let record = match reused.clone() { Some(r) => r, None => self.meta.register_pack(&new).await? };\n        let keys = record.keys();\n        if reused.is_none() { self.blobs"),
      ("            .upload_file(&keys.entries, &written.entries_path)\n            .await?;\n",
       "            .upload_file(&keys.entries, &written.entries_path)\n            .await?; }\n")],
     RACE + ["a_compaction_reusing_a_stored_pack_uploads_and_keeps_it"]),
    ("G5 a cover may record a swept pack (refused as a conflict, not retryably)", MC,
     [("            if !exists {", "            if false && !exists {")],
     STORE + ["store"]),
    ("G6 pending deletes of earlier passes are not re-driven", CG,
     [("            let pending = meta.pending_pack_deletes(batch).await?;",
       "            let pending: Vec<_> = meta.pending_pack_deletes(batch).await?.into_iter().filter(|t| swept.iter().any(|r| r.pack.checksum == t.checksum && r.generation == t.generation)).collect();")],
     RACE + ["a_crashed_workers_deletions_are_finished_by_the_next_pass"]),
    ("G7 express keys ignore the generation", XK,
     [("            key: pack.keys().pack,", "            key: PackKeys::new(pack.pack.repo, &pack.pack.checksum, PackGeneration::FIRST).pack,")],
     ["-p", "aspen-express", "--lib", "a_stored_packs_key_carries_its_generation"]),
    ("G8 aspen serve does not run the worker", 'crates/aspen-cli/src/serve.rs',
     [("            .run(std::future::pending(), |e| {", "            .run(std::future::ready(()), |e| {")],
     ["-p", "aspen-cli", "--test", "cli", "dev_runs_the_compaction_worker"]),
    ("G9 commit_push leaves a swept pack to the foreign key (a conflict, not retryable)",
     'crates/aspen-meta/src/push.rs',
     [("if let Some(gone) = chunk.iter().find(|id| !found.contains(id)) {",
       "if let Some(gone) = chunk.iter().find(|id| false && !found.contains(id)) {")],
     STORE + ["store"]),
    ("G16 a one-statement push commit on Postgres reports a swept pack as a lost race (not retryable)",
     'crates/aspen-meta/src/push.rs',
     [('    if rows::get::<i64>(&row, "packs_found")? == 0 {',
       '    if false && rows::get::<i64>(&row, "packs_found")? == 0 {')],
     STORE + ["postgres_store"]),
    ("G17 a one-statement registration on Postgres inserts at generation 0 while a sweep deletes the old row", MG,
     [("WHERE NOT EXISTS (SELECT 1 FROM seen) AND NOT EXISTS (SELECT 1 FROM tomb)",
       "WHERE NOT EXISTS (SELECT 1 FROM tomb)")],
     STORE + ["postgres_store"]),
    ("G10 the compactor reads a source pack at generation 0", B,
     [("    ) -> Result<LocalSource, CompactError> {\n        let keys = row.keys();",
       "    ) -> Result<LocalSource, CompactError> {\n        let keys = aspen_core::pack::PackKeys::new(row.pack.repo, &row.pack.checksum, aspen_core::pack::PackGeneration::FIRST);")],
     RACE + ["compaction_and_indexing_read_a_source_pack_by_its_generation"]),
    ("G11 the indexer reads a version's pack at generation 0", 'crates/aspen-compact/src/index.rs',
     [("        let keys = row.keys();",
       "        let keys = aspen_core::pack::PackKeys::new(row.pack.repo, &row.pack.checksum, aspen_core::pack::PackGeneration::FIRST);")],
     RACE + ["compaction_and_indexing_read_a_source_pack_by_its_generation"]),
    ("G12 the sweep's watermark passes a candidate another transaction holds", MG,
     [("let decided = walked.iter().take_while(|(_, id)| !held.contains(id));",
       "let decided = walked.iter();")],
     STORE + ["postgres_store"]),
    ("G13 a sweep runs without checking its lease", 'crates/aspen-meta/src/lease.rs',
     [("    if extended == 1 {", "    if extended <= 1 {")],
     STORE + ["store"]),
    ("G14 a live lease is handed to another owner", 'crates/aspen-meta/src/lease.rs',
     [("                        (true, false) => return Ok(None),",
       "                        (true, false) => epoch + 1,")],
     STORE + ["store"]),
]

only = sys.argv[1:]
failed = False
for name, f, edits, args in muts:
    if only and not any(name.startswith(o + " ") for o in only):
        continue
    src = open(f).read()
    mutated = src
    missing = [old for old, _ in edits if mutated.count(old) != 1]
    if missing:
        print(f"{name}: PATTERN NOT FOUND", flush=True)
        failed = True
        continue
    for old, new in edits:
        mutated = mutated.replace(old, new)
    open(f, 'w').write(mutated)
    try:
        r = subprocess.run(["cargo", "test", "--locked"] + args[:-1] + ["--", args[-1]],
                           capture_output=True, text=True, timeout=1500)
        out = r.stdout + r.stderr
        if "error[" in out or "could not compile" in out:
            verdict = "COMPILE ERROR"
            failed = True
        elif r.returncode != 0:
            verdict = "killed (test fails)"
        else:
            verdict = "SURVIVED"
            failed = True
    finally:
        open(f, 'w').write(src)
    print(f"{name}: {verdict}", flush=True)
sys.exit(1 if failed else 0)
