//! Targets for the pack surfaces (`aspen-pack`): push ingest (pass 1 and
//! completion), [`verify_pack`], the stitcher, and the stored formats.
//!
//! Limits are small (1 MiB per object, 16 MiB inflated per pack) so a
//! size the checks miss shows up as a libFuzzer out-of-memory or
//! malloc-limit crash instead of passing unnoticed.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use aspen_core::error::UnpackError;
use aspen_core::id::{ObjectId, PackChecksum, PackId, RepoId, UnixMicros};
use aspen_core::ingest::{InMemoryBases, ObjectKind};
use aspen_core::pack::{
    EntryTable, NewPack, PackGeneration, PackOrigin, PackRecord, PushLimits, entry_table_len,
};
use aspen_pack::graph::PackGraph;
use aspen_pack::idx::IdxV2;
use aspen_pack::ingest::{CompleteOptions, IngestedPack, complete, receive};
use aspen_pack::scan::{ScanOptions, parse_commit, scan};
use aspen_pack::stitch::{SegmentIds, StitchPlan, Stitcher, stitch_all};
use aspen_pack::verify::{VerifiedPack, verify_pack};
use aspen_pack::{MAX_DELTA_DEPTH, MaxInflatedBytes, MaxObjectBytes, header};

use crate::Reached;
use crate::input::{Input, Mix};
use crate::packgen::{self, RawEntry, deflate, fix_trailer, object_id, type_code, write_pack};

/// Object limit for every target: 1 MiB.
pub const MAX_OBJECT: u64 = 1 << 20;
/// Total inflated limit: 16 MiB.
pub const MAX_INFLATED: u64 = 16 << 20;
/// Push limits: header counts above 5,000 and packs above 32 MiB are
/// refused before the body is read.
pub const LIMITS: PushLimits = PushLimits {
    max_objects: 5_000,
    max_pack_bytes: 32 << 20,
};

fn max_object() -> MaxObjectBytes {
    MaxObjectBytes::new(MAX_OBJECT).expect("non-zero")
}

fn max_inflated() -> MaxInflatedBytes {
    MaxInflatedBytes::new(MAX_INFLATED).expect("non-zero")
}

fn complete_options() -> CompleteOptions {
    let mut o = CompleteOptions::new(max_object(), max_inflated());
    o.thread_limit = Some(1);
    o
}

/// A fresh directory for one run, removed on drop. Every run gets its own
/// so a crash input replays on its own.
#[derive(Debug)]
struct Work(PathBuf);

impl Work {
    fn new() -> Self {
        static ROOT: OnceLock<tempfile::TempDir> = OnceLock::new();
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = ROOT.get_or_init(|| {
            tempfile::Builder::new()
                .prefix("aspen-fuzz-")
                .tempdir()
                .expect("a scratch directory")
        });
        let dir = root
            .path()
            .join(NEXT.fetch_add(1, Ordering::Relaxed).to_string());
        fs::create_dir_all(&dir).expect("scratch subdirectory");
        Self(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn verify_bytes(bytes: &[u8], work: &Work, name: &str) -> Result<VerifiedPack, String> {
    let path = work.path(name);
    fs::write(&path, bytes).expect("scratch write");
    verify_pack(&path, &work.0, max_object(), max_inflated()).map_err(|e| e.to_string())
}

fn ids_of(table: &EntryTable) -> BTreeSet<ObjectId> {
    table.entries().iter().map(|e| e.id).collect()
}

/// The checks every pack [`verify_pack`] accepts must pass: its table
/// keeps the layout rules, lists each object once, and its `.idx` parses
/// against the pack with the same objects.
fn check_verified(v: &VerifiedPack, size: u64) {
    v.table
        .check_layout()
        .expect("verified table keeps the layout");
    let ids = ids_of(&v.table);
    assert_eq!(ids.len(), v.table.entries().len(), "an object twice");
    let idx = IdxV2::parse(&v.idx, Some(size)).expect("verified .idx parses");
    assert_eq!(idx.ids().collect::<BTreeSet<_>>(), ids);
    assert_eq!(
        v.max_delta_depth,
        aspen_pack::verify::max_delta_depth(&v.table)
    );
}

/// The stored-pack invariants for a pack ingest accepted, checked through
/// [`verify_pack`] (the A2 oracle) rather than ingest's own code: it
/// indexes alone (self-contained), lists no object twice, puts every base
/// first, and the `.idx`, entry table and row ingest wrote describe it.
/// Then the scans a push and the `index_commits` job run over a stored
/// pack must not panic on the object contents a client chose.
fn check_stored(p: &IngestedPack, work: &Work) -> BTreeSet<ObjectId> {
    let bytes = fs::read(&p.pack_path).expect("stored pack");
    let v = verify_pack(&p.pack_path, &work.0, max_object(), max_inflated())
        .unwrap_or_else(|e| panic!("ingest stored a pack verify_pack refuses: {e}"));
    check_verified(&v, bytes.len() as u64);
    assert_eq!(fs::read(&p.idx_path).expect("stored .idx"), v.idx);
    assert_eq!(
        fs::read(&p.entries_path).expect("stored entry table"),
        v.table.encode()
    );
    assert_eq!(p.size, bytes.len() as u64);
    assert_eq!(p.object_count as usize, v.table.entries().len());
    assert_eq!(p.ref_delta_count, v.table.ref_delta_count());
    assert_eq!(p.checksum.as_bytes(), &bytes[bytes.len() - 20..]);
    assert!(p.max_delta_depth <= MAX_DELTA_DEPTH);
    let repo = RepoId::new(1).expect("non-zero");
    p.new_pack(repo).validate().expect("a valid packs row");
    let ids = ids_of(&v.table);
    for b in p.completion_bases.as_slice().iter() {
        assert!(ids.contains(b), "completion base {b} is not stored");
    }
    let options = ScanOptions {
        commits: true,
        missing: true,
        thread_limit: Some(1),
        max_object_bytes: max_object(),
        max_inflated_bytes: max_inflated(),
        verify_checksums: true,
    };
    if let Ok(found) = scan(&p.pack_path, &p.idx_path, options) {
        for c in &found.commits {
            assert!(ids.contains(&c.id));
        }
    }
    if let Ok(graph) = PackGraph::scan(&p.pack_path, &p.idx_path, options) {
        assert_eq!(graph.len(), ids.len());
        for (i, id) in graph.ids().iter().enumerate() {
            assert_eq!(graph.position(id), Some(i));
            let _ = graph.reach(id, &vec![false; graph.len()]);
        }
    }
    ids
}

/// Receives and completes `bytes` against `bases`.
fn ingest(bytes: &[u8], bases: &InMemoryBases, work: &Work) -> Result<IngestedPack, UnpackError> {
    let received = receive(&mut &bytes[..], &work.path("spool"), &LIMITS)?;
    assert_eq!(received.received_bytes, bytes.len() as u64);
    let out = work.path("out");
    fs::create_dir_all(&out).expect("output directory");
    complete(&received, bases, &out, &complete_options())
}

/// `pack-bytes`: raw bytes as a pushed pack and as a stored pack. With the
/// first byte's low bit set the trailer is recomputed first, so mutations
/// inside entries get past the checksum. [`verify_pack`] and ingest
/// (`receive`, then `complete` with no dependencies) both run; whatever
/// ingest accepts must pass `check_stored`, and a pack `verify_pack`
/// accepts within the push limits must be received and completed, to the
/// same objects (a self-contained, base-first pack is a valid push).
pub fn pack_bytes(data: &[u8]) {
    let Some((&config, rest)) = data.split_first() else {
        return;
    };
    let mut bytes = rest.to_vec();
    if config & 1 == 1 {
        fix_trailer(&mut bytes);
    }
    let work = Work::new();
    let verified = verify_bytes(&bytes, &work, "in.pack").ok();
    if let Some(v) = &verified {
        check_verified(v, bytes.len() as u64);
    }
    let received = match receive(&mut &bytes[..], &work.path("spool"), &LIMITS) {
        Ok(r) => r,
        Err(e) => {
            // A pack verify_pack accepts is well formed; within the push
            // limits, pass 1 has no reason to refuse it.
            if verified.is_some() {
                let count = u32::from_be_bytes(bytes[8..12].try_into().expect("4 bytes"));
                assert!(
                    count > LIMITS.max_objects || bytes.len() as u64 > LIMITS.max_pack_bytes,
                    "verify_pack accepts a pack within the push limits but receive refuses it: {e}"
                );
            }
            return;
        }
    };
    assert_eq!(received.received_bytes, bytes.len() as u64);
    assert_eq!(
        received.object_count,
        u32::from_be_bytes(bytes[8..12].try_into().expect("4 bytes"))
    );
    let out = work.path("out");
    fs::create_dir_all(&out).expect("output directory");
    let completed = complete(
        &received,
        &InMemoryBases::default(),
        &out,
        &complete_options(),
    );
    match (&verified, completed) {
        (_, Ok(p)) => {
            let stored = check_stored(&p, &work);
            if let Some(v) = &verified {
                assert_eq!(stored, ids_of(&v.table), "ingest and verify disagree");
            }
        }
        (Some(v), Err(e)) if v.max_delta_depth <= MAX_DELTA_DEPTH && received.object_count > 0 => {
            panic!("verify_pack accepts the pack but ingest refuses it: {e}")
        }
        (_, Err(_)) => {}
    }
}

/// How an entry of a generated push pack is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    Full,
    /// OFS_DELTA against an earlier entry.
    Ofs(usize),
    /// REF_DELTA against another entry's ID (any entry: a forward base, or
    /// a cycle when the bases loop).
    RefEntry(usize),
    /// REF_DELTA against an object outside the pack (a thin base, which
    /// the lookup may or may not hold).
    RefExternal(usize),
}

#[derive(Debug, Clone)]
struct Obj {
    kind: ObjectKind,
    data: Vec<u8>,
}

impl Obj {
    fn id(&self) -> ObjectId {
        object_id(self.kind, &self.data)
    }
}

/// Content for a generated object: shaped bytes, an edit of an earlier
/// object (so deltas copy real ranges), or, rarely, a large run of one
/// byte (a zip bomb: a few KiB on the wire, up to 3 MiB inflated, over the
/// 1 MiB object limit about two times in three).
fn gen_data(kind: ObjectKind, earlier: &[Obj], input: &mut Input<'_>) -> Vec<u8> {
    if input.one_in(48) {
        let size = input.below(3 << 20);
        return vec![input.byte(); size];
    }
    if !earlier.is_empty() && input.flag() {
        let base = &earlier[input.below(earlier.len())].data;
        let mut v = base.clone();
        let at = input.below(v.len() + 1);
        let cut = input.below(v.len() - at + 1).min(16);
        let insert = input.bytes(24).to_vec();
        v.splice(at..at + cut, insert);
        return v;
    }
    packgen::content(kind, input)
}

/// `pack-ingest`: a generated push pack with fuzzer-chosen layout: full
/// objects, OFS_DELTAs, REF_DELTAs against later entries (forward bases)
/// and against each other (cycles), thin bases the lookup may lack, objects
/// the lookup also holds (resends), repeated objects, zip bombs over the
/// object limit, long delta chains, and lies.
///
/// Every lie makes the pack certainly invalid: a lying entry's header size
/// is changed to a different value, its zlib stream is cut short, or a
/// byte of its zlib checksum (Adler-32) is flipped, and a lying header
/// count differs from the entry count. Some lying entries also carry
/// garbage first (random bytes, or a delta header claiming a huge result),
/// which the certain change then covers. A pack with a lie must be refused.
///
/// For a pack with no lies the model predicts the outcome. One whose every
/// base resolves, with no object twice, every size within the object limit
/// and well within the total limit must be accepted, store exactly its
/// objects plus the thin bases its REF_DELTAs needed, and pass
/// `check_stored`. One that repeats an object, has a base that resolves
/// nowhere, an object over the limit or a delta shorter than Git accepts
/// ([`packgen::MIN_DELTA_DATA`], issue #162) must be refused. Anything accepted
/// must pass `check_stored`.
///
/// Every member of a REF_DELTA cycle, and every delta that leads into one,
/// gets the cycle's one kind: the model does not cover cycles of mixed
/// kinds (see the kind assignment below and `fuzz/README.md`).
pub fn pack_ingest(data: &[u8]) {
    pack_ingest_reached(data);
}

/// [`pack_ingest`], returning the oracle paths the input reached.
pub fn pack_ingest_reached(data: &[u8]) -> BTreeSet<Reached> {
    let mut reached = BTreeSet::new();
    let mut input = Input::new(data);
    // A long OFS_DELTA chain now and then. Git's depth limit (4,095) is
    // pinned by aspen-pack's own tests; a chain that long would cost the
    // fuzzer most of its time.
    let deep_chain = input.one_in(32);
    let n = if deep_chain {
        64 + input.below(256)
    } else {
        1 + input.below(12)
    };

    // External objects first (bases a thin pack may use).
    let mut externals: Vec<Obj> = Vec::new();
    for _ in 0..input.below(4) {
        let kind = packgen::kind(&mut input);
        let data = gen_data(kind, &externals, &mut input);
        externals.push(Obj { kind, data });
    }

    // Entry forms, then kinds (a delta has its root's kind), then data.
    let mut forms = Vec::with_capacity(n);
    for i in 0..n {
        let form = if deep_chain {
            if i == 0 { Form::Full } else { Form::Ofs(i - 1) }
        } else {
            match input.below(6) {
                0 | 1 => Form::Full,
                2 if i > 0 => Form::Ofs(input.below(i)),
                3 | 4 if n > 1 => {
                    let j = input.below(n - 1);
                    Form::RefEntry(if j >= i { j + 1 } else { j })
                }
                5 if !externals.is_empty() => Form::RefExternal(input.below(externals.len())),
                _ => Form::Full,
            }
        };
        forms.push(form);
    }
    let mut kinds: Vec<ObjectKind> = (0..n).map(|_| packgen::kind(&mut input)).collect();
    if deep_chain {
        kinds = vec![ObjectKind::Blob; n];
    }
    // A delta's object has its base's kind; follow each chain to its root.
    // Everything that leads into a cycle gets one kind too: with mixed
    // kinds, a delta resolved against the lookup's copy of a cycle member
    // takes that copy's kind, becomes a different object than the model's,
    // and the cycle the model sees is not one.
    for i in 0..n {
        let mut at = i;
        let mut steps = 0;
        let root = loop {
            match forms[at] {
                Form::Full => break Some(kinds[at]),
                Form::RefExternal(x) => break Some(externals[x].kind),
                Form::Ofs(j) | Form::RefEntry(j) => at = j,
            }
            steps += 1;
            if steps > n {
                break None;
            }
        };
        kinds[i] = root.unwrap_or(ObjectKind::Blob);
    }
    let mut objs: Vec<Obj> = Vec::with_capacity(n);
    for (i, kind) in kinds.iter().enumerate() {
        let data = if deep_chain {
            // Each level appends one byte: a chain of tiny deltas.
            let mut v = objs.last().map_or_else(Vec::new, |o: &Obj| o.data.clone());
            v.push(i as u8);
            v
        } else {
            let earlier: Vec<Obj> = objs.iter().chain(&externals).cloned().collect();
            gen_data(*kind, &earlier, &mut input)
        };
        objs.push(Obj { kind: *kind, data });
    }
    let ids: Vec<ObjectId> = objs.iter().map(Obj::id).collect();

    // The lookup: some externals (missing ones make thin bases fail), and
    // sometimes copies of objects the pack itself holds (a resend).
    let mut bases = InMemoryBases::default();
    let mut held: BTreeSet<ObjectId> = BTreeSet::new();
    for o in &externals {
        if !input.one_in(5) {
            bases.insert(o.id(), o.kind, o.data.clone());
            held.insert(o.id());
        }
    }
    for o in &objs {
        if input.one_in(6) {
            bases.insert(o.id(), o.kind, o.data.clone());
            held.insert(o.id());
        }
    }

    // Encode, with occasional lies.
    let mut lies = false;
    let mut count_lie = false;
    let mut entries = Vec::with_capacity(n);
    for (i, form) in forms.iter().enumerate() {
        let obj = &objs[i];
        let base_data = match *form {
            Form::Full => None,
            Form::Ofs(j) | Form::RefEntry(j) => Some(&objs[j].data),
            Form::RefExternal(x) => Some(&externals[x].data),
        };
        let mut plain = match base_data {
            None => obj.data.clone(),
            Some(b) => packgen::delta(b, &obj.data),
        };
        let lie = !deep_chain && input.one_in(24);
        if lie {
            lies = true;
            match input.below(4) {
                // A delta header that claims a huge result.
                0 => {
                    let mut d = Vec::new();
                    packgen::varint(u64::from(input.u32()) << input.below(24), &mut d);
                    packgen::varint(u64::from(input.u32()) << input.below(24), &mut d);
                    d.extend_from_slice(input.bytes(8));
                    plain = d;
                }
                // Random bytes in place of a delta or object.
                1 => plain = input.bytes(32).to_vec(),
                _ => {}
            }
        }
        let mut declared = plain.len() as u64;
        let mut body = deflate(&plain);
        if lie {
            // Each change makes the entry certainly invalid, whatever the
            // garbage above did to `plain`.
            match input.below(4) {
                // A different header size. `u64 >> k` can land on the
                // true size, so step off it.
                0 => {
                    declared = input.u64() >> input.below(64);
                    if declared == plain.len() as u64 {
                        declared = declared.wrapping_add(1);
                    }
                }
                // A zlib stream cut short: inflating runs on into the
                // next entry or the trailer.
                1 => {
                    let keep = input.below(body.len());
                    body.truncate(keep);
                }
                // A flipped byte of the zlib checksum (Adler-32, the last 4
                // bytes). A flip elsewhere is not certain to matter: the
                // header's level bits and the final byte's padding bits are
                // ignored, and a changed match distance inside a run of one
                // byte copies the same bytes.
                2 => {
                    let at = body.len() - 1 - input.below(4);
                    body[at] ^= input.byte() | 1;
                }
                // One byte more than the stream holds.
                _ => declared = declared.wrapping_add(1),
            }
        }
        let (code, ofs_base, ref_base) = match *form {
            Form::Full => (type_code(obj.kind), None, None),
            Form::Ofs(j) => (6, Some(j), None),
            Form::RefEntry(j) => (7, None, Some(ids[j])),
            Form::RefExternal(x) => (7, None, Some(externals[x].id())),
        };
        entries.push(RawEntry {
            type_code: code,
            declared,
            ofs_base,
            ofs_distance: None,
            ref_base,
            body,
        });
    }
    let count = if !deep_chain && input.one_in(32) {
        // `below(n + 3)` can return `n`, the true count: no lie then.
        let c = input.below(n + 3);
        count_lie = c != n;
        Some(c as u32)
    } else {
        None
    };
    let pack = write_pack(&entries, count);

    // The model: which entries resolve, what the pack must store. A base
    // the pack itself holds resolves only through the pack's copy: ingest
    // refuses the lookup's copy of such an ID (the duplicate rerun), so a
    // lookup cannot break a cycle of in-pack bases. Any other base resolves
    // only through the lookup.
    let pack_ids: BTreeSet<ObjectId> = ids.iter().copied().collect();
    let base_resolves = |id: ObjectId, resolved: &[bool]| {
        if pack_ids.contains(&id) {
            (0..n).any(|k| resolved[k] && ids[k] == id)
        } else {
            held.contains(&id)
        }
    };
    let mut resolved = vec![false; n];
    let mut changed = true;
    while changed {
        changed = false;
        for i in 0..n {
            if resolved[i] {
                continue;
            }
            let ok = match forms[i] {
                Form::Full => true,
                Form::Ofs(j) => resolved[j],
                Form::RefEntry(j) => base_resolves(ids[j], &resolved),
                Form::RefExternal(x) => base_resolves(externals[x].id(), &resolved),
            };
            if ok {
                resolved[i] = true;
                changed = true;
            }
        }
    }
    let all_resolve = resolved.iter().all(|r| *r);
    let repeats = pack_ids.len() != n;
    let mut thin: BTreeSet<ObjectId> = BTreeSet::new();
    let mut inflated: u64 = 0;
    let mut over_object = false;
    // A delta with less data than Git accepts (issue #162): Git's
    // `index-pack` refuses it, so ingest must too.
    let short_delta = forms
        .iter()
        .zip(&entries)
        .any(|(form, e)| *form != Form::Full && e.declared < packgen::MIN_DELTA_DATA);
    for (i, form) in forms.iter().enumerate() {
        let size = objs[i].data.len() as u64;
        inflated += size;
        // The entry's own size counts too: for a delta, its instruction
        // stream, which outgrows the result when most of it is inserted.
        over_object |= size > MAX_OBJECT || entries[i].declared > MAX_OBJECT;
        let base = match *form {
            Form::RefEntry(j) => Some(&objs[j]),
            Form::RefExternal(x) => Some(&externals[x]),
            Form::Ofs(j) => Some(&objs[j]),
            Form::Full => None,
        };
        if let Some(b) = base {
            over_object |= b.data.len() as u64 > MAX_OBJECT;
        }
        if matches!(form, Form::RefEntry(_) | Form::RefExternal(_)) {
            let b = base.expect("a REF_DELTA has a base");
            let id = b.id();
            if held.contains(&id) && !pack_ids.contains(&id) {
                thin.insert(id);
            }
        }
    }
    inflated += thin
        .iter()
        .map(|id| {
            externals
                .iter()
                .find(|o| &o.id() == id)
                .map_or(0, |o| o.data.len() as u64)
        })
        .sum::<u64>();

    let work = Work::new();
    let result = ingest(&pack, &bases, &work);
    if std::env::var_os("ASPEN_FUZZ_EXPLAIN").is_some() {
        for i in 0..n {
            eprintln!(
                "entry {i}: {:?} kind {:?} id {} resolved {} len {}",
                forms[i],
                objs[i].kind,
                ids[i],
                resolved[i],
                objs[i].data.len()
            );
        }
        for (x, o) in externals.iter().enumerate() {
            eprintln!("external {x}: {} held {}", o.id(), held.contains(&o.id()));
        }
        eprintln!("held {held:?}");
        if let Some(dir) = std::env::var_os("ASPEN_FUZZ_DUMP") {
            std::fs::write(std::path::Path::new(&dir).join("pack"), &pack).expect("dump");
        }
        eprintln!(
            "result {:?}",
            result.as_ref().map(|p| p.completion_bases.clone())
        );
    }
    if lies || count_lie {
        if let Ok(p) = &result {
            panic!(
                "accepted a pack with a lie (entry {lies}, count {count_lie}; {} objects stored)",
                p.object_count
            );
        }
        if lies {
            reached.insert(Reached::RefusedLie);
        }
        if count_lie {
            reached.insert(Reached::RefusedCount);
        }
        return reached;
    }
    let must_fail = repeats || !all_resolve || over_object || short_delta;
    let must_pass = !must_fail && inflated <= MAX_INFLATED / 2;
    match &result {
        Ok(_) if must_fail => panic!(
            "accepted a pack the model refuses (repeats {repeats}, all resolve \
             {all_resolve}, over the object limit {over_object}, short delta \
             {short_delta})"
        ),
        Err(e) if must_pass => panic!("refused a valid pack: {e}"),
        Err(_) => {
            for (holds, path) in [
                (repeats, Reached::RefusedRepeat),
                (!all_resolve, Reached::RefusedUnresolved),
                (over_object, Reached::RefusedOverLimit),
                (short_delta, Reached::RefusedShortDelta),
            ] {
                if holds {
                    reached.insert(path);
                }
            }
        }
        Ok(_) => {}
    }
    if let Ok(p) = result {
        let stored = check_stored(&p, &work);
        let mut expected = pack_ids.clone();
        expected.extend(thin.iter().copied());
        assert_eq!(stored, expected, "stored objects");
        let inserted: BTreeSet<ObjectId> = p.completion_bases.as_slice().iter().copied().collect();
        assert_eq!(inserted, thin, "completion bases");
        reached.insert(Reached::ExactAccept);
    }
    reached
}

/// `stitch`: segments built from a shared pool of objects (so they repeat
/// objects across segments, as overlapping covers and completed push bases
/// do), each a valid stored pack: self-contained, base-first, with
/// OFS_DELTAs and REF_DELTAs against earlier entries. The stitched pack
/// must pass [`verify_pack`] (A2's obligation for D3) and hold exactly the
/// union of the segments' objects; the streaming [`Stitcher`], fed in random
/// chunks, must write the same bytes as [`stitch_all`]. Then one segment is
/// damaged (a flipped byte, a short or long feed, or an entry table whose
/// IDs disagree with its `.idx`): the stitch must fail, never emit a pack.
pub fn stitch(data: &[u8]) {
    stitch_reached(data);
}

/// [`stitch`], returning the oracle paths the input reached.
pub fn stitch_reached(data: &[u8]) -> BTreeSet<Reached> {
    let mut reached = BTreeSet::new();
    let mut input = Input::new(data);
    let mut pool: Vec<Obj> = Vec::new();
    for _ in 0..1 + input.below(16) {
        let kind = packgen::kind(&mut input);
        let data = gen_data(kind, &pool, &mut input);
        // Half the limit, so a delta's instructions (one byte per 127
        // inserted) stay under it too.
        if data.len() as u64 > MAX_OBJECT / 2 {
            continue;
        }
        let o = Obj { kind, data };
        if pool.iter().all(|p| p.id() != o.id()) {
            pool.push(o);
        }
    }
    if pool.is_empty() {
        return reached;
    }
    let work = Work::new();
    let mut segments: Vec<(PackId, Vec<u8>, EntryTable, Vec<u8>)> = Vec::new();
    for s in 0..1 + input.below(5) {
        let mut chosen: Vec<usize> = (0..pool.len()).filter(|_| input.flag()).collect();
        if chosen.is_empty() {
            chosen.push(input.below(pool.len()));
        }
        // Shuffle a little: segments list shared objects in different orders.
        for i in (1..chosen.len()).rev() {
            chosen.swap(i, input.below(i + 1));
        }
        let mut entries: Vec<RawEntry> = Vec::new();
        for (pos, &o) in chosen.iter().enumerate() {
            let obj = &pool[o];
            let earlier: Vec<usize> = (0..pos)
                .filter(|&e| pool[chosen[e]].kind == obj.kind)
                .collect();
            let (code, ofs_base, ref_base, plain) = if earlier.is_empty() || input.one_in(3) {
                (type_code(obj.kind), None, None, obj.data.clone())
            } else {
                let e = earlier[input.below(earlier.len())];
                let base = &pool[chosen[e]];
                let d = packgen::delta(&base.data, &obj.data);
                // Git never writes a delta shorter than it accepts (issue
                // #162), so such an object is stored whole; the flag is
                // read either way, so inputs keep their meaning.
                let ofs = input.flag();
                if (d.len() as u64) < packgen::MIN_DELTA_DATA {
                    (type_code(obj.kind), None, None, obj.data.clone())
                } else if ofs {
                    (6, Some(e), None, d)
                } else {
                    (7, None, Some(base.id()), d)
                }
            };
            entries.push(RawEntry {
                type_code: code,
                declared: plain.len() as u64,
                ofs_base,
                ofs_distance: None,
                ref_base,
                body: deflate(&plain),
            });
        }
        let bytes = write_pack(&entries, None);
        let v = verify_bytes(&bytes, &work, &format!("seg{s}.pack"))
            .unwrap_or_else(|e| panic!("a generated segment does not verify: {e}"));
        let id = PackId::new(s as i64 + 1).expect("non-zero");
        segments.push((id, bytes, v.table, v.idx));
    }
    let union: BTreeSet<ObjectId> = segments.iter().flat_map(|(_, _, t, _)| ids_of(t)).collect();
    let total: u64 = segments
        .iter()
        .map(|(_, _, t, _)| t.entries().len() as u64)
        .sum();
    let (out, outcome) = stitch_all(
        segments
            .iter()
            .map(|(id, b, t, _)| (*id, b.clone(), t.clone()))
            .collect(),
    )
    .unwrap_or_else(|e| panic!("stitching valid segments failed: {e}"));
    let v = verify_bytes(&out, &work, "stitched.pack")
        .unwrap_or_else(|e| panic!("the stitched pack does not verify: {e}"));
    check_verified(&v, out.len() as u64);
    assert_eq!(ids_of(&v.table), union, "stitched objects");
    assert_eq!(outcome.object_count as usize, union.len());
    assert_eq!(outcome.dropped, total - union.len() as u64);
    assert_eq!(outcome.size, out.len() as u64);
    assert_eq!(outcome.checksum.as_bytes(), &out[out.len() - 20..]);
    assert_eq!(outcome.max_delta_depth, v.max_delta_depth);

    let mut mix = Mix::new(input.u64());
    let streamed = stream(&segments, &mut mix, None).expect("valid segments stream");
    assert_eq!(streamed, out, "streaming and whole-segment stitches differ");
    reached.insert(Reached::ExactAccept);

    let victim = mix.below(segments.len());
    if let Some(damage) = Damage::pick(&mut mix, &segments[victim]) {
        if let Ok(bytes) = stream(&segments, &mut mix, Some((victim, damage))) {
            panic!(
                "a damaged segment ({damage:?}) stitched into {} bytes",
                bytes.len()
            );
        }
        reached.insert(Reached::RefusedDamage);
    }
    reached
}

/// One way to damage a segment for [`stream`].
#[derive(Debug, Clone, Copy)]
enum Damage {
    /// XOR a pack byte (header, entry or trailer).
    Flip { at: usize, mask: u8 },
    /// Feed only the first `len` bytes.
    Short { len: usize },
    /// Feed one byte more.
    Long,
    /// Swap two entries' IDs in the table (the `.idx` is unchanged).
    SwapIds { a: usize, b: usize },
}

impl Damage {
    fn pick(mix: &mut Mix, seg: &(PackId, Vec<u8>, EntryTable, Vec<u8>)) -> Option<Self> {
        let len = seg.1.len();
        let n = seg.2.entries().len();
        Some(match mix.below(4) {
            0 => Self::Flip {
                at: mix.below(len),
                mask: (mix.below(255) + 1) as u8,
            },
            1 => Self::Short {
                len: mix.below(len),
            },
            2 => Self::Long,
            _ if n >= 2 => {
                let a = mix.below(n);
                let b = (a + 1 + mix.below(n - 1)) % n;
                let entries = seg.2.entries();
                if entries[a].id == entries[b].id {
                    return None;
                }
                Self::SwapIds { a, b }
            }
            _ => return None,
        })
    }
}

/// The streaming stitch: a plan from each segment's `.idx`, then each
/// segment's table and bytes in random chunks, optionally damaging one.
fn stream(
    segments: &[(PackId, Vec<u8>, EntryTable, Vec<u8>)],
    mix: &mut Mix,
    damage: Option<(usize, Damage)>,
) -> Result<Vec<u8>, String> {
    let idxs: Vec<IdxV2<'_>> = segments
        .iter()
        .map(|(_, b, _, i)| IdxV2::parse(i, Some(b.len() as u64)).expect("verified .idx"))
        .collect();
    let plan = StitchPlan::new(
        segments
            .iter()
            .zip(&idxs)
            .map(|((id, _, t, _), idx)| SegmentIds {
                pack: *id,
                checksum: *t.checksum(),
                ids: idx.ids_with_offsets(),
            })
            .collect(),
    )
    .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut s = Stitcher::new(plan, &mut out);
    for (i, (_, bytes, table, _)) in segments.iter().enumerate() {
        let mut bytes = bytes.clone();
        let mut table = table.clone();
        match damage {
            Some((v, Damage::Flip { at, mask })) if v == i => bytes[at] ^= mask,
            Some((v, Damage::Short { len })) if v == i => bytes.truncate(len),
            Some((v, Damage::Long)) if v == i => bytes.push(0),
            Some((v, Damage::SwapIds { a, b })) if v == i => {
                let mut entries = table.entries().to_vec();
                let (ia, ib) = (entries[a].id, entries[b].id);
                entries[a].id = ib;
                entries[b].id = ia;
                table = EntryTable::new(*table.checksum(), bytes.len() as u64, entries)
                    .map_err(|e| e.to_string())?;
            }
            _ => {}
        }
        s.begin_segment(table).map_err(|e| e.to_string())?;
        let mut rest = &bytes[..];
        while !rest.is_empty() {
            let n = (1 + mix.below(3000)).min(rest.len());
            s.feed(&rest[..n], &mut out).map_err(|e| e.to_string())?;
            rest = &rest[n..];
        }
        s.end_segment().map_err(|e| e.to_string())?;
    }
    s.finish(&mut out).map_err(|e| e.to_string())?;
    Ok(out)
}

/// `pack-formats`: the stored formats read back from the blob store and
/// the small parsers around them, on raw bytes. The first byte picks one:
/// a `.idx` (parse, list, look up every ID, both parse modes), an entry
/// table (decoded for a `packs` row taken from its own header, re-encoded
/// and decoded again), an entry header (decode, then re-encode what it
/// decoded) and a commit (Git's commit parsing rules, `parse_commit`).
pub fn pack_formats(data: &[u8]) {
    pack_formats_decodes(data);
}

/// [`pack_formats`], returning whether the parser the first byte picks
/// accepted the rest (for a `.idx`, the strict parse). The replay test uses
/// it to check that the committed `.idx` and entry-table seeds get past
/// their parsers instead of failing at the first check.
pub fn pack_formats_decodes(data: &[u8]) -> bool {
    let Some((&which, bytes)) = data.split_first() else {
        return false;
    };
    match which % 4 {
        0 => {
            let size = (bytes.len() >= 8)
                .then(|| u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")) >> 16);
            let strict = IdxV2::parse(bytes, size).is_ok();
            for parsed in [
                IdxV2::parse(bytes, size),
                IdxV2::parse_allowing_repeats(bytes, size),
            ]
            .into_iter()
            .flatten()
            {
                let ids: Vec<ObjectId> = parsed.ids().collect();
                assert_eq!(ids.len(), parsed.len());
                for (i, id) in ids.iter().enumerate() {
                    let at = parsed.lookup(id).expect("a listed ID is found");
                    assert_eq!(parsed.id(at), *id);
                    let _ = (parsed.offset(i), parsed.crc32(i));
                }
                let by_offset = parsed.by_offset();
                assert_eq!(by_offset.len(), ids.len());
                let _ = parsed.pack_checksum();
            }
            strict
        }
        1 => entry_table(bytes),
        2 => {
            let Ok(h) = header::decode(bytes) else {
                return false;
            };
            assert!(usize::from(h.len) <= bytes.len());
            let mut again = Vec::new();
            header::encode_type_and_size(h.type_code, h.size, &mut again);
            match h.base {
                header::RawBase::None => {}
                header::RawBase::Distance(d) => header::encode_distance(d, &mut again),
                header::RawBase::Id(id) => again.extend_from_slice(id.as_bytes()),
            }
            let back = header::decode(&again).expect("a re-encoded header decodes");
            assert_eq!(
                (back.type_code, back.size, back.base),
                (h.type_code, h.size, h.base)
            );
            assert_eq!(usize::from(back.len), again.len());
            true
        }
        _ => match parse_commit(bytes) {
            Ok((parents, _time)) => {
                assert!(parents.len() <= bytes.len() / 48);
                true
            }
            Err(_) => false,
        },
    }
}

/// The entry-table case of [`pack_formats_decodes`]: whether the table
/// decodes for the row its own header describes.
fn entry_table(bytes: &[u8]) -> bool {
    // Take the row from the table's own header (count at 12, size at 16,
    // checksum at 24), so decoding gets past the row comparison.
    if bytes.len() < 44 {
        return false;
    }
    let count = u32::from_be_bytes(bytes[12..16].try_into().expect("4 bytes"));
    let size = u64::from_be_bytes(bytes[16..24].try_into().expect("8 bytes"));
    let checksum = PackChecksum::from_bytes(bytes[24..44].try_into().expect("20 bytes"));
    let refs = bytes
        .chunks(52)
        .skip(1)
        .filter(|r| r.len() > 8 && r[8] == 7)
        .count() as u32;
    let record = PackRecord {
        id: PackId::new(1).expect("non-zero"),
        pack: NewPack {
            repo: RepoId::new(1).expect("non-zero"),
            checksum,
            size,
            idx_size: 0,
            entries_size: entry_table_len(count),
            object_count: count,
            ref_delta_count: refs,
            origin: PackOrigin::Push { received_bytes: 0 },
        },
        generation: PackGeneration::FIRST,
        created_at: UnixMicros(0),
    };
    let Ok(table) = EntryTable::decode_for(bytes, &record) else {
        return false;
    };
    let encoded = table.encode();
    let again = EntryTable::decode_for(&encoded, &record).expect("re-encoded table decodes");
    assert_eq!(again, table);
    let _ = table.check_layout();
    let _ = aspen_pack::verify::max_delta_depth(&table);
    true
}
