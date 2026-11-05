//! Thin-pack completion against `git index-pack --fix-thin`, over packs
//! whose running offset shift is positive, negative, zero, or changes sign
//! (issue #164).
//!
//! Completion ([`ThinCompletion`](crate::thin)) inserts each base the
//! lookup answers just before the first REF_DELTA on it, turns the
//! REF_DELTAs on that base into OFS_DELTAs, and recomputes every later
//! OFS_DELTA distance from an exact map of old offsets to new ones. Each
//! inserted base adds its bytes and each REF_DELTA turned OFS_DELTA saves
//! 17 to 19 header bytes, so the received entries move by a running shift
//! that can be positive, negative or zero, and can change sign. The
//! fixtures of issue #151 (`thin.rs`) all have a shift that returns to
//! zero. This test generates thin packs from a fixed seed, ingests each,
//! runs stock `git index-pack --fix-thin` on the same bytes with the same
//! bases, and requires both to accept and to store the same object IDs.
//!
//! Each case is classified from the pack ingest stored, not from what the
//! generator aimed for: every received entry's running shift (its stored
//! offset minus its received offset), the bytes of the inserted bases, and
//! each entry's change in header length. The test fails unless the run
//! produced every [`Shape`], so a generator change cannot silently stop
//! covering one. Long random runs are D3's nightly `pack_ingest` target.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use aspen_core::id::ObjectId;
use aspen_core::ingest::InMemoryBases;
use aspen_core::pack::{EntryKind, EntryTable, PackEntry};

use crate::header::{self, distance_len};
use crate::testutil::{
    Base, MAX_INFLATED, MAX_OBJECT, Obj, Spec, build_pack, build_pack_with_offsets, git, git_raw,
    scratch,
};
use crate::verify::verify_pack;

use super::ingest::ingest;

/// The base seed; case `n` uses `SEED + n`, so one case replays alone.
const SEED: u64 = 0x0164_7417_c0de_0001;
/// Enough for every [`Shape`] at this seed, with margin (the rarest,
/// [`Shape::DistanceLen2And3`], comes up in 6 cases). The whole test takes
/// under a second of CPU in a debug build, git included.
const CASES: u64 = 48;

/// What a completed pack did to the received entries' offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Shape {
    /// The stored pack is longer than the received one: the inserted
    /// bases added more bytes than the header changes saved.
    PositiveNet,
    /// The stored pack is shorter: the REF_DELTAs turned OFS_DELTA saved
    /// more header bytes than the inserted bases added.
    NegativeNet,
    /// A received OFS_DELTA whose running shift is zero while its base's
    /// is not: the bytes inserted and saved before it cancel, yet its
    /// distance changes. This is the shape of issue #151, where gix-pack
    /// 0.77 kept the old distance.
    NetZero,
    /// The running shift is positive at one received entry and negative
    /// at another.
    SignChange,
    /// A received OFS_DELTA's distance crosses 127/128, so its varint
    /// changes between 1 and 2 bytes.
    DistanceLen1And2,
    /// A received OFS_DELTA's distance crosses 16383/16384, so its varint
    /// changes between 2 and 3 bytes.
    DistanceLen2And3,
}

const EVERY_SHAPE: [Shape; 6] = [
    Shape::PositiveNet,
    Shape::NegativeNet,
    Shape::NetZero,
    Shape::SignChange,
    Shape::DistanceLen1And2,
    Shape::DistanceLen2And3,
];

#[test]
fn thin_completion_matches_git_fix_thin_for_every_shift_shape() {
    let dir = scratch();
    let cases: Vec<Case> = (0..CASES).map(Case::generate).collect();
    let repo = git_repo_holding(cases.iter().flat_map(|c| &c.thin_bases), dir.path());
    let mut seen: BTreeMap<Shape, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    for case in &cases {
        let n = case.number;
        let (pack, offsets) = build_pack_with_offsets(&case.specs);
        let expected = match git_fix_thin_in(&repo, &pack) {
            Ok(ids) => ids,
            Err(e) => {
                failures.push(format!("case {n}: stock git refuses it: {}", e.trim()));
                continue;
            }
        };
        let case_dir = dir.path().join(format!("case-{n}"));
        std::fs::create_dir(&case_dir).unwrap();
        let mut bases = InMemoryBases::default();
        for x in &case.thin_bases {
            bases.insert(x.id(), x.kind, x.data.clone());
        }
        let stored = match ingest(&pack, &bases, &case_dir) {
            Ok(stored) => stored,
            Err(e) => {
                failures.push(format!(
                    "case {n}: ingest refuses a pack git completes: {e}"
                ));
                continue;
            }
        };
        let table = verify_pack(&stored.pack_path, &case_dir, MAX_OBJECT, MAX_INFLATED)
            .unwrap_or_else(|e| panic!("case {n}: stored pack: {e}"))
            .table;
        let ids: BTreeSet<ObjectId> = table.entries().iter().map(|e| e.id).collect();
        if ids != expected {
            failures.push(format!(
                "case {n}: ingest stored {} objects, git index-pack --fix-thin {}; only in ingest's: {:?}; only in git's: {:?}",
                ids.len(),
                expected.len(),
                ids.difference(&expected).collect::<Vec<_>>(),
                expected.difference(&ids).collect::<Vec<_>>(),
            ));
            continue;
        }
        match classify(case, &pack, &offsets, &table, stored.size) {
            Ok(shapes) => {
                for shape in shapes {
                    *seen.entry(shape).or_default() += 1;
                }
            }
            Err(e) => failures.push(format!("case {n}: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "thin-pack completion disagrees with git index-pack --fix-thin (seed {SEED:#x}):\n{}",
        failures.join("\n")
    );
    let missing: Vec<Shape> = EVERY_SHAPE
        .into_iter()
        .filter(|s| !seen.contains_key(s))
        .collect();
    assert!(
        missing.is_empty(),
        "the generator never produced {missing:?} in {CASES} cases (seed {SEED:#x}), \
         so completion went untested for it; cases per shape: {seen:?}"
    );
}

/// The shapes of one case, read from the pack ingest stored. Errs if a
/// received OFS_DELTA no longer points at its own base.
fn classify(
    case: &Case,
    received: &[u8],
    received_at: &[u64],
    stored: &EntryTable,
    stored_size: u64,
) -> Result<BTreeSet<Shape>, String> {
    let by_id: HashMap<ObjectId, &PackEntry> = stored.entries().iter().map(|e| (e.id, e)).collect();
    // Ingest keeps the received entries in order (no forward bases here),
    // so each one is found by its ID.
    let stored_as: Vec<&PackEntry> = case.objs.iter().map(|o| by_id[&o.id()]).collect();
    let shift: Vec<i64> = stored_as
        .iter()
        .zip(received_at)
        .map(|(e, &old)| e.offset as i64 - old as i64)
        .collect();

    let received_ids: HashSet<ObjectId> = case.objs.iter().map(Obj::id).collect();
    let entries = stored.entries();
    let inserted: i64 = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| !received_ids.contains(&e.id))
        .map(|(i, e)| {
            let end = entries
                .get(i + 1)
                .map_or(stored_size - 20, |next| next.offset);
            (end - e.offset) as i64
        })
        .sum();
    let header_change: i64 = stored_as
        .iter()
        .zip(received_at)
        .map(|(e, &old)| {
            let before = header::decode(&received[old as usize..]).unwrap().len;
            i64::from(e.header_len) - i64::from(before)
        })
        .sum();
    let net = stored_size as i64 - received.len() as i64;
    if net != inserted + header_change {
        return Err(format!(
            "the stored pack grew by {net} bytes, not by its inserted bases ({inserted}) plus its header changes ({header_change})"
        ));
    }

    let mut shapes = BTreeSet::new();
    if net > 0 {
        shapes.insert(Shape::PositiveNet);
    }
    if net < 0 {
        shapes.insert(Shape::NegativeNet);
    }
    if shift.iter().any(|&s| s > 0) && shift.iter().any(|&s| s < 0) {
        shapes.insert(Shape::SignChange);
    }
    for (i, spec) in case.specs.iter().enumerate() {
        let Spec::Ofs(b, _) = spec else { continue };
        let EntryKind::OfsDelta { base_offset } = stored_as[i].kind else {
            return Err(format!(
                "entry {i} was an OFS_DELTA and is stored as {:?}",
                stored_as[i].kind
            ));
        };
        if base_offset != stored_as[*b].offset {
            return Err(format!(
                "entry {i}'s OFS_DELTA points at offset {base_offset}, not at its base entry {b} (offset {})",
                stored_as[*b].offset
            ));
        }
        if shift[i] == 0 && shift[*b] != 0 {
            shapes.insert(Shape::NetZero);
        }
        let old = distance_len(received_at[i] - received_at[*b]);
        let new = distance_len(stored_as[i].offset - base_offset);
        match (old.min(new), old.max(new)) {
            (1, 2) => shapes.insert(Shape::DistanceLen1And2),
            (2, 3) => shapes.insert(Shape::DistanceLen2And3),
            _ => false,
        };
    }
    Ok(shapes)
}

/// SplitMix64 with the same constants as testkit's and the fuzz crate's
/// generators: its output is pinned, so a seed replays the same packs after
/// dependency upgrades.
struct Mix(u64);

impl Mix {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// One generated thin pack: its entries and the thin bases the lookup
/// (and git's repository) holds. Every object carries a line naming its
/// case and number, so no object repeats within or across cases.
struct Case {
    number: u64,
    rng: Mix,
    count: usize,
    specs: Vec<Spec>,
    /// The object of each entry of `specs`.
    objs: Vec<Obj>,
    thin_bases: Vec<Obj>,
}

impl Case {
    /// Leading entries, an optional segment whose shift returns to zero
    /// (first, so the running shift is zero before it: the issue #151
    /// shape), then one to three random segments, with filler between.
    fn generate(number: u64) -> Self {
        let mut c = Case {
            number,
            rng: Mix(SEED.wrapping_add(number)),
            count: 0,
            specs: Vec::new(),
            objs: Vec::new(),
            thin_bases: Vec::new(),
        };
        c.filler();
        if c.rng.below(3) == 0 {
            c.cancelling();
            c.filler();
        }
        for _ in 0..1 + c.rng.below(3) {
            match c.rng.below(6) {
                0 => {
                    c.growing();
                }
                1 => {
                    let refs = 2 + c.rng.below(8);
                    c.shrinking(refs);
                }
                2 => c.cancelling(),
                3 => c.crossing(16384),
                _ => c.crossing(128),
            }
            c.filler();
        }
        c
    }

    fn tag(&mut self) -> Vec<u8> {
        self.count += 1;
        format!("case {} object {}\n", self.number, self.count).into_bytes()
    }

    /// Random bytes, which deflate does not shrink, so sizes are easy to
    /// steer.
    fn noise(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.rng.next_u64() as u8).collect()
    }

    fn blob(&mut self, noise: usize) -> Obj {
        let mut data = self.tag();
        data.extend(self.noise(noise));
        Obj::blob(data)
    }

    /// `base` plus a new line and a few bytes: a delta that copies `base`
    /// whole and inserts the rest (always over Git's 4-byte minimum).
    fn derived(&mut self, base: &Obj) -> Obj {
        let mut data = base.data.clone();
        data.extend(self.tag());
        let extra = self.rng.below(8);
        data.extend(self.noise(extra));
        Obj::blob(data)
    }

    fn push(&mut self, spec: Spec, obj: Obj) -> usize {
        self.specs.push(spec);
        self.objs.push(obj);
        self.specs.len() - 1
    }

    fn full(&mut self, noise: usize) -> usize {
        let obj = self.blob(noise);
        self.push(Spec::Full(obj.clone()), obj)
    }

    fn ofs(&mut self, base: usize) -> usize {
        let obj = self.derived(&self.objs[base].clone());
        self.push(Spec::Ofs(base, obj.clone()), obj)
    }

    /// A REF_DELTA on an earlier entry: the lookup has no answer, so it
    /// stays a REF_DELTA.
    fn ref_in_pack(&mut self, base: usize) -> usize {
        let obj = self.derived(&self.objs[base].clone());
        self.push(Spec::Ref(Base::Entry(base), obj.clone()), obj)
    }

    fn thin_base(&mut self, obj: Obj) -> Obj {
        self.thin_bases.push(obj.clone());
        obj
    }

    /// A REF_DELTA on a thin base: completion inserts the base before the
    /// first one and makes them all OFS_DELTAs.
    fn ref_thin(&mut self, base: &Obj) -> usize {
        let obj = self.derived(base);
        self.push(Spec::Ref(Base::External(base.clone()), obj.clone()), obj)
    }

    /// Up to three full blobs, OFS_DELTAs on any earlier entry (whose
    /// distances change when they span a shift) and in-pack REF_DELTAs.
    fn filler(&mut self) {
        for _ in 0..self.rng.below(4) {
            let n = self.specs.len();
            match self.rng.below(3) {
                0 if n > 0 => {
                    let base = self.rng.below(n);
                    self.ofs(base);
                }
                1 if n > 0 => {
                    let base = self.rng.below(n);
                    self.ref_in_pack(base);
                }
                _ => {
                    let noise = self.rng.below(200);
                    self.full(noise);
                }
            }
        }
    }

    /// A large thin base under one REF_DELTA: the shift grows. Returns the
    /// expected growth (an estimate for steering only).
    fn growing(&mut self) -> i64 {
        let noise = 40 + self.rng.below(360);
        let blob = self.blob(noise);
        let x = self.thin_base(blob);
        self.ref_thin(&x);
        let len = inserted_len(&x.data);
        len as i64 - 20 + distance_len(len) as i64
    }

    /// A tiny thin base under `refs` REF_DELTAs: the shift shrinks. Returns
    /// the expected change, assuming every new distance fits one byte (an
    /// estimate for steering only).
    fn shrinking(&mut self, refs: usize) -> i64 {
        let noise = self.rng.below(4);
        let blob = self.blob(noise);
        let x = self.thin_base(blob);
        for _ in 0..refs {
            self.ref_thin(&x);
        }
        inserted_len(&x.data) as i64 - 19 * refs as i64
    }

    /// A thin base of 18 × k bytes under k REF_DELTAs, each of whose new
    /// distances takes 2 bytes (so each saves 18), with a full blob after
    /// the first and an OFS_DELTA on that blob after the last: the blob
    /// moves, while the OFS_DELTA is back at the shift the segment started
    /// with, so its distance changes. Run first, that shift is zero.
    fn cancelling(&mut self) {
        let k = 8 + self.rng.below(7);
        let tag = self.tag();
        let noise = self.noise(512);
        let target = 18 * k as u64;
        let data = (target.saturating_sub(48) as usize..=noise.len())
            .map(|n| [&tag[..], &noise[..n]].concat())
            .find(|d| inserted_len(d) == target)
            .expect("some prefix of the noise inserts in exactly 18 × k bytes");
        let x = self.thin_base(Obj::blob(data));
        self.ref_thin(&x);
        let noise = self.rng.below(40);
        let moved = self.full(noise);
        for _ in 1..k {
            self.ref_thin(&x);
        }
        self.ofs(moved);
    }

    /// A full blob, a growing or shrinking segment, padding, and an
    /// OFS_DELTA on the blob whose received distance is chosen so the
    /// segment's shift carries it across `boundary`, changing the length
    /// of its distance varint.
    fn crossing(&mut self, boundary: u64) {
        let noise = self.rng.below(30);
        let base = self.full(noise);
        let shift = if self.rng.below(2) == 0 {
            self.growing()
        } else {
            self.shrinking(3)
        };
        let tag = self.tag();
        let noise = self.noise(boundary as usize + 64);
        let distance = |c: &Case, n: usize| {
            let mut between = c.specs[base..].to_vec();
            between.push(Spec::Full(Obj::blob([&tag[..], &noise[..n]].concat())));
            // Only full blobs and thin REF_DELTAs: their entries' lengths
            // do not depend on where they are.
            build_pack(&between).len() as i64 - 32
        };
        let shortest = distance(self, 0);
        let (lo, hi) = if shift > 0 {
            (boundary as i64 - shift, boundary as i64 - 1)
        } else {
            (boundary as i64, boundary as i64 - 1 - shift)
        };
        let target = (lo.max(shortest) + hi) / 2;
        let mut n = 0usize;
        for _ in 0..8 {
            let d = distance(self, n);
            if d == target {
                break;
            }
            n = (n as i64 + target - d).clamp(0, noise.len() as i64) as usize;
        }
        let pad = Obj::blob([&tag[..], &noise[..n]].concat());
        self.push(Spec::Full(pad.clone()), pad);
        self.ofs(base);
    }
}

/// Bytes a thin base takes once completion inserts it: gix's entry for it
/// at the compression completion uses. Steers the generator only; shapes
/// are read from the stored pack.
fn inserted_len(data: &[u8]) -> u64 {
    let obj = gix_object::Data::new(data, gix_object::Kind::Blob, gix_hash::Kind::Sha1);
    let compression = gix_pack::bundle::write::Options::default().compression;
    gix_pack::data::input::Entry::from_data_obj(&obj, 0, compression)
        .unwrap()
        .bytes_in_pack()
}

/// A bare repository holding `objects`, written as one pack.
fn git_repo_holding<'a>(objects: impl IntoIterator<Item = &'a Obj>, dir: &Path) -> PathBuf {
    let repo = dir.join("git.git");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q", "--bare"]);
    let specs: Vec<Spec> = objects.into_iter().cloned().map(Spec::Full).collect();
    let out = git_raw(&repo, &["index-pack", "--stdin"], Some(&build_pack(&specs)));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    repo
}

/// The objects stock `git index-pack --fix-thin` stores for `pack` in
/// `repo`, or its stderr.
fn git_fix_thin_in(repo: &Path, pack: &[u8]) -> Result<BTreeSet<ObjectId>, String> {
    let out = git_raw(repo, &["index-pack", "--stdin", "--fix-thin"], Some(pack));
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).into_owned());
    }
    // `pack\t<checksum>`: list that pack's objects.
    let checksum = String::from_utf8(out.stdout).unwrap();
    let checksum = checksum.trim().trim_start_matches("pack").trim();
    let idx = std::fs::read(repo.join(format!("objects/pack/pack-{checksum}.idx"))).unwrap();
    let listed = git_raw(repo, &["show-index"], Some(&idx));
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    Ok(String::from_utf8(listed.stdout)
        .unwrap()
        .lines()
        .map(|l| ObjectId::parse_hex(l.split_whitespace().nth(1).unwrap()).unwrap())
        .collect())
}
