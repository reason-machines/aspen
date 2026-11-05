//! The per-write pack writer and the pack object graph (node B1).

use std::collections::BTreeSet;

use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;

use crate::graph::PackGraph;
use crate::scan::ScanOptions;
use crate::subset::{ExtraObject, ExtraObjects, ExtraSpill, SubsetSource, write_subset};
use crate::testutil::{
    Base, MAX_INFLATED, MAX_OBJECT, Obj, Spec, build_pack, git_index_pack, scratch, table_of,
};

fn big(tag: &str) -> Vec<u8> {
    (0..300)
        .map(|i| format!("{tag} {i}\n"))
        .collect::<String>()
        .into_bytes()
}

fn grown(base: &Obj, tail: &str) -> Obj {
    let mut d = base.data.clone();
    d.extend_from_slice(tail.as_bytes());
    Obj::blob(d)
}

/// Writes `pack` to `dir` with its `.idx`, and returns the paths.
fn on_disk(pack: &[u8], dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    git_index_pack(pack, dir, false).unwrap();
    let p = dir.join(format!("check-{}.pack", crate::sha1(pack)));
    (p.clone(), p.with_extension("idx"))
}

fn ids(objs: &[&Obj]) -> Vec<String> {
    let mut v: Vec<String> = objs.iter().map(|o| o.id().to_hex()).collect();
    v.sort();
    v
}

fn run(
    pack: &[u8],
    keep: &[bool],
    extras: &[ExtraObject],
) -> Result<(Vec<u8>, crate::subset::SubsetPack), crate::LayoutError> {
    run_with(pack, keep, extras)
}

fn run_with<X: ExtraObjects + ?Sized>(
    pack: &[u8],
    keep: &[bool],
    extras: &X,
) -> Result<(Vec<u8>, crate::subset::SubsetPack), crate::LayoutError> {
    let dir = scratch();
    let (p, i) = on_disk(pack, dir.path());
    let table = table_of(pack, dir.path());
    let out = dir.path().join("out");
    let sub = write_subset(
        Some(SubsetSource {
            pack: &p,
            idx: &i,
            table: &table,
            keep,
        }),
        extras,
        &out,
        MAX_OBJECT,
    )?;
    let bytes = std::fs::read(&sub.pack_path).unwrap();
    // The writer's own .idx and entry table describe exactly that pack.
    let idx = std::fs::read(&sub.idx_path).unwrap();
    let verified = crate::verify::verify_pack(&sub.pack_path, dir.path(), MAX_OBJECT, MAX_INFLATED)
        .unwrap_or_else(|e| panic!("subset pack breaks an invariant: {e}"));
    assert_eq!(verified.idx, idx, "the .idx matches gix's (and so git's)");
    assert_eq!(
        std::fs::read(&sub.entries_path).unwrap(),
        verified.table.encode()
    );
    Ok((bytes, sub))
}

/// Invariant: a kept delta whose base is left out is stored whole, never
/// with the base copied in, and the result is self-contained and
/// base-first.
#[test]
fn a_delta_whose_base_is_left_out_is_stored_whole() {
    let a = Obj::blob(big("a"));
    let b = grown(&a, "b\n");
    let c = grown(&b, "c\n");
    let d = Obj::blob(b"d\n".to_vec());
    let pack = build_pack(&[
        Spec::Full(a.clone()),
        Spec::Ofs(0, b.clone()),
        Spec::Ofs(1, c.clone()),
        Spec::Full(d.clone()),
    ]);
    let (bytes, sub) = run(&pack, &[true, false, true, true], &[]).unwrap();
    assert_eq!(sub.undeltified, 1);
    assert_eq!(sub.object_count, 3);
    let dir = scratch();
    assert_eq!(
        git_index_pack(&bytes, dir.path(), true).unwrap(),
        ids(&[&a, &c, &d])
    );
}

#[test]
fn kept_deltas_keep_their_base_and_get_new_distances() {
    let a = Obj::blob(big("a"));
    let x = Obj::blob(big("x"));
    let b = grown(&a, "b\n");
    let pack = build_pack(&[
        Spec::Full(a.clone()),
        Spec::Full(x.clone()),
        Spec::Ofs(0, b.clone()),
    ]);
    let (bytes, sub) = run(&pack, &[true, false, true], &[]).unwrap();
    assert_eq!(sub.undeltified, 0, "b stays a delta against a");
    assert!(bytes.len() < a.data.len() + b.data.len() / 2);
    let dir = scratch();
    assert_eq!(
        git_index_pack(&bytes, dir.path(), true).unwrap(),
        ids(&[&a, &b])
    );
}

#[test]
fn ref_deltas_stay_ref_deltas_when_their_base_is_kept() {
    let a = Obj::blob(big("a"));
    let b = grown(&a, "b\n");
    let c = grown(&a, "c\n");
    let pack = build_pack(&[
        Spec::Full(a.clone()),
        Spec::Ref(Base::Entry(0), b.clone()),
        Spec::Ref(Base::Entry(0), c.clone()),
    ]);
    let (_, sub) = run(&pack, &[true, true, false], &[]).unwrap();
    assert_eq!((sub.undeltified, sub.ref_delta_count), (0, 1));
    let (_, sub) = run(&pack, &[false, true, true], &[]).unwrap();
    assert_eq!((sub.undeltified, sub.ref_delta_count), (2, 0));
}

#[test]
fn extras_are_appended_whole_and_never_repeat_an_object() {
    let a = Obj::blob(big("a"));
    let e = Obj::blob(b"extra\n".to_vec());
    let pack = build_pack(&[Spec::Full(a.clone())]);
    let extra = |o: &Obj| ExtraObject {
        id: o.id(),
        kind: o.kind,
        data: o.data.clone(),
    };
    let (bytes, sub) = run(&pack, &[true], &[extra(&e)]).unwrap();
    assert_eq!(sub.object_count, 2);
    let dir = scratch();
    assert_eq!(
        git_index_pack(&bytes, dir.path(), true).unwrap(),
        ids(&[&a, &e])
    );
    let err = run(&pack, &[true], &[extra(&a)]).unwrap_err();
    assert!(err.to_string().contains("twice"), "{err}");
    // Extras alone, with no source pack.
    let dir = scratch();
    let sub = write_subset(None, &[extra(&e)][..], dir.path(), MAX_OBJECT).unwrap();
    let bytes = std::fs::read(&sub.pack_path).unwrap();
    assert_eq!(
        git_index_pack(&bytes, dir.path(), true).unwrap(),
        ids(&[&e])
    );
    let none: &[ExtraObject] = &[];
    assert!(write_subset(None, none, dir.path(), MAX_OBJECT).is_err());
}

/// Invariant: extras spilled to disk give the same pack, byte for byte, as
/// the same extras in memory.
#[test]
fn spilled_extras_write_the_same_pack_as_in_memory_ones() {
    let a = Obj::blob(big("a"));
    let e1 = Obj::blob(b"extra one\n".to_vec());
    let e2 = Obj::blob(big("extra two"));
    let pack = build_pack(&[Spec::Full(a.clone())]);
    let extras: Vec<ExtraObject> = [&e1, &e2]
        .iter()
        .map(|o| ExtraObject {
            id: o.id(),
            kind: o.kind,
            data: o.data.clone(),
        })
        .collect();
    let (in_memory, _) = run(&pack, &[true], &extras).unwrap();
    let dir = scratch();
    let mut spill = ExtraSpill::create(dir.path().join("extras")).unwrap();
    for x in &extras {
        spill.push(x.id, x.kind, &x.data).unwrap();
    }
    spill.finish().unwrap();
    assert_eq!(
        (spill.len(), spill.bytes()),
        (2, (e1.data.len() + e2.data.len()) as u64)
    );
    let (spilled, sub) = run_with(&pack, &[true], &spill).unwrap();
    assert_eq!(sub.object_count, 3);
    assert_eq!(spilled, in_memory);
}

fn tree(entries: &[(&str, &str, ObjectId)]) -> Obj {
    let mut data = Vec::new();
    for (mode, name, id) in entries {
        data.extend_from_slice(format!("{mode} {name}\0").as_bytes());
        data.extend_from_slice(id.as_bytes());
    }
    Obj {
        kind: ObjectKind::Tree,
        data,
    }
}

fn commit(tree: ObjectId, parents: &[ObjectId]) -> Obj {
    let mut s = format!("tree {tree}\n");
    for p in parents {
        s.push_str(&format!("parent {p}\n"));
    }
    s.push_str("author A <a@x> 1 +0000\ncommitter A <a@x> 1700000000 +0000\n\nm\n");
    Obj {
        kind: ObjectKind::Commit,
        data: s.into_bytes(),
    }
}

/// Invariant: `reach` enters exactly the pack objects the tip reaches
/// without passing a held object, and reports every object it stopped at.
#[test]
fn the_graph_walk_stops_at_held_and_outside_objects() {
    let outside = ObjectId::from_bytes([7; 20]);
    let b1 = Obj::blob(b"one\n".to_vec());
    let b2 = Obj::blob(b"two\n".to_vec());
    let stray = Obj::blob(b"stray\n".to_vec());
    let sub = tree(&[("100644", "b2", b2.id())]);
    let t = tree(&[
        ("100644", "b1", b1.id()),
        ("40000", "sub", sub.id()),
        ("100644", "x", outside),
    ]);
    let parent = ObjectId::from_bytes([9; 20]);
    let c = commit(t.id(), &[parent]);
    let pack = build_pack(&[
        Spec::Full(b1.clone()),
        Spec::Full(b2.clone()),
        Spec::Full(stray.clone()),
        Spec::Full(sub.clone()),
        Spec::Full(t.clone()),
        Spec::Full(c.clone()),
    ]);
    let dir = scratch();
    let (p, i) = on_disk(&pack, dir.path());
    let g = PackGraph::scan(
        &p,
        &i,
        ScanOptions {
            commits: true,
            missing: true,
            thread_limit: Some(1),
            max_object_bytes: MAX_OBJECT,
            max_inflated_bytes: MAX_INFLATED,
            verify_checksums: true,
        },
    )
    .unwrap();
    assert_eq!(g.len(), 6);
    assert_eq!(
        g.missing()
            .as_slice()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([outside, parent])
    );
    assert_eq!(g.commit(&c.id()).unwrap().parents, vec![parent]);
    let none = vec![false; g.len()];
    let r = g.reach(&c.id(), &none);
    let reached: BTreeSet<ObjectId> = (0..g.len())
        .filter(|&i| r.members[i])
        .map(|i| g.id(i))
        .collect();
    assert_eq!(
        reached,
        BTreeSet::from([c.id(), t.id(), sub.id(), b1.id(), b2.id()]),
        "everything but the stray blob"
    );
    assert_eq!(r.count, 5);
    assert_eq!(
        r.boundary
            .as_slice()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([outside, parent])
    );
    // A held subtree is not entered; it becomes part of the boundary.
    let mut held = none.clone();
    held[g.position(&sub.id()).unwrap()] = true;
    let r = g.reach(&c.id(), &held);
    assert!(!r.members[g.position(&b2.id()).unwrap()]);
    assert!(r.boundary.contains(&sub.id()));
    // A tip outside the pack reaches nothing in it.
    let r = g.reach(&outside, &none);
    assert_eq!(r.count, 0);
    assert_eq!(r.boundary.as_slice(), &[outside]);
}
