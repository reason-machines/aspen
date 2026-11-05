//! Cover selection: turning the needed versions into as few packs as the
//! compacted covers allow.

use std::collections::{BTreeMap, BTreeSet};

use aspen_core::fetch::{PlanError, VersionGraph};
use aspen_core::id::{PackId, Seq};
use aspen_core::meta::Cover;
use aspen_core::name::RefName;

use crate::walk::Needed;

/// The packs to send, ascending by ID, each once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    /// The packs.
    pub packs: Vec<PackId>,
    /// How many covers replaced their versions' packs.
    pub covers_used: usize,
}

/// Picks the packs for `needed`.
///
/// For each ref with needed versions, a compacted cover is used when every
/// version in its range is needed; its pack then replaces theirs. Covers
/// are tried from the highest level down, longest span first, and a cover
/// that overlaps one already chosen is skipped, so each needed version is
/// sent through at most one cover. Every other needed version sends its own
/// pack (a version without a pack sends nothing).
///
/// A cover with an unneeded version in its range is never used: its pack
/// would carry that version's objects, which the client holds or did not
/// ask for. So the result holds exactly the objects of the needed versions'
/// packs, given that a cover's pack holds exactly its range's objects.
///
/// Fails with [`PlanError::MissingPack`] if the graph lacks a row for a
/// selected pack.
pub fn select(graph: &dyn VersionGraph, needed: &Needed<'_>) -> Result<Selection, PlanError> {
    let mut by_ref: BTreeMap<&RefName, BTreeMap<Seq, Option<PackId>>> = BTreeMap::new();
    for version in needed.versions() {
        by_ref
            .entry(&version.name)
            .or_default()
            .insert(version.seq, version.value.pack());
    }

    let mut packs = BTreeSet::new();
    let mut covers_used = 0;
    for (name, seqs) in by_ref {
        let chosen = choose_covers(graph.covers(name), name, &seqs);
        covers_used += chosen.len();
        for (seq, pack) in &seqs {
            if !in_chosen(&chosen, *seq) {
                packs.extend(*pack);
            }
        }
        packs.extend(chosen.values().map(|&(_, pack)| pack));
    }

    for &id in &packs {
        graph.pack(id).ok_or(PlanError::MissingPack(id))?;
    }
    Ok(Selection {
        packs: packs.into_iter().collect(),
        covers_used,
    })
}

/// Chosen covers of one ref, keyed by first seq: `(last seq, pack)`.
type Chosen = BTreeMap<Seq, (Seq, PackId)>;

fn choose_covers(
    covers: &[Cover],
    name: &RefName,
    needed: &BTreeMap<Seq, Option<PackId>>,
) -> Chosen {
    let mut candidates: Vec<(&Cover, PackId)> = covers
        .iter()
        .filter(|c| &c.name == name)
        .filter_map(|c| Some((c, c.pack?)))
        .filter(|(c, _)| fully_needed(needed, c.range.first(), c.range.last()))
        .collect();
    candidates.sort_by_key(|(c, _)| {
        let span = c.range.last().get() - c.range.first().get();
        (
            std::cmp::Reverse(c.level),
            std::cmp::Reverse(span),
            c.range.first(),
        )
    });
    let mut chosen = Chosen::new();
    for (cover, pack) in candidates {
        let (first, last) = (cover.range.first(), cover.range.last());
        if !overlaps(&chosen, first, last) {
            chosen.insert(first, (last, pack));
        }
    }
    chosen
}

/// Whether every seq in `first..=last` is a needed version.
fn fully_needed(needed: &BTreeMap<Seq, Option<PackId>>, first: Seq, last: Seq) -> bool {
    let span = last.get() - first.get() + 1;
    // `needed` holds each seq at most once, so a full count means no gap.
    needed.range(first..=last).count() as u64 == span
}

fn overlaps(chosen: &Chosen, first: Seq, last: Seq) -> bool {
    // The chosen range starting at or before `last` with the greatest start
    // is the only one that can reach into `first..=last` from the left; any
    // other chosen range starting inside it overlaps too.
    chosen
        .range(..=last)
        .next_back()
        .is_some_and(|(_, &(end, _))| end >= first)
}

fn in_chosen(chosen: &Chosen, seq: Seq) -> bool {
    chosen
        .range(..=seq)
        .next_back()
        .is_some_and(|(_, &(end, _))| end >= seq)
}
