//! `ls-refs` (fetch path step 1).
//!
//! Refs come from the instance's in-memory listing of the repository
//! (`upload_pack::ref_snapshot`), brought up to date by one store read of what
//! changed since the last listing, and are filtered by `ref-prefix` in
//! memory. Where no listing is held (the cache is off, or the repository
//! has more refs than it holds) the store answers `ref-prefix` values with
//! prefix scans, page by page, so a client that asks for
//! `refs/heads/feature` never costs a scan of the whole ref table. `HEAD` is a symbolic ref to the repository's trunk, and
//! unborn (`unborn HEAD symref-target:<trunk>`) while trunk has no live
//! version.
//!
//! `peeled:` attributes are not sent in v1: schema v1 does not record what
//! an annotated tag points at, and reading every listed tag object from its
//! pack would make a tag listing cost one object read per tag. Clients
//! treat a tag without `peeled:` as unpeeled; the visible effect is that
//! `git fetch` does not auto-follow new annotated tags (clone and
//! `fetch --tags` still get them). Decisions log, B2.

use std::collections::BTreeMap;
use std::future::Future;

use aspen_core::blob::BlobStore;
use aspen_core::error::{FetchError, ToWire};
use aspen_core::id::ObjectId;
use aspen_core::meta::{MetaStore, RefListQuery};
use aspen_core::name::RefName;
use aspen_core::refs::RefVersion;
use aspen_core::repo::Repo;
use aspen_proto::pkt;
use aspen_proto::v2::{LsRefsEntry, LsRefsName, LsRefsRequest, write_ls_refs};

use crate::Server;

/// Refs read from the store per page.
const PAGE: u32 = 1000;

/// The whole `ls-refs` response, or an `ERR` line.
pub(crate) async fn respond<M: MetaStore, B>(
    server: &Server<M, B>,
    repo: &Repo,
    req: &LsRefsRequest,
) -> Vec<u8>
where
    B: BlobStore,
{
    let mut out = Vec::new();
    let listed = match server.ref_snapshots() {
        Some(snapshots) => match snapshots.refs(server.meta(), repo.id).await {
            Ok(Some(refs)) => Ok(held_entries(&refs, repo, req)),
            Ok(None) => entries(server.meta(), repo, req).await,
            Err(e) => Err(e),
        },
        None => entries(server.meta(), repo, req).await,
    };
    match listed {
        Ok(entries) => {
            if let Err(e) = write_ls_refs(&mut out, req, &entries) {
                out.clear();
                pkt::write_err(&mut out, &format!("internal error: {e}"));
            }
        }
        Err(e) => pkt::write_err(&mut out, &FetchError::Store(e).to_wire().message),
    }
    out
}

/// The response's entries from a held listing: `HEAD` (a symref to trunk,
/// unborn while trunk is not live) when the request matches it, then every
/// matching ref in name order, as [`entries`] lists them from the store.
fn held_entries(
    refs: &BTreeMap<RefName, ObjectId>,
    repo: &Repo,
    req: &LsRefsRequest,
) -> Vec<LsRefsEntry> {
    let trunk = repo.settings.trunk();
    let mut out = Vec::new();
    if req.matches("HEAD") {
        out.push(LsRefsEntry {
            name: LsRefsName::Head,
            tip: refs.get(trunk).copied(),
            symref_target: Some(trunk.clone()),
            peeled: None,
        });
    }
    out.extend(
        refs.iter()
            .filter(|(name, _)| req.matches(name.as_str()))
            .map(|(name, tip)| LsRefsEntry {
                name: LsRefsName::Ref(name.clone()),
                tip: Some(*tip),
                symref_target: None,
                peeled: None,
            }),
    );
    out
}

async fn entries<M: MetaStore>(
    meta: &M,
    repo: &Repo,
    req: &LsRefsRequest,
) -> Result<Vec<LsRefsEntry>, aspen_core::error::StoreError> {
    let trunk = repo.settings.trunk();
    let mut entries = Vec::new();
    if req.matches("HEAD") {
        let tip = meta
            .current_version(repo.id, trunk)
            .await?
            .and_then(|v| v.value.tip());
        entries.push(LsRefsEntry {
            name: LsRefsName::Head,
            tip,
            symref_target: Some(trunk.clone()),
            peeled: None,
        });
    }
    // `HEAD` alone asks for no ref; any other prefix is a scan (an empty
    // list scans everything, which is what a client without prefixes asks).
    let prefixes: Vec<String> = req
        .prefixes
        .iter()
        .filter(|p| !"HEAD".starts_with(p.as_str()) || "refs/".starts_with(p.as_str()))
        .cloned()
        .collect();
    if !req.prefixes.is_empty() && prefixes.is_empty() {
        return Ok(entries);
    }
    let meta_repo = repo.id;
    let refs = live_refs(PAGE, |after, limit| {
        let query = RefListQuery {
            prefixes: prefixes.clone(),
            after,
            limit,
        };
        async move { meta.list_refs(meta_repo, &query).await }
    })
    .await?;
    entries.extend(refs.into_iter().map(|(name, tip)| LsRefsEntry {
        name: LsRefsName::Ref(name),
        tip: Some(tip),
        symref_target: None,
        peeled: None,
    }));
    Ok(entries)
}

/// Pages through a name-ordered ref listing (`page(after, limit)`) and
/// keeps the live refs. The cursor moves past every row a page returns,
/// live or not, so a page of deleted refs never ends the listing early;
/// only a short page does. `MetaStore::list_refs` leaves deleted refs out
/// today, which this does not rely on.
async fn live_refs<F, Fut, E>(page_size: u32, mut page: F) -> Result<Vec<(RefName, ObjectId)>, E>
where
    F: FnMut(Option<RefName>, u32) -> Fut,
    Fut: Future<Output = Result<Vec<RefVersion>, E>>,
{
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let rows = page(after.take(), page_size).await?;
        let full = rows.len() == page_size as usize;
        for v in rows {
            if let Some(tip) = v.value.tip() {
                out.push((v.name.clone(), tip));
            }
            after = Some(v.name);
        }
        if !full || after.is_none() {
            return Ok(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aspen_core::id::{RefVersionId, RepoId, Seq, UnixMicros};
    use aspen_core::refs::{Closure, Deps, RefValue};

    fn row(i: usize, live: bool) -> RefVersion {
        let n = i64::try_from(i).unwrap() + 1;
        RefVersion {
            id: RefVersionId::new(n).unwrap(),
            repo: RepoId::new(1).unwrap(),
            name: RefName::new(format!("refs/heads/r{i:05}")).unwrap(),
            seq: Seq::new(1).unwrap(),
            parent: None,
            value: if live {
                RefValue::Live {
                    tip: ObjectId::from_bytes([7; 20]),
                    pack: None,
                    closure: Closure::Exact,
                }
            } else {
                RefValue::Deleted
            },
            deps: Deps {
                trunk: None,
                scoped_old: None,
            },
            created_at: UnixMicros(0),
        }
    }

    /// Invariant: a listing that pages through rows without a tip (a store
    /// that returns deleted refs) still lists every live ref after them:
    /// the cursor advances on every row, not only on live ones. Here 10
    /// live refs, then 1,000 consecutive deleted ones (one full page), then
    /// 5 live ones; the old cursor stopped after the first page.
    #[tokio::test]
    async fn a_page_of_deleted_refs_does_not_end_the_listing() {
        let rows: Vec<RefVersion> = (0..1015)
            .map(|i| row(i, !(10..1010).contains(&i)))
            .collect();
        let listed = live_refs(PAGE, |after: Option<RefName>, limit| {
            let page: Vec<RefVersion> = rows
                .iter()
                .filter(|r| after.as_ref().is_none_or(|a| r.name > *a))
                .take(limit as usize)
                .cloned()
                .collect();
            async move { Ok::<_, std::convert::Infallible>(page) }
        })
        .await
        .unwrap();
        assert_eq!(listed.len(), 15);
        assert_eq!(listed[14].0.as_str(), "refs/heads/r01014");
    }
}
