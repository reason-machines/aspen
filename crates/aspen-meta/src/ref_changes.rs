//! Which refs changed since a reader last read them:
//! [`MetaStore::ref_changes`].
//!
//! Every `ref_versions` row records the transaction that wrote it
//! (`txid`, migration 0010). A read returns, in the same statement, a
//! [`RefCursor`] that covers every transaction the read could not see, so
//! the next read asks for exactly the versions those transactions wrote:
//!
//! - **Postgres:** `txid` is the writing top-level transaction's 64-bit ID
//!   (`pg_current_xact_id()`). The cursor is the statement's own snapshot
//!   (`pg_current_snapshot()`): a version the snapshot does not see was
//!   written by a transaction numbered at or above its `xmax`, or by one
//!   its in-progress list names. Transactions commit in any order, and a
//!   later-numbered push can commit first; the in-progress list is what
//!   keeps the earlier one from being missed, and it is short (at most one
//!   entry per open connection to the database). A list longer than
//!   [`MAX_ALSO`] falls back to the snapshot's `xmin`, which every entry is
//!   at or above, so the answer stays exact and only reads more.
//! - **SQLite:** write transactions run one at a time (`BEGIN IMMEDIATE`),
//!   so a version's ID orders it against every reader, and a trigger copies
//!   it into `txid`. The cursor is one past the largest version ID the
//!   read saw.
//!
//! The changed versions are joined to `refs`, so only versions that are
//! still current come back: one row per changed ref, however many times it
//! moved.
//!
//! [`MetaStore::ref_changes`]: aspen_core::meta::MetaStore::ref_changes

use aspen_core::error::StoreError;
use aspen_core::id::RepoId;
use aspen_core::meta::{RefChange, RefChanges, RefCursor};
use sqlx::AssertSqlSafe;

use crate::error::store_error;
use crate::rows;
use crate::sql::placeholders;
use crate::store::{Dialect, SqlStore};

/// Most in-progress transactions a cursor lists; a snapshot with more
/// gives a cursor from its `xmin` instead.
pub(crate) const MAX_ALSO: usize = 256;

pub(crate) async fn ref_changes(
    store: &SqlStore,
    repo: RepoId,
    since: Option<&RefCursor>,
    limit: u32,
) -> Result<RefChanges, StoreError> {
    let snapshot = match store.dialect() {
        Dialect::Postgres => "pg_current_snapshot()::text",
        Dialect::Sqlite => "CAST((SELECT coalesce(max(id), 0) + 1 FROM ref_versions) AS TEXT)",
    };
    // `$1` is the repository and the last placeholder the row limit; a
    // cursor binds its `from` and `also` in between.
    let (changed, binds): (String, Vec<i64>) = match since {
        None => (
            "SELECT r.ref_name, v.tip_oid FROM refs r JOIN ref_versions v ON v.id = r.version_id \
             WHERE r.repo_id = $1 AND v.tip_oid IS NOT NULL"
                .to_owned(),
            Vec::new(),
        ),
        Some(c) => {
            let mut cond = "v.txid >= $2".to_owned();
            if !c.also.is_empty() {
                cond.push_str(&format!(
                    " OR v.txid IN ({})",
                    placeholders(3, c.also.len())
                ));
            }
            let mut binds = vec![c.from];
            binds.extend(&c.also);
            (
                format!(
                    "SELECT r.ref_name, v.tip_oid FROM ref_versions v \
                     JOIN refs r ON r.version_id = v.id AND r.repo_id = v.repo_id \
                     WHERE v.repo_id = $1 AND ({cond})"
                ),
                binds,
            )
        }
    };
    // The cursor comes from the same statement as the rows, so both
    // describe one moment; the outer join keeps the cursor's row when no
    // ref changed.
    let sql = format!(
        "WITH s AS (SELECT {snapshot} AS snap) \
         SELECT s.snap, c.ref_name, c.tip_oid FROM s LEFT JOIN ({changed} LIMIT ${}) c ON 1 = 1",
        binds.len() + 2
    );
    let mut q = sqlx::query(AssertSqlSafe(sql)).bind(repo.get());
    for b in binds {
        q = q.bind(b);
    }
    let rows = q
        .bind(i64::from(limit) + 1)
        .fetch_all(&mut *store.admit().await?)
        .await
        .map_err(store_error)?;
    let first = rows
        .first()
        .ok_or_else(|| rows::bad("ref_changes", "snap", "no row"))?;
    let cursor = parse_cursor(&rows::get::<String>(first, "snap")?)?;
    let mut refs = Vec::with_capacity(rows.len());
    for row in &rows {
        if rows::get::<Option<String>>(row, "ref_name")?.is_none() {
            continue;
        }
        let tip: Option<Vec<u8>> = rows::get(row, "tip_oid")?;
        refs.push(RefChange {
            name: rows::ref_name(row, "refs", "ref_name")?,
            tip: match tip {
                Some(_) => Some(rows::object_id(row, "ref_versions", "tip_oid")?),
                None => None,
            },
        });
    }
    let complete = refs.len() <= limit as usize;
    refs.truncate(limit as usize);
    Ok(RefChanges {
        cursor,
        refs,
        complete,
    })
}

/// A cursor from a Postgres snapshot (`xmin:xmax:xip,...`) or SQLite's
/// next version ID.
fn parse_cursor(snap: &str) -> Result<RefCursor, StoreError> {
    let bad = |detail: &str| rows::bad("ref_changes", "snap", format!("{detail}: {snap:?}"));
    let num = |s: &str| s.parse::<i64>().map_err(|_| bad("not a number"));
    let mut parts = snap.split(':');
    let (Some(first), second, third, None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("not a snapshot"));
    };
    match (second, third) {
        (None, None) => Ok(RefCursor {
            from: num(first)?,
            also: Vec::new(),
        }),
        (Some(xmax), Some(xip)) => {
            let xmin = num(first)?;
            let xmax = num(xmax)?;
            let also = if xip.is_empty() {
                Vec::new()
            } else {
                xip.split(',').map(num).collect::<Result<Vec<_>, _>>()?
            };
            Ok(if also.len() > MAX_ALSO {
                RefCursor {
                    from: xmin,
                    also: Vec::new(),
                }
            } else {
                RefCursor { from: xmax, also }
            })
        }
        _ => Err(bad("not a snapshot")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_from_snapshots() {
        assert_eq!(
            parse_cursor("17").unwrap(),
            RefCursor {
                from: 17,
                also: vec![]
            }
        );
        assert_eq!(
            parse_cursor("100:104:").unwrap(),
            RefCursor {
                from: 104,
                also: vec![]
            }
        );
        assert_eq!(
            parse_cursor("100:104:100,102").unwrap(),
            RefCursor {
                from: 104,
                also: vec![100, 102]
            }
        );
        // Too many in progress: from xmin, which every one is at or above.
        let many: Vec<String> = (0..=MAX_ALSO).map(|i| (1000 + i).to_string()).collect();
        assert_eq!(
            parse_cursor(&format!("1000:5000:{}", many.join(","))).unwrap(),
            RefCursor {
                from: 1000,
                also: vec![]
            }
        );
        for bad in ["", "x", "1:2", "1:2:3:4", "1:x:", "1:2:3,y"] {
            assert!(parse_cursor(bad).is_err(), "{bad:?}");
        }
    }
}
