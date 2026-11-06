//! Time-limited leases (migration 0009): [`MetaStore::acquire_lease`], and
//! the fence that work done under a lease runs in its own transaction.
//!
//! A lease is one `leases` row: its owner, an epoch and an expiry by the
//! store's clock. Acquiring writes the row only when it is free, expired or
//! already the caller's; a takeover (a new owner, or the same owner after
//! the lease expired) adds one to the epoch. Work under the lease starts
//! its transaction with [`fence`], one conditional `UPDATE` on owner,
//! epoch and expiry that also extends the lease. So:
//!
//! - a holder that dies stops extending its lease, which expires `ttl`
//!   after its last extension, and the next process to ask takes it;
//! - a holder that stalls past its expiry and then resumes is refused at
//!   its next fence once anyone took the lease over (the epoch differs),
//!   and until then nobody else holds it;
//! - on Postgres the fence's `UPDATE` locks the row until the work's
//!   transaction ends, so a takeover waits for that transaction and then
//!   sees the extended lease; on SQLite every write transaction runs alone.
//!
//! Two processes therefore never run fenced work at the same time.
//!
//! [`MetaStore::acquire_lease`]: aspen_core::meta::MetaStore::acquire_lease

use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::meta::Lease;

use crate::error::{corrupt, store_error};
use crate::gc::micros;
use crate::rows;
use crate::store::{Dialect, SqlStore};
use crate::tx::WriteTx;

pub(crate) async fn acquire_lease(
    store: &SqlStore,
    name: &str,
    owner: &str,
    ttl: Duration,
) -> Result<Option<Lease>, StoreError> {
    let lock = match store.dialect() {
        Dialect::Postgres => " FOR UPDATE",
        Dialect::Sqlite => "",
    };
    store
        .write(async |tx| {
            let now = store.now().0;
            let expires = now.saturating_add(micros(ttl));
            let sql =
                format!("SELECT owner, epoch, expires_at_us FROM leases WHERE name = $1{lock}");
            let row = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(name)
                .fetch_optional(&mut **tx)
                .await
                .map_err(store_error)?;
            let epoch = match row {
                None => {
                    // A concurrent first acquisition inserted the row after
                    // our read: it holds the lease.
                    let inserted = sqlx::query(
                        "INSERT INTO leases (name, owner, epoch, expires_at_us) \
                         VALUES ($1, $2, 1, $3) ON CONFLICT (name) DO NOTHING",
                    )
                    .bind(name)
                    .bind(owner)
                    .bind(expires)
                    .execute(&mut **tx)
                    .await
                    .map_err(store_error)?
                    .rows_affected();
                    if inserted == 0 {
                        return Ok(None);
                    }
                    1
                }
                Some(row) => {
                    let holder: String = rows::get(&row, "owner")?;
                    let epoch: i64 = rows::get(&row, "epoch")?;
                    let until: i64 = rows::get(&row, "expires_at_us")?;
                    let live = until > now;
                    let next = match (live, holder == owner) {
                        (true, true) => epoch,
                        (true, false) => return Ok(None),
                        (false, _) => epoch + 1,
                    };
                    let updated = sqlx::query(
                        "UPDATE leases SET owner = $1, epoch = $2, expires_at_us = $3 \
                         WHERE name = $4 AND epoch = $5",
                    )
                    .bind(owner)
                    .bind(next)
                    .bind(expires)
                    .bind(name)
                    .bind(epoch)
                    .execute(&mut **tx)
                    .await
                    .map_err(store_error)?
                    .rows_affected();
                    if updated == 0 {
                        return Ok(None);
                    }
                    next
                }
            };
            Ok(Some(Lease {
                name: name.to_owned(),
                owner: owner.to_owned(),
                epoch: u64::try_from(epoch)
                    .map_err(|_| corrupt(format!("leases.epoch of {name}: {epoch}")))?,
                ttl,
            }))
        })
        .await
}

/// Checks, inside a write transaction, that `lease` is still held (same
/// owner and epoch, not expired at `now`) and extends it by its `ttl`;
/// [`StoreError::lease_lost`] otherwise, which rolls the transaction back.
pub(crate) async fn fence(tx: &mut WriteTx, lease: &Lease, now: i64) -> Result<(), StoreError> {
    let epoch = i64::try_from(lease.epoch).map_err(|_| {
        StoreError::lease_lost(format!(
            "{}: epoch {} out of range",
            lease.name, lease.epoch
        ))
    })?;
    let extended = sqlx::query(
        "UPDATE leases SET expires_at_us = $1 \
         WHERE name = $2 AND owner = $3 AND epoch = $4 AND expires_at_us > $5",
    )
    .bind(now.saturating_add(micros(lease.ttl)))
    .bind(&lease.name)
    .bind(&lease.owner)
    .bind(epoch)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(store_error)?
    .rows_affected();
    if extended == 1 {
        Ok(())
    } else {
        Err(StoreError::lease_lost(format!(
            "{} (epoch {}) expired or was taken over",
            lease.name, lease.epoch
        )))
    }
}
