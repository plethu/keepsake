use chrono::{DateTime, Utc};
use keepsake::ExpiryCause;
#[cfg(feature = "fulfillment-counters")]
use keepsake::{ExpiryPolicy, FulfillmentSnapshot};
#[cfg(feature = "fulfillment-counters")]
use sqlx::{MySql, Transaction};
#[cfg(feature = "fulfillment-counters")]
use uuid::Uuid;

#[cfg(feature = "fulfillment-counters")]
use crate::repository::FulfilledExpiryCandidate;
use crate::repository::support::expiry_event;
use crate::repository::{
    MySqlKeepsakeRepository, RelationCache, RepositoryResult, TimedExpiryCandidate, validate_limit,
};

use super::audit::record_audit_event_tx;
#[cfg(feature = "fulfillment-counters")]
use super::fulfillment::fulfillment_snapshot_tx;
#[cfg(feature = "fulfillment-counters")]
use super::rows::fulfilled_expiry_candidate_from_row;
use super::rows::{naive_timestamp, timed_expiry_candidate_from_row};

impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Lists due timed expiry candidates in stable batch order.
    pub async fn due_timed_expiry(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= ?
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            ",
        )
        .bind(naive_timestamp(now))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(timed_expiry_candidate_from_row).collect()
    }

    /// Reads the persisted fulfillment snapshot (counters and checklist) for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    /// Reads the persisted fulfillment snapshot for a keepsake.
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> RepositoryResult<FulfillmentSnapshot> {
        let mut tx = self.pool.begin().await?;
        let snapshot = fulfillment_snapshot_tx(&mut tx, keepsake_id).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Lists fulfillment expiry candidates in stable batch order.
    #[cfg(feature = "fulfillment-counters")]
    /// Lists fulfillment expiry candidates in stable batch order.
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.fulfillment_pending = 1
              and r.enabled
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            ",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(fulfilled_expiry_candidate_from_row)
            .collect()
    }

    /// Expires a stable batch whose persisted counter snapshots satisfy fulfillment policy.
    #[cfg(feature = "fulfillment-counters")]
    /// Expires a stable batch whose persisted counter snapshots satisfy fulfillment policy.
    pub async fn expire_due_fulfilled(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let target = u64::try_from(limit).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let mut expired = 0;
        let mut tx = self.pool.begin().await?;
        let mut after = None;
        while expired < target {
            let remaining = i64::try_from(target - expired)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let candidates =
                due_fulfilled_expiry_after_tx(&mut tx, after.as_ref(), remaining).await?;
            if candidates.is_empty() {
                break;
            }
            after = candidates.last().map(FulfilledExpiryCursor::from);
            for candidate in candidates {
                let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
                    continue;
                };
                let snapshot = fulfillment_snapshot_tx(&mut tx, candidate.keepsake_id).await?;
                if policy.is_fulfilled(&snapshot) {
                    let result = sqlx::query(
                        r"
                        update keepsakes
                        set state = 'expired', fulfilled_at = ?, updated_at = ?
                        where id = ?
                          and state = 'applied'
                          and exists (
                            select 1
                            from keepsake_relation_definitions r
                            where r.id = keepsakes.relation_id and r.enabled
                          )
                        ",
                    )
                    .bind(naive_timestamp(now))
                    .bind(naive_timestamp(now))
                    .bind(candidate.keepsake_id.to_string())
                    .execute(&mut *tx)
                    .await?;
                    let rows_affected = result.rows_affected();
                    if rows_affected == 1 {
                        record_audit_event_tx(
                            &mut tx,
                            &expiry_event(
                                now,
                                ExpiryCause::Fulfilled,
                                candidate.keepsake_id,
                                candidate.relation_id,
                                candidate.subject_kind,
                                candidate.subject_id,
                            )?,
                        )
                        .await?;
                    }
                    expired += rows_affected;
                }
            }
        }
        tx.commit().await?;
        Ok(expired)
    }

    /// Expires a stable batch of due timed keepsakes.
    pub async fn expire_due_timed(&self, now: DateTime<Utc>, limit: i64) -> RepositoryResult<u64> {
        let candidates = self.due_timed_expiry(now, limit).await?;
        let mut expired = 0;
        let mut tx = self.pool.begin().await?;
        for candidate in candidates {
            let result = sqlx::query(
                r"
                update keepsakes
                set state = 'expired', updated_at = ?
                where id = ?
                  and state = 'applied'
                  and exists (
                    select 1
                    from keepsake_relation_definitions r
                    where r.id = keepsakes.relation_id and r.enabled
                  )
                ",
            )
            .bind(naive_timestamp(now))
            .bind(candidate.keepsake_id.to_string())
            .execute(&mut *tx)
            .await?;
            let rows_affected = result.rows_affected();
            if rows_affected == 1 {
                record_audit_event_tx(
                    &mut tx,
                    &expiry_event(
                        now,
                        ExpiryCause::Timed,
                        candidate.keepsake_id,
                        candidate.relation_id,
                        candidate.subject_kind,
                        candidate.subject_id,
                    )?,
                )
                .await?;
            }
            expired += rows_affected;
        }
        tx.commit().await?;
        Ok(expired)
    }
}

#[cfg(feature = "fulfillment-counters")]
pub(super) async fn due_fulfilled_expiry_after_tx(
    tx: &mut Transaction<'_, MySql>,
    after: Option<&FulfilledExpiryCursor>,
    limit: i64,
) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
    let after_relation_id = after.map(|cursor| cursor.relation_id.to_string());
    let after_keepsake_id = after.map(|cursor| cursor.keepsake_id.to_string());
    let rows = sqlx::query(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.fulfillment_pending = 1
          and r.enabled
          and (
            ? is null
            or (k.relation_id, k.subject_kind, k.subject_id, k.id) > (?, ?, ?, ?)
          )
        order by k.relation_id, k.subject_kind, k.subject_id, k.id
        limit ?
        ",
    )
    .bind(after_relation_id.as_deref())
    .bind(after_relation_id.as_deref())
    .bind(after.map(|cursor| cursor.subject_kind.as_str()))
    .bind(after.map(|cursor| cursor.subject_id.as_str()))
    .bind(after_keepsake_id.as_deref())
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;
    rows.iter()
        .map(fulfilled_expiry_candidate_from_row)
        .collect()
}
#[cfg(feature = "fulfillment-counters")]
pub(super) struct FulfilledExpiryCursor {
    relation_id: Uuid,
    subject_kind: String,
    subject_id: String,
    keepsake_id: Uuid,
}

#[cfg(feature = "fulfillment-counters")]
impl From<&FulfilledExpiryCandidate> for FulfilledExpiryCursor {
    fn from(candidate: &FulfilledExpiryCandidate) -> Self {
        Self {
            relation_id: candidate.relation_id,
            subject_kind: candidate.subject_kind.clone(),
            subject_id: candidate.subject_id.clone(),
            keepsake_id: candidate.keepsake_id,
        }
    }
}
