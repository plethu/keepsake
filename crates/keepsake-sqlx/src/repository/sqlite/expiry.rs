use chrono::{DateTime, Utc};
use keepsake::ExpiryCause;
#[cfg(feature = "fulfillment-counters")]
use keepsake::{ExpiryPolicy, FulfillmentSnapshot};
#[cfg(feature = "fulfillment-counters")]
use sqlx::{Sqlite, Transaction};
#[cfg(feature = "fulfillment-counters")]
use uuid::Uuid;

#[cfg(feature = "fulfillment-counters")]
use crate::repository::FulfilledExpiryCandidate;
use crate::repository::support::expiry_event;
use crate::repository::{
    RelationCache, RepositoryResult, SqliteBackend, TenantSqlxKeepsakeRepository,
    TimedExpiryCandidate, validate_limit,
};

#[cfg(feature = "fulfillment-counters")]
use super::fulfillment::fulfillment_snapshot_tx;
#[cfg(feature = "fulfillment-counters")]
use super::rows::fulfilled_expiry_candidate_from_row;
use super::rows::{format_timestamp, timed_expiry_candidate_from_row};

impl<C> TenantSqlxKeepsakeRepository<'_, SqliteBackend, C>
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
            join keepsake_relation_definitions r on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = ?1
              and k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= ?2
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?3
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(format_timestamp(now))
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(timed_expiry_candidate_from_row).collect()
    }

    /// Reads the persisted fulfillment snapshot for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> RepositoryResult<FulfillmentSnapshot> {
        let mut tx = super::lifecycle::begin_write_tx(self.pool).await?;
        let snapshot = fulfillment_snapshot_tx(&mut tx, &self.tenant_id, keepsake_id).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Lists fulfillment expiry candidates in stable batch order.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
            from keepsakes k
            join keepsake_relation_definitions r on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = ?1
              and k.state = 'applied'
              and r.enabled
              and json_extract(k.expiry_policy, '$.type') = 'when_fulfilled'
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        rows.iter()
            .map(fulfilled_expiry_candidate_from_row)
            .collect()
    }

    /// Expires a stable batch whose persisted counter snapshots satisfy fulfillment policy.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn expire_due_fulfilled(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let target = u64::try_from(limit).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let mut expired = 0;
        let mut tx = super::lifecycle::begin_write_tx(self.pool).await?;
        let mut after = None;
        while expired < target {
            let remaining = i64::try_from(target - expired)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let candidates =
                due_fulfilled_expiry_after_tx(&mut tx, &self.tenant_id, after.as_ref(), remaining)
                    .await?;
            if candidates.is_empty() {
                break;
            }
            after = candidates.last().map(FulfilledExpiryCursor::from);
            for candidate in candidates {
                expired += expire_fulfilled_candidate_tx(self, &mut tx, now, candidate).await?;
            }
        }
        tx.commit().await?;
        Ok(expired)
    }

    /// Expires a stable batch of due timed keepsakes.
    pub async fn expire_due_timed(&self, now: DateTime<Utc>, limit: i64) -> RepositoryResult<u64> {
        let candidates = self.due_timed_expiry(now, limit).await?;
        let mut expired = 0;
        let mut tx = super::lifecycle::begin_write_tx(self.pool).await?;
        for candidate in candidates {
            let result = sqlx::query(
                r"
                update keepsakes
                set state = 'expired', updated_at = ?2
                where tenant_id = ?1
                  and id = ?3
                  and state = 'applied'
                  and exists (
                    select 1
                    from keepsake_relation_definitions r
                    where r.tenant_id = keepsakes.tenant_id
                      and r.id = keepsakes.relation_id
                      and r.enabled
                  )
                ",
            )
            .bind(self.tenant_id.as_str())
            .bind(format_timestamp(now))
            .bind(candidate.keepsake_id.to_string())
            .execute(&mut *tx)
            .await?;
            let rows_affected = result.rows_affected();
            if rows_affected == 1 {
                self.enqueue_audit_event_tx(
                    &mut tx,
                    &expiry_event(
                        now,
                        ExpiryCause::Timed,
                        self.tenant_id.clone(),
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
async fn expire_fulfilled_candidate_tx<C>(
    repository: &TenantSqlxKeepsakeRepository<'_, SqliteBackend, C>,
    tx: &mut Transaction<'_, Sqlite>,
    now: DateTime<Utc>,
    candidate: FulfilledExpiryCandidate,
) -> RepositoryResult<u64>
where
    C: RelationCache,
{
    let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
        return Ok(0);
    };
    let snapshot =
        fulfillment_snapshot_tx(tx, &repository.tenant_id, candidate.keepsake_id).await?;
    if !policy.is_fulfilled(&snapshot) {
        return Ok(0);
    }

    let result = sqlx::query(
        r"
        update keepsakes
        set state = 'expired', fulfilled_at = ?2, updated_at = ?2
        where tenant_id = ?1
          and id = ?3
          and state = 'applied'
          and exists (
            select 1
            from keepsake_relation_definitions r
            where r.tenant_id = keepsakes.tenant_id
              and r.id = keepsakes.relation_id
              and r.enabled
          )
        ",
    )
    .bind(repository.tenant_id.as_str())
    .bind(format_timestamp(now))
    .bind(candidate.keepsake_id.to_string())
    .execute(&mut **tx)
    .await?;
    let rows_affected = result.rows_affected();
    if rows_affected == 1 {
        repository
            .enqueue_audit_event_tx(
                tx,
                &expiry_event(
                    now,
                    ExpiryCause::Fulfilled,
                    repository.tenant_id.clone(),
                    candidate.keepsake_id,
                    candidate.relation_id,
                    candidate.subject_kind,
                    candidate.subject_id,
                )?,
            )
            .await?;
    }
    Ok(rows_affected)
}

#[cfg(feature = "fulfillment-counters")]
pub(super) async fn due_fulfilled_expiry_after_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    after: Option<&FulfilledExpiryCursor>,
    limit: i64,
) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
    let after_relation_id = after.map(|cursor| cursor.relation_id.to_string());
    let after_keepsake_id = after.map(|cursor| cursor.keepsake_id.to_string());
    let rows = sqlx::query(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
        from keepsakes k
        join keepsake_relation_definitions r on r.tenant_id = k.tenant_id and r.id = k.relation_id
        where k.tenant_id = ?1
          and k.state = 'applied'
          and r.enabled
          and json_extract(k.expiry_policy, '$.type') = 'when_fulfilled'
          and (
            ?2 is null
            or (k.relation_id, k.subject_kind, k.subject_id, k.id) > (?2, ?3, ?4, ?5)
          )
        order by k.relation_id, k.subject_kind, k.subject_id, k.id
        limit ?6
        ",
    )
    .bind(tenant_id.as_str())
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
