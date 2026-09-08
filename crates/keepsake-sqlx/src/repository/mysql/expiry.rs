use keepsake::{ExpiryCause, KeepsakeId, RelationId};
#[cfg(feature = "fulfillment-counters")]
use keepsake::{ExpiryPolicy, FulfillmentSnapshot};
#[cfg(feature = "fulfillment-counters")]
use sqlx::{MySql, Transaction};
use time::OffsetDateTime;
#[cfg(feature = "fulfillment-counters")]
use uuid::Uuid;

#[cfg(feature = "fulfillment-counters")]
use crate::repository::FulfilledExpiryCandidate;
use crate::repository::support::expiry_event;
use crate::repository::{
    MySqlBackend, RelationCache, RepositoryResult, TenantSqlxKeepsakeRepository,
    TimedExpiryCandidate, validate_limit,
};

#[cfg(feature = "fulfillment-counters")]
use super::fulfillment::fulfillment_snapshot_tx;
#[cfg(feature = "fulfillment-counters")]
use super::rows::fulfilled_expiry_candidate_from_row;
use super::rows::{naive_timestamp, timed_expiry_candidate_from_row};

impl<C> TenantSqlxKeepsakeRepository<'_, MySqlBackend, C>
where
    C: RelationCache,
{
    /// Lists due timed expiry candidates in stable batch order.
    ///
    /// # Errors
    ///
    /// Returns `RepositoryError::InvalidLimit` outside the supported batch range, or a database
    /// or invalid-record decoding error.
    pub async fn due_timed_expiry(
        &self,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = ?
              and k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= ?
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(naive_timestamp(now))
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(timed_expiry_candidate_from_row).collect()
    }

    /// Lists due timed expiry candidates for one relation in stable batch order.
    ///
    /// # Errors
    ///
    /// Returns `RepositoryError::InvalidLimit` outside the supported batch range, or a database
    /// or invalid-record decoding error.
    pub async fn due_timed_expiry_for_relation(
        &self,
        relation_id: RelationId,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = ?
              and k.relation_id = ?
              and k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= ?
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(relation_id.to_string())
        .bind(naive_timestamp(now))
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        rows.iter().map(timed_expiry_candidate_from_row).collect()
    }

    /// Reads the persisted fulfillment snapshot for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns database or invalid-projection decoding errors. Missing evidence remains absent
    /// in the returned snapshot and is not fabricated as fulfilled.
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> RepositoryResult<FulfillmentSnapshot> {
        let mut tx = self.pool.begin().await?;
        let snapshot = self
            .fulfillment_snapshot_in_transaction(&mut tx, keepsake_id)
            .await?;
        tx.commit().await?;
        Ok(snapshot.into_snapshot())
    }

    /// Reads fulfillment evidence while retaining locks until the caller ends its transaction.
    ///
    /// Observe the relation first, then read evidence, then perform the protected write.
    /// This method never begins, commits, or rolls back. After error or cancellation,
    /// roll back the whole transaction. Missing projections remain absent from the snapshot;
    /// the consumer must establish evidence completeness before effective-state evaluation.
    /// Requires `InnoDB` REPEATABLE READ. Locks the assignment, then uses current locking
    /// reads for counters and checklist; predicate locks protect absent projection rows.
    /// Retry the whole transaction after a deadlock.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns schema, isolation, missing-assignment, storage or invalid-projection errors.
    /// Roll back the caller transaction on failure, including failed lock acquisition.
    pub async fn fulfillment_snapshot_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        keepsake_id: Uuid,
    ) -> RepositoryResult<keepsake::FulfillmentEvidence> {
        super::observation::require_transaction_schema(tx).await?;
        super::require_repeatable_read(tx).await?;
        sqlx::query("select id from keepsakes where tenant_id = ? and id = ? for update")
            .bind(self.tenant_id.as_str().as_bytes())
            .bind(keepsake_id.to_string())
            .fetch_optional(&mut **tx)
            .await?;
        let snapshot = fulfillment_snapshot_tx(tx, &self.tenant_id, keepsake_id).await?;
        Ok(keepsake::FulfillmentEvidence::new(
            self.tenant_id.clone(),
            keepsake_id,
            snapshot,
        ))
    }

    /// Lists fulfillment expiry candidates in stable batch order.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns `RepositoryError::InvalidLimit` outside the supported batch range, or a database
    /// or invalid-policy decoding error.
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
            where k.tenant_id = ?
              and k.state = 'applied'
              and r.enabled
              and k.fulfillment_pending = 1
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        rows.iter()
            .map(fulfilled_expiry_candidate_from_row)
            .collect()
    }

    /// Expires a stable batch whose persisted counter snapshots satisfy fulfillment policy.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns invalid-limit, schema, isolation, projection, storage or mandatory audit errors.
    /// The owned transaction is not committed when reconciliation fails.
    pub async fn expire_due_fulfilled(
        &self,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<u64> {
        let mut tx = self.pool.begin().await?;
        let expired = self
            .expire_due_fulfilled_in_transaction(&mut tx, now, limit)
            .await?;
        let count =
            u64::try_from(expired.len()).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        tx.commit().await?;
        Ok(count)
    }

    /// Reconciles due expiry inside the caller's transaction, including lifecycle audit.
    /// Returns only transitioned assignment identities, in candidate order, for composing
    /// notification intents before the caller commits.
    ///
    /// This method never begins, commits, or rolls back a transaction. On error or
    /// cancellation, roll back the whole transaction; earlier writes may remain pending.
    /// Candidate rows are locked in batch order, skipping rows another worker locks.
    /// Acquire protected relation observations in sorted scope order before reconciling.
    /// Retry the entire transaction after a deadlock. This worker operation does not
    /// fence absent relations.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns invalid-limit, schema, isolation, projection, storage or mandatory audit errors.
    /// Roll back the caller transaction on any error; earlier transitions may be staged.
    pub async fn expire_due_fulfilled_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        super::observation::require_transaction_schema(tx).await?;
        super::require_repeatable_read(tx).await?;
        let limit = validate_limit(limit)?;
        let target =
            usize::try_from(limit).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let mut expired = Vec::new();
        let mut after = None;
        while expired.len() < target {
            let remaining = i64::try_from(target.saturating_sub(expired.len()))
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let candidates =
                due_fulfilled_expiry_after_tx(tx, &self.tenant_id, after.as_ref(), remaining)
                    .await?;
            if candidates.is_empty() {
                break;
            }
            after = candidates.last().map(FulfilledExpiryCursor::from);
            for candidate in candidates {
                expired.extend(expire_fulfilled_candidate_tx(self, tx, now, candidate).await?);
            }
        }
        Ok(expired)
    }

    /// Expires a stable batch of due timed keepsakes.
    ///
    /// # Errors
    ///
    /// Returns invalid-limit, schema, isolation, storage or mandatory audit errors.
    /// The owned transaction is not committed when reconciliation fails.
    pub async fn expire_due_timed(&self, now: OffsetDateTime, limit: i64) -> RepositoryResult<u64> {
        let mut tx = self.pool.begin().await?;
        let expired = self
            .expire_due_timed_in_transaction(&mut tx, now, limit)
            .await?;
        let count =
            u64::try_from(expired.len()).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        tx.commit().await?;
        Ok(count)
    }

    /// Expires a stable batch of due timed keepsakes for one relation.
    ///
    /// # Errors
    ///
    /// Returns invalid-limit, schema, isolation, storage or mandatory audit errors.
    /// The owned transaction is not committed when reconciliation fails.
    pub async fn expire_due_timed_for_relation(
        &self,
        relation_id: RelationId,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<u64> {
        let mut tx = self.pool.begin().await?;
        let expired = self
            .expire_due_timed_for_relation_in_transaction(&mut tx, relation_id, now, limit)
            .await?;
        let count =
            u64::try_from(expired.len()).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        tx.commit().await?;
        Ok(count)
    }

    /// Reconciles due expiry inside the caller's transaction, including lifecycle audit.
    /// Returns only transitioned assignment identities, in candidate order, for composing
    /// notification intents before the caller commits.
    ///
    /// This method never begins, commits, or rolls back a transaction. On error or
    /// cancellation, roll back the whole transaction; earlier writes may remain pending.
    /// Candidate rows are locked in batch order, skipping rows another worker locks.
    /// Acquire protected relation observations in sorted scope order before reconciling.
    /// Retry the entire transaction after a deadlock. This worker operation does not
    /// fence absent relations.
    ///
    /// # Errors
    ///
    /// Returns invalid-limit, schema, isolation, storage or mandatory audit errors.
    /// Roll back the caller transaction on any error; earlier transitions may be staged.
    pub async fn expire_due_timed_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        self.expire_due_timed_in_transaction_scoped(tx, now, limit, None)
            .await
    }

    /// Reconciles due expiry for one relation inside the caller's transaction.
    ///
    /// # Errors
    ///
    /// Returns invalid-limit, schema, isolation, storage or mandatory audit errors.
    /// Roll back the caller transaction on any error; earlier transitions may be staged.
    pub async fn expire_due_timed_for_relation_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        relation_id: RelationId,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        self.expire_due_timed_in_transaction_scoped(tx, now, limit, Some(relation_id))
            .await
    }

    async fn expire_due_timed_in_transaction_scoped(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        now: OffsetDateTime,
        limit: i64,
        relation_id: Option<RelationId>,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        super::observation::require_transaction_schema(tx).await?;
        super::require_repeatable_read(tx).await?;
        let limit = validate_limit(limit)?;
        let candidates = due_timed_expiry_tx(tx, &self.tenant_id, relation_id, now, limit).await?;
        let mut expired = Vec::new();
        for candidate in candidates {
            let result = sqlx::query(
                r"
                update keepsakes
                set state = 'expired', updated_at = ?
                where tenant_id = ?
                  and id = ?
                  and (? is null or relation_id = ?)
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
            .bind(naive_timestamp(now))
            .bind(self.tenant_id.as_str().as_bytes())
            .bind(candidate.keepsake_id.to_string())
            .bind(relation_id.map(|id| id.to_string()))
            .bind(relation_id.map(|id| id.to_string()))
            .execute(&mut **tx)
            .await?;
            let rows_affected = result.rows_affected();
            if rows_affected == 1 {
                self.enqueue_audit_event_tx(
                    tx,
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
                expired.push(candidate.keepsake_id);
            }
        }
        Ok(expired)
    }
}

#[cfg(feature = "fulfillment-counters")]
async fn expire_fulfilled_candidate_tx<C>(
    repository: &TenantSqlxKeepsakeRepository<'_, MySqlBackend, C>,
    tx: &mut Transaction<'_, MySql>,
    now: OffsetDateTime,
    candidate: FulfilledExpiryCandidate,
) -> RepositoryResult<Option<keepsake::KeepsakeId>>
where
    C: RelationCache,
{
    let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
        return Ok(None);
    };

    let snapshot =
        fulfillment_snapshot_tx(tx, &repository.tenant_id, candidate.keepsake_id).await?;
    if !policy.is_fulfilled(&snapshot) {
        return Ok(None);
    }

    let result = sqlx::query(
        r"
        update keepsakes
        set state = 'expired', fulfilled_at = ?, updated_at = ?
        where tenant_id = ?
          and id = ?
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
    .bind(naive_timestamp(now))
    .bind(naive_timestamp(now))
    .bind(repository.tenant_id.as_str().as_bytes())
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
    Ok((rows_affected == 1).then_some(candidate.keepsake_id))
}

#[cfg(feature = "fulfillment-counters")]
pub(super) async fn due_fulfilled_expiry_after_tx(
    tx: &mut Transaction<'_, MySql>,
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
        where k.tenant_id = ?
          and k.state = 'applied'
          and r.enabled
          and k.fulfillment_pending = 1
          and (
            ? is null
            or (k.relation_id, k.subject_kind, k.subject_id, k.id) > (?, ?, ?, ?)
          )
        order by k.relation_id, k.subject_kind, k.subject_id, k.id
        limit ?
        for update skip locked
        ",
    )
    .bind(tenant_id.as_str().as_bytes())
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

async fn due_timed_expiry_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    tenant_id: &keepsake::TenantId,
    relation_id: Option<RelationId>,
    now: OffsetDateTime,
    limit: i64,
) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
    let rows = sqlx::query(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = ?
              and (? is null or k.relation_id = ?)
              and k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= ?
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit ?
            for update skip locked
            ",
        )
        .bind(tenant_id.as_str().as_bytes())
        .bind(relation_id.map(|id| id.to_string()))
        .bind(relation_id.map(|id| id.to_string()))
        .bind(naive_timestamp(now))
        .bind(limit)
        .fetch_all(&mut **tx)
        .await?;
    rows.iter().map(timed_expiry_candidate_from_row).collect()
}
