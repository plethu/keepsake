use crate::repository::observation;
use std::collections::BTreeSet;

use keepsake::{ExpiryCause, KeepsakeId, RelationId};
use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(feature = "fulfillment-counters")]
mod fulfillment;

use super::PostgresBackend;
use super::{
    RelationCache, RepositoryResult, TenantSqlxKeepsakeRepository, TimedExpiryCandidate,
    support::expiry_event, validate_limit,
};

impl<C> TenantSqlxKeepsakeRepository<'_, PostgresBackend, C>
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
        let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r
              on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = $1 and k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= $2
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit $3
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(now)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
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
        let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r
              on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = $1 and k.relation_id = $2 and k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= $3
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit $4
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation_id)
        .bind(now)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
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
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        relation_id: RelationId,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        self.expire_due_timed_in_transaction_scoped(tx, now, limit, Some(relation_id))
            .await
    }

    async fn expire_due_timed_in_transaction_scoped(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        now: OffsetDateTime,
        limit: i64,
        relation_id: Option<RelationId>,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        observation::require_transaction_schema(tx).await?;
        observation::require_read_committed(tx).await?;
        let limit = validate_limit(limit)?;
        let candidates = due_timed_expiry_tx(tx, &self.tenant_id, relation_id, now, limit).await?;
        let ids = candidates
            .iter()
            .map(|row| row.keepsake_id)
            .collect::<Vec<Uuid>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let transitioned = sqlx::query_scalar::<_, KeepsakeId>(
            r"
            update keepsakes
            set state = 'expired', updated_at = $3
            where tenant_id = $1 and id = any($2)
              and ($4::uuid is null or relation_id = $4)
              and state = 'applied'
              and exists (
                select 1
                from keepsake_relation_definitions r
                where r.tenant_id = keepsakes.tenant_id
                  and r.id = keepsakes.relation_id and r.enabled
              )
            returning id
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(&ids)
        .bind(now)
        .bind(relation_id)
        .fetch_all(&mut **tx)
        .await?;
        let transitioned = transitioned.into_iter().collect::<BTreeSet<_>>();
        let mut expired = Vec::with_capacity(transitioned.len());
        for candidate in candidates {
            if !transitioned.contains(&candidate.keepsake_id) {
                continue;
            }
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
        Ok(expired)
    }
}

async fn due_timed_expiry_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &keepsake::TenantId,
    relation_id: Option<RelationId>,
    now: OffsetDateTime,
    limit: i64,
) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
    let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
        from keepsakes k
        join keepsake_relation_definitions r
          on r.tenant_id = k.tenant_id and r.id = k.relation_id
        where k.tenant_id = $1 and k.state = 'applied'
          and ($4::uuid is null or k.relation_id = $4)
          and r.enabled
          and k.expires_at is not null
          and k.expires_at <= $2
        order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
        limit $3
        for update of k skip locked
        for share of r
        ",
    )
    .bind(tenant_id.as_str())
    .bind(now)
    .bind(limit)
    .bind(relation_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}
