use crate::repository::observation;
use std::collections::{BTreeMap, BTreeSet};

use keepsake::{ExpiryCause, ExpiryPolicy, FulfillmentSnapshot, KeepsakeId};
use time::OffsetDateTime;
use uuid::Uuid;

use super::super::PostgresBackend;
use super::super::{
    FulfilledExpiryCandidate, RelationCache, RepositoryResult, TenantSqlxKeepsakeRepository,
    support::expiry_event, validate_limit,
};

impl<C> TenantSqlxKeepsakeRepository<'_, PostgresBackend, C>
where
    C: RelationCache,
{
    /// Reads the persisted fulfillment snapshot (counters and checklist) for a keepsake.
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
    /// Requires READ COMMITTED. Locks the counters table and then the checklist table
    /// in SHARE ROW EXCLUSIVE mode, including protection against new projection rows.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns schema, isolation, missing-assignment, storage or invalid-projection errors.
    /// Roll back the caller transaction on failure, including failed lock acquisition.
    pub async fn fulfillment_snapshot_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        keepsake_id: Uuid,
    ) -> RepositoryResult<keepsake::FulfillmentEvidence> {
        observation::require_transaction_schema(tx).await?;
        observation::require_read_committed(tx).await?;
        sqlx::query("lock table keepsake_fulfillment_counters, keepsake_fulfillment_checklist in share row exclusive mode")
            .execute(&mut **tx)
            .await?;

        let counters = sqlx::query_as::<_, (String, i64)>(
            r"
            select key, value
            from keepsake_fulfillment_counters
            where tenant_id = $1 and keepsake_id = $2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        let checklist = sqlx::query_as::<_, (String, bool)>(
            r"
            select item, complete
            from keepsake_fulfillment_checklist
            where tenant_id = $1 and keepsake_id = $2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        Ok(keepsake::FulfillmentEvidence::new(
            self.tenant_id.clone(),
            keepsake_id,
            FulfillmentSnapshot {
                counters,
                checklist,
            },
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
        let rows = sqlx::query_as::<_, FulfilledExpiryCandidate>(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
            from keepsakes k
            join keepsake_relation_definitions r
              on r.tenant_id = k.tenant_id and r.id = k.relation_id
            where k.tenant_id = $1 and k.state = 'applied'
              and r.enabled
              and k.expiry_policy->>'type' = 'when_fulfilled'
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit $2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }

    /// Expires a stable batch whose persisted fulfillment snapshots satisfy policy.
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
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        now: OffsetDateTime,
        limit: i64,
    ) -> RepositoryResult<Vec<KeepsakeId>> {
        observation::require_transaction_schema(tx).await?;
        observation::require_read_committed(tx).await?;
        let limit = validate_limit(limit)?;
        let target =
            usize::try_from(limit).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        let mut after = None;
        let mut satisfied_ids = Vec::new();
        let mut satisfied_candidates = Vec::new();

        while satisfied_ids.len() < target {
            let remaining = i64::try_from(target.saturating_sub(satisfied_ids.len()))
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let candidates =
                due_fulfilled_expiry_tx(tx, &self.tenant_id, after.as_ref(), remaining).await?;
            if candidates.is_empty() {
                break;
            }
            after = candidates.last().map(FulfilledExpiryCursor::from);
            let ids = satisfied_fulfillment_ids_tx(tx, &self.tenant_id, candidates.clone()).await?;
            let id_set = ids.iter().copied().collect::<BTreeSet<_>>();
            satisfied_candidates.extend(
                candidates
                    .into_iter()
                    .filter(|candidate| id_set.contains(&candidate.keepsake_id)),
            );
            satisfied_ids.extend(ids);
        }

        if satisfied_ids.is_empty() {
            return Ok(Vec::new());
        }

        let transitioned = sqlx::query_scalar::<_, KeepsakeId>(
            r"
            update keepsakes
            set state = 'expired', fulfilled_at = $3, updated_at = $3
            where tenant_id = $1 and id = any($2)
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
        .bind(&satisfied_ids)
        .bind(now)
        .fetch_all(&mut **tx)
        .await?;
        let transitioned = transitioned.into_iter().collect::<BTreeSet<_>>();
        let mut expired = Vec::with_capacity(transitioned.len());
        for candidate in satisfied_candidates {
            if !transitioned.contains(&candidate.keepsake_id) {
                continue;
            }
            self.enqueue_audit_event_tx(
                tx,
                &expiry_event(
                    now,
                    ExpiryCause::Fulfilled,
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
    /// Upserts a simple fulfillment counter projection.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns invalid-key or database errors, including an assignment outside this tenant.
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: OffsetDateTime,
    ) -> RepositoryResult<()> {
        keepsake::validate_persisted_identifier("fulfillment.key", key)?;
        sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (tenant_id, keepsake_id, key, value, observed_at)
            values ($1, $2, $3, $4, $5)
            on conflict (tenant_id, keepsake_id, key) do update set
                value = excluded.value,
                observed_at = excluded.observed_at
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id)
        .bind(key)
        .bind(value)
        .bind(observed_at)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Atomically adds `delta` to a fulfillment counter and returns the new value.
    ///
    /// Unlike [`upsert_counter_projection`](Self::upsert_counter_projection), the
    /// increment is computed in the database, so concurrent writers cannot lose
    /// updates to a read-modify-write race.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns invalid-key or database errors, including an assignment outside this tenant
    /// and a counter value outside the backend integer range.
    pub async fn increment_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        delta: i64,
        observed_at: OffsetDateTime,
    ) -> RepositoryResult<i64> {
        keepsake::validate_persisted_identifier("fulfillment.key", key)?;
        let (value,) = sqlx::query_as::<_, (i64,)>(
            r"
            insert into keepsake_fulfillment_counters
                (tenant_id, keepsake_id, key, value, observed_at)
            values ($1, $2, $3, $4, $5)
            on conflict (tenant_id, keepsake_id, key) do update set
                value = keepsake_fulfillment_counters.value + excluded.value,
                observed_at = excluded.observed_at
            returning value
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id)
        .bind(key)
        .bind(delta)
        .bind(observed_at)
        .fetch_one(self.pool)
        .await?;
        Ok(value)
    }

    /// Upserts a checklist item completion projection.
    #[cfg(feature = "fulfillment-counters")]
    ///
    /// # Errors
    ///
    /// Returns invalid-key or database errors, including an assignment outside this tenant.
    pub async fn upsert_checklist_projection(
        &self,
        keepsake_id: Uuid,
        item: &str,
        complete: bool,
        observed_at: OffsetDateTime,
    ) -> RepositoryResult<()> {
        keepsake::validate_persisted_identifier("fulfillment.item", item)?;
        sqlx::query(
            r"
            insert into keepsake_fulfillment_checklist
                (tenant_id, keepsake_id, item, complete, observed_at)
            values ($1, $2, $3, $4, $5)
            on conflict (tenant_id, keepsake_id, item) do update set
                complete = excluded.complete,
                observed_at = excluded.observed_at
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id)
        .bind(item)
        .bind(complete)
        .bind(observed_at)
        .execute(self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(feature = "fulfillment-counters")]
async fn satisfied_fulfillment_ids_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &keepsake::TenantId,
    candidates: Vec<FulfilledExpiryCandidate>,
) -> RepositoryResult<Vec<Uuid>> {
    let candidate_ids = candidates
        .iter()
        .map(|candidate| candidate.keepsake_id)
        .collect::<Vec<_>>();
    if candidate_ids.is_empty() {
        return Ok(Vec::new());
    }

    let counter_rows = sqlx::query_as::<_, (Uuid, String, i64)>(
        r"
            select keepsake_id, key, value
            from keepsake_fulfillment_counters
            where tenant_id = $1 and keepsake_id = any($2)
            ",
    )
    .bind(tenant_id.as_str())
    .bind(&candidate_ids)
    .fetch_all(&mut **tx)
    .await?;
    let mut counters_by_keepsake = BTreeMap::<Uuid, BTreeMap<String, i64>>::new();
    for (keepsake_id, key, value) in counter_rows {
        counters_by_keepsake
            .entry(keepsake_id)
            .or_default()
            .insert(key, value);
    }

    let checklist_rows = sqlx::query_as::<_, (Uuid, String, bool)>(
        r"
            select keepsake_id, item, complete
            from keepsake_fulfillment_checklist
            where tenant_id = $1 and keepsake_id = any($2)
            ",
    )
    .bind(tenant_id.as_str())
    .bind(&candidate_ids)
    .fetch_all(&mut **tx)
    .await?;
    let mut checklist_by_keepsake = BTreeMap::<Uuid, BTreeMap<String, bool>>::new();
    for (keepsake_id, item, complete) in checklist_rows {
        checklist_by_keepsake
            .entry(keepsake_id)
            .or_default()
            .insert(item, complete);
    }

    Ok(candidates
        .into_iter()
        .filter_map(|candidate| {
            let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
                return None;
            };

            let snapshot = FulfillmentSnapshot {
                counters: counters_by_keepsake
                    .remove(&candidate.keepsake_id)
                    .unwrap_or_default(),
                checklist: checklist_by_keepsake
                    .remove(&candidate.keepsake_id)
                    .unwrap_or_default(),
            };
            policy
                .is_fulfilled(&snapshot)
                .then_some(candidate.keepsake_id)
        })
        .collect())
}
#[cfg(feature = "fulfillment-counters")]
async fn due_fulfilled_expiry_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &keepsake::TenantId,
    after: Option<&FulfilledExpiryCursor>,
    limit: i64,
) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
    let rows = sqlx::query_as::<_, FulfilledExpiryCandidate>(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
        from keepsakes k
        join keepsake_relation_definitions r
          on r.tenant_id = k.tenant_id and r.id = k.relation_id
        where k.tenant_id = $1 and k.state = 'applied'
          and r.enabled
          and k.expiry_policy->>'type' = 'when_fulfilled'
          and (
            $3::uuid is null
            or (k.relation_id, k.subject_kind, k.subject_id, k.id) > ($3, $4::text, $5::text, $6::uuid)
          )
        order by k.relation_id, k.subject_kind, k.subject_id, k.id
        limit $2
        for update of k skip locked
        for share of r
        ",
    )
    .bind(tenant_id.as_str())
    .bind(limit)
    .bind(after.map(|cursor| cursor.relation_id))
    .bind(after.map(|cursor| cursor.subject_kind.as_str()))
    .bind(after.map(|cursor| cursor.subject_id.as_str()))
    .bind(after.map(|cursor| cursor.keepsake_id))
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

#[cfg(feature = "fulfillment-counters")]
#[derive(Debug, Clone)]
struct FulfilledExpiryCursor {
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
