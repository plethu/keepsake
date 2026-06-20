use chrono::{DateTime, Utc};
#[cfg(feature = "fulfillment-counters")]
use keepsake::{ExpiryPolicy, FulfillmentSnapshot};
#[cfg(feature = "fulfillment-counters")]
use std::collections::BTreeMap;
use uuid::Uuid;

#[cfg(feature = "fulfillment-counters")]
use super::FulfilledExpiryCandidate;
use super::{
    KeepsakeRepository, RelationCache, RepositoryResult, TimedExpiryCandidate, validate_limit,
};

impl<C> KeepsakeRepository<C>
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
        let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= $1
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit $2
            ",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Reads the persisted fulfillment counter snapshot for a keepsake.
    ///
    /// Checklist state is not persisted by this adapter yet, so snapshots always
    /// contain an empty checklist map.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> RepositoryResult<FulfillmentSnapshot> {
        let counters = sqlx::query_as::<_, (String, i64)>(
            r"
            select key, value
            from keepsake_fulfillment_counters
            where keepsake_id = $1
            ",
        )
        .bind(keepsake_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        Ok(FulfillmentSnapshot {
            counters,
            checklist: BTreeMap::new(),
        })
    }

    /// Lists fulfillment expiry candidates in stable batch order.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query_as::<_, FulfilledExpiryCandidate>(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
            from keepsakes k
            where k.state = 'applied'
              and k.expiry_policy->>'type' = 'when_fulfilled'
            order by k.relation_id, k.subject_kind, k.subject_id, k.id
            limit $1
            ",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Expires a stable batch whose persisted counter snapshots satisfy fulfillment policy.
    ///
    /// `ChecklistComplete` policies cannot be expired via this method because
    /// checklist state is not persisted; callers using `ChecklistComplete` must
    /// evaluate fulfillment in application code and call revoke directly.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn expire_due_fulfilled(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let mut tx = self.pool.begin().await?;
        let candidates = due_fulfilled_expiry_tx(&mut tx, limit).await?;
        let candidate_ids = candidates
            .iter()
            .map(|candidate| candidate.keepsake_id)
            .collect::<Vec<_>>();

        if candidate_ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let counter_rows = sqlx::query_as::<_, (Uuid, String, i64)>(
            r"
            select keepsake_id, key, value
            from keepsake_fulfillment_counters
            where keepsake_id = any($1)
            ",
        )
        .bind(&candidate_ids)
        .fetch_all(&mut *tx)
        .await?;
        let mut counters_by_keepsake = BTreeMap::<Uuid, BTreeMap<String, i64>>::new();
        for (keepsake_id, key, value) in counter_rows {
            counters_by_keepsake
                .entry(keepsake_id)
                .or_default()
                .insert(key, value);
        }

        let satisfied_ids = candidates
            .into_iter()
            .filter_map(|candidate| {
                let ExpiryPolicy::WhenFulfilled { policy } = candidate.expiry_policy else {
                    return None;
                };
                let snapshot = FulfillmentSnapshot {
                    counters: counters_by_keepsake
                        .remove(&candidate.keepsake_id)
                        .unwrap_or_default(),
                    checklist: BTreeMap::new(),
                };
                policy
                    .is_fulfilled(&snapshot)
                    .then_some(candidate.keepsake_id)
            })
            .collect::<Vec<_>>();

        if satisfied_ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let result = sqlx::query(
            r"
            update keepsakes
            set state = 'expired', fulfilled_at = $2, updated_at = $2
            where id = any($1)
              and state = 'applied'
              and exists (
                select 1
                from keepsake_relation_definitions r
                where r.id = keepsakes.relation_id and r.enabled
              )
            ",
        )
        .bind(&satisfied_ids)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    /// Expires a stable batch of due timed keepsakes.
    pub async fn expire_due_timed(&self, now: DateTime<Utc>, limit: i64) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let mut tx = self.pool.begin().await?;
        let rows = due_timed_expiry_tx(&mut tx, now, limit).await?;
        let ids = rows
            .into_iter()
            .map(|row| row.keepsake_id)
            .collect::<Vec<Uuid>>();
        if ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let result = sqlx::query(
            r"
            update keepsakes
            set state = 'expired', updated_at = $2
            where id = any($1)
              and state = 'applied'
              and exists (
                select 1
                from keepsake_relation_definitions r
                where r.id = keepsakes.relation_id and r.enabled
              )
            ",
        )
        .bind(&ids)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    /// Upserts a simple fulfillment counter projection.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<()> {
        sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (keepsake_id, key, value, observed_at)
            values ($1, $2, $3, $4)
            on conflict (keepsake_id, key) do update set
                value = excluded.value,
                observed_at = excluded.observed_at
            ",
        )
        .bind(keepsake_id)
        .bind(key)
        .bind(value)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

async fn due_timed_expiry_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    now: DateTime<Utc>,
    limit: i64,
) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
    let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.state = 'applied'
          and r.enabled
          and k.expires_at is not null
          and k.expires_at <= $1
        order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
        limit $2
        for update of k skip locked
        for share of r
        ",
    )
    .bind(now)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

#[cfg(feature = "fulfillment-counters")]
async fn due_fulfilled_expiry_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    limit: i64,
) -> RepositoryResult<Vec<FulfilledExpiryCandidate>> {
    let rows = sqlx::query_as::<_, FulfilledExpiryCandidate>(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expiry_policy
        from keepsakes k
        where k.state = 'applied'
          and k.expiry_policy->>'type' = 'when_fulfilled'
        order by k.relation_id, k.subject_kind, k.subject_id, k.id
        limit $1
        for update of k skip locked
        ",
    )
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}
