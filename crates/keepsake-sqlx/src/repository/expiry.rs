use chrono::{DateTime, Utc};
use uuid::Uuid;

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
