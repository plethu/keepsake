use keepsake::ExpiryCause;
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
    /// Expires a stable batch of due timed keepsakes.
    pub async fn expire_due_timed(&self, now: OffsetDateTime, limit: i64) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let mut tx = self.pool.begin().await?;
        let candidates = due_timed_expiry_tx(&mut tx, &self.tenant_id, now, limit).await?;
        let ids = candidates
            .iter()
            .map(|row| row.keepsake_id)
            .collect::<Vec<Uuid>>();
        if ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let result = sqlx::query(
            r"
            update keepsakes
            set state = 'expired', updated_at = $3
            where tenant_id = $1 and id = any($2)
              and state = 'applied'
              and exists (
                select 1
                from keepsake_relation_definitions r
                where r.tenant_id = keepsakes.tenant_id
                  and r.id = keepsakes.relation_id and r.enabled
              )
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(&ids)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        for candidate in candidates {
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
        tx.commit().await?;
        Ok(result.rows_affected())
    }
}

async fn due_timed_expiry_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &keepsake::TenantId,
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
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}
