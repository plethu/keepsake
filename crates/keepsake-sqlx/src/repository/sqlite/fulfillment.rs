use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use keepsake::FulfillmentSnapshot;
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use crate::repository::{
    RelationCache, RepositoryResult, SqliteBackend, TenantSqlxKeepsakeRepository,
};

use super::rows::format_timestamp;

impl<C> TenantSqlxKeepsakeRepository<'_, SqliteBackend, C>
where
    C: RelationCache,
{
    /// Upserts a simple fulfillment counter projection.
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
                (tenant_id, keepsake_id, key, value, observed_at)
            values (?1, ?2, ?3, ?4, ?5)
            on conflict (tenant_id, keepsake_id, key) do update set
                value = excluded.value,
                observed_at = excluded.observed_at
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(value)
        .bind(format_timestamp(observed_at))
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Atomically adds `delta` to a fulfillment counter and returns the new value.
    pub async fn increment_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        delta: i64,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<i64> {
        let row = sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (tenant_id, keepsake_id, key, value, observed_at)
            values (?1, ?2, ?3, ?4, ?5)
            on conflict (tenant_id, keepsake_id, key) do update set
                value = value + excluded.value,
                observed_at = excluded.observed_at
            returning value
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(delta)
        .bind(format_timestamp(observed_at))
        .fetch_one(self.pool)
        .await?;
        Ok(row.try_get("value")?)
    }

    /// Upserts a checklist item completion projection.
    pub async fn upsert_checklist_projection(
        &self,
        keepsake_id: Uuid,
        item: &str,
        complete: bool,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<()> {
        sqlx::query(
            r"
            insert into keepsake_fulfillment_checklist
                (tenant_id, keepsake_id, item, complete, observed_at)
            values (?1, ?2, ?3, ?4, ?5)
            on conflict (tenant_id, keepsake_id, item) do update set
                complete = excluded.complete,
                observed_at = excluded.observed_at
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(keepsake_id.to_string())
        .bind(item)
        .bind(i64::from(complete))
        .bind(format_timestamp(observed_at))
        .execute(self.pool)
        .await?;
        Ok(())
    }
}

pub(super) async fn fulfillment_snapshot_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
) -> RepositoryResult<FulfillmentSnapshot> {
    let counter_rows = sqlx::query(
        r"
        select key, value
        from keepsake_fulfillment_counters
        where tenant_id = ?1 and keepsake_id = ?2
        ",
    )
    .bind(tenant_id.as_str())
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;

    let mut counters = BTreeMap::new();
    for row in counter_rows {
        counters.insert(row.try_get("key")?, row.try_get("value")?);
    }

    let checklist_rows = sqlx::query(
        r"
        select item, complete
        from keepsake_fulfillment_checklist
        where tenant_id = ?1 and keepsake_id = ?2
        ",
    )
    .bind(tenant_id.as_str())
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;

    let mut checklist = BTreeMap::new();
    for row in checklist_rows {
        checklist.insert(
            row.try_get("item")?,
            row.try_get::<i64, _>("complete")? != 0,
        );
    }
    Ok(FulfillmentSnapshot {
        counters,
        checklist,
    })
}
