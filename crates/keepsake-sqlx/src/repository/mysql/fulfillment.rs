use std::collections::BTreeMap;

use keepsake::FulfillmentSnapshot;
use sqlx::{MySql, Row, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::repository::{
    MySqlBackend, RelationCache, RepositoryResult, TenantSqlxKeepsakeRepository,
};

use super::rows::naive_timestamp;

impl<C> TenantSqlxKeepsakeRepository<'_, MySqlBackend, C>
where
    C: RelationCache,
{
    /// Upserts a simple fulfillment counter projection.
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
                (tenant_id, keepsake_id, `key`, value, observed_at)
            values (?, ?, ?, ?, ?)
            on duplicate key update
                value = values(value),
                observed_at = values(observed_at)
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(value)
        .bind(naive_timestamp(observed_at))
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
        observed_at: OffsetDateTime,
    ) -> RepositoryResult<i64> {
        keepsake::validate_persisted_identifier("fulfillment.key", key)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (tenant_id, keepsake_id, `key`, value, observed_at)
            values (?, ?, ?, ?, ?)
            on duplicate key update
                value = value + values(value),
                observed_at = values(observed_at)
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(keepsake_id.to_string())
        .bind(key)
        .bind(delta)
        .bind(naive_timestamp(observed_at))
        .execute(&mut *tx)
        .await?;
        let value: i64 = sqlx::query(
            r"
            select value
            from keepsake_fulfillment_counters
            where tenant_id = ? and keepsake_id = ? and `key` = ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(keepsake_id.to_string())
        .bind(key)
        .fetch_one(&mut *tx)
        .await?
        .try_get("value")?;
        tx.commit().await?;
        Ok(value)
    }

    /// Upserts a checklist item completion projection.
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
            values (?, ?, ?, ?, ?)
            on duplicate key update
                complete = values(complete),
                observed_at = values(observed_at)
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(keepsake_id.to_string())
        .bind(item)
        .bind(i64::from(complete))
        .bind(naive_timestamp(observed_at))
        .execute(self.pool)
        .await?;
        Ok(())
    }
}

pub(super) async fn fulfillment_snapshot_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
) -> RepositoryResult<FulfillmentSnapshot> {
    let counter_rows = sqlx::query(
        r"
        select `key`, value
        from keepsake_fulfillment_counters
        where tenant_id = ? and keepsake_id = ?
        ",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;
    let checklist_rows = sqlx::query(
        r"
        select item, complete
        from keepsake_fulfillment_checklist
        where tenant_id = ? and keepsake_id = ?
        ",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(keepsake_id.to_string())
    .fetch_all(&mut **tx)
    .await?;
    snapshot_from_rows(&counter_rows, &checklist_rows)
}

fn snapshot_from_rows(
    counter_rows: &[sqlx::mysql::MySqlRow],
    checklist_rows: &[sqlx::mysql::MySqlRow],
) -> RepositoryResult<FulfillmentSnapshot> {
    let mut counters = BTreeMap::new();
    for row in counter_rows {
        counters.insert(row.try_get("key")?, row.try_get("value")?);
    }

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
