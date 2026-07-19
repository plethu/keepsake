use std::collections::BTreeMap;

use keepsake::{AuditEvent, RelationId};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    AuditCursor, AuditEventRecord, AuditEventRow, AuditOutboxCursor, AuditOutboxRecord,
    KeepsakeRepository, RelationCache, RepositoryResult, validate_limit,
};

impl<C> KeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Appends an explicit audit event without mutating lifecycle state.
    ///
    /// Prefer `apply` and `revoke` for lifecycle mutations so state and audit
    /// rows commit together. This helper is for application-owned audit events
    /// that do not have a built-in repository command.
    pub async fn append_audit_event(&self, event: &AuditEvent) -> RepositoryResult<i64> {
        event.subject.validate()?;
        event.actor.validate()?;

        let mut tx = self.pool.begin().await?;
        let audit_event_id = record_audit_event_tx(&mut tx, event).await?;
        tx.commit().await?;
        Ok(audit_event_id)
    }

    /// Reads audit events for a keepsake in stable `(occurred_at, id)` order.
    pub async fn audit_events_for_keepsake(
        &self,
        keepsake_id: Uuid,
        after: Option<&AuditCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditEventRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query_as::<_, AuditEventRow>(
            r"
            select id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
                event_type, decision, occurred_at
            from keepsake_audit_events
            where keepsake_id = $1
              and ($2::timestamptz is null or (occurred_at, id) > ($2, $3))
            order by occurred_at, id
            limit $4
            ",
        )
        .bind(keepsake_id)
        .bind(after.map(|cursor| cursor.occurred_at))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        self.hydrate_audit_records(rows).await
    }

    /// Reads audit events for a relation in stable `(occurred_at, id)` order.
    pub async fn audit_events_for_relation(
        &self,
        relation_id: RelationId,
        after: Option<&AuditCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditEventRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query_as::<_, AuditEventRow>(
            r"
            select id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
                event_type, decision, occurred_at
            from keepsake_audit_events
            where relation_id = $1
              and ($2::timestamptz is null or (occurred_at, id) > ($2, $3))
            order by occurred_at, id
            limit $4
            ",
        )
        .bind(relation_id)
        .bind(after.map(|cursor| cursor.occurred_at))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        self.hydrate_audit_records(rows).await
    }

    /// Exports undelivered audit outbox rows in stable id order.
    pub async fn audit_outbox(
        &self,
        after: Option<&AuditOutboxCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditOutboxRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select id, audit_event_id, event_type, payload, claimed_by, claimed_until, delivered_at
            from keepsake_audit_outbox
            where delivered_at is null and ($1::bigint is null or id > $1)
            order by id
            limit $2
            ",
        )
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(outbox_record_from_pg_row).collect()
    }

    /// Claims a stable batch of undelivered audit outbox rows until `lease_until`.
    pub async fn claim_audit_outbox(
        &self,
        worker_id: &str,
        now: chrono::DateTime<chrono::Utc>,
        lease_until: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditOutboxRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            with claimable as (
              select id
              from keepsake_audit_outbox
              where delivered_at is null
                and (claimed_until is null or claimed_until <= $3)
              order by id
              limit $4
              for update skip locked
            ),
            updated as (
              update keepsake_audit_outbox
              set claimed_by = $1, claimed_until = $2
              where id in (select id from claimable)
              returning id, audit_event_id, event_type, payload, claimed_by, claimed_until, delivered_at
            )
            select id, audit_event_id, event_type, payload, claimed_by, claimed_until, delivered_at
            from updated
            order by id
            ",
        )
        .bind(worker_id)
        .bind(lease_until)
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(outbox_record_from_pg_row).collect()
    }

    /// Acknowledges delivery of a claimed outbox row.
    pub async fn ack_audit_outbox(
        &self,
        outbox_id: i64,
        delivered_at: chrono::DateTime<chrono::Utc>,
    ) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_audit_outbox
            set delivered_at = $2, claimed_by = null, claimed_until = null
            where id = $1 and delivered_at is null and claimed_by is not null
            ",
        )
        .bind(outbox_id)
        .bind(delivered_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Releases a claimed outbox row for another worker to retry.
    pub async fn release_audit_outbox(&self, outbox_id: i64) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_audit_outbox
            set claimed_by = null, claimed_until = null
            where id = $1 and delivered_at is null and claimed_by is not null
            ",
        )
        .bind(outbox_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn hydrate_audit_records(
        &self,
        rows: Vec<AuditEventRow>,
    ) -> RepositoryResult<Vec<AuditEventRecord>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let ids = rows.iter().map(|row| row.id).collect::<Vec<i64>>();
        let attribute_rows = sqlx::query_as::<_, (i64, String, String)>(
            r"
            select audit_event_id, key, value
            from keepsake_audit_context_attributes
            where audit_event_id = any($1)
            ",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        let mut attributes = BTreeMap::<i64, BTreeMap<String, String>>::new();
        for (event_id, key, value) in attribute_rows {
            attributes.entry(event_id).or_default().insert(key, value);
        }
        rows.into_iter()
            .map(|row| {
                let id = row.id;
                row.into_record(attributes.remove(&id).unwrap_or_default())
            })
            .collect()
    }
}

fn outbox_record_from_pg_row(row: &sqlx::postgres::PgRow) -> RepositoryResult<AuditOutboxRecord> {
    use sqlx::Row;

    let payload = serde_json::from_value::<AuditEvent>(row.try_get("payload")?)?;
    Ok(AuditOutboxRecord {
        id: row.try_get("id")?,
        audit_event_id: row.try_get("audit_event_id")?,
        event_type: row.try_get("event_type")?,
        payload,
        claimed_by: row.try_get("claimed_by")?,
        claimed_until: row.try_get("claimed_until")?,
        delivered_at: row.try_get("delivered_at")?,
    })
}

pub(super) async fn record_audit_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: &AuditEvent,
) -> RepositoryResult<i64> {
    let decision = serde_json::to_value(&event.decision)?;
    let audit_event_id = sqlx::query_scalar::<_, i64>(
        r"
        insert into keepsake_audit_events
            (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
             event_type, decision, occurred_at)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        returning id
        ",
    )
    .bind(event.keepsake_id)
    .bind(event.relation_id)
    .bind(event.subject.kind())
    .bind(event.subject.id())
    .bind(event.actor.kind())
    .bind(event.actor.id())
    .bind(event.event_type.as_str())
    .bind(decision)
    .bind(event.at)
    .fetch_one(&mut **tx)
    .await?;

    record_audit_outbox_tx(tx, audit_event_id, event).await?;

    if event.context.attributes.is_empty() {
        return Ok(audit_event_id);
    }

    let keys = event.context.attributes.keys().cloned().collect::<Vec<_>>();
    let values = event
        .context
        .attributes
        .values()
        .cloned()
        .collect::<Vec<_>>();
    sqlx::query(
        r"
        insert into keepsake_audit_context_attributes (audit_event_id, key, value)
        select $1, attribute.key, attribute.value
        from unnest($2::text[], $3::text[]) as attribute(key, value)
        ",
    )
    .bind(audit_event_id)
    .bind(&keys)
    .bind(&values)
    .execute(&mut **tx)
    .await?;

    Ok(audit_event_id)
}

async fn record_audit_outbox_tx(
    tx: &mut Transaction<'_, Postgres>,
    audit_event_id: i64,
    event: &AuditEvent,
) -> RepositoryResult<()> {
    sqlx::query(
        r"
        insert into keepsake_audit_outbox (audit_event_id, payload)
        values ($1, $2)
        ",
    )
    .bind(audit_event_id)
    .bind(serde_json::to_value(event)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
