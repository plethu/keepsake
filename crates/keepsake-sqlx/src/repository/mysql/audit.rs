use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use keepsake::{AuditDecision, AuditEvent, RelationId};
use sqlx::{MySql, Row, Transaction};
use uuid::Uuid;

use crate::repository::support::{AuditEventParts, audit_event_record, parse_uuid};
use crate::repository::{
    AuditCursor, AuditEventRecord, AuditOutboxCursor, AuditOutboxRecord, MySqlKeepsakeRepository,
    RelationCache, RepositoryResult, validate_limit,
};

use super::rows::{naive_timestamp, outbox_record_from_mysql_row, utc_timestamp};

impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Appends an explicit audit event without mutating lifecycle state.
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
        let rows = sqlx::query(
            r"
            select id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
                event_type, decision, occurred_at
            from keepsake_audit_events
            where keepsake_id = ?
              and (
                ? is null
                or (occurred_at, id) > (?, ?)
              )
            order by occurred_at, id
            limit ?
            ",
        )
        .bind(keepsake_id.to_string())
        .bind(after.map(|cursor| naive_timestamp(cursor.occurred_at)))
        .bind(after.map(|cursor| naive_timestamp(cursor.occurred_at)))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        hydrate_audit_records(&self.pool, rows).await
    }

    /// Reads audit events for a relation in stable `(occurred_at, id)` order.
    pub async fn audit_events_for_relation(
        &self,
        relation_id: RelationId,
        after: Option<&AuditCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditEventRecord>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query(
            r"
            select id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
                event_type, decision, occurred_at
            from keepsake_audit_events
            where relation_id = ?
              and (
                ? is null
                or (occurred_at, id) > (?, ?)
              )
            order by occurred_at, id
            limit ?
            ",
        )
        .bind(relation_id.to_string())
        .bind(after.map(|cursor| naive_timestamp(cursor.occurred_at)))
        .bind(after.map(|cursor| naive_timestamp(cursor.occurred_at)))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        hydrate_audit_records(&self.pool, rows).await
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
            where delivered_at is null and (? is null or id > ?)
            order by id
            limit ?
            ",
        )
        .bind(after.map(|cursor| cursor.id))
        .bind(after.map(|cursor| cursor.id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(outbox_record_from_mysql_row).collect()
    }

    /// Claims a stable batch of undelivered audit outbox rows until `lease_until`.
    pub async fn claim_audit_outbox(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<Vec<AuditOutboxRecord>> {
        let limit = validate_limit(limit)?;
        let mut tx = self.pool.begin().await?;
        let ids = sqlx::query_scalar::<_, i64>(
            r"
            select id
            from keepsake_audit_outbox
            where delivered_at is null
              and (claimed_until is null or claimed_until <= ?)
            order by id
            limit ?
            for update skip locked
            ",
        )
        .bind(naive_timestamp(now))
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        for id in &ids {
            sqlx::query(
                r"
                update keepsake_audit_outbox
                set claimed_by = ?, claimed_until = ?
                where id = ? and delivered_at is null
                ",
            )
            .bind(worker_id)
            .bind(naive_timestamp(lease_until))
            .bind(*id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.audit_outbox_records_by_ids(&ids).await
    }

    /// Acknowledges delivery of a claimed outbox row.
    pub async fn ack_audit_outbox(
        &self,
        outbox_id: i64,
        delivered_at: DateTime<Utc>,
    ) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_audit_outbox
            set delivered_at = ?, claimed_by = null, claimed_until = null
            where id = ? and delivered_at is null and claimed_by is not null
            ",
        )
        .bind(naive_timestamp(delivered_at))
        .bind(outbox_id)
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
            where id = ? and delivered_at is null and claimed_by is not null
            ",
        )
        .bind(outbox_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn audit_outbox_records_by_ids(
        &self,
        ids: &[i64],
    ) -> RepositoryResult<Vec<AuditOutboxRecord>> {
        let mut records = Vec::with_capacity(ids.len());
        for id in ids {
            let row = sqlx::query(
                r"
                select id, audit_event_id, event_type, payload, claimed_by, claimed_until, delivered_at
                from keepsake_audit_outbox
                where id = ?
                ",
            )
            .bind(*id)
            .fetch_one(&self.pool)
            .await?;
            records.push(outbox_record_from_mysql_row(&row)?);
        }
        Ok(records)
    }
}

pub(super) async fn record_audit_event_tx(
    tx: &mut Transaction<'_, MySql>,
    event: &AuditEvent,
) -> RepositoryResult<i64> {
    let result = sqlx::query(
        r"
        insert into keepsake_audit_events
            (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
             event_type, decision, occurred_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
    )
    .bind(event.keepsake_id.to_string())
    .bind(event.relation_id.to_string())
    .bind(event.subject.kind())
    .bind(event.subject.id())
    .bind(event.actor.kind())
    .bind(event.actor.id())
    .bind(event.event_type.as_str())
    .bind(serde_json::to_value(&event.decision)?)
    .bind(naive_timestamp(event.at))
    .execute(&mut **tx)
    .await?;
    let audit_event_id = i64::try_from(result.last_insert_id())
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;

    record_audit_outbox_tx(tx, audit_event_id, event).await?;

    if event.context.attributes.is_empty() {
        return Ok(audit_event_id);
    }

    let mut builder = sqlx::QueryBuilder::<MySql>::new(
        "insert into keepsake_audit_context_attributes (audit_event_id, `key`, value) ",
    );
    builder.push_values(&event.context.attributes, |mut row, (key, value)| {
        row.push_bind(audit_event_id)
            .push_bind(key.as_str())
            .push_bind(value.as_str());
    });
    builder.build().execute(&mut **tx).await?;

    Ok(audit_event_id)
}

pub(super) async fn record_audit_outbox_tx(
    tx: &mut Transaction<'_, MySql>,
    audit_event_id: i64,
    event: &AuditEvent,
) -> RepositoryResult<()> {
    sqlx::query(
        r"
        insert into keepsake_audit_outbox (audit_event_id, payload)
        values (?, ?)
        ",
    )
    .bind(audit_event_id)
    .bind(serde_json::to_value(event)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn hydrate_audit_records(
    pool: &sqlx::MySqlPool,
    rows: Vec<sqlx::mysql::MySqlRow>,
) -> RepositoryResult<Vec<AuditEventRecord>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids = rows
        .iter()
        .map(|row| row.try_get::<i64, _>("id"))
        .collect::<Result<Vec<i64>, _>>()?;
    let mut attributes = audit_attributes_by_event(pool, &ids).await?;
    rows.into_iter()
        .map(|row| {
            let id = row.try_get::<i64, _>("id")?;
            let decision = serde_json::from_value::<AuditDecision>(row.try_get("decision")?)?;
            audit_event_record(AuditEventParts {
                id,
                event_type: row.try_get("event_type")?,
                at: utc_timestamp(row.try_get("occurred_at")?),
                actor_kind: row.try_get("actor_kind")?,
                actor_id: row.try_get("actor_id")?,
                keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
                subject_kind: row.try_get("subject_kind")?,
                subject_id: row.try_get("subject_id")?,
                relation_id: parse_uuid(row.try_get("relation_id")?)?,
                decision,
                attributes: attributes.remove(&id).unwrap_or_default(),
            })
        })
        .collect()
}

pub(super) async fn audit_attributes_by_event(
    pool: &sqlx::MySqlPool,
    ids: &[i64],
) -> RepositoryResult<BTreeMap<i64, BTreeMap<String, String>>> {
    let mut builder = sqlx::QueryBuilder::<MySql>::new(
        "select audit_event_id, `key`, value from keepsake_audit_context_attributes \
         where audit_event_id in (",
    );
    let mut separated = builder.separated(", ");
    for id in ids {
        separated.push_bind(id);
    }
    builder.push(")");
    let rows = builder
        .build_query_as::<(i64, String, String)>()
        .fetch_all(pool)
        .await?;
    let mut attributes = BTreeMap::<i64, BTreeMap<String, String>>::new();
    for (event_id, key, value) in rows {
        attributes.entry(event_id).or_default().insert(key, value);
    }
    Ok(attributes)
}
