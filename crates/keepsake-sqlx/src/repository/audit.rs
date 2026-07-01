use std::collections::BTreeMap;

use keepsake::{AuditEvent, RelationId};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    AuditCursor, AuditEventRecord, AuditEventRow, KeepsakeRepository, RelationCache,
    RepositoryResult, validate_limit,
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
