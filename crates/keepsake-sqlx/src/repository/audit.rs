use keepsake::{AuditEvent, CommandContext};
use sqlx::{Postgres, Transaction};

use super::{KeepsakeRepository, RelationCache, RepositoryResult};

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
    .bind(&event.subject.kind)
    .bind(&event.subject.id)
    .bind(&event.actor.kind)
    .bind(&event.actor.id)
    .bind(event.event_type.as_str())
    .bind(decision)
    .bind(event.at)
    .fetch_one(&mut **tx)
    .await?;

    for (key, value) in &event.context.attributes {
        sqlx::query(
            r"
            insert into keepsake_audit_context_attributes
                (audit_event_id, key, value)
            values ($1, $2, $3)
            ",
        )
        .bind(audit_event_id)
        .bind(key)
        .bind(value)
        .execute(&mut **tx)
        .await?;
    }

    Ok(audit_event_id)
}

pub(super) fn audit_context_from_command(context: &CommandContext) -> keepsake::AuditContext {
    let mut attributes = context.metadata.clone();
    if let Some(idempotency_key) = &context.idempotency_key {
        attributes
            .entry("idempotency_key".to_owned())
            .or_insert_with(|| idempotency_key.clone());
    }
    keepsake::AuditContext { attributes }
}
