use chrono::{DateTime, Utc};
use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, RelationDefinition, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

use crate::repository::support::{
    apply_event, canonical_timestamp, dovecote_event, dovecote_tenant_id, expires_at, replay_event,
    revoke_by_subject_event, revoke_event,
};
use crate::repository::{
    AppliedKeepsake, RelationCache, RepositoryError, RepositoryResult, SqliteBackend,
    TenantSqlxKeepsakeRepository,
};

use super::rows::{format_timestamp, keepsake_from_row, relation_from_row};

impl<C> TenantSqlxKeepsakeRepository<'_, SqliteBackend, C>
where
    C: RelationCache,
{
    /// Applies a command idempotently and records its audit event atomically.
    pub async fn apply(&self, command: &ApplyKeepsake) -> RepositoryResult<AppliedKeepsake> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        command.subject.validate()?;
        command.context.validate()?;
        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        let command = &command;

        let mut tx = begin_write_tx(self.pool).await?;
        let relation =
            relation_for_update_tx(&mut tx, &self.tenant_id, command.relation_id).await?;
        if let Some(existing) = active_keepsake_for_subject_relation_tx(
            &mut tx,
            &self.tenant_id,
            &command.subject,
            command.relation_id,
        )
        .await?
        {
            let event = replay_event(
                existing_audit_event_tx(&mut tx, &self.tenant_id, self.audit, command.audit_id)
                    .await?,
                apply_event(command, &existing, true),
            );
            self.enqueue_audit_event_tx(&mut tx, &event).await?;
            tx.commit().await?;
            return Ok(AppliedKeepsake {
                keepsake: existing,
                duplicate_prevented: true,
            });
        }

        if !relation.enabled {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let metadata = serde_json::to_string(&command.metadata)?;
        let expires_at_column = expires_at(&relation.expiry).map(format_timestamp);
        let at = format_timestamp(command.at);
        let result = sqlx::query(
            r"
            insert into keepsakes
                (tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                 expires_at, metadata, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, 'applied', ?6, ?7, ?8, ?9, ?7, ?7)
            on conflict (tenant_id, subject_kind, subject_id, relation_id) where state = 'applied'
            do nothing
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(command.id.to_string())
        .bind(command.subject.kind())
        .bind(command.subject.id())
        .bind(command.relation_id.to_string())
        .bind(expiry_policy)
        .bind(&at)
        .bind(expires_at_column)
        .bind(metadata)
        .execute(&mut *tx)
        .await?;

        let (keepsake, duplicate_prevented) = if result.rows_affected() == 0 {
            let existing = active_keepsake_for_subject_relation_tx(
                &mut tx,
                &self.tenant_id,
                &command.subject,
                command.relation_id,
            )
            .await?
            .ok_or(RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            })?;
            (existing, true)
        } else {
            let keepsake = keepsake_by_id_tx(&mut tx, &self.tenant_id, command.id)
                .await?
                .ok_or(RepositoryError::RelationDefinitionMissing {
                    relation_id: command.relation_id,
                })?;
            (keepsake, false)
        };

        self.enqueue_audit_event_tx(
            &mut tx,
            &apply_event(command, &keepsake, duplicate_prevented),
        )
        .await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented,
        })
    }

    /// Revokes an active keepsake from a command and records its audit event atomically.
    pub async fn revoke(&self, command: &RevokeKeepsake) -> RepositoryResult<bool> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        command.context.validate()?;
        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        let command = &command;

        let mut tx = begin_write_tx(self.pool).await?;
        let revoked = revoke_tx(&mut tx, &self.tenant_id, command.keepsake_id, command.at).await?;
        if let Some(keepsake) = &revoked {
            self.enqueue_audit_event_tx(&mut tx, &revoke_event(command, keepsake))
                .await?;
        }
        tx.commit().await?;
        Ok(revoked.is_some())
    }

    /// Revokes the active keepsake for a subject and relation pair.
    pub async fn revoke_by_subject(
        &self,
        command: &RevokeBySubject,
    ) -> RepositoryResult<Option<KeepsakeId>> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        command.subject.validate()?;
        command.context.validate()?;
        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        let command = &command;

        let mut tx = begin_write_tx(self.pool).await?;
        let revoked = revoke_by_subject_tx(
            &mut tx,
            &self.tenant_id,
            &command.subject,
            command.relation_id,
            command.at,
        )
        .await?;
        let revoked_id = revoked.as_ref().map(Keepsake::id);
        if let Some(keepsake) = &revoked {
            self.enqueue_audit_event_tx(&mut tx, &revoke_by_subject_event(command, keepsake))
                .await?;
        }
        tx.commit().await?;
        Ok(revoked_id)
    }
}

async fn existing_audit_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    config: &super::super::support::DovecoteAuditConfig,
    audit_id: keepsake::AuditEventId,
) -> RepositoryResult<Option<keepsake::AuditEvent>> {
    let event_id = format!("keepsake-audit-{}", audit_id.as_uuid());
    let row = sqlx::query(
        "select data from dovecote_events where tenant_id = ? and source = ? and event_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(config.source())
    .bind(event_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else { return Ok(None) };

    let data: Option<Vec<u8>> = sqlx::Row::try_get(&row, "data")?;
    data.map(|data| serde_json::from_slice(&data).map_err(RepositoryError::from))
        .transpose()
}

impl<C> TenantSqlxKeepsakeRepository<'_, SqliteBackend, C>
where
    C: RelationCache,
{
    pub(super) async fn enqueue_audit_event_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        audit: &keepsake::AuditEvent,
    ) -> RepositoryResult<()> {
        let event = dovecote_event(self.audit, audit)?;
        let tenant_id = dovecote_tenant_id(&self.tenant_id)?;
        dovecote_sqlx_sqlite::SqliteDovecote::new((*self.pool).clone())
            .for_tenant(tenant_id)
            .enqueue(tx, event)
            .await
            .map(|_| ())
            .map_err(|error| RepositoryError::DovecoteEnqueue(error.into()))
    }
}

pub(super) async fn begin_write_tx(
    pool: &sqlx::SqlitePool,
) -> RepositoryResult<Transaction<'static, Sqlite>> {
    dovecote_sqlx_sqlite::begin_write(pool)
        .await
        .map_err(|error| RepositoryError::DovecoteEnqueue(error.into()))
}

pub(super) async fn relation_for_update_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query(
        r"
        select tenant_id, id, kind, key, enabled, expiry_policy
        from keepsake_relation_definitions
        where tenant_id = ?1 and id = ?2
        ",
    )
    .bind(tenant_id.as_str())
    .bind(relation_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    relation_from_row(&row)
}

pub(super) async fn active_keepsake_for_subject_relation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    subject: &SubjectRef,
    relation_id: RelationId,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where tenant_id = ?1 and subject_kind = ?2 and subject_id = ?3 and relation_id = ?4 and state = 'applied'
        ",
    )
    .bind(tenant_id.as_str())
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn keepsake_by_id_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where tenant_id = ?1 and id = ?2
        ",
    )
    .bind(tenant_id.as_str())
    .bind(keepsake_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?3, updated_at = ?3
        where tenant_id = ?1 and id = ?2 and state = 'applied'
        returning tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(tenant_id.as_str())
    .bind(keepsake_id.to_string())
    .bind(format_timestamp(at))
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_by_subject_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    subject: &SubjectRef,
    relation_id: RelationId,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?5, updated_at = ?5
        where tenant_id = ?1 and subject_kind = ?2 and subject_id = ?3 and relation_id = ?4 and state = 'applied'
        returning tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(tenant_id.as_str())
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .bind(format_timestamp(at))
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}
