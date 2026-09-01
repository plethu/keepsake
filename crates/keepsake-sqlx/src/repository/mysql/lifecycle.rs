use chrono::{DateTime, Utc};
use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, RelationDefinition, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
use sqlx::{MySql, Transaction};
use uuid::Uuid;

use crate::repository::support::{
    apply_event, canonical_timestamp, dovecote_event, dovecote_tenant_id, expires_at, replay_event,
    revoke_by_subject_event, revoke_event,
};
use crate::repository::{
    AppliedKeepsake, MySqlBackend, RelationCache, RepositoryError, RepositoryResult,
    TenantSqlxKeepsakeRepository,
};

use super::rows::{keepsake_from_row, naive_timestamp, relation_from_row};

impl<C> TenantSqlxKeepsakeRepository<'_, MySqlBackend, C>
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

        let mut tx = self.pool.begin().await?;
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

        sqlx::query(
            r"
            insert into keepsakes
                (tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                 expires_at, metadata, created_at, updated_at)
            values (?, ?, ?, ?, ?, 'applied', ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(command.id.to_string())
        .bind(command.subject.kind())
        .bind(command.subject.id())
        .bind(command.relation_id.to_string())
        .bind(serde_json::to_value(&relation.expiry)?)
        .bind(naive_timestamp(command.at))
        .bind(expires_at(&relation.expiry).map(naive_timestamp))
        .bind(serde_json::to_value(&command.metadata)?)
        .bind(naive_timestamp(command.at))
        .bind(naive_timestamp(command.at))
        .execute(&mut *tx)
        .await?;

        let keepsake = keepsake_by_id_tx(&mut tx, &self.tenant_id, command.id)
            .await?
            .ok_or(RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            })?;
        self.enqueue_audit_event_tx(&mut tx, &apply_event(command, &keepsake, false))
            .await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented: false,
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

        let mut tx = self.pool.begin().await?;
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

        let mut tx = self.pool.begin().await?;
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
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    config: &super::super::support::DovecoteAuditConfig,
    audit_id: keepsake::AuditEventId,
) -> RepositoryResult<Option<keepsake::AuditEvent>> {
    let event_id = format!("keepsake-audit-{}", audit_id.as_uuid());
    let row = sqlx::query(
        "select data from dovecote_events where tenant_id = ? and source = ? and event_id = ?",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(config.source())
    .bind(event_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else { return Ok(None) };

    let data: Option<Vec<u8>> = sqlx::Row::try_get(&row, "data")?;
    data.map(|data| serde_json::from_slice(&data).map_err(RepositoryError::from))
        .transpose()
}

impl<C> TenantSqlxKeepsakeRepository<'_, MySqlBackend, C>
where
    C: RelationCache,
{
    pub(super) async fn enqueue_audit_event_tx(
        &self,
        tx: &mut Transaction<'_, MySql>,
        audit: &keepsake::AuditEvent,
    ) -> RepositoryResult<()> {
        let event = dovecote_event(self.audit, audit)?;
        let tenant_id = dovecote_tenant_id(&self.tenant_id)?;
        dovecote_sqlx_mysql::MySqlDovecote::new((*self.pool).clone())
            .for_tenant(tenant_id)
            .enqueue(tx, event)
            .await
            .map(|_| ())
            .map_err(|error| RepositoryError::DovecoteEnqueue(error.into()))
    }
}

pub(super) async fn relation_for_update_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query(
        r"
        select tenant_id, id, kind, `key`, enabled, expiry_policy
        from keepsake_relation_definitions
        where tenant_id = ? and id = ?
        for update
        ",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(relation_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    relation_from_row(&row)
}

pub(super) async fn active_keepsake_for_subject_relation_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    subject: &SubjectRef,
    relation_id: RelationId,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where tenant_id = ? and subject_kind = ? and subject_id = ? and relation_id = ? and state = 'applied'
        for update
        ",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn keepsake_by_id_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where tenant_id = ? and id = ?
        ",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(keepsake_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let result = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?, updated_at = ?
        where tenant_id = ? and id = ? and state = 'applied'
        ",
    )
    .bind(naive_timestamp(at))
    .bind(naive_timestamp(at))
    .bind(tenant_id.as_str().as_bytes())
    .bind(keepsake_id.to_string())
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() == 0 {
        return Ok(None);
    }
    keepsake_by_id_tx(tx, tenant_id, keepsake_id).await
}

pub(super) async fn revoke_by_subject_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    subject: &SubjectRef,
    relation_id: RelationId,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let Some(existing) =
        active_keepsake_for_subject_relation_tx(tx, tenant_id, subject, relation_id).await?
    else {
        return Ok(None);
    };
    let result = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?, updated_at = ?
        where tenant_id = ? and subject_kind = ? and subject_id = ? and relation_id = ? and state = 'applied'
        ",
    )
    .bind(naive_timestamp(at))
    .bind(naive_timestamp(at))
    .bind(tenant_id.as_str().as_bytes())
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() == 0 {
        return Ok(None);
    }
    keepsake_by_id_tx(tx, tenant_id, existing.id()).await
}
