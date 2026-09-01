use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, RelationDefinition, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::PostgresBackend;
use super::support::{
    apply_event, canonical_timestamp, decode_current_audit_payload_for_tenant, dovecote_event,
    dovecote_tenant_id, expires_at, replay_event, revoke_by_subject_event, revoke_event,
};
use super::{
    AppliedKeepsake, AppliedKeepsakeRow, AppliedKeepsakeWriteRow, RelationCache, RelationRow,
    RepositoryError, RepositoryResult, TenantSqlxKeepsakeRepository,
};

impl<C> TenantSqlxKeepsakeRepository<'_, PostgresBackend, C>
where
    C: RelationCache,
{
    /// Applies a command idempotently and records its audit event atomically.
    ///
    /// If an active keepsake already exists for the subject and relation, the existing
    /// row is returned with `duplicate_prevented` set to true, even if the relation
    /// has since been disabled. Disabled relations reject new non-duplicate applies.
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
        let relation = relation_for_share_tx(&mut tx, &self.tenant_id, command.relation_id).await?;
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        let expires_at = expires_at(&relation.expiry);
        let metadata = serde_json::to_value(&command.metadata)?;

        let applied = sqlx::query_as::<_, AppliedKeepsakeWriteRow>(
            r"
            insert into keepsakes
                (tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, metadata, created_at, updated_at)
            select
                $1,
                $2,
                $3,
                $4,
                r.id,
                'applied',
                $5,
                $6,
                $7,
                $8,
                $6,
                $6
            from keepsake_relation_definitions r
            where r.tenant_id = $1 and r.id = $9
            on conflict (tenant_id, subject_kind, subject_id, relation_id) where state = 'applied'
            do update set updated_at = keepsakes.updated_at
            returning tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata, (xmax <> 0) as duplicate_prevented
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(command.id)
        .bind(command.subject.kind())
        .bind(command.subject.id())
        .bind(expiry_policy)
        .bind(command.at)
        .bind(expires_at)
        .bind(metadata)
        .bind(command.relation_id)
        .fetch_one(&mut *tx)
        .await?;

        if !relation.enabled && !applied.duplicate_prevented {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        let (keepsake, duplicate_prevented) = applied.try_into_parts()?;
        let event = replay_event(
            existing_audit_event_tx(&mut tx, &self.tenant_id, self.audit, command.audit_id).await?,
            apply_event(command, &keepsake, duplicate_prevented),
        );
        self.enqueue_audit_event_tx(&mut tx, &event).await?;
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

        let mut tx = self.pool.begin().await?;
        let revoked = revoke_tx(&mut tx, &self.tenant_id, command.keepsake_id, command.at).await?;
        if let Some(keepsake) = &revoked {
            let event = revoke_event(command, keepsake);
            self.enqueue_audit_event_tx(&mut tx, &event).await?;
        }
        tx.commit().await?;
        Ok(revoked.is_some())
    }

    /// Revokes the active keepsake for a subject and relation pair.
    ///
    /// Returns the revoked keepsake id, or `None` when no active keepsake exists
    /// for the pair. The active uniqueness invariant guarantees at most one match.
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
            let event = revoke_by_subject_event(command, keepsake);
            self.enqueue_audit_event_tx(&mut tx, &event).await?;
        }
        tx.commit().await?;
        Ok(revoked_id)
    }
}

async fn existing_audit_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &keepsake::TenantId,
    config: &super::support::DovecoteAuditConfig,
    audit_id: keepsake::AuditEventId,
) -> RepositoryResult<Option<keepsake::AuditEvent>> {
    let event_id = format!("keepsake-audit-{}", audit_id.as_uuid());
    let bytes = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "select data from dovecote_events where tenant_id = $1 and source = $2 and event_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(config.source())
    .bind(event_id)
    .fetch_optional(&mut **tx)
    .await?;
    bytes
        .flatten()
        .map(|data| {
            decode_current_audit_payload_for_tenant(&data, tenant_id)
                .map_err(RepositoryError::AuditPayload)
        })
        .transpose()
}

impl<C> TenantSqlxKeepsakeRepository<'_, PostgresBackend, C>
where
    C: RelationCache,
{
    pub(super) async fn enqueue_audit_event_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event: &keepsake::AuditEvent,
    ) -> RepositoryResult<()> {
        let event = dovecote_event(self.audit, event)?;
        let tenant_id = dovecote_tenant_id(&self.tenant_id)?;
        let adapter = dovecote_sqlx_postgres::PostgresDovecote::new(self.pool.clone());
        adapter
            .for_tenant(tenant_id)
            .enqueue(tx, event)
            .await
            .map(|_| ())
            .map_err(|error| RepositoryError::DovecoteEnqueue(error.into()))
    }
}

async fn revoke_by_subject_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &keepsake::TenantId,
    subject: &SubjectRef,
    relation_id: RelationId,
    at: time::OffsetDateTime,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query_as::<_, AppliedKeepsakeRow>(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = $5, updated_at = $5
        where tenant_id = $1 and subject_kind = $2 and subject_id = $3 and relation_id = $4 and state = 'applied'
        returning tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(tenant_id.as_str())
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id)
    .bind(at)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(AppliedKeepsakeRow::try_into_keepsake).transpose()
}

async fn revoke_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
    at: time::OffsetDateTime,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query_as::<_, AppliedKeepsakeRow>(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = $3, updated_at = $3
        where tenant_id = $1 and id = $2 and state = 'applied'
        returning tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(tenant_id.as_str())
    .bind(keepsake_id)
    .bind(at)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(AppliedKeepsakeRow::try_into_keepsake).transpose()
}

async fn relation_for_share_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &keepsake::TenantId,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query_as::<_, RelationRow>(
        r"
        select tenant_id, id, kind, key, enabled, expiry_policy
        from keepsake_relation_definitions
        where tenant_id = $1 and id = $2
        for share
        ",
    )
    .bind(tenant_id.as_str())
    .bind(relation_id)
    .fetch_one(&mut **tx)
    .await?;
    row.try_into_relation()
}
