use super::receipt::existing_audit_event_tx;
use crate::repository::RevokedKeepsake;
use crate::repository::receipt::require_command;
use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, LifecycleCommand, RelationDefinition, RelationId,
    RevokeBySubject, RevokeKeepsake, SubjectRef,
};
use sqlx::{MySql, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::repository::support::{
    apply_event, canonical_expiry_policy, canonical_timestamp, dovecote_event, dovecote_tenant_id,
    expires_at, revoke_by_subject_event, revoke_event,
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
    ///
    /// # Errors
    ///
    /// Returns validation, tenant, disabled-definition, command-conflict or receipt-evidence errors,
    /// and propagates database or mandatory audit failures. A commit error may have an unknown outcome;
    /// retry the same command after revalidating application authority.
    pub async fn apply(&self, command: &ApplyKeepsake) -> RepositoryResult<AppliedKeepsake> {
        let mut tx = self.pool.begin().await?;
        let result = self.apply_in_transaction(&mut tx, command).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Executes inside the caller transaction, including the mandatory lifecycle audit.
    ///
    /// Never begins, commits, or rolls back a transaction. On any error or cancellation,
    /// the caller must roll back the entire transaction rather than commit partial effects.
    /// Authenticate and revalidate current authority before exposing replayed receipts.
    ///
    /// # Errors
    ///
    /// Returns validation, tenant, disabled-definition, schema, isolation, command-conflict or
    /// receipt-evidence errors, and propagates database or mandatory audit failures. Roll back
    /// the caller transaction on any error; it may already contain staged effects.
    pub async fn apply_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        command: &ApplyKeepsake,
    ) -> RepositoryResult<AppliedKeepsake> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        command.subject.validate()?;
        command.context.validate()?;
        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        command.expiry = command.expiry.map(canonical_expiry_policy);
        let command = &command;

        let observation = self
            .observe_in_transaction(tx, &command.subject, command.relation_id)
            .await?;
        if let Some(receipt) = self
            .replay_apply_in_transaction(tx, command, &observation)
            .await?
        {
            return Ok(receipt);
        }

        let relation = observation.relation().clone();
        if let Some(active) = observation.active_relation()? {
            let existing = active.keepsake().clone();
            let event = apply_event(command, &existing, true);
            self.enqueue_audit_event_tx(tx, &event).await?;
            return Ok(AppliedKeepsake {
                replayed: false,
                keepsake: existing,
                duplicate_prevented: true,
            });
        }

        if !relation.enabled {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        let assignment = Keepsake::from_apply(command, &relation)?;

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
        .bind(serde_json::to_value(assignment.expiry())?)
        .bind(naive_timestamp(command.at))
        .bind(expires_at(assignment.expiry()).map(naive_timestamp))
        .bind(serde_json::to_value(&command.metadata)?)
        .bind(naive_timestamp(command.at))
        .bind(naive_timestamp(command.at))
        .execute(&mut **tx)
        .await?;

        let keepsake = keepsake_by_id_tx(tx, &self.tenant_id, command.id)
            .await?
            .ok_or(RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            })?;
        self.enqueue_audit_event_tx(tx, &apply_event(command, &keepsake, false))
            .await?;
        Ok(AppliedKeepsake {
            replayed: false,
            keepsake,
            duplicate_prevented: false,
        })
    }

    /// Revokes an active keepsake from a command and records its audit event atomically.
    ///
    /// # Errors
    ///
    /// Returns invalid-context, tenant, schema, isolation or command-replay errors, and propagates
    /// database or mandatory audit failures. An unknown commit outcome requires an exact command retry.
    pub async fn revoke(&self, command: &RevokeKeepsake) -> RepositoryResult<bool> {
        let mut tx = self.pool.begin().await?;
        match self.revoke_in_transaction(&mut tx, command).await {
            Ok(_) => {
                tx.commit().await?;
                Ok(true)
            }
            Err(RepositoryError::MissingActiveAssignment) => {
                tx.rollback().await?;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Executes inside the caller transaction, including the mandatory lifecycle audit.
    ///
    /// Never begins, commits, or rolls back a transaction. On any error or cancellation,
    /// the caller must roll back the entire transaction rather than commit partial effects.
    /// Authenticate and revalidate current authority before exposing replayed receipts.
    ///
    /// # Errors
    ///
    /// Returns validation, tenant, schema, isolation, missing-assignment or command-replay errors,
    /// and propagates database or mandatory audit failures. Roll back the caller transaction
    /// on any error; it may already contain staged effects.
    pub async fn revoke_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        command: &RevokeKeepsake,
    ) -> RepositoryResult<RevokedKeepsake> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        super::observation::require_transaction_schema(tx).await?;
        super::require_repeatable_read(tx).await?;
        command.context.validate()?;
        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        let command = &command;

        if let Some(assignment) =
            keepsake_by_id_tx(tx, &self.tenant_id, command.keepsake_id).await?
        {
            self.observe_in_transaction(tx, assignment.subject(), assignment.relation_id())
                .await?;
        }

        if let Some(event) =
            existing_audit_event_tx(tx, &self.tenant_id, self.audit, command.audit_id).await?
        {
            require_command(&event, &LifecycleCommand::Revoke(command.clone()))?;
            return Ok(RevokedKeepsake {
                keepsake_id: event.keepsake_id,
                replayed: true,
            });
        }

        let revoked = revoke_tx(tx, &self.tenant_id, command.keepsake_id, command.at).await?;
        if let Some(keepsake) = &revoked {
            self.enqueue_audit_event_tx(tx, &revoke_event(command, keepsake))
                .await?;
        }

        let revoked = revoked.ok_or(RepositoryError::MissingActiveAssignment)?;
        Ok(RevokedKeepsake {
            keepsake_id: revoked.id(),
            replayed: false,
        })
    }

    /// Revokes the active keepsake for a subject and relation pair.
    ///
    /// # Errors
    ///
    /// Returns invalid-context, subject, tenant, schema, isolation or command-replay errors,
    /// and propagates database or mandatory audit failures.
    pub async fn revoke_by_subject(
        &self,
        command: &RevokeBySubject,
    ) -> RepositoryResult<Option<KeepsakeId>> {
        let mut tx = self.pool.begin().await?;
        match self
            .revoke_by_subject_in_transaction(&mut tx, command)
            .await
        {
            Ok(receipt) => {
                tx.commit().await?;
                Ok(Some(receipt.keepsake_id))
            }
            Err(RepositoryError::MissingActiveAssignment) => {
                tx.rollback().await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Executes inside the caller transaction, including the mandatory lifecycle audit.
    ///
    /// Never begins, commits, or rolls back a transaction. On any error or cancellation,
    /// the caller must roll back the entire transaction rather than commit partial effects.
    /// Authenticate and revalidate current authority before exposing replayed receipts.
    ///
    /// # Errors
    ///
    /// Returns validation, tenant, schema, isolation, missing-assignment or command-replay errors,
    /// and propagates database or mandatory audit failures. Roll back the caller transaction
    /// on any error; it may already contain staged effects.
    pub async fn revoke_by_subject_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        command: &RevokeBySubject,
    ) -> RepositoryResult<RevokedKeepsake> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        command.subject.validate()?;
        command.context.validate()?;
        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        let command = &command;

        self.observe_in_transaction(tx, &command.subject, command.relation_id)
            .await?;
        if let Some(event) =
            existing_audit_event_tx(tx, &self.tenant_id, self.audit, command.audit_id).await?
        {
            require_command(&event, &LifecycleCommand::RevokeBySubject(command.clone()))?;
            return Ok(RevokedKeepsake {
                keepsake_id: event.keepsake_id,
                replayed: true,
            });
        }

        let revoked = revoke_by_subject_tx(
            tx,
            &self.tenant_id,
            &command.subject,
            command.relation_id,
            command.at,
        )
        .await?;
        let revoked_id = revoked.as_ref().map(Keepsake::id);
        if let Some(keepsake) = &revoked {
            self.enqueue_audit_event_tx(tx, &revoke_by_subject_event(command, keepsake))
                .await?;
        }

        let keepsake_id = revoked_id.ok_or(RepositoryError::MissingActiveAssignment)?;
        Ok(RevokedKeepsake {
            keepsake_id,
            replayed: false,
        })
    }
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
    keepsake_by_id_connection(tx, tenant_id, keepsake_id).await
}

pub(super) async fn keepsake_by_id_connection(
    connection: &mut sqlx::MySqlConnection,
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
    .fetch_optional(connection)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_tx(
    tx: &mut Transaction<'_, MySql>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
    at: OffsetDateTime,
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
    at: OffsetDateTime,
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
