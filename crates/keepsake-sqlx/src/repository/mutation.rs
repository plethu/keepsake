use super::receipt::existing_audit_event_tx;
use crate::repository::RevokedKeepsake;
use crate::repository::receipt::require_command;
use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, LifecycleCommand, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::PostgresBackend;
use super::support::{
    apply_event, canonical_expiry_policy, canonical_timestamp, dovecote_event, dovecote_tenant_id,
    expires_at, revoke_by_subject_event, revoke_event,
};
use super::{
    AppliedKeepsake, AppliedKeepsakeRow, AppliedKeepsakeWriteRow, RelationCache, RepositoryError,
    RepositoryResult, TenantSqlxKeepsakeRepository,
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
        tx: &mut Transaction<'_, Postgres>,
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
            let keepsake = active.keepsake().clone();
            self.enqueue_audit_event_tx(tx, &apply_event(command, &keepsake, true))
                .await?;
            return Ok(AppliedKeepsake {
                keepsake,
                duplicate_prevented: true,
                replayed: false,
            });
        }

        if !relation.enabled {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        let assignment = Keepsake::from_apply(command, &relation)?;
        let expiry_policy = serde_json::to_value(assignment.expiry())?;
        let expires_at = expires_at(assignment.expiry());
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
        .fetch_one(&mut **tx)
        .await?;

        let (keepsake, duplicate_prevented) = applied.try_into_parts()?;
        let event = apply_event(command, &keepsake, duplicate_prevented);
        self.enqueue_audit_event_tx(tx, &event).await?;
        Ok(AppliedKeepsake {
            replayed: false,
            keepsake,
            duplicate_prevented,
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
        tx: &mut Transaction<'_, Postgres>,
        command: &RevokeKeepsake,
    ) -> RepositoryResult<RevokedKeepsake> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }
        super::observation::require_transaction_schema(tx).await?;
        super::observation::require_read_committed(tx).await?;
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
            let event = revoke_event(command, keepsake);
            self.enqueue_audit_event_tx(tx, &event).await?;
        }

        let revoked = revoked.ok_or(RepositoryError::MissingActiveAssignment)?;
        Ok(RevokedKeepsake {
            keepsake_id: revoked.id(),
            replayed: false,
        })
    }

    /// Revokes the active keepsake for a subject and relation pair.
    ///
    /// Returns the revoked keepsake id, or `None` when no active keepsake exists
    /// for the pair. The active uniqueness invariant guarantees at most one match.
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
        tx: &mut Transaction<'_, Postgres>,
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
            let event = revoke_by_subject_event(command, keepsake);
            self.enqueue_audit_event_tx(tx, &event).await?;
        }

        let keepsake_id = revoked_id.ok_or(RepositoryError::MissingActiveAssignment)?;
        Ok(RevokedKeepsake {
            keepsake_id,
            replayed: false,
        })
    }
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

pub(super) async fn keepsake_by_id_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    keepsake_by_id_connection(tx, tenant_id, keepsake_id).await
}

pub(super) async fn keepsake_by_id_connection(
    connection: &mut sqlx::PgConnection,
    tenant_id: &keepsake::TenantId,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    sqlx::query_as::<_, AppliedKeepsakeRow>("select tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, fulfilled_at, revoked_at, metadata from keepsakes where tenant_id = $1 and id = $2")
        .bind(tenant_id.as_str()).bind(keepsake_id).fetch_optional(connection).await?
        .map(AppliedKeepsakeRow::try_into_keepsake).transpose()
}
