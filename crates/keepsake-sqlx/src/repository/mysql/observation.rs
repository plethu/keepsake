use super::lifecycle::relation_for_update_tx;
use super::rows::keepsake_from_row;
use crate::repository::RevokedKeepsake;
use crate::repository::{
    AppliedKeepsake, MySqlBackend, RelationCache, RelationObservation, RepositoryError,
    RepositoryResult, TenantSqlxKeepsakeRepository,
};
use keepsake::{ApplyKeepsake, RelationId, RevokeBySubject, SubjectRef};
use sqlx::{MySql, Transaction};

impl<C: RelationCache> TenantSqlxKeepsakeRepository<'_, MySqlBackend, C> {
    /// Locks and observes persisted history without consulting a cache.
    ///
    /// Locks remain until outer commit/rollback, including when no assignment exists.
    /// Acquire multiple relations in sorted id order before other writes. See the
    /// transactional lifecycle guide for backend isolation and contention limits.
    ///
    /// # Errors
    ///
    /// Returns subject-validation, schema, isolation, missing-definition, storage or invalid-record
    /// errors. Roll back on failure, including failed lock acquisition.
    pub async fn observe_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        subject: &SubjectRef,
        relation_id: RelationId,
    ) -> RepositoryResult<RelationObservation> {
        subject.validate()?;
        super::require_repeatable_read(tx).await?;
        require_transaction_schema(tx).await?;

        let relation = relation_for_update_tx(tx, &self.tenant_id, relation_id).await?;
        let rows = sqlx::query("select tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, fulfilled_at, revoked_at, metadata from keepsakes where tenant_id = ? and subject_kind = ? and subject_id = ? and relation_id = ? order by id for update")
            .bind(self.tenant_id.as_str().as_bytes()).bind(subject.kind()).bind(subject.id()).bind(relation_id.to_string())
            .fetch_all(&mut **tx).await?;
        let history = rows
            .iter()
            .map(keepsake_from_row)
            .collect::<RepositoryResult<Vec<_>>>()?;
        Ok(RelationObservation {
            tenant_id: self.tenant_id.clone(),
            subject: subject.clone(),
            relation,
            history,
        })
    }

    /// Revalidates exact scoped evidence and holds its locks through the caller's write.
    ///
    /// # Errors
    ///
    /// Returns `RepositoryError::StaleObservation` when scoped history or the definition changed;
    /// otherwise propagates observation failures. Roll back the caller transaction on failure.
    pub async fn revalidate_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        expected: &RelationObservation,
    ) -> RepositoryResult<()> {
        if expected.tenant_id != self.tenant_id {
            return Err(RepositoryError::StaleObservation);
        }

        let current = self
            .observe_in_transaction(tx, &expected.subject, expected.relation.id)
            .await?;
        if &current != expected {
            return Err(RepositoryError::StaleObservation);
        }
        Ok(())
    }

    /// Applies only against matching scoped history. Roll back on any failure.
    ///
    /// # Errors
    ///
    /// Returns stale-observation or scope errors for a new mutation, or propagates apply/replay
    /// failures. Exact committed replay does not require the original observation to remain current.
    /// Roll back the caller transaction on any error.
    pub async fn apply_if_current_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        expected: &RelationObservation,
        command: &ApplyKeepsake,
    ) -> RepositoryResult<AppliedKeepsake> {
        if command.tenant_id != expected.tenant_id
            || command.subject != expected.subject
            || command.relation_id != expected.relation.id
        {
            return Err(RepositoryError::StaleObservation);
        }
        // Reacquire the scope lock before looking up the immutable command receipt.
        let current = self
            .observe_in_transaction(tx, &expected.subject, expected.relation.id)
            .await?;
        if let Some(receipt) = self
            .replay_apply_in_transaction(tx, command, &current)
            .await?
        {
            return Ok(receipt);
        }
        self.revalidate_in_transaction(tx, expected).await?;
        self.apply_in_transaction(tx, command).await
    }

    /// Revokes only against matching scoped history. Roll back on any failure.
    ///
    /// # Errors
    ///
    /// Returns stale-observation or scope errors for a new mutation, or propagates revoke/replay
    /// failures. Exact committed replay does not require the original observation to remain current.
    /// Roll back the caller transaction on any error.
    pub async fn revoke_if_current_in_transaction(
        &self,
        tx: &mut Transaction<'_, MySql>,
        expected: &RelationObservation,
        command: &RevokeBySubject,
    ) -> RepositoryResult<RevokedKeepsake> {
        if command.tenant_id != expected.tenant_id
            || command.subject != expected.subject
            || command.relation_id != expected.relation.id
        {
            return Err(RepositoryError::StaleObservation);
        }
        self.observe_in_transaction(tx, &expected.subject, expected.relation.id)
            .await?;
        if let Some(id) = self
            .replay_revoke_subject_in_transaction(tx, command)
            .await?
        {
            return Ok(RevokedKeepsake {
                keepsake_id: id,
                replayed: true,
            });
        }
        self.revalidate_in_transaction(tx, expected).await?;
        self.revoke_by_subject_in_transaction(tx, command).await
    }
}

pub(super) async fn require_transaction_schema(
    tx: &mut Transaction<'_, MySql>,
) -> RepositoryResult<()> {
    let track: Option<String> =
        sqlx::query_scalar("select value from keepsake_schema_metadata where `key` = 'api_track'")
            .fetch_optional(&mut **tx)
            .await?;
    let backend: Option<String> =
        sqlx::query_scalar("select value from keepsake_schema_metadata where `key` = 'backend'")
            .fetch_optional(&mut **tx)
            .await?;
    if track.as_deref() != Some("4") || backend.as_deref() != Some("mysql") {
        return Err(RepositoryError::BackendMismatch {
            expected: "mysql Keepsake domain schema 4",
            actual: "unverified transaction schema".to_owned(),
        });
    }
    Ok(())
}
