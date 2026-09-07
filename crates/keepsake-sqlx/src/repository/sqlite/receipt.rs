use crate::repository::RelationObservation;
use crate::repository::receipt::{apply_receipt, require_command};
use crate::repository::support::DovecoteAuditConfig;
use crate::repository::support::{
    canonical_expiry_policy, canonical_timestamp, decode_current_audit_payload_for_tenant,
};
use crate::repository::{
    AppliedKeepsake, RelationCache, RepositoryError, RepositoryResult, SqliteBackend,
    TenantSqlxKeepsakeRepository,
};
use keepsake::{ApplyKeepsake, KeepsakeId, LifecycleCommand, RevokeBySubject};
use sqlx::{Sqlite, Transaction};

impl<C: RelationCache> TenantSqlxKeepsakeRepository<'_, SqliteBackend, C> {
    pub(in crate::repository) async fn replay_apply_in_transaction(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        command: &ApplyKeepsake,
        observation: &RelationObservation,
    ) -> RepositoryResult<Option<AppliedKeepsake>> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }

        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        command.expiry = command.expiry.map(canonical_expiry_policy);
        let Some(event) =
            existing_audit_event_tx(tx, &self.tenant_id, self.audit, command.audit_id).await?
        else {
            return Ok(None);
        };
        require_command(&event, &LifecycleCommand::Apply(command.clone()))?;
        let assignment = observation
            .history()
            .iter()
            .find(|assignment| assignment.id() == event.keepsake_id)
            .ok_or(RepositoryError::ReceiptEvidenceUnavailable)?;
        Ok(Some(apply_receipt(&event, &command, assignment)?))
    }

    pub(in crate::repository) async fn replay_revoke_subject_in_transaction(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        command: &RevokeBySubject,
    ) -> RepositoryResult<Option<KeepsakeId>> {
        if command.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }

        let mut command = command.clone();
        command.at = canonical_timestamp(command.at);
        let Some(event) =
            existing_audit_event_tx(tx, &self.tenant_id, self.audit, command.audit_id).await?
        else {
            return Ok(None);
        };
        require_command(&event, &LifecycleCommand::RevokeBySubject(command))?;
        Ok(Some(event.keepsake_id))
    }
}

pub(in crate::repository) async fn existing_audit_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    tenant_id: &keepsake::TenantId,
    config: &DovecoteAuditConfig,
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
    data.map(|data| {
        decode_current_audit_payload_for_tenant(&data, tenant_id)
            .map_err(RepositoryError::AuditPayload)
    })
    .transpose()
}
