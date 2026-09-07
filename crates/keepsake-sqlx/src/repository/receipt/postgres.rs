use super::{apply_receipt, require_command};
use crate::repository::RelationObservation;
use crate::repository::support::DovecoteAuditConfig;
use crate::repository::support::{
    canonical_expiry_policy, canonical_timestamp, decode_current_audit_payload_for_tenant,
};
use crate::repository::{
    AppliedKeepsake, PostgresBackend, RelationCache, RepositoryError, RepositoryResult,
    TenantSqlxKeepsakeRepository,
};
use keepsake::{ApplyKeepsake, KeepsakeId, LifecycleCommand, RevokeBySubject};
use sqlx::{Postgres, Transaction};

impl<C: RelationCache> TenantSqlxKeepsakeRepository<'_, PostgresBackend, C> {
    pub(in crate::repository) async fn replay_apply_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
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
        tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &keepsake::TenantId,
    config: &DovecoteAuditConfig,
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
