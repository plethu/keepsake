use super::{AppliedKeepsake, RepositoryError, RepositoryResult};
use keepsake::{
    ApplyKeepsake, AuditDecision, AuditEvent, Keepsake, KeepsakeRecord, LifecycleCommand,
    LifecycleState,
};

pub(super) fn apply_receipt(
    event: &AuditEvent,
    command: &ApplyKeepsake,
    assignment: &Keepsake,
) -> RepositoryResult<AppliedKeepsake> {
    if event.command.as_ref() != Some(&LifecycleCommand::Apply(command.clone())) {
        return Err(RepositoryError::CommandConflict);
    }

    let AuditDecision::Applied {
        duplicate_prevented,
    } = event.decision
    else {
        return Err(RepositoryError::CommandConflict);
    };
    // Immutable assignment fields survive terminal transitions. Restore the original
    // applied receipt without returning terminal state as if it were a new effect.
    let mut record = KeepsakeRecord::from(assignment);
    record.state = LifecycleState::Applied;
    record.revoked_at = None;
    record.fulfilled_at = None;
    Ok(AppliedKeepsake {
        keepsake: Keepsake::try_from(record)?,
        duplicate_prevented,
        replayed: true,
    })
}

pub(super) fn require_command(
    event: &AuditEvent,
    command: &LifecycleCommand,
) -> RepositoryResult<()> {
    if event.command.is_none() {
        return Err(RepositoryError::ReceiptEvidenceUnavailable);
    }

    if event.command.as_ref() == Some(command) {
        Ok(())
    } else {
        Err(RepositoryError::CommandConflict)
    }
}

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub(super) use postgres::existing_audit_event_tx;
