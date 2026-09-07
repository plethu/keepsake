use super::{AuditDecision, AuditEvent, AuditEventType, LifecycleCommand};
use crate::{CommandContext, KeepsakeError, Result};

impl AuditEvent {
    /// Validates a complete command occurrence against its immutable envelope.
    ///
    /// Legacy events without a command remain readable, but are not exact retry
    /// receipts. Storage adapters must call this before append and after decode.
    /// This does not authenticate the actor or revalidate its current authority.
    ///
    /// # Errors
    ///
    /// Returns a validation or inconsistent-occurrence error when the captured command
    /// does not match the event identity, scope, time, actor or decision.
    pub fn validate_command(&self) -> Result<()> {
        let Some(command) = &self.command else {
            return Ok(());
        };

        let (tenant, id, at, context) = match command {
            LifecycleCommand::Apply(command) => {
                command.subject.validate()?;
                if let Some(expiry) = &command.expiry {
                    expiry.validate()?;
                }

                let coherent = match (&self.event_type, &self.decision) {
                    (
                        AuditEventType::Apply,
                        AuditDecision::Applied {
                            duplicate_prevented: false,
                        },
                    ) => self.keepsake_id == command.id,
                    (
                        AuditEventType::DuplicateApply,
                        AuditDecision::Applied {
                            duplicate_prevented: true,
                        },
                    ) => true,
                    _ => false,
                };

                if !coherent
                    || self.subject != command.subject
                    || self.relation_id != command.relation_id
                {
                    return Err(inconsistent_command());
                }
                (
                    &command.tenant_id,
                    command.audit_id,
                    command.at,
                    &command.context,
                )
            }
            LifecycleCommand::Revoke(command) => {
                if self.event_type != AuditEventType::Revoke
                    || self.decision != AuditDecision::Revoked
                    || self.keepsake_id != command.keepsake_id
                {
                    return Err(inconsistent_command());
                }
                (
                    &command.tenant_id,
                    command.audit_id,
                    command.at,
                    &command.context,
                )
            }
            LifecycleCommand::RevokeBySubject(command) => {
                command.subject.validate()?;
                if self.event_type != AuditEventType::Revoke
                    || self.decision != AuditDecision::Revoked
                    || self.subject != command.subject
                    || self.relation_id != command.relation_id
                {
                    return Err(inconsistent_command());
                }
                (
                    &command.tenant_id,
                    command.audit_id,
                    command.at,
                    &command.context,
                )
            }
        };
        context.validate()?;
        if &self.tenant_id != tenant
            || self.id != id
            || self.at != at
            || self.actor != context.actor
            || !self.context_matches(context)
        {
            return Err(inconsistent_command());
        }

        Ok(())
    }

    fn context_matches(&self, command: &CommandContext) -> bool {
        let mut attributes = command.metadata.clone();
        if let Some(key) = &command.idempotency_key {
            attributes
                .entry("idempotency_key".to_owned())
                .or_insert_with(|| key.clone());
        }
        self.context.attributes == attributes
    }
}

const fn inconsistent_command() -> KeepsakeError {
    KeepsakeError::InvalidKeepsakeLifecycle {
        reason: "audit command does not match occurrence",
    }
}
