//! Typed lifecycle commands.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::audit::AuditEventId;
use crate::error::Result;
use crate::model::{ActorRef, KeepsakeId, RelationId, RelationSpec, SubjectRef, TenantId};

/// Metadata attached to a command for audit and observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandContext {
    /// Actor responsible for the command.
    pub actor: ActorRef,
    /// Optional idempotency key supplied by the application.
    pub idempotency_key: Option<String>,
    /// Opaque application context.
    pub metadata: BTreeMap<String, String>,
}

impl CommandContext {
    /// Creates a command context for an actor.
    #[must_use]
    pub const fn new(actor: ActorRef) -> Self {
        Self {
            actor,
            idempotency_key: None,
            metadata: BTreeMap::new(),
        }
    }

    /// Adds an idempotency key.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Adds an opaque application metadata attribute.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Validates the command context.
    pub fn validate(&self) -> Result<()> {
        self.actor.validate()
    }
}

/// Applies a relation to a subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyKeepsake {
    /// Tenant that owns the new keepsake.
    pub tenant_id: TenantId,
    /// Caller-supplied keepsake id.
    pub id: KeepsakeId,
    /// Subject to receive the relation.
    pub subject: SubjectRef,
    /// Relation definition id.
    pub relation_id: RelationId,
    /// Command timestamp.
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    /// Opaque application metadata.
    pub metadata: BTreeMap<String, String>,
    /// Audit context.
    pub context: CommandContext,
    /// Stable audit occurrence identity retained across retries.
    pub audit_id: AuditEventId,
}

impl ApplyKeepsake {
    /// Creates an apply command with a generated id.
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        subject: SubjectRef,
        relation_id: RelationId,
        at: OffsetDateTime,
        context: CommandContext,
    ) -> Self {
        Self {
            tenant_id,
            id: Uuid::now_v7(),
            subject,
            relation_id,
            at,
            metadata: BTreeMap::new(),
            context,
            audit_id: AuditEventId::new(),
        }
    }

    /// Creates an apply command for a typed relation spec.
    #[must_use]
    pub fn for_spec<Spec>(
        tenant_id: TenantId,
        subject: SubjectRef,
        at: OffsetDateTime,
        context: CommandContext,
    ) -> Self
    where
        Spec: RelationSpec,
    {
        Self::new(tenant_id, subject, Spec::ID, at, context)
    }

    /// Adds opaque application metadata.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Replaces the generated audit identity when restoring a retryable command.
    #[must_use]
    pub const fn with_audit_id(mut self, audit_id: AuditEventId) -> Self {
        self.audit_id = audit_id;
        self
    }
}

/// Revokes an active keepsake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeKeepsake {
    /// Tenant that owns the keepsake.
    pub tenant_id: TenantId,
    /// Keepsake id.
    pub keepsake_id: KeepsakeId,
    /// Command timestamp.
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    /// Audit context.
    pub context: CommandContext,
    /// Stable audit occurrence identity retained across retries.
    pub audit_id: AuditEventId,
}

impl RevokeKeepsake {
    /// Creates a revoke command.
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        keepsake_id: KeepsakeId,
        at: OffsetDateTime,
        context: CommandContext,
    ) -> Self {
        Self {
            tenant_id,
            keepsake_id,
            at,
            context,
            audit_id: AuditEventId::new(),
        }
    }

    /// Replaces the generated audit identity when restoring a retryable command.
    #[must_use]
    pub const fn with_audit_id(mut self, audit_id: AuditEventId) -> Self {
        self.audit_id = audit_id;
        self
    }
}

/// Revokes the active keepsake for a subject and relation pair.
///
/// This addresses callers that know the `(subject, relation)` pair but not the
/// keepsake id, which is the natural shape for relation-oriented access checks.
/// The active uniqueness invariant guarantees at most one matching keepsake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeBySubject {
    /// Tenant containing the subject and relation.
    pub tenant_id: TenantId,
    /// Subject holding the relation.
    pub subject: SubjectRef,
    /// Relation definition id.
    pub relation_id: RelationId,
    /// Command timestamp.
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    /// Audit context.
    pub context: CommandContext,
    /// Stable audit occurrence identity retained across retries.
    pub audit_id: AuditEventId,
}

impl RevokeBySubject {
    /// Creates a revoke-by-subject command.
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        subject: SubjectRef,
        relation_id: RelationId,
        at: OffsetDateTime,
        context: CommandContext,
    ) -> Self {
        Self {
            tenant_id,
            subject,
            relation_id,
            at,
            context,
            audit_id: AuditEventId::new(),
        }
    }

    /// Replaces the generated audit identity when restoring a retryable command.
    #[must_use]
    pub const fn with_audit_id(mut self, audit_id: AuditEventId) -> Self {
        self.audit_id = audit_id;
        self
    }

    /// Creates a revoke-by-subject command for a typed relation spec.
    #[must_use]
    pub fn for_spec<Spec>(
        tenant_id: TenantId,
        subject: SubjectRef,
        at: OffsetDateTime,
        context: CommandContext,
    ) -> Self
    where
        Spec: RelationSpec,
    {
        Self::new(tenant_id, subject, Spec::ID, at, context)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::model::{ActorRef, StaticRelationKey, SubjectRef};
    use crate::{ExpiryPolicy, RelationSpec};

    struct TrustedTag;

    impl RelationSpec for TrustedTag {
        const ID: Uuid = Uuid::from_u128(1);
        const KEY: StaticRelationKey = StaticRelationKey::new("tag", "trusted");

        fn expiry(_at: time::OffsetDateTime) -> ExpiryPolicy {
            ExpiryPolicy::ManualOnly
        }
    }

    #[test]
    fn command_context_builder_sets_idempotency_and_metadata() -> crate::Result<()> {
        let context = CommandContext::new(ActorRef::new("user", "admin")?)
            .with_idempotency_key("request-1")
            .with_metadata("request_id", "req_123");

        assert_eq!(context.actor, ActorRef::new("user", "admin")?);
        assert_eq!(context.idempotency_key.as_deref(), Some("request-1"));
        assert_eq!(
            context.metadata.get("request_id").map(String::as_str),
            Some("req_123")
        );
        Ok(())
    }

    #[test]
    fn apply_builder_attaches_metadata() -> crate::Result<()> {
        let command = ApplyKeepsake::new(
            crate::TenantId::new("tenant-a")?,
            SubjectRef::new("account", "acct_123")?,
            Uuid::nil(),
            OffsetDateTime::now_utc(),
            CommandContext::new(ActorRef::new("system", "worker")?),
        )
        .with_metadata("source", "support");

        assert_eq!(
            command.metadata.get("source").map(String::as_str),
            Some("support")
        );
        Ok(())
    }

    #[test]
    fn typed_apply_and_revoke_constructors_set_command_fields() -> crate::Result<()> {
        let at = OffsetDateTime::now_utc();
        let context = CommandContext::new(ActorRef::new("system", "worker")?);
        let apply = ApplyKeepsake::for_spec::<TrustedTag>(
            crate::TenantId::new("tenant-a")?,
            SubjectRef::new("account", "acct_123")?,
            at,
            context.clone(),
        );

        assert_eq!(apply.relation_id, TrustedTag::ID);
        assert_eq!(apply.at, at);
        assert_eq!(apply.context, context);

        let revoke =
            RevokeKeepsake::new(apply.tenant_id.clone(), apply.id, at, apply.context.clone());
        assert_eq!(revoke.keepsake_id, apply.id);
        assert_eq!(revoke.at, at);
        assert_eq!(revoke.context, apply.context);
        Ok(())
    }

    #[test]
    fn revoke_by_subject_constructors_set_command_fields() -> crate::Result<()> {
        let at = OffsetDateTime::now_utc();
        let subject = SubjectRef::new("account", "acct_123")?;
        let context = CommandContext::new(ActorRef::new("user", "moderator")?);

        let tenant_id = crate::TenantId::new("tenant-a")?;
        let by_id = RevokeBySubject::new(
            tenant_id.clone(),
            subject.clone(),
            Uuid::nil(),
            at,
            context.clone(),
        );
        assert_eq!(by_id.subject, subject);
        assert_eq!(by_id.relation_id, Uuid::nil());
        assert_eq!(by_id.at, at);
        assert_eq!(by_id.context, context);

        let by_spec = RevokeBySubject::for_spec::<TrustedTag>(tenant_id, subject, at, context);
        assert_eq!(by_spec.relation_id, TrustedTag::ID);
        Ok(())
    }
}
