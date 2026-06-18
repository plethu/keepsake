//! Typed lifecycle commands.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{ActorRef, KeepsakeId, RelationId, SubjectRef};

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
}

/// Applies a relation to a subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyKeepsake {
    /// Caller-supplied keepsake id.
    pub id: KeepsakeId,
    /// Subject to receive the relation.
    pub subject: SubjectRef,
    /// Relation definition id.
    pub relation_id: RelationId,
    /// Command timestamp.
    pub at: DateTime<Utc>,
    /// Opaque application metadata.
    pub metadata: BTreeMap<String, String>,
    /// Audit context.
    pub context: CommandContext,
}

impl ApplyKeepsake {
    /// Creates an apply command with a generated id.
    #[must_use]
    pub fn new(
        subject: SubjectRef,
        relation_id: RelationId,
        at: DateTime<Utc>,
        context: CommandContext,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            subject,
            relation_id,
            at,
            metadata: BTreeMap::new(),
            context,
        }
    }

    /// Adds opaque application metadata.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Revokes an active keepsake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeKeepsake {
    /// Keepsake id.
    pub keepsake_id: KeepsakeId,
    /// Command timestamp.
    pub at: DateTime<Utc>,
    /// Audit context.
    pub context: CommandContext,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::model::{ActorRef, SubjectRef};

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
            SubjectRef::new("account", "acct_123")?,
            Uuid::nil(),
            Utc::now(),
            CommandContext::new(ActorRef::new("system", "worker")?),
        )
        .with_metadata("source", "support");

        assert_eq!(
            command.metadata.get("source").map(String::as_str),
            Some("support")
        );
        Ok(())
    }
}
