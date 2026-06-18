//! Durable audit event contracts.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::evaluation::DecisionKind;
use crate::model::{ActorRef, KeepsakeId, RelationId, SubjectRef};

#[cfg(any(test, feature = "test"))]
mod memory;

#[cfg(any(test, feature = "test"))]
pub use memory::{InMemoryAuditError, InMemoryAuditSink};

/// Durable audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Timestamp when the audited change occurred.
    pub at: DateTime<Utc>,
    /// Actor responsible for the change.
    pub actor: ActorRef,
    /// Keepsake id.
    pub keepsake_id: KeepsakeId,
    /// Subject reference.
    pub subject: SubjectRef,
    /// Relation id.
    pub relation_id: RelationId,
    /// Lifecycle decision that was committed.
    pub decision: DecisionKind,
    /// Application audit context carried alongside the durable event.
    pub context: AuditContext,
}

/// Application audit context carried alongside a durable audit event.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditContext {
    /// Deterministic application attributes such as request id, trace id, or source.
    pub attributes: BTreeMap<String, String>,
}

/// Append-only audit sink.
pub trait AuditSink: Send + Sync {
    /// Sink-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Records an audit event after a transition is committed.
    fn record(&self, event: AuditEvent) -> std::result::Result<(), Self::Error>;
}

/// Audit sink that discards events.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopAuditSink;

impl AuditSink for NoopAuditSink {
    type Error = Infallible;

    fn record(&self, _event: AuditEvent) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}
