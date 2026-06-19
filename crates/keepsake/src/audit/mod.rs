//! Durable audit event contracts.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::evaluation::DecisionKind;
use crate::model::{ActorRef, ExpiryCause, KeepsakeId, RelationId, SubjectRef};

#[cfg(any(test, feature = "test"))]
mod memory;

#[cfg(any(test, feature = "test"))]
pub use memory::{InMemoryAuditError, InMemoryAuditSink};

/// Durable audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Event category written to append-only audit storage.
    pub event_type: AuditEventType,
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
    /// Lifecycle decision that was committed or observed.
    pub decision: AuditDecision,
    /// Application audit context carried alongside the durable event.
    pub context: AuditContext,
}

/// Append-only audit event category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    /// A relation was applied.
    Apply,
    /// A duplicate active apply was prevented.
    DuplicateApply,
    /// A relation was explicitly revoked.
    Revoke,
    /// A timed expiry transition was committed.
    TimedExpiry,
    /// A fulfillment expiry transition was committed.
    FulfillmentExpiry,
}

impl AuditEventType {
    /// Returns the stable storage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::DuplicateApply => "duplicate_apply",
            Self::Revoke => "revoke",
            Self::TimedExpiry => "timed_expiry",
            Self::FulfillmentExpiry => "fulfillment_expiry",
        }
    }
}

/// Audited lifecycle decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuditDecision {
    /// An apply command created or found an active keepsake.
    Applied {
        /// Whether an existing active keepsake was returned instead of inserting.
        duplicate_prevented: bool,
    },
    /// A revoke command transitioned an active keepsake.
    Revoked,
    /// An expiry worker transitioned an active keepsake.
    Expired {
        /// Terminal expiry cause.
        cause: ExpiryCause,
    },
    /// A pure lifecycle evaluation decision was recorded.
    Evaluated {
        /// Evaluation decision.
        decision: DecisionKind,
    },
}

/// Application audit context carried alongside a durable audit event.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditContext {
    /// Deterministic application attributes such as request id, trace id, or source.
    pub attributes: BTreeMap<String, String>,
}

/// Result alias for audit sink operations.
pub type AuditResult<T, E> = core::result::Result<T, E>;

/// Append-only audit sink.
pub trait AuditSink: Send + Sync {
    /// Sink-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Records an audit event after a transition is committed.
    fn record(&self, event: AuditEvent) -> AuditResult<(), Self::Error>;
}

/// Audit sink that discards events.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopAuditSink;

impl AuditSink for NoopAuditSink {
    type Error = Infallible;

    fn record(&self, _event: AuditEvent) -> AuditResult<(), Self::Error> {
        Ok(())
    }
}
