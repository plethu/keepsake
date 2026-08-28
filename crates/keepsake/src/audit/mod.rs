//! Durable audit event contracts.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::evaluation::DecisionKind;
use crate::model::{ActorRef, ExpiryCause, KeepsakeId, RelationId, SubjectRef, TenantId};

#[cfg(any(test, feature = "test"))]
mod memory;

#[cfg(any(test, feature = "test"))]
pub use memory::{InMemoryAuditError, InMemoryAuditSink};

/// Durable audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Tenant that owns the audited keepsake.
    pub tenant_id: TenantId,
    /// Stable identity of this audit occurrence.
    ///
    /// The identity is generated before persistence and must be reused when a
    /// caller retries the same logical operation. It is distinct from any
    /// database row or delivery cursor.
    pub id: AuditEventId,
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

/// Stable, project-owned identity for one audit occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuditEventId(Uuid);

impl AuditEventId {
    /// Generates a time-sortable audit identity before persistence.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID, for example one retained by a retrying caller.
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the UUID representation.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Derives an idempotent identity for a deterministic worker transition.
    #[must_use]
    pub fn deterministic(namespace: &[u8]) -> Self {
        // A small fixed hash keeps this core crate independent of a hashing
        // dependency while making worker retries stable before persistence.
        let mut high = 0xcbf2_9ce4_8422_2325_u64;
        let mut low = 0x8422_2325_cbf2_9ce4_u64;
        for byte in namespace {
            high = (high ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
            low = (low ^ u64::from(byte.rotate_left(1))).wrapping_mul(0x0100_0000_01b3);
        }
        Self(Uuid::from_u128((u128::from(high) << 64) | u128::from(low)))
    }
}

impl Default for AuditEventId {
    fn default() -> Self {
        Self::new()
    }
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

    /// Parses a stable storage label back into an event type.
    ///
    /// This is the inverse of [`Self::as_str`]; unknown labels return `None` so
    /// storage adapters can surface a typed decode error.
    #[must_use]
    pub fn from_storage_label(label: &str) -> Option<Self> {
        match label {
            "apply" => Some(Self::Apply),
            "duplicate_apply" => Some(Self::DuplicateApply),
            "revoke" => Some(Self::Revoke),
            "timed_expiry" => Some(Self::TimedExpiry),
            "fulfillment_expiry" => Some(Self::FulfillmentExpiry),
            _ => None,
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

#[cfg(test)]
mod tests {
    use super::AuditEventType;

    #[test]
    fn event_type_storage_label_round_trips() {
        for event_type in [
            AuditEventType::Apply,
            AuditEventType::DuplicateApply,
            AuditEventType::Revoke,
            AuditEventType::TimedExpiry,
            AuditEventType::FulfillmentExpiry,
        ] {
            assert_eq!(
                AuditEventType::from_storage_label(event_type.as_str()),
                Some(event_type)
            );
        }
        assert_eq!(AuditEventType::from_storage_label("unknown"), None);
    }
}
