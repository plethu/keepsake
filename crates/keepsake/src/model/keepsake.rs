use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::policy::ExpiryPolicy;

use super::{KeepsakeId, RelationDefinition, RelationId, SubjectRef};

mod record;

pub use record::KeepsakeRecord;

/// Current lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    /// The relation is currently active.
    Applied,
    /// The relation was explicitly revoked.
    Revoked,
    /// The relation expired by policy.
    Expired,
}

/// Terminal expiry cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryCause {
    /// A fixed timestamp policy became due.
    Timed,
    /// A fulfillment policy became satisfied.
    Fulfilled,
}

/// Lifecycle-specific state carried by a keepsake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum KeepsakeLifecycle {
    /// The relation is currently active.
    Applied,
    /// The relation was explicitly revoked.
    Revoked {
        /// Revocation timestamp.
        revoked_at: DateTime<Utc>,
    },
    /// The relation expired by policy.
    Expired {
        /// Expiry timestamp.
        expired_at: DateTime<Utc>,
        /// Expiry cause.
        cause: ExpiryCause,
    },
}

/// Policy-bearing relation assignment from an opaque subject to a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keepsake {
    /// Stable keepsake id.
    id: KeepsakeId,
    /// Application-owned subject reference.
    subject: SubjectRef,
    /// Relation definition id.
    relation_id: RelationId,
    /// Policy copied at apply time for deterministic replay.
    expiry: ExpiryPolicy,
    /// Timestamp when the keepsake was applied.
    applied_at: DateTime<Utc>,
    /// Lifecycle-specific state.
    lifecycle: KeepsakeLifecycle,
    /// Application metadata kept opaque by Keepsake.
    metadata: BTreeMap<String, String>,
}

impl Keepsake {
    /// Creates a new active keepsake.
    pub fn applied(
        id: KeepsakeId,
        subject: SubjectRef,
        relation: &RelationDefinition,
        applied_at: DateTime<Utc>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self> {
        subject.validate()?;
        relation.expiry.validate()?;
        Ok(Self {
            id,
            subject,
            relation_id: relation.id,
            expiry: relation.expiry.clone(),
            applied_at,
            lifecycle: KeepsakeLifecycle::Applied,
            metadata,
        })
    }

    /// Returns the stable keepsake id.
    #[must_use]
    pub const fn id(&self) -> KeepsakeId {
        self.id
    }

    /// Returns the subject reference.
    #[must_use]
    pub const fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    /// Returns the relation definition id.
    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    /// Returns the copied expiry policy.
    #[must_use]
    pub const fn expiry(&self) -> &ExpiryPolicy {
        &self.expiry
    }

    /// Returns the application timestamp.
    #[must_use]
    pub const fn applied_at(&self) -> DateTime<Utc> {
        self.applied_at
    }

    /// Returns opaque application metadata.
    #[must_use]
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Returns the cheap lifecycle discriminant.
    #[must_use]
    pub const fn state(&self) -> LifecycleState {
        match self.lifecycle {
            KeepsakeLifecycle::Applied => LifecycleState::Applied,
            KeepsakeLifecycle::Revoked { .. } => LifecycleState::Revoked,
            KeepsakeLifecycle::Expired { .. } => LifecycleState::Expired,
        }
    }

    /// Returns the typed lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &KeepsakeLifecycle {
        &self.lifecycle
    }

    /// Returns true when the keepsake is active.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self.lifecycle, KeepsakeLifecycle::Applied)
    }

    /// Returns true when the keepsake is revoked.
    #[must_use]
    pub const fn is_revoked(&self) -> bool {
        matches!(self.lifecycle, KeepsakeLifecycle::Revoked { .. })
    }

    /// Returns true when the keepsake is expired.
    #[must_use]
    pub const fn is_expired(&self) -> bool {
        matches!(self.lifecycle, KeepsakeLifecycle::Expired { .. })
    }

    /// Returns the scheduled timed expiry timestamp, when applicable.
    #[must_use]
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expiry.timed_expiry()
    }

    /// Returns the terminal timestamp for revoked or expired keepsakes.
    #[must_use]
    pub const fn ended_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Applied => None,
            KeepsakeLifecycle::Revoked { revoked_at } => Some(revoked_at),
            KeepsakeLifecycle::Expired { expired_at, .. } => Some(expired_at),
        }
    }

    /// Returns the revocation timestamp for revoked keepsakes.
    #[must_use]
    pub const fn revoked_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Revoked { revoked_at } => Some(revoked_at),
            KeepsakeLifecycle::Applied | KeepsakeLifecycle::Expired { .. } => None,
        }
    }

    /// Returns the expiry timestamp for expired keepsakes.
    #[must_use]
    pub const fn expired_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Expired { expired_at, .. } => Some(expired_at),
            KeepsakeLifecycle::Applied | KeepsakeLifecycle::Revoked { .. } => None,
        }
    }

    /// Returns the fulfillment timestamp for fulfillment-caused expiry.
    #[must_use]
    pub const fn fulfilled_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Expired {
                expired_at,
                cause: ExpiryCause::Fulfilled,
            } => Some(expired_at),
            KeepsakeLifecycle::Applied
            | KeepsakeLifecycle::Revoked { .. }
            | KeepsakeLifecycle::Expired {
                cause: ExpiryCause::Timed,
                ..
            } => None,
        }
    }
}
