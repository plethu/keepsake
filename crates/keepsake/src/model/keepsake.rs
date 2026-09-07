use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::Result;
use crate::policy::ExpiryPolicy;

use super::{KeepsakeId, RelationDefinition, RelationId, SubjectRef, TenantId};

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
        #[serde(with = "time::serde::rfc3339")]
        revoked_at: OffsetDateTime,
    },
    /// The relation expired by policy.
    Expired {
        /// Expiry timestamp.
        #[serde(with = "time::serde::rfc3339")]
        expired_at: OffsetDateTime,
        /// Expiry cause.
        cause: ExpiryCause,
    },
}

/// Policy-bearing relation assignment from an opaque subject to a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keepsake {
    /// Tenant that owns this keepsake.
    tenant_id: TenantId,
    /// Stable keepsake id.
    id: KeepsakeId,
    /// Application-owned subject reference.
    subject: SubjectRef,
    /// Relation definition id.
    relation_id: RelationId,
    /// Policy copied at apply time for deterministic replay.
    expiry: ExpiryPolicy,
    /// Timestamp when the keepsake was applied.
    applied_at: OffsetDateTime,
    /// Lifecycle-specific state.
    lifecycle: KeepsakeLifecycle,
    /// Application metadata kept opaque by Keepsake.
    metadata: BTreeMap<String, String>,
}

impl Keepsake {
    /// Creates a new active keepsake.
    ///
    /// # Errors
    ///
    /// Returns invalid subject, policy or lifecycle errors.
    pub fn applied(
        id: KeepsakeId,
        subject: SubjectRef,
        relation: &RelationDefinition,
        applied_at: OffsetDateTime,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self> {
        subject.validate()?;
        relation.expiry.validate()?;
        Ok(Self {
            tenant_id: relation.tenant_id.clone(),
            id,
            subject,
            relation_id: relation.id,
            expiry: relation.expiry.clone(),
            applied_at,
            lifecycle: KeepsakeLifecycle::Applied,
            metadata,
        })
    }

    /// Builds an assignment from a command and its stored definition.
    ///
    /// Validates tenant, relation and assignment policy at the mutation boundary.
    ///
    /// # Errors
    ///
    /// Returns invalid command/definition, tenant/relation mismatch, disabled-definition
    /// or lifecycle errors before constructing the assignment.
    pub fn from_apply(
        command: &crate::ApplyKeepsake,
        relation: &RelationDefinition,
    ) -> Result<Self> {
        if command.tenant_id != relation.tenant_id {
            return Err(crate::KeepsakeError::TenantMismatch {
                expected: relation.tenant_id.clone(),
                actual: command.tenant_id.clone(),
            });
        }

        if command.relation_id != relation.id {
            return Err(crate::KeepsakeError::ActiveRelationMismatch {
                keepsake_relation_id: command.relation_id,
                relation_id: relation.id,
            });
        }

        if !relation.enabled {
            return Err(crate::KeepsakeError::RelationDisabled {
                relation_id: relation.id,
            });
        }

        command.context.validate()?;
        let mut assignment = Self::applied(
            command.id,
            command.subject.clone(),
            relation,
            command.at,
            command.metadata.clone(),
        )?;
        if let Some(expiry) = &command.expiry {
            expiry.validate()?;
            assignment.expiry = expiry.clone();
        }

        Ok(assignment)
    }

    /// Returns the owning tenant identity.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
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
    pub const fn applied_at(&self) -> OffsetDateTime {
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
    pub const fn expires_at(&self) -> Option<OffsetDateTime> {
        self.expiry.timed_expiry()
    }

    /// Returns the terminal timestamp for revoked or expired keepsakes.
    #[must_use]
    pub const fn ended_at(&self) -> Option<OffsetDateTime> {
        match self.lifecycle {
            KeepsakeLifecycle::Applied => None,
            KeepsakeLifecycle::Revoked { revoked_at } => Some(revoked_at),
            KeepsakeLifecycle::Expired { expired_at, .. } => Some(expired_at),
        }
    }

    /// Returns the revocation timestamp for revoked keepsakes.
    #[must_use]
    pub const fn revoked_at(&self) -> Option<OffsetDateTime> {
        match self.lifecycle {
            KeepsakeLifecycle::Revoked { revoked_at } => Some(revoked_at),
            KeepsakeLifecycle::Applied | KeepsakeLifecycle::Expired { .. } => None,
        }
    }

    /// Returns the expiry timestamp for expired keepsakes.
    #[must_use]
    pub const fn expired_at(&self) -> Option<OffsetDateTime> {
        match self.lifecycle {
            KeepsakeLifecycle::Expired { expired_at, .. } => Some(expired_at),
            KeepsakeLifecycle::Applied | KeepsakeLifecycle::Revoked { .. } => None,
        }
    }

    /// Returns the fulfillment timestamp for fulfillment-caused expiry.
    #[must_use]
    pub const fn fulfilled_at(&self) -> Option<OffsetDateTime> {
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
