use super::{RepositoryError, RepositoryResult};
use keepsake::{ExpiryPolicy, Keepsake};
use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(feature = "postgres")]
use sqlx::{Row, postgres::PgRow};

/// Keyset cursor for active relation membership scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipCursor {
    /// Last seen subject kind.
    pub subject_kind: String,
    /// Last seen subject id.
    pub subject_id: String,
    /// Last seen keepsake id.
    pub keepsake_id: Uuid,
}

impl MembershipCursor {
    /// Creates a cursor positioned after a returned keepsake.
    #[must_use]
    pub fn after(keepsake: &Keepsake) -> Self {
        Self {
            subject_kind: keepsake.subject().kind().to_owned(),
            subject_id: keepsake.subject().id().to_owned(),
            keepsake_id: keepsake.id(),
        }
    }
}

/// Result of an apply operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedKeepsake {
    /// Whether the exact command already committed and no new lifecycle effect occurred.
    pub replayed: bool,
    /// Created keepsake, or the existing active keepsake for duplicate applies.
    pub keepsake: Keepsake,
    /// Whether a duplicate active keepsake was prevented.
    pub duplicate_prevented: bool,
}

/// Due timed expiry candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct TimedExpiryCandidate {
    /// Keepsake id.
    pub keepsake_id: Uuid,
    /// Relation id.
    pub relation_id: Uuid,
    /// Subject kind.
    pub subject_kind: String,
    /// Subject id.
    pub subject_id: String,
    /// Due timestamp.
    #[serde(with = "time::serde::rfc3339")]
    pub due_at: OffsetDateTime,
}

/// Due fulfillment expiry candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FulfilledExpiryCandidate {
    /// Keepsake id.
    pub keepsake_id: Uuid,
    /// Relation id.
    pub relation_id: Uuid,
    /// Subject kind.
    pub subject_kind: String,
    /// Subject id.
    pub subject_id: String,
    /// Copied expiry policy.
    pub expiry_policy: ExpiryPolicy,
}

#[cfg(feature = "postgres")]
impl<'row> sqlx::FromRow<'row, PgRow> for FulfilledExpiryCandidate {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx::Error> {
        let expiry_policy = serde_json::from_value(row.try_get("expiry_policy")?)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok(Self {
            keepsake_id: row.try_get("keepsake_id")?,
            relation_id: row.try_get("relation_id")?,
            subject_kind: row.try_get("subject_kind")?,
            subject_id: row.try_get("subject_id")?,
            expiry_policy,
        })
    }
}

/// Opaque evidence of one tenant, subject and relation's complete persisted history.
///
/// Revalidate inside the transaction that performs the protected business write.
/// Holding this value alone provides no lock or freshness guarantee.
#[derive(Clone, PartialEq, Eq)]
pub struct RelationObservation {
    pub(super) tenant_id: keepsake::TenantId,
    pub(super) subject: keepsake::SubjectRef,
    pub(super) relation: keepsake::RelationDefinition,
    pub(super) history: Vec<Keepsake>,
}

impl RelationObservation {
    /// Converts locked scope evidence into an explicitly scoped effective-state snapshot.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle-model error if a stored active assignment does not match its definition.
    pub fn snapshot(&self) -> RepositoryResult<keepsake::RelationSnapshot> {
        Ok(keepsake::RelationSnapshot::new(
            self.tenant_id.clone(),
            self.subject.clone(),
            self.relation.id,
            self.active_relation()?,
        )?)
    }

    /// Returns the unique persisted applied assignment with its current definition.
    /// Effective expiry must still be evaluated at authoritative observation time.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle-model error if a stored active assignment does not match its definition.
    pub fn active_relation(&self) -> RepositoryResult<Option<keepsake::ActiveRelation>> {
        self.history
            .iter()
            .find(|row| row.state() == keepsake::LifecycleState::Applied)
            .cloned()
            .map(|row| {
                keepsake::ActiveRelation::new(row, self.relation.clone())
                    .map_err(RepositoryError::from)
            })
            .transpose()
    }

    /// Returns complete persisted assignment history, ordered by immutable identity.
    #[must_use]
    pub fn history(&self) -> &[Keepsake] {
        &self.history
    }

    /// Returns the tenant bound to this evidence.
    #[must_use]
    pub const fn tenant_id(&self) -> &keepsake::TenantId {
        &self.tenant_id
    }

    /// Returns the subject bound to this evidence.
    #[must_use]
    pub const fn subject(&self) -> &keepsake::SubjectRef {
        &self.subject
    }

    /// Returns the relation definition bound to this evidence.
    #[must_use]
    pub const fn relation(&self) -> &keepsake::RelationDefinition {
        &self.relation
    }
}

impl fmt::Debug for RelationObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationObservation")
            .field("assignment_count", &self.history.len())
            .finish_non_exhaustive()
    }
}

/// A committed or pending revocation occurrence; outer commit remains caller-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokedKeepsake {
    /// Assignment addressed by this immutable occurrence.
    pub keepsake_id: keepsake::KeepsakeId,
    /// Whether the exact command already committed without a new lifecycle effect.
    pub replayed: bool,
}
