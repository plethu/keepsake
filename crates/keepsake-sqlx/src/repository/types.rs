use chrono::{DateTime, Utc};
use keepsake::{Keepsake, RelationDefinition};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

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
            subject_kind: keepsake.subject().kind.clone(),
            subject_id: keepsake.subject().id.clone(),
            keepsake_id: keepsake.id(),
        }
    }
}

/// Result of an apply operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedKeepsake {
    /// Created keepsake, or the existing active keepsake for duplicate applies.
    pub keepsake: Keepsake,
    /// Whether a duplicate active keepsake was prevented.
    pub duplicate_prevented: bool,
}

/// Active keepsake with its relation definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveRelation {
    /// Active keepsake.
    pub keepsake: Keepsake,
    /// Stored relation definition for the keepsake.
    pub relation: RelationDefinition,
}

/// Due timed expiry candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromRow)]
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
    pub due_at: DateTime<Utc>,
}
