//! Error types for core validation contracts.

/// Result alias for Keepsake operations.
pub type Result<T> = core::result::Result<T, KeepsakeError>;

/// Errors returned by the core model contracts.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum KeepsakeError {
    /// A caller supplied an empty identifier.
    #[error("{field} must not be empty")]
    EmptyIdentifier {
        /// Field name.
        field: &'static str,
    },

    /// A fulfillment policy cannot be satisfied because its threshold is invalid.
    #[error("fulfillment threshold must be positive")]
    InvalidFulfillmentThreshold,

    /// A command targets a disabled relation.
    #[error("relation {relation_id} is disabled")]
    RelationDisabled {
        /// Disabled relation id.
        relation_id: uuid::Uuid,
    },

    /// A caller tried to apply a relation that is already active for a subject.
    #[error("subject {subject_kind}/{subject_id} already has active relation {relation_id}")]
    DuplicateActiveKeepsake {
        /// Subject kind.
        subject_kind: String,
        /// Subject id.
        subject_id: String,
        /// Relation id.
        relation_id: uuid::Uuid,
    },

    /// A flat keepsake record did not satisfy lifecycle invariants.
    #[error("invalid keepsake lifecycle: {reason}")]
    InvalidKeepsakeLifecycle {
        /// Validation failure reason.
        reason: &'static str,
    },

    /// An active relation paired a keepsake with the wrong relation definition.
    #[error(
        "active relation keepsake uses relation {keepsake_relation_id}, but definition uses {relation_id}"
    )]
    ActiveRelationMismatch {
        /// Relation id stored on the keepsake.
        keepsake_relation_id: uuid::Uuid,
        /// Relation id stored on the relation definition.
        relation_id: uuid::Uuid,
    },

    /// An active relation was built from a non-active keepsake.
    #[error("active relation keepsake {keepsake_id} is not active")]
    InactiveActiveRelation {
        /// Keepsake id.
        keepsake_id: uuid::Uuid,
    },
}
