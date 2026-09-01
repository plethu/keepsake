//! Error types for core validation contracts.

/// Result alias for Keepsake operations.
pub type Result<T> = core::result::Result<T, KeepsakeError>;

/// Errors returned by the core model contracts.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeepsakeError {
    /// A caller supplied an empty identifier.
    #[error("{field} must not be empty")]
    EmptyIdentifier {
        /// Field name.
        field: &'static str,
    },

    /// A persisted textual identifier exceeded the storage contract's byte limit.
    #[error("{field} is {actual} UTF-8 bytes; maximum is {max}")]
    IdentifierTooLong {
        /// Identifier field.
        field: &'static str,
        /// Maximum permitted UTF-8 byte length.
        max: usize,
        /// Supplied UTF-8 byte length.
        actual: usize,
    },

    /// A persisted textual identifier has leading or trailing whitespace.
    #[error("{field} must not have leading or trailing whitespace")]
    IdentifierWhitespace {
        /// Identifier field.
        field: &'static str,
    },

    /// A persisted textual identifier contained a Unicode control character.
    #[error("{field} contains forbidden control character U+{code_point:04X}")]
    IdentifierControlCharacter {
        /// Identifier field.
        field: &'static str,
        /// Unicode scalar value of the rejected character.
        code_point: u32,
    },

    /// A persisted textual identifier contained a Unicode noncharacter.
    #[error("{field} contains forbidden noncharacter U+{code_point:04X}")]
    IdentifierNoncharacter {
        /// Identifier field.
        field: &'static str,
        /// Unicode scalar value of the rejected character.
        code_point: u32,
    },

    /// Two values from different tenants were combined.
    #[error("tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch {
        /// Tenant required by the owning scope.
        expected: crate::TenantId,
        /// Tenant carried by the value being checked.
        actual: crate::TenantId,
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
