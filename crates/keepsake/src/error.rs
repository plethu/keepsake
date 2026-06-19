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

    /// A flat keepsake record did not satisfy lifecycle invariants.
    #[error("invalid keepsake lifecycle: {reason}")]
    InvalidKeepsakeLifecycle {
        /// Validation failure reason.
        reason: &'static str,
    },
}
