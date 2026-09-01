//! Typed domain model for relation assignments.

mod fulfillment;
mod identity;
mod keepsake;
mod relation;

#[cfg(test)]
mod tests;

pub use fulfillment::FulfillmentSnapshot;
pub use identity::{ActorRef, KeepsakeId, RelationId, SubjectRef, TenantId};
pub use keepsake::{ExpiryCause, Keepsake, KeepsakeLifecycle, KeepsakeRecord, LifecycleState};
pub use relation::{
    ActiveRelation, RelationDefinition, RelationKey, RelationKind, RelationName, RelationSpec,
    StaticRelationKey,
};

use crate::error::{KeepsakeError, Result};

/// Maximum UTF-8 byte length for persisted textual identifiers.
pub const MAX_PERSISTED_IDENTIFIER_BYTES: usize = 191;

/// Validates a portable persisted identifier without changing its bytes.
///
/// Successful values are retained exactly as supplied: Keepsake does not trim,
/// normalize, or case-fold them.
pub fn validate_persisted_identifier(field: &'static str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(KeepsakeError::EmptyIdentifier { field });
    }

    if value.trim() != value {
        return Err(KeepsakeError::IdentifierWhitespace { field });
    }

    if value.len() > MAX_PERSISTED_IDENTIFIER_BYTES {
        return Err(KeepsakeError::IdentifierTooLong {
            field,
            max: MAX_PERSISTED_IDENTIFIER_BYTES,
            actual: value.len(),
        });
    }

    for character in value.chars() {
        if character.is_control() {
            return Err(KeepsakeError::IdentifierControlCharacter {
                field,
                code_point: character as u32,
            });
        }

        let code_point = character as u32;
        if (0xFDD0..=0xFDEF).contains(&code_point) || (code_point & 0xFFFF) >= 0xFFFE {
            return Err(KeepsakeError::IdentifierNoncharacter { field, code_point });
        }
    }

    Ok(())
}
