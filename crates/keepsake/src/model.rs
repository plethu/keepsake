//! Typed domain model for relation assignments.

mod fulfillment;
mod identity;
mod keepsake;
mod relation;

#[cfg(test)]
mod tests;

pub use fulfillment::FulfillmentSnapshot;
pub use identity::{ActorRef, KeepsakeId, RelationId, SubjectRef};
pub use keepsake::{ExpiryCause, Keepsake, KeepsakeLifecycle, KeepsakeRecord, LifecycleState};
pub use relation::{
    RelationDefinition, RelationKey, RelationKind, RelationName, RelationSpec, StaticRelationKey,
};

use crate::error::{KeepsakeError, Result};

pub(crate) fn validate_not_empty(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(KeepsakeError::EmptyIdentifier { field });
    }
    Ok(())
}
