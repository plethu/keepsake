//! Provider traits for persistence and fulfillment snapshots.

use std::error::Error;

use crate::command::{ApplyKeepsake, RevokeKeepsake};
use crate::model::{FulfillmentSnapshot, Keepsake, KeepsakeId, SubjectRef};

/// Result alias for provider operations.
pub type ProviderResult<T, E> = core::result::Result<T, E>;

/// Application-owned fulfillment snapshot provider.
pub trait FulfillmentProvider: Send + Sync {
    /// Provider-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Returns the current fulfillment snapshot for a keepsake.
    fn snapshot(
        &self,
        keepsake: &Keepsake,
    ) -> ProviderResult<Option<FulfillmentSnapshot>, Self::Error>;
}

/// Persistence boundary for keepsake operations.
pub trait KeepsakeStore: Send + Sync {
    /// Store-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Applies a keepsake.
    fn apply(&self, command: &ApplyKeepsake) -> ProviderResult<Keepsake, Self::Error>;

    /// Revokes a keepsake.
    fn revoke(&self, command: &RevokeKeepsake) -> ProviderResult<Keepsake, Self::Error>;

    /// Finds active keepsakes for a subject.
    fn active_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> ProviderResult<Vec<Keepsake>, Self::Error>;

    /// Finds a keepsake by id.
    fn get(&self, id: KeepsakeId) -> ProviderResult<Option<Keepsake>, Self::Error>;
}
