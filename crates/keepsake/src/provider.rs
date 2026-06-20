//! Provider traits for persistence and fulfillment snapshots.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use crate::command::{ApplyKeepsake, RevokeKeepsake};
use crate::model::{
    ActiveRelation, FulfillmentSnapshot, Keepsake, KeepsakeId, RelationId, RelationKey, SubjectRef,
};

#[cfg(any(test, feature = "test"))]
mod memory;

#[cfg(any(test, feature = "test"))]
pub use memory::{ActiveRelationSeed, InMemoryActiveRelations, InMemoryActiveRelationsError};

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

/// Async read-side boundary for active relation state.
///
/// Prefer generic callers such as `S: ActiveRelationSource` for library and
/// adapter code. Use [`DynActiveRelationSource`] only where application
/// composition needs runtime erasure.
pub trait ActiveRelationSource: Send + Sync {
    /// Source-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Finds active relation memberships for a subject.
    fn active_relations_for_subject<'a>(
        &'a self,
        subject: &'a SubjectRef,
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a;

    /// Finds active relation memberships for a subject, filtered by relation ids.
    fn active_relations_for_subject_by_ids<'a>(
        &'a self,
        subject: &'a SubjectRef,
        relation_ids: &'a [RelationId],
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a;

    /// Finds active relation memberships for a subject, filtered by relation keys.
    fn active_relations_for_subject_by_keys<'a>(
        &'a self,
        subject: &'a SubjectRef,
        keys: &'a [RelationKey],
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a;
}

/// Boxed future returned by erased active relation sources.
pub type DynActiveRelationFuture<'a, E> =
    Pin<Box<dyn Future<Output = ProviderResult<Vec<ActiveRelation>, E>> + Send + 'a>>;

/// Object-safe active relation source for application composition boundaries.
///
/// This trait exists for places that genuinely need heterogeneous runtime
/// storage, such as `Arc<dyn DynActiveRelationSource<Error = E>>`. Prefer
/// [`ActiveRelationSource`] in reusable library APIs.
pub trait DynActiveRelationSource: Send + Sync {
    /// Source-specific error type.
    type Error: Error + Send + Sync + 'static;

    /// Finds active relation memberships for a subject.
    fn active_relations_for_subject<'a>(
        &'a self,
        subject: &'a SubjectRef,
    ) -> DynActiveRelationFuture<'a, Self::Error>;

    /// Finds active relation memberships for a subject, filtered by relation ids.
    fn active_relations_for_subject_by_ids<'a>(
        &'a self,
        subject: &'a SubjectRef,
        relation_ids: &'a [RelationId],
    ) -> DynActiveRelationFuture<'a, Self::Error>;

    /// Finds active relation memberships for a subject, filtered by relation keys.
    fn active_relations_for_subject_by_keys<'a>(
        &'a self,
        subject: &'a SubjectRef,
        keys: &'a [RelationKey],
    ) -> DynActiveRelationFuture<'a, Self::Error>;
}

impl<T> DynActiveRelationSource for T
where
    T: ActiveRelationSource,
{
    type Error = T::Error;

    fn active_relations_for_subject<'a>(
        &'a self,
        subject: &'a SubjectRef,
    ) -> DynActiveRelationFuture<'a, Self::Error> {
        Box::pin(ActiveRelationSource::active_relations_for_subject(
            self, subject,
        ))
    }

    fn active_relations_for_subject_by_ids<'a>(
        &'a self,
        subject: &'a SubjectRef,
        relation_ids: &'a [RelationId],
    ) -> DynActiveRelationFuture<'a, Self::Error> {
        Box::pin(ActiveRelationSource::active_relations_for_subject_by_ids(
            self,
            subject,
            relation_ids,
        ))
    }

    fn active_relations_for_subject_by_keys<'a>(
        &'a self,
        subject: &'a SubjectRef,
        keys: &'a [RelationKey],
    ) -> DynActiveRelationFuture<'a, Self::Error> {
        Box::pin(ActiveRelationSource::active_relations_for_subject_by_keys(
            self, subject, keys,
        ))
    }
}
