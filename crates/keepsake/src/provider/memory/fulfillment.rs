//! In-memory fulfillment snapshots for application and adapter tests.
use crate::provider::{FulfillmentProvider, ProviderResult};
use crate::{FulfillmentSnapshot, Keepsake, KeepsakeId, TenantId};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
/// Error returned by the in-memory fulfillment provider.
#[derive(Debug, thiserror::Error)]
pub enum InMemoryFulfillmentProviderError {
    /// The in-memory fulfillment state lock was poisoned.
    #[error("in-memory fulfillment provider lock poisoned")]
    Poisoned,
}

/// In-memory fulfillment snapshot provider for adapter and application tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryFulfillmentProvider {
    snapshots: Arc<RwLock<BTreeMap<(TenantId, KeepsakeId), FulfillmentSnapshot>>>,
}

impl InMemoryFulfillmentProvider {
    /// Creates an empty in-memory fulfillment provider.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Inserts or replaces a test fulfillment snapshot.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryFulfillmentProviderError::Poisoned` if the snapshot lock was poisoned.
    pub fn insert_snapshot(
        &self,
        tenant_id: TenantId,
        keepsake_id: KeepsakeId,
        snapshot: FulfillmentSnapshot,
    ) -> ProviderResult<(), InMemoryFulfillmentProviderError> {
        self.snapshots
            .write()
            .map_err(|_| InMemoryFulfillmentProviderError::Poisoned)?
            .insert((tenant_id, keepsake_id), snapshot);
        Ok(())
    }
}

impl FulfillmentProvider for InMemoryFulfillmentProvider {
    type Error = InMemoryFulfillmentProviderError;

    fn snapshot(
        &self,
        keepsake: &Keepsake,
    ) -> ProviderResult<Option<FulfillmentSnapshot>, Self::Error> {
        Ok(self
            .snapshots
            .read()
            .map_err(|_| InMemoryFulfillmentProviderError::Poisoned)?
            .get(&(keepsake.tenant_id().clone(), keepsake.id()))
            .cloned())
    }
}
