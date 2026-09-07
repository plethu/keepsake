use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Snapshot of application-owned fulfillment state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FulfillmentSnapshot {
    /// Numeric counters keyed by policy name.
    pub counters: BTreeMap<String, i64>,
    /// Checklist item completion keyed by item name.
    pub checklist: BTreeMap<String, bool>,
}

impl FulfillmentSnapshot {
    /// Returns an empty snapshot.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Adds a counter value.
    #[must_use]
    pub fn with_counter(mut self, key: impl Into<String>, value: i64) -> Self {
        self.counters.insert(key.into(), value);
        self
    }

    /// Adds a checklist item value.
    #[must_use]
    pub fn with_check(mut self, key: impl Into<String>, complete: bool) -> Self {
        self.checklist.insert(key.into(), complete);
        self
    }
}

/// Fulfillment evidence bound to one tenant-owned assignment incarnation.
///
/// The source attests which assignment it read. This value prevents accidental
/// scope substitution at resolution; it does not authenticate the source, prove
/// checklist completeness, or extend the observation's transaction lifetime.
#[derive(Clone, PartialEq, Eq)]
pub struct FulfillmentEvidence {
    tenant_id: super::TenantId,
    keepsake_id: super::KeepsakeId,
    snapshot: FulfillmentSnapshot,
}

impl FulfillmentEvidence {
    /// Binds a snapshot to the assignment read by a trusted evidence source.
    #[must_use]
    pub const fn new(
        tenant_id: super::TenantId,
        keepsake_id: super::KeepsakeId,
        snapshot: FulfillmentSnapshot,
    ) -> Self {
        Self {
            tenant_id,
            keepsake_id,
            snapshot,
        }
    }

    /// Returns the tenant owning the evidence.
    #[must_use]
    pub const fn tenant_id(&self) -> &super::TenantId {
        &self.tenant_id
    }

    /// Returns the assignment incarnation whose fulfillment was observed.
    #[must_use]
    pub const fn keepsake_id(&self) -> super::KeepsakeId {
        self.keepsake_id
    }

    /// Consumes bound evidence for an existing low-level snapshot interface.
    /// The receiving boundary owns any required scope validation.
    #[must_use]
    pub fn into_snapshot(self) -> FulfillmentSnapshot {
        self.snapshot
    }

    /// Returns the policy evidence for low-level pure evaluation.
    /// Callers must first validate the owning assignment scope.
    #[must_use]
    pub const fn snapshot(&self) -> &FulfillmentSnapshot {
        &self.snapshot
    }
}

impl fmt::Debug for FulfillmentEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FulfillmentEvidence")
            .finish_non_exhaustive()
    }
}
