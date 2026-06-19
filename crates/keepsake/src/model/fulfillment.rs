use std::collections::BTreeMap;

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
