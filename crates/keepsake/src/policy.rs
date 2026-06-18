//! Expiry and fulfillment policy types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{KeepsakeError, Result};
use crate::model::FulfillmentSnapshot;

/// Expiry policy for a keepsake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExpiryPolicy {
    /// Never expires automatically.
    ManualOnly,
    /// Expires at a fixed timestamp.
    At {
        /// Due timestamp.
        timestamp: DateTime<Utc>,
    },
    /// Expires when the referenced fulfillment policy becomes true.
    WhenFulfilled {
        /// Fulfillment rule.
        policy: FulfillmentPolicy,
    },
}

impl ExpiryPolicy {
    /// Returns the timed expiry instant, when this policy is time-based.
    #[must_use]
    pub const fn timed_expiry(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::ManualOnly | Self::WhenFulfilled { .. } => None,
            Self::At { timestamp } => Some(*timestamp),
        }
    }

    /// Validates the policy.
    pub const fn validate(&self) -> Result<()> {
        match self {
            Self::ManualOnly | Self::At { .. } => Ok(()),
            Self::WhenFulfilled { policy } => policy.validate(),
        }
    }
}

/// Fulfillment rule evaluated from a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FulfillmentPolicy {
    /// True when a named counter reaches a threshold.
    CounterAtLeast {
        /// Counter key.
        key: String,
        /// Inclusive threshold.
        threshold: i64,
    },
    /// True when all checklist entries with a prefix are complete.
    ChecklistComplete {
        /// Prefix for checklist entries.
        list_key: String,
    },
}

impl FulfillmentPolicy {
    /// Validates the policy.
    pub const fn validate(&self) -> Result<()> {
        match self {
            Self::CounterAtLeast { threshold, .. } if *threshold <= 0 => {
                Err(KeepsakeError::InvalidFulfillmentThreshold)
            }
            Self::CounterAtLeast { .. } | Self::ChecklistComplete { .. } => Ok(()),
        }
    }

    /// Evaluates the policy against a snapshot.
    #[must_use]
    pub fn is_fulfilled(&self, snapshot: &FulfillmentSnapshot) -> bool {
        match self {
            Self::CounterAtLeast { key, threshold } => snapshot
                .counters
                .get(key)
                .is_some_and(|value| value >= threshold),
            Self::ChecklistComplete { list_key } => {
                let mut matched = false;
                for (key, complete) in &snapshot.checklist {
                    if key.starts_with(list_key) {
                        matched = true;
                        if !complete {
                            return false;
                        }
                    }
                }
                matched
            }
        }
    }
}
