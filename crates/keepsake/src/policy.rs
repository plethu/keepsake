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
                let mut matching_entries = snapshot
                    .checklist
                    .iter()
                    .filter(|(key, _)| key.starts_with(list_key))
                    .map(|(_, complete)| complete);
                let Some(first) = matching_entries.next() else {
                    return false;
                };

                *first && matching_entries.all(|complete| *complete)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_threshold_must_be_positive() {
        let policy = FulfillmentPolicy::CounterAtLeast {
            key: "steps".to_owned(),
            threshold: 0,
        };

        assert_eq!(
            policy.validate(),
            Err(KeepsakeError::InvalidFulfillmentThreshold)
        );
    }

    #[test]
    fn counter_policy_uses_the_named_counter_and_inclusive_threshold() {
        let policy = FulfillmentPolicy::CounterAtLeast {
            key: "steps".to_owned(),
            threshold: 3,
        };

        assert!(!policy.is_fulfilled(&FulfillmentSnapshot::empty()));
        assert!(
            !policy.is_fulfilled(
                &FulfillmentSnapshot::empty()
                    .with_counter("other", 10)
                    .with_counter("steps", 2)
            )
        );
        assert!(policy.is_fulfilled(&FulfillmentSnapshot::empty().with_counter("steps", 3)));
    }

    #[test]
    fn checklist_requires_at_least_one_matching_complete_item() {
        let policy = FulfillmentPolicy::ChecklistComplete {
            list_key: "onboarding.".to_owned(),
        };

        assert!(!policy.is_fulfilled(&FulfillmentSnapshot::empty()));
        assert!(!policy.is_fulfilled(&FulfillmentSnapshot::empty().with_check("other.done", true)));
        assert!(
            !policy.is_fulfilled(
                &FulfillmentSnapshot::empty()
                    .with_check("onboarding.profile", true)
                    .with_check("onboarding.terms", false)
            )
        );
        assert!(
            policy.is_fulfilled(
                &FulfillmentSnapshot::empty()
                    .with_check("onboarding.profile", true)
                    .with_check("onboarding.terms", true)
            )
        );
    }
}
