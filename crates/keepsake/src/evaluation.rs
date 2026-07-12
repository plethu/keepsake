//! Synchronous deterministic lifecycle evaluation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::{
    ActiveRelation, FulfillmentSnapshot, Keepsake, LifecycleState, RelationDefinition,
};
use crate::policy::ExpiryPolicy;

/// Lifecycle evaluation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationDecision {
    /// Result kind.
    pub kind: DecisionKind,
    /// State after applying the decision.
    pub resulting_state: LifecycleState,
}

/// Typed lifecycle decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DecisionKind {
    /// No transition should be written.
    Noop {
        /// Reason no transition was selected.
        reason: NoopReason,
    },
    /// Keepsake should transition.
    Transition {
        /// Transition reason.
        reason: TransitionReason,
        /// Timestamp to write as the transition time.
        at: DateTime<Utc>,
    },
}

/// Reasons for no-op evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoopReason {
    /// The relation definition is disabled.
    RelationDisabled,
    /// The keepsake is already revoked or expired.
    AlreadyTerminal,
    /// Policy requires manual changes only.
    ManualOnly,
    /// A timed policy is not due.
    NotDue,
    /// A fulfillment policy needs a snapshot but none was supplied.
    FulfillmentMissing,
    /// A supplied fulfillment snapshot does not satisfy the policy.
    FulfillmentIncomplete,
}

/// Reasons for lifecycle transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionReason {
    /// Fixed timestamp expiry is due.
    TimedExpiryDue,
    /// Fulfillment policy is satisfied.
    FulfillmentSatisfied,
}

/// Evaluates a validated active relation lifecycle without side effects.
#[must_use]
pub fn evaluate_active(
    now: DateTime<Utc>,
    active: &ActiveRelation,
    fulfillment: Option<&FulfillmentSnapshot>,
) -> EvaluationDecision {
    evaluate(now, active.relation(), active.keepsake(), fulfillment)
}

/// Evaluates a keepsake lifecycle without side effects.
///
/// The relation must be the stored definition for `keepsake`. Prefer
/// [`evaluate_active`] when both values came from an [`ActiveRelationSource`](crate::ActiveRelationSource);
/// that type validates the relation id and active lifecycle state at its boundary.
#[must_use]
pub fn evaluate(
    now: DateTime<Utc>,
    relation: &RelationDefinition,
    keepsake: &Keepsake,
    fulfillment: Option<&FulfillmentSnapshot>,
) -> EvaluationDecision {
    if !relation.enabled {
        return noop(NoopReason::RelationDisabled, keepsake.state());
    }

    if !keepsake.is_active() {
        return noop(NoopReason::AlreadyTerminal, keepsake.state());
    }

    match keepsake.expiry() {
        ExpiryPolicy::ManualOnly => noop(NoopReason::ManualOnly, keepsake.state()),
        ExpiryPolicy::At { timestamp } if now >= *timestamp => transition(
            TransitionReason::TimedExpiryDue,
            *timestamp,
            LifecycleState::Expired,
        ),
        ExpiryPolicy::At { .. } => noop(NoopReason::NotDue, keepsake.state()),
        ExpiryPolicy::WhenFulfilled { policy } => match fulfillment {
            None => noop(NoopReason::FulfillmentMissing, keepsake.state()),
            Some(snapshot) if policy.is_fulfilled(snapshot) => transition(
                TransitionReason::FulfillmentSatisfied,
                now,
                LifecycleState::Expired,
            ),
            Some(_) => noop(NoopReason::FulfillmentIncomplete, keepsake.state()),
        },
    }
}

const fn noop(reason: NoopReason, resulting_state: LifecycleState) -> EvaluationDecision {
    EvaluationDecision {
        kind: DecisionKind::Noop { reason },
        resulting_state,
    }
}

const fn transition(
    reason: TransitionReason,
    at: DateTime<Utc>,
    resulting_state: LifecycleState,
) -> EvaluationDecision {
    EvaluationDecision {
        kind: DecisionKind::Transition { reason, at },
        resulting_state,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    use super::*;
    use crate::model::{ActiveRelation, KeepsakeRecord, RelationKey, SubjectRef};
    use crate::policy::FulfillmentPolicy;

    fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
    }

    type TestResult<T> = core::result::Result<T, TestError>;

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Chrono(#[from] chrono::ParseError),

        #[error(transparent)]
        Keepsake(#[from] crate::KeepsakeError),
    }

    fn relation(expiry: ExpiryPolicy) -> TestResult<RelationDefinition> {
        Ok(RelationDefinition::new(
            Uuid::nil(),
            RelationKey::new("tag", "vip")?,
            true,
            expiry,
        )?)
    }

    fn keepsake(relation: &RelationDefinition) -> TestResult<Keepsake> {
        Ok(Keepsake::applied(
            Uuid::nil(),
            SubjectRef::new("user", "u_1")?,
            relation,
            ts("2026-01-01T00:00:00Z")?,
            BTreeMap::new(),
        )?)
    }

    #[test]
    fn timed_expiry_transitions_when_due() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::At {
            timestamp: ts("2026-01-02T00:00:00Z")?,
        })?;
        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            None,
        );

        assert_eq!(decision.resulting_state, LifecycleState::Expired);
        assert!(matches!(
            decision.kind,
            DecisionKind::Transition {
                reason: TransitionReason::TimedExpiryDue,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn manual_only_never_auto_expires() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::ManualOnly)?;
        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            None,
        );

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::ManualOnly
            }
        );
        Ok(())
    }

    #[test]
    fn fulfillment_snapshot_can_expire() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "messages_sent".to_owned(),
                threshold: 3,
            },
        })?;
        let snapshot = FulfillmentSnapshot::empty().with_counter("messages_sent", 3);

        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            Some(&snapshot),
        );

        assert!(matches!(
            decision.kind,
            DecisionKind::Transition {
                reason: TransitionReason::FulfillmentSatisfied,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn disabled_relation_prevents_transition() -> TestResult<()> {
        let mut relation = relation(ExpiryPolicy::At {
            timestamp: ts("2026-01-02T00:00:00Z")?,
        })?;
        relation.enabled = false;

        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            None,
        );

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::RelationDisabled
            }
        );
        Ok(())
    }

    #[test]
    fn terminal_keepsake_is_not_evaluated_again() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::At {
            timestamp: ts("2026-01-02T00:00:00Z")?,
        })?;
        let mut record = KeepsakeRecord::from(&keepsake(&relation)?);
        record.state = LifecycleState::Expired;
        let terminal = Keepsake::try_from(record)?;

        let decision = evaluate(ts("2026-01-03T00:00:00Z")?, &relation, &terminal, None);

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::AlreadyTerminal
            }
        );
        Ok(())
    }

    #[test]
    fn timed_expiry_before_due_date_is_a_noop() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::At {
            timestamp: ts("2026-01-04T00:00:00Z")?,
        })?;
        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            None,
        );

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::NotDue
            }
        );
        Ok(())
    }

    #[test]
    fn fulfillment_policy_without_snapshot_is_a_noop() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "messages_sent".to_owned(),
                threshold: 3,
            },
        })?;
        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            None,
        );

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::FulfillmentMissing
            }
        );
        Ok(())
    }

    #[test]
    fn incomplete_fulfillment_snapshot_is_a_noop() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "messages_sent".to_owned(),
                threshold: 3,
            },
        })?;
        let snapshot = FulfillmentSnapshot::empty().with_counter("messages_sent", 2);
        let decision = evaluate(
            ts("2026-01-03T00:00:00Z")?,
            &relation,
            &keepsake(&relation)?,
            Some(&snapshot),
        );

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::FulfillmentIncomplete
            }
        );
        Ok(())
    }

    #[test]
    fn validated_active_relation_can_be_evaluated_directly() -> TestResult<()> {
        let relation = relation(ExpiryPolicy::ManualOnly)?;
        let active = ActiveRelation::new(keepsake(&relation)?, relation)?;
        let decision = evaluate_active(ts("2026-01-03T00:00:00Z")?, &active, None);

        assert_eq!(
            decision.kind,
            DecisionKind::Noop {
                reason: NoopReason::ManualOnly
            }
        );
        Ok(())
    }
}
