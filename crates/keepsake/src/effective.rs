//! Effective lifecycle state at an explicitly supplied authoritative observation.
use crate::evaluation::evaluate_policy;
use time::OffsetDateTime;

use crate::{
    ActiveRelation, DecisionKind, FulfillmentSnapshot, LifecycleState, NoopReason, evaluate_active,
};

/// Time evidence supplied by the application's authoritative clock boundary.
///
/// This is an assertion, not clock authentication. Applications must classify
/// stale evidence and retain a high-water mark across restart to reject clock
/// regression. Offset and subsecond precision are retained without truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationTime {
    /// A currently authoritative instant.
    Authoritative(OffsetDateTime),
    /// No authoritative time is available.
    Unknown,
    /// Available evidence is older than the application's freshness budget.
    Stale,
    /// The clock moved behind its previously accepted high-water mark.
    Regressed,
}

impl ObservationTime {
    /// Classifies an instant against an application-owned monotonic high-water mark.
    #[must_use]
    pub fn checked(now: OffsetDateTime, high_water: OffsetDateTime) -> Self {
        if now < high_water {
            Self::Regressed
        } else {
            Self::Authoritative(now)
        }
    }

    /// Returns the authoritative instant or a typed unavailable outcome.
    ///
    /// # Errors
    ///
    /// Returns a typed unavailable outcome for unknown, stale or regressed time.
    pub const fn instant(self) -> Result<OffsetDateTime, EffectiveRelationError> {
        match self {
            Self::Authoritative(at) => Ok(at),
            Self::Unknown => Err(EffectiveRelationError::UnknownTime),
            Self::Stale => Err(EffectiveRelationError::StaleTime),
            Self::Regressed => Err(EffectiveRelationError::ClockRegressed),
        }
    }
}

/// Evidence unavailable for an effective relation decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EffectiveRelationError {
    /// No authoritative time was supplied.
    #[error("authoritative observation time is unavailable")]
    UnknownTime,
    /// Supplied time evidence is stale.
    #[error("authoritative observation time is stale")]
    StaleTime,
    /// Supplied time precedes accepted time or this assignment's application.
    #[error("authoritative observation time regressed")]
    ClockRegressed,
    /// Definition is disabled; lifecycle evaluation is suspended.
    #[error("relation definition is disabled")]
    RelationDisabled,
    /// Policy requires fulfillment evidence that was not supplied.
    #[error("fulfillment evidence is unavailable")]
    FulfillmentMissing,
}

/// Evaluates effective state without writing the durable reconciliation event.
///
/// Disabled definitions and missing evidence do not imply either presence or
/// absence, except that a disabled definition cannot extend a provably expired
/// assignment. The caller should report unavailability and retry when evidence can
/// be obtained. A due timed assignment is expired at the exact deadline even if
/// no reconciliation worker has run. Fulfillment evidence must be scoped and
/// current at the application's evidence boundary. An empty snapshot or one
/// without the policy's named evidence is unavailable, not incomplete fulfillment.
///
/// # Errors
///
/// Returns a typed unavailable outcome for unknown, stale or regressed time, a disabled
/// definition without provable expiry, or missing fulfillment evidence.
pub fn effective_state(
    time: ObservationTime,
    active: &ActiveRelation,
    fulfillment: Option<&FulfillmentSnapshot>,
) -> Result<LifecycleState, EffectiveRelationError> {
    let at = time.instant()?;
    if at < active.keepsake().applied_at() {
        return Err(EffectiveRelationError::ClockRegressed);
    }

    if let crate::ExpiryPolicy::WhenFulfilled { policy } = active.keepsake().expiry()
        && !fulfillment.is_some_and(|snapshot| policy.has_evidence(snapshot))
    {
        return Err(EffectiveRelationError::FulfillmentMissing);
    }

    let decision = evaluate_active(at, active, fulfillment);
    match decision.kind {
        DecisionKind::Noop {
            reason: NoopReason::RelationDisabled,
        } => {
            let policy = evaluate_policy(at, active.keepsake(), fulfillment);
            if policy.resulting_state == LifecycleState::Expired {
                Ok(LifecycleState::Expired)
            } else {
                Err(EffectiveRelationError::RelationDisabled)
            }
        }
        DecisionKind::Noop {
            reason: NoopReason::FulfillmentMissing,
        } => Err(EffectiveRelationError::FulfillmentMissing),
        _ => Ok(decision.resulting_state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, FulfillmentPolicy, Keepsake,
        RelationDefinition, RelationKey, SubjectRef, TenantId,
    };
    use uuid::Uuid;

    #[test]
    fn assignment_deadline_and_unavailable_evidence() -> crate::Result<()> {
        let start = OffsetDateTime::UNIX_EPOCH;
        let deadline = start + time::Duration::hours(1);
        let definition = RelationDefinition::new(
            TenantId::new("tenant")?,
            Uuid::nil(),
            RelationKey::new("restriction", "admission")?,
            true,
            ExpiryPolicy::ManualOnly,
        )?;
        let command = ApplyKeepsake::new(
            definition.tenant_id.clone(),
            SubjectRef::new("user", "a")?,
            definition.id,
            start,
            CommandContext::new(ActorRef::new("staff", "b")?),
        )
        .with_expiry(ExpiryPolicy::At {
            timestamp: deadline,
        });
        let assignment = Keepsake::from_apply(&command, &definition)?;
        let active = ActiveRelation::new(assignment, definition.clone())?;
        assert_eq!(active.keepsake().state(), LifecycleState::Applied);
        assert_eq!(
            effective_state(ObservationTime::Authoritative(deadline), &active, None),
            Ok(LifecycleState::Expired)
        );
        assert_eq!(
            effective_state(
                ObservationTime::Authoritative(deadline - time::Duration::nanoseconds(1)),
                &active,
                None
            ),
            Ok(LifecycleState::Applied)
        );
        for (time, error) in [
            (
                ObservationTime::Unknown,
                EffectiveRelationError::UnknownTime,
            ),
            (ObservationTime::Stale, EffectiveRelationError::StaleTime),
            (
                ObservationTime::checked(start, deadline),
                EffectiveRelationError::ClockRegressed,
            ),
        ] {
            assert_eq!(effective_state(time, &active, None), Err(error));
        }

        let mut disabled = definition.clone();
        disabled.enabled = false;
        let disabled = ActiveRelation::new(active.keepsake().clone(), disabled)?;
        assert_eq!(
            effective_state(ObservationTime::Authoritative(deadline), &disabled, None),
            Ok(LifecycleState::Expired)
        );
        assert_eq!(
            effective_state(ObservationTime::Authoritative(start), &disabled, None),
            Err(EffectiveRelationError::RelationDisabled)
        );
        let command = command.with_expiry(ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "tasks".into(),
                threshold: 1,
            },
        });
        let active = ActiveRelation::new(Keepsake::from_apply(&command, &definition)?, definition)?;
        assert_eq!(
            effective_state(ObservationTime::Authoritative(deadline), &active, None),
            Err(EffectiveRelationError::FulfillmentMissing)
        );
        Ok(())
    }
}

#[cfg(test)]
mod fulfillment_evidence_tests {
    use super::*;
    use crate::{
        ExpiryPolicy, FulfillmentPolicy, Keepsake, RelationDefinition, RelationKey, SubjectRef,
        TenantId,
    };
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn relation(policy: FulfillmentPolicy) -> crate::Result<ActiveRelation> {
        let definition = RelationDefinition::new(
            TenantId::new("tenant")?,
            Uuid::nil(),
            RelationKey::new("restriction", "admission")?,
            true,
            ExpiryPolicy::WhenFulfilled { policy },
        )?;
        ActiveRelation::new(
            Keepsake::applied(
                Uuid::nil(),
                SubjectRef::new("user", "a")?,
                &definition,
                OffsetDateTime::UNIX_EPOCH,
                BTreeMap::new(),
            )?,
            definition,
        )
    }

    #[test]
    fn counter_evidence_distinguishes_missing_zero_and_fulfilled() -> crate::Result<()> {
        let active = relation(FulfillmentPolicy::CounterAtLeast {
            key: "tasks".into(),
            threshold: 1,
        })?;
        let at = ObservationTime::Authoritative(OffsetDateTime::UNIX_EPOCH);
        for snapshot in [
            FulfillmentSnapshot::empty(),
            FulfillmentSnapshot::empty().with_counter("unrelated", 10),
        ] {
            assert_eq!(
                effective_state(at, &active, Some(&snapshot)),
                Err(EffectiveRelationError::FulfillmentMissing)
            );
        }

        let zero = FulfillmentSnapshot::empty().with_counter("tasks", 0);
        assert_eq!(
            effective_state(at, &active, Some(&zero)),
            Ok(LifecycleState::Applied)
        );
        let complete = FulfillmentSnapshot::empty().with_counter("tasks", 1);
        assert_eq!(
            effective_state(at, &active, Some(&complete)),
            Ok(LifecycleState::Expired)
        );
        Ok(())
    }

    #[test]
    fn checklist_evidence_distinguishes_missing_false_and_fulfilled() -> crate::Result<()> {
        let active = relation(FulfillmentPolicy::ChecklistComplete {
            list_key: "tasks/".into(),
        })?;
        let at = ObservationTime::Authoritative(OffsetDateTime::UNIX_EPOCH);
        for snapshot in [
            FulfillmentSnapshot::empty(),
            FulfillmentSnapshot::empty().with_check("unrelated", true),
        ] {
            assert_eq!(
                effective_state(at, &active, Some(&snapshot)),
                Err(EffectiveRelationError::FulfillmentMissing)
            );
        }

        let incomplete = FulfillmentSnapshot::empty().with_check("tasks/first", false);
        assert_eq!(
            effective_state(at, &active, Some(&incomplete)),
            Ok(LifecycleState::Applied)
        );
        let complete = FulfillmentSnapshot::empty().with_check("tasks/first", true);
        assert_eq!(
            effective_state(at, &active, Some(&complete)),
            Ok(LifecycleState::Expired)
        );
        Ok(())
    }
}
