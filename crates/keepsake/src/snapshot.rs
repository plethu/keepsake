//! Scoped read evidence, including explicitly scoped absence.
use crate::{ActiveRelation, KeepsakeError, RelationId, Result, SubjectRef, TenantId};

/// A relation snapshot whose scope remains attached even when it is absent.
///
/// The constructor validates scope consistency, not storage authenticity,
/// completeness or concurrency protection. Applications must obtain snapshots
/// from their trusted storage boundary and retain transaction locks for protected
/// writes. Cloning a snapshot does not extend its freshness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationSnapshot {
    tenant_id: TenantId,
    subject: SubjectRef,
    relation_id: RelationId,
    active: Option<ActiveRelation>,
}

impl RelationSnapshot {
    /// Creates a snapshot and checks any present assignment against its scope.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle-model error when a present assignment has a different tenant,
    /// subject or relation from the declared scope.
    pub fn new(
        tenant_id: TenantId,
        subject: SubjectRef,
        relation_id: RelationId,
        active: Option<ActiveRelation>,
    ) -> Result<Self> {
        if let Some(assignment) = &active {
            let stored = assignment.keepsake();
            if stored.tenant_id() != &tenant_id
                || stored.subject() != &subject
                || stored.relation_id() != relation_id
            {
                return Err(KeepsakeError::InvalidKeepsakeLifecycle {
                    reason: "relation snapshot scope mismatch",
                });
            }
        }
        Ok(Self {
            tenant_id,
            subject,
            relation_id,
            active,
        })
    }

    /// Returns the tenant observed, including for absence.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the subject observed, including for absence.
    #[must_use]
    pub const fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    /// Returns the relation observed, including for absence.
    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    /// Returns persisted active state. Effective expiry requires evaluation.
    #[must_use]
    pub const fn active(&self) -> Option<&ActiveRelation> {
        self.active.as_ref()
    }
}
