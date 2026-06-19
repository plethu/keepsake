use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use keepsake::{RelationDefinition, RelationId, RelationSpec, SubjectRef};
use uuid::Uuid;

use super::{
    AppliedKeepsake, KeepsakeRepository, NoopRelationCache, RelationCache, RepositoryResult,
    TimedExpiryCandidate,
};

/// Timestamp-scoped repository view.
///
/// This wrapper does not read the system clock. Callers choose the timestamp once
/// at an operation boundary, then use the forwarding methods to keep related
/// writes and expiry scans on the same deterministic instant.
#[derive(Debug, Clone, Copy)]
pub struct TimedKeepsakeRepository<'repo, C = NoopRelationCache> {
    pub(super) repository: &'repo KeepsakeRepository<C>,
    pub(super) at: DateTime<Utc>,
}

impl<'repo, C> TimedKeepsakeRepository<'repo, C>
where
    C: RelationCache,
{
    /// Returns the repository backing this timestamp-scoped view.
    #[must_use]
    pub const fn repository(&self) -> &'repo KeepsakeRepository<C> {
        self.repository
    }

    /// Returns the timestamp applied by forwarding methods.
    #[must_use]
    pub const fn timestamp(&self) -> DateTime<Utc> {
        self.at
    }

    /// Inserts or updates a relation definition using this view's timestamp.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
    ) -> RepositoryResult<RelationDefinition> {
        self.repository.upsert_relation(relation, self.at).await
    }

    /// Inserts or updates a typed relation spec using this view's timestamp.
    pub async fn upsert_relation_spec<Spec>(&self) -> RepositoryResult<RelationDefinition>
    where
        Spec: RelationSpec,
    {
        self.repository.upsert_relation_spec::<Spec>(self.at).await
    }

    /// Enables or disables a relation using this view's timestamp.
    pub async fn set_relation_enabled(
        &self,
        relation_id: RelationId,
        enabled: bool,
    ) -> RepositoryResult<bool> {
        self.repository
            .set_relation_enabled(relation_id, enabled, self.at)
            .await
    }

    /// Applies a keepsake relation idempotently using this view's timestamp.
    pub async fn apply(
        &self,
        subject: &SubjectRef,
        relation_id: RelationId,
        metadata: &BTreeMap<String, String>,
    ) -> RepositoryResult<AppliedKeepsake> {
        self.repository
            .apply(subject, relation_id, self.at, metadata)
            .await
    }

    /// Applies a typed keepsake relation idempotently using this view's timestamp.
    pub async fn apply_spec<Spec>(
        &self,
        subject: &SubjectRef,
        metadata: &BTreeMap<String, String>,
    ) -> RepositoryResult<AppliedKeepsake>
    where
        Spec: RelationSpec,
    {
        self.repository
            .apply_spec::<Spec>(subject, self.at, metadata)
            .await
    }

    /// Applies a keepsake relation with empty metadata using this view's timestamp.
    pub async fn apply_without_metadata(
        &self,
        subject: &SubjectRef,
        relation_id: RelationId,
    ) -> RepositoryResult<AppliedKeepsake> {
        self.repository
            .apply_without_metadata(subject, relation_id, self.at)
            .await
    }

    /// Applies a typed keepsake relation with empty metadata using this view's timestamp.
    pub async fn apply_spec_without_metadata<Spec>(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<AppliedKeepsake>
    where
        Spec: RelationSpec,
    {
        self.repository
            .apply_spec_without_metadata::<Spec>(subject, self.at)
            .await
    }

    /// Revokes an active keepsake using this view's timestamp.
    pub async fn revoke(&self, keepsake_id: Uuid) -> RepositoryResult<bool> {
        self.repository.revoke(keepsake_id, self.at).await
    }

    /// Lists due timed expiry candidates using this view's timestamp.
    pub async fn due_timed_expiry(
        &self,
        limit: i64,
    ) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
        self.repository.due_timed_expiry(self.at, limit).await
    }

    /// Expires a stable batch of due timed keepsakes using this view's timestamp.
    pub async fn expire_due_timed(&self, limit: i64) -> RepositoryResult<u64> {
        self.repository.expire_due_timed(self.at, limit).await
    }

    /// Upserts a simple fulfillment counter projection using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
    ) -> RepositoryResult<()> {
        self.repository
            .upsert_counter_projection(keepsake_id, key, value, self.at)
            .await
    }
}
