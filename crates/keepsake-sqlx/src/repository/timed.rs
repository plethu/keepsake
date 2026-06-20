use chrono::{DateTime, Utc};
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
use keepsake::{RelationDefinition, RelationId, RelationSpec};
#[cfg(all(
    any(feature = "postgres", feature = "sqlite", feature = "mysql"),
    feature = "fulfillment-counters"
))]
use uuid::Uuid;

#[cfg(all(
    any(feature = "postgres", feature = "sqlite", feature = "mysql"),
    feature = "fulfillment-counters"
))]
use super::FulfilledExpiryCandidate;
#[cfg(feature = "mysql")]
use super::MySqlBackend;
#[cfg(feature = "postgres")]
use super::PostgresBackend;
#[cfg(feature = "sqlite")]
use super::SqliteBackend;
use super::{KeepsakeSqlxBackend, NoopRelationCache, SqlxKeepsakeRepository};

/// Timestamp-scoped repository view.
///
/// This wrapper does not read the system clock. Callers choose the timestamp once
/// at an operation boundary, then use the forwarding methods to keep related
/// writes and expiry scans on the same deterministic instant.
#[derive(Debug, Clone, Copy)]
pub struct TimedSqlxKeepsakeRepository<'repo, B, C = NoopRelationCache>
where
    B: KeepsakeSqlxBackend,
{
    pub(super) repository: &'repo SqlxKeepsakeRepository<B, C>,
    pub(super) at: DateTime<Utc>,
}

/// Default Postgres timestamp-scoped repository view.
#[cfg(feature = "postgres")]
pub type TimedKeepsakeRepository<'repo, C = NoopRelationCache> =
    TimedSqlxKeepsakeRepository<'repo, PostgresBackend, C>;

/// `SQLite` timestamp-scoped repository view.
#[cfg(feature = "sqlite")]
pub type TimedSqliteKeepsakeRepository<'repo, C = NoopRelationCache> =
    TimedSqlxKeepsakeRepository<'repo, SqliteBackend, C>;

/// `MySQL` timestamp-scoped repository view.
#[cfg(feature = "mysql")]
pub type TimedMySqlKeepsakeRepository<'repo, C = NoopRelationCache> =
    TimedSqlxKeepsakeRepository<'repo, MySqlBackend, C>;

impl<'repo, B, C> TimedSqlxKeepsakeRepository<'repo, B, C>
where
    B: KeepsakeSqlxBackend,
{
    /// Returns the repository backing this timestamp-scoped view.
    #[must_use]
    pub const fn repository(&self) -> &'repo SqlxKeepsakeRepository<B, C> {
        self.repository
    }

    /// Returns the timestamp applied by forwarding methods.
    #[must_use]
    pub const fn timestamp(&self) -> DateTime<Utc> {
        self.at
    }
}

#[cfg(feature = "postgres")]
impl<C> TimedKeepsakeRepository<'_, C>
where
    C: super::RelationCache,
{
    /// Inserts or updates a relation definition using this view's timestamp.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
    ) -> super::RepositoryResult<RelationDefinition> {
        self.repository.upsert_relation(relation, self.at).await
    }

    /// Inserts or updates a typed relation spec using this view's timestamp.
    pub async fn upsert_relation_spec<Spec>(&self) -> super::RepositoryResult<RelationDefinition>
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
    ) -> super::RepositoryResult<bool> {
        self.repository
            .set_relation_enabled(relation_id, enabled, self.at)
            .await
    }

    /// Lists due timed expiry candidates using this view's timestamp.
    pub async fn due_timed_expiry(
        &self,
        limit: i64,
    ) -> super::RepositoryResult<Vec<super::TimedExpiryCandidate>> {
        self.repository.due_timed_expiry(self.at, limit).await
    }

    /// Expires a stable batch of due timed keepsakes using this view's timestamp.
    pub async fn expire_due_timed(&self, limit: i64) -> super::RepositoryResult<u64> {
        self.repository.expire_due_timed(self.at, limit).await
    }

    /// Reads the persisted fulfillment counter snapshot for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> super::RepositoryResult<keepsake::FulfillmentSnapshot> {
        self.repository.fulfillment_snapshot(keepsake_id).await
    }

    /// Lists fulfillment expiry candidates.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> super::RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        self.repository.due_fulfilled_expiry(limit).await
    }

    /// Expires fulfillment-satisfied keepsakes using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn expire_due_fulfilled(&self, limit: i64) -> super::RepositoryResult<u64> {
        self.repository.expire_due_fulfilled(self.at, limit).await
    }

    /// Upserts a simple fulfillment counter projection using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
    ) -> super::RepositoryResult<()> {
        self.repository
            .upsert_counter_projection(keepsake_id, key, value, self.at)
            .await
    }
}

#[cfg(feature = "sqlite")]
impl<C> TimedSqliteKeepsakeRepository<'_, C>
where
    C: super::RelationCache,
{
    /// Inserts or updates a relation definition using this view's timestamp.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
    ) -> super::RepositoryResult<RelationDefinition> {
        self.repository.upsert_relation(relation, self.at).await
    }

    /// Inserts or updates a typed relation spec using this view's timestamp.
    pub async fn upsert_relation_spec<Spec>(&self) -> super::RepositoryResult<RelationDefinition>
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
    ) -> super::RepositoryResult<bool> {
        self.repository
            .set_relation_enabled(relation_id, enabled, self.at)
            .await
    }

    /// Lists due timed expiry candidates using this view's timestamp.
    pub async fn due_timed_expiry(
        &self,
        limit: i64,
    ) -> super::RepositoryResult<Vec<super::TimedExpiryCandidate>> {
        self.repository.due_timed_expiry(self.at, limit).await
    }

    /// Expires a stable batch of due timed keepsakes using this view's timestamp.
    pub async fn expire_due_timed(&self, limit: i64) -> super::RepositoryResult<u64> {
        self.repository.expire_due_timed(self.at, limit).await
    }

    /// Reads the persisted fulfillment counter snapshot for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> super::RepositoryResult<keepsake::FulfillmentSnapshot> {
        self.repository.fulfillment_snapshot(keepsake_id).await
    }

    /// Lists fulfillment expiry candidates.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> super::RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        self.repository.due_fulfilled_expiry(limit).await
    }

    /// Expires fulfillment-satisfied keepsakes using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn expire_due_fulfilled(&self, limit: i64) -> super::RepositoryResult<u64> {
        self.repository.expire_due_fulfilled(self.at, limit).await
    }

    /// Upserts a simple fulfillment counter projection using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
    ) -> super::RepositoryResult<()> {
        self.repository
            .upsert_counter_projection(keepsake_id, key, value, self.at)
            .await
    }
}

#[cfg(feature = "mysql")]
impl<C> TimedMySqlKeepsakeRepository<'_, C>
where
    C: super::RelationCache,
{
    /// Inserts or updates a relation definition using this view's timestamp.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
    ) -> super::RepositoryResult<RelationDefinition> {
        self.repository.upsert_relation(relation, self.at).await
    }

    /// Inserts or updates a typed relation spec using this view's timestamp.
    pub async fn upsert_relation_spec<Spec>(&self) -> super::RepositoryResult<RelationDefinition>
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
    ) -> super::RepositoryResult<bool> {
        self.repository
            .set_relation_enabled(relation_id, enabled, self.at)
            .await
    }

    /// Lists due timed expiry candidates using this view's timestamp.
    pub async fn due_timed_expiry(
        &self,
        limit: i64,
    ) -> super::RepositoryResult<Vec<super::TimedExpiryCandidate>> {
        self.repository.due_timed_expiry(self.at, limit).await
    }

    /// Expires a stable batch of due timed keepsakes using this view's timestamp.
    pub async fn expire_due_timed(&self, limit: i64) -> super::RepositoryResult<u64> {
        self.repository.expire_due_timed(self.at, limit).await
    }

    /// Reads the persisted fulfillment counter snapshot for a keepsake.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn fulfillment_snapshot(
        &self,
        keepsake_id: Uuid,
    ) -> super::RepositoryResult<keepsake::FulfillmentSnapshot> {
        self.repository.fulfillment_snapshot(keepsake_id).await
    }

    /// Lists fulfillment expiry candidates.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn due_fulfilled_expiry(
        &self,
        limit: i64,
    ) -> super::RepositoryResult<Vec<FulfilledExpiryCandidate>> {
        self.repository.due_fulfilled_expiry(limit).await
    }

    /// Expires fulfillment-satisfied keepsakes using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn expire_due_fulfilled(&self, limit: i64) -> super::RepositoryResult<u64> {
        self.repository.expire_due_fulfilled(self.at, limit).await
    }

    /// Upserts a simple fulfillment counter projection using this view's timestamp.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
    ) -> super::RepositoryResult<()> {
        self.repository
            .upsert_counter_projection(keepsake_id, key, value, self.at)
            .await
    }
}
