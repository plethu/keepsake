//! Postgres repository implementation.

use std::collections::BTreeMap;

#[cfg(feature = "cache")]
use std::sync::Arc;
#[cfg(feature = "cache")]
use std::sync::RwLock;
#[cfg(feature = "cache")]
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use keepsake::{
    ExpiryPolicy, Keepsake, LifecycleState, RelationDefinition, RelationId, RelationKey,
    RelationSpec, SubjectRef,
};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

#[cfg(feature = "migrations")]
use sqlx::migrate::Migrator;

#[cfg(feature = "migrations")]
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const MAX_BATCH_LIMIT: i64 = 10_000;

/// Result alias for SQL repository operations.
pub type RepositoryResult<T> = std::result::Result<T, RepositoryError>;

/// SQL repository errors.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    /// `SQLx` returned an error.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// Migration failed.
    #[cfg(feature = "migrations")]
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// JSON policy could not be encoded or decoded.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// A Keepsake core model could not be built.
    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),

    /// A command tried to mutate a disabled relation.
    #[error("relation {relation_id} is disabled")]
    RelationDisabled {
        /// Disabled relation id.
        relation_id: Uuid,
    },

    /// A typed relation spec conflicts with an existing natural-key row.
    #[error(
        "relation spec {kind}/{name} expected id {expected_relation_id}, but stored relation uses {stored_relation_id}"
    )]
    RelationSpecIdMismatch {
        /// Relation kind.
        kind: String,
        /// Relation name.
        name: String,
        /// Relation id declared by the typed spec.
        expected_relation_id: Uuid,
        /// Existing stored relation id for the same natural key.
        stored_relation_id: Uuid,
    },

    /// A batch or scan limit was outside the accepted range.
    #[error("limit {limit} is outside the accepted range 1..={max}")]
    InvalidLimit {
        /// Provided limit.
        limit: i64,
        /// Maximum accepted limit.
        max: i64,
    },

    /// A row contained an unknown lifecycle state.
    #[error("unknown lifecycle state {state}")]
    InvalidLifecycleState {
        /// Stored state value.
        state: String,
    },
}

/// `SQLx`-backed keepsake repository.
#[derive(Debug, Clone)]
pub struct KeepsakeRepository<C = NoopRelationCache> {
    pool: PgPool,
    relation_cache: C,
}

impl KeepsakeRepository<NoopRelationCache> {
    /// Creates a repository from a Postgres pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self {
            pool,
            relation_cache: NoopRelationCache,
        }
    }
}

impl<C> KeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Creates a timestamp-scoped repository view.
    ///
    /// Use this at request or job boundaries to keep one explicit clock read while
    /// avoiding repeated timestamp plumbing through related repository calls.
    pub const fn at(&self, at: DateTime<Utc>) -> TimedKeepsakeRepository<'_, C> {
        TimedKeepsakeRepository {
            repository: self,
            at,
        }
    }

    /// Enables relation definition caching for read helper methods.
    #[must_use]
    pub fn with_relation_cache<Next>(self, cache: Next) -> KeepsakeRepository<Next>
    where
        Next: RelationCache,
    {
        KeepsakeRepository {
            pool: self.pool,
            relation_cache: cache,
        }
    }

    /// Enables local in-process relation definition caching for read helper methods.
    #[cfg(feature = "cache")]
    #[must_use]
    pub fn with_local_relation_cache(
        self,
        config: LocalRelationCacheConfig,
    ) -> KeepsakeRepository<LocalRelationCache> {
        self.with_relation_cache(LocalRelationCache::new(config))
    }

    /// Runs embedded migrations.
    #[cfg(feature = "migrations")]
    pub async fn migrate(&self) -> RepositoryResult<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Inserts or updates a relation definition by its natural relation key.
    ///
    /// If a relation already exists for the same kind/name, its stable id is preserved and
    /// the returned definition contains the existing id.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition> {
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            insert into keepsake_relation_definitions
                (id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values ($1, $2, $3, $4, $5, $6, $6)
            on conflict (kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = $6
            returning id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(relation.id)
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(at)
        .fetch_one(&self.pool)
        .await?;
        let relation = row.try_into_relation()?;
        self.relation_cache.store(&relation).await;
        Ok(relation)
    }

    /// Inserts or updates a typed relation spec by its natural relation key.
    pub async fn upsert_relation_spec<Spec>(
        &self,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition>
    where
        Spec: RelationSpec,
    {
        let relation = RelationDefinition::from_spec::<Spec>(at)?;
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            insert into keepsake_relation_definitions
                (id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values ($1, $2, $3, $4, $5, $6, $6)
            on conflict (kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = $6
            where keepsake_relation_definitions.id = excluded.id
            returning id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(relation.id)
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(at)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let stored_relation_id = sqlx::query_scalar::<_, Uuid>(
                r"
                select id
                from keepsake_relation_definitions
                where kind = $1 and key = $2
                ",
            )
            .bind(relation.key.kind())
            .bind(relation.key.name())
            .fetch_one(&mut *tx)
            .await?;
            return Err(RepositoryError::RelationSpecIdMismatch {
                kind: relation.key.kind().to_owned(),
                name: relation.key.name().to_owned(),
                expected_relation_id: relation.id,
                stored_relation_id,
            });
        };

        tx.commit().await?;
        let relation = row.try_into_relation()?;
        self.relation_cache.store(&relation).await;
        Ok(relation)
    }

    /// Looks up a relation definition by stable id.
    pub async fn relation_by_id(
        &self,
        relation_id: RelationId,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_id(relation_id).await {
            return Ok(Some(relation));
        }

        let relation = self.fetch_relation_by_id(relation_id).await?;
        if let Some(relation) = &relation {
            self.relation_cache.store(relation).await;
        }
        Ok(relation)
    }

    /// Looks up a relation definition by its natural relation key.
    pub async fn relation_by_key(
        &self,
        key: &RelationKey,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_key(key).await {
            return Ok(Some(relation));
        }

        let relation = self.fetch_relation_by_key(key).await?;
        if let Some(relation) = &relation {
            self.relation_cache.store(relation).await;
        }
        Ok(relation)
    }

    /// Enables or disables a relation.
    pub async fn set_relation_enabled(
        &self,
        relation_id: RelationId,
        enabled: bool,
        at: DateTime<Utc>,
    ) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_relation_definitions
            set enabled = $2, updated_at = $3
            where id = $1
            ",
        )
        .bind(relation_id)
        .bind(enabled)
        .bind(at)
        .execute(&self.pool)
        .await?;
        let changed = result.rows_affected() == 1;
        if changed {
            self.relation_cache.remove_by_id(relation_id).await;
        }
        Ok(changed)
    }

    async fn fetch_relation_by_id(
        &self,
        relation_id: RelationId,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            select id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = $1
            ",
        )
        .bind(relation_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(RelationRow::try_into_relation).transpose()
    }

    async fn fetch_relation_by_key(
        &self,
        key: &RelationKey,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            select id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = $1 and key = $2
            ",
        )
        .bind(key.kind())
        .bind(key.name())
        .fetch_optional(&self.pool)
        .await?;

        row.map(RelationRow::try_into_relation).transpose()
    }

    /// Applies a keepsake relation idempotently.
    ///
    /// If an active keepsake already exists for the subject and relation, the existing
    /// row is returned with `duplicate_prevented` set to true, even if the relation
    /// has since been disabled. Disabled relations reject new non-duplicate applies.
    pub async fn apply(
        &self,
        subject: &SubjectRef,
        relation_id: RelationId,
        at: DateTime<Utc>,
        metadata: &BTreeMap<String, String>,
    ) -> RepositoryResult<AppliedKeepsake> {
        let mut tx = self.pool.begin().await?;
        let relation = relation_for_share_tx(&mut tx, relation_id).await?;

        let keepsake_id = Uuid::now_v7();
        let metadata = serde_json::to_value(metadata)?;

        let applied = sqlx::query_as::<_, AppliedKeepsakeWriteRow>(
            r"
            insert into keepsakes
                (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, metadata, created_at, updated_at)
            select
                $1,
                $2,
                $3,
                r.id,
                'applied',
                r.expiry_policy,
                $4,
                case
                    when r.expiry_policy->>'type' = 'at'
                    then (r.expiry_policy->>'timestamp')::timestamptz
                    else null
                end,
                $5,
                $4,
                $4
            from keepsake_relation_definitions r
            where r.id = $6
            on conflict (subject_kind, subject_id, relation_id) where state = 'applied'
            do update set updated_at = keepsakes.updated_at
            returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata, (xmax <> 0) as duplicate_prevented
            ",
        )
        .bind(keepsake_id)
        .bind(&subject.kind)
        .bind(&subject.id)
        .bind(at)
        .bind(metadata)
        .bind(relation_id)
        .fetch_one(&mut *tx)
        .await?;

        if !relation.enabled && !applied.duplicate_prevented {
            return Err(RepositoryError::RelationDisabled { relation_id });
        }

        tx.commit().await?;
        let (keepsake, duplicate_prevented) = applied.try_into_parts()?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented,
        })
    }

    /// Applies a typed keepsake relation idempotently.
    pub async fn apply_spec<Spec>(
        &self,
        subject: &SubjectRef,
        at: DateTime<Utc>,
        metadata: &BTreeMap<String, String>,
    ) -> RepositoryResult<AppliedKeepsake>
    where
        Spec: RelationSpec,
    {
        self.apply(subject, Spec::ID, at, metadata).await
    }

    /// Applies a keepsake relation with empty metadata.
    pub async fn apply_without_metadata(
        &self,
        subject: &SubjectRef,
        relation_id: RelationId,
        at: DateTime<Utc>,
    ) -> RepositoryResult<AppliedKeepsake> {
        self.apply(subject, relation_id, at, &BTreeMap::new()).await
    }

    /// Applies a typed keepsake relation with empty metadata.
    pub async fn apply_spec_without_metadata<Spec>(
        &self,
        subject: &SubjectRef,
        at: DateTime<Utc>,
    ) -> RepositoryResult<AppliedKeepsake>
    where
        Spec: RelationSpec,
    {
        self.apply_spec::<Spec>(subject, at, &BTreeMap::new()).await
    }

    /// Revokes an active keepsake.
    pub async fn revoke(&self, keepsake_id: Uuid, at: DateTime<Utc>) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsakes
            set state = 'revoked', revoked_at = $2, updated_at = $2
            where id = $1 and state = 'applied'
            ",
        )
        .bind(keepsake_id)
        .bind(at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Returns active keepsakes for a subject.
    pub async fn active_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<Vec<Keepsake>> {
        let rows = sqlx::query_as::<_, AppliedKeepsakeRow>(
            r"
            select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata
            from keepsakes
            where subject_kind = $1 and subject_id = $2 and state = 'applied'
            order by relation_id, id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(AppliedKeepsakeRow::try_into_keepsake)
            .collect()
    }

    /// Returns active keepsakes for a subject with their relation definitions.
    pub async fn active_relations_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        let rows = sqlx::query_as::<_, ActiveRelationRow>(
            r"
            select
                k.id,
                k.subject_kind,
                k.subject_id,
                k.relation_id,
                k.state,
                k.expiry_policy,
                k.applied_at,
                k.expires_at,
                k.fulfilled_at,
                k.revoked_at,
                k.metadata,
                r.id as relation_definition_id,
                r.kind as relation_kind,
                r.key as relation_key,
                r.enabled as relation_enabled,
                r.expiry_policy as relation_expiry_policy
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.subject_kind = $1 and k.subject_id = $2 and k.state = 'applied'
            order by k.relation_id, k.id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .fetch_all(&self.pool)
        .await?;

        let mut active = Vec::with_capacity(rows.len());
        for row in rows {
            let active_relation = row.try_into_active_relation()?;
            self.relation_cache.store(&active_relation.relation).await;
            active.push(active_relation);
        }
        Ok(active)
    }

    /// Returns active keepsakes for a subject, filtered by relation keys.
    ///
    /// This is the bounded variant of [`Self::active_relations_for_subject`] for
    /// request paths that know the small set of relation keys they care about.
    /// Missing keys are ignored, and disabled relation definitions are still
    /// returned when their keepsake is active.
    pub async fn active_relations_for_subject_by_keys(
        &self,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> RepositoryResult<Vec<ActiveRelation>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let kinds = keys
            .iter()
            .map(|key| key.kind().to_owned())
            .collect::<Vec<String>>();
        let names = keys
            .iter()
            .map(|key| key.name().to_owned())
            .collect::<Vec<String>>();

        let rows = sqlx::query_as::<_, ActiveRelationRow>(
            r"
            with requested_relation_keys(kind, key) as (
                select distinct kind, key
                from unnest($3::text[], $4::text[]) as requested(kind, key)
            )
            select
                k.id,
                k.subject_kind,
                k.subject_id,
                k.relation_id,
                k.state,
                k.expiry_policy,
                k.applied_at,
                k.expires_at,
                k.fulfilled_at,
                k.revoked_at,
                k.metadata,
                r.id as relation_definition_id,
                r.kind as relation_kind,
                r.key as relation_key,
                r.enabled as relation_enabled,
                r.expiry_policy as relation_expiry_policy
            from requested_relation_keys requested
            join keepsake_relation_definitions r
              on r.kind = requested.kind and r.key = requested.key
            join keepsakes k
              on k.relation_id = r.id
             and k.subject_kind = $1
             and k.subject_id = $2
             and k.state = 'applied'
            order by k.relation_id, k.id
            ",
        )
        .bind(&subject.kind)
        .bind(&subject.id)
        .bind(&kinds)
        .bind(&names)
        .fetch_all(&self.pool)
        .await?;

        let mut active = Vec::with_capacity(rows.len());
        for row in rows {
            let active_relation = row.try_into_active_relation()?;
            self.relation_cache.store(&active_relation.relation).await;
            active.push(active_relation);
        }
        Ok(active)
    }

    /// Scans active memberships for a relation in stable order.
    pub async fn active_membership_scan(
        &self,
        relation_id: RelationId,
        limit: i64,
    ) -> RepositoryResult<Vec<Keepsake>> {
        self.active_membership_scan_after(relation_id, None, limit)
            .await
    }

    /// Scans active memberships after a keyset cursor in stable order.
    pub async fn active_membership_scan_after(
        &self,
        relation_id: RelationId,
        after: Option<&MembershipCursor>,
        limit: i64,
    ) -> RepositoryResult<Vec<Keepsake>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query_as::<_, AppliedKeepsakeRow>(
            r"
            select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata
            from keepsakes
            where relation_id = $1
              and state = 'applied'
              and (
                $2::text is null
                or (subject_kind, subject_id, id) > ($2, $3, $4)
              )
            order by subject_kind, subject_id, id
            limit $5
            ",
        )
        .bind(relation_id)
        .bind(after.map(|cursor| cursor.subject_kind.as_str()))
        .bind(after.map(|cursor| cursor.subject_id.as_str()))
        .bind(after.map(|cursor| cursor.keepsake_id))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(AppliedKeepsakeRow::try_into_keepsake)
            .collect()
    }

    /// Lists due timed expiry candidates in stable batch order.
    pub async fn due_timed_expiry(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
        let limit = validate_limit(limit)?;
        let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
            r"
            select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
            from keepsakes k
            join keepsake_relation_definitions r on r.id = k.relation_id
            where k.state = 'applied'
              and r.enabled
              and k.expires_at is not null
              and k.expires_at <= $1
            order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
            limit $2
            ",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Expires a stable batch of due timed keepsakes.
    pub async fn expire_due_timed(&self, now: DateTime<Utc>, limit: i64) -> RepositoryResult<u64> {
        let limit = validate_limit(limit)?;
        let mut tx = self.pool.begin().await?;
        let rows = due_timed_expiry_tx(&mut tx, now, limit).await?;
        let ids = rows
            .into_iter()
            .map(|row| row.keepsake_id)
            .collect::<Vec<Uuid>>();
        if ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let result = sqlx::query(
            r"
            update keepsakes
            set state = 'expired', updated_at = $2
            where id = any($1)
              and state = 'applied'
              and exists (
                select 1
                from keepsake_relation_definitions r
                where r.id = keepsakes.relation_id and r.enabled
              )
            ",
        )
        .bind(&ids)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    /// Upserts a simple fulfillment counter projection.
    #[cfg(feature = "fulfillment-counters")]
    pub async fn upsert_counter_projection(
        &self,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: DateTime<Utc>,
    ) -> RepositoryResult<()> {
        sqlx::query(
            r"
            insert into keepsake_fulfillment_counters
                (keepsake_id, key, value, observed_at)
            values ($1, $2, $3, $4)
            on conflict (keepsake_id, key) do update set
                value = excluded.value,
                observed_at = excluded.observed_at
            ",
        )
        .bind(keepsake_id)
        .bind(key)
        .bind(value)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// Timestamp-scoped repository view.
///
/// This wrapper does not read the system clock. Callers choose the timestamp once
/// at an operation boundary, then use the forwarding methods to keep related
/// writes and expiry scans on the same deterministic instant.
#[derive(Debug, Clone, Copy)]
pub struct TimedKeepsakeRepository<'repo, C = NoopRelationCache> {
    repository: &'repo KeepsakeRepository<C>,
    at: DateTime<Utc>,
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

async fn due_timed_expiry_tx(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    limit: i64,
) -> RepositoryResult<Vec<TimedExpiryCandidate>> {
    let rows = sqlx::query_as::<_, TimedExpiryCandidate>(
        r"
        select k.id as keepsake_id, k.relation_id, k.subject_kind, k.subject_id, k.expires_at as due_at
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.state = 'applied'
          and r.enabled
          and k.expires_at is not null
          and k.expires_at <= $1
        order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
        limit $2
        for update of k skip locked
        for share of r
        ",
    )
    .bind(now)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

fn validate_limit(limit: i64) -> RepositoryResult<i64> {
    if (1..=MAX_BATCH_LIMIT).contains(&limit) {
        Ok(limit)
    } else {
        Err(RepositoryError::InvalidLimit {
            limit,
            max: MAX_BATCH_LIMIT,
        })
    }
}

async fn relation_for_share_tx(
    tx: &mut Transaction<'_, Postgres>,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query_as::<_, RelationRow>(
        r"
        select id, kind, key, enabled, expiry_policy
        from keepsake_relation_definitions
        where id = $1
        for share
        ",
    )
    .bind(relation_id)
    .fetch_one(&mut **tx)
    .await?;
    row.try_into_relation()
}

/// Adapter for relation definition caching.
#[async_trait::async_trait]
pub trait RelationCache: Send + Sync + std::fmt::Debug {
    /// Gets a cached relation by stable id.
    async fn get_by_id(&self, relation_id: RelationId) -> Option<RelationDefinition>;

    /// Gets a cached relation by natural relation key.
    async fn get_by_key(&self, key: &RelationKey) -> Option<RelationDefinition>;

    /// Stores or refreshes a relation definition.
    async fn store(&self, relation: &RelationDefinition);

    /// Removes cached entries for a relation id.
    async fn remove_by_id(&self, relation_id: RelationId);
}

/// Relation cache implementation that never stores entries.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRelationCache;

#[async_trait::async_trait]
impl RelationCache for NoopRelationCache {
    async fn get_by_id(&self, _relation_id: RelationId) -> Option<RelationDefinition> {
        None
    }

    async fn get_by_key(&self, _key: &RelationKey) -> Option<RelationDefinition> {
        None
    }

    async fn store(&self, _relation: &RelationDefinition) {}

    async fn remove_by_id(&self, _relation_id: RelationId) {}
}

/// Configuration for local in-process relation definition caching.
#[cfg(feature = "cache")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRelationCacheConfig {
    /// Time before a cached relation definition must be refreshed from Postgres.
    pub ttl: Duration,
}

#[cfg(feature = "cache")]
impl LocalRelationCacheConfig {
    /// Creates a local relation cache configuration.
    #[must_use]
    pub const fn new(ttl: Duration) -> Self {
        Self { ttl }
    }
}

/// Local in-process relation definition cache.
#[cfg(feature = "cache")]
#[derive(Debug, Clone)]
pub struct LocalRelationCache {
    config: LocalRelationCacheConfig,
    // Local cache handles may be cloned or shared across repository clones.
    // Locks protect a small in-process map and are never held across `.await`.
    // Cross-pod invalidation belongs in another `RelationCache` adapter.
    state: Arc<RwLock<LocalRelationCacheState>>,
}

#[cfg(feature = "cache")]
impl LocalRelationCache {
    /// Creates a local in-process relation definition cache.
    #[must_use]
    pub fn new(config: LocalRelationCacheConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(LocalRelationCacheState::default())),
        }
    }
}

#[cfg(feature = "cache")]
#[async_trait::async_trait]
impl RelationCache for LocalRelationCache {
    async fn get_by_id(&self, relation_id: RelationId) -> Option<RelationDefinition> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.by_id.get(&relation_id).cloned())
            .and_then(CacheEntry::fresh_relation)
    }

    async fn get_by_key(&self, key: &RelationKey) -> Option<RelationDefinition> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.by_key.get(key).cloned())
            .and_then(CacheEntry::fresh_relation)
    }

    async fn store(&self, relation: &RelationDefinition) {
        let entry = CacheEntry {
            relation: relation.clone(),
            expires_at: Instant::now() + self.config.ttl,
        };
        if let Ok(mut state) = self.state.write() {
            state.by_id.insert(relation.id, entry.clone());
            state.by_key.insert(relation.key.clone(), entry);
        }
    }

    async fn remove_by_id(&self, relation_id: RelationId) {
        if let Ok(mut state) = self.state.write()
            && let Some(entry) = state.by_id.remove(&relation_id)
        {
            state.by_key.remove(&entry.relation.key);
        }
    }
}

#[cfg(feature = "cache")]
#[derive(Debug, Default)]
struct LocalRelationCacheState {
    by_id: BTreeMap<RelationId, CacheEntry>,
    by_key: BTreeMap<RelationKey, CacheEntry>,
}

#[cfg(feature = "cache")]
#[derive(Debug, Clone)]
struct CacheEntry {
    relation: RelationDefinition,
    expires_at: Instant,
}

#[cfg(feature = "cache")]
impl CacheEntry {
    fn fresh_relation(self) -> Option<RelationDefinition> {
        (Instant::now() <= self.expires_at).then_some(self.relation)
    }
}

/// Keyset cursor for active relation membership scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipCursor {
    /// Last seen subject kind.
    pub subject_kind: String,
    /// Last seen subject id.
    pub subject_id: String,
    /// Last seen keepsake id.
    pub keepsake_id: Uuid,
}

impl MembershipCursor {
    /// Creates a cursor positioned after a returned keepsake.
    #[must_use]
    pub fn after(keepsake: &Keepsake) -> Self {
        Self {
            subject_kind: keepsake.subject.kind.clone(),
            subject_id: keepsake.subject.id.clone(),
            keepsake_id: keepsake.id,
        }
    }
}

/// Result of an apply operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedKeepsake {
    /// Created keepsake, or the existing active keepsake for duplicate applies.
    pub keepsake: Keepsake,
    /// Whether a duplicate active keepsake was prevented.
    pub duplicate_prevented: bool,
}

/// Active keepsake with its relation definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveRelation {
    /// Active keepsake.
    pub keepsake: Keepsake,
    /// Stored relation definition for the keepsake.
    pub relation: RelationDefinition,
}

/// Due timed expiry candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct TimedExpiryCandidate {
    /// Keepsake id.
    pub keepsake_id: Uuid,
    /// Relation id.
    pub relation_id: Uuid,
    /// Subject kind.
    pub subject_kind: String,
    /// Subject id.
    pub subject_id: String,
    /// Due timestamp.
    pub due_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct RelationRow {
    id: Uuid,
    kind: String,
    key: String,
    enabled: bool,
    expiry_policy: serde_json::Value,
}

impl RelationRow {
    fn try_into_relation(self) -> RepositoryResult<RelationDefinition> {
        let expiry = serde_json::from_value::<ExpiryPolicy>(self.expiry_policy)?;
        Ok(RelationDefinition::new(
            self.id,
            RelationKey::new(self.kind, self.key)?,
            self.enabled,
            expiry,
        )?)
    }
}

#[derive(Debug, FromRow)]
struct AppliedKeepsakeRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
}

impl AppliedKeepsakeRow {
    fn try_into_keepsake(self) -> RepositoryResult<Keepsake> {
        let expiry = serde_json::from_value::<ExpiryPolicy>(self.expiry_policy)?;
        let metadata = serde_json::from_value::<BTreeMap<String, String>>(self.metadata)?;
        Ok(Keepsake {
            id: self.id,
            subject: SubjectRef {
                kind: self.subject_kind,
                id: self.subject_id,
            },
            relation_id: self.relation_id,
            state: parse_state(self.state)?,
            expiry,
            applied_at: self.applied_at,
            expires_at: self.expires_at,
            fulfilled_at: self.fulfilled_at,
            revoked_at: self.revoked_at,
            metadata,
        })
    }
}

#[derive(Debug, FromRow)]
struct AppliedKeepsakeWriteRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
    duplicate_prevented: bool,
}

impl AppliedKeepsakeWriteRow {
    fn try_into_parts(self) -> RepositoryResult<(Keepsake, bool)> {
        let expiry = serde_json::from_value::<ExpiryPolicy>(self.expiry_policy)?;
        let metadata = serde_json::from_value::<BTreeMap<String, String>>(self.metadata)?;
        let keepsake = Keepsake {
            id: self.id,
            subject: SubjectRef {
                kind: self.subject_kind,
                id: self.subject_id,
            },
            relation_id: self.relation_id,
            state: parse_state(self.state)?,
            expiry,
            applied_at: self.applied_at,
            expires_at: self.expires_at,
            fulfilled_at: self.fulfilled_at,
            revoked_at: self.revoked_at,
            metadata,
        };
        Ok((keepsake, self.duplicate_prevented))
    }
}

#[derive(Debug, FromRow)]
struct ActiveRelationRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
    relation_definition_id: Uuid,
    relation_kind: String,
    relation_key: String,
    relation_enabled: bool,
    relation_expiry_policy: serde_json::Value,
}

impl ActiveRelationRow {
    fn try_into_active_relation(self) -> RepositoryResult<ActiveRelation> {
        let expiry = serde_json::from_value::<ExpiryPolicy>(self.expiry_policy)?;
        let relation_expiry = serde_json::from_value::<ExpiryPolicy>(self.relation_expiry_policy)?;
        let metadata = serde_json::from_value::<BTreeMap<String, String>>(self.metadata)?;
        Ok(ActiveRelation {
            keepsake: Keepsake {
                id: self.id,
                subject: SubjectRef {
                    kind: self.subject_kind,
                    id: self.subject_id,
                },
                relation_id: self.relation_id,
                state: parse_state(self.state)?,
                expiry,
                applied_at: self.applied_at,
                expires_at: self.expires_at,
                fulfilled_at: self.fulfilled_at,
                revoked_at: self.revoked_at,
                metadata,
            },
            relation: RelationDefinition::new(
                self.relation_definition_id,
                RelationKey::new(self.relation_kind, self.relation_key)?,
                self.relation_enabled,
                relation_expiry,
            )?,
        })
    }
}

fn parse_state(value: String) -> RepositoryResult<LifecycleState> {
    match value.as_str() {
        "applied" => Ok(LifecycleState::Applied),
        "revoked" => Ok(LifecycleState::Revoked),
        "expired" => Ok(LifecycleState::Expired),
        _ => Err(RepositoryError::InvalidLifecycleState { state: value }),
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
    }

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Chrono(#[from] chrono::ParseError),

        #[error(transparent)]
        Keepsake(#[from] keepsake::KeepsakeError),

        #[error(transparent)]
        Repository(#[from] RepositoryError),

        #[error(transparent)]
        SerdeJson(#[from] serde_json::Error),

        #[error(transparent)]
        Sqlx(#[from] sqlx::Error),
    }

    #[tokio::test]
    async fn timestamp_scoped_repository_reuses_explicit_timestamp() -> Result<(), TestError> {
        let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/keepsake")?;
        let repo = KeepsakeRepository::new(pool);
        let at = ts("2026-01-02T00:00:00Z")?;
        let timed_repo = repo.at(at);

        assert_eq!(timed_repo.timestamp(), at);
        Ok(())
    }

    #[tokio::test]
    async fn active_relations_for_subject_by_keys_short_circuits_empty_keys()
    -> Result<(), TestError> {
        let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/keepsake")?;
        let repo = KeepsakeRepository::new(pool);
        let subject = SubjectRef::new("account", "acct_123")?;

        let active = repo
            .active_relations_for_subject_by_keys(&subject, &[])
            .await?;

        assert!(active.is_empty());
        Ok(())
    }

    #[test]
    fn membership_cursor_serializes_for_api_boundaries() -> RepositoryResult<()> {
        let cursor = MembershipCursor {
            subject_kind: "account".to_owned(),
            subject_id: "acct_123".to_owned(),
            keepsake_id: Uuid::nil(),
        };

        let encoded = serde_json::to_string(&cursor)?;
        let decoded = serde_json::from_str::<MembershipCursor>(&encoded)?;

        assert_eq!(decoded, cursor);
        Ok(())
    }

    #[test]
    fn timed_expiry_candidate_serializes_with_stable_field_names() -> Result<(), TestError> {
        let candidate = TimedExpiryCandidate {
            keepsake_id: Uuid::nil(),
            relation_id: Uuid::nil(),
            subject_kind: "account".to_owned(),
            subject_id: "acct_123".to_owned(),
            due_at: ts("2026-01-02T00:00:00Z")?,
        };

        let encoded = serde_json::to_value(&candidate)?;

        assert_eq!(
            encoded,
            serde_json::json!({
                "keepsake_id": "00000000-0000-0000-0000-000000000000",
                "relation_id": "00000000-0000-0000-0000-000000000000",
                "subject_kind": "account",
                "subject_id": "acct_123",
                "due_at": "2026-01-02T00:00:00Z"
            })
        );
        assert_eq!(
            serde_json::from_value::<TimedExpiryCandidate>(encoded)?,
            candidate
        );
        Ok(())
    }

    #[test]
    fn parse_state_rejects_unknown_values() {
        let error = parse_state("archived".to_owned())
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert_eq!(error, Err("unknown lifecycle state archived".to_owned()));
    }
}
