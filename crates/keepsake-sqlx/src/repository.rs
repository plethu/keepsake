//! Postgres repository implementation.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use keepsake::{Keepsake, RelationDefinition, RelationId, RelationKey, RelationSpec, SubjectRef};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

#[cfg(feature = "migrations")]
use sqlx::migrate::Migrator;

mod cache;
mod rows;
mod timed;
mod types;

#[cfg(feature = "cache")]
pub use cache::{LocalRelationCache, LocalRelationCacheConfig};
pub use cache::{NoopRelationCache, RelationCache};
pub use timed::TimedKeepsakeRepository;
pub use types::{ActiveRelation, AppliedKeepsake, MembershipCursor, TimedExpiryCandidate};

use rows::{ActiveRelationRow, AppliedKeepsakeRow, AppliedKeepsakeWriteRow, RelationRow};

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
        subject.validate()?;
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

        let (keepsake, duplicate_prevented) = applied.try_into_parts()?;
        tx.commit().await?;
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

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use sqlx::postgres::PgPoolOptions;

    use super::rows::parse_state;
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
