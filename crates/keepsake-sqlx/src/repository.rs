//! Postgres repository implementation.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[cfg(feature = "migrations")]
use sqlx::migrate::Migrator;

mod audit;
mod cache;
mod expiry;
mod mutation;
mod query;
mod relation;
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
pub type RepositoryResult<T> = core::result::Result<T, RepositoryError>;

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

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use keepsake::SubjectRef;
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
