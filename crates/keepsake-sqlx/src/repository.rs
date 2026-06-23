//! `SQLx` repository implementation.

use chrono::{DateTime, Utc};
use sqlx::Pool;
use uuid::Uuid;

#[cfg(feature = "migrations")]
use sqlx::migrate::Migrator;

#[cfg(feature = "postgres")]
mod audit;
mod backend;
mod cache;
#[cfg(feature = "postgres")]
mod expiry;
#[cfg(feature = "postgres")]
mod mutation;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod query;
#[cfg(feature = "postgres")]
mod relation;
#[cfg(feature = "postgres")]
mod rows;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod support;
mod timed;
mod types;

pub use backend::KeepsakeSqlxBackend;
#[cfg(feature = "mysql")]
pub use backend::MySqlBackend;
#[cfg(feature = "postgres")]
pub use backend::PostgresBackend;
#[cfg(feature = "sqlite")]
pub use backend::SqliteBackend;
#[cfg(feature = "cache")]
pub use cache::{LocalRelationCache, LocalRelationCacheConfig};
pub use cache::{NoopRelationCache, RelationCache};
pub use keepsake::ActiveRelation;
#[cfg(feature = "postgres")]
pub use timed::TimedKeepsakeRepository;
#[cfg(feature = "mysql")]
pub use timed::TimedMySqlKeepsakeRepository;
#[cfg(feature = "sqlite")]
pub use timed::TimedSqliteKeepsakeRepository;
pub use timed::TimedSqlxKeepsakeRepository;
pub use types::{
    AppliedKeepsake, AuditCursor, AuditEventRecord, FulfilledExpiryCandidate, MembershipCursor,
    TimedExpiryCandidate,
};

use backend::BackendMarker;
#[cfg(feature = "postgres")]
use rows::{
    ActiveRelationRow, AppliedKeepsakeRow, AppliedKeepsakeWriteRow, AuditEventRow, RelationRow,
};

#[cfg(all(feature = "migrations", feature = "postgres"))]
static POSTGRES_MIGRATOR: Migrator = sqlx::migrate!("./migrations/postgres");

#[cfg(all(feature = "migrations", feature = "sqlite"))]
static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

#[cfg(all(feature = "migrations", feature = "mysql"))]
static MYSQL_MIGRATOR: Migrator = sqlx::migrate!("./migrations/mysql");

#[allow(dead_code)]
const MAX_BATCH_LIMIT: i64 = 10_000;

/// Result alias for SQL repository operations.
pub type RepositoryResult<T> = core::result::Result<T, RepositoryError>;

/// SQL repository errors.
#[non_exhaustive]
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

    /// Existing schema metadata belongs to a different backend.
    #[error("schema backend mismatch: expected {expected}, found {actual}")]
    BackendMismatch {
        /// Backend expected by this repository.
        expected: &'static str,
        /// Backend found in schema metadata.
        actual: String,
    },

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

    /// A keepsake row referenced a missing relation definition.
    #[error("relation definition {relation_id} was not found")]
    RelationDefinitionMissing {
        /// Missing relation id.
        relation_id: Uuid,
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

    /// A stored audit event carried an unknown event type label.
    #[error("unknown audit event type {event_type}")]
    InvalidAuditEventType {
        /// Stored event type label.
        event_type: String,
    },
}

/// `SQLx`-backed keepsake repository.
#[derive(Debug)]
pub struct SqlxKeepsakeRepository<B, C = NoopRelationCache>
where
    B: KeepsakeSqlxBackend,
{
    pool: Pool<B::Database>,
    #[allow(dead_code)]
    relation_cache: C,
    backend: BackendMarker<B>,
}

impl<B, C> Clone for SqlxKeepsakeRepository<B, C>
where
    B: KeepsakeSqlxBackend,
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            relation_cache: self.relation_cache.clone(),
            backend: self.backend,
        }
    }
}

/// Postgres-backed keepsake repository.
#[cfg(feature = "postgres")]
pub type PostgresKeepsakeRepository<C = NoopRelationCache> =
    SqlxKeepsakeRepository<PostgresBackend, C>;

/// Default Postgres-backed keepsake repository.
#[cfg(feature = "postgres")]
pub type KeepsakeRepository<C = NoopRelationCache> = PostgresKeepsakeRepository<C>;

/// SQLite-backed keepsake repository.
#[cfg(feature = "sqlite")]
pub type SqliteKeepsakeRepository<C = NoopRelationCache> = SqlxKeepsakeRepository<SqliteBackend, C>;

/// MySQL-backed keepsake repository.
#[cfg(feature = "mysql")]
pub type MySqlKeepsakeRepository<C = NoopRelationCache> = SqlxKeepsakeRepository<MySqlBackend, C>;

#[cfg(feature = "postgres")]
impl PostgresKeepsakeRepository<NoopRelationCache> {
    /// Creates a repository from a Postgres pool.
    #[must_use]
    pub const fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            relation_cache: NoopRelationCache,
            backend: BackendMarker::new(),
        }
    }
}

#[cfg(feature = "sqlite")]
impl SqliteKeepsakeRepository<NoopRelationCache> {
    /// Creates a repository from a `SQLite` pool.
    #[must_use]
    pub const fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool,
            relation_cache: NoopRelationCache,
            backend: BackendMarker::new(),
        }
    }
}

#[cfg(feature = "mysql")]
impl MySqlKeepsakeRepository<NoopRelationCache> {
    /// Creates a repository from a `MySQL` pool.
    #[must_use]
    pub const fn new(pool: sqlx::MySqlPool) -> Self {
        Self {
            pool,
            relation_cache: NoopRelationCache,
            backend: BackendMarker::new(),
        }
    }
}

impl<B, C> SqlxKeepsakeRepository<B, C>
where
    B: KeepsakeSqlxBackend,
    C: RelationCache,
{
    /// Creates a timestamp-scoped repository view.
    ///
    /// Use this at request or job boundaries to keep one explicit clock read while
    /// avoiding repeated timestamp plumbing through related repository calls.
    pub const fn at(&self, at: DateTime<Utc>) -> TimedSqlxKeepsakeRepository<'_, B, C> {
        TimedSqlxKeepsakeRepository {
            repository: self,
            at,
        }
    }

    /// Enables relation definition caching for read helper methods.
    #[must_use]
    pub fn with_relation_cache<Next>(self, cache: Next) -> SqlxKeepsakeRepository<B, Next>
    where
        Next: RelationCache,
    {
        SqlxKeepsakeRepository {
            pool: self.pool,
            relation_cache: cache,
            backend: self.backend,
        }
    }

    /// Enables local in-process relation definition caching for read helper methods.
    ///
    /// This cache is per-process and has no cross-pod invalidation. Keep the
    /// default [`NoopRelationCache`] when relation definitions change frequently
    /// or when a multi-pod deployment needs invalidation guarantees.
    #[cfg(feature = "cache")]
    #[must_use]
    pub fn with_local_relation_cache(
        self,
        config: LocalRelationCacheConfig,
    ) -> SqlxKeepsakeRepository<B, LocalRelationCache> {
        self.with_relation_cache(LocalRelationCache::new(config))
    }
}

#[cfg(all(feature = "postgres", feature = "migrations"))]
impl<C> PostgresKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Runs embedded migrations.
    pub async fn migrate(&self) -> RepositoryResult<()> {
        postgres_schema_preflight(&self.pool).await?;
        POSTGRES_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
impl<C> SqliteKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Runs embedded `SQLite` migrations.
    pub async fn migrate(&self) -> RepositoryResult<()> {
        sqlite_schema_preflight(&self.pool).await?;
        SQLITE_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }
}

#[cfg(all(feature = "mysql", feature = "migrations"))]
impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Runs embedded `MySQL` migrations.
    pub async fn migrate(&self) -> RepositoryResult<()> {
        mysql_schema_preflight(&self.pool).await?;
        MYSQL_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }
}

#[allow(dead_code)]
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

#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn postgres_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        r"
        select to_regclass('public.keepsake_schema_metadata')::text
        ",
    )
    .fetch_one(pool)
    .await?;

    if metadata_table.is_none() {
        return postgres_unmarked_schema_preflight(pool).await;
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        r"
        select value
        from keepsake_schema_metadata
        where key = 'backend'
        ",
    )
    .fetch_one(pool)
    .await?;

    match backend.as_deref() {
        Some(PostgresBackend::NAME) | None => Ok(()),
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: actual.to_owned(),
        }),
    }
}

#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn postgres_unmarked_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let user_table_count = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from information_schema.tables
        where table_schema = 'public'
          and table_type = 'BASE TABLE'
        ",
    )
    .fetch_one(pool)
    .await?;

    if user_table_count == 0 {
        return Ok(());
    }

    let has_keepsake_tables = sqlx::query_scalar::<_, bool>(
        r"
        select to_regclass('public.keepsake_relation_definitions') is not null
           and to_regclass('public.keepsakes') is not null
           and to_regclass('public._sqlx_migrations') is not null
        ",
    )
    .fetch_one(pool)
    .await?;
    if !has_keepsake_tables {
        return Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        });
    }

    let known_migrations = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from _sqlx_migrations
        where version in (1, 2)
        ",
    )
    .fetch_one(pool)
    .await?;

    if known_migrations == 2 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: "unmarked unknown migration history".to_owned(),
        })
    }
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn sqlite_schema_preflight(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        r"
        select name
        from sqlite_master
        where type = 'table' and name = 'keepsake_schema_metadata'
        ",
    )
    .fetch_optional(pool)
    .await?
    .flatten();

    if metadata_table.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            r"
            select value
            from keepsake_schema_metadata
            where key = 'backend'
            ",
        )
        .fetch_one(pool)
        .await?;
        return match backend.as_deref() {
            Some(SqliteBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: SqliteBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing_tables = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from sqlite_master
        where type = 'table'
          and name not like 'sqlite_%'
        ",
    )
    .fetch_one(pool)
    .await?;

    if existing_tables == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: SqliteBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}

#[cfg(all(feature = "mysql", feature = "migrations"))]
async fn mysql_schema_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        r"
        select table_name
        from information_schema.tables
        where table_schema = database()
          and table_name = 'keepsake_schema_metadata'
        ",
    )
    .fetch_optional(pool)
    .await?
    .flatten();

    if metadata_table.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            r"
            select value
            from keepsake_schema_metadata
            where `key` = 'backend'
            ",
        )
        .fetch_one(pool)
        .await?;
        return match backend.as_deref() {
            Some(MySqlBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: MySqlBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing_tables = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from information_schema.tables
        where table_schema = database()
        ",
    )
    .fetch_one(pool)
    .await?;

    if existing_tables == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: MySqlBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use chrono::DateTime;
    use keepsake::SubjectRef;
    use sqlx::postgres::PgPoolOptions;

    use super::support::parse_state;
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
    fn fulfilled_expiry_candidate_serializes_with_stable_field_names() -> Result<(), TestError> {
        let candidate = FulfilledExpiryCandidate {
            keepsake_id: Uuid::nil(),
            relation_id: Uuid::nil(),
            subject_kind: "account".to_owned(),
            subject_id: "acct_123".to_owned(),
            expiry_policy: keepsake::ExpiryPolicy::WhenFulfilled {
                policy: keepsake::FulfillmentPolicy::CounterAtLeast {
                    key: "steps".to_owned(),
                    threshold: 3,
                },
            },
        };

        let encoded = serde_json::to_value(&candidate)?;

        assert_eq!(
            encoded,
            serde_json::json!({
                "keepsake_id": "00000000-0000-0000-0000-000000000000",
                "relation_id": "00000000-0000-0000-0000-000000000000",
                "subject_kind": "account",
                "subject_id": "acct_123",
                "expiry_policy": {
                    "type": "when_fulfilled",
                    "policy": {
                        "type": "counter_at_least",
                        "key": "steps",
                        "threshold": 3
                    }
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<FulfilledExpiryCandidate>(encoded)?,
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
