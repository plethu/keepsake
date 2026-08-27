//! `SQLx` repository implementation.

use chrono::{DateTime, Utc};
use sqlx::Pool;
use uuid::Uuid;

#[cfg(feature = "migrations")]
use sqlx::migrate::Migrator;

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
#[cfg_attr(
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite")),
    allow(dead_code)
)]
mod support;
mod timed;
mod types;
#[cfg(feature = "migrations")]
mod upgrade;

use backend::BackendMarker;
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
use rows::{ActiveRelationRow, AppliedKeepsakeRow, AppliedKeepsakeWriteRow, RelationRow};
pub use support::DovecoteAuditConfig;
#[cfg(feature = "postgres")]
pub use timed::TimedKeepsakeRepository;
#[cfg(feature = "mysql")]
pub use timed::TimedMySqlKeepsakeRepository;
#[cfg(feature = "sqlite")]
pub use timed::TimedSqliteKeepsakeRepository;
pub use timed::TimedSqlxKeepsakeRepository;
pub use types::{
    AppliedKeepsake, FulfilledExpiryCandidate, MembershipCursor, TimedExpiryCandidate,
};

#[cfg(all(feature = "migrations", feature = "postgres"))]
static POSTGRES_MIGRATOR: Migrator = sqlx::migrate!("./migrations/postgres");

#[cfg(all(feature = "migrations", feature = "postgres"))]
static POSTGRES_V2_MIGRATOR: Migrator = sqlx::migrate!("./migrations/v2/postgres");

#[cfg(all(feature = "migrations", feature = "sqlite"))]
static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

#[cfg(all(feature = "migrations", feature = "sqlite"))]
static SQLITE_V2_MIGRATOR: Migrator = sqlx::migrate!("./migrations/v2/sqlite");

#[cfg(all(feature = "migrations", feature = "mysql"))]
static MYSQL_MIGRATOR: Migrator = sqlx::migrate!("./migrations/mysql");

#[cfg(all(feature = "migrations", feature = "mysql"))]
static MYSQL_V2_MIGRATOR: Migrator = sqlx::migrate!("./migrations/v2/mysql");

#[allow(dead_code)]
const MAX_BATCH_LIMIT: i64 = 10_000;

/// Result alias for SQL repository operations.
pub type RepositoryResult<T> = core::result::Result<T, RepositoryError>;

/// Backend-preserving Dovecote enqueue failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DovecoteEnqueueError {
    /// `PostgreSQL` adapter failure.
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL Dovecote enqueue: {0}")]
    Postgres(#[from] dovecote_sqlx_postgres::EnqueueError),
    /// `SQLite` adapter failure.
    #[cfg(feature = "sqlite")]
    #[error("SQLite Dovecote enqueue: {0}")]
    Sqlite(#[from] dovecote_sqlx_sqlite::EnqueueError),
    /// MySQL/MariaDB adapter failure.
    #[cfg(feature = "mysql")]
    #[error("MySQL Dovecote enqueue: {0}")]
    Mysql(#[from] dovecote_sqlx_mysql::EnqueueError),
}

/// Backend-preserving Dovecote schema-check failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DovecoteSchemaError {
    /// `PostgreSQL` adapter failure.
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL Dovecote schema: {0}")]
    Postgres(#[from] dovecote_sqlx_postgres::SchemaError),
    /// `SQLite` adapter failure.
    #[cfg(feature = "sqlite")]
    #[error("SQLite Dovecote schema: {0}")]
    Sqlite(#[from] dovecote_sqlx_sqlite::SchemaError),
    /// MySQL/MariaDB adapter failure.
    #[cfg(feature = "mysql")]
    #[error("MySQL Dovecote schema: {0}")]
    Mysql(#[from] dovecote_sqlx_mysql::SchemaError),
}

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

    /// Dovecote rejected an event before SQL mutation.
    #[error(transparent)]
    DovecoteValidation(#[from] dovecote::ValidationError),

    /// Dovecote enqueue failed during a lifecycle operation.
    #[error(transparent)]
    DovecoteEnqueue(#[from] DovecoteEnqueueError),

    /// Dovecote schema verification failed.
    #[error(transparent)]
    DovecoteSchema(#[from] DovecoteSchemaError),

    /// An occurrence timestamp cannot be represented by Dovecote.
    #[error("audit timestamp is outside Dovecote's supported range: {detail}")]
    TimestampOutOfRange {
        /// Conversion detail.
        detail: String,
    },

    /// Existing schema metadata belongs to a different backend.
    #[error("schema backend mismatch: expected {expected}, found {actual}")]
    BackendMismatch {
        /// Backend expected by this repository.
        expected: &'static str,
        /// Backend found in schema metadata.
        actual: String,
    },

    /// Importer evidence did not describe a complete, zero-delta scan.
    #[error("invalid upgrade evidence field: {field}")]
    InvalidUpgradeEvidence {
        /// Evidence field that failed its contract.
        field: &'static str,
    },

    /// A second complete-history scan disagreed with recorded evidence.
    #[error("upgrade evidence conflict: {detail}")]
    UpgradeEvidenceConflict {
        /// Conflict detail.
        detail: String,
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
    audit: support::DovecoteAuditConfig,
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
            audit: self.audit.clone(),
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
    /// Creates a repository from a Postgres pool and application-owned source.
    pub fn new(pool: sqlx::PgPool, source: impl Into<String>) -> RepositoryResult<Self> {
        Ok(Self {
            pool,
            relation_cache: NoopRelationCache,
            backend: BackendMarker::new(),
            audit: support::DovecoteAuditConfig::new(source)?,
        })
    }
}

#[cfg(feature = "sqlite")]
impl SqliteKeepsakeRepository<NoopRelationCache> {
    /// Creates a repository from a `SQLite` pool and application-owned source.
    pub fn new(pool: sqlx::SqlitePool, source: impl Into<String>) -> RepositoryResult<Self> {
        Ok(Self {
            pool,
            relation_cache: NoopRelationCache,
            backend: BackendMarker::new(),
            audit: support::DovecoteAuditConfig::new(source)?,
        })
    }
}

#[cfg(feature = "mysql")]
impl MySqlKeepsakeRepository<NoopRelationCache> {
    /// Creates a repository from a `MySQL` pool and application-owned source.
    pub fn new(pool: sqlx::MySqlPool, source: impl Into<String>) -> RepositoryResult<Self> {
        Ok(Self {
            pool,
            relation_cache: NoopRelationCache,
            backend: BackendMarker::new(),
            audit: support::DovecoteAuditConfig::new(source)?,
        })
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
            audit: self.audit,
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

#[cfg(feature = "postgres")]
impl<C> PostgresKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Verifies the Keepsake 2.0 domain schema and the selected Dovecote schema.
    pub async fn check_schema(&self) -> RepositoryResult<()> {
        schema::postgres_runtime_schema_check(&self.pool).await?;
        dovecote_sqlx_postgres::check_schema(&self.pool)
            .await
            .map_err(|error| RepositoryError::DovecoteSchema(error.into()))
    }

    /// Runs embedded migrations.
    #[cfg(feature = "migrations")]
    pub async fn migrate(&self) -> RepositoryResult<()> {
        schema::postgres_clean_schema_preflight(&self.pool).await?;
        POSTGRES_V2_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Runs the explicit 1.x upgrade track, preserving legacy audit tables as
    /// read-only migration material. New 2.0 operations never use this track.
    #[cfg(feature = "migrations")]
    pub async fn upgrade_migrate(&self) -> RepositoryResult<()> {
        schema::postgres_upgrade_schema_preflight(&self.pool).await?;
        POSTGRES_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Activates the 2.0 runtime after complete-history import and reconciliation.
    #[cfg(feature = "migrations")]
    pub async fn activate_upgrade(&self) -> RepositoryResult<()> {
        schema::postgres_upgrade_schema_preflight(&self.pool).await?;
        dovecote_sqlx_postgres::check_schema(&self.pool)
            .await
            .map_err(|error| RepositoryError::DovecoteSchema(error.into()))?;
        schema::postgres_upgrade_schema_check(&self.pool).await?;
        let evidence = sqlx::query_as::<_, upgrade::UpgradeEvidenceRow>("select evidence_schema_version::bigint as evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete from keepsake_upgrade_evidence where evidence_id = 1")
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| RepositoryError::BackendMismatch { expected: "complete zero-delta upgrade evidence", actual: "no reconciliation evidence".to_owned() })?;
        upgrade::validate(&evidence, self.audit.source())?;
        sqlx::query("insert into keepsake_schema_metadata (key, value) values ('api_track', '2') on conflict (key) do update set value = excluded.value")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
impl<C> SqliteKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Verifies the Keepsake 2.0 domain schema and the selected Dovecote schema.
    pub async fn check_schema(&self) -> RepositoryResult<()> {
        schema::sqlite_runtime_schema_check(&self.pool).await?;
        dovecote_sqlx_sqlite::check_schema(&self.pool)
            .await
            .map_err(|error| RepositoryError::DovecoteSchema(error.into()))
    }

    /// Runs embedded `SQLite` migrations.
    #[cfg(feature = "migrations")]
    pub async fn migrate(&self) -> RepositoryResult<()> {
        schema::sqlite_clean_schema_preflight(&self.pool).await?;
        SQLITE_V2_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Runs the explicit 1.x upgrade track and leaves legacy audit tables
    /// available for reconciliation and rollback.
    #[cfg(feature = "migrations")]
    pub async fn upgrade_migrate(&self) -> RepositoryResult<()> {
        schema::sqlite_upgrade_schema_preflight(&self.pool).await?;
        SQLITE_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Activates the 2.0 runtime after complete-history import and reconciliation.
    #[cfg(feature = "migrations")]
    pub async fn activate_upgrade(&self) -> RepositoryResult<()> {
        schema::sqlite_upgrade_schema_preflight(&self.pool).await?;
        dovecote_sqlx_sqlite::check_schema(&self.pool)
            .await
            .map_err(|error| RepositoryError::DovecoteSchema(error.into()))?;
        schema::sqlite_upgrade_schema_check(&self.pool).await?;
        let evidence = sqlx::query_as::<_, upgrade::UpgradeEvidenceRow>("select evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete from keepsake_upgrade_evidence where evidence_id = 1")
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| RepositoryError::BackendMismatch { expected: "complete zero-delta upgrade evidence", actual: "no reconciliation evidence".to_owned() })?;
        upgrade::validate(&evidence, self.audit.source())?;
        sqlx::query("insert into keepsake_schema_metadata (key, value) values ('api_track', '2') on conflict (key) do update set value = excluded.value")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(feature = "mysql")]
impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Verifies the Keepsake 2.0 domain schema and the selected Dovecote schema.
    pub async fn check_schema(&self) -> RepositoryResult<()> {
        schema::mysql_runtime_schema_check(&self.pool).await?;
        dovecote_sqlx_mysql::check_schema(&self.pool)
            .await
            .map_err(|error| RepositoryError::DovecoteSchema(error.into()))
    }

    /// Runs embedded `MySQL` migrations.
    #[cfg(feature = "migrations")]
    pub async fn migrate(&self) -> RepositoryResult<()> {
        schema::mysql_clean_schema_preflight(&self.pool).await?;
        MYSQL_V2_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Runs the explicit 1.x upgrade track and leaves legacy audit tables
    /// available for reconciliation and rollback.
    #[cfg(feature = "migrations")]
    pub async fn upgrade_migrate(&self) -> RepositoryResult<()> {
        schema::mysql_upgrade_schema_preflight(&self.pool).await?;
        MYSQL_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Activates the 2.0 runtime after complete-history import and reconciliation.
    #[cfg(feature = "migrations")]
    pub async fn activate_upgrade(&self) -> RepositoryResult<()> {
        schema::mysql_upgrade_schema_preflight(&self.pool).await?;
        dovecote_sqlx_mysql::check_schema(&self.pool)
            .await
            .map_err(|error| RepositoryError::DovecoteSchema(error.into()))?;
        schema::mysql_upgrade_schema_check(&self.pool).await?;
        let evidence = sqlx::query_as::<_, upgrade::UpgradeEvidenceRow>("select evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete from keepsake_upgrade_evidence where evidence_id = 1")
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| RepositoryError::BackendMismatch { expected: "complete zero-delta upgrade evidence", actual: "no reconciliation evidence".to_owned() })?;
        upgrade::validate(&evidence, self.audit.source())?;
        sqlx::query("insert into keepsake_schema_metadata (`key`, value) values ('api_track', '2') on duplicate key update value = '2'")
            .execute(&self.pool)
            .await?;
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

mod schema;
#[cfg(all(test, feature = "postgres"))]
mod tests;
