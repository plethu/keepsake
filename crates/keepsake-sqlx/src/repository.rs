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
#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
mod tenant_upgrade;
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
pub use support::{AuditEventDecodeError, DovecoteAuditConfig, decode_audit_event};
#[cfg(feature = "postgres")]
pub use timed::TimedKeepsakeRepository;
#[cfg(feature = "mysql")]
pub use timed::TimedMySqlKeepsakeRepository;
#[cfg(feature = "sqlite")]
pub use timed::TimedSqliteKeepsakeRepository;
pub use timed::TimedTenantSqlxKeepsakeRepository;
pub use types::{
    AppliedKeepsake, FulfilledExpiryCandidate, MembershipCursor, TimedExpiryCandidate,
};

#[cfg(all(feature = "migrations", feature = "postgres"))]
static POSTGRES_MIGRATOR: Migrator = sqlx::migrate!("./migrations/postgres");

#[cfg(all(feature = "migrations", feature = "postgres"))]
static POSTGRES_V3_MIGRATOR: Migrator = sqlx::migrate!("./migrations/v3/postgres");

#[cfg(all(feature = "migrations", feature = "sqlite"))]
static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

#[cfg(all(feature = "migrations", feature = "sqlite"))]
static SQLITE_V3_MIGRATOR: Migrator = sqlx::migrate!("./migrations/v3/sqlite");

#[cfg(all(feature = "migrations", feature = "mysql"))]
static MYSQL_MIGRATOR: Migrator = sqlx::migrate!("./migrations/mysql");

#[cfg(all(feature = "migrations", feature = "mysql"))]
static MYSQL_V3_MIGRATOR: Migrator = sqlx::migrate!("./migrations/v3/mysql");

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

    /// An upsert reused a stable id for a different natural relation key.
    #[error(
        "relation id {relation_id} already belongs to {stored_kind}/{stored_name}, not {incoming_kind}/{incoming_name}"
    )]
    RelationIdentityConflict {
        /// Conflicting stable relation id.
        relation_id: Uuid,
        /// Existing relation kind.
        stored_kind: String,
        /// Existing relation name.
        stored_name: String,
        /// Requested relation kind.
        incoming_kind: String,
        /// Requested relation name.
        incoming_name: String,
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

    /// A tenant-aware trait call attempted to use a different tenant than the
    /// handle that received it.
    #[error("tenant does not match the scoped repository handle")]
    TenantScopeMismatch,
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

/// A Keepsake repository view whose ordinary operations are restricted to one
/// validated tenant. The root repository intentionally exposes no unscoped
/// relation or lifecycle operations.
#[derive(Debug)]
pub struct TenantSqlxKeepsakeRepository<'repo, B, C = NoopRelationCache>
where
    B: KeepsakeSqlxBackend,
{
    pub(super) pool: &'repo Pool<B::Database>,
    pub(super) relation_cache: &'repo C,
    pub(super) audit: &'repo support::DovecoteAuditConfig,
    pub(super) tenant_id: keepsake::TenantId,
}

impl<B, C> Clone for TenantSqlxKeepsakeRepository<'_, B, C>
where
    B: KeepsakeSqlxBackend,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool,
            relation_cache: self.relation_cache,
            audit: self.audit,
            tenant_id: self.tenant_id.clone(),
        }
    }
}

/// Explicit administrative construction view. All data operations still
/// require an explicit tenant through [`Self::for_tenant`].
#[derive(Debug, Clone, Copy)]
pub struct AdminSqlxKeepsakeRepository<'repo, B, C = NoopRelationCache>
where
    B: KeepsakeSqlxBackend,
{
    repository: &'repo SqlxKeepsakeRepository<B, C>,
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

/// Tenant-scoped `PostgreSQL` keepsake repository.
#[cfg(feature = "postgres")]
pub type TenantKeepsakeRepository<'repo, C = NoopRelationCache> =
    TenantSqlxKeepsakeRepository<'repo, PostgresBackend, C>;

/// Explicit administrative `PostgreSQL` construction view.
#[cfg(feature = "postgres")]
pub type AdminKeepsakeRepository<'repo, C = NoopRelationCache> =
    AdminSqlxKeepsakeRepository<'repo, PostgresBackend, C>;

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

impl<B, C> SqlxKeepsakeRepository<B, C>
where
    B: KeepsakeSqlxBackend,
{
    /// Creates a tenant-scoped view for ordinary relation and lifecycle work.
    pub const fn for_tenant(
        &self,
        tenant_id: keepsake::TenantId,
    ) -> TenantSqlxKeepsakeRepository<'_, B, C> {
        TenantSqlxKeepsakeRepository {
            pool: &self.pool,
            relation_cache: &self.relation_cache,
            audit: &self.audit,
            tenant_id,
        }
    }

    /// Creates an explicit administrative construction view.
    pub const fn admin(&self) -> AdminSqlxKeepsakeRepository<'_, B, C> {
        AdminSqlxKeepsakeRepository { repository: self }
    }
}

impl<'repo, B, C> AdminSqlxKeepsakeRepository<'repo, B, C>
where
    B: KeepsakeSqlxBackend,
{
    /// Creates a tenant-scoped view from this explicit administrative handle.
    pub const fn for_tenant(
        &self,
        tenant_id: keepsake::TenantId,
    ) -> TenantSqlxKeepsakeRepository<'repo, B, C> {
        self.repository.for_tenant(tenant_id)
    }
}

impl<'repo, B, C> TenantSqlxKeepsakeRepository<'repo, B, C>
where
    B: KeepsakeSqlxBackend,
{
    /// Returns this view's validated tenant identity.
    #[must_use]
    pub const fn tenant_id(&self) -> &keepsake::TenantId {
        &self.tenant_id
    }

    /// Creates a timestamp-scoped view for this tenant.
    #[must_use]
    pub const fn at(
        &self,
        at: DateTime<Utc>,
    ) -> TimedTenantSqlxKeepsakeRepository<'_, 'repo, B, C> {
        TimedTenantSqlxKeepsakeRepository {
            repository: self,
            at,
        }
    }
}

#[cfg(feature = "postgres")]
impl<C> PostgresKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Verifies the Keepsake 3.0 domain schema and the selected Dovecote schema.
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
        POSTGRES_V3_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Installs nullable tenant columns on a Keepsake 2.x schema.
    ///
    /// This is an operator step. It does not assign tenants; callers must
    /// backfill using an independently reviewed mapping before activation.
    #[cfg(feature = "migrations")]
    pub async fn prepare_tenant_upgrade(&self) -> RepositoryResult<()> {
        sqlx::raw_sql(tenant_upgrade::POSTGRES_PREPARE_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Activates a fully backfilled Keepsake 2.x schema as Keepsake 3.x.
    ///
    /// Activation fails while any tenant is missing and never chooses a
    /// sentinel or inferred tenant.
    #[cfg(feature = "migrations")]
    pub async fn activate_tenant_upgrade(&self) -> RepositoryResult<()> {
        sqlx::raw_sql(tenant_upgrade::POSTGRES_ACTIVATE_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Runs the explicit 1.x upgrade track, preserving legacy audit tables as
    /// read-only migration material. New 3.0 operations never use this track.
    #[cfg(feature = "migrations")]
    pub async fn upgrade_migrate(&self) -> RepositoryResult<()> {
        schema::postgres_upgrade_schema_preflight(&self.pool).await?;
        POSTGRES_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Activates the historical 2.0 runtime after complete-history import and reconciliation.
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
    /// Verifies the Keepsake 3.0 domain schema and the selected Dovecote schema.
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
        SQLITE_V3_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Adds nullable tenant columns to a `SQLite` Keepsake 2.x schema.
    #[cfg(feature = "migrations")]
    pub async fn prepare_tenant_upgrade(&self) -> RepositoryResult<()> {
        sqlx::raw_sql(tenant_upgrade::SQLITE_PREPARE_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Activates a fully backfilled `SQLite` Keepsake 2.x schema as Keepsake 3.x.
    #[cfg(feature = "migrations")]
    pub async fn activate_tenant_upgrade(&self) -> RepositoryResult<()> {
        // The artifact disables foreign-key enforcement before BEGIN IMMEDIATE
        // because SQLite does not allow changing that pragma inside a
        // transaction. Keep the script on one acquired connection so a
        // validation failure can be rolled back before the connection returns
        // to the pool.
        let mut connection = self.pool.acquire().await?;
        match sqlx::raw_sql(tenant_upgrade::SQLITE_ACTIVATE_SQL)
            .execute(&mut *connection)
            .await
        {
            Ok(_) => {
                // The artifact also restores this, but repeat it after a
                // successful script so the connection invariant is explicit.
                sqlx::query("pragma foreign_keys = on")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            }
            Err(error) => {
                // The activation script starts BEGIN IMMEDIATE and performs
                // all DDL and validation before COMMIT. Roll back the open
                // transaction before restoring the connection pragma.
                sqlx::raw_sql("rollback").execute(&mut *connection).await?;
                sqlx::query("pragma foreign_keys = on")
                    .execute(&mut *connection)
                    .await?;
                Err(error.into())
            }
        }
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
    /// Verifies the Keepsake 3.0 domain schema and the selected Dovecote schema.
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
        MYSQL_V3_MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Adds nullable tenant columns to a `MySQL` Keepsake 2.x schema.
    #[cfg(feature = "migrations")]
    pub async fn prepare_tenant_upgrade(&self) -> RepositoryResult<()> {
        sqlx::raw_sql(tenant_upgrade::MYSQL_PREPARE_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Activates a fully backfilled `MySQL` Keepsake 2.x schema as Keepsake 3.x.
    #[cfg(feature = "migrations")]
    pub async fn activate_tenant_upgrade(&self) -> RepositoryResult<()> {
        sqlx::raw_sql(tenant_upgrade::MYSQL_ACTIVATE_SQL)
            .execute(&self.pool)
            .await?;
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
