//! Completes operator-managed tenant upgrades without replaying a clean baseline.
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
use super::{RelationCache, RepositoryError, RepositoryResult};
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
use sqlx::migrate::Migrator;

#[cfg(feature = "postgres")]
impl<C: RelationCache> super::PostgresKeepsakeRepository<C> {
    /// Completes an explicitly tenant-activated v3 database with the published v4 contract.
    ///
    /// Validates existing identifiers and catalog shape before applying the existing
    /// additive artifact. Never replays or fabricates a clean-baseline migration.
    /// Fence writers and retain a verified backup first; `MySQL` DDL is not transactional.
    /// A failed migration must be repaired using its recorded `SQLx` migration failure
    /// and backup, not by clearing the dirty marker and blindly retrying. An already
    /// verified v4 schema needs no work. Run `check_schema` before serving requests.
    ///
    /// # Errors
    ///
    /// Returns incompatible-track/catalog/identifier, missing-or-failed migration receipt,
    /// database or migration errors. Keep writers fenced until recovery and schema checks succeed.
    pub async fn upgrade_identifier_contract(&self) -> RepositoryResult<()> {
        let track: Option<String> = sqlx::query_scalar(
            "select value from keepsake_schema_metadata where key = 'api_track'",
        )
        .fetch_optional(&self.pool)
        .await?;
        if !matches!(track.as_deref(), Some("3" | "4")) {
            return Err(RepositoryError::BackendMismatch {
                expected: "tenant-activated domain schema 3 or 4",
                actual: "incompatible upgrade track".to_owned(),
            });
        }

        super::schema::postgres_clean_schema_preflight(&self.pool).await?;
        if track.as_deref() == Some("4") {
            let completed: Option<bool> =
                sqlx::query_scalar("select success from _sqlx_migrations where version = 4000")
                    .fetch_optional(&self.pool)
                    .await?;
            if completed != Some(true) {
                return Err(RepositoryError::BackendMismatch {
                    expected: "successful identifier migration receipt",
                    actual: "missing or failed migration receipt".to_owned(),
                });
            }
        }

        let mut migrator =
            Migrator::with_migrations(super::POSTGRES_V4_MIGRATOR.iter().cloned().collect());
        // The validated tenant-activation track may retain historical receipts.
        // Their migrations are deliberately not replayed by this additive step.
        migrator.set_ignore_missing(true);
        migrator.run(&self.pool).await?;
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
impl<C: RelationCache> super::SqliteKeepsakeRepository<C> {
    /// Completes an explicitly tenant-activated v3 database with the published v4 contract.
    ///
    /// Validates existing identifiers and catalog shape before applying the existing
    /// additive artifact. Never replays or fabricates a clean-baseline migration.
    /// Fence writers and retain a verified backup first; `MySQL` DDL is not transactional.
    /// A failed migration must be repaired using its recorded `SQLx` migration failure
    /// and backup, not by clearing the dirty marker and blindly retrying. An already
    /// verified v4 schema needs no work. Run `check_schema` before serving requests.
    ///
    /// # Errors
    ///
    /// Returns incompatible-track/catalog/identifier, missing-or-failed migration receipt,
    /// database or migration errors. Keep writers fenced until recovery and schema checks succeed.
    pub async fn upgrade_identifier_contract(&self) -> RepositoryResult<()> {
        let track: Option<String> = sqlx::query_scalar(
            "select value from keepsake_schema_metadata where key = 'api_track'",
        )
        .fetch_optional(&self.pool)
        .await?;
        if !matches!(track.as_deref(), Some("3" | "4")) {
            return Err(RepositoryError::BackendMismatch {
                expected: "tenant-activated domain schema 3 or 4",
                actual: "incompatible upgrade track".to_owned(),
            });
        }

        super::schema::sqlite_clean_schema_preflight(&self.pool).await?;
        if track.as_deref() == Some("4") {
            let completed: Option<bool> =
                sqlx::query_scalar("select success from _sqlx_migrations where version = 4000")
                    .fetch_optional(&self.pool)
                    .await?;
            if completed != Some(true) {
                return Err(RepositoryError::BackendMismatch {
                    expected: "successful identifier migration receipt",
                    actual: "missing or failed migration receipt".to_owned(),
                });
            }
        }

        let mut migrator =
            Migrator::with_migrations(super::SQLITE_V4_MIGRATOR.iter().cloned().collect());
        // The validated tenant-activation track may retain historical receipts.
        // Their migrations are deliberately not replayed by this additive step.
        migrator.set_ignore_missing(true);
        migrator.run(&self.pool).await?;
        Ok(())
    }
}

#[cfg(feature = "mysql")]
impl<C: RelationCache> super::MySqlKeepsakeRepository<C> {
    /// Completes an explicitly tenant-activated v3 database with the published v4 contract.
    ///
    /// Validates existing identifiers and catalog shape before applying the existing
    /// additive artifact. Never replays or fabricates a clean-baseline migration.
    /// Fence writers and retain a verified backup first; `MySQL` DDL is not transactional.
    /// A failed migration must be repaired using its recorded `SQLx` migration failure
    /// and backup, not by clearing the dirty marker and blindly retrying. An already
    /// verified v4 schema needs no work. Run `check_schema` before serving requests.
    ///
    /// # Errors
    ///
    /// Returns incompatible-track/catalog/identifier, missing-or-failed migration receipt,
    /// database or migration errors. Keep writers fenced until recovery and schema checks succeed.
    pub async fn upgrade_identifier_contract(&self) -> RepositoryResult<()> {
        let track: Option<String> = sqlx::query_scalar(
            "select value from keepsake_schema_metadata where `key` = 'api_track'",
        )
        .fetch_optional(&self.pool)
        .await?;
        if !matches!(track.as_deref(), Some("3" | "4")) {
            return Err(RepositoryError::BackendMismatch {
                expected: "tenant-activated domain schema 3 or 4",
                actual: "incompatible upgrade track".to_owned(),
            });
        }

        super::schema::mysql_clean_schema_preflight(&self.pool).await?;
        if track.as_deref() == Some("4") {
            let completed: Option<bool> =
                sqlx::query_scalar("select success from _sqlx_migrations where version = 4000")
                    .fetch_optional(&self.pool)
                    .await?;
            if completed != Some(true) {
                return Err(RepositoryError::BackendMismatch {
                    expected: "successful identifier migration receipt",
                    actual: "missing or failed migration receipt".to_owned(),
                });
            }
        }

        let mut migrator =
            Migrator::with_migrations(super::MYSQL_V4_MIGRATOR.iter().cloned().collect());
        // The validated tenant-activation track may retain historical receipts.
        // Their migrations are deliberately not replayed by this additive step.
        migrator.set_ignore_missing(true);
        migrator.run(&self.pool).await?;
        Ok(())
    }
}
