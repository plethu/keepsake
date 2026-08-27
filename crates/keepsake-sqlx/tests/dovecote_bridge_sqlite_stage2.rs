#![allow(missing_docs)]

//! Second half of the `SQLite` migration compatibility fixture.
//!
//! Cargo compiles this target with the bridge feature disabled.  It then
//! reopens the file produced by the bridge-enabled process and runs the same
//! public migration API.  `SQLx` therefore validates the known bridge migration
//! (including its checksum) without linking Dovecote into the legacy binary.

#![cfg(not(feature = "dovecote-sqlite"))]

use std::{env, path::PathBuf};

use keepsake_sqlx::SqliteKeepsakeRepository;
use sqlx::{Row, sqlite::SqlitePoolOptions};

const DATABASE_ENV: &str = "KEEPSAKE_SQLITE_MIGRATION_COMPAT_DB";

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn bridge_disabled_migration_accepts_the_existing_file_backed_schema() -> TestResult<()> {
    let Some(path) = database_path() else {
        // The ordinary all-feature test run has no cross-process fixture path.
        // The canonical project gate supplies one explicitly.
        return Ok(());
    };

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false),
        )
        .await?;
    let repository = SqliteKeepsakeRepository::new(pool.clone());

    // This call is compiled without dovecote-sqlite.  The compatibility path
    // must retain SQLx's unknown-version and checksum validation.
    repository.migrate().await?;

    let migration = sqlx::query("select version, checksum from _sqlx_migrations where version = 6")
        .fetch_one(&pool)
        .await?;
    assert_eq!(migration.try_get::<i64, _>("version")?, 6);
    assert!(!migration.try_get::<Vec<u8>, _>("checksum")?.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from sqlite_master where type = 'table' and name = 'keepsake_dovecote_bridge_config'",
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    Ok(())
}

fn database_path() -> Option<PathBuf> {
    env::var_os(DATABASE_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}
