#![allow(missing_docs)]

//! First half of the `SQLite` migration compatibility fixture.
//!
//! This target is intentionally run in a process compiled with the bridge
//! dependency enabled.  The second target opens the same file after being
//! compiled without that dependency; keeping the targets separate exercises
//! the feature-disabled migration path rather than a same-process helper.

#![cfg(feature = "dovecote-sqlite")]

use std::{env, path::PathBuf};

use keepsake_sqlx::SqliteKeepsakeRepository;
use sqlx::{Row, sqlite::SqlitePoolOptions};

const DATABASE_ENV: &str = "KEEPSAKE_SQLITE_MIGRATION_COMPAT_DB";

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn bridge_enabled_migration_creates_the_file_backed_schema() -> TestResult<()> {
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
                .create_if_missing(true),
        )
        .await?;
    let repository = SqliteKeepsakeRepository::new(pool.clone());

    // Call the public repository migration entry point.  Do not invoke a
    // private migrator or duplicate its feature selection in this fixture.
    repository.migrate().await?;

    let migrations = sqlx::query(
        "select version, checksum from _sqlx_migrations where version in (1, 2, 3, 4, 5, 6) order by version",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(migrations.len(), 6);
    assert_eq!(migrations[5].try_get::<i64, _>("version")?, 6);
    assert!(!migrations[5].try_get::<Vec<u8>, _>("checksum")?.is_empty());

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
