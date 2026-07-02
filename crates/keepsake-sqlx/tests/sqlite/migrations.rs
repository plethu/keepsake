use super::support::*;

use keepsake_sqlx::{RepositoryError, SqliteKeepsakeRepository};
use sqlx::sqlite::SqlitePoolOptions;

#[tokio::test]
async fn sqlite_migration_initializes_backend_marker() -> TestResult<()> {
    backend_cases::migration_initializes_backend_marker::<SqliteHarness>().await
}
#[tokio::test]
async fn sqlite_migration_rejects_wrong_backend_marker() -> TestResult<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::query(
        "create table keepsake_schema_metadata (key text primary key, value text not null)",
    )
    .execute(&pool)
    .await?;
    sqlx::query("insert into keepsake_schema_metadata (key, value) values ('backend', 'postgres')")
        .execute(&pool)
        .await?;

    let repo = SqliteKeepsakeRepository::new(pool);
    let result = repo.migrate().await;

    assert!(matches!(
        result,
        Err(RepositoryError::BackendMismatch {
            expected: "sqlite",
            actual
        }) if actual == "postgres"
    ));
    Ok(())
}
