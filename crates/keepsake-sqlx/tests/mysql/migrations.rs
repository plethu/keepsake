use super::support::*;

use keepsake_sqlx::{MySqlKeepsakeRepository, RepositoryError};

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_migration_initializes_backend_marker() -> TestResult<()> {
    backend_cases::migration_initializes_backend_marker::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_migration_rejects_wrong_backend_marker() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    sqlx::query(
        "create table keepsake_schema_metadata (`key` varchar(191) primary key, value varchar(191) not null)",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "insert into keepsake_schema_metadata (`key`, value) values ('backend', 'postgres')",
    )
    .execute(&pool)
    .await?;

    let repo = MySqlKeepsakeRepository::new(pool);
    let result = repo.migrate().await;

    assert!(matches!(
        result,
        Err(RepositoryError::BackendMismatch {
            expected: "mysql",
            actual
        }) if actual == "postgres"
    ));
    Ok(())
}
