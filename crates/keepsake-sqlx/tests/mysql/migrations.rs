use super::support::*;

use keepsake_sqlx::{MySqlKeepsakeRepository, RepositoryError};

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_migration_initializes_backend_marker() -> TestResult<()> {
    backend_cases::migration_initializes_backend_marker::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
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

    let repo = MySqlKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/mysql")?;
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

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_runtime_check_rejects_v3_track() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/v3/mysql/3000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    let repo = MySqlKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/mysql-v4")?;

    let result = repo.check_schema().await;
    assert!(matches!(
        result,
        Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual
        }) if actual.contains("3.0 API track")
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_v3_preflight_rejects_invalid_utf8() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/v3/mysql/3000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(
        "insert into keepsake_relation_definitions (tenant_id, id, kind, `key`, enabled, expiry_policy, created_at, updated_at) values (?, ?, ?, ?, true, ?, ?, ?)",
    )
    .bind(vec![0xff_u8])
    .bind("00000000-0000-0000-0000-000000000001")
    .bind("tag")
    .bind("migration-test")
    .bind("{\"type\":\"manual_only\"}")
    .bind("2026-01-01 00:00:00")
    .bind("2026-01-01 00:00:00")
    .execute(&pool)
    .await?;

    let repo = MySqlKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/mysql-v4")?;
    let result = repo.migrate().await;
    assert!(result.is_err(), "invalid v3 row must block v4");
    let Some(error) = result.err() else {
        return Ok(());
    };
    assert!(
        matches!(error, RepositoryError::BackendMismatch { ref actual, .. } if actual.contains("invalid UTF-8")),
        "{error}"
    );
    Ok(())
}
