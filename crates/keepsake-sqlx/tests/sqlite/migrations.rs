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

    let repo = SqliteKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/sqlite")?;
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

async fn sqlite_v3_pool() -> TestResult<sqlx::SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/v3/sqlite/3000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(pool)
}

#[tokio::test]
async fn sqlite_runtime_check_rejects_v3_track() -> TestResult<()> {
    let pool = sqlite_v3_pool().await?;
    let repo = SqliteKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/sqlite-v4")?;

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

async fn insert_v3_relation(
    pool: &sqlx::SqlitePool,
    tenant_id: impl Into<String>,
    kind: &str,
) -> TestResult<()> {
    sqlx::query(
        "insert into keepsake_relation_definitions (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at) values (?, ?, ?, ?, 1, ?, ?, ?)",
    )
    .bind(tenant_id.into())
    .bind("00000000-0000-0000-0000-000000000001")
    .bind(kind)
    .bind("migration-test")
    .bind("{\"type\":\"manual_only\"}")
    .bind("2026-01-01T00:00:00Z")
    .bind("2026-01-01T00:00:00Z")
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn sqlite_v3_preflight_rejects_unicode_edge_whitespace() -> TestResult<()> {
    let pool = sqlite_v3_pool().await?;
    insert_v3_relation(&pool, "tenant-a", "\u{2003}tag").await?;
    let repo = SqliteKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/sqlite-v4")?;

    let result = repo.migrate().await;
    assert!(result.is_err(), "invalid v3 row must block v4");
    let Some(error) = result.err() else {
        return Ok(());
    };
    assert!(
        matches!(error, RepositoryError::BackendMismatch { ref actual, .. } if actual.contains("relation.kind") && actual.contains("leading or trailing whitespace")),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_v3_preflight_rejects_invalid_utf8_and_dynamic_types() -> TestResult<()> {
    let pool = sqlite_v3_pool().await?;
    sqlx::query(
        "insert into keepsake_relation_definitions (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at) values (?, ?, cast(x'ff' as blob), ?, 1, ?, ?, ?)",
    )
    .bind("tenant-a")
    .bind("00000000-0000-0000-0000-000000000001")
    .bind("migration-test")
    .bind("{\"type\":\"manual_only\"}")
    .bind("2026-01-01T00:00:00Z")
    .bind("2026-01-01T00:00:00Z")
    .execute(&pool)
    .await?;
    let repo = SqliteKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/sqlite-v4")?;

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

#[tokio::test]
async fn sqlite_v3_preflight_rejects_valid_utf8_blob_type() -> TestResult<()> {
    let pool = sqlite_v3_pool().await?;
    sqlx::query(
        "insert into keepsake_relation_definitions (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at) values (?, ?, cast('tag' as blob), ?, 1, ?, ?, ?)",
    )
    .bind("tenant-a")
    .bind("00000000-0000-0000-0000-000000000001")
    .bind("migration-test")
    .bind("{\"type\":\"manual_only\"}")
    .bind("2026-01-01T00:00:00Z")
    .bind("2026-01-01T00:00:00Z")
    .execute(&pool)
    .await?;
    let repo = SqliteKeepsakeRepository::new(pool, "https://tests.invalid/keepsake/sqlite-v4")?;

    let result = repo.migrate().await;
    assert!(result.is_err(), "non-text v3 row must block v4");
    let Some(error) = result.err() else {
        return Ok(());
    };
    assert!(
        matches!(error, RepositoryError::BackendMismatch { ref actual, .. } if actual.contains("expected text, found blob")),
        "{error}"
    );
    Ok(())
}
