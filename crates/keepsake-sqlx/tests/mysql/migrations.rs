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

#[tokio::test]
#[ignore = "requires disposable MySQL database"]
async fn mysql_identifier_upgrade_preserves_terminal_history_and_receipt() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/v3/mysql/3000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    sqlx::query("insert into keepsake_relation_definitions (tenant_id,id,kind,`key`,enabled,expiry_policy,created_at,updated_at) values ('tenant-upgrade','00000000-0000-0000-0000-000000000001','block','social',1,'{\"type\":\"manual_only\"}','2026-01-01 00:00:00.000000','2026-01-01 00:00:00.000000')")
        .execute(&pool).await?;
    sqlx::query("insert into keepsakes (tenant_id,id,subject_kind,subject_id,relation_id,state,expiry_policy,applied_at,expires_at,fulfilled_at,revoked_at,metadata,created_at,updated_at) values ('tenant-upgrade','00000000-0000-0000-0000-000000000002','directed-pair','a:b','00000000-0000-0000-0000-000000000001','revoked','{\"type\":\"manual_only\"}','2026-01-01 00:00:00.000000',null,null,'2026-01-01 00:00:00.000000','{\"origin\":\"published-v3\"}','2026-01-01 00:00:00.000000','2026-01-01 00:00:00.000000')")
        .execute(&pool).await?;
    let root =
        MySqlKeepsakeRepository::new(pool.clone(), "https://tests.invalid/identifier-upgrade")?;
    root.upgrade_identifier_contract().await?;
    root.upgrade_identifier_contract().await?;
    sqlx::raw_sql(
        dovecote_sqlx_mysql::MIGRATIONS
            .first()
            .ok_or(sqlx::Error::RowNotFound)?
            .sql(),
    )
    .execute(&pool)
    .await?;
    root.check_schema().await?;
    let scoped = root.for_tenant(keepsake::TenantId::new("tenant-upgrade")?);
    let mut tx = pool.begin().await?;
    let observation = scoped
        .observe_in_transaction(
            &mut tx,
            &keepsake::SubjectRef::new("directed-pair", "a:b")?,
            uuid::Uuid::from_u128(1),
        )
        .await?;
    assert_eq!(observation.history().len(), 1);
    assert_eq!(observation.history()[0].id(), uuid::Uuid::from_u128(2));
    assert_eq!(
        observation.history()[0].state(),
        keepsake::LifecycleState::Revoked
    );
    assert_eq!(
        observation.history()[0]
            .metadata()
            .get("origin")
            .map(String::as_str),
        Some("published-v3")
    );
    assert!(observation.active_relation()?.is_none());
    tx.rollback().await?;
    let events: i64 = sqlx::query_scalar("select count(*) from dovecote_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(events, 0);
    let baseline: i64 =
        sqlx::query_scalar("select count(*) from _sqlx_migrations where version = 3000")
            .fetch_one(&pool)
            .await?;
    assert_eq!(baseline, 0);
    sqlx::query("update _sqlx_migrations set success = false where version = 4000")
        .execute(&pool)
        .await?;
    assert!(matches!(
        root.upgrade_identifier_contract().await,
        Err(RepositoryError::BackendMismatch { .. })
    ));
    Ok(())
}
