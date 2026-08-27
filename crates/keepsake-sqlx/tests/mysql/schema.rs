use super::support;
use super::support::*;
use keepsake_sqlx::MySqlKeepsakeRepository;
use sqlx::MySqlPool;

async fn seed_importer_evidence(pool: &MySqlPool, source: &str) -> TestResult<()> {
    sqlx::query("insert into keepsake_upgrade_evidence (evidence_id, evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete) values (1, 1, 'keepsake-dovecote-importer', ?, 'keepsake-sqlx-1.1', 'keepsake-audit', 0, 0, 0, 0, 0, 0, 0, 'keepsake.audit.json.v1', true)")
        .bind(source)
        .execute(pool)
        .await?;
    Ok(())
}

/// This ignored check deliberately mutates the configured integration
/// database; run it only against an isolated URL and alone.
#[tokio::test]
#[ignore = "requires an isolated MySQL/MariaDB URL; run explicitly with --ignored --test-threads=1"]
async fn catalog_check_rejects_changed_column_index_and_constraint() -> TestResult<()> {
    let _database_url = std::env::var("MYSQL_DATABASE_URL")?;
    let pool = support::mysql_pool().await?;
    support::reset_schema(&pool).await?;
    let repo = MySqlKeepsakeRepository::new(
        pool.clone(),
        "https://tests.invalid/keepsake/mysql-schema-catalog",
    )?;
    repo.migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_mysql::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    repo.check_schema().await?;
    let clean_state_checks = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.table_constraints where constraint_schema = database() and table_name = 'keepsakes' and constraint_name = 'keepsakes_state_check' and constraint_type = 'CHECK'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(clean_state_checks, 1);

    sqlx::query("alter table keepsakes modify subject_id varchar(190) not null")
        .execute(&pool)
        .await?;
    assert!(repo.check_schema().await.is_err());
    sqlx::query("alter table keepsakes modify subject_id varchar(191) not null")
        .execute(&pool)
        .await?;
    repo.check_schema().await?;

    sqlx::query("alter table keepsakes drop index keepsakes_active_subject_lookup")
        .execute(&pool)
        .await?;
    sqlx::query(
        "create index keepsakes_active_subject_lookup on keepsakes (subject_kind, subject_id)",
    )
    .execute(&pool)
    .await?;
    assert!(repo.check_schema().await.is_err());
    sqlx::query("drop index keepsakes_active_subject_lookup on keepsakes")
        .execute(&pool)
        .await?;
    sqlx::query(
        "create index keepsakes_active_subject_lookup on keepsakes (subject_kind, subject_id, relation_id, id)",
    )
    .execute(&pool)
    .await?;
    repo.check_schema().await?;

    let version = sqlx::query_scalar::<_, String>("select version()")
        .fetch_one(&pool)
        .await?;
    let drop_check = if version.to_ascii_lowercase().contains("mariadb") {
        "alter table keepsakes drop constraint keepsakes_lifecycle_timestamps"
    } else {
        "alter table keepsakes drop check keepsakes_lifecycle_timestamps"
    };
    sqlx::query(drop_check).execute(&pool).await?;
    sqlx::query(
        "alter table keepsakes add constraint keepsakes_lifecycle_timestamps check ((json_unquote(json_extract(expiry_policy, '$.type')) in ('manual_only', 'at', 'when_fulfilled')) and ((state = 'applied' and revoked_at is null and fulfilled_at is null) or (state = 'revoked' and revoked_at is not null and fulfilled_at is null) or (state = 'expired' and revoked_at is null)))",
    )
    .execute(&pool)
    .await?;
    assert!(repo.check_schema().await.is_err());
    sqlx::query(drop_check).execute(&pool).await?;
    sqlx::query(
        "alter table keepsakes add constraint keepsakes_lifecycle_timestamps check (((state = 'applied' and revoked_at is null and fulfilled_at is null) or (state = 'revoked' and revoked_at is not null and fulfilled_at is null) or (state = 'expired' and revoked_at is null and ((json_unquote(json_extract(expiry_policy, '$.type')) = 'at' and expires_at is not null and fulfilled_at is null) or (json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' and fulfilled_at is not null and expires_at is null)))) is true)",
    )
    .execute(&pool)
    .await?;
    repo.check_schema().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable MySQL or MariaDB URL; run explicitly with --ignored --test-threads=1"]
async fn upgrade_track_activates_after_importer_evidence() -> TestResult<()> {
    let _database_url = std::env::var("MYSQL_DATABASE_URL")?;
    let pool = support::mysql_pool().await?;
    support::reset_schema(&pool).await?;
    let repo =
        MySqlKeepsakeRepository::new(pool.clone(), "https://tests.invalid/keepsake/mysql-upgrade")?;
    repo.upgrade_migrate().await?;
    let version = sqlx::query_scalar::<_, String>("select version()")
        .fetch_one(&pool)
        .await?;
    let historical_state_name = if version.to_ascii_lowercase().contains("mariadb") {
        "state"
    } else {
        "keepsakes_chk_1"
    };
    let historical_state_checks = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.table_constraints where constraint_schema = database() and table_name = 'keepsakes' and constraint_name = ? and constraint_type = 'CHECK'")
        .bind(historical_state_name)
        .fetch_one(&pool)
        .await?;
    assert_eq!(historical_state_checks, 1);
    let historical_unique = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.table_constraints where constraint_schema = database() and table_name = 'keepsake_relation_definitions' and constraint_name = 'kind' and constraint_type = 'UNIQUE'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(historical_unique, 1);
    sqlx::raw_sql(dovecote_sqlx_mysql::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    seed_importer_evidence(&pool, "https://tests.invalid/keepsake/mysql-upgrade").await?;
    repo.activate_upgrade().await?;
    repo.check_schema().await?;
    Ok(())
}
