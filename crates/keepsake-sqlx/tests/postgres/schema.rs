use super::support::*;

async fn seed_importer_evidence(pool: &PgPool, source: &str) -> TestResult<()> {
    sqlx::query("insert into keepsake_upgrade_evidence (evidence_id, evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete) values (1, 1, 'keepsake-dovecote-importer', $1, 'keepsake-sqlx-1.1', 'keepsake-audit', 0, 0, 0, 0, 0, 0, 0, 'keepsake.audit.json.v1', true)")
        .bind(source)
        .execute(pool)
        .await?;
    Ok(())
}

async fn reset_schema_fixture(pool: &PgPool) -> TestResult<()> {
    sqlx::raw_sql(
        "drop table if exists dovecote_deliveries, dovecote_events, dovecote_schema, keepsake_upgrade_evidence, keepsake_dovecote_bridge_claims, keepsake_dovecote_bridge_ledger, keepsake_dovecote_bridge_config, keepsake_audit_outbox, keepsake_audit_context_attributes, keepsake_audit_events, keepsake_fulfillment_checklist, keepsake_fulfillment_counters, keepsakes, keepsake_relation_definitions, keepsake_schema_metadata cascade; drop table if exists _sqlx_migrations cascade",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// This ignored check deliberately mutates the configured integration
/// database; run it only against an isolated URL and alone.
#[tokio::test]
#[ignore = "requires an isolated PostgreSQL URL; run explicitly with --ignored --test-threads=1"]
async fn catalog_check_rejects_changed_column_index_and_constraint() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    reset_schema_fixture(&pool).await?;
    let repo = KeepsakeRepository::new(
        pool.clone(),
        "https://tests.invalid/keepsake/postgres-schema-catalog",
    )?;
    repo.migrate().await?;
    let has_dovecote =
        sqlx::query_scalar::<_, bool>("select to_regclass('public.dovecote_events') is not null")
            .fetch_one(&pool)
            .await?;
    if !has_dovecote {
        sqlx::raw_sql(dovecote_sqlx_postgres::MIGRATIONS[0].sql())
            .execute(&pool)
            .await?;
    }
    repo.check_schema().await?;

    sqlx::query(
        "alter table keepsakes alter column subject_id type varchar(190) using subject_id::varchar(190)",
    )
    .execute(&pool)
    .await?;
    assert!(repo.check_schema().await.is_err());
    sqlx::query("alter table keepsakes alter column subject_id type text using subject_id::text")
        .execute(&pool)
        .await?;
    repo.check_schema().await?;

    sqlx::query("drop index keepsakes_active_subject_lookup")
        .execute(&pool)
        .await?;
    sqlx::query(
        "create index keepsakes_active_subject_lookup on keepsakes (subject_kind, subject_id) where state = 'applied'",
    )
    .execute(&pool)
    .await?;
    assert!(repo.check_schema().await.is_err());
    sqlx::query("drop index keepsakes_active_subject_lookup")
        .execute(&pool)
        .await?;
    sqlx::query(
        "create index keepsakes_active_subject_lookup on keepsakes (subject_kind, subject_id, relation_id, id) where state = 'applied'",
    )
    .execute(&pool)
    .await?;
    repo.check_schema().await?;

    sqlx::query("alter table keepsakes drop constraint keepsakes_lifecycle_timestamps")
        .execute(&pool)
        .await?;
    sqlx::query(
        "alter table keepsakes add constraint keepsakes_lifecycle_timestamps check ((state = 'applied' and revoked_at is null and fulfilled_at is null) or (state = 'revoked' and revoked_at is not null and fulfilled_at is null) or (state = 'expired' and revoked_at is null))",
    )
    .execute(&pool)
    .await?;
    assert!(repo.check_schema().await.is_err());
    sqlx::query("alter table keepsakes drop constraint keepsakes_lifecycle_timestamps")
        .execute(&pool)
        .await?;
    sqlx::query(
        "alter table keepsakes add constraint keepsakes_lifecycle_timestamps check (coalesce((state = 'applied' and revoked_at is null and fulfilled_at is null) or (state = 'revoked' and revoked_at is not null and fulfilled_at is null) or (state = 'expired' and revoked_at is null and ((expiry_policy->>'type' = 'at' and expires_at is not null and fulfilled_at is null) or (expiry_policy->>'type' = 'when_fulfilled' and fulfilled_at is not null and expires_at is null))), false))",
    )
    .execute(&pool)
    .await?;
    repo.check_schema().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL URL; run explicitly with --ignored --test-threads=1"]
async fn upgrade_track_activates_after_importer_evidence() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(
        pool.clone(),
        "https://tests.invalid/keepsake/postgres-upgrade",
    )?;
    // The clean and upgrade tracks share SQLx's metadata table. Isolate this
    // destructive upgrade-path test so a preceding clean test cannot make
    // `upgrade_migrate` believe the historical track is already applied.
    reset_schema_fixture(&pool).await?;
    repo.upgrade_migrate().await?;
    let has_dovecote =
        sqlx::query_scalar::<_, bool>("select to_regclass('public.dovecote_events') is not null")
            .fetch_one(&pool)
            .await?;
    if !has_dovecote {
        sqlx::raw_sql(dovecote_sqlx_postgres::MIGRATIONS[0].sql())
            .execute(&pool)
            .await?;
    }
    seed_importer_evidence(&pool, "https://tests.invalid/keepsake/postgres-upgrade").await?;
    repo.activate_upgrade().await?;
    repo.check_schema().await?;
    Ok(())
}
