use super::support::*;

async fn seed_importer_evidence(pool: &PgPool, source: &str) -> TestResult<()> {
    sqlx::query("insert into keepsake_upgrade_evidence (evidence_id, evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete) values (1, 1, 'keepsake-dovecote-importer', $1, 'keepsake-sqlx-1.1', 'keepsake-audit', 0, 0, 0, 0, 0, 0, 0, 'keepsake.audit.json.v1', true)")
        .bind(source)
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
    reset_schema(&pool).await?;
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
    sqlx::query(
        "alter table keepsakes alter column subject_id type text collate \"C\" using subject_id::text",
    )
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
        "create index keepsakes_active_subject_lookup on keepsakes (tenant_id, subject_kind, subject_id, relation_id, id) where state = 'applied'",
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
    reset_schema(&pool).await?;
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
    assert!(repo.check_schema().await.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL URL; run explicitly with --ignored --test-threads=1"]
async fn postgres_v3_preflight_rejects_unicode_edge_whitespace() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    reset_schema(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/v3/postgres/3000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    let at = ts("2026-01-01T00:00:00Z")?;
    sqlx::query(
        "insert into keepsake_relation_definitions (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at) values ($1, $2, $3, $4, true, $5, $6, $6)",
    )
    .bind("tenant-a")
    .bind(Uuid::from_u128(1))
    .bind("\u{2003}tag")
    .bind("migration-test")
    .bind(serde_json::json!({"type": "manual_only"}))
    .bind(at)
    .execute(&pool)
    .await?;

    let repo = KeepsakeRepository::new(pool, "https://tests.invalid/keepsake/postgres-v4")?;
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
#[ignore = "requires an isolated PostgreSQL URL; run explicitly with --ignored --test-threads=1"]
async fn postgres_runtime_check_rejects_v3_track() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    reset_schema(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/v3/postgres/3000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    let repo = KeepsakeRepository::new(pool, "https://tests.invalid/keepsake/postgres-v4")?;

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

async fn seed_v2_tenant_upgrade_fixture(pool: &PgPool) -> TestResult<Uuid> {
    let relation_id = Uuid::from_u128(42);
    let keepsake_id = Uuid::from_u128(43);
    let observed_at = ts("2026-01-01T00:00:00Z")?;
    let expiry_policy = serde_json::json!({"type": "manual_only"});
    sqlx::raw_sql(include_str!(
        "../../migrations/v2/postgres/2000_clean_baseline.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into keepsake_relation_definitions (id, kind, key, enabled, expiry_policy, created_at, updated_at) values ($1, 'tag', 'mapped', true, $2, $3, $3)",
    )
    .bind(relation_id)
    .bind(expiry_policy.clone())
    .bind(observed_at)
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into keepsakes (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at) values ($1, 'account', 'legacy-subject', $2, 'applied', $3, $4, null, null, null, $5, $4, $4)",
    )
    .bind(keepsake_id)
    .bind(relation_id)
    .bind(expiry_policy)
    .bind(observed_at)
    .bind(serde_json::json!({"origin": "v2-fixture"}))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into keepsake_fulfillment_counters (keepsake_id, key, value, observed_at) values ($1, 'review', 1, $2)",
    )
    .bind(keepsake_id)
    .bind(observed_at)
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into keepsake_fulfillment_checklist (keepsake_id, item, complete, observed_at) values ($1, 'identity', true, $2)",
    )
    .bind(keepsake_id)
    .bind(observed_at)
    .execute(pool)
    .await?;
    Ok(relation_id)
}

/// This ignored check deliberately mutates the configured integration
/// database; run it only against an isolated URL and alone.
#[tokio::test]
#[ignore = "requires an isolated PostgreSQL URL; run explicitly with --ignored --test-threads=1"]
async fn tenant_upgrade_activates_v3_schema_with_explicit_backfill() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    reset_schema(&pool).await?;
    let relation_id = seed_v2_tenant_upgrade_fixture(&pool).await?;

    let repo = KeepsakeRepository::new(
        pool.clone(),
        "https://tests.invalid/keepsake/postgres-tenant-upgrade",
    )?;
    repo.prepare_tenant_upgrade().await?;
    assert!(repo.activate_tenant_upgrade().await.is_err());

    let tenant = TenantId::new("tenant-upgrade")?;
    sqlx::query("update keepsake_relation_definitions set tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&pool)
        .await?;
    sqlx::query("update keepsakes set tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&pool)
        .await?;
    sqlx::query("update keepsake_fulfillment_counters set tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&pool)
        .await?;
    // The checklist remains unmapped until this final explicit backfill, so
    // activation must continue to reject the prepared schema.
    assert!(repo.activate_tenant_upgrade().await.is_err());
    sqlx::query("update keepsake_fulfillment_checklist set tenant_id = $1")
        .bind(tenant.as_str())
        .execute(&pool)
        .await?;

    repo.activate_tenant_upgrade().await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "select value from keepsake_schema_metadata where key = 'api_track'",
        )
        .fetch_one(&pool)
        .await?,
        "3"
    );
    sqlx::raw_sql(dovecote_sqlx_postgres::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    assert!(repo.check_schema().await.is_err());

    let scoped = repo.for_tenant(tenant.clone());
    assert_eq!(
        scoped
            .relation_by_id(relation_id)
            .await?
            .map(|relation| relation.tenant_id),
        Some(tenant.clone())
    );
    assert!(
        repo.for_tenant(TenantId::new("other-tenant")?)
            .relation_by_id(relation_id)
            .await?
            .is_none()
    );

    let subject = SubjectRef::new("account", "post-upgrade-subject")?;
    let command = ApplyKeepsake::new(
        tenant.clone(),
        subject.clone(),
        relation_id,
        ts("2026-01-01T00:05:00Z")?,
        test_context("upgrade-test")?,
    );
    let applied = scoped.apply(&command).await?;
    assert_eq!(applied.keepsake.tenant_id(), &tenant);
    assert_eq!(scoped.active_for_subject(&subject).await?.len(), 1);
    Ok(())
}
