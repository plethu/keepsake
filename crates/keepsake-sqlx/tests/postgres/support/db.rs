use super::*;

use std::sync::OnceLock;

const POSTGRES_TEST_DATABASE_LOCK_KEY: i64 = 0x4b45_4550_5341_4b45;
static POSTGRES_TEST_DATABASE_LOCK: OnceLock<()> = OnceLock::new();

pub async fn single_connection_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
}

/// Drops all Keepsake and Dovecote test objects so every integration test can
/// select its migration track independently of the preceding test.
///
/// The first reset in a test process also takes a session-level advisory lock
/// on the configured database and holds the connection until process exit.
/// This serializes separate `cargo test` invocations targeting the same
/// disposable URL; `--test-threads=1` remains required for ordering within one
/// process. The connection is deliberately leaked because returning it to the
/// pool would retain the session lock for an unrelated future checkout.
pub async fn reset_schema(pool: &PgPool) -> TestResult<()> {
    if POSTGRES_TEST_DATABASE_LOCK.get().is_none() {
        let mut connection = pool.acquire().await?;
        sqlx::query("select pg_advisory_lock($1::bigint)")
            .bind(POSTGRES_TEST_DATABASE_LOCK_KEY)
            .execute(&mut *connection)
            .await?;
        if POSTGRES_TEST_DATABASE_LOCK.set(()).is_ok() {
            let _ = Box::leak(Box::new(connection.leak()));
        } else {
            connection.close().await?;
        }
    }
    sqlx::raw_sql(
        "drop table if exists dovecote_deliveries, dovecote_events, dovecote_schema, keepsake_upgrade_evidence, keepsake_dovecote_bridge_claims, keepsake_dovecote_bridge_ledger, keepsake_dovecote_bridge_config, keepsake_audit_outbox, keepsake_audit_context_attributes, keepsake_audit_events, keepsake_fulfillment_checklist, keepsake_fulfillment_counters, keepsakes, keepsake_relation_definitions, keepsake_schema_metadata cascade; drop table if exists _sqlx_migrations cascade",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn reset_database(pool: &PgPool) -> TestResult<()> {
    let has_dovecote =
        sqlx::query_scalar::<_, bool>("select to_regclass('public.dovecote_events') is not null")
            .fetch_one(pool)
            .await?;
    if !has_dovecote {
        sqlx::raw_sql(dovecote_sqlx_postgres::MIGRATIONS[0].sql())
            .execute(pool)
            .await?;
    }
    sqlx::query(
        r"
        truncate table
            dovecote_deliveries,
            dovecote_events,
            keepsake_fulfillment_checklist,
            keepsake_fulfillment_counters,
            keepsakes,
            keepsake_relation_definitions
        restart identity cascade
        ",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_raw_keepsake(
    pool: &PgPool,
    relation_id: Uuid,
    expiry: &ExpiryPolicy,
    state: &str,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
) -> TestResult<()> {
    insert_raw_keepsake_value(
        pool,
        relation_id,
        serde_json::to_value(expiry)?,
        state,
        expires_at,
        fulfilled_at,
        revoked_at,
    )
    .await
}

pub async fn insert_raw_keepsake_value(
    pool: &PgPool,
    relation_id: Uuid,
    expiry_policy: serde_json::Value,
    state: &str,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
) -> TestResult<()> {
    sqlx::query(
        r"
        insert into keepsakes
          (tenant_id, id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
           expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values ($1, $2, 'user', $3, $4, $5, $6, $7, $8, $9, $10, '{}'::jsonb, $7, $7)
        ",
    )
    .bind(test_tenant().as_str())
    .bind(Uuid::now_v7())
    .bind(format!("invalid_{}", Uuid::now_v7()))
    .bind(relation_id)
    .bind(state)
    .bind(expiry_policy)
    .bind(ts("2026-01-01T00:00:00Z")?)
    .bind(expires_at)
    .bind(fulfilled_at)
    .bind(revoked_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_lock_timeout(pool: &PgPool, timeout: &str) -> TestResult<()> {
    sqlx::query("select set_config('lock_timeout', $1, false)")
        .bind(timeout)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn lock_relation_for_share(
    tx: &mut Transaction<'_, Postgres>,
    relation_id: Uuid,
) -> TestResult<()> {
    sqlx::query(
        r"
        select id
        from keepsake_relation_definitions
        where tenant_id = $1 and id = $2
        for share
        ",
    )
    .bind(test_tenant().as_str())
    .bind(relation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn lock_due_keepsake_and_relation_for_expiry(
    tx: &mut Transaction<'_, Postgres>,
    relation_id: Uuid,
) -> TestResult<()> {
    sqlx::query(
        r"
        select k.id
        from keepsakes k
        join keepsake_relation_definitions r
          on r.tenant_id = k.tenant_id and r.id = k.relation_id
        where k.tenant_id = $1 and k.relation_id = $2
          and k.state = 'applied'
          and r.enabled
          and k.expires_at is not null
        order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
        limit 1
        for update of k skip locked
        for share of r
        ",
    )
    .bind(test_tenant().as_str())
    .bind(relation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
