use super::*;

pub async fn single_connection_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
}

pub async fn reset_database(pool: &PgPool) -> TestResult<()> {
    sqlx::query(
        r"
        truncate table
            keepsake_audit_context_attributes,
            keepsake_audit_events,
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

pub async fn audit_rows_for_keepsake(
    pool: &PgPool,
    keepsake_id: Uuid,
) -> TestResult<Vec<AuditRow>> {
    Ok(sqlx::query_as::<_, AuditRow>(
        r"
        select id, event_type, actor_kind, actor_id, decision, occurred_at
        from keepsake_audit_events
        where keepsake_id = $1
        order by id
        ",
    )
    .bind(keepsake_id)
    .fetch_all(pool)
    .await?)
}

pub async fn audit_attributes(
    pool: &PgPool,
    audit_event_id: i64,
) -> TestResult<BTreeMap<String, String>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r"
        select key, value
        from keepsake_audit_context_attributes
        where audit_event_id = $1
        order by key
        ",
    )
    .bind(audit_event_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
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
          (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
           expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values ($1, 'user', $2, $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $6, $6)
        ",
    )
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
        where id = $1
        for share
        ",
    )
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
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.relation_id = $1
          and k.state = 'applied'
          and r.enabled
          and k.expires_at is not null
        order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
        limit 1
        for update of k skip locked
        for share of r
        ",
    )
    .bind(relation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
