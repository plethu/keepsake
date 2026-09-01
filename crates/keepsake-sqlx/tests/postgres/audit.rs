use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, AuditEvent, AuditEventId, AuditEventType, CommandContext,
    RevokeBySubject, SubjectRef,
};
use sqlx::Row;

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn lifecycle_events_are_typed_dovecote_rows() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    reset_schema(&pool).await?;
    let root = KeepsakeRepository::new(pool.clone(), "https://tests.invalid/keepsake/postgres")?;
    root.migrate().await?;
    if sqlx::query_scalar::<_, bool>("select to_regclass('public.dovecote_events') is not null")
        .fetch_one(&pool)
        .await?
    {
        sqlx::query("truncate table dovecote_deliveries, dovecote_events restart identity")
            .execute(&pool)
            .await?;
    } else {
        sqlx::raw_sql(dovecote_sqlx_postgres::MIGRATIONS[0].sql())
            .execute(&pool)
            .await?;
    }
    reset_database(&pool).await?;
    let repo = root.for_tenant(test_tenant());

    let relation = timed_relation(&repo, "dovecote-audit", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("audit_{}", Uuid::now_v7()))?;
    let apply_at = ts("2026-01-01T00:01:00.123456Z")?;
    let apply_id = AuditEventId::deterministic(b"postgres-apply");
    let command = ApplyKeepsake::new(
        test_tenant(),
        subject.clone(),
        relation.id,
        apply_at,
        CommandContext::new(ActorRef::new("test", "worker")?)
            .with_idempotency_key("req-1")
            .with_metadata("reason", "support"),
    )
    .with_audit_id(apply_id);
    let applied = repo.apply(&command).await?;
    let revoke_id = AuditEventId::deterministic(b"postgres-revoke");
    repo.revoke_by_subject(
        &RevokeBySubject::new(
            test_tenant(),
            subject,
            relation.id,
            ts("2026-01-01T00:02:00Z")?,
            CommandContext::new(ActorRef::new("test", "moderator")?)
                .with_metadata("reason", "appeal"),
        )
        .with_audit_id(revoke_id),
    )
    .await?;

    let rows = sqlx::query(
        "select stream, event_id, source, event_type, occurred_at, datacontenttype, data from dovecote_events order by row_id",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<String, _>("stream")?, "keepsake-audit");
    assert_eq!(
        rows[0].try_get::<String, _>("source")?,
        "https://tests.invalid/keepsake/postgres"
    );
    assert_eq!(
        rows[0].try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        rows[0].try_get::<OffsetDateTime, _>("occurred_at")?,
        apply_at
    );
    assert_eq!(
        rows[0].try_get::<String, _>("datacontenttype")?,
        "application/json"
    );
    assert_eq!(
        rows[0].try_get::<String, _>("event_id")?,
        format!("keepsake-audit-{}", apply_id.as_uuid())
    );
    let apply: AuditEvent = serde_json::from_slice(&rows[0].try_get::<Vec<u8>, _>("data")?)?;
    assert_eq!(apply.id, apply_id);
    assert_eq!(apply.keepsake_id, applied.keepsake.id());
    assert_eq!(apply.event_type, AuditEventType::Apply);
    assert_eq!(apply.at, apply_at);
    let revoke: AuditEvent = serde_json::from_slice(&rows[1].try_get::<Vec<u8>, _>("data")?)?;
    assert_eq!(revoke.id, revoke_id);
    assert_eq!(revoke.event_type, AuditEventType::Revoke);
    let states = sqlx::query_scalar::<_, String>(
        "select state from dovecote_deliveries order by event_row_id",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(states, vec!["pending", "pending"]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn exact_replay_is_idempotent_and_changed_content_conflicts() -> TestResult<()> {
    let root = repo().await?;
    let repo = root.for_tenant(test_tenant());
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let relation = timed_relation(&repo, "dovecote-replay", "2026-01-02T00:00:00Z").await?;
    let id = AuditEventId::deterministic(b"postgres-replay");
    let command = ApplyKeepsake::new(
        test_tenant(),
        SubjectRef::new("user", format!("replay_{}", Uuid::now_v7()))?,
        relation.id,
        ts("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    )
    .with_audit_id(id);
    repo.apply(&command).await?;
    repo.apply(&command).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        1
    );
    let changed = ApplyKeepsake::new(
        test_tenant(),
        command.subject.clone(),
        relation.id,
        command.at,
        CommandContext::new(ActorRef::new("test", "changed")?),
    )
    .with_audit_id(id);
    assert!(repo.apply(&changed).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn exact_replay_rejects_payload_tenant_mismatch() -> TestResult<()> {
    let root = repo().await?;
    let repo = root.for_tenant(test_tenant());
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let relation =
        timed_relation(&repo, "dovecote-tenant-mismatch", "2026-01-02T00:00:00Z").await?;
    let id = AuditEventId::deterministic(b"postgres-tenant-mismatch");
    let command = ApplyKeepsake::new(
        test_tenant(),
        SubjectRef::new("user", "postgres_tenant_mismatch")?,
        relation.id,
        ts("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    )
    .with_audit_id(id);
    repo.apply(&command).await?;

    let event_id = format!("keepsake-audit-{}", id.as_uuid());
    let payload = sqlx::query_scalar::<_, Vec<u8>>(
        "select data from dovecote_events where tenant_id = $1 and source = $2 and event_id = $3",
    )
    .bind(test_tenant().as_str())
    .bind("https://tests.invalid/keepsake/postgres")
    .bind(&event_id)
    .fetch_one(&pool)
    .await?;
    let mut payload: serde_json::Value = serde_json::from_slice(&payload)?;
    payload["tenant_id"] = serde_json::Value::String("tenant-other".to_owned());
    sqlx::query(
        "update dovecote_events set data = $1 where tenant_id = $2 and source = $3 and event_id = $4",
    )
    .bind(serde_json::to_vec(&payload)?)
    .bind(test_tenant().as_str())
    .bind("https://tests.invalid/keepsake/postgres")
    .bind(event_id)
    .execute(&pool)
    .await?;

    let result = repo.apply(&command).await;
    assert!(matches!(
        result,
        Err(keepsake_sqlx::RepositoryError::AuditPayload(
            keepsake_sqlx::AuditEventDecodeError::TenantMismatch {
                storage_tenant,
                payload_tenant,
            }
        )) if storage_tenant == "tenant-test" && payload_tenant == "tenant-other"
    ));
    Ok(())
}
