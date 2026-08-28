use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, AuditEvent, AuditEventId, AuditEventType, CommandContext,
    ExpiryPolicy, RevokeBySubject, SubjectRef,
};
use sqlx::Row;

#[tokio::test]
async fn sqlite_lifecycle_events_are_typed_dovecote_rows() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "sqlite_acct_audit")?;
    let apply_at = ts("2026-01-01T00:01:00.123456Z")?;
    let apply_id = AuditEventId::deterministic(b"sqlite-apply");
    let command = ApplyKeepsake::new(
        SqliteHarness::tenant(),
        subject.clone(),
        relation.id,
        apply_at,
        CommandContext::new(ActorRef::new("test", "worker")?)
            .with_idempotency_key("req-1")
            .with_metadata("reason", "support"),
    )
    .with_audit_id(apply_id);
    let applied = repo.apply(&command).await?;

    let revoke_id = AuditEventId::deterministic(b"sqlite-revoke");
    repo.revoke_by_subject(
        &RevokeBySubject::new(
            SqliteHarness::tenant(),
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
        "https://tests.invalid/keepsake/sqlite"
    );
    assert_eq!(
        rows[0].try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        rows[0].try_get::<String, _>("occurred_at")?,
        "2026-01-01T00:01:00.123456Z"
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
    assert_eq!(
        apply.context.attributes.get("reason").map(String::as_str),
        Some("support")
    );

    let revoke: AuditEvent = serde_json::from_slice(&rows[1].try_get::<Vec<u8>, _>("data")?)?;
    assert_eq!(revoke.id, revoke_id);
    assert_eq!(revoke.event_type, AuditEventType::Revoke);
    assert_eq!(
        revoke.context.attributes.get("reason").map(String::as_str),
        Some("appeal")
    );
    let states = sqlx::query_scalar::<_, String>(
        "select state from dovecote_deliveries order by event_row_id",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(states, vec!["pending", "pending"]);
    Ok(())
}

#[tokio::test]
async fn sqlite_exact_replay_is_idempotent_and_changed_content_conflicts() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let id = AuditEventId::deterministic(b"sqlite-replay");
    let command = ApplyKeepsake::new(
        SqliteHarness::tenant(),
        SubjectRef::new("account", "sqlite_replay")?,
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
        SqliteHarness::tenant(),
        command.subject.clone(),
        relation.id,
        command.at,
        CommandContext::new(ActorRef::new("test", "changed")?),
    )
    .with_audit_id(id);
    let result = repo.apply(&changed).await;
    assert!(result.is_err(), "changed immutable content must conflict");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        1
    );
    Ok(())
}
