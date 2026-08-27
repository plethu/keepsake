use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, AuditEvent, AuditEventId, AuditEventType, CommandContext,
    ExpiryPolicy, RevokeBySubject, SubjectRef,
};
use sqlx::Row;

fn decode_utf8(bytes: Vec<u8>, field: &str) -> Result<String, sqlx::Error> {
    String::from_utf8(bytes).map_err(|source| {
        sqlx::Error::Decode(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("stored {field} is not UTF-8: {source}"),
            )
            .into(),
        )
    })
}

fn text(row: &sqlx::mysql::MySqlRow, column: &str) -> TestResult<String> {
    Ok(decode_utf8(row.try_get::<Vec<u8>, _>(column)?, column)?)
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_lifecycle_events_are_typed_dovecote_rows() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "mysql_acct_audit")?;
    let apply_at = ts("2026-01-01T00:01:00Z")?;
    let apply_id = AuditEventId::deterministic(b"mysql-apply");
    let command = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        apply_at,
        CommandContext::new(ActorRef::new("test", "worker")?)
            .with_idempotency_key("req-1")
            .with_metadata("reason", "support"),
    )
    .with_audit_id(apply_id);
    let applied = repo.apply(&command).await?;

    let revoke_id = AuditEventId::deterministic(b"mysql-revoke");
    repo.revoke_by_subject(
        &RevokeBySubject::new(
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
    assert_eq!(text(&rows[0], "stream")?, "keepsake-audit");
    assert_eq!(
        text(&rows[0], "source")?,
        "https://tests.invalid/keepsake/mysql"
    );
    assert_eq!(
        text(&rows[0], "event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        rows[0].try_get::<chrono::NaiveDateTime, _>("occurred_at")?,
        ts("2026-01-01T00:01:00Z")?.naive_utc()
    );
    assert_eq!(text(&rows[0], "datacontenttype")?, "application/json");
    assert_eq!(
        text(&rows[0], "event_id")?,
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
    let states = sqlx::query_scalar::<_, Vec<u8>>(
        "select state from dovecote_deliveries order by event_row_id",
    )
    .fetch_all(&pool)
    .await?;
    let states = states
        .into_iter()
        .map(|state| decode_utf8(state, "state"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(states, vec!["pending", "pending"]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_exact_replay_is_idempotent_and_changed_content_conflicts() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let id = AuditEventId::deterministic(b"mysql-replay");
    let command = ApplyKeepsake::new(
        SubjectRef::new("account", "mysql_replay")?,
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
