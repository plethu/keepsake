use super::support::*;
use keepsake::ExpiryPolicy;
use sqlx::Row;

#[tokio::test]
async fn sqlite_timed_expiry_expires_due_keepsake() -> TestResult<()> {
    backend_cases::timed_expiry_expires_due_keepsake::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_timed_expiry_writes_typed_dovecote_event() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(
        &repo,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-01T00:02:00Z")?,
        },
    )
    .await?;
    let applied = repo
        .apply(&keepsake::ApplyKeepsake::new(
            keepsake::SubjectRef::new("account", "sqlite_acct_timed")?,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            keepsake::CommandContext::new(keepsake::ActorRef::new("test", "worker")?),
        ))
        .await?;
    assert_eq!(
        repo.expire_due_timed(ts("2026-01-01T00:03:00Z")?, 10)
            .await?,
        1
    );
    let row = sqlx::query(
        "select event_type, occurred_at, data from dovecote_events order by row_id desc limit 1",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        row.try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        row.try_get::<String, _>("occurred_at")?,
        "2026-01-01T00:03:00Z"
    );
    let event: keepsake::AuditEvent = serde_json::from_slice(&row.try_get::<Vec<u8>, _>("data")?)?;
    assert_eq!(event.event_type, keepsake::AuditEventType::TimedExpiry);
    assert_eq!(event.keepsake_id, applied.keepsake.id());
    Ok(())
}

#[tokio::test]
async fn sqlite_fulfilled_expiry_uses_counter_snapshot_and_audits() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(
        &repo,
        keepsake::ExpiryPolicy::WhenFulfilled {
            policy: keepsake::FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )
    .await?;
    let applied = repo
        .apply(&keepsake::ApplyKeepsake::new(
            keepsake::SubjectRef::new("account", "sqlite_acct_steps")?,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            keepsake::CommandContext::new(keepsake::ActorRef::new("test", "worker")?),
        ))
        .await?;
    repo.upsert_counter_projection(
        applied.keepsake.id(),
        "steps",
        3,
        ts("2026-01-01T00:02:00Z")?,
    )
    .await?;
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-01T00:03:00Z")?, 10)
            .await?,
        1
    );
    let row =
        sqlx::query("select event_type, data from dovecote_events order by row_id desc limit 1")
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        row.try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    let event: keepsake::AuditEvent = serde_json::from_slice(&row.try_get::<Vec<u8>, _>("data")?)?;
    assert_eq!(
        event.event_type,
        keepsake::AuditEventType::FulfillmentExpiry
    );
    assert_eq!(event.keepsake_id, applied.keepsake.id());
    Ok(())
}

#[tokio::test]
async fn sqlite_fulfilled_expiry_skips_disabled_relations_before_limit() -> TestResult<()> {
    backend_cases::fulfilled_expiry_skips_disabled_relations_before_limit::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_fulfilled_expiry_skips_unfulfilled_relations_before_limit() -> TestResult<()> {
    backend_cases::fulfilled_expiry_skips_unfulfilled_relations_before_limit::<SqliteHarness>()
        .await
}
