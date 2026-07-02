use super::support::*;

#[tokio::test]
async fn sqlite_timed_expiry_expires_due_keepsake() -> TestResult<()> {
    backend_cases::timed_expiry_expires_due_keepsake::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_timed_expiry_writes_audit_outbox() -> TestResult<()> {
    use keepsake::{
        ActorRef, ApplyKeepsake, AuditEventType, CommandContext, ExpiryPolicy, SubjectRef,
    };

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(
        &repo,
        ExpiryPolicy::At {
            timestamp: backend_cases::ts("2026-01-01T00:02:00Z")?,
        },
    )
    .await?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "sqlite_acct_timed_outbox")?,
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;

    assert_eq!(
        repo.expire_due_timed(backend_cases::ts("2026-01-01T00:03:00Z")?, 10)
            .await?,
        1
    );

    let outbox = repo.audit_outbox(None, 10).await?;
    assert_eq!(outbox.len(), 2);
    assert_eq!(outbox[1].payload.event_type, AuditEventType::TimedExpiry);
    assert_eq!(outbox[1].payload.keepsake_id, applied.keepsake.id());
    Ok(())
}

#[tokio::test]
async fn sqlite_fulfilled_expiry_uses_counter_snapshot() -> TestResult<()> {
    backend_cases::fulfilled_expiry_uses_counter_snapshot::<SqliteHarness>().await
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
