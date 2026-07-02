use super::support::*;
use keepsake::ExpiryPolicy;

#[tokio::test]
async fn sqlite_apply_persists_multiple_audit_context_attributes() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, SubjectRef};

    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let context = CommandContext::new(ActorRef::new("test", "worker")?)
        .with_idempotency_key("request-1")
        .with_metadata("request_id", "req_123")
        .with_metadata("source", "support");
    let command = ApplyKeepsake::new(
        SubjectRef::new("account", "sqlite_acct_attrs")?,
        relation.id,
        backend_cases::ts("2026-01-01T00:01:00Z")?,
        context,
    );

    repo.apply(&command).await?;

    let attributes = sqlx::query_as::<_, (String, String)>(
        "select key, value from keepsake_audit_context_attributes order by key",
    )
    .fetch_all(&pool)
    .await?;

    assert_eq!(
        attributes,
        vec![
            ("idempotency_key".to_owned(), "request-1".to_owned()),
            ("request_id".to_owned(), "req_123".to_owned()),
            ("source".to_owned(), "support".to_owned()),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_audit_event_read_paginates_in_order() -> TestResult<()> {
    use keepsake::{
        ActorRef, ApplyKeepsake, AuditEventType, CommandContext, RevokeKeepsake, SubjectRef,
    };
    use keepsake_sqlx::AuditCursor;

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "sqlite_acct_audit")?;
    let context = CommandContext::new(ActorRef::new("test", "worker")?)
        .with_idempotency_key("req-1")
        .with_metadata("source", "support");
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            context,
        ))
        .await?;
    repo.revoke(&RevokeKeepsake::new(
        applied.keepsake.id(),
        backend_cases::ts("2026-01-01T00:02:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    ))
    .await?;

    let events = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 10)
        .await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.event_type, AuditEventType::Apply);
    assert_eq!(events[1].event.event_type, AuditEventType::Revoke);
    assert_eq!(
        events[0].event.context.attributes.get("source").cloned(),
        Some("support".to_owned())
    );
    assert_eq!(
        events[0]
            .event
            .context
            .attributes
            .get("idempotency_key")
            .cloned(),
        Some("req-1".to_owned())
    );
    assert!(events[1].event.context.attributes.is_empty());

    let first = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 1)
        .await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].event.event_type, AuditEventType::Apply);
    let next = repo
        .audit_events_for_keepsake(
            applied.keepsake.id(),
            Some(&AuditCursor::after(&first[0])),
            10,
        )
        .await?;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].event.event_type, AuditEventType::Revoke);

    let by_relation = repo
        .audit_events_for_relation(relation.id, None, 10)
        .await?;
    assert_eq!(by_relation.len(), 2);
    Ok(())
}

#[tokio::test]
async fn sqlite_audit_outbox_exports_claims_acks_and_releases() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, AuditEventType, CommandContext, SubjectRef};
    use keepsake_sqlx::AuditOutboxCursor;

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "sqlite_acct_outbox")?,
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;

    let outbox = repo.audit_outbox(None, 10).await?;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].audit_event_id, 1);
    assert_eq!(outbox[0].payload.event_type, AuditEventType::Apply);
    assert_eq!(outbox[0].payload.keepsake_id, applied.keepsake.id());
    assert!(
        !repo
            .ack_audit_outbox(outbox[0].id, backend_cases::ts("2026-01-01T00:02:00Z")?)
            .await?
    );
    assert!(!repo.release_audit_outbox(outbox[0].id).await?);

    let after = repo
        .audit_outbox(Some(&AuditOutboxCursor::after(&outbox[0])), 10)
        .await?;
    assert!(after.is_empty());

    let claimed = repo
        .claim_audit_outbox(
            "worker-a",
            backend_cases::ts("2026-01-01T00:02:00Z")?,
            backend_cases::ts("2026-01-01T00:05:00Z")?,
            10,
        )
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].claimed_by.as_deref(), Some("worker-a"));

    assert!(repo.release_audit_outbox(claimed[0].id).await?);
    let reclaimed = repo
        .claim_audit_outbox(
            "worker-b",
            backend_cases::ts("2026-01-01T00:03:00Z")?,
            backend_cases::ts("2026-01-01T00:06:00Z")?,
            10,
        )
        .await?;
    assert_eq!(reclaimed[0].claimed_by.as_deref(), Some("worker-b"));

    assert!(
        repo.ack_audit_outbox(reclaimed[0].id, backend_cases::ts("2026-01-01T00:04:00Z")?)
            .await?
    );
    assert!(repo.audit_outbox(None, 10).await?.is_empty());
    Ok(())
}
