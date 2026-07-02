use super::support::*;
use keepsake::ExpiryPolicy;

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_audit_event_read_paginates_in_order() -> TestResult<()> {
    use keepsake::{
        ActorRef, ApplyKeepsake, AuditEventType, CommandContext, RevokeKeepsake, SubjectRef,
    };
    use keepsake_sqlx::AuditCursor;

    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "mysql_acct_audit")?;
    let context = CommandContext::new(ActorRef::new("test", "worker")?)
        .with_idempotency_key("req-1")
        .with_metadata("source", "support");
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            context,
        ))
        .await?;
    repo.revoke(&RevokeKeepsake::new(
        applied.keepsake.id(),
        ts("2026-01-01T00:02:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    ))
    .await?;

    let events = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 10)
        .await?;
    assert_eq!(
        events
            .iter()
            .map(|record| record.event.event_type)
            .collect::<Vec<_>>(),
        vec![AuditEventType::Apply, AuditEventType::Revoke]
    );
    assert_eq!(
        events[0].event.context.attributes.get("source").cloned(),
        Some("support".to_owned())
    );
    assert!(events[1].event.context.attributes.is_empty());

    let first = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 1)
        .await?;
    assert_eq!(first.len(), 1);
    let next = repo
        .audit_events_for_keepsake(
            applied.keepsake.id(),
            Some(&AuditCursor::after(&first[0])),
            10,
        )
        .await?;
    assert_eq!(
        next.iter()
            .map(|record| record.event.event_type)
            .collect::<Vec<_>>(),
        vec![AuditEventType::Revoke]
    );

    let by_relation = repo
        .audit_events_for_relation(relation.id, None, 10)
        .await?;
    assert_eq!(by_relation.len(), 2);
    Ok(())
}
