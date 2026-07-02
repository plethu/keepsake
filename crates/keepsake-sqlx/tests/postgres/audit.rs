use super::support::*;

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn apply_records_audit_event_with_context() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let relation = timed_relation(&repo, "apply-audit", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("audit_apply_{}", Uuid::now_v7()))?;
    let context = CommandContext::new(ActorRef::new("user", "admin")?)
        .with_idempotency_key("request-1")
        .with_metadata("reason", "support");
    let command = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:00:00Z")?,
        context,
    )
    .with_metadata("source", "console");

    let applied = repo.apply(&command).await?;

    assert_eq!(applied.keepsake.id(), command.id);
    assert_eq!(
        applied
            .keepsake
            .metadata()
            .get("source")
            .map(String::as_str),
        Some("console")
    );

    let audit_rows = audit_rows_for_keepsake(&pool, applied.keepsake.id()).await?;
    assert_eq!(audit_rows.len(), 1);
    let audit = &audit_rows[0];
    assert_eq!(audit.event_type, "apply");
    assert_eq!(audit.actor_kind, "user");
    assert_eq!(audit.actor_id, "admin");
    assert_eq!(audit.occurred_at, command.at);
    assert_eq!(
        serde_json::from_value::<AuditDecision>(audit.decision.clone())?,
        AuditDecision::Applied {
            duplicate_prevented: false
        }
    );
    assert_eq!(
        audit_attributes(&pool, audit.id).await?,
        BTreeMap::from([
            ("idempotency_key".to_owned(), "request-1".to_owned()),
            ("reason".to_owned(), "support".to_owned()),
        ])
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn audit_events_read_paginates_in_order() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "audit-read", "2026-02-01T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("audit_read_{}", Uuid::now_v7()))?;
    let context = CommandContext::new(ActorRef::new("user", "admin")?)
        .with_idempotency_key("req-1")
        .with_metadata("reason", "support");
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:00:00Z")?,
            context,
        ))
        .await?;
    revoke_at(&repo, applied.keepsake.id(), "2026-01-01T00:05:00Z").await?;

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
        events[0].event.context.attributes,
        BTreeMap::from([
            ("idempotency_key".to_owned(), "req-1".to_owned()),
            ("reason".to_owned(), "support".to_owned()),
        ])
    );

    let first = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 1)
        .await?;
    assert_eq!(first.len(), 1);
    let next = repo
        .audit_events_for_keepsake(
            applied.keepsake.id(),
            Some(&keepsake_sqlx::AuditCursor::after(&first[0])),
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

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn duplicate_apply_records_duplicate_audit_event() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let relation = timed_relation(&repo, "duplicate-audit", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("dup_audit_{}", Uuid::now_v7()))?;
    let context = CommandContext::new(ActorRef::new("user", "admin")?);
    let first = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:00:00Z")?,
        context.clone(),
    );
    let duplicate = ApplyKeepsake::new(subject, relation.id, ts("2026-01-01T00:01:00Z")?, context);

    let applied = repo.apply(&first).await?;
    let duplicate = repo.apply(&duplicate).await?;

    assert!(duplicate.duplicate_prevented);
    assert_eq!(duplicate.keepsake.id(), applied.keepsake.id());

    let audit_rows = audit_rows_for_keepsake(&pool, applied.keepsake.id()).await?;
    assert_eq!(
        audit_rows
            .iter()
            .map(|row| row.event_type.as_str())
            .collect::<Vec<&str>>(),
        vec!["apply", "duplicate_apply"]
    );
    assert_eq!(
        serde_json::from_value::<AuditDecision>(audit_rows[1].decision.clone())?,
        AuditDecision::Applied {
            duplicate_prevented: true
        }
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn revoke_records_audit_event_with_context() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let relation = timed_relation(&repo, "revoke-audit", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("audit_revoke_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let command = RevokeKeepsake::new(
        applied.keepsake.id(),
        ts("2026-01-01T00:05:00Z")?,
        CommandContext::new(ActorRef::new("user", "moderator")?).with_metadata("reason", "appeal"),
    );

    assert!(repo.revoke(&command).await?);
    assert!(repo.active_for_subject(&subject).await?.is_empty());

    let audit_rows = audit_rows_for_keepsake(&pool, applied.keepsake.id()).await?;
    assert_eq!(
        audit_rows
            .iter()
            .map(|row| row.event_type.as_str())
            .collect::<Vec<&str>>(),
        vec!["apply", "revoke"]
    );
    let audit = &audit_rows[1];
    assert_eq!(audit.event_type, "revoke");
    assert_eq!(audit.actor_kind, "user");
    assert_eq!(audit.actor_id, "moderator");
    assert_eq!(audit.occurred_at, command.at);
    assert_eq!(
        serde_json::from_value::<AuditDecision>(audit.decision.clone())?,
        AuditDecision::Revoked
    );
    assert_eq!(
        audit_attributes(&pool, audit.id).await?,
        BTreeMap::from([("reason".to_owned(), "appeal".to_owned())])
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn append_audit_event_records_explicit_event() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let due_at = ts("2026-01-01T00:05:00Z")?;
    let relation = timed_relation(&repo, "append-audit", "2026-01-01T00:05:00Z").await?;
    let subject = SubjectRef::new("user", format!("audit_append_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    assert_eq!(repo.expire_due_timed(due_at, 10).await?, 1);

    let event = AuditEvent {
        event_type: AuditEventType::TimedExpiry,
        at: due_at,
        actor: ActorRef::new("system", "expiry-worker")?,
        keepsake_id: applied.keepsake.id(),
        subject,
        relation_id: relation.id,
        decision: AuditDecision::Expired {
            cause: ExpiryCause::Timed,
        },
        context: AuditContext {
            attributes: BTreeMap::from([("batch".to_owned(), "cron-1".to_owned())]),
        },
    };

    let audit_event_id = repo.append_audit_event(&event).await?;

    let audit_rows = audit_rows_for_keepsake(&pool, applied.keepsake.id()).await?;
    assert_eq!(
        audit_rows
            .iter()
            .map(|row| row.event_type.as_str())
            .collect::<Vec<&str>>(),
        vec!["apply", "timed_expiry", "timed_expiry"]
    );
    assert_eq!(audit_rows[2].id, audit_event_id);
    assert_eq!(audit_rows[2].actor_kind, "system");
    assert_eq!(audit_rows[2].actor_id, "expiry-worker");
    assert_eq!(audit_rows[2].occurred_at, due_at);
    assert_eq!(
        serde_json::from_value::<AuditDecision>(audit_rows[2].decision.clone())?,
        AuditDecision::Expired {
            cause: ExpiryCause::Timed
        }
    );
    assert_eq!(
        audit_attributes(&pool, audit_event_id).await?,
        BTreeMap::from([("batch".to_owned(), "cron-1".to_owned())])
    );
    Ok(())
}

#[test]
fn audit_ref_constructors_reject_empty_parts() {
    let result = SubjectRef::new("", "audit-invalid");
    assert!(
        matches!(
            result,
            Err(keepsake::KeepsakeError::EmptyIdentifier {
                field: "subject.kind"
            })
        ),
        "unexpected result: {result:?}"
    );

    let result = ActorRef::new("system", "");
    assert!(
        matches!(
            result,
            Err(keepsake::KeepsakeError::EmptyIdentifier { field: "actor.id" })
        ),
        "unexpected result: {result:?}"
    );
}
