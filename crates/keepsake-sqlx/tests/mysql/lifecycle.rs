use super::support::*;
use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_apply_duplicate_and_active_read() -> TestResult<()> {
    backend_cases::apply_duplicate_and_active_read::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_concurrent_duplicate_apply_creates_one_active_keepsake() -> TestResult<()> {
    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", format!("mysql_race_{}", Uuid::now_v7()))?;
    let at = ts("2026-01-01T00:01:00Z")?;

    let spawn_apply = |repo: keepsake_sqlx::MySqlKeepsakeRepository| {
        let subject = subject.clone();
        let relation_id = relation.id;
        tokio::spawn(async move {
            let command = ApplyKeepsake::new(
                subject,
                relation_id,
                at,
                CommandContext::new(ActorRef::new("test", "worker")?),
            );
            repo.apply(&command).await
        })
    };
    let apply_a = spawn_apply(repo.clone());
    let apply_b = spawn_apply(repo.clone());
    let result_a = apply_a.await??;
    let result_b = apply_b.await??;
    let active = repo.active_for_subject(&subject).await?;

    assert_eq!(active.len(), 1);
    assert_eq!(result_a.keepsake.id(), active[0].id());
    assert_eq!(result_b.keepsake.id(), active[0].id());
    assert_ne!(result_a.duplicate_prevented, result_b.duplicate_prevented);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_timed_expiry_expires_due_keepsake() -> TestResult<()> {
    backend_cases::timed_expiry_expires_due_keepsake::<MySqlHarness>().await
}
#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_lifecycle_invariants_reject_invalid_rows() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?, 'account', 'invalid', ?, 'applied', ?, ?, null, null, ?, '{}', ?, ?)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(serde_json::to_value(&ExpiryPolicy::ManualOnly)?)
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_lifecycle_invariants_reject_malformed_policy_rows() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?, 'account', 'malformed', ?, 'applied', '{}', ?, null, null, null, '{}', ?, ?)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_projection_invariant_rejects_fractional_expiry_mismatch() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let policy = serde_json::json!({
        "type": "at",
        "timestamp": "2026-01-01T00:00:00.123456Z"
    });
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?, 'account', 'fractional', ?, 'applied', ?, ?, ?, null, null, '{}', ?, ?)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(policy)
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00.654321Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}
#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_revoke_by_subject_revokes_active_keepsake() -> TestResult<()> {
    use keepsake::{
        ActorRef, ApplyKeepsake, AuditEventType, CommandContext, RevokeBySubject, SubjectRef,
    };

    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "mysql_acct_revoke_subject")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;

    let revoked = repo
        .revoke_by_subject(&RevokeBySubject::new(
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:02:00Z")?,
            CommandContext::new(ActorRef::new("test", "moderator")?)
                .with_metadata("reason", "appeal"),
        ))
        .await?;
    assert_eq!(revoked, Some(applied.keepsake.id()));
    assert!(repo.active_for_subject(&subject).await?.is_empty());

    let again = repo
        .revoke_by_subject(&RevokeBySubject::new(
            subject,
            relation.id,
            ts("2026-01-01T00:03:00Z")?,
            CommandContext::new(ActorRef::new("test", "moderator")?),
        ))
        .await?;
    assert_eq!(again, None);

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
        events[1].event.context.attributes.get("reason").cloned(),
        Some("appeal".to_owned())
    );
    Ok(())
}
