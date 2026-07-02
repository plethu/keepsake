use super::support::*;
use keepsake::ExpiryPolicy;
use uuid::Uuid;

#[tokio::test]
async fn sqlite_apply_duplicate_and_active_read() -> TestResult<()> {
    backend_cases::apply_duplicate_and_active_read::<SqliteHarness>().await
}
#[tokio::test]
async fn sqlite_lifecycle_invariants_reject_invalid_rows() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?1, 'account', 'invalid', ?2, 'applied', ?3, ?4, null, null, ?4, '{}', ?4, ?4)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(serde_json::to_string(&ExpiryPolicy::ManualOnly)?)
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
async fn sqlite_lifecycle_invariants_reject_malformed_policy_rows() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?1, 'account', 'malformed', ?2, 'applied', '{}', ?3, null, null, null, '{}', ?3, ?3)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
async fn sqlite_projection_invariant_rejects_fractional_expiry_mismatch() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let policy = serde_json::json!({
        "type": "at",
        "timestamp": "2026-01-01T00:00:00.123456Z"
    });
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?1, 'account', 'fractional', ?2, 'applied', ?3, ?4, ?5, null, null, '{}', ?4, ?4)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(policy.to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.654321Z")
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
async fn sqlite_revoke_by_subject_revokes_active_keepsake() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, RevokeBySubject, SubjectRef};

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "sqlite_acct_revoke_subject")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject.clone(),
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;

    let revoked = repo
        .revoke_by_subject(&RevokeBySubject::new(
            subject.clone(),
            relation.id,
            backend_cases::ts("2026-01-01T00:02:00Z")?,
            CommandContext::new(ActorRef::new("test", "moderator")?)
                .with_metadata("reason", "appeal"),
        ))
        .await?;
    assert_eq!(revoked, Some(applied.keepsake.id()));
    assert!(repo.active_for_subject(&subject).await?.is_empty());

    // Idempotent: a second revoke finds nothing active and records no event.
    let again = repo
        .revoke_by_subject(&RevokeBySubject::new(
            subject,
            relation.id,
            backend_cases::ts("2026-01-01T00:03:00Z")?,
            CommandContext::new(ActorRef::new("test", "moderator")?),
        ))
        .await?;
    assert_eq!(again, None);

    let events = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 10)
        .await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event.event_type, keepsake::AuditEventType::Revoke);
    assert_eq!(
        events[1].event.context.attributes.get("reason").cloned(),
        Some("appeal".to_owned())
    );
    Ok(())
}
