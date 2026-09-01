use super::support::*;
use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef};
use time::UtcOffset;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_apply_duplicate_and_active_read() -> TestResult<()> {
    backend_cases::apply_duplicate_and_active_read::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_nanosecond_timed_policy_round_trips_at_sql_precision() -> TestResult<()> {
    backend_cases::nanosecond_timed_policy_round_trips_at_sql_precision::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_non_utc_instants_round_trip_as_same_instant() -> TestResult<()> {
    let (repo, _pool) = MySqlHarness::repo().await?;
    let applied_at = ts("2026-01-01T06:01:00.123456+05:30")?;
    let expires_at = ts("2026-02-01T06:01:00.654321+05:30")?;
    let relation = upsert_relation::<MySqlHarness>(
        &repo,
        ExpiryPolicy::At {
            timestamp: expires_at,
        },
    )
    .await?;
    let subject = SubjectRef::new("account", "mysql-non-utc-instant")?;

    let applied = repo
        .apply(&ApplyKeepsake::new(
            MySqlHarness::tenant(),
            subject.clone(),
            relation.id,
            applied_at,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    let expected_applied_at = applied_at.to_offset(UtcOffset::UTC);
    let expected_expires_at = expires_at.to_offset(UtcOffset::UTC);

    assert_eq!(applied.keepsake.applied_at(), expected_applied_at);
    assert_eq!(applied.keepsake.expires_at(), Some(expected_expires_at));

    let fetched = repo.active_for_subject(&subject).await?;
    assert_eq!(fetched.len(), 1);
    let fetched = &fetched[0];
    assert_eq!(fetched.applied_at(), expected_applied_at);
    assert_eq!(fetched.expires_at(), Some(expected_expires_at));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_legacy_nanosecond_relation_policy_applies_at_sql_precision() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let legacy_policy = serde_json::json!({
        "type": "at",
        "timestamp": "2026-02-01T00:00:00.123456789Z"
    });
    sqlx::query(
        "update keepsake_relation_definitions set expiry_policy = ? where tenant_id = ? and id = ?",
    )
    .bind(&legacy_policy)
    .bind(MySqlHarness::tenant().as_str().as_bytes())
    .bind(relation.id.to_string())
    .execute(&pool)
    .await?;

    let subject = SubjectRef::new("account", "mysql-legacy-nanos")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            MySqlHarness::tenant(),
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    let canonical_expiry = ts("2026-02-01T00:00:00.123456Z")?;
    assert_eq!(applied.keepsake.expires_at(), Some(canonical_expiry));

    assert_eq!(
        applied.keepsake.expiry(),
        &ExpiryPolicy::At {
            timestamp: canonical_expiry
        }
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_concurrent_duplicate_apply_creates_one_active_keepsake() -> TestResult<()> {
    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", format!("mysql_race_{}", Uuid::now_v7()))?;
    let at = ts("2026-01-01T00:01:00Z")?;

    let spawn_apply = |repo: keepsake_sqlx::TenantSqlxKeepsakeRepository<
        'static,
        keepsake_sqlx::MySqlBackend,
    >| {
        let subject = subject.clone();
        let relation_id = relation.id;
        tokio::spawn(async move {
            let command = ApplyKeepsake::new(
                MySqlHarness::tenant(),
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
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
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
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
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
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00.654321Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .bind(naive_utc(ts("2026-01-01T00:00:00Z")?))
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}
#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_revoke_by_subject_revokes_active_keepsake() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, RevokeBySubject, SubjectRef};

    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "mysql_acct_revoke_subject")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            MySqlHarness::tenant(),
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;

    let revoked = repo
        .revoke_by_subject(&RevokeBySubject::new(
            MySqlHarness::tenant(),
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
            MySqlHarness::tenant(),
            subject,
            relation.id,
            ts("2026-01-01T00:03:00Z")?,
            CommandContext::new(ActorRef::new("test", "moderator")?),
        ))
        .await?;
    assert_eq!(again, None);

    Ok(())
}
