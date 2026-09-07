use super::support::*;
use keepsake::ExpiryPolicy;

#[tokio::test]
async fn sqlite_increment_counter_projection_is_atomic_and_returns_value() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, SubjectRef};

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "sqlite_acct_increment")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            SqliteHarness::tenant()?,
            subject,
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    let keepsake_id = applied.keepsake.id();

    let first = repo
        .increment_counter_projection(
            keepsake_id,
            "steps",
            2,
            backend_cases::ts("2026-01-01T00:02:00Z")?,
        )
        .await?;
    assert_eq!(first, 2);
    let second = repo
        .increment_counter_projection(
            keepsake_id,
            "steps",
            3,
            backend_cases::ts("2026-01-01T00:03:00Z")?,
        )
        .await?;
    assert_eq!(second, 5);

    let snapshot = repo.fulfillment_snapshot(keepsake_id).await?;
    assert_eq!(snapshot.counters.get("steps").copied(), Some(5));
    Ok(())
}

#[tokio::test]
async fn sqlite_checklist_fulfillment_persists_and_expires() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, FulfillmentPolicy, SubjectRef};

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(
        &repo,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::ChecklistComplete {
                list_key: "onboarding.".to_owned(),
            },
        },
    )
    .await?;
    let subject = SubjectRef::new("account", "sqlite_acct_checklist")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            SqliteHarness::tenant()?,
            subject,
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    let keepsake_id = applied.keepsake.id();

    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.profile",
        true,
        backend_cases::ts("2026-01-01T00:02:00Z")?,
    )
    .await?;
    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.payment",
        false,
        backend_cases::ts("2026-01-01T00:02:00Z")?,
    )
    .await?;

    let snapshot = repo.fulfillment_snapshot(keepsake_id).await?;
    assert_eq!(
        snapshot.checklist.get("onboarding.profile").copied(),
        Some(true)
    );
    assert_eq!(
        snapshot.checklist.get("onboarding.payment").copied(),
        Some(false)
    );

    // Not all items complete: nothing expires.
    assert_eq!(
        repo.expire_due_fulfilled(backend_cases::ts("2026-01-01T00:03:00Z")?, 10)
            .await?,
        0
    );

    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.payment",
        true,
        backend_cases::ts("2026-01-01T00:04:00Z")?,
    )
    .await?;

    // All items complete: the keepsake is expired by the fulfillment sweep.
    assert_eq!(
        repo.expire_due_fulfilled(backend_cases::ts("2026-01-01T00:05:00Z")?, 10)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn counter_overflow_preserves_integer_value_and_observation() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef};
    use keepsake_sqlx::RepositoryError;
    use sqlx::Row;

    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let at = ts("2026-01-01T00:00:00Z")?;
    let later = ts("2026-01-01T00:01:00Z")?;
    let assignment = repo
        .apply(&ApplyKeepsake::new(
            SqliteHarness::tenant()?,
            SubjectRef::new("user", "bounded-counter")?,
            relation.id,
            at,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    for (key, boundary, delta) in [("upper", i64::MAX, 1), ("lower", i64::MIN, -1)] {
        repo.upsert_counter_projection(assignment.keepsake.id(), key, boundary, at)
            .await?;
        assert!(matches!(
            repo.increment_counter_projection(assignment.keepsake.id(), key, delta, later)
                .await,
            Err(RepositoryError::CounterOverflow)
        ));
        let row = sqlx::query("select value, typeof(value) as storage_type, observed_at from keepsake_fulfillment_counters where tenant_id = ? and keepsake_id = ? and key = ?")
            .bind(SqliteHarness::tenant()?.as_str()).bind(assignment.keepsake.id().to_string()).bind(key)
            .fetch_one(&pool).await?;
        assert_eq!(row.try_get::<i64, _>("value")?, boundary);
        assert_eq!(row.try_get::<String, _>("storage_type")?, "integer");
        assert_eq!(
            row.try_get::<String, _>("observed_at")?,
            "2026-01-01T00:00:00.000000Z"
        );
        assert_eq!(
            repo.increment_counter_projection(assignment.keepsake.id(), key, 0, later)
                .await?,
            boundary
        );
    }
    Ok(())
}
