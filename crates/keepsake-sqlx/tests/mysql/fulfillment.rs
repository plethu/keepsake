use super::support::*;
use keepsake::ExpiryPolicy;

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_fulfilled_expiry_uses_counter_snapshot() -> TestResult<()> {
    backend_cases::fulfilled_expiry_uses_counter_snapshot::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_fulfilled_expiry_skips_disabled_relations_before_limit() -> TestResult<()> {
    backend_cases::fulfilled_expiry_skips_disabled_relations_before_limit::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_fulfilled_expiry_skips_unfulfilled_relations_before_limit() -> TestResult<()> {
    backend_cases::fulfilled_expiry_skips_unfulfilled_relations_before_limit::<MySqlHarness>().await
}
#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_increment_counter_projection_is_atomic_and_returns_value() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, SubjectRef};

    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "mysql_acct_increment")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    let keepsake_id = applied.keepsake.id();

    assert_eq!(
        repo.increment_counter_projection(keepsake_id, "steps", 2, ts("2026-01-01T00:02:00Z")?)
            .await?,
        2
    );
    assert_eq!(
        repo.increment_counter_projection(keepsake_id, "steps", 3, ts("2026-01-01T00:03:00Z")?)
            .await?,
        5
    );
    assert_eq!(
        repo.fulfillment_snapshot(keepsake_id)
            .await?
            .counters
            .get("steps")
            .copied(),
        Some(5)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_checklist_fulfillment_persists_and_expires() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, FulfillmentPolicy, SubjectRef};

    let (repo, _pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(
        &repo,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::ChecklistComplete {
                list_key: "onboarding.".to_owned(),
            },
        },
    )
    .await?;
    let subject = SubjectRef::new("account", "mysql_acct_checklist")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            CommandContext::new(ActorRef::new("test", "worker")?),
        ))
        .await?;
    let keepsake_id = applied.keepsake.id();

    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.profile",
        true,
        ts("2026-01-01T00:02:00Z")?,
    )
    .await?;
    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.payment",
        false,
        ts("2026-01-01T00:02:00Z")?,
    )
    .await?;
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-01T00:03:00Z")?, 10)
            .await?,
        0
    );

    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.payment",
        true,
        ts("2026-01-01T00:04:00Z")?,
    )
    .await?;
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-01T00:05:00Z")?, 10)
            .await?,
        1
    );
    Ok(())
}
