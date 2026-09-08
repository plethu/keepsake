use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, RelationDefinition, RelationKey,
    SubjectRef, TenantId,
};
use keepsake_sqlx::SqliteKeepsakeRepository;
use uuid::Uuid;

#[tokio::test]
async fn sqlite_bounded_relation_reads_filter_in_the_database() -> TestResult<()> {
    backend_cases::bounded_relation_reads_filter_in_the_database::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_identifier_contract_round_trips_case_and_unicode() -> TestResult<()> {
    backend_cases::identifier_contract_round_trips_case_and_unicode::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_exact_keepsake_read_is_tenant_scoped_and_transactional() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let root =
        SqliteKeepsakeRepository::new(pool.clone(), "https://tests.invalid/keepsake/sqlite")?;
    let other_tenant = TenantId::new("sqlite-other-tenant")?;
    let other = root.for_tenant(other_tenant.clone());
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let other_relation = other
        .upsert_relation(
            &RelationDefinition::enabled(
                other_tenant,
                Uuid::now_v7(),
                RelationKey::new("tag", format!("exact-read-b-{}", Uuid::now_v7()))?,
                ExpiryPolicy::ManualOnly,
            )?,
            ts("2026-01-01T00:00:00Z")?,
        )
        .await?;
    let shared_id = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0201);
    let other_id = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0202);
    let mut command = ApplyKeepsake::new(
        SqliteHarness::tenant()?,
        SubjectRef::new("account", "exact-read-a")?,
        relation.id,
        ts("2026-01-01T00:00:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    );
    command.id = shared_id;
    let applied = repo.apply(&command).await?;
    let mut other_command = ApplyKeepsake::new(
        other.tenant_id().clone(),
        SubjectRef::new("account", "exact-read-b")?,
        other_relation.id,
        ts("2026-01-01T00:00:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    );
    other_command.id = shared_id;
    let other_applied = other.apply(&other_command).await?;
    let mut wrong_tenant_command = ApplyKeepsake::new(
        other.tenant_id().clone(),
        SubjectRef::new("account", "exact-read-c")?,
        other_relation.id,
        ts("2026-01-01T00:00:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    );
    wrong_tenant_command.id = other_id;
    let wrong_tenant = other.apply(&wrong_tenant_command).await?;
    assert_eq!(
        repo.keepsake_by_id(shared_id).await?.as_ref(),
        Some(&applied.keepsake)
    );
    assert_eq!(
        other.keepsake_by_id(shared_id).await?.as_ref(),
        Some(&other_applied.keepsake)
    );
    assert!(repo.keepsake_by_id(other_id).await?.is_none());
    assert!(
        repo.keepsake_by_id(Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0203))
            .await?
            .is_none()
    );
    let mut tx = pool.begin().await?;
    assert_eq!(
        repo.keepsake_by_id_in_transaction(&mut tx, shared_id)
            .await?
            .as_ref(),
        Some(&applied.keepsake)
    );
    assert_eq!(
        other
            .keepsake_by_id_in_transaction(&mut tx, shared_id)
            .await?
            .as_ref(),
        Some(&other_applied.keepsake)
    );
    assert_eq!(
        other
            .keepsake_by_id_in_transaction(&mut tx, other_id)
            .await?
            .as_ref(),
        Some(&wrong_tenant.keepsake)
    );
    assert!(
        repo.keepsake_by_id_in_transaction(&mut tx, other_id)
            .await?
            .is_none()
    );
    assert!(
        repo.keepsake_by_id_in_transaction(
            &mut tx,
            Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0203)
        )
        .await?
        .is_none()
    );
    tx.rollback().await?;
    Ok(())
}
