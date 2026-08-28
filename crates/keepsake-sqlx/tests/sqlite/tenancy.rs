use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, RelationDefinition, RelationKey,
    SubjectRef, TenantId,
};
use keepsake_sqlx::SqliteKeepsakeRepository;
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn sqlite_tenants_isolate_same_ids_and_reject_wrong_scope() -> TestResult<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let root =
        SqliteKeepsakeRepository::new(pool.clone(), "https://tests.invalid/keepsake/sqlite")?;
    root.migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;

    let tenant_a = TenantId::new("sqlite-tenant-a")?;
    let tenant_b = TenantId::new("sqlite-tenant-b")?;
    let a = root.for_tenant(tenant_a.clone());
    let b = root.for_tenant(tenant_b.clone());
    let relation_id = Uuid::from_u128(7);
    let key = RelationKey::new("tag", "same-id")?;
    let relation_a = RelationDefinition::enabled(
        tenant_a.clone(),
        relation_id,
        key.clone(),
        ExpiryPolicy::ManualOnly,
    )?;
    let relation_b = RelationDefinition::enabled(
        tenant_b.clone(),
        relation_id,
        key.clone(),
        ExpiryPolicy::ManualOnly,
    )?;
    a.upsert_relation(&relation_a, ts("2026-01-01T00:00:00Z")?)
        .await?;
    b.upsert_relation(&relation_b, ts("2026-01-01T00:00:00Z")?)
        .await?;

    assert_eq!(
        a.relation_by_id(relation_id)
            .await?
            .map(|relation| relation.tenant_id),
        Some(tenant_a.clone())
    );
    assert_eq!(
        b.relation_by_id(relation_id)
            .await?
            .map(|relation| relation.tenant_id),
        Some(tenant_b.clone())
    );
    let subject = SubjectRef::new("account", "same-subject")?;
    let command_a = ApplyKeepsake::new(
        tenant_a.clone(),
        subject.clone(),
        relation_id,
        ts("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    );
    let command_b = ApplyKeepsake::new(
        tenant_b.clone(),
        subject.clone(),
        relation_id,
        ts("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    );
    let applied_a = a.apply(&command_a).await?;
    let applied_b = b.apply(&command_b).await?;
    assert_ne!(applied_a.keepsake.id(), applied_b.keepsake.id());
    assert_eq!(a.active_for_subject(&subject).await?.len(), 1);
    assert_eq!(b.active_for_subject(&subject).await?.len(), 1);

    assert!(matches!(
        a.apply(&command_b).await,
        Err(keepsake_sqlx::RepositoryError::TenantScopeMismatch)
    ));
    Ok(())
}
