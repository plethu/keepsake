use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, RelationDefinition, RelationKey,
    SubjectRef, TenantId,
};
use keepsake_sqlx::MySqlKeepsakeRepository;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_tenants_isolate_same_ids_and_reject_wrong_scope() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    let root =
        MySqlKeepsakeRepository::new(pool.clone(), "https://tests.invalid/keepsake/mysql-tenancy")?;
    root.migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_mysql::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;

    let tenant_a = TenantId::new("mysql-tenant-a")?;
    let tenant_b = TenantId::new("mysql-tenant-b")?;
    let a = root.for_tenant(tenant_a.clone());
    let b = root.for_tenant(tenant_b.clone());
    let relation_id = Uuid::from_u128(7);
    let key = RelationKey::new("tag", "same-id")?;
    for tenant in [tenant_a.clone(), tenant_b.clone()] {
        root.for_tenant(tenant.clone())
            .upsert_relation(
                &RelationDefinition::enabled(
                    tenant,
                    relation_id,
                    key.clone(),
                    ExpiryPolicy::ManualOnly,
                )?,
                ts("2026-01-01T00:00:00Z")?,
            )
            .await?;
    }

    let subject = SubjectRef::new("account", "same-subject")?;
    let command_a = ApplyKeepsake::new(
        tenant_a.clone(),
        subject.clone(),
        relation_id,
        ts("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    );
    let command_b = ApplyKeepsake::new(
        tenant_b,
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
