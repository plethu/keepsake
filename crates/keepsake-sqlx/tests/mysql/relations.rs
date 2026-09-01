use super::support::*;

#[cfg(feature = "cache")]
use keepsake::{ExpiryPolicy, RelationDefinition, RelationKey};
#[cfg(feature = "cache")]
use keepsake_sqlx::{LocalRelationCacheConfig, MySqlKeepsakeRepository};
#[cfg(feature = "cache")]
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn relation_upsert_rejects_same_id_with_a_different_key_without_mutation() -> TestResult<()> {
    let (repo, _) = MySqlHarness::repo().await?;
    let first = keepsake::RelationDefinition::new(
        MySqlHarness::tenant(),
        Uuid::now_v7(),
        keepsake::RelationKey::new("tag", "original")?,
        true,
        keepsake::ExpiryPolicy::ManualOnly,
    )?;
    let stored = repo
        .upsert_relation(&first, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let incoming = keepsake::RelationDefinition::new(
        MySqlHarness::tenant(),
        stored.id,
        keepsake::RelationKey::new("tag", "different")?,
        false,
        keepsake::ExpiryPolicy::At {
            timestamp: ts("2026-02-01T00:00:00Z")?,
        },
    )?;

    let result = repo
        .upsert_relation(&incoming, ts("2026-01-02T00:00:00Z")?)
        .await;

    assert!(matches!(
        result,
        Err(keepsake_sqlx::RepositoryError::RelationIdentityConflict { relation_id, .. })
            if relation_id == stored.id
    ));
    assert_eq!(repo.relation_by_id(stored.id).await?, Some(stored));
    assert_eq!(repo.relation_by_key(&incoming.key).await?, None);
    Ok(())
}

#[cfg(feature = "cache")]
#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_relation_upsert_refreshes_enabled_and_expiry_cache_state() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    let root = MySqlKeepsakeRepository::new(pool.clone(), "https://tests.invalid/keepsake/mysql")?
        .with_local_relation_cache(LocalRelationCacheConfig::new(Duration::from_mins(1)));
    root.migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_mysql::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;

    let repo = root.for_tenant(MySqlHarness::tenant());
    let key = RelationKey::new("tag", format!("cache-upsert-{}", Uuid::now_v7()))?;
    let first = RelationDefinition::new(
        MySqlHarness::tenant(),
        Uuid::now_v7(),
        key.clone(),
        true,
        ExpiryPolicy::ManualOnly,
    )?;
    let stored = repo
        .upsert_relation(&first, ts("2026-01-01T00:00:00Z")?)
        .await?;
    assert_eq!(repo.relation_by_key(&key).await?, Some(stored.clone()));

    let expiry = ts("2026-02-01T00:00:00Z")?;
    let second = RelationDefinition::new(
        MySqlHarness::tenant(),
        Uuid::now_v7(),
        key,
        false,
        ExpiryPolicy::At { timestamp: expiry },
    )?;
    let updated = repo
        .upsert_relation(&second, ts("2026-01-02T00:00:00Z")?)
        .await?;

    assert_eq!(updated.id, stored.id);
    assert!(!updated.enabled);
    assert_eq!(updated.expiry, ExpiryPolicy::At { timestamp: expiry });
    assert_eq!(repo.relation_by_id(stored.id).await?, Some(updated.clone()));
    assert_eq!(repo.relation_by_key(&updated.key).await?, Some(updated));
    Ok(())
}
