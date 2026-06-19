use super::support::*;

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn relation_upsert_is_idempotent_by_natural_key() -> TestResult<()> {
    let repo = repo().await?;
    let key = RelationKey::new("tag", unique_key("idempotent"))?;
    let first =
        RelationDefinition::new(Uuid::now_v7(), key.clone(), true, ExpiryPolicy::ManualOnly)?;
    let second = RelationDefinition::new(
        Uuid::now_v7(),
        key,
        false,
        ExpiryPolicy::At {
            timestamp: ts("2026-02-01T00:00:00Z")?,
        },
    )?;

    let inserted = upsert_relation(&repo, &first).await?;
    let updated = upsert_relation(&repo, &second).await?;

    assert_eq!(inserted.id, updated.id);
    assert!(!updated.enabled);
    assert!(matches!(updated.expiry, ExpiryPolicy::At { .. }));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn relation_reads_return_stored_relation_definition() -> TestResult<()> {
    let repo = repo().await?;
    let key = RelationKey::new("tag", unique_key("lookup"))?;
    let relation = RelationDefinition::new(
        Uuid::now_v7(),
        key.clone(),
        true,
        ExpiryPolicy::At {
            timestamp: ts("2026-02-01T00:00:00Z")?,
        },
    )?;
    let stored = upsert_relation(&repo, &relation).await?;

    assert_eq!(repo.relation_by_id(stored.id).await?, Some(stored.clone()));
    assert_eq!(repo.relation_by_key(&key).await?, Some(stored));
    assert_eq!(repo.relation_by_id(Uuid::now_v7()).await?, None);
    assert_eq!(
        repo.relation_by_key(&RelationKey::new("tag", unique_key("missing"))?)
            .await?,
        None
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn typed_relation_specs_upsert_and_apply_by_marker_type() -> TestResult<()> {
    let repo = repo().await?;
    let now = ts("2026-01-01T00:00:00Z")?;
    let relation = repo.upsert_relation_spec::<TrustedAccountTag>(now).await?;
    let subject = SubjectRef::new("account", format!("typed_{}", Uuid::now_v7()))?;

    let command =
        ApplyKeepsake::for_spec::<TrustedAccountTag>(subject.clone(), now, test_context("worker")?);
    let applied = repo.apply(&command).await?;

    assert_eq!(relation.id, TrustedAccountTag::ID);
    assert_eq!(relation.key.kind(), "tag");
    assert_eq!(relation.key.name(), "trusted_account");
    assert_eq!(applied.keepsake.relation_id(), TrustedAccountTag::ID);
    assert_eq!(repo.active_for_subject(&subject).await?.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn typed_relation_specs_reject_existing_key_with_different_id() -> TestResult<()> {
    let repo = repo().await?;
    let now = ts("2026-01-01T00:00:00Z")?;
    let existing = RelationDefinition::enabled(
        Uuid::now_v7(),
        ConflictingTrustedAccountTag::KEY.to_relation_key()?,
        ExpiryPolicy::At {
            timestamp: ts("2026-02-01T00:00:00Z")?,
        },
    )?;
    let existing = upsert_relation(&repo, &existing).await?;

    let result = repo
        .upsert_relation_spec::<ConflictingTrustedAccountTag>(now)
        .await;

    assert!(matches!(
        result,
        Err(RepositoryError::RelationSpecIdMismatch {
            expected_relation_id,
            stored_relation_id,
            ..
        }) if expected_relation_id == ConflictingTrustedAccountTag::ID
            && stored_relation_id == existing.id
    ));
    assert_eq!(repo.relation_by_id(existing.id).await?, Some(existing));
    Ok(())
}

#[cfg(feature = "cache")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn relation_cache_serves_reads_and_invalidates_local_writes() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone())
        .with_local_relation_cache(LocalRelationCacheConfig::new(Duration::from_secs(60)));
    repo.migrate().await?;
    reset_database(&pool).await?;

    let key = RelationKey::new("tag", unique_key("cached"))?;
    let relation =
        RelationDefinition::new(Uuid::now_v7(), key.clone(), true, ExpiryPolicy::ManualOnly)?;
    let stored = upsert_relation(&repo, &relation).await?;

    assert_eq!(repo.relation_by_key(&key).await?, Some(stored.clone()));

    sqlx::query(
        r"
        update keepsake_relation_definitions
        set enabled = false
        where id = $1
        ",
    )
    .bind(stored.id)
    .execute(&pool)
    .await?;

    let cached = repo.relation_by_id(stored.id).await?;
    assert_eq!(cached, Some(stored.clone()));

    assert!(set_relation_enabled(&repo, stored.id, true).await?);
    let refreshed = repo.relation_by_id(stored.id).await?;
    assert_eq!(refreshed, Some(stored));
    Ok(())
}
