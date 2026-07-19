use super::support::*;

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
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
#[ignore = "requires docker postgres; run `mise run test-db`"]
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
#[ignore = "requires docker postgres; run `mise run test-db`"]
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
#[ignore = "requires docker postgres; run `mise run test-db`"]
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
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn relation_cache_serves_reads_and_invalidates_local_writes() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone())
        .with_local_relation_cache(LocalRelationCacheConfig::new(Duration::from_mins(1)));
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

#[derive(Debug, Clone, Default)]
struct SpyRelationCache {
    state: std::sync::Arc<std::sync::Mutex<SpyRelationCacheState>>,
}

#[derive(Debug, Default)]
struct SpyRelationCacheState {
    by_id: BTreeMap<RelationId, RelationDefinition>,
    by_key: BTreeMap<RelationKey, RelationDefinition>,
    get_by_id_calls: usize,
    get_by_key_calls: usize,
    store_calls: usize,
    remove_by_id_calls: usize,
}

impl SpyRelationCache {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, SpyRelationCacheState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(error) => error.into_inner(),
        }
    }

    fn counts(&self) -> (usize, usize, usize, usize) {
        let state = self.lock_state();
        (
            state.get_by_id_calls,
            state.get_by_key_calls,
            state.store_calls,
            state.remove_by_id_calls,
        )
    }
}

#[async_trait::async_trait]
impl RelationCache for SpyRelationCache {
    async fn get_by_id(&self, relation_id: RelationId) -> Option<RelationDefinition> {
        let mut state = self.lock_state();
        state.get_by_id_calls += 1;
        state.by_id.get(&relation_id).cloned()
    }

    async fn get_by_key(&self, key: &RelationKey) -> Option<RelationDefinition> {
        let mut state = self.lock_state();
        state.get_by_key_calls += 1;
        state.by_key.get(key).cloned()
    }

    async fn store(&self, relation: &RelationDefinition) {
        let mut state = self.lock_state();
        state.store_calls += 1;
        state.by_id.insert(relation.id, relation.clone());
        state.by_key.insert(relation.key.clone(), relation.clone());
    }

    async fn remove_by_id(&self, relation_id: RelationId) {
        let mut state = self.lock_state();
        state.remove_by_id_calls += 1;
        if let Some(relation) = state.by_id.remove(&relation_id) {
            state.by_key.remove(&relation.key);
        }
    }
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn relation_lookup_hits_cache_after_first_database_read() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let cache = SpyRelationCache::default();
    let repo = KeepsakeRepository::new(pool.clone()).with_relation_cache(cache.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let key = RelationKey::new("tag", unique_key("spy-cached"))?;
    let relation =
        RelationDefinition::new(Uuid::now_v7(), key.clone(), true, ExpiryPolicy::ManualOnly)?;
    let stored = upsert_relation(&repo, &relation).await?;

    assert_eq!(repo.relation_by_key(&key).await?, Some(stored.clone()));
    assert_eq!(cache.counts(), (0, 1, 1, 1));

    assert_eq!(repo.relation_by_key(&key).await?, Some(stored));
    assert_eq!(cache.counts(), (0, 2, 1, 1));
    Ok(())
}
