use super::support::*;

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn duplicate_active_apply_returns_existing_keepsake() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "duplicate", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("dup_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let duplicate = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    assert!(!applied.duplicate_prevented);
    assert!(duplicate.duplicate_prevented);
    assert_eq!(duplicate.keepsake.id(), applied.keepsake.id());

    let active = repo.active_for_subject(&subject).await?;
    assert_eq!(active.len(), 1);

    assert!(revoke_at(&repo, applied.keepsake.id(), "2026-01-01T00:05:00Z").await?);
    let reapplied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:10:00Z").await?;
    assert!(!reapplied.duplicate_prevented);
    assert_ne!(reapplied.keepsake.id(), applied.keepsake.id());

    let active_after_reapply = repo.active_for_subject(&subject).await?;
    assert_eq!(active_after_reapply.len(), 1);
    assert_eq!(active_after_reapply[0].id(), reapplied.keepsake.id());
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn invalid_subject_apply_fails_without_persisting_row() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "invalid-subject", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef {
        kind: String::new(),
        id: String::new(),
    };

    let command = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:00:00Z")?,
        test_context("worker")?,
    );
    let result = repo.apply(&command).await;

    assert!(
        matches!(result, Err(RepositoryError::Keepsake(keepsake::KeepsakeError::EmptyIdentifier { field })) if field == "subject.kind")
    );
    assert!(repo.active_for_subject(&subject).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn invalid_actor_apply_fails_without_persisting_row() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "invalid-apply-actor", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("invalid_actor_{}", Uuid::now_v7()))?;
    let context = CommandContext {
        actor: ActorRef {
            kind: "system".to_owned(),
            id: String::new(),
        },
        idempotency_key: None,
        metadata: BTreeMap::new(),
    };

    let command = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:00:00Z")?,
        context,
    );
    let result = repo.apply(&command).await;

    assert!(
        matches!(result, Err(RepositoryError::Keepsake(keepsake::KeepsakeError::EmptyIdentifier { field })) if field == "actor.id")
    );
    assert!(repo.active_for_subject(&subject).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn invalid_actor_revoke_fails_without_transition_or_audit() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let relation = timed_relation(&repo, "invalid-revoke-actor", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("invalid_revoke_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let context = CommandContext {
        actor: ActorRef {
            kind: "system".to_owned(),
            id: String::new(),
        },
        idempotency_key: None,
        metadata: BTreeMap::new(),
    };
    let command = RevokeKeepsake::new(applied.keepsake.id(), ts("2026-01-01T00:05:00Z")?, context);
    let result = repo.revoke(&command).await;

    assert!(
        matches!(result, Err(RepositoryError::Keepsake(keepsake::KeepsakeError::EmptyIdentifier { field })) if field == "actor.id")
    );
    let active = repo.active_for_subject(&subject).await?;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id(), applied.keepsake.id());
    assert_eq!(
        audit_rows_for_keepsake(&pool, applied.keepsake.id())
            .await?
            .iter()
            .map(|row| row.event_type.as_str())
            .collect::<Vec<&str>>(),
        vec!["apply"]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn duplicate_apply_after_disable_returns_existing_keepsake() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "disabled-duplicate", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("disabled_dup_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    assert!(set_relation_enabled(&repo, relation.id, false).await?);

    let duplicate = apply_at(&repo, &subject, relation.id, "2026-01-01T00:10:00Z").await?;

    assert!(duplicate.duplicate_prevented);
    assert_eq!(duplicate.keepsake.id(), applied.keepsake.id());
    assert_eq!(repo.active_for_subject(&subject).await?.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn concurrent_duplicate_apply_creates_one_active_keepsake() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "concurrent-apply", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("race_{}", Uuid::now_v7()))?;

    let applied_at = ts("2026-01-01T00:00:00Z")?;
    let apply_a = spawn_apply(repo.clone(), subject.clone(), relation.id, applied_at);
    let apply_b = spawn_apply(repo.clone(), subject.clone(), relation.id, applied_at);

    let result_a = apply_a.await??;
    let result_b = apply_b.await??;
    let active = repo.active_for_subject(&subject).await?;
    let active_id = active[0].id();

    assert_eq!(active.len(), 1);
    assert_eq!(result_a.keepsake.id(), active_id);
    assert_eq!(result_b.keepsake.id(), active_id);
    assert_ne!(result_a.duplicate_prevented, result_b.duplicate_prevented);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn disabled_relation_rejects_apply() -> TestResult<()> {
    let repo = repo().await?;
    let relation = RelationDefinition::new(
        Uuid::now_v7(),
        RelationKey::new("tag", unique_key("disabled-apply"))?,
        false,
        ExpiryPolicy::ManualOnly,
    )?;
    let relation = upsert_relation(&repo, &relation).await?;
    let subject = SubjectRef::new("user", format!("disabled_{}", Uuid::now_v7()))?;

    let command = ApplyKeepsake::new(
        subject,
        relation.id,
        ts("2026-01-01T00:00:00Z")?,
        test_context("worker")?,
    );
    let result = repo.apply(&command).await;

    assert!(matches!(
        result,
        Err(RepositoryError::RelationDisabled { relation_id }) if relation_id == relation.id
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn concurrent_apply_and_disable_have_ordered_outcomes() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "apply-disable", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("apply_disable_{}", Uuid::now_v7()))?;
    let applied_at = ts("2026-01-01T00:00:00Z")?;

    let apply_task = spawn_apply(repo.clone(), subject.clone(), relation.id, applied_at);
    let disable_task = tokio::spawn({
        let repo = repo.clone();
        let disabled_at = ts("2026-01-01T00:01:00Z")?;
        async move {
            repo.set_relation_enabled(relation.id, false, disabled_at)
                .await
        }
    });

    let apply_result = apply_task.await?;
    let disabled = disable_task.await??;
    let active = repo.active_for_subject(&subject).await?;

    assert!(disabled);
    match apply_result {
        Ok(applied) => {
            assert!(!applied.duplicate_prevented);
            assert_eq!(active.len(), 1);
            assert_eq!(active[0].id(), applied.keepsake.id());
        }
        Err(RepositoryError::RelationDisabled { relation_id }) => {
            assert_eq!(relation_id, relation.id);
            assert!(active.is_empty());
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn relation_share_lock_blocks_disable_until_apply_order_is_resolved() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "apply-lock", "2026-01-02T00:00:00Z").await?;
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let disable_pool = single_connection_pool(&database_url).await?;
    let disable_repo = KeepsakeRepository::new(disable_pool.clone());
    let mut tx = pool.begin().await?;

    lock_relation_for_share(&mut tx, relation.id).await?;

    set_lock_timeout(&disable_pool, "50ms").await?;
    let blocked = disable_repo
        .set_relation_enabled(relation.id, false, ts("2026-01-01T00:01:00Z")?)
        .await;
    assert!(
        matches!(blocked, Err(RepositoryError::Sqlx(sqlx::Error::Database(error))) if error.code().as_deref() == Some("55P03"))
    );

    tx.commit().await?;
    set_lock_timeout(&disable_pool, "0").await?;
    assert!(
        disable_repo
            .set_relation_enabled(relation.id, false, ts("2026-01-01T00:02:00Z")?)
            .await?
    );
    Ok(())
}
