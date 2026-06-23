use super::support::*;

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn lifecycle_commands_and_timed_batches_use_stable_order() -> TestResult<()> {
    let repo = repo().await?;

    let relation = RelationDefinition::new(
        Uuid::now_v7(),
        RelationKey::new("tag", unique_key("stable"))?,
        true,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-02T00:00:00Z")?,
        },
    )?;
    let relation = upsert_relation(&repo, &relation).await?;

    let subject_b = SubjectRef::new("user", format!("b_{}", Uuid::now_v7()))?;
    let subject_a = SubjectRef::new("user", format!("a_{}", Uuid::now_v7()))?;
    let subject_c = SubjectRef::new("user", format!("c_{}", Uuid::now_v7()))?;

    let applied_b = apply_at(&repo, &subject_b, relation.id, "2026-01-01T00:00:00Z").await?;
    let applied_a = apply_at(&repo, &subject_a, relation.id, "2026-01-01T00:00:00Z").await?;
    let applied_c = apply_at(&repo, &subject_c, relation.id, "2026-01-01T00:00:00Z").await?;

    assert_eq!(applied_b.keepsake.state(), LifecycleState::Applied);

    let active = repo.active_for_subject(&subject_b).await?;
    assert_eq!(active.len(), 1);

    let due = repo
        .due_timed_expiry(ts("2026-01-03T00:00:00Z")?, 2)
        .await?;
    let due_ids = due.iter().map(|row| row.keepsake_id).collect::<Vec<Uuid>>();
    assert_eq!(
        due_ids,
        vec![applied_a.keepsake.id(), applied_b.keepsake.id()]
    );

    let expired = repo
        .expire_due_timed(ts("2026-01-03T00:00:00Z")?, 2)
        .await?;
    assert_eq!(expired, 2);

    let active_after_b = repo.active_for_subject(&subject_b).await?;
    let active_after_a = repo.active_for_subject(&subject_a).await?;
    let active_after_c = repo.active_for_subject(&subject_c).await?;
    assert!(active_after_b.is_empty());
    assert!(active_after_a.is_empty());
    assert_eq!(active_after_c.len(), 1);
    assert_eq!(active_after_c[0].id(), applied_c.keepsake.id());
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn disabled_relation_is_excluded_from_timed_expiry() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "disabled-expiry", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("disabled_expiry_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    assert!(set_relation_enabled(&repo, relation.id, false).await?);

    let due = repo
        .due_timed_expiry(ts("2026-01-03T00:00:00Z")?, 10)
        .await?;
    assert!(
        !due.iter()
            .any(|row| row.keepsake_id == applied.keepsake.id())
    );

    let expired = repo
        .expire_due_timed(ts("2026-01-03T00:00:00Z")?, 10)
        .await?;
    assert_eq!(expired, 0);

    let active = repo.active_for_subject(&subject).await?;
    assert_eq!(active.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn lifecycle_check_constraints_reject_invalid_rows() -> TestResult<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;

    let manual = RelationDefinition::new(
        Uuid::now_v7(),
        RelationKey::new("tag", unique_key("manual-constraint"))?,
        true,
        ExpiryPolicy::ManualOnly,
    )?;
    let manual = upsert_relation(&repo, &manual).await?;
    let timed = timed_relation(&repo, "timed-constraint", "2026-01-02T00:00:00Z").await?;

    assert_check_violation(
        insert_raw_keepsake(
            &pool,
            manual.id,
            &ExpiryPolicy::ManualOnly,
            "expired",
            None,
            None,
            None,
        )
        .await,
    );
    assert_check_violation(
        insert_raw_keepsake(
            &pool,
            manual.id,
            &ExpiryPolicy::ManualOnly,
            "applied",
            None,
            None,
            Some(ts("2026-01-03T00:00:00Z")?),
        )
        .await,
    );
    assert_check_violation(
        insert_raw_keepsake(
            &pool,
            manual.id,
            &ExpiryPolicy::ManualOnly,
            "revoked",
            None,
            Some(ts("2026-01-03T00:00:00Z")?),
            Some(ts("2026-01-03T00:00:00Z")?),
        )
        .await,
    );
    assert_check_violation(
        insert_raw_keepsake(
            &pool,
            timed.id,
            &ExpiryPolicy::At {
                timestamp: ts("2026-01-02T00:00:00Z")?,
            },
            "expired",
            None,
            None,
            None,
        )
        .await,
    );
    assert_check_violation(
        insert_raw_keepsake_value(
            &pool,
            manual.id,
            serde_json::json!({ "type": "unknown" }),
            "applied",
            None,
            None,
            None,
        )
        .await,
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn concurrent_expiry_workers_expire_each_due_row_once() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "concurrent-expiry", "2026-01-02T00:00:00Z").await?;
    let subjects = [
        SubjectRef::new("user", format!("expire_a_{}", Uuid::now_v7()))?,
        SubjectRef::new("user", format!("expire_b_{}", Uuid::now_v7()))?,
        SubjectRef::new("user", format!("expire_c_{}", Uuid::now_v7()))?,
        SubjectRef::new("user", format!("expire_d_{}", Uuid::now_v7()))?,
    ];

    for subject in &subjects {
        apply_at(&repo, subject, relation.id, "2026-01-01T00:00:00Z").await?;
    }

    let due_at = ts("2026-01-03T00:00:00Z")?;
    let worker_a = spawn_expire_due(repo.clone(), due_at);
    let worker_b = spawn_expire_due(repo.clone(), due_at);
    let expired = worker_a.await?? + worker_b.await??;

    assert_eq!(expired, subjects.len() as u64);
    for subject in &subjects {
        assert!(repo.active_for_subject(subject).await?.is_empty());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn concurrent_expiry_and_disable_have_ordered_outcomes() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "expiry-disable", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("expiry_disable_{}", Uuid::now_v7()))?;
    apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    let expire_task = spawn_expire_due(repo.clone(), ts("2026-01-03T00:00:00Z")?);
    let disable_task = tokio::spawn({
        let repo = repo.clone();
        let disabled_at = ts("2026-01-03T00:01:00Z")?;
        async move {
            repo.set_relation_enabled(relation.id, false, disabled_at)
                .await
        }
    });

    let expired = expire_task.await??;
    let disabled = disable_task.await??;
    let active = repo.active_for_subject(&subject).await?;

    assert!(disabled);
    if expired == 0 {
        assert_eq!(active.len(), 1);
    } else {
        assert_eq!(expired, 1);
        assert!(active.is_empty());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn relation_share_lock_blocks_disable_until_expiry_order_is_resolved() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "expiry-lock", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("expiry_lock_{}", Uuid::now_v7()))?;
    apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let disable_pool = single_connection_pool(&database_url).await?;
    let disable_repo = KeepsakeRepository::new(disable_pool.clone());
    let mut tx = pool.begin().await?;

    lock_due_keepsake_and_relation_for_expiry(&mut tx, relation.id).await?;

    set_lock_timeout(&disable_pool, "50ms").await?;
    let blocked = disable_repo
        .set_relation_enabled(relation.id, false, ts("2026-01-03T00:01:00Z")?)
        .await;
    assert!(
        matches!(blocked, Err(RepositoryError::Sqlx(sqlx::Error::Database(error))) if error.code().as_deref() == Some("55P03"))
    );

    tx.commit().await?;
    set_lock_timeout(&disable_pool, "0").await?;
    assert!(
        disable_repo
            .set_relation_enabled(relation.id, false, ts("2026-01-03T00:02:00Z")?)
            .await?
    );
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
async fn fulfilled_relation(
    repo: &KeepsakeRepository,
    key_prefix: &str,
) -> TestResult<RelationDefinition> {
    let relation = RelationDefinition::new(
        Uuid::now_v7(),
        RelationKey::new("tag", unique_key(key_prefix))?,
        true,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    upsert_relation(repo, &relation).await
}

#[cfg(feature = "fulfillment-counters")]
async fn stored_state(keepsake_id: Uuid) -> TestResult<String> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    Ok(sqlx::query_scalar::<_, String>(
        r"
        select state
        from keepsakes
        where id = $1
        ",
    )
    .bind(keepsake_id)
    .fetch_one(&pool)
    .await?)
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn counter_at_least_fulfillment_expiry_runs_end_to_end() -> TestResult<()> {
    let repo = repo().await?;
    let relation = fulfilled_relation(&repo, "fulfilled-counter").await?;
    let subject = SubjectRef::new("user", format!("fulfilled_counter_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let keepsake_id = applied.keepsake.id();

    assert_eq!(
        repo.fulfillment_snapshot(keepsake_id).await?,
        FulfillmentSnapshot::empty()
    );
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:00:00Z")?, 10)
            .await?,
        0
    );

    repo.upsert_counter_projection(keepsake_id, "steps", 2, ts("2026-01-02T00:00:00Z")?)
        .await?;
    assert_eq!(
        repo.fulfillment_snapshot(keepsake_id).await?,
        FulfillmentSnapshot::empty().with_counter("steps", 2)
    );
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:00:00Z")?, 10)
            .await?,
        0
    );

    repo.upsert_counter_projection(keepsake_id, "steps", 3, ts("2026-01-02T00:01:00Z")?)
        .await?;
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:01:00Z")?, 10)
            .await?,
        1
    );
    assert_eq!(stored_state(keepsake_id).await?, "expired");
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn disabled_relation_is_not_expired_by_fulfillment() -> TestResult<()> {
    let repo = repo().await?;
    let relation = fulfilled_relation(&repo, "fulfilled-disabled").await?;
    let subject = SubjectRef::new("user", format!("fulfilled_disabled_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let keepsake_id = applied.keepsake.id();

    assert!(set_relation_enabled(&repo, relation.id, false).await?);
    repo.upsert_counter_projection(keepsake_id, "steps", 3, ts("2026-01-02T00:00:00Z")?)
        .await?;

    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:00:00Z")?, 10)
            .await?,
        0
    );
    assert_eq!(stored_state(keepsake_id).await?, "applied");
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn fulfilled_expiry_skips_disabled_relations_before_limit() -> TestResult<()> {
    let repo = repo().await?;
    let disabled_relation = RelationDefinition::enabled(
        Uuid::from_u128(1),
        RelationKey::new("tag", unique_key("fulfilled-disabled-first"))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let enabled_relation = RelationDefinition::enabled(
        Uuid::from_u128(2),
        RelationKey::new("tag", unique_key("fulfilled-enabled-second"))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let disabled_relation = upsert_relation(&repo, &disabled_relation).await?;
    let enabled_relation = upsert_relation(&repo, &enabled_relation).await?;
    let disabled_subject = SubjectRef::new("user", format!("disabled_first_{}", Uuid::now_v7()))?;
    let enabled_subject = SubjectRef::new("user", format!("enabled_second_{}", Uuid::now_v7()))?;
    let disabled = apply_at(
        &repo,
        &disabled_subject,
        disabled_relation.id,
        "2026-01-01T00:00:00Z",
    )
    .await?;
    let enabled = apply_at(
        &repo,
        &enabled_subject,
        enabled_relation.id,
        "2026-01-01T00:00:00Z",
    )
    .await?;

    assert!(set_relation_enabled(&repo, disabled_relation.id, false).await?);
    for keepsake_id in [disabled.keepsake.id(), enabled.keepsake.id()] {
        repo.upsert_counter_projection(keepsake_id, "steps", 3, ts("2026-01-02T00:00:00Z")?)
            .await?;
    }

    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:01:00Z")?, 1)
            .await?,
        1
    );
    assert_eq!(stored_state(disabled.keepsake.id()).await?, "applied");
    assert_eq!(stored_state(enabled.keepsake.id()).await?, "expired");
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn fulfilled_expiry_skips_unfulfilled_relations_before_limit() -> TestResult<()> {
    let repo = repo().await?;
    let unfulfilled_relation = RelationDefinition::enabled(
        Uuid::from_u128(1),
        RelationKey::new("tag", unique_key("fulfilled-unfulfilled-first"))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let fulfilled_relation = RelationDefinition::enabled(
        Uuid::from_u128(2),
        RelationKey::new("tag", unique_key("fulfilled-fulfilled-second"))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let unfulfilled_relation = upsert_relation(&repo, &unfulfilled_relation).await?;
    let fulfilled_relation = upsert_relation(&repo, &fulfilled_relation).await?;
    let unfulfilled_subject =
        SubjectRef::new("user", format!("unfulfilled_first_{}", Uuid::now_v7()))?;
    let fulfilled_subject =
        SubjectRef::new("user", format!("fulfilled_second_{}", Uuid::now_v7()))?;
    let unfulfilled = apply_at(
        &repo,
        &unfulfilled_subject,
        unfulfilled_relation.id,
        "2026-01-01T00:00:00Z",
    )
    .await?;
    let fulfilled = apply_at(
        &repo,
        &fulfilled_subject,
        fulfilled_relation.id,
        "2026-01-01T00:00:00Z",
    )
    .await?;

    repo.upsert_counter_projection(
        fulfilled.keepsake.id(),
        "steps",
        3,
        ts("2026-01-02T00:00:00Z")?,
    )
    .await?;

    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:01:00Z")?, 1)
            .await?,
        1
    );
    assert_eq!(stored_state(unfulfilled.keepsake.id()).await?, "applied");
    assert_eq!(stored_state(fulfilled.keepsake.id()).await?, "expired");
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn due_fulfilled_expiry_returns_only_when_fulfilled_keepsakes() -> TestResult<()> {
    let repo = repo().await?;
    let fulfilled = fulfilled_relation(&repo, "fulfilled-due").await?;
    let timed = timed_relation(&repo, "fulfilled-due-timed", "2026-01-02T00:00:00Z").await?;
    let fulfilled_subject = SubjectRef::new("user", format!("fulfilled_due_{}", Uuid::now_v7()))?;
    let timed_subject = SubjectRef::new("user", format!("timed_due_{}", Uuid::now_v7()))?;
    let fulfilled_applied = apply_at(
        &repo,
        &fulfilled_subject,
        fulfilled.id,
        "2026-01-01T00:00:00Z",
    )
    .await?;
    let timed_applied = apply_at(&repo, &timed_subject, timed.id, "2026-01-01T00:00:00Z").await?;

    let due = repo.due_fulfilled_expiry(10).await?;

    assert!(
        due.iter()
            .any(|row| row.keepsake_id == fulfilled_applied.keepsake.id())
    );
    assert!(
        !due.iter()
            .any(|row| row.keepsake_id == timed_applied.keepsake.id())
    );
    assert!(
        due.iter()
            .all(|row| matches!(row.expiry_policy, ExpiryPolicy::WhenFulfilled { .. }))
    );
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
async fn checklist_relation(
    repo: &KeepsakeRepository,
    key_prefix: &str,
) -> TestResult<RelationDefinition> {
    let relation = RelationDefinition::new(
        Uuid::now_v7(),
        RelationKey::new("tag", unique_key(key_prefix))?,
        true,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::ChecklistComplete {
                list_key: "onboarding.".to_owned(),
            },
        },
    )?;
    upsert_relation(repo, &relation).await
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn increment_counter_projection_is_atomic_and_returns_value() -> TestResult<()> {
    let repo = repo().await?;
    let relation = fulfilled_relation(&repo, "increment-counter").await?;
    let subject = SubjectRef::new("user", format!("increment_counter_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let keepsake_id = applied.keepsake.id();

    assert_eq!(
        repo.increment_counter_projection(keepsake_id, "steps", 2, ts("2026-01-02T00:00:00Z")?)
            .await?,
        2
    );
    assert_eq!(
        repo.increment_counter_projection(keepsake_id, "steps", 3, ts("2026-01-02T00:01:00Z")?)
            .await?,
        5
    );
    assert_eq!(
        repo.fulfillment_snapshot(keepsake_id).await?,
        FulfillmentSnapshot::empty().with_counter("steps", 5)
    );
    Ok(())
}

#[cfg(feature = "fulfillment-counters")]
#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn checklist_fulfillment_persists_and_expires() -> TestResult<()> {
    let repo = repo().await?;
    let relation = checklist_relation(&repo, "checklist-fulfill").await?;
    let subject = SubjectRef::new("user", format!("checklist_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;
    let keepsake_id = applied.keepsake.id();

    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.profile",
        true,
        ts("2026-01-02T00:00:00Z")?,
    )
    .await?;
    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.payment",
        false,
        ts("2026-01-02T00:00:00Z")?,
    )
    .await?;
    assert_eq!(
        repo.fulfillment_snapshot(keepsake_id).await?,
        FulfillmentSnapshot::empty()
            .with_check("onboarding.profile", true)
            .with_check("onboarding.payment", false)
    );
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:01:00Z")?, 10)
            .await?,
        0
    );

    repo.upsert_checklist_projection(
        keepsake_id,
        "onboarding.payment",
        true,
        ts("2026-01-02T00:02:00Z")?,
    )
    .await?;
    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-02T00:03:00Z")?, 10)
            .await?,
        1
    );
    assert_eq!(stored_state(keepsake_id).await?, "expired");
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn revoke_by_subject_revokes_active_keepsake() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "revoke-by-subject", "2026-02-01T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("revoke_by_subject_{}", Uuid::now_v7()))?;
    let applied = apply_at(&repo, &subject, relation.id, "2026-01-01T00:00:00Z").await?;

    let revoked = repo
        .revoke_by_subject(&RevokeBySubject::new(
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:05:00Z")?,
            test_context("moderator")?,
        ))
        .await?;
    assert_eq!(revoked, Some(applied.keepsake.id()));
    assert!(repo.active_for_subject(&subject).await?.is_empty());

    let again = repo
        .revoke_by_subject(&RevokeBySubject::new(
            subject,
            relation.id,
            ts("2026-01-01T00:06:00Z")?,
            test_context("moderator")?,
        ))
        .await?;
    assert_eq!(again, None);

    let events = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 10)
        .await?;
    assert_eq!(
        events
            .iter()
            .map(|record| record.event.event_type)
            .collect::<Vec<_>>(),
        vec![AuditEventType::Apply, AuditEventType::Revoke]
    );
    Ok(())
}
