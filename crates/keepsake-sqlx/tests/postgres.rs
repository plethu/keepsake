#![allow(missing_docs)]
#![cfg(feature = "postgres-tests")]
//! Docker-backed Postgres integration tests.

use std::collections::BTreeMap;
#[cfg(feature = "cache")]
use std::time::Duration;

use chrono::{DateTime, Utc};
use keepsake::{
    ExpiryPolicy, LifecycleState, RelationDefinition, RelationId, RelationKey, RelationSpec,
    StaticRelationKey, SubjectRef,
};
#[cfg(feature = "cache")]
use keepsake_sqlx::LocalRelationCacheConfig;
use keepsake_sqlx::{KeepsakeRepository, MembershipCursor, RelationCache, RepositoryError};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};
use uuid::Uuid;

struct TrustedAccountTag;

impl RelationSpec for TrustedAccountTag {
    const ID: RelationId = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0101);
    const KEY: StaticRelationKey = StaticRelationKey::new("tag", "trusted_account");

    fn expiry(_at: DateTime<Utc>) -> ExpiryPolicy {
        ExpiryPolicy::ManualOnly
    }
}

struct ConflictingTrustedAccountTag;

impl RelationSpec for ConflictingTrustedAccountTag {
    const ID: RelationId = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0102);
    const KEY: StaticRelationKey = StaticRelationKey::new("tag", "trusted_account");

    fn expiry(_at: DateTime<Utc>) -> ExpiryPolicy {
        ExpiryPolicy::ManualOnly
    }
}

fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

type TestResult<T> = std::result::Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),

    #[error(transparent)]
    Env(#[from] std::env::VarError),

    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),

    #[error(transparent)]
    Repository(#[from] RepositoryError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

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

    assert_eq!(applied_b.keepsake.state, LifecycleState::Applied);

    let active = repo.active_for_subject(&subject_b).await?;
    assert_eq!(active.len(), 1);

    let due = repo
        .due_timed_expiry(ts("2026-01-03T00:00:00Z")?, 2)
        .await?;
    let due_ids = due.iter().map(|row| row.keepsake_id).collect::<Vec<Uuid>>();
    assert_eq!(due_ids, vec![applied_a.keepsake.id, applied_b.keepsake.id]);

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
    assert_eq!(active_after_c[0].id, applied_c.keepsake.id);
    Ok(())
}

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

    let applied = repo
        .apply_spec_without_metadata::<TrustedAccountTag>(&subject, now)
        .await?;

    assert_eq!(relation.id, TrustedAccountTag::ID);
    assert_eq!(relation.key.kind(), "tag");
    assert_eq!(relation.key.name(), "trusted_account");
    assert_eq!(applied.keepsake.relation_id, TrustedAccountTag::ID);
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

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn duplicate_active_apply_returns_existing_keepsake() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "duplicate", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("dup_{}", Uuid::now_v7()))?;
    let applied = repo
        .apply(
            &subject,
            relation.id,
            ts("2026-01-01T00:00:00Z")?,
            &BTreeMap::new(),
        )
        .await?;
    let duplicate = repo
        .apply(
            &subject,
            relation.id,
            ts("2026-01-01T00:00:00Z")?,
            &BTreeMap::new(),
        )
        .await?;

    assert!(!applied.duplicate_prevented);
    assert!(duplicate.duplicate_prevented);
    assert_eq!(duplicate.keepsake.id, applied.keepsake.id);

    let active = repo.active_for_subject(&subject).await?;
    assert_eq!(active.len(), 1);

    assert!(
        repo.revoke(applied.keepsake.id, ts("2026-01-01T00:05:00Z")?)
            .await?
    );
    let reapplied = repo
        .apply(
            &subject,
            relation.id,
            ts("2026-01-01T00:10:00Z")?,
            &BTreeMap::new(),
        )
        .await?;
    assert!(!reapplied.duplicate_prevented);
    assert_ne!(reapplied.keepsake.id, applied.keepsake.id);

    let active_after_reapply = repo.active_for_subject(&subject).await?;
    assert_eq!(active_after_reapply.len(), 1);
    assert_eq!(active_after_reapply[0].id, reapplied.keepsake.id);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn active_membership_scan_uses_keyset_pagination() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "membership-pages", "2026-01-02T00:00:00Z").await?;
    let subjects = [
        SubjectRef::new("user", format!("a_{}", Uuid::now_v7()))?,
        SubjectRef::new("user", format!("b_{}", Uuid::now_v7()))?,
        SubjectRef::new("user", format!("c_{}", Uuid::now_v7()))?,
    ];

    let applied_b = apply_at(&repo, &subjects[1], relation.id, "2026-01-01T00:00:00Z").await?;
    let applied_a = apply_at(&repo, &subjects[0], relation.id, "2026-01-01T00:00:00Z").await?;
    let applied_c = apply_at(&repo, &subjects[2], relation.id, "2026-01-01T00:00:00Z").await?;

    let first_page = repo.active_membership_scan(relation.id, 2).await?;
    let cursor = MembershipCursor::after(&first_page[1]);
    let second_page = repo
        .active_membership_scan_after(relation.id, Some(&cursor), 2)
        .await?;
    let empty_page = repo
        .active_membership_scan_after(
            relation.id,
            Some(&MembershipCursor::after(&second_page[0])),
            2,
        )
        .await?;

    assert_eq!(
        first_page
            .iter()
            .map(|keepsake| keepsake.id)
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id, applied_b.keepsake.id]
    );
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].id, applied_c.keepsake.id);
    assert!(empty_page.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn active_relations_for_subject_returns_joined_relation_definitions() -> TestResult<()> {
    let repo = repo().await?;
    let relation_a = timed_relation(&repo, "joined-a", "2026-01-02T00:00:00Z").await?;
    let relation_b = timed_relation(&repo, "joined-b", "2026-01-03T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("joined_{}", Uuid::now_v7()))?;

    let applied_a = repo
        .apply_without_metadata(&subject, relation_a.id, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let applied_b = repo
        .apply_without_metadata(&subject, relation_b.id, ts("2026-01-01T00:00:00Z")?)
        .await?;

    let active = repo.active_relations_for_subject(&subject).await?;

    assert_eq!(active.len(), 2);
    assert_eq!(
        active
            .iter()
            .map(|row| row.keepsake.id)
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id, applied_b.keepsake.id]
    );
    assert_eq!(active[0].relation, relation_a);
    assert_eq!(active[1].relation, relation_b);
    assert!(active.iter().all(|row| row.keepsake.metadata.is_empty()));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn active_relations_for_subject_by_keys_returns_requested_active_relations() -> TestResult<()>
{
    let repo = repo().await?;
    let relation_a = timed_relation(&repo, "keyed-a", "2026-01-04T00:00:00Z").await?;
    let relation_b = timed_relation(&repo, "keyed-b", "2026-01-05T00:00:00Z").await?;
    let disabled = timed_relation(&repo, "keyed-disabled", "2026-01-06T00:00:00Z").await?;
    let revoked = timed_relation(&repo, "keyed-revoked", "2026-01-07T00:00:00Z").await?;
    let expired = timed_relation(&repo, "keyed-expired", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("keyed_{}", Uuid::now_v7()))?;

    let applied_a = repo
        .apply_without_metadata(&subject, relation_a.id, ts("2026-01-01T00:00:00Z")?)
        .await?;
    repo.apply_without_metadata(&subject, relation_b.id, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let applied_disabled = repo
        .apply_without_metadata(&subject, disabled.id, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let applied_revoked = repo
        .apply_without_metadata(&subject, revoked.id, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let applied_expired = repo
        .apply_without_metadata(&subject, expired.id, ts("2026-01-01T00:00:00Z")?)
        .await?;

    assert!(set_relation_enabled(&repo, disabled.id, false).await?);
    assert!(
        repo.revoke(applied_revoked.keepsake.id, ts("2026-01-01T00:05:00Z")?)
            .await?
    );
    assert_eq!(
        repo.expire_due_timed(ts("2026-01-03T00:00:00Z")?, 10)
            .await?,
        1
    );

    let requested = vec![
        relation_a.key.clone(),
        relation_a.key.clone(),
        disabled.key.clone(),
        revoked.key.clone(),
        expired.key.clone(),
        RelationKey::new("tag", unique_key("keyed-missing"))?,
    ];
    let active = repo
        .active_relations_for_subject_by_keys(&subject, &requested)
        .await?;

    assert_eq!(
        active
            .iter()
            .map(|row| row.keepsake.id)
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id, applied_disabled.keepsake.id]
    );
    assert_eq!(active[0].relation, relation_a);
    assert!(!active[1].relation.enabled);
    assert_eq!(active[1].keepsake.id, applied_disabled.keepsake.id);
    assert!(
        active
            .iter()
            .all(|row| row.keepsake.id != applied_expired.keepsake.id)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `make test-db`"]
async fn batch_queries_reject_invalid_limits() -> TestResult<()> {
    let repo = repo().await?;
    let relation = timed_relation(&repo, "invalid-limit", "2026-01-02T00:00:00Z").await?;

    let membership = repo.active_membership_scan(relation.id, 0).await;
    assert!(matches!(
        membership,
        Err(RepositoryError::InvalidLimit { limit: 0, .. })
    ));

    let due = repo.due_timed_expiry(ts("2026-01-03T00:00:00Z")?, -1).await;
    assert!(matches!(
        due,
        Err(RepositoryError::InvalidLimit { limit: -1, .. })
    ));

    let expire = repo
        .expire_due_timed(ts("2026-01-03T00:00:00Z")?, 10_001)
        .await;
    assert!(matches!(
        expire,
        Err(RepositoryError::InvalidLimit { limit: 10_001, .. })
    ));
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

    let duplicate = repo
        .apply(
            &subject,
            relation.id,
            ts("2026-01-01T00:10:00Z")?,
            &BTreeMap::new(),
        )
        .await?;

    assert!(duplicate.duplicate_prevented);
    assert_eq!(duplicate.keepsake.id, applied.keepsake.id);
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
    let active_id = active[0].id;

    assert_eq!(active.len(), 1);
    assert_eq!(result_a.keepsake.id, active_id);
    assert_eq!(result_b.keepsake.id, active_id);
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

    let result = repo
        .apply(
            &subject,
            relation.id,
            ts("2026-01-01T00:00:00Z")?,
            &BTreeMap::new(),
        )
        .await;

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
            assert_eq!(active[0].id, applied.keepsake.id);
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
    assert!(!due.iter().any(|row| row.keepsake_id == applied.keepsake.id));

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

async fn repo() -> TestResult<KeepsakeRepository> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;
    Ok(repo)
}

async fn single_connection_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
}

async fn reset_database(pool: &PgPool) -> TestResult<()> {
    sqlx::query(
        r"
        truncate table
            keepsake_audit_context_attributes,
            keepsake_audit_events,
            keepsake_fulfillment_counters,
            keepsakes,
            keepsake_relation_definitions
        restart identity cascade
        ",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn timed_relation(
    repo: &KeepsakeRepository,
    key_prefix: &str,
    expires_at: &str,
) -> TestResult<RelationDefinition> {
    let relation = RelationDefinition::new(
        Uuid::now_v7(),
        RelationKey::new("tag", unique_key(key_prefix))?,
        true,
        ExpiryPolicy::At {
            timestamp: ts(expires_at)?,
        },
    )?;
    upsert_relation(repo, &relation).await
}

async fn upsert_relation<C>(
    repo: &KeepsakeRepository<C>,
    relation: &RelationDefinition,
) -> TestResult<RelationDefinition>
where
    C: RelationCache,
{
    Ok(repo
        .upsert_relation(relation, ts("2026-01-01T00:00:00Z")?)
        .await?)
}

async fn set_relation_enabled<C>(
    repo: &KeepsakeRepository<C>,
    relation_id: Uuid,
    enabled: bool,
) -> TestResult<bool>
where
    C: RelationCache,
{
    Ok(repo
        .set_relation_enabled(relation_id, enabled, ts("2026-01-01T00:01:00Z")?)
        .await?)
}

async fn apply_at(
    repo: &KeepsakeRepository,
    subject: &SubjectRef,
    relation_id: Uuid,
    applied_at: &str,
) -> TestResult<keepsake_sqlx::AppliedKeepsake> {
    Ok(repo
        .apply(subject, relation_id, ts(applied_at)?, &BTreeMap::new())
        .await?)
}

fn unique_key(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::now_v7())
}

fn spawn_apply(
    repo: KeepsakeRepository,
    subject: SubjectRef,
    relation_id: Uuid,
    applied_at: DateTime<Utc>,
) -> tokio::task::JoinHandle<Result<keepsake_sqlx::AppliedKeepsake, keepsake_sqlx::RepositoryError>>
{
    tokio::spawn(async move {
        repo.apply(&subject, relation_id, applied_at, &BTreeMap::new())
            .await
    })
}

fn spawn_expire_due(
    repo: KeepsakeRepository,
    due_at: DateTime<Utc>,
) -> tokio::task::JoinHandle<Result<u64, keepsake_sqlx::RepositoryError>> {
    tokio::spawn(async move { repo.expire_due_timed(due_at, 2).await })
}

async fn set_lock_timeout(pool: &PgPool, timeout: &str) -> TestResult<()> {
    sqlx::query("select set_config('lock_timeout', $1, false)")
        .bind(timeout)
        .execute(pool)
        .await?;
    Ok(())
}

async fn lock_relation_for_share(
    tx: &mut Transaction<'_, Postgres>,
    relation_id: Uuid,
) -> TestResult<()> {
    sqlx::query(
        r"
        select id
        from keepsake_relation_definitions
        where id = $1
        for share
        ",
    )
    .bind(relation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn lock_due_keepsake_and_relation_for_expiry(
    tx: &mut Transaction<'_, Postgres>,
    relation_id: Uuid,
) -> TestResult<()> {
    sqlx::query(
        r"
        select k.id
        from keepsakes k
        join keepsake_relation_definitions r on r.id = k.relation_id
        where k.relation_id = $1
          and k.state = 'applied'
          and r.enabled
          and k.expires_at is not null
        order by k.expires_at, k.relation_id, k.subject_kind, k.subject_id, k.id
        limit 1
        for update of k skip locked
        for share of r
        ",
    )
    .bind(relation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
