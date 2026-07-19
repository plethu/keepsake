use super::support::*;

use std::sync::Arc;

#[tokio::test]
async fn active_relation_source_accepts_generic_and_erased_sqlx_repository() -> TestResult<()> {
    fn assert_generic<S>(_: &S)
    where
        S: ActiveRelationSource<Error = RepositoryError>,
    {
    }

    let pool = PgPoolOptions::new().connect_lazy("postgres://keepsake@example.invalid/keepsake")?;
    let repo = KeepsakeRepository::new(pool);

    assert_generic(&repo);
    let erased: Arc<dyn DynActiveRelationSource<Error = RepositoryError>> = Arc::new(repo);
    drop(erased);
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
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
            .map(keepsake::Keepsake::id)
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id(), applied_b.keepsake.id()]
    );
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].id(), applied_c.keepsake.id());
    assert!(empty_page.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn active_relations_for_subject_returns_joined_relation_definitions() -> TestResult<()> {
    let repo = repo().await?;
    let relation_a = timed_relation(&repo, "joined-a", "2026-01-02T00:00:00Z").await?;
    let relation_b = timed_relation(&repo, "joined-b", "2026-01-03T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("joined_{}", Uuid::now_v7()))?;

    let applied_a = apply_at(&repo, &subject, relation_a.id, "2026-01-01T00:00:00Z").await?;
    let applied_b = apply_at(&repo, &subject, relation_b.id, "2026-01-01T00:00:00Z").await?;

    let active = repo.active_relations_for_subject(&subject).await?;

    assert_eq!(active.len(), 2);
    assert_eq!(
        active
            .iter()
            .map(|row| row.keepsake().id())
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id(), applied_b.keepsake.id()]
    );
    assert_eq!(active[0].relation(), &relation_a);
    assert_eq!(active[1].relation(), &relation_b);
    assert!(
        active
            .iter()
            .all(|row| row.keepsake().metadata().is_empty())
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn active_relations_for_subject_by_ids_returns_requested_active_relations() -> TestResult<()>
{
    let repo = repo().await?;
    let relation_a = timed_relation(&repo, "ids-a", "2026-01-04T00:00:00Z").await?;
    let relation_b = timed_relation(&repo, "ids-b", "2026-01-05T00:00:00Z").await?;
    let disabled = timed_relation(&repo, "ids-disabled", "2026-01-06T00:00:00Z").await?;
    let revoked = timed_relation(&repo, "ids-revoked", "2026-01-07T00:00:00Z").await?;
    let expired = timed_relation(&repo, "ids-expired", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("ids_{}", Uuid::now_v7()))?;

    let applied_a = apply_at(&repo, &subject, relation_a.id, "2026-01-01T00:00:00Z").await?;
    apply_at(&repo, &subject, relation_b.id, "2026-01-01T00:00:00Z").await?;
    let applied_disabled = apply_at(&repo, &subject, disabled.id, "2026-01-01T00:00:00Z").await?;
    let applied_revoked = apply_at(&repo, &subject, revoked.id, "2026-01-01T00:00:00Z").await?;
    let applied_expired = apply_at(&repo, &subject, expired.id, "2026-01-01T00:00:00Z").await?;

    assert!(set_relation_enabled(&repo, disabled.id, false).await?);
    assert!(revoke_at(&repo, applied_revoked.keepsake.id(), "2026-01-01T00:05:00Z").await?);
    assert_eq!(
        repo.expire_due_timed(ts("2026-01-03T00:00:00Z")?, 10)
            .await?,
        1
    );

    let requested = vec![
        relation_a.id,
        relation_a.id,
        disabled.id,
        revoked.id,
        expired.id,
        Uuid::now_v7(),
    ];
    let active = repo
        .active_relations_for_subject_by_ids(&subject, &requested)
        .await?;

    assert_eq!(
        active
            .iter()
            .map(|row| row.keepsake().id())
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id(), applied_disabled.keepsake.id()]
    );
    assert_eq!(active[0].relation(), &relation_a);
    assert!(!active[1].relation().enabled);
    assert_eq!(active[1].keepsake().id(), applied_disabled.keepsake.id());
    assert!(
        active
            .iter()
            .all(|row| row.keepsake().id() != applied_expired.keepsake.id())
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn active_relations_for_subject_by_keys_returns_requested_active_relations() -> TestResult<()>
{
    let repo = repo().await?;
    let relation_a = timed_relation(&repo, "keyed-a", "2026-01-04T00:00:00Z").await?;
    let relation_b = timed_relation(&repo, "keyed-b", "2026-01-05T00:00:00Z").await?;
    let disabled = timed_relation(&repo, "keyed-disabled", "2026-01-06T00:00:00Z").await?;
    let revoked = timed_relation(&repo, "keyed-revoked", "2026-01-07T00:00:00Z").await?;
    let expired = timed_relation(&repo, "keyed-expired", "2026-01-02T00:00:00Z").await?;
    let subject = SubjectRef::new("user", format!("keyed_{}", Uuid::now_v7()))?;

    let applied_a = apply_at(&repo, &subject, relation_a.id, "2026-01-01T00:00:00Z").await?;
    apply_at(&repo, &subject, relation_b.id, "2026-01-01T00:00:00Z").await?;
    let applied_disabled = apply_at(&repo, &subject, disabled.id, "2026-01-01T00:00:00Z").await?;
    let applied_revoked = apply_at(&repo, &subject, revoked.id, "2026-01-01T00:00:00Z").await?;
    let applied_expired = apply_at(&repo, &subject, expired.id, "2026-01-01T00:00:00Z").await?;

    assert!(set_relation_enabled(&repo, disabled.id, false).await?);
    assert!(revoke_at(&repo, applied_revoked.keepsake.id(), "2026-01-01T00:05:00Z").await?);
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
            .map(|row| row.keepsake().id())
            .collect::<Vec<Uuid>>(),
        vec![applied_a.keepsake.id(), applied_disabled.keepsake.id()]
    );
    assert_eq!(active[0].relation(), &relation_a);
    assert!(!active[1].relation().enabled);
    assert_eq!(active[1].keepsake().id(), applied_disabled.keepsake.id());
    assert!(
        active
            .iter()
            .all(|row| row.keepsake().id() != applied_expired.keepsake.id())
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
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
