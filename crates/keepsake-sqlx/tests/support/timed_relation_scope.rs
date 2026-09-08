use super::support::backend_cases::{BackendHarness, TestResult};
use keepsake::{ApplyKeepsake, CommandContext, ExpiryPolicy, LifecycleState, SubjectRef};
use keepsake_sqlx::RepositoryError;
use uuid::Uuid;

use super::support::backend_cases::{ts, upsert_relation};

async fn apply_scoped<H>(
    repo: &H::Repo,
    relation_id: Uuid,
    subject_id: &str,
    metadata: Option<(&str, &str)>,
) -> TestResult<keepsake_sqlx::AppliedKeepsake>
where
    H: BackendHarness,
{
    let command = ApplyKeepsake::new(
        H::tenant()?,
        SubjectRef::new("account", subject_id)?,
        relation_id,
        ts("2026-01-01T00:00:00Z")?,
        CommandContext::new(keepsake::ActorRef::new("test", "worker")?),
    );
    let command = match metadata {
        Some((key, value)) => command.with_metadata(key, value),
        None => command,
    };
    Ok(H::apply(repo, &command).await?)
}

struct ScopedExpiryFixture {
    selected_relation_id: Uuid,
    disabled_relation_id: Uuid,
    selected: keepsake_sqlx::AppliedKeepsake,
    unrelated: keepsake_sqlx::AppliedKeepsake,
    disabled: keepsake_sqlx::AppliedKeepsake,
}

pub(super) async fn scoped_timed_expiry_is_relation_bounded<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, fixture) = prepare_fixture::<H>().await?;
    let now = ts("2026-01-01T00:03:00Z")?;
    assert_invalid_limits::<H>(&repo, fixture.selected_relation_id, now).await?;
    assert_candidates::<H>(&repo, &fixture, now).await?;
    expire_and_assert::<H>(&repo, fixture, now).await
}

async fn prepare_fixture<H>() -> TestResult<(H::Repo, ScopedExpiryFixture)>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let unrelated_relation = upsert_relation::<H>(
        &repo,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-01T00:01:00Z")?,
        },
    )
    .await?;
    let selected_relation = upsert_relation::<H>(
        &repo,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-01T00:02:00Z")?,
        },
    )
    .await?;
    let disabled_relation = upsert_relation::<H>(
        &repo,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-01T00:00:30Z")?,
        },
    )
    .await?;
    let unrelated = apply_scoped::<H>(
        &repo,
        unrelated_relation.id,
        &format!("{}_unrelated", H::BACKEND),
        None,
    )
    .await?;
    let selected = apply_scoped::<H>(
        &repo,
        selected_relation.id,
        &format!("{}_selected", H::BACKEND),
        Some(("source", "scoped-expiry")),
    )
    .await?;
    let disabled = apply_scoped::<H>(
        &repo,
        disabled_relation.id,
        &format!("{}_disabled", H::BACKEND),
        None,
    )
    .await?;
    H::set_relation_enabled(
        &repo,
        disabled_relation.id,
        false,
        ts("2026-01-01T00:00:00Z")?,
    )
    .await?;
    Ok((
        repo,
        ScopedExpiryFixture {
            selected_relation_id: selected_relation.id,
            disabled_relation_id: disabled_relation.id,
            selected,
            unrelated,
            disabled,
        },
    ))
}

async fn assert_invalid_limits<H>(
    repo: &H::Repo,
    relation_id: Uuid,
    now: time::OffsetDateTime,
) -> TestResult<()>
where
    H: BackendHarness,
{
    for limit in [0, 10_001] {
        assert!(matches!(
            H::due_timed_expiry_for_relation(repo, relation_id, now, limit).await,
            Err(RepositoryError::InvalidLimit { limit: actual, .. }) if actual == limit
        ));
        assert!(matches!(
            H::expire_due_timed_for_relation(repo, relation_id, now, limit).await,
            Err(RepositoryError::InvalidLimit { limit: actual, .. }) if actual == limit
        ));
    }

    Ok(())
}

async fn assert_candidates<H>(
    repo: &H::Repo,
    fixture: &ScopedExpiryFixture,
    now: time::OffsetDateTime,
) -> TestResult<()>
where
    H: BackendHarness,
{
    let candidates =
        H::due_timed_expiry_for_relation(repo, fixture.selected_relation_id, now, 1).await?;
    assert_eq!(candidates.len(), 1);
    let candidate = candidates.first().ok_or(sqlx::Error::RowNotFound)?;
    assert_eq!(candidate.keepsake_id, fixture.selected.keepsake.id());
    assert_eq!(candidate.relation_id, fixture.selected_relation_id);
    assert_eq!(candidate.subject_kind, "account");
    assert_eq!(candidate.subject_id, format!("{}_selected", H::BACKEND));
    assert_eq!(candidate.due_at, ts("2026-01-01T00:02:00Z")?);

    assert!(
        H::due_timed_expiry_for_relation(repo, Uuid::from_u128(99), now, 1)
            .await?
            .is_empty()
    );
    assert!(
        H::due_timed_expiry_for_relation(repo, fixture.disabled_relation_id, now, 1)
            .await?
            .is_empty()
    );
    Ok(())
}

async fn expire_and_assert<H>(
    repo: &H::Repo,
    fixture: ScopedExpiryFixture,
    now: time::OffsetDateTime,
) -> TestResult<()>
where
    H: BackendHarness,
{
    assert_eq!(
        H::expire_due_timed_for_relation(repo, Uuid::from_u128(99), now, 1).await?,
        0
    );
    assert_eq!(
        H::expire_due_timed_for_relation(repo, fixture.selected_relation_id, now, 1).await?,
        1
    );
    assert_eq!(
        H::expire_due_timed_for_relation(repo, fixture.selected_relation_id, now, 1).await?,
        0
    );

    assert_scoped_rows::<H>(
        repo,
        &fixture.selected,
        fixture.selected_relation_id,
        &fixture.unrelated,
        &fixture.disabled,
    )
    .await
}

async fn assert_scoped_rows<H>(
    repo: &H::Repo,
    selected: &keepsake_sqlx::AppliedKeepsake,
    selected_relation_id: Uuid,
    unrelated: &keepsake_sqlx::AppliedKeepsake,
    disabled: &keepsake_sqlx::AppliedKeepsake,
) -> TestResult<()>
where
    H: BackendHarness,
{
    let selected = H::keepsake_by_id(repo, selected.keepsake.id())
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    assert_eq!(selected.state(), LifecycleState::Expired);
    assert_eq!(selected.relation_id(), selected_relation_id);
    assert_eq!(selected.subject().kind(), "account");
    assert_eq!(selected.subject().id(), format!("{}_selected", H::BACKEND));
    assert_eq!(
        selected.metadata().get("source"),
        Some(&"scoped-expiry".to_owned())
    );
    assert_eq!(selected.expires_at(), Some(ts("2026-01-01T00:02:00Z")?));

    let unrelated = H::keepsake_by_id(repo, unrelated.keepsake.id())
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    assert_eq!(unrelated.state(), LifecycleState::Applied);
    let disabled = H::keepsake_by_id(repo, disabled.keepsake.id())
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    assert_eq!(disabled.state(), LifecycleState::Applied);
    Ok(())
}
