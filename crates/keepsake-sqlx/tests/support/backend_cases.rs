use std::collections::BTreeSet;
use std::env;
use std::num::TryFromIntError;
use time::error::Parse;
use time::format_description::well_known::Rfc3339;
use tokio::task::JoinError;

use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, FulfillmentPolicy, RelationDefinition,
    RelationKey, SubjectRef, TenantId,
};
use keepsake_sqlx::RepositoryError;
use time::OffsetDateTime;
use uuid::Uuid;

pub(in super::super) type TestResult<T> = Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum TestError {
    #[error("test timestamp exceeds the supported range")]
    TimestampRange,
    #[error(transparent)]
    Integer(#[from] TryFromIntError),
    #[error(transparent)]
    Time(#[from] Parse),
    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Env(#[from] env::VarError),
    #[error(transparent)]
    Join(#[from] JoinError),
}

#[async_trait::async_trait]
pub(in super::super) trait BackendHarness {
    const BACKEND: &'static str;
    const TENANT: &'static str;

    type Pool: Send + Sync;
    type Repo: Send + Sync;

    fn tenant() -> keepsake::Result<TenantId> {
        TenantId::new(Self::TENANT)
    }

    async fn repo() -> TestResult<(Self::Repo, Self::Pool)>;
    async fn backend_marker(pool: &Self::Pool) -> Result<String, sqlx::Error>;
    async fn upsert_relation(
        repo: &Self::Repo,
        relation: &RelationDefinition,
        at: OffsetDateTime,
    ) -> Result<RelationDefinition, RepositoryError>;
    async fn apply(
        repo: &Self::Repo,
        command: &ApplyKeepsake,
    ) -> Result<keepsake_sqlx::AppliedKeepsake, RepositoryError>;
    async fn active_relations_for_subject(
        repo: &Self::Repo,
        subject: &SubjectRef,
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError>;
    async fn active_relations_for_subject_by_ids(
        repo: &Self::Repo,
        subject: &SubjectRef,
        relation_ids: &[Uuid],
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError>;
    async fn active_relations_for_subject_by_keys(
        repo: &Self::Repo,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError>;
    async fn active_for_subject(
        repo: &Self::Repo,
        subject: &SubjectRef,
    ) -> Result<Vec<keepsake::Keepsake>, RepositoryError>;
    async fn expire_due_timed(
        repo: &Self::Repo,
        now: OffsetDateTime,
        limit: i64,
    ) -> Result<u64, RepositoryError>;
    async fn due_timed_expiry_for_relation(
        repo: &Self::Repo,
        relation_id: Uuid,
        now: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<keepsake_sqlx::TimedExpiryCandidate>, RepositoryError>;
    async fn expire_due_timed_for_relation(
        repo: &Self::Repo,
        relation_id: Uuid,
        now: OffsetDateTime,
        limit: i64,
    ) -> Result<u64, RepositoryError>;
    async fn keepsake_by_id(
        repo: &Self::Repo,
        keepsake_id: Uuid,
    ) -> Result<Option<keepsake::Keepsake>, RepositoryError>;
    async fn upsert_counter_projection(
        repo: &Self::Repo,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: OffsetDateTime,
    ) -> Result<(), RepositoryError>;
    async fn set_relation_enabled(
        repo: &Self::Repo,
        relation_id: Uuid,
        enabled: bool,
        at: OffsetDateTime,
    ) -> Result<bool, RepositoryError>;
    async fn expire_due_fulfilled(
        repo: &Self::Repo,
        now: OffsetDateTime,
        limit: i64,
    ) -> Result<u64, RepositoryError>;
}

pub(in super::super) fn ts(value: &str) -> Result<OffsetDateTime, Parse> {
    OffsetDateTime::parse(value, &Rfc3339)
}

fn context() -> TestResult<CommandContext> {
    Ok(CommandContext::new(ActorRef::new("test", "worker")?))
}

pub(in super::super) async fn upsert_relation<H>(
    repo: &H::Repo,
    expiry: ExpiryPolicy,
) -> TestResult<RelationDefinition>
where
    H: BackendHarness,
{
    let relation = RelationDefinition::enabled(
        H::tenant()?,
        Uuid::now_v7(),
        RelationKey::new("tag", format!("{}-{}", H::BACKEND, Uuid::now_v7()))?,
        expiry,
    )?;
    Ok(H::upsert_relation(repo, &relation, ts("2026-01-01T00:00:00Z")?).await?)
}

pub(in super::super) async fn migration_initializes_backend_marker<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (_repo, pool) = H::repo().await?;
    let marker = H::backend_marker(&pool).await?;

    assert_eq!(marker, H::BACKEND);
    Ok(())
}

pub(in super::super) async fn apply_duplicate_and_active_read<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let relation = upsert_relation::<H>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", format!("{}_acct_123", H::BACKEND))?;
    let command = ApplyKeepsake::new(
        H::tenant()?,
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:01:00Z")?,
        context()?,
    );

    let first = H::apply(&repo, &command).await?;
    let second = H::apply(
        &repo,
        &ApplyKeepsake::new(
            H::tenant()?,
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:02:00Z")?,
            context()?,
        ),
    )
    .await?;
    let active = H::active_relations_for_subject(&repo, &subject).await?;

    assert!(!first.duplicate_prevented);
    assert!(second.duplicate_prevented);
    assert_eq!(first.keepsake.id(), second.keepsake.id());
    assert_eq!(active.len(), 1);
    assert_eq!(
        active.first().map(|row| row.relation().id),
        Some(relation.id)
    );
    Ok(())
}

pub(in super::super) async fn nanosecond_timed_policy_round_trips_at_sql_precision<H>()
-> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let raw_expiry = ts("2026-02-01T00:00:00.123456789Z")?;
    let canonical_expiry = ts("2026-02-01T00:00:00.123456Z")?;
    let relation = upsert_relation::<H>(
        &repo,
        ExpiryPolicy::At {
            timestamp: raw_expiry,
        },
    )
    .await?;
    assert_eq!(
        relation.expiry,
        ExpiryPolicy::At {
            timestamp: canonical_expiry
        }
    );

    let command = ApplyKeepsake::new(
        H::tenant()?,
        SubjectRef::new("account", format!("{}_nanos", H::BACKEND))?,
        relation.id,
        ts("2026-01-01T00:01:00.987654321Z")?,
        context()?,
    );
    let applied = H::apply(&repo, &command).await?;

    assert_eq!(applied.keepsake.expiry(), &relation.expiry);
    assert_eq!(applied.keepsake.expires_at(), Some(canonical_expiry));
    Ok(())
}

pub(in super::super) async fn bounded_relation_reads_filter_in_the_database<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let relation_a = upsert_relation::<H>(&repo, ExpiryPolicy::ManualOnly).await?;
    let relation_b = upsert_relation::<H>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", format!("{}_bounded_reads", H::BACKEND))?;
    for relation_id in [relation_a.id, relation_b.id] {
        H::apply(
            &repo,
            &ApplyKeepsake::new(
                H::tenant()?,
                subject.clone(),
                relation_id,
                ts("2026-01-01T00:01:00Z")?,
                context()?,
            ),
        )
        .await?;
    }

    let by_ids = H::active_relations_for_subject_by_ids(
        &repo,
        &subject,
        &[relation_a.id, relation_a.id, Uuid::nil()],
    )
    .await?;
    assert_eq!(by_ids.len(), 1);
    assert_eq!(
        by_ids.first().map(|row| row.relation().id),
        Some(relation_a.id)
    );

    let missing_key = RelationKey::new("tag", format!("{}-missing", H::BACKEND))?;
    let by_keys = H::active_relations_for_subject_by_keys(
        &repo,
        &subject,
        &[relation_b.key.clone(), relation_b.key.clone(), missing_key],
    )
    .await?;
    assert_eq!(by_keys.len(), 1);
    assert_eq!(
        by_keys.first().map(|row| row.relation().id),
        Some(relation_b.id)
    );
    Ok(())
}

pub(in super::super) async fn identifier_contract_round_trips_case_and_unicode<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let at = ts("2026-01-01T00:00:00Z")?;
    let boundary = format!("{}a", "é".repeat(95));
    assert_eq!(boundary.len(), keepsake::MAX_PERSISTED_IDENTIFIER_BYTES);
    let keys = [
        RelationKey::new("Tag", "Case")?,
        RelationKey::new("Tag", "case")?,
        RelationKey::new("Tag", "é")?,
        RelationKey::new("Tag", "e\u{301}")?,
        RelationKey::new("Tag", boundary.clone())?,
    ];
    let relations = keys
        .iter()
        .zip(1_u128..)
        .map(|(key, id)| {
            RelationDefinition::enabled(
                H::tenant()?,
                Uuid::from_u128(id),
                key.clone(),
                ExpiryPolicy::ManualOnly,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for relation in &relations {
        let stored = H::upsert_relation(&repo, relation, at).await?;
        assert_eq!(stored.id, relation.id);
        assert_eq!(stored.key, relation.key);
    }

    let subject = SubjectRef::new("User", boundary.clone())?;
    let mut keepsake_ids = Vec::with_capacity(relations.len());
    for (index, relation) in relations.iter().enumerate() {
        let applied = H::apply(
            &repo,
            &ApplyKeepsake::new(
                H::tenant()?,
                subject.clone(),
                relation.id,
                at.checked_add(time::Duration::seconds(i64::try_from(index)?))
                    .ok_or(TestError::TimestampRange)?,
                context()?,
            ),
        )
        .await?;
        keepsake_ids.push(applied.keepsake.id());
    }

    let found = H::active_relations_for_subject_by_keys(&repo, &subject, &keys).await?;
    let found_keys = found
        .iter()
        .map(|relation| {
            (
                relation.relation().key.kind().to_owned(),
                relation.relation().key.name().to_owned(),
            )
        })
        .collect::<BTreeSet<_>>();
    let expected_keys = keys
        .iter()
        .map(|key| (key.kind().to_owned(), key.name().to_owned()))
        .collect::<BTreeSet<_>>();
    assert_eq!(found_keys, expected_keys);

    let projected = H::upsert_counter_projection(
        &repo,
        keepsake_ids
            .first()
            .copied()
            .ok_or(sqlx::Error::RowNotFound)?,
        &boundary,
        1,
        at,
    )
    .await;
    assert!(projected.is_ok());
    assert!(
        H::upsert_counter_projection(
            &repo,
            keepsake_ids
                .first()
                .copied()
                .ok_or(sqlx::Error::RowNotFound)?,
            &"é".repeat(96),
            1,
            at,
        )
        .await
        .is_err()
    );
    assert!(SubjectRef::new("User", "é".repeat(96)).is_err());
    assert!(RelationKey::new("Tag", "é".repeat(96)).is_err());
    Ok(())
}

pub(in super::super) async fn timed_expiry_expires_due_keepsake<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let relation = upsert_relation::<H>(
        &repo,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-01T00:02:00Z")?,
        },
    )
    .await?;
    let subject = SubjectRef::new("account", format!("{}_acct_expiring", H::BACKEND))?;
    let applied = H::apply(
        &repo,
        &ApplyKeepsake::new(
            H::tenant()?,
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            context()?,
        ),
    )
    .await?;

    let expired = H::expire_due_timed(&repo, ts("2026-01-01T00:02:00Z")?, 10).await?;
    let keepsake = H::active_for_subject(&repo, applied.keepsake.subject()).await?;

    assert_eq!(expired, 1);
    assert!(keepsake.is_empty());
    Ok(())
}

pub(in super::super) async fn fulfilled_expiry_skips_disabled_relations_before_limit<H>()
-> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let disabled_relation = RelationDefinition::enabled(
        H::tenant()?,
        Uuid::from_u128(1),
        RelationKey::new("tag", format!("{}-disabled-first", H::BACKEND))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let enabled_relation = RelationDefinition::enabled(
        H::tenant()?,
        Uuid::from_u128(2),
        RelationKey::new("tag", format!("{}-enabled-second", H::BACKEND))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let disabled_relation =
        H::upsert_relation(&repo, &disabled_relation, ts("2026-01-01T00:00:00Z")?).await?;
    let enabled_relation =
        H::upsert_relation(&repo, &enabled_relation, ts("2026-01-01T00:00:00Z")?).await?;

    let disabled_subject = SubjectRef::new("account", format!("{}_disabled_first", H::BACKEND))?;
    let enabled_subject = SubjectRef::new("account", format!("{}_enabled_second", H::BACKEND))?;
    let disabled = H::apply(
        &repo,
        &ApplyKeepsake::new(
            H::tenant()?,
            disabled_subject.clone(),
            disabled_relation.id,
            ts("2026-01-01T00:02:00Z")?,
            context()?,
        ),
    )
    .await?;
    let enabled = H::apply(
        &repo,
        &ApplyKeepsake::new(
            H::tenant()?,
            enabled_subject.clone(),
            enabled_relation.id,
            ts("2026-01-01T00:02:00Z")?,
            context()?,
        ),
    )
    .await?;
    assert!(
        H::set_relation_enabled(
            &repo,
            disabled_relation.id,
            false,
            ts("2026-01-01T00:03:00Z")?,
        )
        .await?
    );
    for keepsake_id in [disabled.keepsake.id(), enabled.keepsake.id()] {
        H::upsert_counter_projection(&repo, keepsake_id, "steps", 3, ts("2026-01-01T00:04:00Z")?)
            .await?;
    }

    assert_eq!(
        H::expire_due_fulfilled(&repo, ts("2026-01-01T00:05:00Z")?, 1).await?,
        1
    );
    assert_eq!(
        H::active_for_subject(&repo, &disabled_subject).await?.len(),
        1
    );
    assert!(
        H::active_for_subject(&repo, &enabled_subject)
            .await?
            .is_empty()
    );
    Ok(())
}

pub(in super::super) async fn fulfilled_expiry_skips_unfulfilled_relations_before_limit<H>()
-> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let unfulfilled_relation = RelationDefinition::enabled(
        H::tenant()?,
        Uuid::from_u128(1),
        RelationKey::new("tag", format!("{}-unfulfilled-first", H::BACKEND))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let fulfilled_relation = RelationDefinition::enabled(
        H::tenant()?,
        Uuid::from_u128(2),
        RelationKey::new("tag", format!("{}-fulfilled-second", H::BACKEND))?,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )?;
    let unfulfilled_relation =
        H::upsert_relation(&repo, &unfulfilled_relation, ts("2026-01-01T00:00:00Z")?).await?;
    let fulfilled_relation =
        H::upsert_relation(&repo, &fulfilled_relation, ts("2026-01-01T00:00:00Z")?).await?;

    let unfulfilled_subject =
        SubjectRef::new("account", format!("{}_unfulfilled_first", H::BACKEND))?;
    let fulfilled_subject = SubjectRef::new("account", format!("{}_fulfilled_second", H::BACKEND))?;
    let _unfulfilled = H::apply(
        &repo,
        &ApplyKeepsake::new(
            H::tenant()?,
            unfulfilled_subject.clone(),
            unfulfilled_relation.id,
            ts("2026-01-01T00:02:00Z")?,
            context()?,
        ),
    )
    .await?;
    let fulfilled = H::apply(
        &repo,
        &ApplyKeepsake::new(
            H::tenant()?,
            fulfilled_subject.clone(),
            fulfilled_relation.id,
            ts("2026-01-01T00:02:00Z")?,
            context()?,
        ),
    )
    .await?;
    H::upsert_counter_projection(
        &repo,
        fulfilled.keepsake.id(),
        "steps",
        3,
        ts("2026-01-01T00:03:00Z")?,
    )
    .await?;

    assert_eq!(
        H::expire_due_fulfilled(&repo, ts("2026-01-01T00:04:00Z")?, 1).await?,
        1
    );
    assert_eq!(
        H::active_for_subject(&repo, &unfulfilled_subject)
            .await?
            .len(),
        1
    );
    assert!(
        H::active_for_subject(&repo, &fulfilled_subject)
            .await?
            .is_empty()
    );
    Ok(())
}
