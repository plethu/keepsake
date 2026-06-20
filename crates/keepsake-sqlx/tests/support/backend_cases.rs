#![allow(missing_docs, clippy::missing_panics_doc)]

use chrono::{DateTime, Utc};
use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, FulfillmentPolicy, RelationDefinition,
    RelationKey, SubjectRef,
};
use keepsake_sqlx::RepositoryError;
use uuid::Uuid;

pub type TestResult<T> = Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
pub enum TestError {
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),
    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Env(#[from] std::env::VarError),
}

#[async_trait::async_trait]
pub trait BackendHarness {
    const BACKEND: &'static str;

    type Pool: Send + Sync;
    type Repo: Send + Sync;

    async fn repo() -> TestResult<(Self::Repo, Self::Pool)>;
    async fn backend_marker(pool: &Self::Pool) -> Result<String, sqlx::Error>;
    async fn upsert_relation(
        repo: &Self::Repo,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> Result<RelationDefinition, RepositoryError>;
    async fn apply(
        repo: &Self::Repo,
        command: &ApplyKeepsake,
    ) -> Result<keepsake_sqlx::AppliedKeepsake, RepositoryError>;
    async fn active_relations_for_subject(
        repo: &Self::Repo,
        subject: &SubjectRef,
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError>;
    async fn active_for_subject(
        repo: &Self::Repo,
        subject: &SubjectRef,
    ) -> Result<Vec<keepsake::Keepsake>, RepositoryError>;
    async fn expire_due_timed(
        repo: &Self::Repo,
        now: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError>;
    async fn upsert_counter_projection(
        repo: &Self::Repo,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: DateTime<Utc>,
    ) -> Result<(), RepositoryError>;
    async fn expire_due_fulfilled(
        repo: &Self::Repo,
        now: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError>;
}

pub fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

fn context() -> TestResult<CommandContext> {
    Ok(CommandContext::new(ActorRef::new("test", "worker")?))
}

pub async fn upsert_relation<H>(
    repo: &H::Repo,
    expiry: ExpiryPolicy,
) -> TestResult<RelationDefinition>
where
    H: BackendHarness,
{
    let relation = RelationDefinition::enabled(
        Uuid::now_v7(),
        RelationKey::new("tag", format!("{}-{}", H::BACKEND, Uuid::now_v7()))?,
        expiry,
    )?;
    Ok(H::upsert_relation(repo, &relation, ts("2026-01-01T00:00:00Z")?).await?)
}

pub async fn migration_initializes_backend_marker<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (_repo, pool) = H::repo().await?;
    let marker = H::backend_marker(&pool).await?;

    assert_eq!(marker, H::BACKEND);
    Ok(())
}

pub async fn apply_duplicate_and_active_read<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let relation = upsert_relation::<H>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", format!("{}_acct_123", H::BACKEND))?;
    let command = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:01:00Z")?,
        context()?,
    );

    let first = H::apply(&repo, &command).await?;
    let second = H::apply(
        &repo,
        &ApplyKeepsake::new(
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
    assert_eq!(active[0].relation().id, relation.id);
    Ok(())
}

pub async fn timed_expiry_expires_due_keepsake<H>() -> TestResult<()>
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

pub async fn fulfilled_expiry_uses_counter_snapshot<H>() -> TestResult<()>
where
    H: BackendHarness,
{
    let (repo, _pool) = H::repo().await?;
    let relation = upsert_relation::<H>(
        &repo,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )
    .await?;
    let subject = SubjectRef::new("account", format!("{}_acct_steps", H::BACKEND))?;
    let applied = H::apply(
        &repo,
        &ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            context()?,
        ),
    )
    .await?;

    assert_eq!(
        H::expire_due_fulfilled(&repo, ts("2026-01-01T00:02:00Z")?, 10).await?,
        0
    );
    H::upsert_counter_projection(
        &repo,
        applied.keepsake.id(),
        "steps",
        3,
        ts("2026-01-01T00:03:00Z")?,
    )
    .await?;

    assert_eq!(
        H::expire_due_fulfilled(&repo, ts("2026-01-01T00:04:00Z")?, 10).await?,
        1
    );
    Ok(())
}
