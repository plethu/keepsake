pub use std::collections::BTreeMap;
#[cfg(feature = "cache")]
pub use std::time::Duration;

pub use chrono::{DateTime, Utc};
pub use keepsake::{
    ActiveRelationSource, ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent,
    AuditEventType, CommandContext, DynActiveRelationSource, ExpiryCause, ExpiryPolicy,
    LifecycleState, RelationDefinition, RelationId, RelationKey, RelationSpec, RevokeKeepsake,
    StaticRelationKey, SubjectRef,
};
#[cfg(feature = "cache")]
pub use keepsake_sqlx::LocalRelationCacheConfig;
pub use keepsake_sqlx::{KeepsakeRepository, MembershipCursor, RelationCache, RepositoryError};
pub use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};
pub use uuid::Uuid;

#[path = "support/db.rs"]
mod db;

pub use db::*;

#[derive(Debug, sqlx::FromRow)]
pub struct AuditRow {
    pub id: i64,
    pub event_type: String,
    pub actor_kind: String,
    pub actor_id: String,
    pub decision: serde_json::Value,
    pub occurred_at: DateTime<Utc>,
}

pub struct TrustedAccountTag;

impl RelationSpec for TrustedAccountTag {
    const ID: RelationId = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0101);
    const KEY: StaticRelationKey = StaticRelationKey::new("tag", "trusted_account");

    fn expiry(_at: DateTime<Utc>) -> ExpiryPolicy {
        ExpiryPolicy::ManualOnly
    }
}

pub struct ConflictingTrustedAccountTag;

impl RelationSpec for ConflictingTrustedAccountTag {
    const ID: RelationId = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0102);
    const KEY: StaticRelationKey = StaticRelationKey::new("tag", "trusted_account");

    fn expiry(_at: DateTime<Utc>) -> ExpiryPolicy {
        ExpiryPolicy::ManualOnly
    }
}

pub fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

pub type TestResult<T> = core::result::Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
pub enum TestError {
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
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

pub async fn repo() -> TestResult<KeepsakeRepository> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    reset_database(&pool).await?;
    Ok(repo)
}

pub async fn timed_relation(
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

pub async fn upsert_relation<C>(
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

pub async fn set_relation_enabled<C>(
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

pub async fn apply_at(
    repo: &KeepsakeRepository,
    subject: &SubjectRef,
    relation_id: Uuid,
    applied_at: &str,
) -> TestResult<keepsake_sqlx::AppliedKeepsake> {
    let command = ApplyKeepsake::new(
        subject.clone(),
        relation_id,
        ts(applied_at)?,
        test_context("worker")?,
    );
    Ok(repo.apply(&command).await?)
}

pub async fn revoke_at(
    repo: &KeepsakeRepository,
    keepsake_id: Uuid,
    revoked_at: &str,
) -> TestResult<bool> {
    let command = RevokeKeepsake::new(keepsake_id, ts(revoked_at)?, test_context("worker")?);
    Ok(repo.revoke(&command).await?)
}

pub fn assert_check_violation(result: TestResult<()>) {
    assert!(
        matches!(result, Err(TestError::Sqlx(sqlx::Error::Database(error))) if error.code().as_deref() == Some("23514"))
    );
}

pub fn unique_key(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::now_v7())
}

pub fn spawn_apply(
    repo: KeepsakeRepository,
    subject: SubjectRef,
    relation_id: Uuid,
    applied_at: DateTime<Utc>,
) -> tokio::task::JoinHandle<Result<keepsake_sqlx::AppliedKeepsake, keepsake_sqlx::RepositoryError>>
{
    tokio::spawn(async move {
        let command = ApplyKeepsake::new(
            subject,
            relation_id,
            applied_at,
            CommandContext::new(ActorRef::new("test", "worker")?),
        );
        repo.apply(&command).await
    })
}

pub fn spawn_expire_due(
    repo: KeepsakeRepository,
    due_at: DateTime<Utc>,
) -> tokio::task::JoinHandle<Result<u64, keepsake_sqlx::RepositoryError>> {
    tokio::spawn(async move { repo.expire_due_timed(due_at, 2).await })
}

pub fn test_context(actor_id: &str) -> TestResult<CommandContext> {
    Ok(CommandContext::new(ActorRef::new("test", actor_id)?))
}
