pub use std::collections::BTreeMap;
#[cfg(feature = "cache")]
pub use std::time::Duration;

pub use chrono::{DateTime, Utc};
pub use keepsake::{
    ExpiryPolicy, LifecycleState, RelationDefinition, RelationId, RelationKey, RelationSpec,
    StaticRelationKey, SubjectRef,
};
#[cfg(feature = "cache")]
pub use keepsake_sqlx::LocalRelationCacheConfig;
pub use keepsake_sqlx::{KeepsakeRepository, MembershipCursor, RelationCache, RepositoryError};
pub use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};
pub use uuid::Uuid;

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

pub type TestResult<T> = std::result::Result<T, TestError>;

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

pub async fn single_connection_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
}

pub async fn reset_database(pool: &PgPool) -> TestResult<()> {
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
    Ok(repo
        .apply(subject, relation_id, ts(applied_at)?, &BTreeMap::new())
        .await?)
}

pub async fn insert_raw_keepsake(
    pool: &PgPool,
    relation_id: Uuid,
    expiry: &ExpiryPolicy,
    state: &str,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
) -> TestResult<()> {
    insert_raw_keepsake_value(
        pool,
        relation_id,
        serde_json::to_value(expiry)?,
        state,
        expires_at,
        fulfilled_at,
        revoked_at,
    )
    .await
}

pub async fn insert_raw_keepsake_value(
    pool: &PgPool,
    relation_id: Uuid,
    expiry_policy: serde_json::Value,
    state: &str,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
) -> TestResult<()> {
    sqlx::query(
        r"
        insert into keepsakes
          (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
           expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values ($1, 'user', $2, $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $6, $6)
        ",
    )
    .bind(Uuid::now_v7())
    .bind(format!("invalid_{}", Uuid::now_v7()))
    .bind(relation_id)
    .bind(state)
    .bind(expiry_policy)
    .bind(ts("2026-01-01T00:00:00Z")?)
    .bind(expires_at)
    .bind(fulfilled_at)
    .bind(revoked_at)
    .execute(pool)
    .await?;
    Ok(())
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
        repo.apply(&subject, relation_id, applied_at, &BTreeMap::new())
            .await
    })
}

pub fn spawn_expire_due(
    repo: KeepsakeRepository,
    due_at: DateTime<Utc>,
) -> tokio::task::JoinHandle<Result<u64, keepsake_sqlx::RepositoryError>> {
    tokio::spawn(async move { repo.expire_due_timed(due_at, 2).await })
}

pub async fn set_lock_timeout(pool: &PgPool, timeout: &str) -> TestResult<()> {
    sqlx::query("select set_config('lock_timeout', $1, false)")
        .bind(timeout)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn lock_relation_for_share(
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

pub async fn lock_due_keepsake_and_relation_for_expiry(
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
