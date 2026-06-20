#![allow(missing_docs)]
#![cfg(feature = "sqlite-tests")]

use chrono::{DateTime, Utc};
use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, FulfillmentPolicy, RelationDefinition,
    RelationKey, SubjectRef,
};
use keepsake_sqlx::{RepositoryError, SqliteKeepsakeRepository};
use sqlx::{Row, sqlite::SqlitePoolOptions};
use uuid::Uuid;

type TestResult<T> = Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
enum TestError {
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
}

fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

async fn repo() -> TestResult<(SqliteKeepsakeRepository, sqlx::SqlitePool)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    Ok((repo, pool))
}

fn context() -> TestResult<CommandContext> {
    Ok(CommandContext::new(ActorRef::new("test", "worker")?))
}

async fn upsert_relation(
    repo: &SqliteKeepsakeRepository,
    expiry: ExpiryPolicy,
) -> TestResult<RelationDefinition> {
    let relation = RelationDefinition::enabled(
        Uuid::now_v7(),
        RelationKey::new("tag", format!("sqlite-{}", Uuid::now_v7()))?,
        expiry,
    )?;
    Ok(repo
        .upsert_relation(&relation, ts("2026-01-01T00:00:00Z")?)
        .await?)
}

#[tokio::test]
async fn sqlite_migration_initializes_backend_marker() -> TestResult<()> {
    let (_repo, pool) = repo().await?;
    let marker = sqlx::query("select value from keepsake_schema_metadata where key = 'backend'")
        .fetch_one(&pool)
        .await?
        .try_get::<String, _>("value")?;

    assert_eq!(marker, "sqlite");
    Ok(())
}

#[tokio::test]
async fn sqlite_apply_duplicate_and_active_read() -> TestResult<()> {
    let (repo, _pool) = repo().await?;
    let relation = upsert_relation(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "acct_123")?;
    let command = ApplyKeepsake::new(
        subject.clone(),
        relation.id,
        ts("2026-01-01T00:01:00Z")?,
        context()?,
    );

    let first = repo.apply(&command).await?;
    let second = repo
        .apply(&ApplyKeepsake::new(
            subject.clone(),
            relation.id,
            ts("2026-01-01T00:02:00Z")?,
            context()?,
        ))
        .await?;
    let active = repo.active_relations_for_subject(&subject).await?;

    assert!(!first.duplicate_prevented);
    assert!(second.duplicate_prevented);
    assert_eq!(first.keepsake.id(), second.keepsake.id());
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].relation().id, relation.id);
    Ok(())
}

#[tokio::test]
async fn sqlite_timed_expiry_expires_due_keepsake() -> TestResult<()> {
    let (repo, _pool) = repo().await?;
    let relation = upsert_relation(
        &repo,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-01T00:02:00Z")?,
        },
    )
    .await?;
    let subject = SubjectRef::new("account", "acct_expiring")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            context()?,
        ))
        .await?;

    let expired = repo
        .expire_due_timed(ts("2026-01-01T00:02:00Z")?, 10)
        .await?;
    let keepsake = repo.active_for_subject(applied.keepsake.subject()).await?;

    assert_eq!(expired, 1);
    assert!(keepsake.is_empty());
    Ok(())
}

#[tokio::test]
async fn sqlite_lifecycle_invariants_reject_invalid_rows() -> TestResult<()> {
    let (repo, pool) = repo().await?;
    let relation = upsert_relation(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?1, 'account', 'invalid', ?2, 'applied', ?3, ?4, null, null, ?4, '{}', ?4, ?4)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(serde_json::to_string(&ExpiryPolicy::ManualOnly)?)
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
async fn sqlite_lifecycle_invariants_reject_malformed_policy_rows() -> TestResult<()> {
    let (repo, pool) = repo().await?;
    let relation = upsert_relation(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?1, 'account', 'malformed', ?2, 'applied', '{}', ?3, null, null, null, '{}', ?3, ?3)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
async fn sqlite_projection_invariant_rejects_fractional_expiry_mismatch() -> TestResult<()> {
    let (repo, pool) = repo().await?;
    let relation = upsert_relation(&repo, ExpiryPolicy::ManualOnly).await?;
    let policy = serde_json::json!({
        "type": "at",
        "timestamp": "2026-01-01T00:00:00.123456Z"
    });
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?1, 'account', 'fractional', ?2, 'applied', ?3, ?4, ?5, null, null, '{}', ?4, ?4)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(policy.to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.654321Z")
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
async fn sqlite_fulfilled_expiry_uses_counter_snapshot() -> TestResult<()> {
    let (repo, _pool) = repo().await?;
    let relation = upsert_relation(
        &repo,
        ExpiryPolicy::WhenFulfilled {
            policy: FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    )
    .await?;
    let subject = SubjectRef::new("account", "acct_steps")?;
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            ts("2026-01-01T00:01:00Z")?,
            context()?,
        ))
        .await?;

    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-01T00:02:00Z")?, 10)
            .await?,
        0
    );
    repo.upsert_counter_projection(
        applied.keepsake.id(),
        "steps",
        3,
        ts("2026-01-01T00:03:00Z")?,
    )
    .await?;

    assert_eq!(
        repo.expire_due_fulfilled(ts("2026-01-01T00:04:00Z")?, 10)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_migration_rejects_wrong_backend_marker() -> TestResult<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::query(
        "create table keepsake_schema_metadata (key text primary key, value text not null)",
    )
    .execute(&pool)
    .await?;
    sqlx::query("insert into keepsake_schema_metadata (key, value) values ('backend', 'postgres')")
        .execute(&pool)
        .await?;

    let repo = SqliteKeepsakeRepository::new(pool);
    let result = repo.migrate().await;

    assert!(matches!(
        result,
        Err(RepositoryError::BackendMismatch {
            expected: "sqlite",
            actual
        }) if actual == "postgres"
    ));
    Ok(())
}
