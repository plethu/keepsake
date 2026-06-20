#![allow(missing_docs)]
#![cfg(feature = "mysql-tests")]

#[path = "support/backend_cases.rs"]
mod backend_cases;

use keepsake::ExpiryPolicy;
use keepsake_sqlx::{MySqlKeepsakeRepository, RepositoryError};
use sqlx::{Executor, MySqlPool, mysql::MySqlPoolOptions};
use uuid::Uuid;

use backend_cases::{BackendHarness, TestResult, ts, upsert_relation};

const DEFAULT_MYSQL_DATABASE_URL: &str = "mysql://keepsake:keepsake@localhost:53306/keepsake";

struct MySqlHarness;

#[async_trait::async_trait]
impl BackendHarness for MySqlHarness {
    const BACKEND: &'static str = "mysql";

    type Pool = MySqlPool;
    type Repo = MySqlKeepsakeRepository;

    async fn repo() -> TestResult<(Self::Repo, Self::Pool)> {
        let pool = mysql_pool().await?;
        reset_schema(&pool).await?;
        let repo = MySqlKeepsakeRepository::new(pool.clone());
        repo.migrate().await?;
        Ok((repo, pool))
    }

    async fn backend_marker(pool: &Self::Pool) -> Result<String, sqlx::Error> {
        sqlx::query_scalar("select value from keepsake_schema_metadata where `key` = 'backend'")
            .fetch_one(pool)
            .await
    }

    async fn upsert_relation(
        repo: &Self::Repo,
        relation: &keepsake::RelationDefinition,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<keepsake::RelationDefinition, RepositoryError> {
        repo.upsert_relation(relation, at).await
    }

    async fn apply(
        repo: &Self::Repo,
        command: &keepsake::ApplyKeepsake,
    ) -> Result<keepsake_sqlx::AppliedKeepsake, RepositoryError> {
        repo.apply(command).await
    }

    async fn active_relations_for_subject(
        repo: &Self::Repo,
        subject: &keepsake::SubjectRef,
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError> {
        repo.active_relations_for_subject(subject).await
    }

    async fn active_for_subject(
        repo: &Self::Repo,
        subject: &keepsake::SubjectRef,
    ) -> Result<Vec<keepsake::Keepsake>, RepositoryError> {
        repo.active_for_subject(subject).await
    }

    async fn expire_due_timed(
        repo: &Self::Repo,
        now: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        repo.expire_due_timed(now, limit).await
    }

    async fn upsert_counter_projection(
        repo: &Self::Repo,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), RepositoryError> {
        repo.upsert_counter_projection(keepsake_id, key, value, observed_at)
            .await
    }

    async fn expire_due_fulfilled(
        repo: &Self::Repo,
        now: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        repo.expire_due_fulfilled(now, limit).await
    }
}

async fn mysql_pool() -> TestResult<MySqlPool> {
    let database_url = std::env::var("MYSQL_DATABASE_URL")
        .unwrap_or_else(|_| DEFAULT_MYSQL_DATABASE_URL.to_owned());
    Ok(MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?)
}

async fn reset_schema(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    pool.execute("set foreign_key_checks = 0").await?;
    for query in [
        "drop table if exists keepsake_audit_context_attributes",
        "drop table if exists keepsake_audit_events",
        "drop table if exists keepsake_fulfillment_counters",
        "drop table if exists keepsakes",
        "drop table if exists keepsake_relation_definitions",
        "drop table if exists keepsake_schema_metadata",
        "drop table if exists _sqlx_migrations",
    ] {
        pool.execute(query).await?;
    }
    pool.execute("set foreign_key_checks = 1").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_migration_initializes_backend_marker() -> TestResult<()> {
    backend_cases::migration_initializes_backend_marker::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_apply_duplicate_and_active_read() -> TestResult<()> {
    backend_cases::apply_duplicate_and_active_read::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_timed_expiry_expires_due_keepsake() -> TestResult<()> {
    backend_cases::timed_expiry_expires_due_keepsake::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_lifecycle_invariants_reject_invalid_rows() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?, 'account', 'invalid', ?, 'applied', ?, ?, null, null, ?, '{}', ?, ?)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(serde_json::to_value(&ExpiryPolicy::ManualOnly)?)
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_lifecycle_invariants_reject_malformed_policy_rows() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?, 'account', 'malformed', ?, 'applied', '{}', ?, null, null, null, '{}', ?, ?)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_projection_invariant_rejects_fractional_expiry_mismatch() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let relation = upsert_relation::<MySqlHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let policy = serde_json::json!({
        "type": "at",
        "timestamp": "2026-01-01T00:00:00.123456Z"
    });
    let result = sqlx::query(
        r"
        insert into keepsakes
            (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
             expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at)
        values (?, 'account', 'fractional', ?, 'applied', ?, ?, ?, null, null, '{}', ?, ?)
        ",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(relation.id.to_string())
    .bind(policy)
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00.654321Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .bind(ts("2026-01-01T00:00:00Z")?.naive_utc())
    .execute(&pool)
    .await;

    assert!(matches!(result, Err(sqlx::Error::Database(_))));
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_fulfilled_expiry_uses_counter_snapshot() -> TestResult<()> {
    backend_cases::fulfilled_expiry_uses_counter_snapshot::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_migration_rejects_wrong_backend_marker() -> TestResult<()> {
    let pool = mysql_pool().await?;
    reset_schema(&pool).await?;
    sqlx::query(
        "create table keepsake_schema_metadata (`key` varchar(191) primary key, value varchar(191) not null)",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "insert into keepsake_schema_metadata (`key`, value) values ('backend', 'postgres')",
    )
    .execute(&pool)
    .await?;

    let repo = MySqlKeepsakeRepository::new(pool);
    let result = repo.migrate().await;

    assert!(matches!(
        result,
        Err(RepositoryError::BackendMismatch {
            expected: "mysql",
            actual
        }) if actual == "postgres"
    ));
    Ok(())
}
