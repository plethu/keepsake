#[path = "../support/backend_cases.rs"]
pub mod backend_cases;

use keepsake_sqlx::{MySqlKeepsakeRepository, RepositoryError};
use sqlx::{Executor, MySqlPool, mysql::MySqlPoolOptions};
use uuid::Uuid;

pub use backend_cases::{BackendHarness, TestResult, ts, upsert_relation};

const DEFAULT_MYSQL_DATABASE_URL: &str = "mysql://keepsake:keepsake@localhost:53306/keepsake";

pub struct MySqlHarness;

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

    async fn set_relation_enabled(
        repo: &Self::Repo,
        relation_id: Uuid,
        enabled: bool,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, RepositoryError> {
        repo.set_relation_enabled(relation_id, enabled, at).await
    }

    async fn expire_due_fulfilled(
        repo: &Self::Repo,
        now: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        repo.expire_due_fulfilled(now, limit).await
    }
}

pub async fn mysql_pool() -> TestResult<MySqlPool> {
    let database_url = std::env::var("MYSQL_DATABASE_URL")
        .unwrap_or_else(|_| DEFAULT_MYSQL_DATABASE_URL.to_owned());
    Ok(MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?)
}

pub async fn reset_schema(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    pool.execute("set foreign_key_checks = 0").await?;
    for query in [
        "drop table if exists keepsake_audit_outbox",
        "drop table if exists keepsake_audit_context_attributes",
        "drop table if exists keepsake_audit_events",
        "drop table if exists keepsake_fulfillment_checklist",
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
