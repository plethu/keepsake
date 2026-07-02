#[path = "../support/backend_cases.rs"]
pub mod backend_cases;

use keepsake_sqlx::{RepositoryError, SqliteKeepsakeRepository};
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;

pub use backend_cases::{BackendHarness, TestResult, upsert_relation};

pub struct SqliteHarness;

#[async_trait::async_trait]
impl BackendHarness for SqliteHarness {
    const BACKEND: &'static str = "sqlite";

    type Pool = sqlx::SqlitePool;
    type Repo = SqliteKeepsakeRepository;

    async fn repo() -> TestResult<(Self::Repo, Self::Pool)> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let repo = SqliteKeepsakeRepository::new(pool.clone());
        repo.migrate().await?;
        Ok((repo, pool))
    }

    async fn backend_marker(pool: &Self::Pool) -> Result<String, sqlx::Error> {
        sqlx::query_scalar("select value from keepsake_schema_metadata where key = 'backend'")
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
