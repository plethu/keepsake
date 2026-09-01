#[path = "../support/backend_cases.rs"]
pub mod backend_cases;

use keepsake_sqlx::{
    RepositoryError, SqliteBackend, SqliteKeepsakeRepository, TenantSqlxKeepsakeRepository,
};
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;

pub use backend_cases::{BackendHarness, TestResult, ts, upsert_relation};

pub struct SqliteHarness;

#[async_trait::async_trait]
impl BackendHarness for SqliteHarness {
    const BACKEND: &'static str = "sqlite";
    const TENANT: &'static str = "sqlite-test-tenant";

    type Pool = sqlx::SqlitePool;
    type Repo = TenantSqlxKeepsakeRepository<'static, SqliteBackend>;

    async fn repo() -> TestResult<(Self::Repo, Self::Pool)> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let root = Box::leak(Box::new(SqliteKeepsakeRepository::new(
            pool.clone(),
            "https://tests.invalid/keepsake/sqlite",
        )?));
        root.migrate().await?;
        sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
            .execute(&pool)
            .await?;
        Ok((root.for_tenant(Self::tenant()), pool))
    }

    async fn backend_marker(pool: &Self::Pool) -> Result<String, sqlx::Error> {
        sqlx::query_scalar("select value from keepsake_schema_metadata where key = 'backend'")
            .fetch_one(pool)
            .await
    }

    async fn upsert_relation(
        repo: &Self::Repo,
        relation: &keepsake::RelationDefinition,
        at: time::OffsetDateTime,
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

    async fn active_relations_for_subject_by_ids(
        repo: &Self::Repo,
        subject: &keepsake::SubjectRef,
        relation_ids: &[Uuid],
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError> {
        repo.active_relations_for_subject_by_ids(subject, relation_ids)
            .await
    }

    async fn active_relations_for_subject_by_keys(
        repo: &Self::Repo,
        subject: &keepsake::SubjectRef,
        keys: &[keepsake::RelationKey],
    ) -> Result<Vec<keepsake_sqlx::ActiveRelation>, RepositoryError> {
        repo.active_relations_for_subject_by_keys(subject, keys)
            .await
    }

    async fn active_for_subject(
        repo: &Self::Repo,
        subject: &keepsake::SubjectRef,
    ) -> Result<Vec<keepsake::Keepsake>, RepositoryError> {
        repo.active_for_subject(subject).await
    }

    async fn expire_due_timed(
        repo: &Self::Repo,
        now: time::OffsetDateTime,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        repo.expire_due_timed(now, limit).await
    }

    async fn upsert_counter_projection(
        repo: &Self::Repo,
        keepsake_id: Uuid,
        key: &str,
        value: i64,
        observed_at: time::OffsetDateTime,
    ) -> Result<(), RepositoryError> {
        repo.upsert_counter_projection(keepsake_id, key, value, observed_at)
            .await
    }

    async fn set_relation_enabled(
        repo: &Self::Repo,
        relation_id: Uuid,
        enabled: bool,
        at: time::OffsetDateTime,
    ) -> Result<bool, RepositoryError> {
        repo.set_relation_enabled(relation_id, enabled, at).await
    }

    async fn expire_due_fulfilled(
        repo: &Self::Repo,
        now: time::OffsetDateTime,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        repo.expire_due_fulfilled(now, limit).await
    }
}
