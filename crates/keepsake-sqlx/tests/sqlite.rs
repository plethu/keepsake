#![allow(missing_docs)]
#![cfg(feature = "sqlite-tests")]

#[path = "support/backend_cases.rs"]
mod backend_cases;

use keepsake::ExpiryPolicy;
use keepsake_sqlx::{RepositoryError, SqliteKeepsakeRepository};
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;

use backend_cases::{BackendHarness, TestResult, upsert_relation};

struct SqliteHarness;

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

    async fn expire_due_fulfilled(
        repo: &Self::Repo,
        now: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> Result<u64, RepositoryError> {
        repo.expire_due_fulfilled(now, limit).await
    }
}

#[tokio::test]
async fn sqlite_migration_initializes_backend_marker() -> TestResult<()> {
    backend_cases::migration_initializes_backend_marker::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_apply_duplicate_and_active_read() -> TestResult<()> {
    backend_cases::apply_duplicate_and_active_read::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_timed_expiry_expires_due_keepsake() -> TestResult<()> {
    backend_cases::timed_expiry_expires_due_keepsake::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_lifecycle_invariants_reject_invalid_rows() -> TestResult<()> {
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
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
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
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
    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
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
    backend_cases::fulfilled_expiry_uses_counter_snapshot::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_apply_persists_multiple_audit_context_attributes() -> TestResult<()> {
    use keepsake::{ActorRef, ApplyKeepsake, CommandContext, SubjectRef};

    let (repo, pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let context = CommandContext::new(ActorRef::new("test", "worker")?)
        .with_idempotency_key("request-1")
        .with_metadata("request_id", "req_123")
        .with_metadata("source", "support");
    let command = ApplyKeepsake::new(
        SubjectRef::new("account", "sqlite_acct_attrs")?,
        relation.id,
        backend_cases::ts("2026-01-01T00:01:00Z")?,
        context,
    );

    repo.apply(&command).await?;

    let attributes = sqlx::query_as::<_, (String, String)>(
        "select key, value from keepsake_audit_context_attributes order by key",
    )
    .fetch_all(&pool)
    .await?;

    assert_eq!(
        attributes,
        vec![
            ("idempotency_key".to_owned(), "request-1".to_owned()),
            ("request_id".to_owned(), "req_123".to_owned()),
            ("source".to_owned(), "support".to_owned()),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_audit_event_read_paginates_in_order() -> TestResult<()> {
    use keepsake::{
        ActorRef, ApplyKeepsake, AuditEventType, CommandContext, RevokeKeepsake, SubjectRef,
    };
    use keepsake_sqlx::AuditCursor;

    let (repo, _pool) = SqliteHarness::repo().await?;
    let relation = upsert_relation::<SqliteHarness>(&repo, ExpiryPolicy::ManualOnly).await?;
    let subject = SubjectRef::new("account", "sqlite_acct_audit")?;
    let context = CommandContext::new(ActorRef::new("test", "worker")?)
        .with_idempotency_key("req-1")
        .with_metadata("source", "support");
    let applied = repo
        .apply(&ApplyKeepsake::new(
            subject,
            relation.id,
            backend_cases::ts("2026-01-01T00:01:00Z")?,
            context,
        ))
        .await?;
    repo.revoke(&RevokeKeepsake::new(
        applied.keepsake.id(),
        backend_cases::ts("2026-01-01T00:02:00Z")?,
        CommandContext::new(ActorRef::new("test", "worker")?),
    ))
    .await?;

    let events = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 10)
        .await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.event_type, AuditEventType::Apply);
    assert_eq!(events[1].event.event_type, AuditEventType::Revoke);
    assert_eq!(
        events[0].event.context.attributes.get("source").cloned(),
        Some("support".to_owned())
    );
    assert_eq!(
        events[0]
            .event
            .context
            .attributes
            .get("idempotency_key")
            .cloned(),
        Some("req-1".to_owned())
    );
    assert!(events[1].event.context.attributes.is_empty());

    let first = repo
        .audit_events_for_keepsake(applied.keepsake.id(), None, 1)
        .await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].event.event_type, AuditEventType::Apply);
    let next = repo
        .audit_events_for_keepsake(
            applied.keepsake.id(),
            Some(&AuditCursor::after(&first[0])),
            10,
        )
        .await?;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].event.event_type, AuditEventType::Revoke);

    let by_relation = repo.audit_events_for_relation(relation.id, None, 10).await?;
    assert_eq!(by_relation.len(), 2);
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
