#![allow(missing_docs)]

use chrono::{DateTime, Utc};
use keepsake::{
    ActorRef, ApplyKeepsake, AuditEventId, CommandContext, ExpiryPolicy, RelationDefinition,
    RelationKey, SubjectRef, TenantId,
};
use keepsake_sqlx::{DovecoteAuditConfig, SqliteKeepsakeRepository};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use uuid::Uuid;

async fn repository()
-> Result<(SqliteKeepsakeRepository, sqlx::SqlitePool), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let repository = SqliteKeepsakeRepository::new(pool.clone(), "https://example.test/keepsake")?;
    repository.migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    repository.check_schema().await?;
    Ok((repository, pool))
}

async fn pool() -> Result<sqlx::SqlitePool, sqlx::Error> {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
}

fn timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|value| value.with_timezone(&Utc))
}

fn tenant() -> TenantId {
    TenantId::new("sqlite-test-tenant").unwrap_or_else(|_| unreachable!("test tenant is valid"))
}

async fn seed_importer_evidence(
    pool: &sqlx::SqlitePool,
    audit_high_water: i64,
    outbox_high_water: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("insert into keepsake_upgrade_evidence (evidence_id, evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete) values (1, 1, 'keepsake-dovecote-importer', 'https://example.test/keepsake', 'keepsake-sqlx-1.1', 'keepsake-audit', ?, ?, 0, 0, 0, 0, 0, 'keepsake.audit.json.v1', 1)")
        .bind(audit_high_water)
        .bind(outbox_high_water)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn clean_track_writes_one_exact_typed_dovecote_event()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, pool) = repository().await?;
    let repository = root.for_tenant(tenant());
    let relation = RelationDefinition::new(
        tenant(),
        Uuid::now_v7(),
        RelationKey::new("tag", "trusted")?,
        true,
        ExpiryPolicy::ManualOnly,
    )?;
    repository
        .upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    let command = ApplyKeepsake::new(
        tenant(),
        SubjectRef::new("account", "café")?,
        relation.id,
        timestamp("2026-01-01T00:01:00.123456Z")?,
        CommandContext::new(ActorRef::new("operator", "mari")?),
    );
    let expected_id = command.audit_id;
    let expected = repository.apply(&command).await?;
    repository.apply(&command).await?;
    let mut changed = command.clone();
    changed.context = CommandContext::new(ActorRef::new("operator", "different")?);
    assert!(repository.apply(&changed).await.is_err());
    let distinct_duplicate = ApplyKeepsake::new(
        tenant(),
        SubjectRef::new("account", "café")?,
        relation.id,
        command.at,
        command.context.clone(),
    );
    assert_ne!(distinct_duplicate.audit_id, expected_id);
    repository.apply(&distinct_duplicate).await?;

    let row = sqlx::query(
        "select stream, event_id, source, event_type, occurred_at, datacontenttype, data_kind, data from dovecote_events",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.try_get::<String, _>("stream")?, "keepsake-audit");
    assert_eq!(
        row.try_get::<String, _>("event_id")?,
        format!("keepsake-audit-{}", expected_id.as_uuid())
    );
    assert_eq!(
        row.try_get::<String, _>("source")?,
        "https://example.test/keepsake"
    );
    assert_eq!(
        row.try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        row.try_get::<String, _>("occurred_at")?,
        "2026-01-01T00:01:00.123456Z"
    );
    assert_eq!(
        row.try_get::<String, _>("datacontenttype")?,
        "application/json"
    );
    assert_eq!(row.try_get::<String, _>("data_kind")?, "json");
    let bytes = row.try_get::<Vec<u8>, _>("data")?;
    let decoded: keepsake::AuditEvent = serde_json::from_slice(&bytes)?;
    assert_eq!(decoded.id, expected_id);
    assert_eq!(decoded.keepsake_id, expected.keepsake.id());
    assert_eq!(decoded.at, command.at);
    assert_eq!(decoded.subject.id(), "café");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        2
    );
    assert!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_audit_events")
            .fetch_one(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_upgrade_evidence")
            .fetch_one(&pool)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn identical_audit_identity_is_independent_per_tenant()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, pool) = repository().await?;
    let tenant_a = TenantId::new("sqlite-tenant-a")?;
    let tenant_b = TenantId::new("sqlite-tenant-b")?;
    let repo_a = root.for_tenant(tenant_a.clone());
    let repo_b = root.for_tenant(tenant_b.clone());
    let relation_id = Uuid::from_u128(71);

    for (repo, tenant, key) in [
        (&repo_a, tenant_a.clone(), "trusted-a"),
        (&repo_b, tenant_b.clone(), "trusted-b"),
    ] {
        let relation = RelationDefinition::new(
            tenant,
            relation_id,
            RelationKey::new("tag", key)?,
            true,
            ExpiryPolicy::ManualOnly,
        )?;
        repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
            .await?;
    }

    let audit_id = AuditEventId::from_uuid(Uuid::from_u128(72));
    let mut command_a = ApplyKeepsake::new(
        tenant_a.clone(),
        SubjectRef::new("account", "shared")?,
        relation_id,
        timestamp("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("operator", "a")?),
    );
    command_a.audit_id = audit_id;
    let mut command_b = ApplyKeepsake::new(
        tenant_b.clone(),
        SubjectRef::new("account", "shared")?,
        relation_id,
        timestamp("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("operator", "b")?),
    );
    command_b.audit_id = audit_id;

    repo_a.apply(&command_a).await?;
    repo_b.apply(&command_b).await?;

    let rows =
        sqlx::query("select tenant_id, source, event_id from dovecote_events order by row_id")
            .fetch_all(&pool)
            .await?;
    assert_eq!(rows.len(), 2);
    for (row, tenant) in rows.iter().zip([tenant_a.clone(), tenant_b.clone()]) {
        assert_eq!(row.try_get::<String, _>("tenant_id")?, tenant.as_str());
        assert_eq!(
            row.try_get::<String, _>("source")?,
            "https://example.test/keepsake"
        );
        assert_eq!(
            row.try_get::<String, _>("event_id")?,
            format!("keepsake-audit-{}", audit_id.as_uuid())
        );
    }

    let config = DovecoteAuditConfig::new("https://example.test/keepsake")?;
    let adapter = dovecote_sqlx_sqlite::SqliteDovecote::new(pool);
    let page_a = adapter
        .for_tenant(dovecote::TenantId::new(tenant_a.as_str())?)
        .page(None, dovecote::Limit::new(10)?)
        .await?;
    let page_b = adapter
        .for_tenant(dovecote::TenantId::new(tenant_b.as_str())?)
        .page(None, dovecote::Limit::new(10)?)
        .await?;
    assert_eq!(page_a.len(), 1);
    assert_eq!(page_b.len(), 1);
    assert_eq!(
        keepsake_sqlx::decode_audit_event(&config, &page_a[0])?.tenant_id,
        tenant_a
    );
    assert_eq!(
        keepsake_sqlx::decode_audit_event(&config, &page_b[0])?.tenant_id,
        tenant_b
    );
    Ok(())
}

#[test]
fn source_configuration_rejects_relative_uris() {
    let result = DovecoteAuditConfig::new("keepsake");
    assert!(result.is_err(), "source must be absolute");
}

#[tokio::test]
async fn upgrade_track_activates_clean_schema_marker() -> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let repository = SqliteKeepsakeRepository::new(pool.clone(), "https://example.test/keepsake")?;
    repository.upgrade_migrate().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from sqlite_master where type = 'table' and name = 'keepsake_dovecote_bridge_config'",
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    assert!(repository.check_schema().await.is_err());
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    assert!(repository.activate_upgrade().await.is_err());
    seed_importer_evidence(&pool, 0, 0).await?;
    repository.activate_upgrade().await?;
    repository.check_schema().await?;
    let track = sqlx::query_scalar::<_, String>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(track, "2");
    assert!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_audit_events")
            .fetch_one(&pool)
            .await
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn activation_rejects_corrupt_upgrade_domain_before_writing_marker()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let repository = SqliteKeepsakeRepository::new(pool.clone(), "https://example.test/keepsake")?;
    repository.upgrade_migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    seed_importer_evidence(&pool, 0, 0).await?;

    // Evidence is valid, but the upgrade-track domain shape is not. Activation
    // must perform the complete catalog check before it can write api_track=2.
    sqlx::query("drop trigger keepsakes_lifecycle_timestamps_update")
        .execute(&pool)
        .await?;
    assert!(repository.activate_upgrade().await.is_err());
    let track = sqlx::query_scalar::<_, String>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(&pool)
    .await?;
    assert_ne!(track.as_deref(), Some("2"));
    Ok(())
}

#[tokio::test]
async fn migration_tracks_refuse_cross_use() -> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let repository = SqliteKeepsakeRepository::new(pool, "https://example.test/keepsake")?;
    repository.migrate().await?;
    assert!(repository.upgrade_migrate().await.is_err());
    Ok(())
}

#[tokio::test]
async fn tenant_upgrade_requires_mapping_and_activates_v3_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let relation_id = Uuid::from_u128(42);
    let keepsake_id = Uuid::from_u128(43);
    sqlx::raw_sql(include_str!(
        "../migrations/v2/sqlite/2000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(
        "insert into keepsake_relation_definitions (id, kind, key, enabled, expiry_policy, created_at, updated_at) values (?, 'tag', 'mapped', 1, ?, ?, ?)",
    )
    .bind(Uuid::from_u128(42).to_string())
    .bind(serde_json::json!({"type": "manual_only"}).to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await?;
    let repository =
        SqliteKeepsakeRepository::new(pool.clone(), "https://example.test/keepsake-upgrade")?;
    sqlx::query(
        "insert into keepsakes (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at) values (?, 'account', 'mapped', ?, 'applied', ?, ?, null, null, null, '{}', ?, ?)",
    )
    .bind(keepsake_id.to_string())
    .bind(relation_id.to_string())
    .bind(serde_json::json!({"type": "manual_only"}).to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await?;
    repository.prepare_tenant_upgrade().await?;
    assert!(repository.activate_tenant_upgrade().await.is_err());
    sqlx::query("update keepsake_relation_definitions set tenant_id = 'mapped-tenant'")
        .execute(&pool)
        .await?;
    assert!(repository.activate_tenant_upgrade().await.is_err());
    sqlx::query("update keepsakes set tenant_id = 'mapped-tenant'")
        .execute(&pool)
        .await?;
    repository.activate_tenant_upgrade().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("pragma foreign_keys")
            .fetch_one(&pool)
            .await?,
        1
    );
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    repository.check_schema().await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "select value from keepsake_schema_metadata where key = 'api_track'",
        )
        .fetch_one(&pool)
        .await?,
        "3"
    );
    Ok(())
}

#[tokio::test]
async fn tenant_upgrade_activation_rolls_back_populated_v2_on_validation_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let relation_id = Uuid::from_u128(42);
    let keepsake_id = Uuid::from_u128(43);
    sqlx::raw_sql(include_str!(
        "../migrations/v2/sqlite/2000_clean_baseline.sql"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(
        "insert into keepsake_relation_definitions (id, kind, key, enabled, expiry_policy, created_at, updated_at) values (?, 'tag', 'rollback', 1, ?, ?, ?)",
    )
    .bind(relation_id.to_string())
    .bind(serde_json::json!({"type": "manual_only"}).to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await?;

    // Inject a populated but invalid graph while foreign-key enforcement is
    // disabled. The activation validator must reject it after rebuilding and
    // leave the prepared v2 schema untouched.
    sqlx::raw_sql("pragma foreign_keys = off")
        .execute(&pool)
        .await?;
    sqlx::query(
        "insert into keepsakes (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, fulfilled_at, revoked_at, metadata, created_at, updated_at) values (?, 'account', 'rollback', ?, 'applied', ?, ?, null, null, null, '{}', ?, ?)",
    )
    .bind(keepsake_id.to_string())
    .bind(Uuid::from_u128(99).to_string())
    .bind(serde_json::json!({"type": "manual_only"}).to_string())
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.000000Z")
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await?;
    sqlx::raw_sql("pragma foreign_keys = on")
        .execute(&pool)
        .await?;

    let repository =
        SqliteKeepsakeRepository::new(pool.clone(), "https://example.test/keepsake-rollback")?;
    repository.prepare_tenant_upgrade().await?;
    sqlx::query("update keepsake_relation_definitions set tenant_id = 'mapped-tenant'")
        .execute(&pool)
        .await?;
    sqlx::query("update keepsakes set tenant_id = 'mapped-tenant'")
        .execute(&pool)
        .await?;

    assert!(repository.activate_tenant_upgrade().await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from sqlite_master where type = 'table' and name = 'keepsakes_v2'",
        )
        .fetch_one(&pool)
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsakes")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "select value from keepsake_schema_metadata where key = 'api_track'",
        )
        .fetch_one(&pool)
        .await?,
        "2"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("pragma foreign_keys")
            .fetch_one(&pool)
            .await?,
        1
    );

    // Repair the operator-owned mapping and prove the same prepared database
    // can complete the atomic activation after the failed attempt.
    sqlx::query("update keepsakes set relation_id = ?")
        .bind(relation_id.to_string())
        .execute(&pool)
        .await?;
    repository.activate_tenant_upgrade().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("pragma foreign_keys")
            .fetch_one(&pool)
            .await?,
        1
    );
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    repository.check_schema().await?;
    Ok(())
}

#[tokio::test]
async fn enqueue_failure_rolls_back_domain_mutation_and_event()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, pool) = repository().await?;
    let repository = root.for_tenant(tenant());
    let relation = RelationDefinition::new(
        tenant(),
        Uuid::now_v7(),
        RelationKey::new("tag", "rollback")?,
        true,
        ExpiryPolicy::ManualOnly,
    )?;
    repository
        .upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    sqlx::query("drop table dovecote_deliveries")
        .execute(&pool)
        .await?;

    let command = ApplyKeepsake::new(
        tenant(),
        SubjectRef::new("account", "rollback")?,
        relation.id,
        timestamp("2026-01-01T00:01:00Z")?,
        CommandContext::new(ActorRef::new("operator", "mari")?),
    );
    assert!(repository.apply(&command).await.is_err());
    assert!(
        repository
            .active_for_subject(&command.subject)
            .await?
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn check_schema_rejects_a_corrupted_dovecote_shape() -> Result<(), Box<dyn std::error::Error>>
{
    let (repository, pool) = repository().await?;
    sqlx::query("drop index dovecote_events_tenant_source_event_id")
        .execute(&pool)
        .await?;
    assert!(repository.check_schema().await.is_err());
    Ok(())
}

#[tokio::test]
async fn check_schema_rejects_a_rebuilt_domain_table_with_a_missing_column()
-> Result<(), Box<dyn std::error::Error>> {
    let (repository, pool) = repository().await?;
    sqlx::query("drop trigger keepsakes_clean_invariants_insert")
        .execute(&pool)
        .await?;
    sqlx::query("drop trigger keepsakes_clean_invariants_update")
        .execute(&pool)
        .await?;
    sqlx::query("drop index keepsakes_one_active_relation_per_subject")
        .execute(&pool)
        .await?;
    sqlx::query("drop index keepsakes_active_subject_lookup")
        .execute(&pool)
        .await?;
    sqlx::query("drop index keepsakes_active_relation_membership")
        .execute(&pool)
        .await?;
    sqlx::query("drop index keepsakes_due_timed_expiry")
        .execute(&pool)
        .await?;
    sqlx::query("drop index keepsakes_due_fulfilled_expiry")
        .execute(&pool)
        .await?;
    sqlx::query("alter table keepsakes rename to keepsakes_rebuilt")
        .execute(&pool)
        .await?;
    sqlx::raw_sql(
        "create table keepsakes (
            id text primary key,
            subject_kind text not null,
            subject_id text not null,
            relation_id text not null references keepsake_relation_definitions(id),
            state text not null check (state in ('applied', 'revoked', 'expired')),
            expiry_policy text not null check (json_valid(expiry_policy)),
            applied_at text not null,
            expires_at text,
            fulfilled_at text,
            revoked_at text,
            metadata text not null default '{}' check (json_valid(metadata)),
            created_at text not null
        )",
    )
    .execute(&pool)
    .await?;
    assert!(repository.check_schema().await.is_err());
    Ok(())
}

#[tokio::test]
async fn check_schema_rejects_a_missing_or_wrong_domain_index()
-> Result<(), Box<dyn std::error::Error>> {
    let (repository1, pool) = repository().await?;
    sqlx::query("drop index keepsakes_due_timed_expiry")
        .execute(&pool)
        .await?;
    assert!(repository1.check_schema().await.is_err());

    let (repository2, pool) = repository().await?;
    sqlx::query("drop index keepsakes_due_timed_expiry")
        .execute(&pool)
        .await?;
    sqlx::query(
        "create index keepsakes_due_timed_expiry
         on keepsakes (subject_kind, subject_id, relation_id)
         where state = 'applied'",
    )
    .execute(&pool)
    .await?;
    assert!(repository2.check_schema().await.is_err());
    Ok(())
}

#[tokio::test]
async fn check_schema_rejects_a_missing_or_wrong_domain_trigger()
-> Result<(), Box<dyn std::error::Error>> {
    let (repository2, pool) = repository().await?;
    sqlx::query("drop trigger keepsakes_clean_invariants_insert")
        .execute(&pool)
        .await?;
    assert!(repository2.check_schema().await.is_err());

    let (repository, pool) = repository().await?;
    sqlx::query("drop trigger keepsakes_clean_invariants_insert")
        .execute(&pool)
        .await?;
    sqlx::raw_sql(
        "create trigger keepsakes_clean_invariants_insert
         before insert on keepsakes
         for each row
         begin
           select raise(abort, 'wrong invariant');
         end",
    )
    .execute(&pool)
    .await?;
    assert!(repository.check_schema().await.is_err());
    Ok(())
}

#[tokio::test]
async fn activation_rejects_nonzero_reconciliation_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = pool().await?;
    let repository = SqliteKeepsakeRepository::new(pool.clone(), "https://example.test/keepsake")?;
    repository.upgrade_migrate().await?;
    sqlx::raw_sql(dovecote_sqlx_sqlite::MIGRATIONS[0].sql())
        .execute(&pool)
        .await?;
    seed_importer_evidence(&pool, 0, 0).await?;
    sqlx::query("update keepsake_upgrade_evidence set missing_count = 1")
        .execute(&pool)
        .await?;
    assert!(repository.activate_upgrade().await.is_err());
    let track = sqlx::query_scalar::<_, String>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(&pool)
    .await?;
    assert_ne!(track.as_deref(), Some("2"));
    Ok(())
}
