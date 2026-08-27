#![allow(missing_docs)]
#![cfg(all(feature = "postgres-tests", feature = "dovecote-postgres"))]

//! Live `PostgreSQL` evidence for the opt-in Keepsake/Dovecote bridge.
//!
//! These tests deliberately exercise the bridge through its public repository
//! view. They use a disposable database supplied by `DATABASE_URL`, just like
//! the existing `PostgreSQL` integration target, and are ignored in ordinary
//! local runs because they mutate that database.

use chrono::{DateTime, Utc};
use dovecote::{Lease, Limit, WorkerId};
use dovecote_sqlx_postgres::{MIGRATIONS, PostgresDovecote};
use keepsake::{
    ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent, AuditEventType,
    CommandContext, ExpiryPolicy, RelationDefinition, RelationKey, SubjectRef,
};
use keepsake_sqlx::{BridgeError, BridgeImportOptions, DovecoteBridgeConfig, KeepsakeRepository};
use sqlx::{PgPool, Row, postgres::PgPoolOptions, query, raw_sql};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn postgres_bridge_dual_write_preserves_identity_payload_and_pending_state() -> TestResult<()>
{
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let relation = relation(100)?;
    repository
        .upsert_relation(&relation, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repository.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let command = ApplyKeepsake::new(
        SubjectRef::new("account", "pg-bridge")?,
        relation.id,
        ts("2026-01-01T00:00:00.123456Z")?,
        CommandContext::new(ActorRef::new("system", "postgres")?)
            .with_metadata("request", "pg-bridge"),
    );
    bridge.apply(&command).await?;

    let row = query(
        "select o.id, o.event_type, o.payload as outbox_payload, l.source, l.stream, l.event_id, l.occurred_at, l.payload as ledger_payload, l.dovecote_row_id, d.state, e.data as event_payload, e.datacontenttype, e.occurred_at as event_occurred_at from keepsake_audit_outbox o join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'outbox' and l.legacy_id = o.id join dovecote_events e on e.row_id = l.dovecote_row_id join dovecote_deliveries d on d.event_row_id = e.row_id",
    )
    .fetch_one(&pool)
    .await?;
    let outbox_id: i64 = row.try_get("id")?;
    assert_eq!(
        row.try_get::<String, _>("source")?,
        "https://example.org/keepsake"
    );
    assert_eq!(row.try_get::<String, _>("stream")?, "keepsake-audit");
    assert_eq!(
        row.try_get::<String, _>("event_id")?,
        format!("keepsake-outbox-{outbox_id}")
    );
    assert_eq!(
        row.try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        row.try_get::<DateTime<Utc>, _>("occurred_at")?,
        ts("2026-01-01T00:00:00.123456Z")?
    );
    assert_eq!(row.try_get::<String, _>("state")?, "pending");
    assert_eq!(
        row.try_get::<String, _>("datacontenttype")?,
        "application/json"
    );
    assert_eq!(
        row.try_get::<Option<DateTime<Utc>>, _>("event_occurred_at")?,
        Some(ts("2026-01-01T00:00:00.123456Z")?)
    );
    let outbox_payload: serde_json::Value = row.try_get("outbox_payload")?;
    let ledger_payload: Vec<u8> = row.try_get("ledger_payload")?;
    let event_payload: Vec<u8> = row.try_get("event_payload")?;
    // PostgreSQL JSONB has one semantic representation. The bridge ledger and
    // Dovecote retain the exact UTF-8 bytes emitted by the typed codec.
    assert_eq!(ledger_payload, event_payload);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&event_payload)?,
        outbox_payload
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
#[allow(clippy::too_many_lines)]
async fn postgres_bridge_finalizer_rejects_event_and_ledger_drift() -> TestResult<()> {
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let audit_only_id = insert_audit(
        &pool,
        &audit_event(106, "2025-12-31T23:00:00Z", "audit-only")?,
    )
    .await?;
    let relation = relation(105)?;
    repository
        .upsert_relation(&relation, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repository.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    bridge
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "pg-finalizer")?,
            relation.id,
            ts("2026-01-01T00:00:00.123456Z")?,
            CommandContext::new(ActorRef::new("system", "postgres")?),
        ))
        .await?;
    let audit_id: i64 = query("select max(id) as id from keepsake_audit_events")
        .fetch_one(&pool)
        .await?
        .try_get("id")?;
    let outbox_id: i64 = query("select max(id) as id from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?
        .try_get("id")?;
    let row_id: i64 = query(
        "select dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?
    .try_get("dovecote_row_id")?;
    let options = BridgeImportOptions::new(audit_id).with_outbox_high_water(outbox_id);
    assert!(bridge.import_history(&options).await?.complete);
    bridge.finalize_upgrade_reconciliation().await?;

    let audit_origin: String = query(
        "select payload_origin from keepsake_dovecote_bridge_ledger where legacy_kind = 'audit' and legacy_id = $1",
    )
    .bind(audit_only_id)
    .fetch_one(&pool)
    .await?
    .try_get("payload_origin")?;
    assert_eq!(audit_origin, "reconstructed_v1");
    query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = $1 where legacy_kind = 'audit' and legacy_id = $2",
    )
    .bind("bridge_exact")
    .bind(audit_only_id)
    .execute(&pool)
    .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = $1 where legacy_kind = 'audit' and legacy_id = $2",
    )
    .bind(&audit_origin)
    .bind(audit_only_id)
    .execute(&pool)
    .await?;

    let outbox_origin: String = query(
        "select payload_origin from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?
    .try_get("payload_origin")?;
    query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = $1 where legacy_kind = 'outbox' and legacy_id = $2",
    )
    .bind("reconstructed_v1")
    .bind(outbox_id)
    .execute(&pool)
    .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = $1 where legacy_kind = 'outbox' and legacy_id = $2",
    )
    .bind(&outbox_origin)
    .bind(outbox_id)
    .execute(&pool)
    .await?;

    query("update dovecote_events set occurred_at = $1 where row_id = $2")
        .bind(ts("2026-01-01T00:00:00.123457Z")?)
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query("update dovecote_events set occurred_at = $1 where row_id = $2")
        .bind(ts("2026-01-01T00:00:00.123456Z")?)
        .bind(row_id)
        .execute(&pool)
        .await?;

    query("update dovecote_events set datacontenttype = $1 where row_id = $2")
        .bind("text/plain")
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query("update dovecote_events set datacontenttype = $1 where row_id = $2")
        .bind("application/json")
        .bind(row_id)
        .execute(&pool)
        .await?;

    let digest: String = query(
        "select payload_sha256 from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?
    .try_get("payload_sha256")?;
    query("update keepsake_dovecote_bridge_ledger set payload_sha256 = $1 where legacy_kind = 'outbox' and legacy_id = $2")
        .bind("0".repeat(64))
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query("update keepsake_dovecote_bridge_ledger set payload_sha256 = $1 where legacy_kind = 'outbox' and legacy_id = $2")
        .bind(digest)
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn postgres_bridge_claim_skips_unreconciled_legacy_outbox_rows() -> TestResult<()> {
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let old_event = audit_event(120, "2026-01-01T00:30:00Z", "legacy-only")?;
    let old_audit_id = insert_audit(&pool, &old_event).await?;
    let old_outbox_id = insert_outbox(&pool, old_audit_id, &old_event).await?;
    let relation = relation(121)?;
    repository
        .upsert_relation(&relation, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repository.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    bridge
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "pg-claim-filter")?,
            relation.id,
            ts("2026-01-01T01:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "postgres")?),
        ))
        .await?;
    let bridged_outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    assert!(old_outbox_id < bridged_outbox_id);
    let claims = bridge
        .claim_delivery(
            "bridge-worker",
            ts("2026-01-01T00:00:00Z")?,
            ts("2037-01-01T00:00:00Z")?,
            10,
        )
        .await?;
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].record().id, bridged_outbox_id);
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn postgres_bridge_history_fences_claims_and_preserves_delivered_as_nonpublishable()
-> TestResult<()> {
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let first = audit_event(101, "2026-01-01T01:00:00Z", "reconstructed")?;
    let second = audit_event(102, "2026-01-01T02:00:00Z", "delivered")?;
    let first_id = insert_audit(&pool, &first).await?;
    let second_id = insert_audit(&pool, &second).await?;
    let outbox_id = insert_outbox(&pool, second_id, &second).await?;
    sqlx::query(
        "update keepsake_audit_outbox set claimed_by = $1, claimed_until = $2 where id = $3",
    )
    .bind("legacy-worker")
    .bind(ts("2037-01-01T00:10:00Z")?)
    .bind(outbox_id)
    .execute(&pool)
    .await?;

    let bridge =
        repository.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let options = BridgeImportOptions::new(second_id).with_batch_size(2);
    let blocked = bridge.import_history(&options).await?;
    assert_eq!(
        (blocked.examined, blocked.imported, blocked.blocked),
        (2, 1, 1)
    );
    assert_eq!(blocked.cursor, first_id);
    assert!(!blocked.complete);

    sqlx::query(
        "update keepsake_audit_outbox set claimed_by = null, claimed_until = null, delivered_at = $1 where id = $2",
    )
    .bind(ts("2026-01-01T02:30:00.123456Z")?)
    .bind(outbox_id)
    .execute(&pool)
    .await?;
    let completed = bridge.import_history(&options).await?;
    assert_eq!((completed.examined, completed.imported), (1, 1));
    assert_eq!(completed.cursor, second_id);
    assert!(completed.complete);

    let delivered_row = query("select e.row_id from dovecote_events e where e.event_id = $1")
        .bind(format!("keepsake-outbox-{outbox_id}"))
        .fetch_one(&pool)
        .await?;
    let delivered_row_id: i64 = delivered_row.try_get("row_id")?;
    let delivery = query(
        "select d.state, d.delivered_at from dovecote_deliveries d where d.event_row_id = $1",
    )
    .bind(delivered_row_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(delivery.try_get::<String, _>("state")?, "delivered");
    assert_eq!(
        delivery.try_get::<Option<DateTime<Utc>>, _>("delivered_at")?,
        Some(ts("2026-01-01T02:30:00.123456Z")?)
    );
    let dovecote = PostgresDovecote::new(pool.clone());
    let claimed = dovecote
        .claim(
            WorkerId::new("postgres-history-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(10)?,
        )
        .await?;
    assert!(
        claimed
            .iter()
            .all(|event| { event.event().id().as_str() != format!("keepsake-outbox-{outbox_id}") })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn postgres_bridge_history_finds_a_later_old_writer_row_after_a_completed_pass()
-> TestResult<()> {
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let bridge =
        repository.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);

    // These rows model a 1.1 writer that has not yet been upgraded to the
    // bridge. The first bounded pass completes before the later writer row is
    // inserted, so the second high-water pass must reopen progress and find it.
    let first_event = audit_event(103, "2026-01-01T03:00:00Z", "first-old-writer")?;
    let first_audit_id = insert_audit(&pool, &first_event).await?;
    let _first_outbox_id = insert_outbox(&pool, first_audit_id, &first_event).await?;
    let first_pass = bridge
        .import_history(&BridgeImportOptions::new(first_audit_id))
        .await?;
    assert_eq!(
        (first_pass.imported, first_pass.cursor),
        (1, first_audit_id)
    );
    assert!(first_pass.complete);

    let second_event = audit_event(104, "2026-01-01T04:00:00Z", "second-old-writer")?;
    let second_audit_id = insert_audit(&pool, &second_event).await?;
    let second_outbox_id = insert_outbox(&pool, second_audit_id, &second_event).await?;
    let second_pass = bridge
        .import_history(&BridgeImportOptions::new(second_audit_id))
        .await?;
    assert_eq!(
        (second_pass.imported, second_pass.cursor),
        (1, second_audit_id)
    );
    assert!(second_pass.complete);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events where event_id = $1",)
            .bind(format!("keepsake-outbox-{second_outbox_id}"))
            .fetch_one(&pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn postgres_bridge_ack_fences_stale_lease_and_rolls_back_on_finalize_error() -> TestResult<()>
{
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let relation = relation(110)?;
    repository
        .upsert_relation(&relation, ts("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repository.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    bridge
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "pg-ack")?,
            relation.id,
            ts("2026-01-01T00:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "postgres")?),
        ))
        .await?;
    let outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    let new_lease = ts("2037-01-01T00:10:00Z")?;
    let stale = bridge
        .claim_delivery("worker-a", ts("2026-01-01T00:00:00Z")?, new_lease, 1)
        .await?
        .into_iter()
        .next()
        .ok_or("bridge claim did not return the outbox row")?;
    sqlx::query("update keepsake_audit_outbox set claimed_until = $1 where id = $2")
        .bind(ts("2000-01-01T00:00:00Z")?)
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    let current = bridge
        .claim_delivery("worker-a", ts("2026-01-01T00:00:00Z")?, new_lease, 1)
        .await?
        .into_iter()
        .next()
        .ok_or("bridge reclaim did not return the outbox row")?;
    assert_ne!(stale.claim_token(), current.claim_token());
    assert!(matches!(
        bridge
            .acknowledge_delivery(
                outbox_id,
                "worker-a",
                stale.claim_token(),
                ts("2026-01-01T00:01:00Z")?
            )
            .await,
        Err(BridgeError::DeliveryOwnership { outbox_id: id }) if id == outbox_id
    ));

    let row_id: i64 = sqlx::query_scalar(
        "select dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?;
    sqlx::query("update dovecote_deliveries set attempts = 1 where event_row_id = $1")
        .bind(row_id)
        .execute(&pool)
        .await?;
    let delivered_at = ts("2026-01-01T00:01:00.123456Z")?;
    assert!(matches!(
        bridge
            .acknowledge_delivery(outbox_id, "worker-a", current.claim_token(), delivered_at)
            .await,
        Err(BridgeError::DovecotePostgresFinalize(_))
    ));
    let legacy = query(
        "select delivered_at, claimed_by, claimed_until from keepsake_audit_outbox where id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?;
    assert!(
        legacy
            .try_get::<Option<DateTime<Utc>>, _>("delivered_at")?
            .is_none()
    );
    assert_eq!(
        legacy.try_get::<Option<String>, _>("claimed_by")?,
        Some("worker-a".to_owned())
    );
    assert_eq!(
        legacy.try_get::<Option<DateTime<Utc>>, _>("claimed_until")?,
        Some(new_lease)
    );
    Ok(())
}

async fn database() -> TestResult<Option<(KeepsakeRepository, PgPool)>> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping Keepsake Dovecote PostgreSQL bridge tests: DATABASE_URL is unset");
        return Ok(None);
    };

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    reset(&pool).await?;
    let repository = KeepsakeRepository::new(pool.clone());
    repository.migrate().await?;
    raw_sql(MIGRATIONS[0].sql()).execute(&pool).await?;
    PostgresDovecote::new(pool.clone()).check_schema().await?;
    Ok(Some((repository, pool)))
}

async fn reset(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Dovecote's PostgreSQL migration owns types and functions as well as
    // tables. Reset the disposable schema as a unit so a later Keepsake
    // migration never sees orphaned, unmarked Dovecote objects.
    query("drop schema public cascade").execute(pool).await?;
    query("create schema public").execute(pool).await?;
    Ok(())
}

fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn relation(id: u128) -> TestResult<RelationDefinition> {
    Ok(RelationDefinition::new(
        Uuid::from_u128(id),
        RelationKey::new("tag", format!("postgres-bridge-{id}"))?,
        true,
        ExpiryPolicy::ManualOnly,
    )?)
}

fn audit_event(id: u128, at: &str, history: &str) -> TestResult<AuditEvent> {
    let mut context = AuditContext::default();
    context
        .attributes
        .insert("history".to_owned(), history.to_owned());
    Ok(AuditEvent {
        event_type: AuditEventType::Apply,
        at: ts(at)?,
        actor: ActorRef::new("system", "history")?,
        keepsake_id: Uuid::from_u128(id),
        subject: SubjectRef::new("account", format!("acct-{id}"))?,
        relation_id: Uuid::from_u128(id + 1000),
        decision: AuditDecision::Applied {
            duplicate_prevented: false,
        },
        context,
    })
}

async fn insert_audit(pool: &PgPool, event: &AuditEvent) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "insert into keepsake_audit_events (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id, event_type, decision, occurred_at) values ($1, $2, $3, $4, $5, $6, $7, $8, $9) returning id",
    )
    .bind(event.keepsake_id)
    .bind(event.relation_id)
    .bind(event.subject.kind())
    .bind(event.subject.id())
    .bind(event.actor.kind())
    .bind(event.actor.id())
    .bind(event.event_type.as_str())
    .bind(serde_json::to_value(&event.decision)?)
    .bind(event.at)
    .fetch_one(pool)
    .await?)
}

async fn insert_outbox(pool: &PgPool, audit_id: i64, event: &AuditEvent) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "insert into keepsake_audit_outbox (audit_event_id, event_type, payload) values ($1, $2, $3) returning id",
    )
    .bind(audit_id)
    .bind("keepsake.audit_event_recorded")
    .bind(serde_json::to_value(event)?)
    .fetch_one(pool)
    .await?)
}
