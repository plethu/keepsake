#![allow(missing_docs)]
#![cfg(all(feature = "mysql-tests", feature = "dovecote-mysql"))]

//! Live MySQL-family evidence for the opt-in Keepsake/Dovecote bridge.
//!
//! The same target runs against `MySQL` LTS, `MySQL` Innovation, and `MariaDB` in
//! the repository's backend matrix. Each run uses the disposable
//! `MYSQL_DATABASE_URL` database and is ignored in ordinary local tests.

use chrono::{DateTime, Utc};
use dovecote::{Lease, Limit, WorkerId};
use dovecote_sqlx_mysql::{MIGRATIONS, MySqlDovecote};
use keepsake::{
    ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent, AuditEventType,
    CommandContext, ExpiryPolicy, RelationDefinition, RelationKey, SubjectRef,
};
use keepsake_sqlx::{
    BridgeError, BridgeImportOptions, DovecoteBridgeConfig, MySqlKeepsakeRepository,
};
use sqlx::{AssertSqlSafe, Executor, MySqlPool, Row, mysql::MySqlPoolOptions, query};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn mysql_bridge_migration_quotes_reserved_cursor_column() -> TestResult<()> {
    let migration = include_str!("../migrations/mysql/0006_dovecote_bridge.sql");
    let (config, ledger) = migration
        .split_once("create table keepsake_dovecote_bridge_ledger")
        .ok_or_else(|| {
            std::io::Error::other("bridge migration must define config before ledger")
        })?;
    assert!(config.contains("  source varchar(2048) not null,"));
    assert!(config.contains("  audit_cursor bigint not null default 0,"));
    assert!(config.contains("  outbox_cursor bigint not null default 0,"));
    assert!(!config.contains("  cursor bigint not null default 0,"));
    assert!(ledger.contains("  source varbinary(2048) not null,"));
    assert!(ledger.contains("  event_id varbinary(1024) not null,"));
    assert!(ledger.contains(
        "constraint keepsake_dovecote_bridge_identity_length check (octet_length(source) + octet_length(event_id) <= 2048)"
    ));
    Ok(())
}

#[test]
fn mysql_bridge_timestamp_schema_matches_typed_decoding_contract() {
    let legacy = include_str!("../migrations/mysql/0005_audit_outbox.sql");
    let bridge = include_str!("../migrations/mysql/0006_dovecote_bridge.sql");
    assert!(legacy.contains("claimed_until timestamp(6) null,"));
    assert!(legacy.contains("delivered_at timestamp(6) null,"));
    assert!(bridge.contains("claimed_until datetime(6) not null,"));
}

#[tokio::test]
#[ignore = "requires disposable MySQL-family server; run with MYSQL_DATABASE_URL and --ignored"]
async fn mysql_bridge_dual_write_preserves_identity_payload_and_pending_state() -> TestResult<()> {
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
        SubjectRef::new("account", "mysql-bridge")?,
        relation.id,
        ts("2026-01-01T00:00:00.123456Z")?,
        CommandContext::new(ActorRef::new("system", "mysql")?)
            .with_metadata("request", "mysql-bridge"),
    );
    bridge.apply(&command).await?;

    let row = query(
        "select o.id, o.event_type, o.payload as outbox_payload, l.source, l.stream, l.event_id, l.occurred_at, l.payload as ledger_payload, l.dovecote_row_id, d.state, e.data as event_payload, e.datacontenttype, e.occurred_at as event_occurred_at from keepsake_audit_outbox o join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'outbox' and l.legacy_id = o.id join dovecote_events e on e.row_id = l.dovecote_row_id join dovecote_deliveries d on d.event_row_id = e.row_id",
    )
    .fetch_one(&pool)
    .await?;
    let outbox_id: i64 = row.try_get("id")?;
    assert_eq!(
        String::from_utf8(row.try_get::<Vec<u8>, _>("source")?)?,
        "https://example.org/keepsake"
    );
    assert_eq!(row.try_get::<String, _>("stream")?, "keepsake-audit");
    assert_eq!(
        String::from_utf8(row.try_get::<Vec<u8>, _>("event_id")?)?,
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
    assert_eq!(row.try_get::<Vec<u8>, _>("state")?, b"pending");
    assert_eq!(
        row.try_get::<Vec<u8>, _>("datacontenttype")?,
        b"application/json"
    );
    assert_eq!(
        row.try_get::<Option<DateTime<Utc>>, _>("event_occurred_at")?,
        Some(ts("2026-01-01T00:00:00.123456Z")?)
    );
    let outbox_payload: serde_json::Value = row.try_get("outbox_payload")?;
    let ledger_payload: Vec<u8> = row.try_get("ledger_payload")?;
    let event_payload: Vec<u8> = row.try_get("event_payload")?;
    assert_eq!(ledger_payload, event_payload);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&event_payload)?,
        outbox_payload
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable MySQL-family server; run with MYSQL_DATABASE_URL and --ignored"]
#[allow(clippy::too_many_lines)]
async fn mysql_bridge_finalizer_rejects_event_and_ledger_drift() -> TestResult<()> {
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
            SubjectRef::new("account", "mysql-finalizer")?,
            relation.id,
            ts("2026-01-01T00:00:00.123456Z")?,
            CommandContext::new(ActorRef::new("system", "mysql")?),
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
        "select dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?
    .try_get("dovecote_row_id")?;
    let options = BridgeImportOptions::new(audit_id).with_outbox_high_water(outbox_id);
    assert!(bridge.import_history(&options).await?.complete);
    bridge.finalize_upgrade_reconciliation().await?;

    let audit_origin: String = query(
        "select payload_origin from keepsake_dovecote_bridge_ledger where legacy_kind = 'audit' and legacy_id = ?",
    )
    .bind(audit_only_id)
    .fetch_one(&pool)
    .await?
    .try_get("payload_origin")?;
    assert_eq!(audit_origin, "reconstructed_v1");
    query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = ? where legacy_kind = 'audit' and legacy_id = ?",
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
        "update keepsake_dovecote_bridge_ledger set payload_origin = ? where legacy_kind = 'audit' and legacy_id = ?",
    )
    .bind(&audit_origin)
    .bind(audit_only_id)
    .execute(&pool)
    .await?;

    let outbox_origin: String = query(
        "select payload_origin from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?
    .try_get("payload_origin")?;
    query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = ? where legacy_kind = 'outbox' and legacy_id = ?",
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
        "update keepsake_dovecote_bridge_ledger set payload_origin = ? where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(&outbox_origin)
    .bind(outbox_id)
    .execute(&pool)
    .await?;

    query("update dovecote_events set occurred_at = ? where row_id = ?")
        .bind(ts("2026-01-01T00:00:00.123457Z")?)
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query("update dovecote_events set occurred_at = ? where row_id = ?")
        .bind(ts("2026-01-01T00:00:00.123456Z")?)
        .bind(row_id)
        .execute(&pool)
        .await?;

    query("update dovecote_events set datacontenttype = ? where row_id = ?")
        .bind(b"text/plain".as_slice())
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query("update dovecote_events set datacontenttype = ? where row_id = ?")
        .bind(b"application/json".as_slice())
        .bind(row_id)
        .execute(&pool)
        .await?;

    let digest: String = query(
        "select payload_sha256 from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?
    .try_get("payload_sha256")?;
    query("update keepsake_dovecote_bridge_ledger set payload_sha256 = ? where legacy_kind = 'outbox' and legacy_id = ?")
        .bind("0".repeat(64))
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    query("update keepsake_dovecote_bridge_ledger set payload_sha256 = ? where legacy_kind = 'outbox' and legacy_id = ?")
        .bind(digest)
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable MySQL-family server; run with MYSQL_DATABASE_URL and --ignored"]
async fn mysql_bridge_claim_skips_unreconciled_legacy_outbox_rows() -> TestResult<()> {
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
            SubjectRef::new("account", "mysql-claim-filter")?,
            relation.id,
            ts("2026-01-01T01:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "mysql")?),
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
#[ignore = "requires disposable MySQL-family server; run with MYSQL_DATABASE_URL and --ignored"]
async fn mysql_bridge_history_fences_claims_and_preserves_delivered_as_nonpublishable()
-> TestResult<()> {
    let Some((repository, pool)) = database().await? else {
        return Ok(());
    };

    let first = audit_event(101, "2026-01-01T01:00:00Z", "reconstructed")?;
    let second = audit_event(102, "2026-01-01T02:00:00Z", "delivered")?;
    let first_id = insert_audit(&pool, &first).await?;
    let second_id = insert_audit(&pool, &second).await?;
    let outbox_id = insert_outbox(&pool, second_id, &second).await?;
    sqlx::query("update keepsake_audit_outbox set claimed_by = ?, claimed_until = ? where id = ?")
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
        "update keepsake_audit_outbox set claimed_by = null, claimed_until = null, delivered_at = ? where id = ?",
    )
    .bind(ts("2026-01-01T02:30:00.123456Z")?)
    .bind(outbox_id)
    .execute(&pool)
    .await?;
    let completed = bridge.import_history(&options).await?;
    assert_eq!((completed.examined, completed.imported), (1, 1));
    assert_eq!(completed.cursor, second_id);
    assert!(completed.complete);

    let delivered_row = query("select e.row_id from dovecote_events e where e.event_id = ?")
        .bind(format!("keepsake-outbox-{outbox_id}").as_bytes())
        .fetch_one(&pool)
        .await?;
    let delivered_row_id: i64 = delivered_row.try_get("row_id")?;
    let delivery =
        query("select d.state, d.delivered_at from dovecote_deliveries d where d.event_row_id = ?")
            .bind(delivered_row_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(delivery.try_get::<Vec<u8>, _>("state")?, b"delivered");
    assert_eq!(
        delivery.try_get::<Option<DateTime<Utc>>, _>("delivered_at")?,
        Some(ts("2026-01-01T02:30:00.123456Z")?)
    );
    let dovecote = MySqlDovecote::new(pool.clone());
    let claimed = dovecote
        .claim(
            WorkerId::new("mysql-history-worker")?,
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
#[ignore = "requires disposable MySQL-family server; run with MYSQL_DATABASE_URL and --ignored"]
async fn mysql_bridge_history_finds_a_later_old_writer_row_after_a_completed_pass() -> TestResult<()>
{
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
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events where event_id = ?",)
            .bind(format!("keepsake-outbox-{second_outbox_id}").as_bytes())
            .fetch_one(&pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable MySQL-family server; run with MYSQL_DATABASE_URL and --ignored"]
async fn mysql_bridge_ack_fences_stale_lease_and_rolls_back_on_finalize_error() -> TestResult<()> {
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
            SubjectRef::new("account", "mysql-ack")?,
            relation.id,
            ts("2026-01-01T00:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "mysql")?),
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
    sqlx::query("update keepsake_audit_outbox set claimed_until = ? where id = ?")
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
        "select dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?;
    sqlx::query("update dovecote_deliveries set attempts = 1 where event_row_id = ?")
        .bind(row_id)
        .execute(&pool)
        .await?;
    let delivered_at = ts("2026-01-01T00:01:00.123456Z")?;
    assert!(matches!(
        bridge
            .acknowledge_delivery(outbox_id, "worker-a", current.claim_token(), delivered_at)
            .await,
        Err(BridgeError::DovecoteMySqlFinalize(_))
    ));
    let legacy = query(
        "select delivered_at, claimed_by, claimed_until from keepsake_audit_outbox where id = ?",
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

async fn database() -> TestResult<Option<(MySqlKeepsakeRepository, MySqlPool)>> {
    let Ok(url) = std::env::var("MYSQL_DATABASE_URL") else {
        eprintln!("skipping Keepsake Dovecote MySQL bridge tests: MYSQL_DATABASE_URL is unset");
        return Ok(None);
    };

    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    reset(&pool).await?;
    let repository = MySqlKeepsakeRepository::new(pool.clone());
    repository.migrate().await?;
    install_dovecote(&pool).await?;
    MySqlDovecote::new(pool.clone()).check_schema().await?;
    Ok(Some((repository, pool)))
}

async fn reset(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    // `foreign_key_checks` is session-scoped. Keep the setting and every
    // destructive reset statement on one checked-out connection instead of
    // allowing pool execution to move between sessions.
    let mut connection = pool.acquire().await?;
    // MySQL rejects administrative statements sent through its prepared
    // statement protocol. `Executor::execute` on a literal or an explicitly
    // audited dynamic string uses the text protocol instead.
    (&mut *connection)
        .execute("set foreign_key_checks = 0")
        .await?;
    for statement in [
        "drop trigger if exists dovecote_events_row_id_positive_insert",
        "drop trigger if exists dovecote_events_row_id_positive_update",
        "drop table if exists dovecote_deliveries",
        "drop table if exists dovecote_events",
        "drop table if exists dovecote_schema",
        "drop table if exists keepsake_upgrade_evidence",
        "drop table if exists keepsake_dovecote_bridge_claims",
        "drop table if exists keepsake_dovecote_bridge_ledger",
        "drop table if exists keepsake_dovecote_bridge_config",
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
        (&mut *connection).execute(statement).await?;
    }
    (&mut *connection)
        .execute("set foreign_key_checks = 1")
        .await?;
    Ok(())
}

async fn install_dovecote(pool: &MySqlPool) -> TestResult<()> {
    // MySQL's text protocol needs each statement separately, and the release
    // artifact contains two triggers whose BEGIN blocks contain semicolons.
    let mut trigger = false;
    let mut buffered = String::new();
    for fragment in MIGRATIONS[0].sql().split(';') {
        let fragment = fragment.trim();
        if fragment.is_empty() {
            continue;
        }

        if fragment.to_ascii_uppercase().starts_with("CREATE TRIGGER") || trigger {
            if !buffered.is_empty() {
                buffered.push(';');
            }
            buffered.push_str(fragment);
            trigger = !fragment.to_ascii_uppercase().ends_with("END");
            if trigger {
                continue;
            }
            pool.execute(AssertSqlSafe(buffered.as_str())).await?;
            buffered.clear();
        } else {
            pool.execute(fragment).await?;
        }
    }

    Ok(())
}

fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn relation(id: u128) -> TestResult<RelationDefinition> {
    Ok(RelationDefinition::new(
        Uuid::from_u128(id),
        RelationKey::new("tag", format!("mysql-bridge-{id}"))?,
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

async fn insert_audit(pool: &MySqlPool, event: &AuditEvent) -> TestResult<i64> {
    let id = sqlx::query(
        "insert into keepsake_audit_events (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id, event_type, decision, occurred_at, recorded_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(event.keepsake_id.to_string())
    .bind(event.relation_id.to_string())
    .bind(event.subject.kind())
    .bind(event.subject.id())
    .bind(event.actor.kind())
    .bind(event.actor.id())
    .bind(event.event_type.as_str())
    .bind(serde_json::to_value(&event.decision)?)
    .bind(event.at)
    .bind(event.at)
    .execute(pool)
    .await?
    .last_insert_id();
    Ok(i64::try_from(id)?)
}

async fn insert_outbox(pool: &MySqlPool, audit_id: i64, event: &AuditEvent) -> TestResult<i64> {
    let id = sqlx::query(
        "insert into keepsake_audit_outbox (audit_event_id, event_type, payload) values (?, ?, ?)",
    )
    .bind(audit_id)
    .bind("keepsake.audit_event_recorded")
    .bind(serde_json::to_value(event)?)
    .execute(pool)
    .await?
    .last_insert_id();
    Ok(i64::try_from(id)?)
}
