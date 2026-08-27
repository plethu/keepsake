#![allow(missing_docs)]
#![allow(clippy::too_many_lines)]
#![cfg(feature = "dovecote-sqlite-tests")]

use chrono::{DateTime, SecondsFormat, Utc};
use keepsake::{
    ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent, AuditEventType,
    CommandContext, ExpiryPolicy, RelationDefinition, RelationKey, SubjectRef,
};
use keepsake_sqlx::{
    BridgeError, BridgeImportOptions, DovecoteBridgeConfig, SqliteKeepsakeRepository,
};
use sqlx::{Row, SqlitePool, raw_sql, sqlite::SqlitePoolOptions};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn dual_write_uses_outbox_identity_and_keeps_dovecote_pending() -> TestResult<()> {
    let pool = database().await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(100),
        RelationKey::new("tag", "bridge-test")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;

    // Skew the normalized audit and outbox sequences. The bridge must derive
    // identity from the captured outbox ID, not from the audit ID.
    insert_audit(
        &pool,
        &audit_event(1, "2025-12-31T23:00:00Z", "audit-only")?,
    )
    .await?;

    let bridge =
        repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let command = ApplyKeepsake::new(
        SubjectRef::new("account", "acct-1")?,
        relation.id,
        timestamp("2026-01-01T00:00:00.123456Z")?,
        CommandContext::new(ActorRef::new("system", "test")?).with_metadata("request", "bridge-1"),
    );
    bridge.apply(&command).await?;

    let audit_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_events")
        .fetch_one(&pool)
        .await?;
    let outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    assert_ne!(audit_id, outbox_id);

    let ledger = sqlx::query(
        "select source, stream, event_id, event_type, occurred_at, payload_codec, payload_origin, payload, dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?;
    let source: String = ledger.try_get("source")?;
    let stream: String = ledger.try_get("stream")?;
    let event_id: String = ledger.try_get("event_id")?;
    let ledger_event_type: String = ledger.try_get("event_type")?;
    let ledger_occurred_at: String = ledger.try_get("occurred_at")?;
    let payload_codec: String = ledger.try_get("payload_codec")?;
    let payload_origin: String = ledger.try_get("payload_origin")?;
    let payload: Vec<u8> = ledger.try_get("payload")?;
    let row_id: i64 = ledger.try_get("dovecote_row_id")?;
    assert_eq!(source, "https://example.org/keepsake");
    assert_eq!(stream, "keepsake-audit");
    assert_eq!(event_id, format!("keepsake-outbox-{outbox_id}"));
    assert_eq!(ledger_event_type, "keepsake.audit_event_recorded");
    assert_eq!(ledger_occurred_at, "2026-01-01T00:00:00.123456Z");
    assert_eq!(payload_codec, "keepsake.audit.json.v1");
    assert_eq!(payload_origin, "bridge_exact");

    let event = sqlx::query(
        "select stream, source, event_id, event_type, occurred_at, datacontenttype, data from dovecote_events where row_id = ?",
    )
    .bind(row_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(event.try_get::<String, _>("stream")?, stream);
    assert_eq!(event.try_get::<String, _>("source")?, source);
    assert_eq!(event.try_get::<String, _>("event_id")?, event_id);
    assert_eq!(
        event.try_get::<String, _>("event_type")?,
        "keepsake.audit_event_recorded"
    );
    assert_eq!(
        event.try_get::<String, _>("datacontenttype")?,
        "application/json"
    );
    assert_eq!(
        event.try_get::<Option<String>, _>("occurred_at")?,
        Some("2026-01-01T00:00:00.123456Z".to_owned())
    );
    assert_eq!(event.try_get::<Vec<u8>, _>("data")?, payload);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "select state from dovecote_deliveries where event_row_id = ?",
        )
        .bind(row_id)
        .fetch_one(&pool)
        .await?,
        "pending"
    );

    let identity = bridge
        .publisher_identity(outbox_id)
        .await?
        .ok_or_else(|| std::io::Error::other("missing persisted publisher identity"))?;
    assert_eq!(identity.source(), source);
    assert_eq!(identity.event_id(), event_id);
    assert_eq!(identity.payload(), payload.as_slice());

    let first_pass = bridge
        .import_history(&BridgeImportOptions::new(audit_id).with_outbox_high_water(outbox_id))
        .await?;
    assert!(first_pass.complete);
    bridge.finalize_upgrade_reconciliation().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_upgrade_evidence")
            .fetch_one(&pool)
            .await?,
        1
    );

    // An audit-only row is reconstructed through the project codec. It must
    // never be relabelled as an outbox or bridge payload in activation
    // evidence.
    let audit_origin: String = sqlx::query_scalar(
        "select payload_origin from keepsake_dovecote_bridge_ledger where legacy_kind = 'audit'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(audit_origin, "reconstructed_v1");
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = 'bridge_exact' where legacy_kind = 'audit'",
    )
    .execute(&pool)
    .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = ? where legacy_kind = 'audit'",
    )
    .bind(&audit_origin)
    .execute(&pool)
    .await?;

    // A historical SQLite outbox row retains its validated TEXT bytes. It
    // must not be relabelled as a reconstruction.
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = 'reconstructed_v1' where legacy_kind = 'outbox'",
    )
    .execute(&pool)
    .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set payload_origin = ? where legacy_kind = 'outbox'",
    )
    .bind(&payload_origin)
    .execute(&pool)
    .await?;

    sqlx::query("update dovecote_events set occurred_at = ? where row_id = ?")
        .bind("2026-01-01T00:00:00.123457Z")
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    sqlx::query("update dovecote_events set occurred_at = ? where row_id = ?")
        .bind("2026-01-01T00:00:00.123456Z")
        .bind(row_id)
        .execute(&pool)
        .await?;

    sqlx::query("update dovecote_events set datacontenttype = ? where row_id = ?")
        .bind("text/plain")
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    sqlx::query("update dovecote_events set datacontenttype = ? where row_id = ?")
        .bind("application/json")
        .bind(row_id)
        .execute(&pool)
        .await?;

    let digest: String = sqlx::query_scalar(
        "select payload_sha256 from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?;
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set payload_sha256 = ? where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind("0".repeat(64))
    .bind(outbox_id)
    .execute(&pool)
    .await?;
    assert!(matches!(
        bridge.finalize_upgrade_reconciliation().await,
        Err(BridgeError::Reconciliation { digest_delta, .. }) if digest_delta > 0
    ));
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set payload_sha256 = ? where legacy_kind = 'outbox' and legacy_id = ?",
    )
    .bind(digest)
    .bind(outbox_id)
    .execute(&pool)
    .await?;

    // The ledger is authoritative once dual-write has established the
    // identity. A later JSON representation change in the legacy outbox must
    // not alter the exact Dovecote bytes or create a conflicting event.
    sqlx::query("update keepsake_audit_outbox set payload = ? where id = ?")
        .bind(r#"{"different":true}"#)
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    let rerun = bridge
        .import_history(&BridgeImportOptions::new(audit_id).with_outbox_high_water(outbox_id))
        .await?;
    assert_eq!((rerun.imported, rerun.already_imported), (0, 0));
    let rerun_payload: Vec<u8> =
        sqlx::query_scalar("select data from dovecote_events where row_id = ?")
            .bind(row_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(rerun_payload, payload);
    let finalization_error = bridge
        .finalize_upgrade_reconciliation()
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("source drift must reject finalization"))?;
    assert!(match finalization_error {
        BridgeError::Reconciliation { digest_delta, .. } => digest_delta > 0,
        BridgeError::Reconstruction { .. } => true,
        _ => false,
    });
    Ok(())
}

#[tokio::test]
async fn bridge_claim_skips_unreconciled_legacy_outbox_rows() -> TestResult<()> {
    let pool = database().await?;
    let old_event = audit_event(120, "2026-01-01T00:30:00Z", "legacy-only")?;
    let old_audit_id = insert_audit(&pool, &old_event).await?;
    let old_outbox_id = insert_outbox(&pool, old_audit_id, &old_event).await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(121),
        RelationKey::new("tag", "claim-filter")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    bridge
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "sqlite-claim-filter")?,
            relation.id,
            timestamp("2026-01-01T01:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "sqlite")?),
        ))
        .await?;
    let bridged_outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    assert!(old_outbox_id < bridged_outbox_id);
    let claims = bridge
        .claim_delivery(
            "bridge-worker",
            timestamp("2026-01-01T00:00:00Z")?,
            timestamp("2037-01-01T00:00:00Z")?,
            10,
        )
        .await?;
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].record().id, bridged_outbox_id);
    Ok(())
}

#[tokio::test]
async fn history_import_fences_claims_reconstructs_context_and_preserves_delivery() -> TestResult<()>
{
    let pool = database().await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let pre_outbox = audit_event(11, "2026-01-01T01:00:00Z", "pre-outbox")?;
    let outbox_event = audit_event(22, "2026-01-01T02:00:00Z", "outbox")?;
    let first_audit_id = insert_audit(&pool, &pre_outbox).await?;
    let second_audit_id = insert_audit(&pool, &outbox_event).await?;
    let outbox_id = insert_outbox(&pool, second_audit_id, &outbox_event).await?;
    assert_eq!((first_audit_id, second_audit_id, outbox_id), (1, 2, 1));
    sqlx::query("update keepsake_audit_outbox set claimed_by = ?, claimed_until = ? where id = ?")
        .bind("legacy-worker")
        .bind("2037-01-01T00:10:00.000000Z")
        .bind(outbox_id)
        .execute(&pool)
        .await?;

    let bridge =
        repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let options = BridgeImportOptions::new(second_audit_id).with_batch_size(2);
    let blocked = bridge.import_history(&options).await?;
    assert_eq!(
        (blocked.examined, blocked.imported, blocked.blocked),
        (2, 1, 1)
    );
    assert_eq!(blocked.cursor, first_audit_id);
    assert!(!blocked.complete);

    sqlx::query(
        "update keepsake_audit_outbox set claimed_by = null, claimed_until = null, delivered_at = ? where id = ?",
    )
    .bind("2026-01-01T02:30:00.123456Z")
    .bind(outbox_id)
    .execute(&pool)
    .await?;
    let completed = bridge.import_history(&options).await?;
    assert_eq!((completed.examined, completed.imported), (1, 1));
    assert_eq!(completed.cursor, second_audit_id);
    assert!(completed.complete);

    let replay = bridge.import_history(&options).await?;
    assert_eq!(
        (replay.examined, replay.imported, replay.already_imported),
        (0, 0, 0)
    );
    assert!(replay.complete);

    let pre_row = sqlx::query(
        "select e.data from dovecote_events e where e.event_id = 'keepsake-audit-legacy-1'",
    )
    .fetch_one(&pool)
    .await?;
    let pre_payload: Vec<u8> = pre_row.try_get("data")?;
    let pre_imported: AuditEvent = serde_json::from_slice(&pre_payload)?;
    assert_eq!(
        pre_imported.context.attributes.get("history"),
        Some(&"pre-outbox".to_owned())
    );

    let delivery = sqlx::query(
        "select d.state, d.delivered_at from dovecote_deliveries d join dovecote_events e on e.row_id = d.event_row_id where e.event_id = ?",
    )
    .bind(format!("keepsake-outbox-{outbox_id}"))
    .fetch_one(&pool)
    .await?;
    assert_eq!(delivery.try_get::<String, _>("state")?, "delivered");
    assert_eq!(
        delivery.try_get::<String, _>("delivered_at")?,
        "2026-01-01T02:30:00.123456Z"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_dovecote_bridge_ledger")
            .fetch_one(&pool)
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from keepsake_dovecote_bridge_ledger where payload_origin = 'reconstructed_v1'",
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from keepsake_dovecote_bridge_ledger where payload_origin = 'legacy_outbox_exact_text'",
        )
        .fetch_one(&pool)
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn history_import_keeps_duplicate_outboxes_across_batch_boundaries() -> TestResult<()> {
    let pool = database().await?;
    let event = audit_event(333, "2026-01-01T03:00:00Z", "duplicate-outboxes")?;
    let audit_id = insert_audit(&pool, &event).await?;
    insert_outbox(&pool, audit_id, &event).await?;
    let second_outbox = insert_outbox(&pool, audit_id, &event).await?;
    let bridge = SqliteKeepsakeRepository::new(pool.clone())
        .with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let options = BridgeImportOptions::new(audit_id)
        .with_outbox_high_water(second_outbox)
        .with_batch_size(1);

    let first = bridge.import_history(&options).await?;
    assert_eq!(
        (first.imported, first.audit_cursor, first.outbox_cursor),
        (2, audit_id, second_outbox)
    );
    assert!(first.complete);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox'"
        )
        .fetch_one(&pool)
        .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn publisher_identity_reports_persisted_configuration_drift() -> TestResult<()> {
    let pool = database().await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(200),
        RelationKey::new("tag", "drift-test")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge = repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/one")?);
    let command = ApplyKeepsake::new(
        SubjectRef::new("account", "acct-drift")?,
        relation.id,
        timestamp("2026-01-01T00:00:00Z")?,
        CommandContext::new(ActorRef::new("system", "test")?),
    );
    bridge.apply(&command).await?;
    let outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    let drifted = bridge.with_config(DovecoteBridgeConfig::new("https://example.org/two")?);
    assert!(matches!(
        drifted.publisher_identity(outbox_id).await,
        Err(BridgeError::ConfigurationConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn acknowledge_delivery_checks_owner_and_is_exactly_idempotent() -> TestResult<()> {
    let pool = database().await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(210),
        RelationKey::new("tag", "ack-test")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    bridge
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "ack")?,
            relation.id,
            timestamp("2026-01-01T00:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "test")?),
        ))
        .await?;
    let outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    let delivered_at = timestamp("2026-01-01T00:01:00.123456Z")?;
    let active_lease = timestamp("2037-01-01T00:00:00.000000Z")?;
    let stale = bridge
        .claim_delivery(
            "worker-a",
            timestamp("2026-01-01T00:00:00Z")?,
            active_lease,
            1,
        )
        .await?
        .into_iter()
        .next()
        .ok_or("bridge claim did not return the outbox row")?;

    assert!(matches!(
        bridge
            .acknowledge_delivery(
                outbox_id + 100,
                "worker-a",
                stale.claim_token(),
                delivered_at
            )
            .await,
        Err(BridgeError::DeliveryOwnership { outbox_id: id }) if id == outbox_id + 100
    ));

    assert!(matches!(
        bridge
            .acknowledge_delivery(outbox_id, "worker-b", stale.claim_token(), delivered_at)
            .await,
        Err(BridgeError::DeliveryOwnership { outbox_id: id }) if id == outbox_id
    ));

    sqlx::query("update keepsake_audit_outbox set claimed_until = ? where id = ?")
        .bind(
            timestamp("2000-01-01T00:00:00.000000Z")?.to_rfc3339_opts(SecondsFormat::Micros, true),
        )
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge
            .acknowledge_delivery(outbox_id, "worker-a", stale.claim_token(), delivered_at)
            .await,
        Err(BridgeError::DeliveryOwnership { outbox_id: id }) if id == outbox_id
    ));

    let current = bridge
        .claim_delivery(
            "worker-a",
            timestamp("2026-01-01T00:00:00Z")?,
            active_lease,
            1,
        )
        .await?
        .into_iter()
        .next()
        .ok_or("bridge reclaim did not return the outbox row")?;
    assert_ne!(stale.claim_token(), current.claim_token());
    assert!(matches!(
        bridge
            .acknowledge_delivery(outbox_id, "worker-a", stale.claim_token(), delivered_at)
            .await,
        Err(BridgeError::DeliveryOwnership { outbox_id: id }) if id == outbox_id
    ));
    bridge
        .acknowledge_delivery(outbox_id, "worker-a", current.claim_token(), delivered_at)
        .await?;
    // A retry after the legacy acknowledgement has cleared the lease remains
    // an exact no-op, while a different timestamp is a durable conflict.
    bridge
        .acknowledge_delivery(outbox_id, "worker-a", current.claim_token(), delivered_at)
        .await?;
    assert!(matches!(
        bridge
            .acknowledge_delivery(
                outbox_id,
                "worker-a",
                current.claim_token(),
                timestamp("2026-01-01T00:01:01.123456Z")?
            )
            .await,
        Err(BridgeError::DeliveryConflict { outbox_id: id }) if id == outbox_id
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "select state from dovecote_deliveries d join keepsake_dovecote_bridge_ledger l on l.dovecote_row_id = d.event_row_id where l.legacy_id = ?",
        )
        .bind(outbox_id)
        .fetch_one(&pool)
        .await?,
        "delivered"
    );
    Ok(())
}

#[tokio::test]
async fn acknowledge_delivery_rolls_back_legacy_ack_when_dovecote_finalization_fails()
-> TestResult<()> {
    let pool = database().await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(220),
        RelationKey::new("tag", "ack-rollback")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    let bridge =
        repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    bridge
        .apply(&ApplyKeepsake::new(
            SubjectRef::new("account", "ack-rollback")?,
            relation.id,
            timestamp("2026-01-01T00:00:00Z")?,
            CommandContext::new(ActorRef::new("system", "test")?),
        ))
        .await?;
    let outbox_id: i64 = sqlx::query_scalar("select max(id) from keepsake_audit_outbox")
        .fetch_one(&pool)
        .await?;
    let active_lease = timestamp("2037-01-01T00:00:00.000000Z")?;
    let claim = bridge
        .claim_delivery(
            "worker-a",
            timestamp("2026-01-01T00:00:00Z")?,
            active_lease,
            1,
        )
        .await?
        .into_iter()
        .next()
        .ok_or("bridge claim did not return the outbox row")?;
    let row_id: i64 = sqlx::query_scalar(
        "select dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_id = ?",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await?;
    sqlx::query("update keepsake_audit_outbox set claimed_by = ?, claimed_until = ? where id = ?")
        .bind("worker-a")
        .bind(active_lease.to_rfc3339_opts(SecondsFormat::Micros, true))
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    sqlx::query("update dovecote_deliveries set attempts = 1 where event_row_id = ?")
        .bind(row_id)
        .execute(&pool)
        .await?;
    assert!(matches!(
        bridge
            .acknowledge_delivery(
                outbox_id,
                "worker-a",
                claim.claim_token(),
                timestamp("2026-01-01T00:01:00.123456Z")?
            )
            .await,
        Err(BridgeError::DovecoteSqliteFinalize(_))
    ));
    let legacy =
        sqlx::query("select delivered_at, claimed_by from keepsake_audit_outbox where id = ?")
            .bind(outbox_id)
            .fetch_one(&pool)
            .await?;
    assert!(
        legacy
            .try_get::<Option<String>, _>("delivered_at")?
            .is_none()
    );
    assert_eq!(
        legacy.try_get::<Option<String>, _>("claimed_by")?,
        Some("worker-a".to_owned())
    );
    Ok(())
}

#[tokio::test]
async fn history_import_advances_to_a_successive_high_water() -> TestResult<()> {
    let pool = database().await?;
    let first = insert_audit(&pool, &audit_event(51, "2026-01-01T01:00:00Z", "first")?).await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let bridge =
        repo.with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let first_pass = bridge
        .import_history(&BridgeImportOptions::new(first))
        .await?;
    assert!(first_pass.complete);
    let second = insert_audit(&pool, &audit_event(52, "2026-01-01T02:00:00Z", "second")?).await?;
    let second_pass = bridge
        .import_history(&BridgeImportOptions::new(second))
        .await?;
    assert_eq!((second_pass.imported, second_pass.cursor), (1, second));
    assert!(second_pass.complete);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn disabled_bridge_preserves_legacy_only_default_behavior() -> TestResult<()> {
    let pool = database().await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(230),
        RelationKey::new("tag", "legacy-only")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, timestamp("2026-01-01T00:00:00Z")?)
        .await?;
    repo.apply(&ApplyKeepsake::new(
        SubjectRef::new("account", "legacy-only")?,
        relation.id,
        timestamp("2026-01-01T00:00:00Z")?,
        CommandContext::new(ActorRef::new("system", "test")?),
    ))
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from dovecote_events")
            .fetch_one(&pool)
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_audit_outbox")
            .fetch_one(&pool)
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn history_import_rejects_duplicate_cutover_identity() -> TestResult<()> {
    let pool = database().await?;
    let first = audit_event(31, "2026-01-01T01:00:00Z", "first")?;
    let second = audit_event(32, "2026-01-01T02:00:00Z", "second")?;
    let first_id = insert_audit(&pool, &first).await?;
    let second_id = insert_audit(&pool, &second).await?;
    let outbox_id = insert_outbox(&pool, second_id, &second).await?;
    sqlx::query("update keepsake_audit_outbox set claimed_by = ?, claimed_until = ? where id = ?")
        .bind("legacy-worker")
        .bind("2037-01-01T00:10:00.000000Z")
        .bind(outbox_id)
        .execute(&pool)
        .await?;
    let bridge = SqliteKeepsakeRepository::new(pool.clone())
        .with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let options = BridgeImportOptions::new(second_id);
    let blocked = bridge.import_history(&options).await?;
    assert_eq!(blocked.cursor, first_id);
    sqlx::query(
        "update keepsake_dovecote_bridge_ledger set event_id = ? where legacy_kind = 'audit' and legacy_id = ?",
    )
    .bind(format!("keepsake-outbox-{outbox_id}"))
    .bind(first_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "update keepsake_audit_outbox set claimed_by = null, claimed_until = null where id = ?",
    )
    .bind(outbox_id)
    .execute(&pool)
    .await?;
    let Err(error) = bridge.import_history(&options).await else {
        return Err("identity conflict was not reported".into());
    };
    assert!(matches!(error, BridgeError::IdentityConflict { .. }));
    Ok(())
}

#[tokio::test]
async fn history_import_rolls_back_a_partial_batch_on_reconstruction_error() -> TestResult<()> {
    let pool = database().await?;
    let first = audit_event(41, "2026-01-01T01:00:00Z", "first")?;
    let second = audit_event(42, "2026-01-01T02:00:00Z", "second")?;
    insert_audit(&pool, &first).await?;
    let second_id = insert_audit(&pool, &second).await?;
    sqlx::query("update keepsake_audit_events set decision = ? where id = ?")
        .bind(r#"{"invalid":true}"#)
        .bind(second_id)
        .execute(&pool)
        .await?;
    let bridge = SqliteKeepsakeRepository::new(pool.clone())
        .with_dovecote_bridge(DovecoteBridgeConfig::new("https://example.org/keepsake")?);
    let options = BridgeImportOptions::new(second_id).with_batch_size(2);
    let Err(error) = bridge.import_history(&options).await else {
        return Err("reconstruction error was not reported".into());
    };
    assert!(matches!(error, BridgeError::Reconstruction { audit_id, .. } if audit_id == second_id));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from keepsake_dovecote_bridge_ledger")
            .fetch_one(&pool)
            .await?,
        0
    );
    sqlx::query("update keepsake_audit_events set decision = ? where id = ?")
        .bind(serde_json::to_string(&first.decision)?)
        .bind(second_id)
        .execute(&pool)
        .await?;
    let completed = bridge.import_history(&options).await?;
    assert_eq!((completed.imported, completed.cursor), (2, second_id));
    assert!(completed.complete);
    Ok(())
}

async fn database() -> TestResult<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let repo = SqliteKeepsakeRepository::new(pool.clone());
    repo.migrate().await?;
    raw_sql(include_str!(
        "../../../../carrier/crates/dovecote-sqlx-sqlite/migrations/0001_dovecote.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(pool)
}

fn timestamp(value: &str) -> TestResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn audit_event(id: u128, at: &str, history: &str) -> TestResult<AuditEvent> {
    let mut context = AuditContext::default();
    context
        .attributes
        .insert("history".to_owned(), history.to_owned());
    Ok(AuditEvent {
        event_type: AuditEventType::Apply,
        at: timestamp(at)?,
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

async fn insert_audit(pool: &SqlitePool, event: &AuditEvent) -> TestResult<i64> {
    let result = sqlx::query(
        "insert into keepsake_audit_events (keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id, event_type, decision, occurred_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(event.keepsake_id.to_string())
    .bind(event.relation_id.to_string())
    .bind(event.subject.kind())
    .bind(event.subject.id())
    .bind(event.actor.kind())
    .bind(event.actor.id())
    .bind(event.event_type.as_str())
    .bind(serde_json::to_string(&event.decision)?)
    .bind(event.at.to_rfc3339_opts(SecondsFormat::Micros, true))
    .execute(pool)
    .await?;
    let audit_id = result.last_insert_rowid();
    for (key, value) in &event.context.attributes {
        sqlx::query(
            "insert into keepsake_audit_context_attributes (audit_event_id, key, value) values (?, ?, ?)",
        )
        .bind(audit_id)
        .bind(key)
        .bind(value)
        .execute(pool)
        .await?;
    }
    Ok(audit_id)
}

async fn insert_outbox(pool: &SqlitePool, audit_id: i64, event: &AuditEvent) -> TestResult<i64> {
    let result = sqlx::query(
        "insert into keepsake_audit_outbox (audit_event_id, event_type, payload) values (?, ?, ?)",
    )
    .bind(audit_id)
    .bind("keepsake.audit_event_recorded")
    .bind(serde_json::to_string(event)?)
    .execute(pool)
    .await?;
    Ok(result.last_insert_rowid())
}
