//! `SQLite` implementation of the opt-in Keepsake/Dovecote bridge.

#![allow(
    clippy::excessive_nesting,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

use chrono::{DateTime, Utc};
use dovecote::{FinalizeOutcome, RowId};
use keepsake::{ApplyKeepsake, AuditEvent, RevokeBySubject, RevokeKeepsake};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use super::dovecote_bridge::{
    BridgeClaimToken, BridgeDeliveryClaim, BridgeError, BridgeImportOptions, BridgeImportReport,
    BridgePublisherIdentity, DovecoteBridgeConfig, DovecoteBridgeRepository, LEGACY_EVENT_TYPE,
    LegacyAuditEventV1, PAYLOAD_CODEC, PAYLOAD_ORIGIN_BRIDGE_EXACT,
    PAYLOAD_ORIGIN_LEGACY_OUTBOX_EXACT_TEXT, PAYLOAD_ORIGIN_RECONSTRUCTED_V1, audit_event_id,
    encode_payload, import_outcome, imported_delivery_state, outbox_event_id, payload_digest,
    payload_origin_matches, reconstruct_audit_event_v1, to_dovecote_event,
};
use super::sqlite::audit::record_audit_event_and_outbox_tx;
use super::sqlite::lifecycle::{
    active_keepsake_for_subject_relation_tx, keepsake_by_id_tx, relation_for_update_tx,
    revoke_by_subject_tx, revoke_tx,
};
use super::support::expires_at;
use super::{AppliedKeepsake, AuditOutboxRecord, RelationCache, RepositoryError, SqliteBackend};

impl<C> DovecoteBridgeRepository<SqliteBackend, C>
where
    C: RelationCache,
{
    /// Claims legacy outbox rows and records an opaque bridge generation for
    /// each claim. Use this instead of `claim_audit_outbox` while the bridge is
    /// enabled; the returned generation is required by `acknowledge_delivery`.
    pub async fn claim_delivery(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<BridgeDeliveryClaim>, BridgeError> {
        if !(1..=10_000).contains(&limit) {
            return Err(BridgeError::InvalidLimit {
                field: "limit",
                value: limit,
                max: 10_000,
            });
        }

        let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
        ensure_config_tx(&mut tx, &self.config).await?;
        let ids = sqlx::query_scalar::<_, i64>(
            "select o.id from keepsake_audit_outbox o join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'outbox' and l.legacy_id = o.id where o.delivered_at is null and (o.claimed_until is null or o.claimed_until <= ?1) order by o.id limit ?2",
        )
        .bind(super::sqlite::rows::format_timestamp(now))
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let mut claims = Vec::with_capacity(ids.len());
        for outbox_id in ids {
            sqlx::query(
                "update keepsake_audit_outbox set claimed_by = ?1, claimed_until = ?2 where id = ?3 and delivered_at is null",
            )
            .bind(worker_id)
            .bind(super::sqlite::rows::format_timestamp(lease_until))
            .bind(outbox_id)
            .execute(&mut *tx)
            .await?;
            let token = BridgeClaimToken::fresh();
            sqlx::query(
                "insert into keepsake_dovecote_bridge_claims (outbox_id, claim_token, claimed_by, claimed_until, updated_at) values (?1, ?2, ?3, ?4, ?5) on conflict(outbox_id) do update set claim_token = excluded.claim_token, claimed_by = excluded.claimed_by, claimed_until = excluded.claimed_until, updated_at = excluded.updated_at",
            )
            .bind(outbox_id)
            .bind(token.as_bytes().as_slice())
            .bind(worker_id)
            .bind(super::sqlite::rows::format_timestamp(lease_until))
            .bind(super::sqlite::rows::format_timestamp(Utc::now()))
            .execute(&mut *tx)
            .await?;
            let row = sqlx::query(
                "select id, audit_event_id, event_type, payload, claimed_by, claimed_until, delivered_at from keepsake_audit_outbox where id = ?1",
            )
            .bind(outbox_id)
            .fetch_one(&mut *tx)
            .await?;
            claims.push(BridgeDeliveryClaim::new(
                outbox_record_from_sqlite_row(&row)?,
                token,
            ));
        }
        tx.commit().await?;
        Ok(claims)
    }

    /// Acknowledges a legacy outbox row and finalizes its pending Dovecote
    /// delivery in the same transaction. This never invokes a publisher.
    ///
    /// `claim_token` is the opaque generation returned by `claim_delivery`; a
    /// same-worker stale lease cannot acknowledge a row after it has been
    /// reclaimed, even when its expiry is identical.
    pub async fn acknowledge_delivery(
        &self,
        outbox_id: i64,
        worker: &str,
        claim_token: &BridgeClaimToken,
        delivered_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), BridgeError> {
        let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
        ensure_config_tx(&mut tx, &self.config).await?;
        let row_id: i64 = sqlx::query_scalar("select dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?1")
            .bind(outbox_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(BridgeError::DeliveryOwnership { outbox_id })?;
        let legacy = sqlx::query("select delivered_at, claimed_by, claimed_until from keepsake_audit_outbox where id = ?1")
            .bind(outbox_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(BridgeError::DeliveryOwnership { outbox_id })?;
        let generation = sqlx::query("select claim_token, claimed_by, claimed_until from keepsake_dovecote_bridge_claims where outbox_id = ?1")
            .bind(outbox_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(BridgeError::DeliveryOwnership { outbox_id })?;
        let stored_token: Vec<u8> = generation.try_get("claim_token")?;
        let stored_token = BridgeClaimToken::from_bytes(stored_token)?;
        let generation_worker: String = generation.try_get("claimed_by")?;
        if stored_token != *claim_token || generation_worker != worker {
            return Err(BridgeError::DeliveryOwnership { outbox_id });
        }

        let existing_delivered_at: Option<String> = legacy.try_get("delivered_at")?;
        if let Some(existing) = existing_delivered_at {
            if super::sqlite::rows::parse_timestamp(&existing)? != delivered_at {
                return Err(BridgeError::DeliveryConflict { outbox_id });
            }
        } else {
            let claimed_by: Option<String> = legacy.try_get("claimed_by")?;
            let stored_claimed_until: Option<String> = legacy.try_get("claimed_until")?;
            let generation_until: String = generation.try_get("claimed_until")?;
            if stored_claimed_until.as_deref() != Some(generation_until.as_str())
                || claimed_by.as_deref() != Some(worker)
            {
                return Err(BridgeError::DeliveryOwnership { outbox_id });
            }

            let result = sqlx::query("update keepsake_audit_outbox set delivered_at = ?1, claimed_by = null, claimed_until = null where id = ?2 and delivered_at is null and claimed_by = ?3 and claimed_until = ?4 and claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                .bind(super::sqlite::rows::format_timestamp(delivered_at))
                .bind(outbox_id)
                .bind(worker)
                .bind(generation_until)
                .execute(&mut *tx)
                .await?;
            if result.rows_affected() != 1 {
                return Err(BridgeError::DeliveryOwnership { outbox_id });
            }
        }

        let nanos = delivered_at
            .timestamp_nanos_opt()
            .ok_or_else(|| BridgeError::Dovecote {
                detail: "delivery timestamp is outside the Dovecote range".to_owned(),
            })?;
        let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(nanos))?;
        let outcome = dovecote_sqlx_sqlite::finalize_pending_delivery_for_migration(
            &mut tx,
            RowId::new(row_id)?,
            timestamp,
        )
        .await?;
        if !matches!(
            outcome,
            FinalizeOutcome::Finalized { .. } | FinalizeOutcome::AlreadyFinalized { .. }
        ) {
            return Err(BridgeError::Dovecote {
                detail: "unsupported Dovecote finalization outcome".to_owned(),
            });
        }
        tx.commit().await?;
        Ok(())
    }

    /// Returns the identity persisted for a dual-written legacy outbox row.
    pub async fn publisher_identity(
        &self,
        outbox_id: i64,
    ) -> Result<Option<BridgePublisherIdentity>, BridgeError> {
        let config_row = sqlx::query(
            "select source, stream, payload_codec from keepsake_dovecote_bridge_config where id = 1",
        )
        .fetch_optional(&self.repository.pool)
        .await?;
        if let Some(config_row) = config_row
            && (config_row.try_get::<String, _>("source")? != self.config.source().as_str()
                || config_row.try_get::<String, _>("stream")? != self.config.stream().as_str()
                || config_row.try_get::<String, _>("payload_codec")? != PAYLOAD_CODEC)
        {
            return Err(BridgeError::ConfigurationConflict);
        }

        let row = sqlx::query("select source, stream, event_id, event_type, occurred_at, payload_codec, payload from keepsake_dovecote_bridge_ledger where legacy_kind = 'outbox' and legacy_id = ?1")
            .bind(outbox_id)
            .fetch_optional(&self.repository.pool)
            .await?;
        row.map(|row| {
            if row.try_get::<String, _>("source")? != self.config.source().as_str()
                || row.try_get::<String, _>("stream")? != self.config.stream().as_str()
                || row.try_get::<String, _>("payload_codec")? != PAYLOAD_CODEC
            {
                return Err(BridgeError::ConfigurationConflict);
            }
            Ok(BridgePublisherIdentity::from_parts(
                row.try_get("source")?,
                row.try_get("event_type")?,
                row.try_get("event_id")?,
                super::sqlite::rows::parse_timestamp(row.try_get("occurred_at")?).map_err(
                    |error| BridgeError::Dovecote {
                        detail: error.to_string(),
                    },
                )?,
                row.try_get("payload")?,
            ))
        })
        .transpose()
    }

    /// Applies a command and imports its pending event atomically using
    /// `SQLite`'s single-writer transaction.
    pub async fn apply(&self, command: &ApplyKeepsake) -> Result<AppliedKeepsake, BridgeError> {
        command.subject.validate()?;
        command.context.validate()?;
        let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
        ensure_config_tx(&mut tx, &self.config).await?;
        let relation = relation_for_update_tx(&mut tx, command.relation_id).await?;
        let (keepsake, duplicate_prevented) = if let Some(existing) =
            active_keepsake_for_subject_relation_tx(&mut tx, &command.subject, command.relation_id)
                .await?
        {
            (existing, true)
        } else {
            if !relation.enabled {
                return Err(RepositoryError::RelationDisabled {
                    relation_id: command.relation_id,
                }
                .into());
            }

            let result = sqlx::query(
                "insert into keepsakes (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, metadata, created_at, updated_at) values (?1, ?2, ?3, ?4, 'applied', ?5, ?6, ?7, ?8, ?6, ?6) on conflict (subject_kind, subject_id, relation_id) where state = 'applied' do nothing",
            )
            .bind(command.id.to_string())
            .bind(command.subject.kind())
            .bind(command.subject.id())
            .bind(command.relation_id.to_string())
            .bind(serde_json::to_string(&relation.expiry)?)
            .bind(super::sqlite::rows::format_timestamp(command.at))
            .bind(expires_at(&relation.expiry).map(super::sqlite::rows::format_timestamp))
            .bind(serde_json::to_string(&command.metadata)?)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 0 {
                (
                    active_keepsake_for_subject_relation_tx(
                        &mut tx,
                        &command.subject,
                        command.relation_id,
                    )
                    .await?
                    .ok_or(RepositoryError::RelationDefinitionMissing {
                        relation_id: command.relation_id,
                    })?,
                    true,
                )
            } else {
                (
                    keepsake_by_id_tx(&mut tx, command.id).await?.ok_or(
                        RepositoryError::RelationDefinitionMissing {
                            relation_id: command.relation_id,
                        },
                    )?,
                    false,
                )
            }
        };

        let audit_event = super::support::apply_event(command, &keepsake, duplicate_prevented);
        let (_, outbox_id) = record_audit_event_and_outbox_tx(&mut tx, &audit_event).await?;
        let payload = encode_payload(&audit_event)?;
        let event = to_dovecote_event(
            &self.config,
            outbox_event_id(outbox_id),
            LEGACY_EVENT_TYPE,
            payload.clone(),
            Some(audit_event.at),
        )?;
        let outcome = dovecote_sqlx_sqlite::import_for_migration(
            &mut tx,
            event,
            dovecote::ImportedDeliveryState::pending(),
        )
        .await?;
        let (row_id, _) = import_outcome(&outcome)?;
        record_ledger_tx(
            &mut tx,
            "outbox",
            outbox_id,
            &self.config,
            &outbox_event_id(outbox_id),
            LEGACY_EVENT_TYPE,
            audit_event.at,
            PAYLOAD_ORIGIN_BRIDGE_EXACT,
            payload_digest(&payload),
            &payload,
            row_id,
        )
        .await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented,
        })
    }

    /// Revokes a command and dual-writes its pending event atomically.
    pub async fn revoke(&self, command: &RevokeKeepsake) -> Result<bool, BridgeError> {
        command.context.validate()?;
        let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
        ensure_config_tx(&mut tx, &self.config).await?;
        let Some(keepsake) = revoke_tx(&mut tx, command.keepsake_id, command.at).await? else {
            tx.commit().await?;
            return Ok(false);
        };

        let audit_event = super::support::revoke_event(command, &keepsake);
        let (_, outbox_id) = record_audit_event_and_outbox_tx(&mut tx, &audit_event).await?;
        import_new_audit_event(&mut tx, &self.config, outbox_id, &audit_event).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Revokes by subject and dual-writes its pending event atomically.
    pub async fn revoke_by_subject(
        &self,
        command: &RevokeBySubject,
    ) -> Result<Option<Uuid>, BridgeError> {
        command.subject.validate()?;
        command.context.validate()?;
        let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
        ensure_config_tx(&mut tx, &self.config).await?;
        let revoked =
            revoke_by_subject_tx(&mut tx, &command.subject, command.relation_id, command.at)
                .await?;
        let Some(keepsake) = revoked else {
            tx.commit().await?;
            return Ok(None);
        };

        let id = keepsake.id();
        let audit_event = super::support::revoke_by_subject_event(command, &keepsake);
        let (_, outbox_id) = record_audit_event_and_outbox_tx(&mut tx, &audit_event).await?;
        import_new_audit_event(&mut tx, &self.config, outbox_id, &audit_event).await?;
        tx.commit().await?;
        Ok(Some(id))
    }

    /// Imports normalized history in bounded resumable writer transactions.
    pub async fn import_history(
        &self,
        options: &BridgeImportOptions,
    ) -> Result<BridgeImportReport, BridgeError> {
        validate_options(options)?;
        let mut report = BridgeImportReport {
            audit_high_water: options.audit_high_water(),
            outbox_high_water: options.outbox_high_water(),
            ..BridgeImportReport::default()
        };
        loop {
            let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
            let (audit_cursor, outbox_cursor) =
                ensure_progress_tx(&mut tx, &self.config, options).await?;
            let rows = sqlx::query(HISTORY_SQL)
                .bind(outbox_cursor)
                .bind(options.outbox_high_water())
                .bind(audit_cursor)
                .bind(options.audit_high_water())
                .bind(options.batch_size())
                .fetch_all(&mut *tx)
                .await?;
            if rows.is_empty() {
                mark_complete_tx(
                    &mut tx,
                    options.audit_high_water(),
                    options.outbox_high_water(),
                )
                .await?;
                tx.commit().await?;
                report.cursor = options.audit_high_water();
                report.audit_cursor = options.audit_high_water();
                report.outbox_cursor = options.outbox_high_water();
                report.complete = true;
                return Ok(report);
            }

            let mut blocked = false;
            for row in rows {
                let audit_id: i64 = row.try_get("audit_id")?;
                let sequence_kind: String = row.try_get("sequence_kind")?;
                let sequence_id: i64 = row.try_get("sequence_id")?;
                report.examined += 1;
                let _claimed_until = row
                    .try_get::<Option<String>, _>("claimed_until")?
                    .map(|value| super::sqlite::rows::parse_timestamp(&value))
                    .transpose()
                    .map_err(|error| BridgeError::Dovecote {
                        detail: error.to_string(),
                    })?;
                let delivered_at = row
                    .try_get::<Option<String>, _>("delivered_at")?
                    .map(|value| super::sqlite::rows::parse_timestamp(&value))
                    .transpose()
                    .map_err(|error| BridgeError::Dovecote {
                        detail: error.to_string(),
                    })?;
                let active_claim = if let Some(outbox_id) =
                    row.try_get::<Option<i64>, _>("outbox_id")?
                {
                    sqlx::query("update keepsake_audit_outbox set claimed_by = null, claimed_until = null where id = ?1 and delivered_at is null and claimed_until <= strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        .bind(outbox_id).execute(&mut *tx).await?;
                    sqlx::query_scalar::<_, Option<String>>("select claimed_until from keepsake_audit_outbox where id = ?1 and delivered_at is null and claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                        .bind(outbox_id).fetch_optional(&mut *tx).await?.flatten()
                        .map(|value| super::sqlite::rows::parse_timestamp(&value)).transpose()
                        .map_err(|error| BridgeError::Dovecote { detail: error.to_string() })?
                } else {
                    None
                };
                if delivered_at.is_none() && active_claim.is_some() {
                    report.blocked += 1;
                    blocked = true;
                    break;
                }

                let outbox_id: Option<i64> = row.try_get("outbox_id")?;
                let (legacy_kind, legacy_id) =
                    outbox_id.map_or(("audit", audit_id), |outbox_id| ("outbox", outbox_id));
                let expected_event_type = row
                    .try_get::<Option<String>, _>("outbox_event_type")?
                    .unwrap_or_else(|| LEGACY_EVENT_TYPE.to_owned());
                let expected_occurred_at = super::sqlite::rows::parse_timestamp(
                    row.try_get("occurred_at")?,
                )
                .map_err(|error| BridgeError::Reconstruction {
                    audit_id,
                    detail: error.to_string(),
                })?;
                let persisted = load_ledger_tx(&mut tx, legacy_kind, legacy_id).await?;
                let attributes = sqlx::query("select key, value from keepsake_audit_context_attributes where audit_event_id = ?1 order by key")
                    .bind(audit_id)
                    .fetch_all(&mut *tx)
                    .await?;
                let (
                    event,
                    payload,
                    event_id,
                    event_type,
                    occurred_at,
                    payload_origin,
                    expected_row_id,
                ) = if let Some(ledger) = persisted {
                    if ledger.source != self.config.source().as_str()
                        || ledger.stream != self.config.stream().as_str()
                        || ledger.payload_codec != PAYLOAD_CODEC
                    {
                        return Err(BridgeError::ConfigurationConflict);
                    }

                    if !payload_origin_matches(
                        legacy_kind,
                        &ledger.payload_origin,
                        PAYLOAD_ORIGIN_LEGACY_OUTBOX_EXACT_TEXT,
                    ) {
                        return Err(BridgeError::Reconstruction {
                            audit_id,
                            detail: format!(
                                "payload provenance {} is incompatible with legacy {} source",
                                ledger.payload_origin, legacy_kind
                            ),
                        });
                    }

                    if ledger.event_type != expected_event_type
                        || ledger.occurred_at != expected_occurred_at
                    {
                        return Err(BridgeError::IdentityConflict {
                            legacy_kind,
                            legacy_id,
                        });
                    }

                    let event = to_dovecote_event(
                        &self.config,
                        ledger.event_id.clone(),
                        &ledger.event_type,
                        ledger.payload.clone(),
                        Some(ledger.occurred_at),
                    )?;
                    (
                        event,
                        ledger.payload,
                        ledger.event_id,
                        ledger.event_type,
                        ledger.occurred_at,
                        ledger.payload_origin,
                        Some(ledger.dovecote_row_id),
                    )
                } else {
                    let history = HistoryRow::from_row(&row, &self.config, &attributes)?;
                    let origin = if outbox_id.is_some() {
                        PAYLOAD_ORIGIN_LEGACY_OUTBOX_EXACT_TEXT
                    } else {
                        PAYLOAD_ORIGIN_RECONSTRUCTED_V1
                    };
                    (
                        history.event,
                        history.payload,
                        history.event_id,
                        expected_event_type,
                        expected_occurred_at,
                        origin.to_owned(),
                        None,
                    )
                };

                let outcome = dovecote_sqlx_sqlite::import_for_migration(
                    &mut tx,
                    event,
                    imported_delivery_state(delivered_at)?,
                )
                .await?;
                let (row_id, already) = import_outcome(&outcome)?;
                if expected_row_id.is_some_and(|expected| expected != row_id) {
                    return Err(BridgeError::IdentityConflict {
                        legacy_kind,
                        legacy_id,
                    });
                }

                if already {
                    report.already_imported += 1;
                } else {
                    report.imported += 1;
                }
                record_ledger_tx(
                    &mut tx,
                    legacy_kind,
                    legacy_id,
                    &self.config,
                    &event_id,
                    &event_type,
                    occurred_at,
                    &payload_origin,
                    payload_digest(&payload),
                    &payload,
                    row_id,
                )
                .await?;
                if sequence_kind == "outbox" {
                    report.outbox_cursor = sequence_id;
                } else {
                    report.audit_cursor = sequence_id;
                    report.cursor = sequence_id;
                }
                update_cursor_tx(&mut tx, &sequence_kind, sequence_id).await?;
            }
            tx.commit().await?;
            if blocked {
                return Ok(report);
            }
        }
    }

    /// Finalizes the fenced 1.x upgrade after a complete ordinary import.
    ///
    /// This method has no caller-supplied counts or bounds. It reads the
    /// persisted independent checkpoints, verifies that both source tables
    /// are still at those bounds, and writes activation evidence only after
    /// the live source and Dovecote state reconcile to zero delta.
    ///
    /// The caller must stop and fence every legacy writer and publisher, then
    /// hold the legacy audit and outbox tables read-only for this operation
    /// and the rollback window. This method cannot acquire an
    /// application-wide writer fence; concurrent legacy writes invalidate the
    /// evidence and are rejected as reconciliation drift.
    pub async fn finalize_upgrade_reconciliation(&self) -> Result<(), BridgeError> {
        let mut tx = dovecote_sqlx_sqlite::begin_write(&self.repository.pool).await?;
        ensure_config_tx(&mut tx, &self.config).await?;
        let row = sqlx::query(
            "select audit_high_water, outbox_high_water, completed_at from keepsake_dovecote_bridge_config where id = 1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let audit_high_water: i64 = row.try_get("audit_high_water")?;
        let outbox_high_water: i64 = row.try_get("outbox_high_water")?;
        let completed_at: Option<String> = row.try_get("completed_at")?;
        if completed_at.is_none() {
            return Err(BridgeError::ConfigurationConflict);
        }
        verify_history_tx(&mut tx, &self.config, audit_high_water, outbox_high_water).await?;
        write_evidence_tx(&mut tx, &self.config, audit_high_water, outbox_high_water).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// Re-read every legacy source row through the same typed reconstruction used
/// by the importer.  This is deliberately a separate pass: a completed
/// moving high-water import is not proof that the source stayed unchanged
/// while old writers were still active.
async fn verify_history_tx(
    tx: &mut Transaction<'_, Sqlite>,
    config: &DovecoteBridgeConfig,
    audit_high_water: i64,
    outbox_high_water: i64,
) -> Result<(), BridgeError> {
    let mut audit_cursor = 0;
    let mut outbox_cursor = 0;
    loop {
        let rows = sqlx::query(HISTORY_SQL)
            .bind(outbox_cursor)
            .bind(outbox_high_water)
            .bind(audit_cursor)
            .bind(audit_high_water)
            .bind(1_000_i64)
            .fetch_all(&mut **tx)
            .await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let sequence_kind: String = row.try_get("sequence_kind")?;
            let sequence_id: i64 = row.try_get("sequence_id")?;
            let audit_id: i64 = row.try_get("audit_id")?;
            let outbox_id: Option<i64> = row.try_get("outbox_id")?;
            let legacy_kind = if outbox_id.is_some() {
                "outbox"
            } else {
                "audit"
            };
            let legacy_id = outbox_id.unwrap_or(audit_id);
            let expected_event_type = row
                .try_get::<Option<String>, _>("outbox_event_type")?
                .unwrap_or_else(|| LEGACY_EVENT_TYPE.to_owned());
            let expected_occurred_at = super::sqlite::rows::parse_timestamp(
                row.try_get("occurred_at")?,
            )
            .map_err(|error| BridgeError::Reconstruction {
                audit_id,
                detail: error.to_string(),
            })?;
            let attributes = sqlx::query(
                "select key, value from keepsake_audit_context_attributes where audit_event_id = ?1 order by key",
            )
            .bind(audit_id)
            .fetch_all(&mut **tx)
            .await?;
            let history = HistoryRow::from_row(&row, config, &attributes)?;
            let Some(ledger) = load_ledger_tx(tx, legacy_kind, legacy_id).await? else {
                return Err(BridgeError::Reconciliation {
                    missing: 1,
                    extra: 0,
                    state_delta: 0,
                    digest_delta: 1,
                    active_claims: 0,
                });
            };

            if ledger.source != config.source().as_str()
                || ledger.stream != config.stream().as_str()
                || ledger.payload_codec != PAYLOAD_CODEC
                || !payload_origin_matches(
                    legacy_kind,
                    &ledger.payload_origin,
                    PAYLOAD_ORIGIN_LEGACY_OUTBOX_EXACT_TEXT,
                )
                || ledger.event_id != history.event_id
                || ledger.event_type != expected_event_type
                || ledger.occurred_at != expected_occurred_at
                || ledger.payload != history.payload
                || ledger.payload_sha256 != payload_digest(&ledger.payload)
            {
                return Err(BridgeError::Reconciliation {
                    missing: 0,
                    extra: 0,
                    state_delta: 0,
                    digest_delta: 1,
                    active_claims: 0,
                });
            }

            if sequence_kind == "outbox" {
                outbox_cursor = sequence_id;
            } else {
                audit_cursor = sequence_id;
            }
        }
    }

    Ok(())
}

async fn import_new_audit_event(
    tx: &mut Transaction<'_, Sqlite>,
    config: &DovecoteBridgeConfig,
    outbox_id: i64,
    audit_event: &AuditEvent,
) -> Result<i64, BridgeError> {
    let payload = encode_payload(audit_event)?;
    let event_id = outbox_event_id(outbox_id);
    let event = to_dovecote_event(
        config,
        event_id.clone(),
        LEGACY_EVENT_TYPE,
        payload.clone(),
        Some(audit_event.at),
    )?;
    let outcome = dovecote_sqlx_sqlite::import_for_migration(
        tx,
        event,
        dovecote::ImportedDeliveryState::pending(),
    )
    .await?;
    let (row_id, _) = import_outcome(&outcome)?;
    record_ledger_tx(
        tx,
        "outbox",
        outbox_id,
        config,
        &event_id,
        LEGACY_EVENT_TYPE,
        audit_event.at,
        PAYLOAD_ORIGIN_BRIDGE_EXACT,
        payload_digest(&payload),
        &payload,
        row_id,
    )
    .await?;
    Ok(row_id)
}

async fn ensure_config_tx(
    tx: &mut Transaction<'_, Sqlite>,
    config: &DovecoteBridgeConfig,
) -> Result<(), BridgeError> {
    let row = sqlx::query(
        "select source, stream, payload_codec from keepsake_dovecote_bridge_config where id = 1",
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = row {
        if row.try_get::<String, _>("source")? != config.source().as_str()
            || row.try_get::<String, _>("stream")? != config.stream().as_str()
            || row.try_get::<String, _>("payload_codec")? != PAYLOAD_CODEC
        {
            return Err(BridgeError::ConfigurationConflict);
        }
    } else {
        sqlx::query("insert into keepsake_dovecote_bridge_config (id, source, stream, payload_codec, updated_at) values (1, ?1, ?2, ?3, ?4)").bind(config.source().as_str()).bind(config.stream().as_str()).bind(PAYLOAD_CODEC).bind(super::sqlite::rows::format_timestamp(Utc::now())).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn ensure_progress_tx(
    tx: &mut Transaction<'_, Sqlite>,
    config: &DovecoteBridgeConfig,
    options: &BridgeImportOptions,
) -> Result<(i64, i64), BridgeError> {
    ensure_config_tx(tx, config).await?;
    let row = sqlx::query(
        "select audit_high_water, audit_cursor, outbox_high_water, outbox_cursor, completed_at from keepsake_dovecote_bridge_config where id = 1",
    )
    .fetch_one(&mut **tx)
    .await?;
    let stored_audit: Option<i64> = row.try_get("audit_high_water")?;
    let stored_outbox: Option<i64> = row.try_get("outbox_high_water")?;
    let completed: Option<String> = row.try_get("completed_at")?;
    for (stored, requested) in [
        (stored_audit, options.audit_high_water()),
        (stored_outbox, options.outbox_high_water()),
    ] {
        if stored
            .is_some_and(|value| value > requested || (value != requested && completed.is_none()))
        {
            return Err(BridgeError::ConfigurationConflict);
        }
    }

    if stored_audit.is_none()
        || stored_outbox.is_none()
        || stored_audit.is_some_and(|value| value < options.audit_high_water())
        || stored_outbox.is_some_and(|value| value < options.outbox_high_water())
    {
        sqlx::query("update keepsake_dovecote_bridge_config set audit_high_water = ?1, outbox_high_water = ?2, completed_at = case when audit_high_water != ?1 or outbox_high_water != ?2 then null else completed_at end, updated_at = ?3 where id = 1")
            .bind(options.audit_high_water())
            .bind(options.outbox_high_water())
            .bind(super::sqlite::rows::format_timestamp(Utc::now()))
            .execute(&mut **tx)
            .await?;
    }
    Ok((row.try_get("audit_cursor")?, row.try_get("outbox_cursor")?))
}

async fn update_cursor_tx(
    tx: &mut Transaction<'_, Sqlite>,
    sequence_kind: &str,
    cursor: i64,
) -> Result<(), BridgeError> {
    let column = if sequence_kind == "outbox" {
        "outbox_cursor"
    } else {
        "audit_cursor"
    };
    match column {
        "outbox_cursor" => sqlx::query(
            "update keepsake_dovecote_bridge_config set outbox_cursor = ?1, updated_at = ?2 where id = 1",
        )
        .bind(cursor)
        .bind(super::sqlite::rows::format_timestamp(Utc::now()))
        .execute(&mut **tx)
        .await?,
        _ => sqlx::query(
            "update keepsake_dovecote_bridge_config set audit_cursor = ?1, updated_at = ?2 where id = 1",
        )
        .bind(cursor)
        .bind(super::sqlite::rows::format_timestamp(Utc::now()))
        .execute(&mut **tx)
        .await?,
    };
    Ok(())
}

async fn mark_complete_tx(
    tx: &mut Transaction<'_, Sqlite>,
    audit_high_water: i64,
    outbox_high_water: i64,
) -> Result<(), BridgeError> {
    sqlx::query("update keepsake_dovecote_bridge_config set audit_cursor = ?1, outbox_cursor = ?2, completed_at = coalesce(completed_at, ?3), updated_at = ?3 where id = 1")
        .bind(audit_high_water)
        .bind(outbox_high_water)
        .bind(super::sqlite::rows::format_timestamp(Utc::now()))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn write_evidence_tx(
    tx: &mut Transaction<'_, Sqlite>,
    config: &DovecoteBridgeConfig,
    audit_high_water: i64,
    outbox_high_water: i64,
) -> Result<(), BridgeError> {
    if config.stream().as_str() != super::upgrade::STREAM {
        return Err(BridgeError::ConfigurationConflict);
    }

    let max_audit: i64 =
        sqlx::query_scalar("select coalesce(max(id), 0) from keepsake_audit_events")
            .fetch_one(&mut **tx)
            .await?;
    let max_outbox: i64 =
        sqlx::query_scalar("select coalesce(max(id), 0) from keepsake_audit_outbox")
            .fetch_one(&mut **tx)
            .await?;
    if max_audit != audit_high_water || max_outbox != outbox_high_water {
        return Err(BridgeError::ConfigurationConflict);
    }
    validate_persisted_events_tx(tx, config).await?;

    let missing: i64 = sqlx::query_scalar(
        "select (select count(*) from keepsake_audit_outbox o left join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'outbox' and l.legacy_id = o.id where o.id <= ?1 and l.legacy_id is null) + (select count(*) from keepsake_audit_events a left join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'audit' and l.legacy_id = a.id where a.id <= ?2 and not exists (select 1 from keepsake_audit_outbox o where o.audit_event_id = a.id) and l.legacy_id is null)",
    )
    .bind(outbox_high_water)
    .bind(audit_high_water)
    .fetch_one(&mut **tx)
    .await?;
    let extra: i64 = sqlx::query_scalar(
        "select (select count(*) from keepsake_dovecote_bridge_ledger l where (l.legacy_kind = 'outbox' and not exists (select 1 from keepsake_audit_outbox o where o.id = l.legacy_id)) or (l.legacy_kind = 'audit' and not exists (select 1 from keepsake_audit_events a where a.id = l.legacy_id))) + (select count(*) from dovecote_events e where e.source = ?1 and e.stream = ?2 and not exists (select 1 from keepsake_dovecote_bridge_ledger l where l.dovecote_row_id = e.row_id))",
    )
    .bind(config.source().as_str())
    .bind(config.stream().as_str())
    .fetch_one(&mut **tx)
    .await?;
    let state_delta: i64 = sqlx::query_scalar(
        "select (select count(*) from keepsake_audit_outbox o join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'outbox' and l.legacy_id = o.id left join dovecote_deliveries d on d.event_row_id = l.dovecote_row_id where o.id <= ?1 and (d.event_row_id is null or (o.delivered_at is null and d.state <> 'pending') or (o.delivered_at is not null and (d.state <> 'delivered' or d.delivered_at <> o.delivered_at)))) + (select count(*) from keepsake_audit_events a join keepsake_dovecote_bridge_ledger l on l.legacy_kind = 'audit' and l.legacy_id = a.id left join dovecote_deliveries d on d.event_row_id = l.dovecote_row_id where a.id <= ?2 and not exists (select 1 from keepsake_audit_outbox o where o.audit_event_id = a.id) and (d.event_row_id is null or d.state <> 'pending'))",
    )
    .bind(outbox_high_water)
    .bind(audit_high_water)
    .fetch_one(&mut **tx)
    .await?;
    let digest_delta: i64 = sqlx::query_scalar(
        "select (select count(*) from keepsake_dovecote_bridge_ledger l left join dovecote_events e on e.row_id = l.dovecote_row_id where e.row_id is null or e.source <> l.source or e.event_id <> l.event_id or e.event_type <> l.event_type or e.data <> l.payload) + (select count(*) from keepsake_dovecote_bridge_ledger l join keepsake_audit_outbox o on l.legacy_kind = 'outbox' and l.legacy_id = o.id where cast(o.payload as blob) <> l.payload)",
    )
    .fetch_one(&mut **tx)
    .await?;
    let active_claims: i64 = sqlx::query_scalar(
        "select count(*) from keepsake_audit_outbox where id <= ?1 and delivered_at is null and claimed_until > strftime('%Y-%m-%dT%H:%M:%fZ','now')",
    )
    .bind(outbox_high_water)
    .fetch_one(&mut **tx)
    .await?;
    if [missing, extra, state_delta, digest_delta, active_claims]
        .into_iter()
        .any(|count| count != 0)
    {
        return Err(BridgeError::Reconciliation {
            missing,
            extra,
            state_delta,
            digest_delta,
            active_claims,
        });
    }

    sqlx::query("insert into keepsake_upgrade_evidence (evidence_id, evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete) values (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0, 0, 0, 0, ?8, 1) on conflict(evidence_id) do nothing")
        .bind(super::upgrade::EVIDENCE_SCHEMA_VERSION)
        .bind(super::upgrade::PROVENANCE)
        .bind(config.source().as_str())
        .bind(super::upgrade::SOURCE_SCHEMA)
        .bind(super::upgrade::STREAM)
        .bind(audit_high_water)
        .bind(outbox_high_water)
        .bind(PAYLOAD_CODEC)
        .execute(&mut **tx)
        .await?;
    let existing = sqlx::query_as::<_, super::upgrade::EvidenceRow>("select evidence_schema_version, provenance, source, source_schema, stream, audit_high_water, outbox_high_water, missing_count, extra_count, state_delta_count, digest_delta_count, active_claim_count, codec_version, complete from keepsake_upgrade_evidence where evidence_id = 1")
        .fetch_one(&mut **tx)
        .await?;
    if existing.evidence_schema_version != super::upgrade::EVIDENCE_SCHEMA_VERSION
        || existing.provenance != super::upgrade::PROVENANCE
        || existing.source != config.source().as_str()
        || existing.source_schema != super::upgrade::SOURCE_SCHEMA
        || existing.stream != super::upgrade::STREAM
        || existing.audit_high_water != audit_high_water
        || existing.outbox_high_water != outbox_high_water
        || existing.codec_version != PAYLOAD_CODEC
        || !existing.complete
        || existing.missing_count != 0
        || existing.extra_count != 0
        || existing.state_delta_count != 0
        || existing.digest_delta_count != 0
        || existing.active_claim_count != 0
    {
        return Err(BridgeError::EvidenceConflict);
    }
    Ok(())
}

/// Check every immutable field written by the Dovecote importer, rather than
/// only the fields needed by the ordinary delivery reconciliation query.
async fn validate_persisted_events_tx(
    tx: &mut Transaction<'_, Sqlite>,
    config: &DovecoteBridgeConfig,
) -> Result<(), BridgeError> {
    let rows = sqlx::query(
        "select l.legacy_kind, l.dovecote_row_id as ledger_row_id, l.source as ledger_source, l.stream as ledger_stream, l.event_id as ledger_event_id, l.event_type as ledger_event_type, l.occurred_at as ledger_occurred_at, l.payload_codec, l.payload_origin, l.payload, l.payload_sha256, e.row_id as event_row_id, e.stream as event_stream, e.specversion as event_specversion, e.event_id as event_event_id, e.source as event_source, e.event_type as event_event_type, e.subject as event_subject, e.occurred_at as event_occurred_at, e.datacontenttype as event_datacontenttype, e.dataschema as event_dataschema, e.partitionkey as event_partitionkey, e.extensions as event_extensions, e.data_kind as event_data_kind, e.data as event_data from keepsake_dovecote_bridge_ledger l left join dovecote_events e on e.row_id = l.dovecote_row_id",
    )
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let ledger_row_id: i64 = row.try_get("ledger_row_id")?;
        let ledger_legacy_kind: String = row.try_get("legacy_kind")?;
        let ledger_source: String = row.try_get("ledger_source")?;
        let ledger_stream: String = row.try_get("ledger_stream")?;
        let ledger_event_id: String = row.try_get("ledger_event_id")?;
        let ledger_event_type: String = row.try_get("ledger_event_type")?;
        let ledger_occurred_at =
            super::sqlite::rows::parse_timestamp(&row.try_get::<String, _>("ledger_occurred_at")?)?;
        let ledger_payload_codec: String = row.try_get("payload_codec")?;
        let ledger_payload_origin: String = row.try_get("payload_origin")?;
        let ledger_payload: Vec<u8> = row.try_get("payload")?;
        let ledger_payload_sha256: String = row.try_get("payload_sha256")?;
        let event_row_id: Option<i64> = row.try_get("event_row_id")?;
        let event_stream: Option<String> = row.try_get("event_stream")?;
        let event_specversion: Option<String> = row.try_get("event_specversion")?;
        let event_event_id: Option<String> = row.try_get("event_event_id")?;
        let event_source: Option<String> = row.try_get("event_source")?;
        let event_event_type: Option<String> = row.try_get("event_event_type")?;
        let event_subject: Option<String> = row.try_get("event_subject")?;
        let event_occurred_at = row
            .try_get::<Option<String>, _>("event_occurred_at")?
            .map(|value| super::sqlite::rows::parse_timestamp(&value))
            .transpose()?;
        let event_datacontenttype: Option<String> = row.try_get("event_datacontenttype")?;
        let event_dataschema: Option<String> = row.try_get("event_dataschema")?;
        let event_partitionkey: Option<String> = row.try_get("event_partitionkey")?;
        let event_extensions: Option<String> = row.try_get("event_extensions")?;
        let event_data_kind: Option<String> = row.try_get("event_data_kind")?;
        let event_data: Option<Vec<u8>> = row.try_get("event_data")?;
        let valid_origin = payload_origin_matches(
            &ledger_legacy_kind,
            &ledger_payload_origin,
            PAYLOAD_ORIGIN_LEGACY_OUTBOX_EXACT_TEXT,
        );
        let drift = payload_digest(&ledger_payload) != ledger_payload_sha256
            || !valid_origin
            || ledger_source != config.source().as_str()
            || ledger_stream != config.stream().as_str()
            || ledger_payload_codec != PAYLOAD_CODEC
            || event_row_id != Some(ledger_row_id)
            || event_stream.as_deref() != Some(config.stream().as_str())
            || event_specversion.as_deref() != Some(dovecote::SPEC_VERSION)
            || event_event_id.as_deref() != Some(ledger_event_id.as_str())
            || event_source.as_deref() != Some(ledger_source.as_str())
            || event_event_type.as_deref() != Some(ledger_event_type.as_str())
            || event_subject.is_some()
            || event_occurred_at != Some(ledger_occurred_at)
            || event_datacontenttype.as_deref() != Some("application/json")
            || event_dataschema.is_some()
            || event_partitionkey.is_some()
            || event_extensions.as_deref() != Some("{}")
            || event_data_kind.as_deref() != Some("json")
            || event_data.as_deref() != Some(ledger_payload.as_slice());
        if drift {
            return Err(BridgeError::Reconciliation {
                missing: 0,
                extra: 0,
                state_delta: 0,
                digest_delta: 1,
                active_claims: 0,
            });
        }
    }
    Ok(())
}

struct PersistedLedger {
    source: String,
    stream: String,
    event_id: String,
    event_type: String,
    occurred_at: chrono::DateTime<Utc>,
    payload_codec: String,
    payload_origin: String,
    payload: Vec<u8>,
    payload_sha256: String,
    dovecote_row_id: i64,
}

async fn load_ledger_tx(
    tx: &mut Transaction<'_, Sqlite>,
    legacy_kind: &str,
    legacy_id: i64,
) -> Result<Option<PersistedLedger>, BridgeError> {
    sqlx::query(
        "select source, stream, event_id, event_type, occurred_at, payload_codec, payload_origin, payload, payload_sha256, dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = ?1 and legacy_id = ?2",
    )
    .bind(legacy_kind)
    .bind(legacy_id)
    .fetch_optional(&mut **tx)
    .await?
    .map(|row| {
        Ok(PersistedLedger {
            source: row.try_get("source")?,
            stream: row.try_get("stream")?,
            event_id: row.try_get("event_id")?,
            event_type: row.try_get("event_type")?,
            occurred_at: super::sqlite::rows::parse_timestamp(row.try_get("occurred_at")?)?,
            payload_codec: row.try_get("payload_codec")?,
            payload_origin: row.try_get("payload_origin")?,
            payload: row.try_get("payload")?,
            payload_sha256: row.try_get("payload_sha256")?,
            dovecote_row_id: row.try_get("dovecote_row_id")?,
        })
    })
    .transpose()
}

async fn record_ledger_tx(
    tx: &mut Transaction<'_, Sqlite>,
    legacy_kind: &str,
    legacy_id: i64,
    config: &DovecoteBridgeConfig,
    event_id: &str,
    event_type: &str,
    occurred_at: chrono::DateTime<Utc>,
    payload_origin: &str,
    digest: String,
    payload: &[u8],
    row_id: i64,
) -> Result<(), BridgeError> {
    let result = sqlx::query("insert into keepsake_dovecote_bridge_ledger (legacy_kind, legacy_id, source, stream, event_id, event_type, occurred_at, payload_codec, payload_origin, payload, payload_sha256, dovecote_row_id, imported_at) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) on conflict do nothing").bind(legacy_kind).bind(legacy_id).bind(config.source().as_str()).bind(config.stream().as_str()).bind(event_id).bind(event_type).bind(super::sqlite::rows::format_timestamp(occurred_at)).bind(PAYLOAD_CODEC).bind(payload_origin).bind(payload).bind(&digest).bind(row_id).bind(super::sqlite::rows::format_timestamp(Utc::now())).execute(&mut **tx).await?;
    if result.rows_affected() == 0 {
        let old = sqlx::query("select source, stream, event_id, event_type, occurred_at, payload_codec, payload_origin, payload, payload_sha256, dovecote_row_id from keepsake_dovecote_bridge_ledger where legacy_kind = ?1 and legacy_id = ?2").bind(legacy_kind).bind(legacy_id).fetch_optional(&mut **tx).await?;
        let Some(old) = old else {
            return Err(BridgeError::IdentityConflict {
                legacy_kind: if legacy_kind == "audit" {
                    "audit"
                } else {
                    "outbox"
                },
                legacy_id,
            });
        };

        if old.try_get::<String, _>("source")? != config.source().as_str()
            || old.try_get::<String, _>("stream")? != config.stream().as_str()
            || old.try_get::<String, _>("event_id")? != event_id
            || old.try_get::<String, _>("event_type")? != event_type
            || super::sqlite::rows::parse_timestamp(old.try_get("occurred_at")?)? != occurred_at
            || old.try_get::<String, _>("payload_codec")? != PAYLOAD_CODEC
            || old.try_get::<String, _>("payload_origin")? != payload_origin
            || old.try_get::<Vec<u8>, _>("payload")? != payload
            || old.try_get::<String, _>("payload_sha256")? != digest
            || old.try_get::<i64, _>("dovecote_row_id")? != row_id
        {
            return Err(BridgeError::IdentityConflict {
                legacy_kind: if legacy_kind == "audit" {
                    "audit"
                } else {
                    "outbox"
                },
                legacy_id,
            });
        }
    }
    Ok(())
}

fn validate_options(options: &BridgeImportOptions) -> Result<(), BridgeError> {
    if options.audit_high_water() < 0 {
        return Err(BridgeError::InvalidLimit {
            field: "audit_high_water",
            value: options.audit_high_water(),
            max: i64::MAX,
        });
    }

    if options.outbox_high_water() < 0 {
        return Err(BridgeError::InvalidLimit {
            field: "outbox_high_water",
            value: options.outbox_high_water(),
            max: i64::MAX,
        });
    }

    if !(1..=10_000).contains(&options.batch_size()) {
        return Err(BridgeError::InvalidLimit {
            field: "batch_size",
            value: options.batch_size(),
            max: 10_000,
        });
    }
    Ok(())
}

fn outbox_record_from_sqlite_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<AuditOutboxRecord, BridgeError> {
    let payload = serde_json::from_value::<AuditEvent>(row.try_get("payload")?)?;
    Ok(AuditOutboxRecord {
        id: row.try_get("id")?,
        audit_event_id: row.try_get("audit_event_id")?,
        event_type: row.try_get("event_type")?,
        payload,
        claimed_by: row.try_get("claimed_by")?,
        claimed_until: row
            .try_get::<Option<String>, _>("claimed_until")?
            .map(|value| super::sqlite::rows::parse_timestamp(&value))
            .transpose()
            .map_err(|error| BridgeError::Dovecote {
                detail: error.to_string(),
            })?,
        delivered_at: row
            .try_get::<Option<String>, _>("delivered_at")?
            .map(|value| super::sqlite::rows::parse_timestamp(&value))
            .transpose()
            .map_err(|error| BridgeError::Dovecote {
                detail: error.to_string(),
            })?,
    })
}

struct HistoryRow {
    event: dovecote::NewEvent,
    payload: Vec<u8>,
    event_id: String,
}

impl HistoryRow {
    fn from_row(
        row: &sqlx::sqlite::SqliteRow,
        config: &DovecoteBridgeConfig,
        attributes: &[sqlx::sqlite::SqliteRow],
    ) -> Result<Self, BridgeError> {
        let audit_id: i64 = row.try_get("audit_id")?;
        let outbox_id: Option<i64> = row.try_get("outbox_id")?;
        let event_type = row
            .try_get::<Option<String>, _>("outbox_event_type")?
            .unwrap_or_else(|| LEGACY_EVENT_TYPE.to_owned());
        let payload = if let Some(payload) = row.try_get::<Option<String>, _>("outbox_payload")? {
            serde_json::from_str::<AuditEvent>(&payload).map_err(|error| {
                BridgeError::Reconstruction {
                    audit_id,
                    detail: error.to_string(),
                }
            })?;
            payload.into_bytes()
        } else {
            encode_payload(&reconstruct(row, attributes)?)?
        };
        let event_id = outbox_id.map_or_else(|| audit_event_id(audit_id), outbox_event_id);
        let occurred =
            super::sqlite::rows::parse_timestamp(row.try_get("occurred_at")?).map_err(|error| {
                BridgeError::Reconstruction {
                    audit_id,
                    detail: error.to_string(),
                }
            })?;
        let event = to_dovecote_event(
            config,
            event_id.clone(),
            &event_type,
            payload.clone(),
            Some(occurred),
        )?;
        Ok(Self {
            event,
            payload,
            event_id,
        })
    }
}

fn reconstruct(
    row: &sqlx::sqlite::SqliteRow,
    attributes: &[sqlx::sqlite::SqliteRow],
) -> Result<AuditEvent, BridgeError> {
    let audit_id: i64 = row.try_get("audit_id")?;
    let mut context_attributes = std::collections::BTreeMap::new();
    for attribute in attributes {
        context_attributes.insert(attribute.try_get("key")?, attribute.try_get("value")?);
    }

    let occurred_at =
        super::sqlite::rows::parse_timestamp(row.try_get("occurred_at")?).map_err(|error| {
            BridgeError::Reconstruction {
                audit_id,
                detail: error.to_string(),
            }
        })?;
    reconstruct_audit_event_v1(LegacyAuditEventV1 {
        audit_id,
        event_type: row.try_get("audit_event_type")?,
        occurred_at,
        actor_kind: row.try_get("actor_kind")?,
        actor_id: row.try_get("actor_id")?,
        keepsake_id: super::support::parse_uuid(row.try_get("keepsake_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        relation_id: super::support::parse_uuid(row.try_get("relation_id")?)?,
        decision: serde_json::from_str(row.try_get("decision")?)?,
        context_attributes,
    })
}

const HISTORY_SQL: &str = "select 'outbox' as sequence_kind, o.id as sequence_id, a.id as audit_id, o.id as outbox_id, o.event_type as outbox_event_type, o.payload as outbox_payload, o.claimed_until, o.delivered_at, a.event_type as audit_event_type, a.occurred_at, a.actor_kind, a.actor_id, a.keepsake_id, a.subject_kind, a.subject_id, a.relation_id, a.decision from keepsake_audit_outbox o join keepsake_audit_events a on a.id = o.audit_event_id where o.id > ?1 and o.id <= ?2 union all select 'audit' as sequence_kind, a.id as sequence_id, a.id as audit_id, null as outbox_id, null as outbox_event_type, null as outbox_payload, null as claimed_until, null as delivered_at, a.event_type as audit_event_type, a.occurred_at, a.actor_kind, a.actor_id, a.keepsake_id, a.subject_kind, a.subject_id, a.relation_id, a.decision from keepsake_audit_events a where a.id > ?3 and a.id <= ?4 and not exists (select 1 from keepsake_audit_outbox o where o.audit_event_id = a.id) order by sequence_id, sequence_kind limit ?5";
