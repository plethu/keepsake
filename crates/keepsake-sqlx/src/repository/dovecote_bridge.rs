//! Opt-in migration bridge from the Keepsake 1.x audit outbox to Dovecote.
//!
//! The bridge is deliberately a separate repository view.  Constructing a
//! normal `SQLx` repository never enables Dovecote, changes its publisher, or
//! reads bridge configuration.  Applications opt in with
//! [`SqlxKeepsakeRepository::with_dovecote_bridge`].

use chrono::{DateTime, Utc};
use dovecote::{
    AbsoluteUri, ContentType, EventData, EventId, EventSource, EventType, ImportedDeliveryState,
    NewEvent, StreamName,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{AuditOutboxRecord, RepositoryError, SqlxKeepsakeRepository};
use crate::repository::KeepsakeSqlxBackend;

/// The default logical stream for Keepsake audit events in Dovecote.
pub const DEFAULT_STREAM: &str = "keepsake-audit";
/// The existing durable event type used by the Keepsake legacy outbox.
pub const LEGACY_EVENT_TYPE: &str = "keepsake.audit_event_recorded";
/// Stable name of the exact legacy JSON byte codec.
pub const PAYLOAD_CODEC: &str = "keepsake.audit.json.v1";
/// Provenance for bytes written by the enabled dual-write path.
pub const PAYLOAD_ORIGIN_BRIDGE_EXACT: &str = "bridge_exact";
/// Provenance for historical rows whose legacy outbox JSON was re-encoded by
/// the bridge.  This is deterministic, but is not a claim about preserving
/// the original database representation.
pub const PAYLOAD_ORIGIN_LEGACY_OUTBOX_REENCODED: &str = "legacy_outbox_reencoded";
/// Provenance for `SQLite` legacy outbox `TEXT` whose original UTF-8 bytes are
/// retained after typed JSON validation.
#[allow(dead_code)]
pub const PAYLOAD_ORIGIN_LEGACY_OUTBOX_EXACT_TEXT: &str = "legacy_outbox_exact_text";
/// Provenance for historical audit rows reconstructed from normalized columns
/// and context attributes with [`PAYLOAD_CODEC`].
pub const PAYLOAD_ORIGIN_RECONSTRUCTED_V1: &str = "reconstructed_v1";

/// Checks that ledger provenance agrees with the legacy source path that
/// produced the row.  An audit-only row can only come from the normalized
/// reconstruction codec.  An outbox row may be a bridge dual-write (whose
/// exact bytes are authoritative) or the backend's historical outbox export.
/// Keeping this check next to the provenance constants prevents each adapter
/// from accepting a different, forgeable combination of source and origin.
pub(super) fn payload_origin_matches(
    legacy_kind: &str,
    payload_origin: &str,
    historical_outbox_origin: &str,
) -> bool {
    match legacy_kind {
        "audit" => payload_origin == PAYLOAD_ORIGIN_RECONSTRUCTED_V1,
        "outbox" => {
            payload_origin == PAYLOAD_ORIGIN_BRIDGE_EXACT
                || payload_origin == historical_outbox_origin
        }
        _ => false,
    }
}

/// Typed source values needed to reconstruct a pre-outbox Keepsake audit row.
///
/// This is migration infrastructure, not a second audit write API.  The
/// adapter-specific history readers translate their legacy columns into this
/// representation and then use [`encode_reconstructed_audit_v1`].  Keeping the
/// conversion here gives every backend and the migration fixture one named,
/// deterministic codec owned by Keepsake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyAuditEventV1 {
    /// Legacy normalized audit row identifier, used only for diagnostics.
    pub audit_id: i64,
    /// Keepsake's stable storage event label.
    pub event_type: String,
    /// Authoritative occurrence time.
    pub occurred_at: DateTime<Utc>,
    /// Actor kind from the normalized row.
    pub actor_kind: String,
    /// Actor identifier from the normalized row.
    pub actor_id: String,
    /// Keepsake identifier from the normalized row.
    pub keepsake_id: Uuid,
    /// Subject kind from the normalized row.
    pub subject_kind: String,
    /// Subject identifier from the normalized row.
    pub subject_id: String,
    /// Relation identifier from the normalized row.
    pub relation_id: Uuid,
    /// Versioned JSON representation of the legacy decision.
    pub decision: serde_json::Value,
    /// Normalized context attributes, retaining absent and empty values.
    pub context_attributes: BTreeMap<String, String>,
}

/// Typed bridge configuration owned by the application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DovecoteBridgeConfig {
    source: AbsoluteUri,
    stream: StreamName,
}

impl DovecoteBridgeConfig {
    /// Creates configuration with the default `keepsake-audit` stream.
    pub fn new(source: impl Into<String>) -> Result<Self, BridgeConfigError> {
        let source = AbsoluteUri::new(source).map_err(BridgeConfigError::InvalidSource)?;
        let stream =
            StreamName::new(DEFAULT_STREAM.to_owned()).map_err(BridgeConfigError::InvalidStream)?;
        Ok(Self { source, stream })
    }

    /// Replaces the default stream with an application-owned stream.
    pub fn with_stream(self, stream: impl Into<String>) -> Result<Self, BridgeConfigError> {
        let stream = StreamName::new(stream).map_err(BridgeConfigError::InvalidStream)?;
        Ok(Self { stream, ..self })
    }

    /// Returns the stable absolute producer source.
    #[must_use]
    pub const fn source(&self) -> &AbsoluteUri {
        &self.source
    }

    /// Returns the configured logical destination stream.
    #[must_use]
    pub const fn stream(&self) -> &StreamName {
        &self.stream
    }
}

/// Errors while validating bridge configuration.
#[derive(Debug, thiserror::Error)]
pub enum BridgeConfigError {
    /// The source was not an absolute URI.
    #[error("bridge source must be an absolute URI: {0}")]
    InvalidSource(dovecote::ValidationError),
    /// The stream was empty, malformed, or too large.
    #[error("invalid bridge stream: {0}")]
    InvalidStream(dovecote::ValidationError),
}

/// Stable identity exposed to a legacy publisher during dual-write cutover.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgePublisherIdentity {
    source: String,
    event_type: String,
    event_id: String,
    occurred_at: DateTime<Utc>,
    payload: Vec<u8>,
}

/// Opaque generation returned by a bridge-aware legacy outbox claim.
///
/// The legacy outbox schema has no claim token. The bridge persists this
/// separate 128-bit generation for every bridge-aware claim so a worker that
/// is reclaimed with the same owner and expiry cannot acknowledge a later
/// claim using stale information.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeClaimToken([u8; 16]);

impl BridgeClaimToken {
    pub(crate) fn fresh() -> Self {
        Self(Uuid::new_v4().into_bytes())
    }

    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Self, BridgeError> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| BridgeError::InvalidClaimToken)?;
        Ok(Self(bytes))
    }

    /// Returns the opaque generation bytes for durable worker state.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// A legacy outbox record claimed through the migration bridge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeDeliveryClaim {
    record: AuditOutboxRecord,
    token: BridgeClaimToken,
}

impl BridgeDeliveryClaim {
    pub(crate) const fn new(record: AuditOutboxRecord, token: BridgeClaimToken) -> Self {
        Self { record, token }
    }

    /// Returns the claimed legacy outbox record.
    #[must_use]
    pub const fn record(&self) -> &AuditOutboxRecord {
        &self.record
    }

    /// Returns the persisted claim generation required for acknowledgement.
    #[must_use]
    pub const fn claim_token(&self) -> &BridgeClaimToken {
        &self.token
    }

    /// Splits the claim into its record and opaque generation.
    #[must_use]
    pub fn into_parts(self) -> (AuditOutboxRecord, BridgeClaimToken) {
        (self.record, self.token)
    }
}

impl BridgePublisherIdentity {
    /// Returns the configured source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the deterministic Dovecote event id.
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the durable event type.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the authoritative legacy occurrence time.
    #[must_use]
    pub const fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    /// Returns the exact UTF-8 payload persisted for the bridge row.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Creates an identity from the values read from the durable bridge ledger.
    #[must_use]
    pub const fn from_parts(
        source: String,
        event_type: String,
        event_id: String,
        occurred_at: DateTime<Utc>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            source,
            event_type,
            event_id,
            occurred_at,
            payload,
        }
    }
}

/// Import options for a bounded, resumable complete-history pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BridgeImportOptions {
    audit_high_water: i64,
    outbox_high_water: i64,
    batch_size: i64,
}

impl BridgeImportOptions {
    /// Creates an import pass ending at the inclusive audit-event high-water.
    #[must_use]
    pub const fn new(high_water: i64) -> Self {
        Self {
            audit_high_water: high_water,
            outbox_high_water: high_water,
            batch_size: 100,
        }
    }

    /// Sets the inclusive legacy-outbox high-water independently of the
    /// normalized audit-event high-water.
    #[must_use]
    pub const fn with_outbox_high_water(mut self, high_water: i64) -> Self {
        self.outbox_high_water = high_water;
        self
    }

    /// Sets the bounded number of normalized audit rows examined per pass.
    #[must_use]
    pub const fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Inclusive normalized-audit high-water mark.
    #[must_use]
    pub const fn high_water(&self) -> i64 {
        self.audit_high_water
    }

    /// Inclusive normalized-audit high-water mark.
    #[must_use]
    pub const fn audit_high_water(&self) -> i64 {
        self.audit_high_water
    }

    /// Inclusive legacy-outbox high-water mark.
    #[must_use]
    pub const fn outbox_high_water(&self) -> i64 {
        self.outbox_high_water
    }

    /// Bounded number of rows in one transaction.
    #[must_use]
    pub const fn batch_size(&self) -> i64 {
        self.batch_size
    }
}

/// One row's import disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BridgeRowOutcome {
    /// A new pending or delivered Dovecote event was inserted.
    Imported {
        /// Dovecote event row id.
        row_id: i64,
    },
    /// The immutable source and payload were already imported.
    AlreadyImported {
        /// Existing Dovecote event row id.
        row_id: i64,
    },
    /// A legacy claim is still active at the declared fence.
    Blocked {
        /// Legacy row id held back by the fence.
        legacy_id: i64,
        /// Claim lease expiry.
        claimed_until: DateTime<Utc>,
    },
}

/// Summary of one complete-history import invocation.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct BridgeImportReport {
    /// Number of normalized audit rows examined.
    pub examined: u64,
    /// Number of new Dovecote events inserted.
    pub imported: u64,
    /// Number of idempotent reruns.
    pub already_imported: u64,
    /// Number of active legacy claims held back by the fence.
    pub blocked: u64,
    /// Last normalized audit id examined and durably checkpointed.
    pub cursor: i64,
    /// Last normalized audit id checkpointed by the independent audit scan.
    pub audit_cursor: i64,
    /// Last legacy outbox id checkpointed by the independent outbox scan.
    pub outbox_cursor: i64,
    /// Inclusive normalized-audit high-water used by this invocation.
    pub audit_high_water: i64,
    /// Inclusive legacy-outbox high-water used by this invocation.
    pub outbox_high_water: i64,
    /// Whether the inclusive high-water range is complete.
    pub complete: bool,
}

/// Typed bridge errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BridgeError {
    /// The underlying Keepsake repository operation failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// Keepsake core validation failed before a transaction could commit.
    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),
    /// JSON value encoding/decoding failed at the bridge boundary.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// `SQLx` rejected a bridge bookkeeping operation.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed.
        operation: &'static str,
        /// Preserved `SQLx` source.
        #[source]
        source: sqlx::Error,
    },
    /// `SQLx` error outside a more specific operation wrapper.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    /// `MySQL` or `MariaDB` Dovecote migration import failed.
    #[cfg(feature = "dovecote-mysql")]
    #[error("mysql dovecote import: {0}")]
    DovecoteMySql(#[from] dovecote_sqlx_mysql::ImportError),
    /// `MySQL` Dovecote migration finalization failed.
    #[cfg(feature = "dovecote-mysql")]
    #[error("mysql dovecote finalization: {0}")]
    DovecoteMySqlFinalize(#[from] dovecote_sqlx_mysql::FinalizeError),
    /// `PostgreSQL` Dovecote migration importer failed.
    #[cfg(feature = "dovecote-postgres")]
    #[error("postgres dovecote import: {0}")]
    DovecotePostgres(#[from] dovecote_sqlx_postgres::ImportError),
    /// `PostgreSQL` Dovecote migration finalization failed.
    #[cfg(feature = "dovecote-postgres")]
    #[error("postgres dovecote finalization: {0}")]
    DovecotePostgresFinalize(#[from] dovecote_sqlx_postgres::FinalizeError),
    /// `SQLite` Dovecote migration importer failed.
    #[cfg(feature = "dovecote-sqlite")]
    #[error("sqlite dovecote import: {0}")]
    DovecoteSqlite(#[from] dovecote_sqlx_sqlite::ImportError),
    /// `SQLite` Dovecote write transaction could not be started.
    #[cfg(feature = "dovecote-sqlite")]
    #[error("sqlite dovecote transaction: {0}")]
    DovecoteSqliteEnqueue(#[from] dovecote_sqlx_sqlite::EnqueueError),
    /// `SQLite` Dovecote migration finalization failed.
    #[cfg(feature = "dovecote-sqlite")]
    #[error("sqlite dovecote finalization: {0}")]
    DovecoteSqliteFinalize(#[from] dovecote_sqlx_sqlite::FinalizeError),
    /// The Dovecote event could not be built from trusted legacy values.
    #[error("dovecote event: {detail}")]
    Dovecote {
        /// Human-readable conversion detail.
        detail: String,
    },
    #[error(transparent)]
    /// A structurally preserved Dovecote validation failure.
    DovecoteValidation(#[from] dovecote::ValidationError),
    /// A timestamp could not be represented by Dovecote's time type.
    #[error("dovecote timestamp: {0}")]
    Time(#[from] time::error::ComponentRange),
    /// A bridge option was outside its bounded range.
    #[error("{field} {value} is outside the accepted range 1..={max}")]
    InvalidLimit {
        /// Option name.
        field: &'static str,
        /// Supplied value.
        value: i64,
        /// Maximum accepted value.
        max: i64,
    },
    /// The persisted configuration belongs to another producer or stream.
    #[error("bridge configuration conflicts with the persisted source or stream")]
    ConfigurationConflict,
    /// A completed source/Dovecote reconciliation disagreed with its ledger.
    #[error(
        "bridge reconciliation is not zero-delta: missing={missing}, extra={extra}, state={state_delta}, digest={digest_delta}, active_claims={active_claims}"
    )]
    Reconciliation {
        /// Source rows without a ledger entry.
        missing: i64,
        /// Ledger rows without a source row.
        extra: i64,
        /// Delivery state mismatches.
        state_delta: i64,
        /// Immutable payload mismatches.
        digest_delta: i64,
        /// Claims that remained active at the fence.
        active_claims: i64,
    },
    /// A second completed reconciliation changed immutable activation evidence.
    #[error("upgrade evidence conflicts with the existing complete reconciliation")]
    EvidenceConflict,
    /// The legacy row has an active claim at the cutover fence.
    #[error("legacy outbox row {legacy_id} remains claimed until {claimed_until}")]
    ActiveClaim {
        /// Legacy outbox row id.
        legacy_id: i64,
        /// Claim lease expiry.
        claimed_until: DateTime<Utc>,
    },
    /// The source and id already identify different immutable content.
    #[error("bridge identity conflict for {legacy_kind} row {legacy_id}")]
    IdentityConflict {
        /// Legacy identity namespace.
        legacy_kind: &'static str,
        /// Legacy id.
        legacy_id: i64,
    },
    /// The historical payload could not be deterministically reconstructed.
    #[error("cannot reconstruct normalized audit row {audit_id}: {detail}")]
    Reconstruction {
        /// Normalized audit row id.
        audit_id: i64,
        /// Human-readable reconstruction detail.
        detail: String,
    },
    /// The legacy acknowledgement did not match an owned live lease.
    #[error("legacy outbox row {outbox_id} is not owned by the supplied live worker lease")]
    DeliveryOwnership {
        /// Legacy outbox identifier.
        outbox_id: i64,
    },
    /// The bridge claim table did not contain a valid opaque generation.
    #[error("legacy outbox row has no valid bridge claim generation")]
    InvalidClaimToken,
    /// The legacy delivery timestamp conflicts with the durable acknowledgement.
    #[error("legacy outbox row {outbox_id} has a conflicting delivery timestamp")]
    DeliveryConflict {
        /// Legacy outbox identifier.
        outbox_id: i64,
    },
}

pub fn outbox_event_id(id: i64) -> String {
    // This function is intentionally not const in practice: formatting an i64
    // is stable and gives the exact ASCII identity required by the contract.
    format!("keepsake-outbox-{id}")
}

pub fn audit_event_id(id: i64) -> String {
    format!("keepsake-audit-legacy-{id}")
}

/// Reconstructs a typed Keepsake audit event from legacy normalized columns.
///
/// The input is deliberately a project-owned row representation rather than a
/// generic JSON envelope.  Unknown labels, malformed references, and invalid
/// decisions are rejected before the caller can build a Dovecote event.
pub fn reconstruct_audit_event_v1(
    input: LegacyAuditEventV1,
) -> Result<keepsake::AuditEvent, BridgeError> {
    let event_type =
        keepsake::AuditEventType::from_storage_label(&input.event_type).ok_or_else(|| {
            BridgeError::Reconstruction {
                audit_id: input.audit_id,
                detail: format!("unknown event type {}", input.event_type),
            }
        })?;
    let actor = keepsake::ActorRef::new(input.actor_kind, input.actor_id).map_err(|error| {
        BridgeError::Reconstruction {
            audit_id: input.audit_id,
            detail: error.to_string(),
        }
    })?;
    let subject =
        keepsake::SubjectRef::new(input.subject_kind, input.subject_id).map_err(|error| {
            BridgeError::Reconstruction {
                audit_id: input.audit_id,
                detail: error.to_string(),
            }
        })?;
    let decision =
        serde_json::from_value::<keepsake::AuditDecision>(input.decision).map_err(|error| {
            BridgeError::Reconstruction {
                audit_id: input.audit_id,
                detail: error.to_string(),
            }
        })?;
    Ok(keepsake::AuditEvent {
        event_type,
        at: input.occurred_at,
        actor,
        keepsake_id: input.keepsake_id,
        subject,
        relation_id: input.relation_id,
        decision,
        context: keepsake::AuditContext {
            attributes: input.context_attributes,
        },
    })
}

/// Encodes a pre-outbox normalized audit row with Keepsake's versioned JSON
/// codec.  The returned bytes are reconstructed bytes: callers must not label
/// them as the original legacy database spelling.
pub fn encode_reconstructed_audit_v1(input: LegacyAuditEventV1) -> Result<Vec<u8>, BridgeError> {
    encode_payload(&reconstruct_audit_event_v1(input)?)
}

pub fn encode_payload(event: &keepsake::AuditEvent) -> Result<Vec<u8>, BridgeError> {
    serde_json::to_vec(event).map_err(|error| BridgeError::Dovecote {
        detail: format!("encode {PAYLOAD_CODEC}: {error}"),
    })
}

pub fn payload_digest(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = std::fmt::Write::write_fmt(&mut encoded, format_args!("{byte:02x}"));
    }
    encoded
}

pub fn to_dovecote_event(
    config: &DovecoteBridgeConfig,
    event_id: String,
    event_type: &str,
    payload: Vec<u8>,
    occurred_at: Option<DateTime<Utc>>,
) -> Result<NewEvent, BridgeError> {
    let event_id = EventId::new(event_id)?;
    let source = EventSource::new(config.source().as_str())?;
    let event_type = EventType::new(event_type.to_owned())?;
    let content_type = ContentType::new("application/json")?;
    let data = EventData::json(payload)?;
    let mut builder = NewEvent::builder(config.stream().clone(), event_id, source, event_type)
        .datacontenttype(content_type)
        .data(data);
    if let Some(occurred_at) = occurred_at {
        let nanos = occurred_at
            .timestamp_nanos_opt()
            .ok_or_else(|| BridgeError::Dovecote {
                detail: "audit timestamp is outside the Dovecote range".to_owned(),
            })?;
        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(i128::from(nanos))?;
        builder = builder.time(timestamp);
    }
    builder.build().map_err(BridgeError::DovecoteValidation)
}

pub fn imported_delivery_state(
    delivered_at: Option<DateTime<Utc>>,
) -> Result<ImportedDeliveryState, BridgeError> {
    let Some(delivered_at) = delivered_at else {
        return Ok(ImportedDeliveryState::pending());
    };

    let nanos = delivered_at
        .timestamp_nanos_opt()
        .ok_or_else(|| BridgeError::Dovecote {
            detail: "delivery timestamp is outside the Dovecote range".to_owned(),
        })?;
    let timestamp = OffsetDateTime::from_unix_timestamp_nanos(i128::from(nanos))?;
    ImportedDeliveryState::delivered(timestamp).map_err(BridgeError::DovecoteValidation)
}

pub fn import_outcome(outcome: &dovecote::ImportOutcome) -> Result<(i64, bool), BridgeError> {
    match outcome {
        dovecote::ImportOutcome::Imported { row_id } => Ok((row_id.get(), false)),
        dovecote::ImportOutcome::AlreadyImported { row_id } => Ok((row_id.get(), true)),
        _ => Err(BridgeError::Dovecote {
            detail: "unsupported importer outcome".to_owned(),
        }),
    }
}

/// Bridge-enabled repository view.
#[derive(Clone, Debug)]
pub struct DovecoteBridgeRepository<B, C = super::NoopRelationCache>
where
    B: KeepsakeSqlxBackend,
{
    pub(crate) repository: SqlxKeepsakeRepository<B, C>,
    pub(crate) config: DovecoteBridgeConfig,
}

impl<B, C> DovecoteBridgeRepository<B, C>
where
    B: KeepsakeSqlxBackend,
{
    /// Returns the bridge configuration.
    #[must_use]
    pub const fn config(&self) -> &DovecoteBridgeConfig {
        &self.config
    }

    /// Rebinds the bridge view to a candidate configuration for drift checks.
    #[must_use]
    pub fn with_config(&self, config: DovecoteBridgeConfig) -> Self
    where
        C: Clone,
    {
        Self {
            repository: self.repository.clone(),
            config,
        }
    }
}

impl<B, C> SqlxKeepsakeRepository<B, C>
where
    B: KeepsakeSqlxBackend,
{
    /// Enables the opt-in dual-write migration bridge.
    pub const fn with_dovecote_bridge(
        self,
        config: DovecoteBridgeConfig,
    ) -> DovecoteBridgeRepository<B, C> {
        DovecoteBridgeRepository {
            repository: self,
            config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use keepsake::{ActorRef, AuditContext, AuditDecision, AuditEvent, AuditEventType, SubjectRef};
    use uuid::Uuid;

    #[test]
    fn config_requires_absolute_source_and_defaults_stream()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = DovecoteBridgeConfig::new("https://example.org/keepsake")?;
        assert_eq!(config.source().as_str(), "https://example.org/keepsake");
        assert_eq!(config.stream().as_str(), DEFAULT_STREAM);
        assert!(DovecoteBridgeConfig::new("relative/source").is_err());
        Ok(())
    }

    #[test]
    fn identity_is_deterministic_ascii() {
        let event_id = outbox_event_id(42);
        assert_eq!(event_id, "keepsake-outbox-42");
        assert!(event_id.is_ascii());
        assert_eq!(audit_event_id(42), "keepsake-audit-legacy-42");
        assert!(audit_event_id(42).is_ascii());
    }

    #[test]
    fn payload_codec_is_exact_deterministic_json_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let event = AuditEvent {
            event_type: AuditEventType::Apply,
            at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")?.with_timezone(&Utc),
            actor: ActorRef::new("system", "bridge")?,
            keepsake_id: Uuid::from_u128(1),
            subject: SubjectRef::new("account", "acct-1")?,
            relation_id: Uuid::from_u128(2),
            decision: AuditDecision::Applied {
                duplicate_prevented: false,
            },
            context: AuditContext::default(),
        };
        assert_eq!(encode_payload(&event)?, serde_json::to_vec(&event)?);
        assert_eq!(
            payload_digest(&encode_payload(&event)?),
            "018fd9aae88356cd5ec86bb7ae017572e3d0221a36c9c8b21685c8d6068a1353"
        );
        Ok(())
    }

    #[test]
    fn reconstructed_v1_codec_preserves_non_ascii_and_empty_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = AuditEvent {
            event_type: AuditEventType::Apply,
            at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456Z")?
                .with_timezone(&Utc),
            actor: ActorRef::new("service", "π")?,
            keepsake_id: Uuid::from_u128(1),
            subject: SubjectRef::new("user", "münchen")?,
            relation_id: Uuid::from_u128(2),
            decision: AuditDecision::Applied {
                duplicate_prevented: false,
            },
            context: AuditContext::default(),
        };
        let mut context_attributes = BTreeMap::new();
        context_attributes.insert("empty".to_owned(), String::new());
        let encoded = encode_reconstructed_audit_v1(LegacyAuditEventV1 {
            audit_id: 7,
            event_type: "apply".to_owned(),
            occurred_at: event.at,
            actor_kind: "service".to_owned(),
            actor_id: "π".to_owned(),
            keepsake_id: event.keepsake_id,
            subject_kind: "user".to_owned(),
            subject_id: "münchen".to_owned(),
            relation_id: event.relation_id,
            decision: serde_json::to_value(event.decision.clone())?,
            context_attributes,
        })?;
        let mut expected = event;
        expected
            .context
            .attributes
            .insert("empty".to_owned(), String::new());
        assert_eq!(encoded, encode_payload(&expected)?);
        Ok(())
    }

    #[test]
    fn payload_digest_is_sha256_hex() {
        assert_eq!(
            payload_digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn v1_fixture_round_trips_without_reconstruction_loss() -> Result<(), Box<dyn std::error::Error>>
    {
        let fixture = include_str!("../../tests/fixtures/dovecote-bridge-audit-v1.json");
        let event: AuditEvent = serde_json::from_str(fixture)?;
        assert_eq!(encode_payload(&event)?, fixture.trim_end().as_bytes());
        Ok(())
    }

    #[test]
    fn dovecote_mapping_is_explicit_and_stable() -> Result<(), Box<dyn std::error::Error>> {
        let config = DovecoteBridgeConfig::new("https://example.org/keepsake")?;
        let payload = br#"{"event":"audit"}"#.to_vec();
        let event = to_dovecote_event(
            &config,
            "keepsake-outbox-7".to_owned(),
            LEGACY_EVENT_TYPE,
            payload.clone(),
            None,
        )?;
        assert_eq!(event.stream().as_str(), DEFAULT_STREAM);
        assert_eq!(event.source().as_str(), "https://example.org/keepsake");
        assert_eq!(event.id().as_str(), "keepsake-outbox-7");
        assert_eq!(event.event_type().as_str(), LEGACY_EVENT_TYPE);
        assert_eq!(
            event.datacontenttype().map(dovecote::ContentType::as_str),
            Some("application/json")
        );
        assert_eq!(
            event.data().map(dovecote::EventData::as_bytes),
            Some(payload.as_slice())
        );
        assert!(event.time().is_none());
        Ok(())
    }

    #[test]
    fn imported_state_preserves_only_delivered_instants() -> Result<(), Box<dyn std::error::Error>>
    {
        assert!(matches!(
            imported_delivery_state(None)?,
            ImportedDeliveryState::Pending
        ));
        let delivered_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456Z")?
            .with_timezone(&Utc);
        let nanos = delivered_at
            .timestamp_nanos_opt()
            .ok_or_else(|| std::io::Error::other("timestamp"))?;
        let state = imported_delivery_state(Some(delivered_at))?;
        assert!(matches!(
            state,
            ImportedDeliveryState::Delivered { delivered_at: value }
                if value.unix_timestamp_nanos() == i128::from(nanos)
        ));
        Ok(())
    }
}
