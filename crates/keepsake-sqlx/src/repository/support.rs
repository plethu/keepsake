//! Dialect-independent domain helpers shared across SQL backends.
//!
//! Everything here is pure model logic with no SQL text or driver coupling.
//! Each backend module owns its own SQL strings, placeholder syntax, and row
//! decoding; this module owns the parts of those flows that do not vary by
//! dialect so they are written and tested once.

use keepsake::{AUDIT_PAYLOAD_SCHEMA_VERSION, AuditEvent};
use time::OffsetDateTime;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod occurrence;
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(super) use occurrence::parse_uuid;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(super) use occurrence::{
    apply_event, canonical_expiry_policy, canonical_relation, canonical_timestamp, dovecote_event,
    dovecote_tenant_id, expires_at, expiry_event, parse_state, revoke_by_subject_event,
    revoke_event,
};

use super::{RepositoryError, RepositoryResult};

/// Application-selected `CloudEvents` settings for Keepsake audit events.
#[derive(Clone, Debug)]
pub struct DovecoteAuditConfig {
    pub(super) source: dovecote::EventSource,
    pub(super) stream: dovecote::StreamName,
    pub(super) event_type: dovecote::EventType,
}

impl DovecoteAuditConfig {
    /// Creates a configuration with the required application-owned absolute source URI.
    ///
    /// # Errors
    ///
    /// Returns an error when the event source is not a valid absolute Dovecote source URI.
    pub fn new(source: impl Into<String>) -> RepositoryResult<Self> {
        let source = source.into();
        dovecote::AbsoluteUri::new(source.clone()).map_err(RepositoryError::DovecoteValidation)?;
        Ok(Self {
            source: dovecote::EventSource::new(source)
                .map_err(RepositoryError::DovecoteValidation)?,
            stream: dovecote::StreamName::new("keepsake-audit")
                .map_err(RepositoryError::DovecoteValidation)?,
            event_type: dovecote::EventType::new("keepsake.audit_event_recorded")
                .map_err(RepositoryError::DovecoteValidation)?,
        })
    }

    /// Returns the configured `CloudEvents` source URI.
    #[must_use]
    pub fn source(&self) -> &str {
        self.source.as_str()
    }

    /// Returns the configured Dovecote stream.
    #[must_use]
    pub fn stream(&self) -> &str {
        self.stream.as_str()
    }

    /// Returns the configured Dovecote event type.
    #[must_use]
    pub fn event_type(&self) -> &str {
        self.event_type.as_str()
    }
}

/// A typed failure while projecting one Dovecote event into a current
/// [`keepsake::AuditEvent`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuditEventDecodeError {
    /// A required `CloudEvents` envelope member did not match the configuration.
    #[error("invalid Keepsake audit event envelope: {field}")]
    InvalidEnvelope {
        /// Envelope member which failed validation.
        field: &'static str,
    },

    /// The event did not contain structured JSON data.
    #[error("Keepsake audit event has no JSON payload")]
    MissingJsonPayload,

    /// A typed lifecycle command disagrees with its immutable occurrence.
    #[error("invalid lifecycle audit occurrence: {0}")]
    InvalidCommand(#[from] keepsake::KeepsakeError),

    /// The JSON payload was not a current `AuditEvent`.
    #[error("invalid current Keepsake audit payload: {0}")]
    Json(#[from] serde_json::Error),

    /// A v1 migrated event identity was recognized, but its historical
    /// payload is deliberately not silently reinterpreted as a v2 event.
    #[error(
        "Keepsake audit event {event_id} uses a migrated legacy identity; the current-event decoder cannot reinterpret its payload"
    )]
    LegacyEvent {
        /// Legacy outer Dovecote event identity.
        event_id: String,
    },

    /// A v3 JSON payload must be handled by an application-owned legacy
    /// decoder rather than being reinterpreted as a current event.
    #[error(
        "Keepsake audit payload schema version {schema_version} is legacy and requires an explicit legacy decoder"
    )]
    LegacyPayload {
        /// Historical payload schema discriminator.
        schema_version: u16,
    },

    /// A payload advertised a schema version this crate does not understand.
    #[error("unknown Keepsake audit payload schema version {schema_version}")]
    UnknownPayloadVersion {
        /// Unsupported payload schema discriminator.
        schema_version: u16,
    },

    /// The tenant stored with the Dovecote page disagreed with the tenant in
    /// the typed audit payload.
    #[error(
        "Keepsake audit event tenant does not match storage: storage={storage_tenant}, payload={payload_tenant}"
    )]
    TenantMismatch {
        /// Tenant recorded by Dovecote storage.
        storage_tenant: String,
        /// Tenant declared by the typed audit payload.
        payload_tenant: String,
    },
}

const fn legacy_payload_schema_version() -> u16 {
    3
}

#[derive(serde::Deserialize)]
struct AuditPayloadHeader {
    #[serde(default = "legacy_payload_schema_version")]
    schema_version: u16,
}

fn decode_current_audit_payload_value(
    value: serde_json::Value,
) -> Result<AuditEvent, AuditEventDecodeError> {
    let header: AuditPayloadHeader = serde_json::from_value(value.clone())?;
    match header.schema_version {
        AUDIT_PAYLOAD_SCHEMA_VERSION => {}
        3 => {
            return Err(AuditEventDecodeError::LegacyPayload {
                schema_version: header.schema_version,
            });
        }
        schema_version => {
            return Err(AuditEventDecodeError::UnknownPayloadVersion { schema_version });
        }
    }

    Ok(serde_json::from_value(value)?)
}

/// Decodes a stored payload only when it advertises the current audit schema.
///
/// Replay loads the payload from Dovecote before it has an envelope object to
/// validate, but must still route legacy and future payloads before current
/// shape decoding.
pub(super) fn decode_current_audit_payload(
    payload: &[u8],
) -> Result<AuditEvent, AuditEventDecodeError> {
    decode_current_audit_payload_value(serde_json::from_slice(payload)?)
}

/// Decodes a replay payload and verifies that its declared tenant is the
/// tenant selected by the storage lookup.
///
/// The Dovecote query is scoped by the storage tenant, but the JSON payload is
/// independently mutable data. Keep the two identities coupled before replay
/// equivalence can reuse the stored occurrence.
#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
pub(super) fn decode_current_audit_payload_for_tenant(
    payload: &[u8],
    storage_tenant: &keepsake::TenantId,
) -> Result<AuditEvent, AuditEventDecodeError> {
    let event = decode_current_audit_payload(payload)?;
    if event.tenant_id != *storage_tenant {
        return Err(AuditEventDecodeError::TenantMismatch {
            storage_tenant: storage_tenant.as_str().to_owned(),
            payload_tenant: event.tenant_id.as_str().to_owned(),
        });
    }
    event.validate_command()?;
    Ok(event)
}

/// Decodes and validates one Dovecote page event emitted by Keepsake 4.0.
///
/// This projection is backend-independent: callers can pass an event from a
/// live or snapshot Dovecote page regardless of which `SQLx` adapter produced
/// it. The page carries the storage tenant, which is checked against the JSON
/// payload before the typed value is returned. Source, stream, type, JSON
/// content, event identity, and occurrence time are also checked. Historical
/// identities (`keepsake-outbox-N` and `keepsake-audit-legacy-N`) return
/// [`AuditEventDecodeError::LegacyEvent`] because v1 payloads do not carry the
/// current event identity and require an application-specific legacy decoder.
/// Payloads without a discriminator are treated as v3 and return
/// [`AuditEventDecodeError::LegacyPayload`]; unknown explicit versions return
/// [`AuditEventDecodeError::UnknownPayloadVersion`].
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use time::OffsetDateTime;
/// use keepsake::{ActorRef, AuditContext, AuditDecision, AuditEvent, AuditEventId,
///     AuditEventType, AuditPayloadSchemaVersion, SubjectRef, TenantId};
/// use keepsake_sqlx::{decode_audit_event, DovecoteAuditConfig};
///
/// let event = AuditEvent {
///     command: None,
///     schema_version: AuditPayloadSchemaVersion::CURRENT,
///     tenant_id: TenantId::new("tenant-a")?,
///     id: AuditEventId::from_uuid(uuid::Uuid::nil()),
///     event_type: AuditEventType::Apply,
///     at: OffsetDateTime::from_unix_timestamp(1_700_000_000)?,
///     actor: ActorRef::new("system", "example")?,
///     keepsake_id: uuid::Uuid::nil(),
///     subject: SubjectRef::new("account", "acct-1")?,
///     relation_id: uuid::Uuid::nil(),
///     decision: AuditDecision::Applied { duplicate_prevented: false },
///     context: AuditContext::default(),
/// };
/// let config = DovecoteAuditConfig::new("https://example.invalid/keepsake")?;
/// let occurred_at = time::OffsetDateTime::from_unix_timestamp(event.at.unix_timestamp())?;
/// let stored = dovecote::NewEvent::builder(
///     dovecote::StreamName::new(config.stream())?,
///     dovecote::EventId::new(format!("keepsake-audit-{}", event.id.as_uuid()))?,
///     dovecote::EventSource::new(config.source())?,
///     dovecote::EventType::new(config.event_type())?,
/// )
/// .time(occurred_at)
/// .datacontenttype(dovecote::ContentType::new("application/json")?)
/// .data(dovecote::EventData::json(serde_json::to_vec(&event)?)?)
/// .build()?.into_stored()?;
/// let paged = dovecote::PagedEvent::new(
///     dovecote::TenantId::new("tenant-a")?,
///     dovecote::RowId::new(1)?,
///     stored,
///     occurred_at,
///     dovecote::DeliverySnapshot::pending(
///         occurred_at,
///         dovecote::AttemptCount::new(0)?,
///         None,
///     )?,
/// )?;
/// assert_eq!(decode_audit_event(&config, &paged)?, event);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns typed envelope, tenant, identity, occurrence-time, legacy/version or payload errors.
/// A successfully decoded historical event is not necessarily an exact command receipt.
pub fn decode_audit_event(
    config: &DovecoteAuditConfig,
    page: &dovecote::PagedEvent,
) -> Result<AuditEvent, AuditEventDecodeError> {
    let event = page.event();
    if event.source().as_str() != config.source() {
        return Err(AuditEventDecodeError::InvalidEnvelope { field: "source" });
    }

    if event.stream().as_str() != config.stream() {
        return Err(AuditEventDecodeError::InvalidEnvelope { field: "stream" });
    }

    if event.event_type().as_str() != config.event_type() {
        return Err(AuditEventDecodeError::InvalidEnvelope { field: "type" });
    }

    if !event
        .datacontenttype()
        .is_some_and(dovecote::ContentType::is_json)
    {
        return Err(AuditEventDecodeError::InvalidEnvelope {
            field: "JSON content type",
        });
    }

    let Some(dovecote::EventData::Json(payload)) = event.data() else {
        return Err(AuditEventDecodeError::MissingJsonPayload);
    };

    if is_legacy_event_id(event.id().as_str()) {
        return Err(AuditEventDecodeError::LegacyEvent {
            event_id: event.id().as_str().to_owned(),
        });
    }

    let decoded = decode_current_audit_payload(payload.as_bytes())?;
    if page.tenant_id().as_str() != decoded.tenant_id.as_str() {
        return Err(AuditEventDecodeError::TenantMismatch {
            storage_tenant: page.tenant_id().as_str().to_owned(),
            payload_tenant: decoded.tenant_id.as_str().to_owned(),
        });
    }

    let expected_id = format!("keepsake-audit-{}", decoded.id.as_uuid());
    if event.id().as_str() != expected_id {
        return Err(AuditEventDecodeError::InvalidEnvelope {
            field: "event identity",
        });
    }

    let Some(event_time) = event.time() else {
        return Err(AuditEventDecodeError::InvalidEnvelope {
            field: "occurrence time",
        });
    };

    if time_to_dovecote_for_decode(decoded.at) != Some(event_time) {
        return Err(AuditEventDecodeError::InvalidEnvelope {
            field: "occurrence time",
        });
    }

    decoded.validate_command()?;
    Ok(decoded)
}

fn is_legacy_event_id(value: &str) -> bool {
    ["keepsake-outbox-", "keepsake-audit-legacy-"]
        .iter()
        .any(|prefix| {
            value
                .strip_prefix(prefix)
                .and_then(|suffix| suffix.parse::<u64>().ok())
                .is_some_and(|sequence| sequence > 0)
        })
}

fn time_to_dovecote_for_decode(value: OffsetDateTime) -> Option<time::OffsetDateTime> {
    let nanos = value.nanosecond();
    time::OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
        .ok()
        .and_then(|value| value.replace_nanosecond(nanos).ok())
        .map(|value| value.to_offset(time::UtcOffset::UTC))
}

#[cfg(test)]
mod tests {
    use keepsake::AuditPayloadSchemaVersion;
    use keepsake::{
        ActorRef, AuditContext, AuditDecision, AuditEventId, AuditEventType, KeepsakeId,
        RelationId, SubjectRef, TenantId,
    };
    use std::error;
    use uuid::Uuid;

    use super::*;

    fn current_event() -> Result<AuditEvent, Box<dyn error::Error>> {
        Ok(AuditEvent {
            command: None,
            schema_version: AuditPayloadSchemaVersion::CURRENT,
            tenant_id: TenantId::new("tenant-test")?,
            id: AuditEventId::from_uuid(Uuid::nil()),
            event_type: AuditEventType::Apply,
            at: OffsetDateTime::from_unix_timestamp(1_700_000_000)?,
            actor: ActorRef::new("system", "test")?,
            keepsake_id: KeepsakeId::nil(),
            subject: SubjectRef::new("account", "acct-1")?,
            relation_id: RelationId::nil(),
            decision: AuditDecision::Applied {
                duplicate_prevented: false,
            },
            context: AuditContext::default(),
        })
    }

    #[test]
    fn stored_legacy_payload_is_not_decoded_as_current() -> Result<(), Box<dyn error::Error>> {
        let mut omitted_version = serde_json::to_value(current_event()?)?;
        omitted_version
            .as_object_mut()
            .ok_or("audit event did not serialize as an object")?
            .remove("schema_version");
        assert!(matches!(
            decode_current_audit_payload(&serde_json::to_vec(&omitted_version)?),
            Err(AuditEventDecodeError::LegacyPayload { schema_version: 3 })
        ));

        let mut explicit_version = serde_json::to_value(current_event()?)?;
        explicit_version["schema_version"] = serde_json::json!(3);
        assert!(matches!(
            decode_current_audit_payload(&serde_json::to_vec(&explicit_version)?),
            Err(AuditEventDecodeError::LegacyPayload { schema_version: 3 })
        ));
        Ok(())
    }

    #[test]
    fn stored_unknown_payload_is_not_decoded_as_current() -> Result<(), Box<dyn error::Error>> {
        let payload = serde_json::to_vec(&serde_json::json!({"schema_version": 99}))?;
        assert!(matches!(
            decode_current_audit_payload(&payload),
            Err(AuditEventDecodeError::UnknownPayloadVersion { schema_version: 99 })
        ));
        Ok(())
    }
}
