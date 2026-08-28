//! Dialect-independent domain helpers shared across SQL backends.
//!
//! Everything here is pure model logic with no SQL text or driver coupling.
//! Each backend module owns its own SQL strings, placeholder syntax, and row
//! decoding; this module owns the parts of those flows that do not vary by
//! dialect so they are written and tested once.

use chrono::{DateTime, Utc};
use keepsake::{
    ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent, AuditEventId, AuditEventType,
    CommandContext, ExpiryCause, Keepsake, KeepsakeId, LifecycleState, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
#[cfg(any(feature = "mysql", feature = "sqlite"))]
use uuid::Uuid;

#[cfg(any(feature = "mysql", feature = "sqlite"))]
use keepsake::ExpiryPolicy;

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
}

/// Decodes and validates one Dovecote event emitted by Keepsake 2.0.
///
/// This projection is backend-independent: callers can pass an event from a
/// live or snapshot Dovecote page regardless of which `SQLx` adapter produced
/// it. Source, stream, type, JSON content, event identity, and occurrence time
/// are all checked before the typed value is returned. Historical identities
/// (`keepsake-outbox-N` and `keepsake-audit-legacy-N`) return
/// [`AuditEventDecodeError::LegacyEvent`] because v1 payloads do not carry the
/// current event identity and require an application-specific legacy decoder.
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use chrono::{TimeZone, Utc};
/// use keepsake::{ActorRef, AuditContext, AuditDecision, AuditEvent, AuditEventId,
///     AuditEventType, SubjectRef};
/// use keepsake_sqlx::{decode_audit_event, DovecoteAuditConfig};
///
/// let event = AuditEvent {
///     id: AuditEventId::from_uuid(uuid::Uuid::nil()),
///     event_type: AuditEventType::Apply,
///     at: Utc.timestamp_opt(1_700_000_000, 0).single().ok_or("timestamp")?,
///     actor: ActorRef::new("system", "example")?,
///     keepsake_id: uuid::Uuid::nil(),
///     subject: SubjectRef::new("account", "acct-1")?,
///     relation_id: uuid::Uuid::nil(),
///     decision: AuditDecision::Applied { duplicate_prevented: false },
///     context: AuditContext::default(),
/// };
/// let config = DovecoteAuditConfig::new("https://example.invalid/keepsake")?;
/// let stored = dovecote::NewEvent::builder(
///     dovecote::StreamName::new(config.stream())?,
///     dovecote::EventId::new(format!("keepsake-audit-{}", event.id.as_uuid()))?,
///     dovecote::EventSource::new(config.source())?,
///     dovecote::EventType::new(config.event_type())?,
/// )
/// .time(time::OffsetDateTime::from_unix_timestamp(event.at.timestamp())?)
/// .datacontenttype(dovecote::ContentType::new("application/json")?)
/// .data(dovecote::EventData::json(serde_json::to_vec(&event)?)?)
/// .build()?.into_stored()?;
/// assert_eq!(decode_audit_event(&config, &stored)?, event);
/// # Ok(())
/// # }
/// ```
pub fn decode_audit_event(
    config: &DovecoteAuditConfig,
    event: &dovecote::StoredEvent,
) -> Result<AuditEvent, AuditEventDecodeError> {
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

    let value: serde_json::Value = serde_json::from_slice(payload.as_bytes())?;
    let decoded: AuditEvent = serde_json::from_value(value)?;
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

    if chrono_to_dovecote_for_decode(decoded.at) != Some(event_time) {
        return Err(AuditEventDecodeError::InvalidEnvelope {
            field: "occurrence time",
        });
    }

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

fn chrono_to_dovecote_for_decode(value: DateTime<Utc>) -> Option<time::OffsetDateTime> {
    let nanos = value.timestamp_subsec_nanos();
    time::OffsetDateTime::from_unix_timestamp(value.timestamp())
        .ok()
        .and_then(|value| value.replace_nanosecond(nanos).ok())
        .map(|value| value.to_offset(time::UtcOffset::UTC))
}

/// Parses a stored lifecycle state token.
pub(super) fn parse_state(value: String) -> RepositoryResult<LifecycleState> {
    match value.as_str() {
        "applied" => Ok(LifecycleState::Applied),
        "revoked" => Ok(LifecycleState::Revoked),
        "expired" => Ok(LifecycleState::Expired),
        _ => Err(RepositoryError::InvalidLifecycleState { state: value }),
    }
}

/// Parses a UUID stored as text, mapping failures to a decode error.
///
/// Only the text-store backends keep UUIDs as strings; Postgres decodes the
/// native `uuid` type directly.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(super) fn parse_uuid(value: &str) -> RepositoryResult<Uuid> {
    Ok(Uuid::parse_str(value).map_err(|error| sqlx::Error::Decode(Box::new(error)))?)
}

/// Projects the materialized `expires_at` column from an expiry policy.
///
/// Postgres derives this inside SQL; the text-store backends compute it here so
/// the projection rule lives in exactly one place.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(super) const fn expires_at(expiry: &ExpiryPolicy) -> Option<DateTime<Utc>> {
    match expiry {
        ExpiryPolicy::At { timestamp } => Some(*timestamp),
        ExpiryPolicy::ManualOnly | ExpiryPolicy::WhenFulfilled { .. } => None,
    }
}

/// Builds the audit context for a command, defaulting the idempotency key attribute.
pub(super) fn audit_context_from_command(context: &CommandContext) -> AuditContext {
    let mut attributes = context.metadata.clone();
    if let Some(idempotency_key) = &context.idempotency_key {
        attributes
            .entry("idempotency_key".to_owned())
            .or_insert_with(|| idempotency_key.clone());
    }
    AuditContext { attributes }
}

/// Maps one typed occurrence to a validated Dovecote event with exact JSON
/// payload bytes. The application source is never invented by this adapter.
pub(super) fn dovecote_event(
    config: &DovecoteAuditConfig,
    event: &AuditEvent,
) -> RepositoryResult<dovecote::NewEvent> {
    let payload = serde_json::to_vec(event)?;
    let time = chrono_to_dovecote(event.at)?;
    let event_id = dovecote::EventId::new(format!("keepsake-audit-{}", event.id.as_uuid()))
        .map_err(RepositoryError::DovecoteValidation)?;
    let content_type = dovecote::ContentType::new("application/json")
        .map_err(RepositoryError::DovecoteValidation)?;
    dovecote::NewEvent::builder(
        config.stream.clone(),
        event_id,
        config.source.clone(),
        config.event_type.clone(),
    )
    .time(time)
    .datacontenttype(content_type)
    .data(dovecote::EventData::json(payload).map_err(RepositoryError::DovecoteValidation)?)
    .build()
    .map_err(RepositoryError::DovecoteValidation)
}

fn chrono_to_dovecote(value: DateTime<Utc>) -> RepositoryResult<time::OffsetDateTime> {
    let nanos = value.timestamp_subsec_nanos();
    time::OffsetDateTime::from_unix_timestamp(value.timestamp())
        .and_then(|value| value.replace_nanosecond(nanos))
        .map(|value| value.to_offset(time::UtcOffset::UTC))
        .map_err(|error| RepositoryError::TimestampOutOfRange {
            detail: error.to_string(),
        })
}

/// Builds the audit event for an apply or duplicate-prevented apply.
pub(super) fn apply_event(
    command: &ApplyKeepsake,
    keepsake: &Keepsake,
    duplicate_prevented: bool,
) -> AuditEvent {
    AuditEvent {
        id: command.audit_id,
        event_type: if duplicate_prevented {
            AuditEventType::DuplicateApply
        } else {
            AuditEventType::Apply
        },
        at: command.at,
        actor: command.context.actor.clone(),
        keepsake_id: keepsake.id(),
        subject: keepsake.subject().clone(),
        relation_id: command.relation_id,
        decision: AuditDecision::Applied {
            duplicate_prevented,
        },
        context: audit_context_from_command(&command.context),
    }
}

/// Reuses the original immutable occurrence for an exact command replay.
/// A changed command with the same identity is deliberately left untouched so
/// Dovecote can return its typed identity conflict.
pub(super) fn replay_event(existing: Option<AuditEvent>, candidate: AuditEvent) -> AuditEvent {
    let Some(existing) = existing else {
        return candidate;
    };

    let equivalent = existing.id == candidate.id
        && existing.actor == candidate.actor
        && existing.at == candidate.at
        && existing.keepsake_id == candidate.keepsake_id
        && existing.subject == candidate.subject
        && existing.relation_id == candidate.relation_id
        && existing.context == candidate.context
        && matches!(
            existing.event_type,
            AuditEventType::Apply | AuditEventType::DuplicateApply
        )
        && matches!(
            candidate.event_type,
            AuditEventType::Apply | AuditEventType::DuplicateApply
        );
    if equivalent { existing } else { candidate }
}

/// Builds the audit event for a revoke against the keepsake it resolved to.
///
/// Both the id-addressed and subject-addressed revoke commands resolve to a
/// single keepsake, so the event is constructed from the resolved row plus the
/// command's timestamp and context.
fn revoke_audit_event(
    id: AuditEventId,
    at: DateTime<Utc>,
    context: &CommandContext,
    keepsake: &Keepsake,
) -> AuditEvent {
    AuditEvent {
        id,
        event_type: AuditEventType::Revoke,
        at,
        actor: context.actor.clone(),
        keepsake_id: keepsake.id(),
        subject: keepsake.subject().clone(),
        relation_id: keepsake.relation_id(),
        decision: AuditDecision::Revoked,
        context: audit_context_from_command(context),
    }
}

/// Builds the audit event for an id-addressed revoke.
pub(super) fn revoke_event(command: &RevokeKeepsake, keepsake: &Keepsake) -> AuditEvent {
    revoke_audit_event(command.audit_id, command.at, &command.context, keepsake)
}

/// Builds the audit event for a subject-addressed revoke.
pub(super) fn revoke_by_subject_event(
    command: &RevokeBySubject,
    keepsake: &Keepsake,
) -> AuditEvent {
    revoke_audit_event(command.audit_id, command.at, &command.context, keepsake)
}

/// Builds the audit event for an expiry worker transition.
pub(super) fn expiry_event(
    at: DateTime<Utc>,
    cause: ExpiryCause,
    keepsake_id: KeepsakeId,
    relation_id: RelationId,
    subject_kind: impl Into<String>,
    subject_id: impl Into<String>,
) -> RepositoryResult<AuditEvent> {
    Ok(AuditEvent {
        id: AuditEventId::deterministic(
            format!("keepsake-expiry:{keepsake_id}:{at}:{cause:?}").as_bytes(),
        ),
        event_type: match cause {
            ExpiryCause::Timed => AuditEventType::TimedExpiry,
            ExpiryCause::Fulfilled => AuditEventType::FulfillmentExpiry,
        },
        at,
        actor: ActorRef::new("system", "keepsake-expiry")?,
        keepsake_id,
        subject: SubjectRef::new(subject_kind, subject_id)?,
        relation_id,
        decision: AuditDecision::Expired { cause },
        context: AuditContext::default(),
    })
}
