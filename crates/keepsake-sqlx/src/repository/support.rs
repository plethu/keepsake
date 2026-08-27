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
