//! Canonical lifecycle storage values and immutable audit occurrences.
use super::DovecoteAuditConfig;
use crate::repository::{RepositoryError, RepositoryResult};
use keepsake::{
    ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent, AuditEventId, AuditEventType,
    AuditPayloadSchemaVersion, CommandContext, ExpiryCause, ExpiryPolicy, Keepsake, KeepsakeId,
    LifecycleState, RelationDefinition, RelationId, RevokeBySubject, RevokeKeepsake, SubjectRef,
};
use time::OffsetDateTime;
#[cfg(any(feature = "mysql", feature = "sqlite"))]
use uuid::Uuid;
/// Parses a stored lifecycle state token.
pub(in crate::repository) fn parse_state(value: String) -> RepositoryResult<LifecycleState> {
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
pub(in crate::repository) fn parse_uuid(value: &str) -> RepositoryResult<Uuid> {
    Ok(Uuid::parse_str(value).map_err(|error| sqlx::Error::Decode(Box::new(error)))?)
}

/// Projects the materialized `expires_at` column from an expiry policy.
///
/// All backends compute this from the canonical policy before persistence so
/// database-specific timestamp rounding cannot split the two representations.
pub(in crate::repository) const fn expires_at(expiry: &ExpiryPolicy) -> Option<OffsetDateTime> {
    match expiry {
        ExpiryPolicy::At { timestamp } => Some(*timestamp),
        ExpiryPolicy::ManualOnly | ExpiryPolicy::WhenFulfilled { .. } => None,
    }
}

/// Builds the audit context for a command, defaulting the idempotency key attribute.
pub(in crate::repository) fn audit_context_from_command(context: &CommandContext) -> AuditContext {
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
pub(in crate::repository) fn dovecote_event(
    config: &DovecoteAuditConfig,
    event: &AuditEvent,
) -> RepositoryResult<dovecote::NewEvent> {
    event.validate_command()?;
    let payload = serde_json::to_vec(event)?;
    let time = time_to_dovecote(event.at)?;
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

/// Canonicalises occurrence timestamps to the precision shared by
/// `PostgreSQL`, `MySQL`, `SQLite`, and Dovecote's durable event contract.
pub(in crate::repository) fn canonical_timestamp(value: OffsetDateTime) -> OffsetDateTime {
    // Remove only the submicrosecond remainder; it cannot exceed the nanosecond value.
    let micros = value
        .nanosecond()
        .saturating_sub(value.nanosecond() % 1_000);
    value.replace_nanosecond(micros).unwrap_or(value)
}

pub(in crate::repository) fn canonical_expiry_policy(policy: ExpiryPolicy) -> ExpiryPolicy {
    match policy {
        ExpiryPolicy::At { timestamp } => ExpiryPolicy::At {
            timestamp: canonical_timestamp(timestamp),
        },
        policy => policy,
    }
}

/// Canonicalises timestamps embedded in a relation before storing its policy
/// beside microsecond-precision SQL timestamp columns.
pub(in crate::repository) fn canonical_relation(
    relation: &RelationDefinition,
) -> RelationDefinition {
    let mut relation = relation.clone();
    relation.expiry = canonical_expiry_policy(relation.expiry);
    relation
}

/// Converts Keepsake's domain-owned tenant value at the Dovecote adapter
/// boundary. The two crates intentionally do not share a public identity type.
pub(in crate::repository) fn dovecote_tenant_id(
    tenant_id: &keepsake::TenantId,
) -> RepositoryResult<dovecote::TenantId> {
    dovecote::TenantId::new(tenant_id.as_str().to_owned())
        .map_err(RepositoryError::DovecoteValidation)
}

fn time_to_dovecote(value: OffsetDateTime) -> RepositoryResult<time::OffsetDateTime> {
    let nanos = value.nanosecond();
    time::OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
        .and_then(|value| value.replace_nanosecond(nanos))
        .map(|value| value.to_offset(time::UtcOffset::UTC))
        .map_err(|error| RepositoryError::TimestampOutOfRange {
            detail: error.to_string(),
        })
}

/// Builds the audit event for an apply or duplicate-prevented apply.
pub(in crate::repository) fn apply_event(
    command: &ApplyKeepsake,
    keepsake: &Keepsake,
    duplicate_prevented: bool,
) -> AuditEvent {
    AuditEvent {
        command: Some(keepsake::LifecycleCommand::Apply(command.clone())),
        schema_version: AuditPayloadSchemaVersion::CURRENT,
        tenant_id: command.tenant_id.clone(),
        id: command.audit_id,
        event_type: if duplicate_prevented {
            AuditEventType::DuplicateApply
        } else {
            AuditEventType::Apply
        },
        at: canonical_timestamp(command.at),
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

/// Builds the audit event for a revoke against the keepsake it resolved to.
///
/// Both the id-addressed and subject-addressed revoke commands resolve to a
/// single keepsake, so the event is constructed from the resolved row plus the
/// command's timestamp and context.
fn revoke_audit_event(
    id: AuditEventId,
    at: OffsetDateTime,
    context: &CommandContext,
    keepsake: &Keepsake,
) -> AuditEvent {
    AuditEvent {
        command: None,
        schema_version: AuditPayloadSchemaVersion::CURRENT,
        tenant_id: keepsake.tenant_id().clone(),
        id,
        event_type: AuditEventType::Revoke,
        at: canonical_timestamp(at),
        actor: context.actor.clone(),
        keepsake_id: keepsake.id(),
        subject: keepsake.subject().clone(),
        relation_id: keepsake.relation_id(),
        decision: AuditDecision::Revoked,
        context: audit_context_from_command(context),
    }
}

/// Builds the audit event for an id-addressed revoke.
pub(in crate::repository) fn revoke_event(
    command: &RevokeKeepsake,
    keepsake: &Keepsake,
) -> AuditEvent {
    let mut event = revoke_audit_event(command.audit_id, command.at, &command.context, keepsake);
    event.command = Some(keepsake::LifecycleCommand::Revoke(command.clone()));
    event
}

/// Builds the audit event for a subject-addressed revoke.
pub(in crate::repository) fn revoke_by_subject_event(
    command: &RevokeBySubject,
    keepsake: &Keepsake,
) -> AuditEvent {
    let mut event = revoke_audit_event(command.audit_id, command.at, &command.context, keepsake);
    event.command = Some(keepsake::LifecycleCommand::RevokeBySubject(command.clone()));
    event
}

/// Builds the audit event for an expiry worker transition.
pub(in crate::repository) fn expiry_event(
    at: OffsetDateTime,
    cause: ExpiryCause,
    tenant_id: keepsake::TenantId,
    keepsake_id: KeepsakeId,
    relation_id: RelationId,
    subject_kind: impl Into<String>,
    subject_id: impl Into<String>,
) -> RepositoryResult<AuditEvent> {
    let at = canonical_timestamp(at);
    Ok(AuditEvent {
        command: None,
        schema_version: AuditPayloadSchemaVersion::CURRENT,
        tenant_id,
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
