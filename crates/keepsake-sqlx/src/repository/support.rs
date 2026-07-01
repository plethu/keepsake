//! Dialect-independent domain helpers shared across SQL backends.
//!
//! Everything here is pure model logic with no SQL text or driver coupling.
//! Each backend module owns its own SQL strings, placeholder syntax, and row
//! decoding; this module owns the parts of those flows that do not vary by
//! dialect so they are written and tested once.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use keepsake::{
    ActiveRelation, ActorRef, ApplyKeepsake, AuditContext, AuditDecision, AuditEvent,
    AuditEventType, CommandContext, Keepsake, LifecycleState, RelationId, RelationKey,
    RevokeBySubject, RevokeKeepsake, SubjectRef,
};
use uuid::Uuid;

#[cfg(any(feature = "mysql", feature = "sqlite"))]
use keepsake::ExpiryPolicy;

use super::{AuditEventRecord, RepositoryError, RepositoryResult};

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

/// Builds the audit event for an apply or duplicate-prevented apply.
pub(super) fn apply_event(
    command: &ApplyKeepsake,
    keepsake: &Keepsake,
    duplicate_prevented: bool,
) -> AuditEvent {
    AuditEvent {
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

/// Decoded primitive columns of a stored audit event row.
///
/// Backends decode their own row types into these dialect-independent parts so
/// the event reconstruction below is written once.
pub(super) struct AuditEventParts {
    pub id: i64,
    pub event_type: String,
    pub at: DateTime<Utc>,
    pub actor_kind: String,
    pub actor_id: String,
    pub keepsake_id: Uuid,
    pub subject_kind: String,
    pub subject_id: String,
    pub relation_id: Uuid,
    pub decision: AuditDecision,
    pub attributes: BTreeMap<String, String>,
}

/// Reconstructs a stored audit event, rejecting unknown event type labels.
pub(super) fn audit_event_record(parts: AuditEventParts) -> RepositoryResult<AuditEventRecord> {
    let event_type = AuditEventType::from_storage_label(&parts.event_type).ok_or_else(|| {
        RepositoryError::InvalidAuditEventType {
            event_type: parts.event_type.clone(),
        }
    })?;
    Ok(AuditEventRecord {
        id: parts.id,
        event: AuditEvent {
            event_type,
            at: parts.at,
            actor: ActorRef::new(parts.actor_kind, parts.actor_id)?,
            keepsake_id: parts.keepsake_id,
            subject: SubjectRef::new(parts.subject_kind, parts.subject_id)?,
            relation_id: parts.relation_id,
            decision: parts.decision,
            context: AuditContext {
                attributes: parts.attributes,
            },
        },
    })
}

pub(super) fn filter_active_relations_by_ids(
    active: Vec<ActiveRelation>,
    relation_ids: &[RelationId],
) -> Vec<ActiveRelation> {
    if relation_ids.is_empty() {
        return Vec::new();
    }

    let requested = relation_ids.iter().copied().collect::<BTreeSet<_>>();
    active
        .into_iter()
        .filter(|active| requested.contains(&active.relation().id))
        .collect()
}

pub(super) fn filter_active_relations_by_keys(
    active: Vec<ActiveRelation>,
    keys: &[RelationKey],
) -> Vec<ActiveRelation> {
    if keys.is_empty() {
        return Vec::new();
    }

    let requested = keys.iter().collect::<BTreeSet<_>>();
    active
        .into_iter()
        .filter(|active| requested.contains(&active.relation().key))
        .collect()
}

/// Builds the audit event for a revoke against the keepsake it resolved to.
///
/// Both the id-addressed and subject-addressed revoke commands resolve to a
/// single keepsake, so the event is constructed from the resolved row plus the
/// command's timestamp and context.
fn revoke_audit_event(
    at: DateTime<Utc>,
    context: &CommandContext,
    keepsake: &Keepsake,
) -> AuditEvent {
    AuditEvent {
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
    revoke_audit_event(command.at, &command.context, keepsake)
}

/// Builds the audit event for a subject-addressed revoke.
pub(super) fn revoke_by_subject_event(
    command: &RevokeBySubject,
    keepsake: &Keepsake,
) -> AuditEvent {
    revoke_audit_event(command.at, &command.context, keepsake)
}
