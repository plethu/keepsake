use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use keepsake::{
    AuditEvent, ExpiryPolicy, Keepsake, KeepsakeRecord, RelationDefinition, RelationKey, SubjectRef,
};
use sqlx::Row;

use crate::repository::support::{parse_state, parse_uuid};
use crate::repository::{
    AuditOutboxRecord, FulfilledExpiryCandidate, RepositoryResult, TimedExpiryCandidate,
};
pub(super) fn relation_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_str::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
    Ok(RelationDefinition::new(
        parse_uuid(row.try_get("id")?)?,
        RelationKey::new(
            row.try_get::<String, _>("kind")?,
            row.try_get::<String, _>("key")?,
        )?,
        row.try_get("enabled")?,
        expiry,
    )?)
}

pub(super) fn keepsake_from_row(row: &sqlx::sqlite::SqliteRow) -> RepositoryResult<Keepsake> {
    let metadata = serde_json::from_str::<BTreeMap<String, String>>(row.try_get("metadata")?)?;
    let expiry = serde_json::from_str::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
    Ok(KeepsakeRecord {
        id: parse_uuid(row.try_get("id")?)?,
        subject: SubjectRef::new(
            row.try_get::<String, _>("subject_kind")?,
            row.try_get::<String, _>("subject_id")?,
        )?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        state: parse_state(row.try_get("state")?)?,
        expiry,
        applied_at: parse_timestamp(row.try_get("applied_at")?)?,
        expires_at: optional_timestamp(row.try_get("expires_at")?)?,
        fulfilled_at: optional_timestamp(row.try_get("fulfilled_at")?)?,
        revoked_at: optional_timestamp(row.try_get("revoked_at")?)?,
        metadata,
    }
    .try_into()?)
}

pub(super) fn relation_definition_from_active_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_str::<ExpiryPolicy>(row.try_get("relation_expiry_policy")?)?;
    Ok(RelationDefinition::new(
        parse_uuid(row.try_get("relation_definition_id")?)?,
        RelationKey::new(
            row.try_get::<String, _>("relation_kind")?,
            row.try_get::<String, _>("relation_key")?,
        )?,
        row.try_get("relation_enabled")?,
        expiry,
    )?)
}

pub(super) fn timed_expiry_candidate_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<TimedExpiryCandidate> {
    Ok(TimedExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        due_at: parse_timestamp(row.try_get("due_at")?)?,
    })
}

#[cfg(feature = "fulfillment-counters")]
pub(super) fn fulfilled_expiry_candidate_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<FulfilledExpiryCandidate> {
    Ok(FulfilledExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        expiry_policy: serde_json::from_str(row.try_get("expiry_policy")?)?,
    })
}

pub(super) fn outbox_record_from_sqlite_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<AuditOutboxRecord> {
    let payload = serde_json::from_str::<AuditEvent>(row.try_get("payload")?)?;
    let claimed_until = row
        .try_get::<Option<String>, _>("claimed_until")?
        .as_deref()
        .map(parse_timestamp)
        .transpose()?;
    let delivered_at = row
        .try_get::<Option<String>, _>("delivered_at")?
        .as_deref()
        .map(parse_timestamp)
        .transpose()?;
    Ok(AuditOutboxRecord {
        id: row.try_get("id")?,
        audit_event_id: row.try_get("audit_event_id")?,
        event_type: row.try_get("event_type")?,
        payload,
        claimed_by: row.try_get("claimed_by")?,
        claimed_until,
        delivered_at,
    })
}

pub(super) fn parse_timestamp(value: &str) -> RepositoryResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?
        .with_timezone(&Utc))
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn optional_timestamp(value: Option<String>) -> RepositoryResult<Option<DateTime<Utc>>> {
    value.as_deref().map(parse_timestamp).transpose()
}

pub(super) fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}
