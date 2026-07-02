use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDateTime, Utc};
use keepsake::{
    AuditEvent, ExpiryPolicy, Keepsake, KeepsakeRecord, RelationDefinition, RelationKey, SubjectRef,
};
use sqlx::Row;

use crate::repository::support::{parse_state, parse_uuid};
use crate::repository::{
    AuditOutboxRecord, FulfilledExpiryCandidate, RepositoryResult, TimedExpiryCandidate,
};
pub(super) fn relation_from_row(
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
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

pub(super) fn outbox_record_from_mysql_row(
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<AuditOutboxRecord> {
    let payload = serde_json::from_value::<AuditEvent>(row.try_get("payload")?)?;
    Ok(AuditOutboxRecord {
        id: row.try_get("id")?,
        audit_event_id: row.try_get("audit_event_id")?,
        event_type: row.try_get("event_type")?,
        payload,
        claimed_by: row.try_get("claimed_by")?,
        claimed_until: optional_utc_timestamp(row.try_get("claimed_until")?),
        delivered_at: optional_utc_timestamp(row.try_get("delivered_at")?),
    })
}

pub(super) fn keepsake_from_row(row: &sqlx::mysql::MySqlRow) -> RepositoryResult<Keepsake> {
    let metadata = serde_json::from_value::<BTreeMap<String, String>>(row.try_get("metadata")?)?;
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.try_get("expiry_policy")?)?;
    Ok(KeepsakeRecord {
        id: parse_uuid(row.try_get("id")?)?,
        subject: SubjectRef::new(
            row.try_get::<String, _>("subject_kind")?,
            row.try_get::<String, _>("subject_id")?,
        )?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        state: parse_state(row.try_get("state")?)?,
        expiry,
        applied_at: utc_timestamp(row.try_get("applied_at")?),
        expires_at: optional_utc_timestamp(row.try_get("expires_at")?),
        fulfilled_at: optional_utc_timestamp(row.try_get("fulfilled_at")?),
        revoked_at: optional_utc_timestamp(row.try_get("revoked_at")?),
        metadata,
    }
    .try_into()?)
}

pub(super) fn relation_definition_from_active_row(
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.try_get("relation_expiry_policy")?)?;
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
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<TimedExpiryCandidate> {
    Ok(TimedExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        due_at: utc_timestamp(row.try_get("due_at")?),
    })
}

#[cfg(feature = "fulfillment-counters")]
pub(super) fn fulfilled_expiry_candidate_from_row(
    row: &sqlx::mysql::MySqlRow,
) -> RepositoryResult<FulfilledExpiryCandidate> {
    Ok(FulfilledExpiryCandidate {
        keepsake_id: parse_uuid(row.try_get("keepsake_id")?)?,
        relation_id: parse_uuid(row.try_get("relation_id")?)?,
        subject_kind: row.try_get("subject_kind")?,
        subject_id: row.try_get("subject_id")?,
        expiry_policy: serde_json::from_value(row.try_get("expiry_policy")?)?,
    })
}

#[cfg(feature = "fulfillment-counters")]
pub(super) const fn naive_timestamp(value: DateTime<Utc>) -> NaiveDateTime {
    value.naive_utc()
}

pub(super) const fn utc_timestamp(value: NaiveDateTime) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(value, Utc)
}

pub(super) fn optional_utc_timestamp(value: Option<NaiveDateTime>) -> Option<DateTime<Utc>> {
    value.map(utc_timestamp)
}
