use std::collections::BTreeMap;

use keepsake::{
    ExpiryPolicy, Keepsake, KeepsakeRecord, RelationDefinition, RelationKey, SubjectRef, TenantId,
};
use sqlx::Row;
use time::{OffsetDateTime, UtcOffset};

#[cfg(feature = "fulfillment-counters")]
use crate::repository::FulfilledExpiryCandidate;
use crate::repository::support::{canonical_expiry_policy, parse_state, parse_uuid};
use crate::repository::{RepositoryResult, TimedExpiryCandidate};
pub(super) fn relation_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<RelationDefinition> {
    let expiry = canonical_expiry_policy(serde_json::from_str::<ExpiryPolicy>(
        row.try_get("expiry_policy")?,
    )?);
    Ok(RelationDefinition::new(
        TenantId::new(row.try_get::<String, _>("tenant_id")?)?,
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
    let expiry = canonical_expiry_policy(serde_json::from_str::<ExpiryPolicy>(
        row.try_get("expiry_policy")?,
    )?);
    Ok(KeepsakeRecord {
        tenant_id: TenantId::new(row.try_get::<String, _>("tenant_id")?)?,
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
    let expiry = canonical_expiry_policy(serde_json::from_str::<ExpiryPolicy>(
        row.try_get("relation_expiry_policy")?,
    )?);
    Ok(RelationDefinition::new(
        TenantId::new(row.try_get::<String, _>("tenant_id")?)?,
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
        expiry_policy: canonical_expiry_policy(serde_json::from_str(
            row.try_get("expiry_policy")?,
        )?),
    })
}

pub(super) fn parse_timestamp(value: &str) -> RepositoryResult<OffsetDateTime> {
    Ok(
        OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?
            .to_offset(UtcOffset::UTC),
    )
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn optional_timestamp(
    value: Option<String>,
) -> RepositoryResult<Option<OffsetDateTime>> {
    value.as_deref().map(parse_timestamp).transpose()
}

pub(super) fn format_timestamp(value: OffsetDateTime) -> String {
    let format = time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]Z"
    );
    value
        .to_offset(UtcOffset::UTC)
        .format(&format)
        .unwrap_or_default()
}
