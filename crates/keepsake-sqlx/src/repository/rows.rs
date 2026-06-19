use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use keepsake::{
    ExpiryPolicy, Keepsake, KeepsakeRecord, LifecycleState, RelationDefinition, RelationKey,
    SubjectRef,
};
use sqlx::FromRow;
use uuid::Uuid;

use super::{ActiveRelation, RepositoryError, RepositoryResult};

#[derive(Debug, FromRow)]
pub(super) struct RelationRow {
    id: Uuid,
    kind: String,
    key: String,
    enabled: bool,
    expiry_policy: serde_json::Value,
}

impl RelationRow {
    pub(super) fn try_into_relation(self) -> RepositoryResult<RelationDefinition> {
        let expiry = serde_json::from_value::<ExpiryPolicy>(self.expiry_policy)?;
        Ok(RelationDefinition::new(
            self.id,
            RelationKey::new(self.kind, self.key)?,
            self.enabled,
            expiry,
        )?)
    }
}

#[derive(Debug, FromRow)]
pub(super) struct AppliedKeepsakeRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
}

impl AppliedKeepsakeRow {
    pub(super) fn try_into_keepsake(self) -> RepositoryResult<Keepsake> {
        row_into_keepsake(KeepsakeRow {
            id: self.id,
            subject_kind: self.subject_kind,
            subject_id: self.subject_id,
            relation_id: self.relation_id,
            state: self.state,
            expiry_policy: self.expiry_policy,
            applied_at: self.applied_at,
            expires_at: self.expires_at,
            fulfilled_at: self.fulfilled_at,
            revoked_at: self.revoked_at,
            metadata: self.metadata,
        })
    }
}

#[derive(Debug, FromRow)]
pub(super) struct AppliedKeepsakeWriteRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
    pub(super) duplicate_prevented: bool,
}

impl AppliedKeepsakeWriteRow {
    pub(super) fn try_into_parts(self) -> RepositoryResult<(Keepsake, bool)> {
        let duplicate_prevented = self.duplicate_prevented;
        let keepsake = row_into_keepsake(KeepsakeRow {
            id: self.id,
            subject_kind: self.subject_kind,
            subject_id: self.subject_id,
            relation_id: self.relation_id,
            state: self.state,
            expiry_policy: self.expiry_policy,
            applied_at: self.applied_at,
            expires_at: self.expires_at,
            fulfilled_at: self.fulfilled_at,
            revoked_at: self.revoked_at,
            metadata: self.metadata,
        })?;
        Ok((keepsake, duplicate_prevented))
    }
}

#[derive(Debug, FromRow)]
pub(super) struct ActiveRelationRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
    relation_definition_id: Uuid,
    relation_kind: String,
    relation_key: String,
    relation_enabled: bool,
    relation_expiry_policy: serde_json::Value,
}

impl ActiveRelationRow {
    pub(super) fn try_into_active_relation(self) -> RepositoryResult<ActiveRelation> {
        let relation_expiry = serde_json::from_value::<ExpiryPolicy>(self.relation_expiry_policy)?;
        let keepsake = row_into_keepsake(KeepsakeRow {
            id: self.id,
            subject_kind: self.subject_kind,
            subject_id: self.subject_id,
            relation_id: self.relation_id,
            state: self.state,
            expiry_policy: self.expiry_policy,
            applied_at: self.applied_at,
            expires_at: self.expires_at,
            fulfilled_at: self.fulfilled_at,
            revoked_at: self.revoked_at,
            metadata: self.metadata,
        })?;
        Ok(ActiveRelation {
            keepsake,
            relation: RelationDefinition::new(
                self.relation_definition_id,
                RelationKey::new(self.relation_kind, self.relation_key)?,
                self.relation_enabled,
                relation_expiry,
            )?,
        })
    }
}

struct KeepsakeRow {
    id: Uuid,
    subject_kind: String,
    subject_id: String,
    relation_id: Uuid,
    state: String,
    expiry_policy: serde_json::Value,
    applied_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    fulfilled_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    metadata: serde_json::Value,
}

fn row_into_keepsake(row: KeepsakeRow) -> RepositoryResult<Keepsake> {
    let expiry = serde_json::from_value::<ExpiryPolicy>(row.expiry_policy)?;
    let metadata = serde_json::from_value::<BTreeMap<String, String>>(row.metadata)?;
    Ok(KeepsakeRecord {
        id: row.id,
        subject: SubjectRef {
            kind: row.subject_kind,
            id: row.subject_id,
        },
        relation_id: row.relation_id,
        state: parse_state(row.state)?,
        expiry,
        applied_at: row.applied_at,
        expires_at: row.expires_at,
        fulfilled_at: row.fulfilled_at,
        revoked_at: row.revoked_at,
        metadata,
    }
    .try_into()?)
}

pub(super) fn parse_state(value: String) -> RepositoryResult<LifecycleState> {
    match value.as_str() {
        "applied" => Ok(LifecycleState::Applied),
        "revoked" => Ok(LifecycleState::Revoked),
        "expired" => Ok(LifecycleState::Expired),
        _ => Err(RepositoryError::InvalidLifecycleState { state: value }),
    }
}
