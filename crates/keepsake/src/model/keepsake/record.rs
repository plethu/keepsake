use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{KeepsakeError, Result};
use crate::policy::ExpiryPolicy;

use super::{ExpiryCause, Keepsake, KeepsakeLifecycle, LifecycleState};
use crate::model::{KeepsakeId, RelationId, SubjectRef};

type SerdeResult<T, E> = core::result::Result<T, E>;

/// Flat storage and serde boundary record for keepsakes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepsakeRecord {
    /// Stable keepsake id.
    pub id: KeepsakeId,
    /// Application-owned subject reference.
    pub subject: SubjectRef,
    /// Relation definition id.
    pub relation_id: RelationId,
    /// Current lifecycle state.
    pub state: LifecycleState,
    /// Policy copied at apply time for deterministic replay.
    pub expiry: ExpiryPolicy,
    /// Timestamp when the keepsake was applied.
    pub applied_at: DateTime<Utc>,
    /// Denormalized timed expiry instant for efficient scans.
    pub expires_at: Option<DateTime<Utc>>,
    /// Timestamp when a fulfillment condition was observed as satisfied.
    pub fulfilled_at: Option<DateTime<Utc>>,
    /// Timestamp when the keepsake was revoked.
    pub revoked_at: Option<DateTime<Utc>>,
    /// Application metadata kept opaque by Keepsake.
    pub metadata: BTreeMap<String, String>,
}

impl TryFrom<KeepsakeRecord> for Keepsake {
    type Error = KeepsakeError;

    fn try_from(record: KeepsakeRecord) -> Result<Self> {
        record.subject.validate()?;
        record.expiry.validate()?;
        let expected_expires_at = record.expiry.timed_expiry();
        if record.expires_at != expected_expires_at {
            return Err(invalid_lifecycle("expires_at must match the expiry policy"));
        }

        let lifecycle = match record.state {
            LifecycleState::Applied => {
                if record.fulfilled_at.is_some() || record.revoked_at.is_some() {
                    return Err(invalid_lifecycle(
                        "applied keepsakes must not have terminal timestamps",
                    ));
                }
                KeepsakeLifecycle::Applied
            }
            LifecycleState::Revoked => {
                let Some(revoked_at) = record.revoked_at else {
                    return Err(invalid_lifecycle("revoked keepsakes require revoked_at"));
                };

                if record.fulfilled_at.is_some() {
                    return Err(invalid_lifecycle(
                        "revoked keepsakes must not have fulfilled_at",
                    ));
                }
                KeepsakeLifecycle::Revoked { revoked_at }
            }
            LifecycleState::Expired => match &record.expiry {
                ExpiryPolicy::ManualOnly => {
                    return Err(invalid_lifecycle("manual-only keepsakes cannot expire"));
                }
                ExpiryPolicy::At { timestamp } => {
                    if record.fulfilled_at.is_some() || record.revoked_at.is_some() {
                        return Err(invalid_lifecycle(
                            "timed expiry must not have revoked_at or fulfilled_at",
                        ));
                    }
                    KeepsakeLifecycle::Expired {
                        expired_at: *timestamp,
                        cause: ExpiryCause::Timed,
                    }
                }
                ExpiryPolicy::WhenFulfilled { .. } => {
                    let Some(fulfilled_at) = record.fulfilled_at else {
                        return Err(invalid_lifecycle(
                            "fulfillment expiry requires fulfilled_at",
                        ));
                    };

                    if record.revoked_at.is_some() {
                        return Err(invalid_lifecycle(
                            "fulfillment expiry must not have revoked_at",
                        ));
                    }
                    KeepsakeLifecycle::Expired {
                        expired_at: fulfilled_at,
                        cause: ExpiryCause::Fulfilled,
                    }
                }
            },
        };

        Ok(Self {
            id: record.id,
            subject: record.subject,
            relation_id: record.relation_id,
            expiry: record.expiry,
            applied_at: record.applied_at,
            lifecycle,
            metadata: record.metadata,
        })
    }
}

impl From<&Keepsake> for KeepsakeRecord {
    fn from(keepsake: &Keepsake) -> Self {
        Self {
            id: keepsake.id,
            subject: keepsake.subject.clone(),
            relation_id: keepsake.relation_id,
            state: keepsake.state(),
            expiry: keepsake.expiry.clone(),
            applied_at: keepsake.applied_at,
            expires_at: keepsake.expires_at(),
            fulfilled_at: keepsake.fulfilled_at(),
            revoked_at: keepsake.revoked_at(),
            metadata: keepsake.metadata.clone(),
        }
    }
}

impl Serialize for Keepsake {
    fn serialize<S>(&self, serializer: S) -> SerdeResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        KeepsakeRecord::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Keepsake {
    fn deserialize<D>(deserializer: D) -> SerdeResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        KeepsakeRecord::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

const fn invalid_lifecycle(reason: &'static str) -> KeepsakeError {
    KeepsakeError::InvalidKeepsakeLifecycle { reason }
}
