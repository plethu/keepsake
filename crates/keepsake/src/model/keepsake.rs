use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{KeepsakeError, Result};
use crate::policy::ExpiryPolicy;

use super::{KeepsakeId, RelationDefinition, RelationId, SubjectRef};

/// Current lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    /// The relation is currently active.
    Applied,
    /// The relation was explicitly revoked.
    Revoked,
    /// The relation expired by policy.
    Expired,
}

/// Terminal expiry cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryCause {
    /// A fixed timestamp policy became due.
    Timed,
    /// A fulfillment policy became satisfied.
    Fulfilled,
}

/// Lifecycle-specific state carried by a keepsake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum KeepsakeLifecycle {
    /// The relation is currently active.
    Applied,
    /// The relation was explicitly revoked.
    Revoked {
        /// Revocation timestamp.
        revoked_at: DateTime<Utc>,
    },
    /// The relation expired by policy.
    Expired {
        /// Expiry timestamp.
        expired_at: DateTime<Utc>,
        /// Expiry cause.
        cause: ExpiryCause,
    },
}

/// Policy-bearing relation assignment from an opaque subject to a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keepsake {
    /// Stable keepsake id.
    id: KeepsakeId,
    /// Application-owned subject reference.
    subject: SubjectRef,
    /// Relation definition id.
    relation_id: RelationId,
    /// Policy copied at apply time for deterministic replay.
    expiry: ExpiryPolicy,
    /// Timestamp when the keepsake was applied.
    applied_at: DateTime<Utc>,
    /// Lifecycle-specific state.
    lifecycle: KeepsakeLifecycle,
    /// Application metadata kept opaque by Keepsake.
    metadata: BTreeMap<String, String>,
}

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

impl Keepsake {
    /// Creates a new active keepsake.
    pub fn applied(
        id: KeepsakeId,
        subject: SubjectRef,
        relation: &RelationDefinition,
        applied_at: DateTime<Utc>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self> {
        subject.validate()?;
        relation.expiry.validate()?;
        Ok(Self {
            id,
            subject,
            relation_id: relation.id,
            expiry: relation.expiry.clone(),
            applied_at,
            lifecycle: KeepsakeLifecycle::Applied,
            metadata,
        })
    }

    /// Returns the stable keepsake id.
    #[must_use]
    pub const fn id(&self) -> KeepsakeId {
        self.id
    }

    /// Returns the subject reference.
    #[must_use]
    pub const fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    /// Returns the relation definition id.
    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    /// Returns the copied expiry policy.
    #[must_use]
    pub const fn expiry(&self) -> &ExpiryPolicy {
        &self.expiry
    }

    /// Returns the application timestamp.
    #[must_use]
    pub const fn applied_at(&self) -> DateTime<Utc> {
        self.applied_at
    }

    /// Returns opaque application metadata.
    #[must_use]
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Returns the cheap lifecycle discriminant.
    #[must_use]
    pub const fn state(&self) -> LifecycleState {
        match self.lifecycle {
            KeepsakeLifecycle::Applied => LifecycleState::Applied,
            KeepsakeLifecycle::Revoked { .. } => LifecycleState::Revoked,
            KeepsakeLifecycle::Expired { .. } => LifecycleState::Expired,
        }
    }

    /// Returns the typed lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &KeepsakeLifecycle {
        &self.lifecycle
    }

    /// Returns true when the keepsake is active.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self.lifecycle, KeepsakeLifecycle::Applied)
    }

    /// Returns true when the keepsake is revoked.
    #[must_use]
    pub const fn is_revoked(&self) -> bool {
        matches!(self.lifecycle, KeepsakeLifecycle::Revoked { .. })
    }

    /// Returns true when the keepsake is expired.
    #[must_use]
    pub const fn is_expired(&self) -> bool {
        matches!(self.lifecycle, KeepsakeLifecycle::Expired { .. })
    }

    /// Returns the scheduled timed expiry timestamp, when applicable.
    #[must_use]
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expiry.timed_expiry()
    }

    /// Returns the terminal timestamp for revoked or expired keepsakes.
    #[must_use]
    pub const fn ended_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Applied => None,
            KeepsakeLifecycle::Revoked { revoked_at } => Some(revoked_at),
            KeepsakeLifecycle::Expired { expired_at, .. } => Some(expired_at),
        }
    }

    /// Returns the revocation timestamp for revoked keepsakes.
    #[must_use]
    pub const fn revoked_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Revoked { revoked_at } => Some(revoked_at),
            KeepsakeLifecycle::Applied | KeepsakeLifecycle::Expired { .. } => None,
        }
    }

    /// Returns the expiry timestamp for expired keepsakes.
    #[must_use]
    pub const fn expired_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Expired { expired_at, .. } => Some(expired_at),
            KeepsakeLifecycle::Applied | KeepsakeLifecycle::Revoked { .. } => None,
        }
    }

    /// Returns the fulfillment timestamp for fulfillment-caused expiry.
    #[must_use]
    pub const fn fulfilled_at(&self) -> Option<DateTime<Utc>> {
        match self.lifecycle {
            KeepsakeLifecycle::Expired {
                expired_at,
                cause: ExpiryCause::Fulfilled,
            } => Some(expired_at),
            KeepsakeLifecycle::Applied
            | KeepsakeLifecycle::Revoked { .. }
            | KeepsakeLifecycle::Expired {
                cause: ExpiryCause::Timed,
                ..
            } => None,
        }
    }
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
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        KeepsakeRecord::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Keepsake {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
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
