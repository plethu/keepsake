//! Typed domain model for relation assignments.

use std::{collections::BTreeMap, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KeepsakeError, Result};
use crate::policy::ExpiryPolicy;

/// Stable identifier for a keepsake row.
pub type KeepsakeId = Uuid;

/// Stable identifier for a relation definition.
pub type RelationId = Uuid;

/// Opaque application subject identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SubjectRef {
    /// Application-owned subject kind, such as `user`, `account`, or `device`.
    pub kind: String,
    /// Application-owned subject id.
    pub id: String,
}

impl SubjectRef {
    /// Builds a validated subject reference.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Result<Self> {
        let subject = Self {
            kind: kind.into(),
            id: id.into(),
        };
        subject.validate()?;
        Ok(subject)
    }

    /// Validates the subject reference.
    pub fn validate(&self) -> Result<()> {
        validate_not_empty("subject.kind", &self.kind)?;
        validate_not_empty("subject.id", &self.id)
    }
}

/// Application-owned actor metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActorRef {
    /// Actor kind, such as `user`, `system`, or `job`.
    pub kind: String,
    /// Actor id.
    pub id: String,
}

impl ActorRef {
    /// Builds a validated actor reference.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Result<Self> {
        let actor = Self {
            kind: kind.into(),
            id: id.into(),
        };
        validate_not_empty("actor.kind", &actor.kind)?;
        validate_not_empty("actor.id", &actor.id)?;
        Ok(actor)
    }
}

/// Human-meaningful relation identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RelationKey {
    /// Relation kind, such as `tag`, `sanction`, `entitlement`, or `feature_gate`.
    pub kind: RelationKind,
    /// Relation name within the kind.
    pub name: RelationName,
}

impl RelationKey {
    /// Builds a validated relation key from dynamic components.
    pub fn new(kind: impl Into<String>, name: impl Into<String>) -> Result<Self> {
        let relation = Self {
            kind: RelationKind::new(kind)?,
            name: RelationName::new(name)?,
        };
        Ok(relation)
    }

    /// Validates the relation key.
    pub fn validate(&self) -> Result<()> {
        self.kind.validate()?;
        self.name.validate()
    }

    /// Returns the relation kind as a string slice.
    #[must_use]
    pub fn kind(&self) -> &str {
        self.kind.as_str()
    }

    /// Returns the relation name as a string slice.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

/// Relation category, such as `tag`, `sanction`, `entitlement`, or `feature_gate`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RelationKind(String);

impl RelationKind {
    /// Builds a validated relation kind.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_not_empty("relation.kind", &value)?;
        Ok(Self(value))
    }

    /// Validates the relation kind.
    pub fn validate(&self) -> Result<()> {
        validate_not_empty("relation.kind", &self.0)
    }

    /// Returns the relation kind as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RelationKind {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RelationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Relation name within a relation kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RelationName(String);

impl RelationName {
    /// Builds a validated relation name.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_not_empty("relation.name", &value)?;
        Ok(Self(value))
    }

    /// Validates the relation name.
    pub fn validate(&self) -> Result<()> {
        validate_not_empty("relation.name", &self.0)
    }

    /// Returns the relation name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RelationName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RelationName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Static relation identity for application-owned relation catalogues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StaticRelationKey {
    /// Relation kind.
    pub kind: &'static str,
    /// Relation name within the kind.
    pub name: &'static str,
}

impl StaticRelationKey {
    /// Builds a static relation key.
    #[must_use]
    pub const fn new(kind: &'static str, name: &'static str) -> Self {
        Self { kind, name }
    }

    /// Converts this static key into a validated owned relation key.
    pub fn to_relation_key(self) -> Result<RelationKey> {
        RelationKey::new(self.kind, self.name)
    }
}

/// Configured relation definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationDefinition {
    /// Stable relation id.
    pub id: RelationId,
    /// Human-meaningful relation identity.
    pub key: RelationKey,
    /// Whether new commands and automatic lifecycle transitions may run.
    pub enabled: bool,
    /// Expiry policy applied to keepsakes of this relation.
    pub expiry: ExpiryPolicy,
}

impl RelationDefinition {
    /// Builds a validated relation definition.
    pub fn new(
        id: RelationId,
        key: RelationKey,
        enabled: bool,
        expiry: ExpiryPolicy,
    ) -> Result<Self> {
        expiry.validate()?;
        Ok(Self {
            id,
            key,
            enabled,
            expiry,
        })
    }

    /// Builds an enabled relation definition.
    pub fn enabled(id: RelationId, key: RelationKey, expiry: ExpiryPolicy) -> Result<Self> {
        Self::new(id, key, true, expiry)
    }

    /// Builds a disabled relation definition.
    pub fn disabled(id: RelationId, key: RelationKey, expiry: ExpiryPolicy) -> Result<Self> {
        Self::new(id, key, false, expiry)
    }

    /// Builds a relation definition from a typed relation spec.
    pub fn from_spec<Spec>(at: DateTime<Utc>) -> Result<Self>
    where
        Spec: RelationSpec,
    {
        Self::new(
            Spec::ID,
            Spec::KEY.to_relation_key()?,
            Spec::ENABLED,
            Spec::expiry(at),
        )
    }
}

/// Compile-time relation definition owned by application code.
///
/// Implement this on zero-sized marker types to define a typed relation
/// catalogue and avoid repeating natural-key strings throughout call sites.
pub trait RelationSpec {
    /// Stable relation id.
    const ID: RelationId;
    /// Human-meaningful static relation key.
    const KEY: StaticRelationKey;
    /// Whether the relation should be enabled when materialized.
    const ENABLED: bool = true;

    /// Expiry policy for this relation at materialization time.
    fn expiry(at: DateTime<Utc>) -> ExpiryPolicy;
}

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

/// Policy-bearing relation assignment from an opaque subject to a relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Keepsake {
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
    #[must_use]
    pub fn applied(
        id: KeepsakeId,
        subject: SubjectRef,
        relation: &RelationDefinition,
        applied_at: DateTime<Utc>,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        let expires_at = relation.expiry.timed_expiry();
        Self {
            id,
            subject,
            relation_id: relation.id,
            state: LifecycleState::Applied,
            expiry: relation.expiry.clone(),
            applied_at,
            expires_at,
            fulfilled_at: None,
            revoked_at: None,
            metadata,
        }
    }

    /// Returns true when the keepsake is active.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self.state, LifecycleState::Applied)
    }
}

/// Snapshot of application-owned fulfillment state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FulfillmentSnapshot {
    /// Numeric counters keyed by policy name.
    pub counters: BTreeMap<String, i64>,
    /// Checklist item completion keyed by item name.
    pub checklist: BTreeMap<String, bool>,
}

impl FulfillmentSnapshot {
    /// Returns an empty snapshot.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Adds a counter value.
    #[must_use]
    pub fn with_counter(mut self, key: impl Into<String>, value: i64) -> Self {
        self.counters.insert(key.into(), value);
        self
    }

    /// Adds a checklist item value.
    #[must_use]
    pub fn with_check(mut self, key: impl Into<String>, complete: bool) -> Self {
        self.checklist.insert(key.into(), complete);
        self
    }
}

pub(crate) fn validate_not_empty(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(KeepsakeError::EmptyIdentifier { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    type TestResult<T> = std::result::Result<T, TestError>;

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Chrono(#[from] chrono::ParseError),

        #[error(transparent)]
        Keepsake(#[from] KeepsakeError),
    }

    #[test]
    fn relation_definition_enabled_and_disabled_helpers_set_state() -> Result<()> {
        let key = RelationKey::new("tag", "trusted")?;
        let enabled =
            RelationDefinition::enabled(Uuid::nil(), key.clone(), ExpiryPolicy::ManualOnly)?;
        let disabled = RelationDefinition::disabled(Uuid::nil(), key, ExpiryPolicy::ManualOnly)?;

        assert!(enabled.enabled);
        assert!(!disabled.enabled);
        Ok(())
    }

    #[test]
    fn relation_key_components_validate_independently() {
        assert_eq!(
            RelationKind::new(" ").map_err(|error| error.to_string()),
            Err("relation.kind must not be empty".to_owned())
        );
        assert_eq!(
            RelationName::new(" ").map_err(|error| error.to_string()),
            Err("relation.name must not be empty".to_owned())
        );
    }

    #[test]
    fn relation_key_components_format_for_logs_and_labels() -> Result<()> {
        let kind = RelationKind::new("sanction")?;
        let name = RelationName::new("mute_24h")?;

        assert_eq!(kind.as_ref(), "sanction");
        assert_eq!(kind.to_string(), "sanction");
        assert_eq!(name.as_ref(), "mute_24h");
        assert_eq!(name.to_string(), "mute_24h");
        Ok(())
    }

    crate::relation_spec! {
        struct TrustedTag {
            id: 0;
            key: ("tag", "trusted");
            expiry(_at) => ExpiryPolicy::ManualOnly;
        }
    }

    crate::relation_spec! {
        struct DisabledTimedSanction {
            id: 0x018f_0000_0000_7000_8000_0000_0000_0003;
            key: ("sanction", "review_hold");
            enabled: false;
            expiry(at) => ExpiryPolicy::At { timestamp: at };
        }
    }

    #[test]
    fn relation_definition_can_be_built_from_spec() -> TestResult<()> {
        let definition = RelationDefinition::from_spec::<TrustedTag>(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .map(|timestamp| timestamp.with_timezone(&Utc))?,
        )?;

        assert_eq!(definition.id, Uuid::nil());
        assert_eq!(definition.key.kind(), "tag");
        assert_eq!(definition.key.name(), "trusted");
        assert!(definition.enabled);
        assert_eq!(definition.expiry, ExpiryPolicy::ManualOnly);
        Ok(())
    }

    #[test]
    fn relation_spec_macro_supports_disabled_timed_specs() -> TestResult<()> {
        let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .map(|timestamp| timestamp.with_timezone(&Utc))?;
        let definition = RelationDefinition::from_spec::<DisabledTimedSanction>(at)?;

        assert_eq!(
            definition.id,
            Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0003)
        );
        assert_eq!(definition.key.kind(), "sanction");
        assert_eq!(definition.key.name(), "review_hold");
        assert!(!definition.enabled);
        assert_eq!(definition.expiry, ExpiryPolicy::At { timestamp: at });
        Ok(())
    }
}
