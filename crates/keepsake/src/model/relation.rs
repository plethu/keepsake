use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{KeepsakeError, Result};
use crate::policy::ExpiryPolicy;

use super::{Keepsake, RelationId, validate_not_empty};

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
        assert_valid_static_relation_component(kind);
        assert_valid_static_relation_component(name);
        Self { kind, name }
    }

    /// Converts this static key into a validated owned relation key.
    pub fn to_relation_key(self) -> Result<RelationKey> {
        RelationKey::new(self.kind, self.name)
    }
}

const fn assert_valid_static_relation_component(value: &str) {
    let bytes = value.as_bytes();
    assert!(
        !bytes.is_empty(),
        "static relation component must not be empty"
    );
    let mut index = 0;
    let mut has_non_whitespace = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if !(byte == b' ' || byte == b'\n' || byte == b'\r' || byte == b'\t') {
            has_non_whitespace = true;
        }
        index += 1;
    }
    assert!(
        has_non_whitespace,
        "static relation component must not be whitespace"
    );
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

/// Active keepsake membership with its stored relation definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActiveRelation {
    /// Active keepsake.
    keepsake: Keepsake,
    /// Stored relation definition for the keepsake.
    relation: RelationDefinition,
}

impl ActiveRelation {
    /// Builds an active relation and validates the membership relation id.
    pub fn new(keepsake: Keepsake, relation: RelationDefinition) -> Result<Self> {
        if keepsake.relation_id() != relation.id {
            return Err(KeepsakeError::ActiveRelationMismatch {
                keepsake_relation_id: keepsake.relation_id(),
                relation_id: relation.id,
            });
        }

        if !keepsake.is_active() {
            return Err(KeepsakeError::InactiveActiveRelation {
                keepsake_id: keepsake.id(),
            });
        }
        Ok(Self { keepsake, relation })
    }

    /// Returns the active keepsake.
    #[must_use]
    pub const fn keepsake(&self) -> &Keepsake {
        &self.keepsake
    }

    /// Returns the stored relation definition.
    #[must_use]
    pub const fn relation(&self) -> &RelationDefinition {
        &self.relation
    }

    /// Decomposes the active relation into its owned parts.
    #[must_use]
    pub fn into_parts(self) -> (Keepsake, RelationDefinition) {
        (self.keepsake, self.relation)
    }
}

impl<'de> Deserialize<'de> for ActiveRelation {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ActiveRelationRecord {
            keepsake: Keepsake,
            relation: RelationDefinition,
        }

        let record = ActiveRelationRecord::deserialize(deserializer)?;
        Self::new(record.keepsake, record.relation).map_err(serde::de::Error::custom)
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
