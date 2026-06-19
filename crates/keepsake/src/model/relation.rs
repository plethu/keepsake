use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::policy::ExpiryPolicy;

use super::{RelationId, validate_not_empty};

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
