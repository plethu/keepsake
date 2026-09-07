use core::result;
use serde::de;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use time::OffsetDateTime;

use crate::error::{KeepsakeError, Result};
use crate::policy::ExpiryPolicy;

use super::{Keepsake, RelationId, TenantId, validate_persisted_identifier};

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
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
    pub fn new(kind: impl Into<String>, name: impl Into<String>) -> Result<Self> {
        let relation = Self {
            kind: RelationKind::new(kind)?,
            name: RelationName::new(name)?,
        };
        Ok(relation)
    }

    /// Validates the relation key.
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RelationKind(String);

impl RelationKind {
    /// Builds a validated relation kind.
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_persisted_identifier("relation.kind", &value)?;
        Ok(Self(value))
    }

    /// Validates the relation kind.
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
    pub fn validate(&self) -> Result<()> {
        validate_persisted_identifier("relation.kind", &self.0)
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

impl<'de> Deserialize<'de> for RelationKind {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// Relation name within a relation kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RelationName(String);

impl RelationName {
    /// Builds a validated relation name.
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_persisted_identifier("relation.name", &value)?;
        Ok(Self(value))
    }

    /// Validates the relation name.
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
    pub fn validate(&self) -> Result<()> {
        validate_persisted_identifier("relation.name", &self.0)
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

impl<'de> Deserialize<'de> for RelationName {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
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
    ///
    /// # Panics
    ///
    /// Panics if either component is empty, exceeds 191 UTF-8 bytes, or has
    /// leading or trailing Unicode whitespace. In a const context invalid input
    /// is rejected at compile time.
    #[must_use]
    pub const fn new(kind: &'static str, name: &'static str) -> Self {
        assert_valid_static_relation_component(kind);
        assert_valid_static_relation_component(name);
        Self { kind, name }
    }

    /// Converts this static key into a validated owned relation key.
    ///
    /// # Errors
    ///
    /// Returns an identifier error for an invalid relation component.
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
    assert!(
        bytes.len() <= super::MAX_PERSISTED_IDENTIFIER_BYTES,
        "static relation component exceeds 191 UTF-8 bytes"
    );
    assert!(
        !starts_with_whitespace(bytes) && !ends_with_whitespace(bytes),
        "static relation component must not have leading or trailing whitespace"
    );
}

// `str::trim` and `char::is_whitespace` are not const on the pinned compiler.
// Match their White_Space scalars at UTF-8 boundaries. The exhaustive scalar
// parity test below makes a future standard-library Unicode change visible.
const fn starts_with_whitespace(bytes: &[u8]) -> bool {
    matches!(
        bytes,
        [0x09..=0x0d | 0x20, ..]
            | [0xc2, 0x85 | 0xa0, ..]
            | [0xe1, 0x9a, 0x80, ..]
            | [0xe2, 0x80, 0x80..=0x8a | 0xa8 | 0xa9 | 0xaf, ..]
            | [0xe2, 0x81, 0x9f, ..]
            | [0xe3, 0x80, 0x80, ..]
    )
}

const fn ends_with_whitespace(bytes: &[u8]) -> bool {
    matches!(
        bytes,
        [.., 0x09..=0x0d | 0x20]
            | [.., 0xc2, 0x85 | 0xa0]
            | [.., 0xe1, 0x9a, 0x80]
            | [.., 0xe2, 0x80, 0x80..=0x8a | 0xa8 | 0xa9 | 0xaf]
            | [.., 0xe2, 0x81, 0x9f]
            | [.., 0xe3, 0x80, 0x80]
    )
}

/// Configured relation definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelationDefinition {
    /// Tenant that owns this relation definition.
    pub tenant_id: TenantId,
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
    ///
    /// # Errors
    ///
    /// Returns relation-key or expiry-policy validation errors.
    pub fn new(
        tenant_id: TenantId,
        id: RelationId,
        key: RelationKey,
        enabled: bool,
        expiry: ExpiryPolicy,
    ) -> Result<Self> {
        key.validate()?;
        expiry.validate()?;
        Ok(Self {
            tenant_id,
            id,
            key,
            enabled,
            expiry,
        })
    }

    /// Revalidates a relation definition received across a trust boundary.
    ///
    /// # Errors
    ///
    /// Returns relation-key or expiry-policy validation errors.
    pub fn validate(&self) -> Result<()> {
        self.key.validate()?;
        self.expiry.validate()
    }

    /// Builds an enabled relation definition.
    ///
    /// # Errors
    ///
    /// Returns relation-key or expiry-policy validation errors.
    pub fn enabled(
        tenant_id: TenantId,
        id: RelationId,
        key: RelationKey,
        expiry: ExpiryPolicy,
    ) -> Result<Self> {
        Self::new(tenant_id, id, key, true, expiry)
    }

    /// Builds a disabled relation definition.
    ///
    /// # Errors
    ///
    /// Returns relation-key or expiry-policy validation errors.
    pub fn disabled(
        tenant_id: TenantId,
        id: RelationId,
        key: RelationKey,
        expiry: ExpiryPolicy,
    ) -> Result<Self> {
        Self::new(tenant_id, id, key, false, expiry)
    }

    /// Builds a relation definition from a typed relation spec.
    ///
    /// # Errors
    ///
    /// Returns relation-key or expiry-policy validation errors.
    pub fn from_spec<Spec>(tenant_id: TenantId, at: OffsetDateTime) -> Result<Self>
    where
        Spec: RelationSpec,
    {
        Self::new(
            tenant_id,
            Spec::ID,
            Spec::KEY.to_relation_key()?,
            Spec::ENABLED,
            Spec::expiry(at),
        )
    }
}

impl<'de> Deserialize<'de> for RelationDefinition {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireRelationDefinition {
            tenant_id: TenantId,
            id: RelationId,
            key: RelationKey,
            enabled: bool,
            expiry: ExpiryPolicy,
        }

        let wire = WireRelationDefinition::deserialize(deserializer)?;
        Self::new(wire.tenant_id, wire.id, wire.key, wire.enabled, wire.expiry)
            .map_err(de::Error::custom)
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
    ///
    /// # Errors
    ///
    /// Returns a lifecycle-model error unless the assignment is applied and its tenant
    /// and relation identity match the supplied definition.
    pub fn new(keepsake: Keepsake, relation: RelationDefinition) -> Result<Self> {
        if keepsake.tenant_id() != &relation.tenant_id {
            return Err(KeepsakeError::TenantMismatch {
                expected: relation.tenant_id,
                actual: keepsake.tenant_id().clone(),
            });
        }

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
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ActiveRelationRecord {
            keepsake: Keepsake,
            relation: RelationDefinition,
        }

        let record = ActiveRelationRecord::deserialize(deserializer)?;
        Self::new(record.keepsake, record.relation).map_err(de::Error::custom)
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
    fn expiry(at: OffsetDateTime) -> ExpiryPolicy;
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    #[test]
    fn const_whitespace_matches_runtime_for_every_unicode_scalar() {
        for scalar in (0..=0x10_ffff).filter_map(char::from_u32) {
            let mut buffer = [0; 4];
            let text = scalar.encode_utf8(&mut buffer);
            let expected = scalar.is_whitespace();
            assert_eq!(
                starts_with_whitespace(text.as_bytes()),
                expected,
                "{scalar:?}"
            );
            assert_eq!(
                ends_with_whitespace(text.as_bytes()),
                expected,
                "{scalar:?}"
            );
        }
    }

    #[test]
    fn static_relation_keys_reject_unicode_edge_whitespace() {
        const VALID: StaticRelationKey = StaticRelationKey::new("game\u{2003}tag", "trusted");
        for invalid in [
            "\u{2003}",
            "\u{2003}tag",
            "tag\u{2003}",
            "\u{b}tag",
            "tag\u{c}",
        ] {
            assert!(RelationKey::new(invalid, "valid").is_err());
            assert!(RelationKey::new("valid", invalid).is_err());
            assert!(catch_unwind(|| StaticRelationKey::new(invalid, "valid")).is_err());
            assert!(catch_unwind(|| StaticRelationKey::new("valid", invalid)).is_err());
        }
        assert!(VALID.to_relation_key().is_ok());
    }

    #[test]
    fn serde_rejects_whitespace_relation_components() {
        let kind = serde_json::from_str::<RelationKind>(r#""   ""#);
        let name = serde_json::from_str::<RelationName>(r#""\n\t""#);

        assert!(kind.is_err());
        assert!(name.is_err());
    }

    #[test]
    fn serde_rejects_invalid_nested_fulfillment_policy() {
        let json = r#"{
            "tenant_id":"tenant-a",
            "id":"00000000-0000-0000-0000-000000000001",
            "key":{"kind":"tag","name":"trusted"},
            "enabled":true,
            "expiry":{"type":"when_fulfilled","policy":{"type":"counter_at_least","key":"steps","threshold":0}}
        }"#;

        assert!(serde_json::from_str::<RelationDefinition>(json).is_err());
    }
}
