use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::error::Result;

use super::validate_not_empty;

/// Validated tenant identity owned by Keepsake.
///
/// A tenant is always supplied explicitly at the domain boundary. Keepsake
/// does not define a default or an implicit all-tenant scope.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    const MAX_UTF8_BYTES: usize = 255;

    /// Builds a tenant identity from an application-owned value.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_not_empty("tenant_id", &value)?;
        if value.len() > Self::MAX_UTF8_BYTES {
            return Err(crate::KeepsakeError::TenantIdTooLong {
                max: Self::MAX_UTF8_BYTES,
                actual: value.len(),
            });
        }
        for character in value.chars() {
            if character.is_control() {
                return Err(crate::KeepsakeError::TenantIdControlCharacter {
                    code_point: character as u32,
                });
            }
            let code_point = character as u32;
            if (0xFDD0..=0xFDEF).contains(&code_point) || (code_point & 0xFFFF) >= 0xFFFE {
                return Err(crate::KeepsakeError::TenantIdNoncharacter { code_point });
            }
        }
        Ok(Self(value))
    }

    /// Returns the tenant identity as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<String> for TenantId {
    type Error = crate::KeepsakeError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl TryFrom<&str> for TenantId {
    type Error = crate::KeepsakeError;

    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Stable identifier for a keepsake row.
pub type KeepsakeId = Uuid;

/// Stable identifier for a relation definition.
pub type RelationId = Uuid;

/// Opaque application subject identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct SubjectRef {
    /// Application-owned subject kind, such as `user`, `account`, or `device`.
    kind: String,
    /// Application-owned subject id.
    id: String,
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
        validate_not_empty("subject.kind", self.kind())?;
        validate_not_empty("subject.id", self.id())
    }

    /// Returns the application-owned subject kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the application-owned subject id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<'de> Deserialize<'de> for SubjectRef {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SubjectRefRecord {
            kind: String,
            id: String,
        }

        let record = SubjectRefRecord::deserialize(deserializer)?;
        Self::new(record.kind, record.id).map_err(serde::de::Error::custom)
    }
}

/// Application-owned actor metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ActorRef {
    /// Actor kind, such as `user`, `system`, or `job`.
    kind: String,
    /// Actor id.
    id: String,
}

impl ActorRef {
    /// Builds a validated actor reference.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Result<Self> {
        let actor = Self {
            kind: kind.into(),
            id: id.into(),
        };
        actor.validate()?;
        Ok(actor)
    }

    /// Validates the actor reference.
    pub fn validate(&self) -> Result<()> {
        validate_not_empty("actor.kind", self.kind())?;
        validate_not_empty("actor.id", self.id())
    }

    /// Returns the actor kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the actor id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<'de> Deserialize<'de> for ActorRef {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ActorRefRecord {
            kind: String,
            id: String,
        }

        let record = ActorRefRecord::deserialize(deserializer)?;
        Self::new(record.kind, record.id).map_err(serde::de::Error::custom)
    }
}
