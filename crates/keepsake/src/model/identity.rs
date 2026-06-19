use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;

use super::validate_not_empty;

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
        actor.validate()?;
        Ok(actor)
    }

    /// Validates the actor reference.
    pub fn validate(&self) -> Result<()> {
        validate_not_empty("actor.kind", &self.kind)?;
        validate_not_empty("actor.id", &self.id)
    }
}
