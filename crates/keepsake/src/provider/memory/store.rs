//! In-memory lifecycle storage for application and adapter tests.
use crate::provider::{KeepsakeStore, ProviderResult};
use crate::{
    ApplyKeepsake, ExpiryPolicy, Keepsake, KeepsakeError, KeepsakeId, KeepsakeRecord,
    LifecycleState, RelationDefinition, RelationId, RelationKey, RevokeKeepsake, SubjectRef,
    TenantId,
};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
/// Error returned by the in-memory keepsake store.
#[derive(Debug, thiserror::Error)]
pub enum InMemoryKeepsakeStoreError {
    /// The in-memory keepsake state lock was poisoned.
    #[error("in-memory keepsake store lock poisoned")]
    Poisoned,

    /// A keepsake id was not present in the store.
    #[error("keepsake {keepsake_id} was not found")]
    KeepsakeNotFound {
        /// Missing keepsake id.
        keepsake_id: KeepsakeId,
    },

    /// A keepsake id is already present in the store.
    #[error("keepsake {keepsake_id} already exists")]
    DuplicateKeepsakeId {
        /// Duplicate keepsake id.
        keepsake_id: KeepsakeId,
    },

    /// An apply command targeted a different relation than the provided definition.
    #[error(
        "apply command targets relation {command_relation_id}, but definition uses {relation_id}"
    )]
    RelationMismatch {
        /// Relation id carried by the apply command.
        command_relation_id: RelationId,
        /// Relation id carried by the relation definition.
        relation_id: RelationId,
    },

    /// A revoke targeted a terminal keepsake.
    #[error("keepsake {keepsake_id} is already terminal")]
    AlreadyTerminal {
        /// Terminal keepsake id.
        keepsake_id: KeepsakeId,
    },

    /// A core model invariant failed.
    #[error(transparent)]
    Keepsake(#[from] KeepsakeError),
}

/// In-memory keepsake store for adapter and application tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryKeepsakeStore {
    keepsakes: Arc<RwLock<BTreeMap<(TenantId, KeepsakeId), Keepsake>>>,
}

impl InMemoryKeepsakeStore {
    /// Creates an empty in-memory keepsake store.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Applies a keepsake using a full relation definition.
    ///
    /// This helper is useful when tests need the stored keepsake to carry a
    /// non-manual expiry policy. The trait method only receives a relation id.
    ///
    /// # Errors
    ///
    /// Returns validation, tenant/relation mismatch, duplicate identity/active-relation
    /// or poisoned-store errors.
    pub fn apply_with_relation(
        &self,
        command: &ApplyKeepsake,
        relation: &RelationDefinition,
    ) -> ProviderResult<Keepsake, InMemoryKeepsakeStoreError> {
        command.subject.validate()?;
        command.context.validate()?;
        if command.tenant_id != relation.tenant_id {
            return Err(InMemoryKeepsakeStoreError::Keepsake(
                KeepsakeError::TenantMismatch {
                    expected: relation.tenant_id.clone(),
                    actual: command.tenant_id.clone(),
                },
            ));
        }

        if command.relation_id != relation.id {
            return Err(InMemoryKeepsakeStoreError::RelationMismatch {
                command_relation_id: command.relation_id,
                relation_id: relation.id,
            });
        }

        let mut keepsakes = self
            .keepsakes
            .write()
            .map_err(|_| InMemoryKeepsakeStoreError::Poisoned)?;
        if keepsakes.contains_key(&(command.tenant_id.clone(), command.id)) {
            return Err(InMemoryKeepsakeStoreError::DuplicateKeepsakeId {
                keepsake_id: command.id,
            });
        }

        if keepsakes.values().any(|keepsake| {
            keepsake.is_active()
                && keepsake.tenant_id() == &command.tenant_id
                && keepsake.subject() == &command.subject
                && keepsake.relation_id() == command.relation_id
        }) {
            return Err(KeepsakeError::DuplicateActiveKeepsake {
                subject_kind: command.subject.kind().to_owned(),
                subject_id: command.subject.id().to_owned(),
                relation_id: command.relation_id,
            }
            .into());
        }

        if !relation.enabled {
            return Err(KeepsakeError::RelationDisabled {
                relation_id: relation.id,
            }
            .into());
        }

        let keepsake = Keepsake::from_apply(command, relation)?;
        keepsakes.insert(
            (keepsake.tenant_id().clone(), keepsake.id()),
            keepsake.clone(),
        );
        drop(keepsakes);
        Ok(keepsake)
    }

    fn synthetic_relation(command: &ApplyKeepsake) -> Result<RelationDefinition, KeepsakeError> {
        RelationDefinition::enabled(
            command.tenant_id.clone(),
            command.relation_id,
            RelationKey::new("relation", command.relation_id.to_string())?,
            ExpiryPolicy::ManualOnly,
        )
    }
}

impl KeepsakeStore for InMemoryKeepsakeStore {
    type Error = InMemoryKeepsakeStoreError;

    fn apply(&self, command: &ApplyKeepsake) -> ProviderResult<Keepsake, Self::Error> {
        let relation = Self::synthetic_relation(command)?;
        self.apply_with_relation(command, &relation)
    }

    fn revoke(&self, command: &RevokeKeepsake) -> ProviderResult<Keepsake, Self::Error> {
        command.context.validate()?;
        let mut keepsakes = self
            .keepsakes
            .write()
            .map_err(|_| InMemoryKeepsakeStoreError::Poisoned)?;
        let keepsake = keepsakes
            .get(&(command.tenant_id.clone(), command.keepsake_id))
            .cloned()
            .ok_or(InMemoryKeepsakeStoreError::KeepsakeNotFound {
                keepsake_id: command.keepsake_id,
            })?;
        if !keepsake.is_active() {
            return Err(InMemoryKeepsakeStoreError::AlreadyTerminal {
                keepsake_id: command.keepsake_id,
            });
        }

        let revoked: Keepsake = KeepsakeRecord {
            tenant_id: keepsake.tenant_id().clone(),
            id: keepsake.id(),
            subject: keepsake.subject().clone(),
            relation_id: keepsake.relation_id(),
            state: LifecycleState::Revoked,
            expiry: keepsake.expiry().clone(),
            applied_at: keepsake.applied_at(),
            expires_at: keepsake.expires_at(),
            fulfilled_at: None,
            revoked_at: Some(command.at),
            metadata: keepsake.metadata().clone(),
        }
        .try_into()?;
        keepsakes.insert(
            (command.tenant_id.clone(), command.keepsake_id),
            revoked.clone(),
        );
        drop(keepsakes);
        Ok(revoked)
    }

    fn active_for_subject(
        &self,
        tenant_id: &TenantId,
        subject: &SubjectRef,
    ) -> ProviderResult<Vec<Keepsake>, Self::Error> {
        let mut active = self
            .keepsakes
            .read()
            .map_err(|_| InMemoryKeepsakeStoreError::Poisoned)?
            .values()
            .filter(|keepsake| {
                keepsake.is_active()
                    && keepsake.tenant_id() == tenant_id
                    && keepsake.subject() == subject
            })
            .cloned()
            .collect::<Vec<_>>();
        active.sort_by_key(|keepsake| (keepsake.relation_id(), keepsake.id()));
        Ok(active)
    }

    fn get(
        &self,
        tenant_id: &TenantId,
        id: KeepsakeId,
    ) -> ProviderResult<Option<Keepsake>, Self::Error> {
        Ok(self
            .keepsakes
            .read()
            .map_err(|_| InMemoryKeepsakeStoreError::Poisoned)?
            .get(&(tenant_id.clone(), id))
            .cloned())
    }
}
