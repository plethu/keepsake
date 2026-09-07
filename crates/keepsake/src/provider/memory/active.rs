//! Active relation seeds and scoped in-memory reads.
use crate::provider::{ActiveRelationSource, ProviderResult};
use crate::{
    ActiveRelation, Keepsake, KeepsakeError, KeepsakeId, RelationDefinition, RelationId,
    RelationKey, RelationSpec, SubjectRef, TenantId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use time::OffsetDateTime;
/// Error returned by the in-memory active relation source.
#[derive(Debug, thiserror::Error)]
pub enum InMemoryActiveRelationsError {
    /// The in-memory relation state lock was poisoned.
    #[error("in-memory active relation source lock poisoned")]
    Poisoned,

    /// A core model invariant failed while seeding active relations.
    #[error(transparent)]
    Keepsake(#[from] KeepsakeError),
}

/// In-memory active relation source for adapter and application tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryActiveRelations {
    active: Arc<RwLock<Vec<ActiveRelation>>>,
}

/// Builder for seeding one active typed relation into [`InMemoryActiveRelations`].
#[derive(Debug, Clone)]
pub struct ActiveRelationSeed<Spec> {
    tenant_id: TenantId,
    keepsake_id: KeepsakeId,
    subject: SubjectRef,
    active_at: OffsetDateTime,
    metadata: BTreeMap<String, String>,
    _spec: PhantomData<fn() -> Spec>,
}

impl<Spec> ActiveRelationSeed<Spec>
where
    Spec: RelationSpec,
{
    /// Starts an active relation seed with an explicit keepsake instance id.
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        keepsake_id: KeepsakeId,
        subject: SubjectRef,
        active_at: OffsetDateTime,
    ) -> Self {
        Self {
            tenant_id,
            keepsake_id,
            subject,
            active_at,
            metadata: BTreeMap::new(),
            _spec: PhantomData,
        }
    }

    /// Starts an active relation seed from a deterministic UUID integer.
    #[must_use]
    pub fn from_u128(
        tenant_id: TenantId,
        instance_id: u128,
        subject: SubjectRef,
        active_at: OffsetDateTime,
    ) -> Self {
        Self::new(
            tenant_id,
            uuid::Uuid::from_u128(instance_id),
            subject,
            active_at,
        )
    }

    /// Adds one opaque application metadata attribute.
    #[must_use]
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Adds opaque application metadata attributes.
    #[must_use]
    pub fn with_attributes<K, V>(mut self, attributes: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.metadata.extend(
            attributes
                .into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
        self
    }

    fn into_active_relation(self) -> Result<ActiveRelation, KeepsakeError> {
        let relation = RelationDefinition::from_spec::<Spec>(self.tenant_id, self.active_at)?;
        let keepsake = Keepsake::applied(
            self.keepsake_id,
            self.subject,
            &relation,
            self.active_at,
            self.metadata,
        )?;
        ActiveRelation::new(keepsake, relation)
    }
}

impl InMemoryActiveRelations {
    /// Creates an empty in-memory active relation source.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates an in-memory active relation source from validated active relations.
    #[must_use]
    pub fn new(active: impl IntoIterator<Item = ActiveRelation>) -> Self {
        Self {
            active: Arc::new(RwLock::new(active.into_iter().collect())),
        }
    }

    /// Adds a validated active relation.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryActiveRelationsError::Poisoned` if the relation lock was poisoned.
    pub fn insert(
        &self,
        active: ActiveRelation,
    ) -> ProviderResult<(), InMemoryActiveRelationsError> {
        self.active
            .write()
            .map_err(|_| InMemoryActiveRelationsError::Poisoned)?
            .push(active);
        Ok(())
    }

    /// Inserts an active keepsake for a typed relation spec with empty metadata.
    ///
    /// # Errors
    ///
    /// Returns seed/definition validation errors or a poisoned relation-lock error.
    pub fn insert_active_for_spec<Spec>(
        &self,
        tenant_id: TenantId,
        instance_id: u128,
        subject: SubjectRef,
        active_at: OffsetDateTime,
    ) -> ProviderResult<(), InMemoryActiveRelationsError>
    where
        Spec: RelationSpec,
    {
        self.insert_active_relation(ActiveRelationSeed::<Spec>::from_u128(
            tenant_id,
            instance_id,
            subject,
            active_at,
        ))
    }

    /// Inserts an active relation seed built from a typed relation spec.
    ///
    /// # Errors
    ///
    /// Returns seed/definition validation errors or a poisoned relation-lock error.
    pub fn insert_active_relation<Spec>(
        &self,
        seed: ActiveRelationSeed<Spec>,
    ) -> ProviderResult<(), InMemoryActiveRelationsError>
    where
        Spec: RelationSpec,
    {
        self.insert(seed.into_active_relation()?)
    }

    /// Adds an active keepsake for a typed relation spec.
    ///
    /// # Errors
    ///
    /// Returns seed/definition validation errors or a poisoned relation-lock error.
    pub fn insert_for_spec<Spec>(
        &self,
        tenant_id: TenantId,
        keepsake_id: KeepsakeId,
        subject: SubjectRef,
        applied_at: OffsetDateTime,
        metadata: BTreeMap<String, String>,
    ) -> ProviderResult<(), InMemoryActiveRelationsError>
    where
        Spec: RelationSpec,
    {
        self.insert_active_relation(
            ActiveRelationSeed::<Spec>::new(tenant_id, keepsake_id, subject, applied_at)
                .with_attributes(metadata),
        )
    }

    fn active_for_subject(
        &self,
        tenant_id: &TenantId,
        subject: &SubjectRef,
    ) -> ProviderResult<Vec<ActiveRelation>, InMemoryActiveRelationsError> {
        let mut active = self
            .active
            .read()
            .map_err(|_| InMemoryActiveRelationsError::Poisoned)?
            .iter()
            .filter(|active| {
                active.keepsake().tenant_id() == tenant_id && active.keepsake().subject() == subject
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_active_relations(&mut active);
        Ok(active)
    }

    fn active_for_subject_by_ids(
        &self,
        tenant_id: &TenantId,
        subject: &SubjectRef,
        relation_ids: &[RelationId],
    ) -> ProviderResult<Vec<ActiveRelation>, InMemoryActiveRelationsError> {
        if relation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let requested = relation_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut active = self
            .active
            .read()
            .map_err(|_| InMemoryActiveRelationsError::Poisoned)?
            .iter()
            .filter(|active| {
                active.keepsake().tenant_id() == tenant_id
                    && active.keepsake().subject() == subject
                    && requested.contains(&active.keepsake().relation_id())
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_active_relations(&mut active);
        Ok(active)
    }

    fn active_for_subject_by_keys(
        &self,
        tenant_id: &TenantId,
        subject: &SubjectRef,
        keys: &[RelationKey],
    ) -> ProviderResult<Vec<ActiveRelation>, InMemoryActiveRelationsError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let requested = keys.iter().collect::<BTreeSet<_>>();
        let mut active = self
            .active
            .read()
            .map_err(|_| InMemoryActiveRelationsError::Poisoned)?
            .iter()
            .filter(|active| {
                active.keepsake().tenant_id() == tenant_id
                    && active.keepsake().subject() == subject
                    && requested.contains(&active.relation().key)
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_active_relations(&mut active);
        Ok(active)
    }
}

impl ActiveRelationSource for InMemoryActiveRelations {
    type Error = InMemoryActiveRelationsError;

    fn active_relations_for_subject<'a>(
        &'a self,
        tenant_id: &'a TenantId,
        subject: &'a SubjectRef,
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a {
        ready(self.active_for_subject(tenant_id, subject))
    }

    fn active_relations_for_subject_by_ids<'a>(
        &'a self,
        tenant_id: &'a TenantId,
        subject: &'a SubjectRef,
        relation_ids: &'a [RelationId],
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a {
        ready(self.active_for_subject_by_ids(tenant_id, subject, relation_ids))
    }

    fn active_relations_for_subject_by_keys<'a>(
        &'a self,
        tenant_id: &'a TenantId,
        subject: &'a SubjectRef,
        keys: &'a [RelationKey],
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a {
        ready(self.active_for_subject_by_keys(tenant_id, subject, keys))
    }
}

fn sort_active_relations(active: &mut [ActiveRelation]) {
    active.sort_by_key(|active| (active.keepsake().relation_id(), active.keepsake().id()));
}

#[cfg(test)]
mod tests;
