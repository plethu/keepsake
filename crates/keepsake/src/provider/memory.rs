use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, ready};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::error::KeepsakeError;
use crate::model::{
    ActiveRelation, Keepsake, KeepsakeId, RelationDefinition, RelationId, RelationKey,
    RelationSpec, SubjectRef,
};

use super::{ActiveRelationSource, ProviderResult};

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

    /// Adds an active keepsake for a typed relation spec.
    pub fn insert_for_spec<Spec>(
        &self,
        keepsake_id: KeepsakeId,
        subject: SubjectRef,
        applied_at: DateTime<Utc>,
        metadata: BTreeMap<String, String>,
    ) -> ProviderResult<(), InMemoryActiveRelationsError>
    where
        Spec: RelationSpec,
    {
        let relation = RelationDefinition::from_spec::<Spec>(applied_at)?;
        let keepsake = Keepsake::applied(keepsake_id, subject, &relation, applied_at, metadata)?;
        self.insert(ActiveRelation::new(keepsake, relation)?)
    }

    fn active_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> ProviderResult<Vec<ActiveRelation>, InMemoryActiveRelationsError> {
        let mut active = self
            .active
            .read()
            .map_err(|_| InMemoryActiveRelationsError::Poisoned)?
            .iter()
            .filter(|active| active.keepsake().subject() == subject)
            .cloned()
            .collect::<Vec<_>>();
        sort_active_relations(&mut active);
        Ok(active)
    }

    fn active_for_subject_by_ids(
        &self,
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
                active.keepsake().subject() == subject
                    && requested.contains(&active.keepsake().relation_id())
            })
            .cloned()
            .collect::<Vec<_>>();
        sort_active_relations(&mut active);
        Ok(active)
    }

    fn active_for_subject_by_keys(
        &self,
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
                active.keepsake().subject() == subject && requested.contains(&active.relation().key)
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
        subject: &'a SubjectRef,
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a {
        ready(self.active_for_subject(subject))
    }

    fn active_relations_for_subject_by_ids<'a>(
        &'a self,
        subject: &'a SubjectRef,
        relation_ids: &'a [RelationId],
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a {
        ready(self.active_for_subject_by_ids(subject, relation_ids))
    }

    fn active_relations_for_subject_by_keys<'a>(
        &'a self,
        subject: &'a SubjectRef,
        keys: &'a [RelationKey],
    ) -> impl Future<Output = ProviderResult<Vec<ActiveRelation>, Self::Error>> + Send + 'a {
        ready(self.active_for_subject_by_keys(subject, keys))
    }
}

fn sort_active_relations(active: &mut [ActiveRelation]) {
    active.sort_by_key(|active| (active.keepsake().relation_id(), active.keepsake().id()));
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{ExpiryPolicy, StaticRelationKey};

    type TestResult<T> = core::result::Result<T, TestError>;

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Chrono(#[from] chrono::ParseError),

        #[error(transparent)]
        InMemory(#[from] InMemoryActiveRelationsError),

        #[error(transparent)]
        Keepsake(#[from] KeepsakeError),
    }

    struct TrustedTag;

    impl RelationSpec for TrustedTag {
        const ID: RelationId = Uuid::from_u128(1);
        const KEY: StaticRelationKey = StaticRelationKey::new("tag", "trusted");

        fn expiry(_at: DateTime<Utc>) -> ExpiryPolicy {
            ExpiryPolicy::ManualOnly
        }
    }

    struct AdminTag;

    impl RelationSpec for AdminTag {
        const ID: RelationId = Uuid::from_u128(2);
        const KEY: StaticRelationKey = StaticRelationKey::new("tag", "admin");

        fn expiry(_at: DateTime<Utc>) -> ExpiryPolicy {
            ExpiryPolicy::ManualOnly
        }
    }

    fn ts(value: &str) -> core::result::Result<DateTime<Utc>, chrono::ParseError> {
        DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
    }

    #[test]
    fn reads_active_relations_by_subject_ids_and_keys() -> TestResult<()> {
        let source = InMemoryActiveRelations::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let other_subject = SubjectRef::new("account", "acct_456")?;
        let at = ts("2026-01-01T00:00:00Z")?;

        source.insert_for_spec::<TrustedTag>(
            Uuid::from_u128(10),
            subject.clone(),
            at,
            BTreeMap::new(),
        )?;
        source.insert_for_spec::<AdminTag>(
            Uuid::from_u128(20),
            subject.clone(),
            at,
            BTreeMap::new(),
        )?;
        source.insert_for_spec::<AdminTag>(
            Uuid::from_u128(30),
            other_subject,
            at,
            BTreeMap::new(),
        )?;

        let all = source.active_for_subject(&subject)?;
        assert_eq!(
            all.iter()
                .map(|active| active.keepsake().id())
                .collect::<Vec<_>>(),
            vec![Uuid::from_u128(10), Uuid::from_u128(20)]
        );

        let by_ids = source.active_for_subject_by_ids(
            &subject,
            &[AdminTag::ID, AdminTag::ID, Uuid::from_u128(99)],
        )?;
        assert_eq!(by_ids.len(), 1);
        assert_eq!(by_ids[0].relation().id, AdminTag::ID);

        let keys = [
            TrustedTag::KEY.to_relation_key()?,
            TrustedTag::KEY.to_relation_key()?,
            RelationKey::new("tag", "missing")?,
        ];
        let by_keys = source.active_for_subject_by_keys(&subject, &keys)?;
        assert_eq!(by_keys.len(), 1);
        assert_eq!(by_keys[0].relation().id, TrustedTag::ID);

        assert!(source.active_for_subject_by_ids(&subject, &[])?.is_empty());
        assert!(source.active_for_subject_by_keys(&subject, &[])?.is_empty());
        Ok(())
    }
}
