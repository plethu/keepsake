use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::command::{ApplyKeepsake, RevokeKeepsake};
use crate::error::KeepsakeError;
use crate::model::KeepsakeRecord;
use crate::model::{
    ActiveRelation, FulfillmentSnapshot, Keepsake, KeepsakeId, LifecycleState, RelationDefinition,
    RelationId, RelationKey, RelationSpec, SubjectRef,
};
use crate::policy::ExpiryPolicy;

use super::{ActiveRelationSource, FulfillmentProvider, KeepsakeStore, ProviderResult};

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
    keepsakes: Arc<RwLock<BTreeMap<KeepsakeId, Keepsake>>>,
}

/// Error returned by the in-memory fulfillment provider.
#[derive(Debug, thiserror::Error)]
pub enum InMemoryFulfillmentProviderError {
    /// The in-memory fulfillment state lock was poisoned.
    #[error("in-memory fulfillment provider lock poisoned")]
    Poisoned,
}

/// In-memory fulfillment snapshot provider for adapter and application tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryFulfillmentProvider {
    snapshots: Arc<RwLock<BTreeMap<KeepsakeId, FulfillmentSnapshot>>>,
}

/// Builder for seeding one active typed relation into [`InMemoryActiveRelations`].
#[derive(Debug, Clone)]
pub struct ActiveRelationSeed<Spec> {
    keepsake_id: KeepsakeId,
    subject: SubjectRef,
    active_at: DateTime<Utc>,
    metadata: BTreeMap<String, String>,
    _spec: PhantomData<fn() -> Spec>,
}

impl<Spec> ActiveRelationSeed<Spec>
where
    Spec: RelationSpec,
{
    /// Starts an active relation seed with an explicit keepsake instance id.
    #[must_use]
    pub fn new(keepsake_id: KeepsakeId, subject: SubjectRef, active_at: DateTime<Utc>) -> Self {
        Self {
            keepsake_id,
            subject,
            active_at,
            metadata: BTreeMap::new(),
            _spec: PhantomData,
        }
    }

    /// Starts an active relation seed from a deterministic UUID integer.
    #[must_use]
    pub fn from_u128(instance_id: u128, subject: SubjectRef, active_at: DateTime<Utc>) -> Self {
        Self::new(uuid::Uuid::from_u128(instance_id), subject, active_at)
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
        let relation = RelationDefinition::from_spec::<Spec>(self.active_at)?;
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
    pub fn insert_active_for_spec<Spec>(
        &self,
        instance_id: u128,
        subject: SubjectRef,
        active_at: DateTime<Utc>,
    ) -> ProviderResult<(), InMemoryActiveRelationsError>
    where
        Spec: RelationSpec,
    {
        self.insert_active_relation(ActiveRelationSeed::<Spec>::from_u128(
            instance_id,
            subject,
            active_at,
        ))
    }

    /// Inserts an active relation seed built from a typed relation spec.
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
        self.insert_active_relation(
            ActiveRelationSeed::<Spec>::new(keepsake_id, subject, applied_at)
                .with_attributes(metadata),
        )
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
    pub fn apply_with_relation(
        &self,
        command: &ApplyKeepsake,
        relation: &RelationDefinition,
    ) -> ProviderResult<Keepsake, InMemoryKeepsakeStoreError> {
        command.subject.validate()?;
        command.context.validate()?;
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
        if keepsakes.contains_key(&command.id) {
            return Err(InMemoryKeepsakeStoreError::DuplicateKeepsakeId {
                keepsake_id: command.id,
            });
        }

        if keepsakes.values().any(|keepsake| {
            keepsake.is_active()
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

        let keepsake = Keepsake::applied(
            command.id,
            command.subject.clone(),
            relation,
            command.at,
            command.metadata.clone(),
        )?;
        keepsakes.insert(keepsake.id(), keepsake.clone());
        drop(keepsakes);
        Ok(keepsake)
    }

    fn synthetic_relation(command: &ApplyKeepsake) -> Result<RelationDefinition, KeepsakeError> {
        RelationDefinition::enabled(
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
        let keepsake = keepsakes.get(&command.keepsake_id).cloned().ok_or(
            InMemoryKeepsakeStoreError::KeepsakeNotFound {
                keepsake_id: command.keepsake_id,
            },
        )?;
        if !keepsake.is_active() {
            return Err(InMemoryKeepsakeStoreError::AlreadyTerminal {
                keepsake_id: command.keepsake_id,
            });
        }

        let revoked: Keepsake = KeepsakeRecord {
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
        keepsakes.insert(command.keepsake_id, revoked.clone());
        drop(keepsakes);
        Ok(revoked)
    }

    fn active_for_subject(
        &self,
        subject: &SubjectRef,
    ) -> ProviderResult<Vec<Keepsake>, Self::Error> {
        let mut active = self
            .keepsakes
            .read()
            .map_err(|_| InMemoryKeepsakeStoreError::Poisoned)?
            .values()
            .filter(|keepsake| keepsake.is_active() && keepsake.subject() == subject)
            .cloned()
            .collect::<Vec<_>>();
        active.sort_by_key(|keepsake| (keepsake.relation_id(), keepsake.id()));
        Ok(active)
    }

    fn get(&self, id: KeepsakeId) -> ProviderResult<Option<Keepsake>, Self::Error> {
        Ok(self
            .keepsakes
            .read()
            .map_err(|_| InMemoryKeepsakeStoreError::Poisoned)?
            .get(&id)
            .cloned())
    }
}

impl InMemoryFulfillmentProvider {
    /// Creates an empty in-memory fulfillment provider.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Inserts or replaces a test fulfillment snapshot.
    pub fn insert_snapshot(
        &self,
        keepsake_id: KeepsakeId,
        snapshot: FulfillmentSnapshot,
    ) -> ProviderResult<(), InMemoryFulfillmentProviderError> {
        self.snapshots
            .write()
            .map_err(|_| InMemoryFulfillmentProviderError::Poisoned)?
            .insert(keepsake_id, snapshot);
        Ok(())
    }
}

impl FulfillmentProvider for InMemoryFulfillmentProvider {
    type Error = InMemoryFulfillmentProviderError;

    fn snapshot(
        &self,
        keepsake: &Keepsake,
    ) -> ProviderResult<Option<FulfillmentSnapshot>, Self::Error> {
        Ok(self
            .snapshots
            .read()
            .map_err(|_| InMemoryFulfillmentProviderError::Poisoned)?
            .get(&keepsake.id())
            .cloned())
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
    use crate::{
        DecisionKind, ExpiryPolicy, FulfillmentPolicy, StaticRelationKey, TransitionReason,
        evaluate,
    };

    type TestResult<T> = core::result::Result<T, TestError>;

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Chrono(#[from] chrono::ParseError),

        #[error(transparent)]
        InMemory(#[from] InMemoryActiveRelationsError),

        #[error(transparent)]
        InMemoryFulfillment(#[from] InMemoryFulfillmentProviderError),

        #[error(transparent)]
        InMemoryStore(#[from] InMemoryKeepsakeStoreError),

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

    fn context() -> crate::Result<crate::CommandContext> {
        Ok(crate::CommandContext::new(crate::ActorRef::new(
            "test", "worker",
        )?))
    }

    fn apply_command(
        id: KeepsakeId,
        subject: SubjectRef,
        relation_id: RelationId,
        at: DateTime<Utc>,
    ) -> crate::Result<ApplyKeepsake> {
        let mut command = ApplyKeepsake::new(subject, relation_id, at, context()?);
        command.id = id;
        Ok(command)
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

    #[test]
    fn inserts_active_for_spec_with_explicit_id_time_and_empty_metadata() -> TestResult<()> {
        let source = InMemoryActiveRelations::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;

        source.insert_active_for_spec::<TrustedTag>(
            0xaaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa,
            subject.clone(),
            at,
        )?;

        let active = source.active_for_subject(&subject)?;
        assert_eq!(active.len(), 1);
        assert_eq!(
            active[0].keepsake().id(),
            Uuid::from_u128(0xaaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa)
        );
        assert_eq!(active[0].keepsake().applied_at(), at);
        assert_eq!(active[0].relation().id, TrustedTag::ID);
        assert!(active[0].keepsake().metadata().is_empty());
        Ok(())
    }

    #[test]
    fn active_relation_seed_preserves_attributes() -> TestResult<()> {
        let source = InMemoryActiveRelations::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;

        source.insert_active_relation(
            ActiveRelationSeed::<AdminTag>::new(
                Uuid::from_u128(0xbbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb),
                subject.clone(),
                at,
            )
            .with_attribute("ticket", "case-1")
            .with_attributes([("source", "fixture")]),
        )?;

        let active = source.active_for_subject(&subject)?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].keepsake().applied_at(), at);
        assert_eq!(active[0].relation().id, AdminTag::ID);
        assert_eq!(
            active[0]
                .keepsake()
                .metadata()
                .get("ticket")
                .map(String::as_str),
            Some("case-1")
        );
        assert_eq!(
            active[0]
                .keepsake()
                .metadata()
                .get("source")
                .map(String::as_str),
            Some("fixture")
        );
        Ok(())
    }

    #[test]
    fn keepsake_store_apply_then_active_for_subject_returns_keepsake() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let command = apply_command(Uuid::from_u128(100), subject.clone(), TrustedTag::ID, at)?;

        let keepsake = store.apply(&command)?;

        assert_eq!(store.active_for_subject(&subject)?, vec![keepsake]);
        Ok(())
    }

    #[test]
    fn keepsake_store_rejects_duplicate_active_subject_relation() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let first = apply_command(Uuid::from_u128(100), subject.clone(), TrustedTag::ID, at)?;
        let second = apply_command(Uuid::from_u128(101), subject, TrustedTag::ID, at)?;

        store.apply(&first)?;
        let error = store
            .apply(&second)
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert_eq!(
            error,
            Err(format!(
                "subject account/acct_123 already has active relation {}",
                TrustedTag::ID
            ))
        );
        Ok(())
    }

    #[test]
    fn keepsake_store_rejects_duplicate_keepsake_id() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let at = ts("2026-01-01T00:00:00Z")?;
        let id = Uuid::from_u128(100);
        let first = apply_command(
            id,
            SubjectRef::new("account", "acct_123")?,
            TrustedTag::ID,
            at,
        )?;
        let second = apply_command(
            id,
            SubjectRef::new("account", "acct_456")?,
            AdminTag::ID,
            at,
        )?;

        store.apply(&first)?;
        let error = store
            .apply(&second)
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert_eq!(error, Err(format!("keepsake {id} already exists")));
        Ok(())
    }

    #[test]
    fn keepsake_store_rejects_apply_relation_mismatch() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let relation = RelationDefinition::from_spec::<TrustedTag>(at)?;
        let command = apply_command(Uuid::from_u128(100), subject, AdminTag::ID, at)?;

        let error = store
            .apply_with_relation(&command, &relation)
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert_eq!(
            error,
            Err(format!(
                "apply command targets relation {}, but definition uses {}",
                AdminTag::ID,
                TrustedTag::ID
            ))
        );
        Ok(())
    }

    #[test]
    fn keepsake_store_revoke_removes_from_active_subject_results() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let command = apply_command(Uuid::from_u128(100), subject.clone(), TrustedTag::ID, at)?;
        let keepsake = store.apply(&command)?;
        let revoke = RevokeKeepsake::new(keepsake.id(), ts("2026-01-02T00:00:00Z")?, context()?);

        let revoked = store.revoke(&revoke)?;

        assert!(revoked.is_revoked());
        assert!(store.active_for_subject(&subject)?.is_empty());
        Ok(())
    }

    #[test]
    fn keepsake_store_rejects_revoking_already_terminal_keepsake() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let command = apply_command(Uuid::from_u128(100), subject, TrustedTag::ID, at)?;
        let keepsake = store.apply(&command)?;
        let revoke = RevokeKeepsake::new(keepsake.id(), ts("2026-01-02T00:00:00Z")?, context()?);

        store.revoke(&revoke)?;
        let error = store
            .revoke(&revoke)
            .map(|_| ())
            .map_err(|error| error.to_string());

        assert_eq!(
            error,
            Err(format!("keepsake {} is already terminal", keepsake.id()))
        );
        Ok(())
    }

    #[test]
    fn keepsake_store_get_returns_none_then_some_after_apply() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let id = Uuid::from_u128(100);
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let command = apply_command(id, subject, TrustedTag::ID, at)?;

        assert_eq!(store.get(id)?, None);
        let keepsake = store.apply(&command)?;
        assert_eq!(store.get(id)?, Some(keepsake));
        Ok(())
    }

    #[test]
    fn fulfillment_provider_snapshot_returns_inserted_snapshot() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let provider = InMemoryFulfillmentProvider::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let command = apply_command(Uuid::from_u128(100), subject, TrustedTag::ID, at)?;
        let keepsake = store.apply(&command)?;
        let snapshot = FulfillmentSnapshot::empty().with_counter("steps", 3);

        assert_eq!(provider.snapshot(&keepsake)?, None);
        provider.insert_snapshot(keepsake.id(), snapshot.clone())?;

        assert_eq!(provider.snapshot(&keepsake)?, Some(snapshot));
        Ok(())
    }

    #[test]
    fn in_memory_store_and_fulfillment_provider_drive_fulfilled_evaluation() -> TestResult<()> {
        let store = InMemoryKeepsakeStore::empty();
        let provider = InMemoryFulfillmentProvider::empty();
        let subject = SubjectRef::new("account", "acct_123")?;
        let at = ts("2026-01-01T00:00:00Z")?;
        let relation = RelationDefinition::enabled(
            Uuid::from_u128(300),
            RelationKey::new("tag", "steps_done")?,
            ExpiryPolicy::WhenFulfilled {
                policy: FulfillmentPolicy::CounterAtLeast {
                    key: "steps".to_owned(),
                    threshold: 3,
                },
            },
        )?;
        let command = apply_command(Uuid::from_u128(100), subject, relation.id, at)?;
        let keepsake = store.apply_with_relation(&command, &relation)?;
        let snapshot = FulfillmentSnapshot::empty().with_counter("steps", 3);
        provider.insert_snapshot(keepsake.id(), snapshot)?;

        let decision = evaluate(
            ts("2026-01-02T00:00:00Z")?,
            &relation,
            &keepsake,
            provider.snapshot(&keepsake)?.as_ref(),
        );

        assert!(matches!(
            decision.kind,
            DecisionKind::Transition {
                reason: TransitionReason::FulfillmentSatisfied,
                ..
            }
        ));
        Ok(())
    }
}
