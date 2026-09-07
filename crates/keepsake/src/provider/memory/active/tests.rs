//! Shared in-memory provider contracts.
use super::super::{
    InMemoryFulfillmentProvider, InMemoryFulfillmentProviderError, InMemoryKeepsakeStore,
    InMemoryKeepsakeStoreError,
};
use crate::provider::{FulfillmentProvider, KeepsakeStore};
use crate::{
    ApplyKeepsake, FulfillmentSnapshot, KeepsakeError, KeepsakeId, RelationDefinition, RelationId,
    RelationKey, RelationSpec, RevokeKeepsake, SubjectRef, TenantId,
};
use core::result;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::error::Parse;
use time::format_description::well_known::Rfc3339;

use uuid::Uuid;

use super::*;
use crate::{
    DecisionKind, ExpiryPolicy, FulfillmentPolicy, StaticRelationKey, TransitionReason, evaluate,
};

type TestResult<T> = result::Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Time(#[from] Parse),

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

    fn expiry(_at: OffsetDateTime) -> ExpiryPolicy {
        ExpiryPolicy::ManualOnly
    }
}

struct AdminTag;

impl RelationSpec for AdminTag {
    const ID: RelationId = Uuid::from_u128(2);
    const KEY: StaticRelationKey = StaticRelationKey::new("tag", "admin");

    fn expiry(_at: OffsetDateTime) -> ExpiryPolicy {
        ExpiryPolicy::ManualOnly
    }
}

fn ts(value: &str) -> result::Result<OffsetDateTime, Parse> {
    OffsetDateTime::parse(value, &Rfc3339)
}

fn context() -> crate::Result<crate::CommandContext> {
    Ok(crate::CommandContext::new(crate::ActorRef::new(
        "test", "worker",
    )?))
}

fn tenant() -> crate::Result<TenantId> {
    TenantId::new("tenant-a")
}

fn apply_command(
    id: KeepsakeId,
    subject: SubjectRef,
    relation_id: RelationId,
    at: OffsetDateTime,
) -> crate::Result<ApplyKeepsake> {
    let mut command = ApplyKeepsake::new(tenant()?, subject, relation_id, at, context()?);
    command.id = id;
    Ok(command)
}

#[test]
fn reads_active_relations_by_subject_ids_and_keys() -> TestResult<()> {
    let source = InMemoryActiveRelations::empty();
    let subject = SubjectRef::new("account", "acct_123")?;
    let other_subject = SubjectRef::new("account", "acct_456")?;
    let at = ts("2026-01-01T00:00:00Z")?;

    let tenant = tenant()?;
    source.insert_for_spec::<TrustedTag>(
        tenant.clone(),
        Uuid::from_u128(10),
        subject.clone(),
        at,
        BTreeMap::new(),
    )?;
    source.insert_for_spec::<AdminTag>(
        tenant.clone(),
        Uuid::from_u128(20),
        subject.clone(),
        at,
        BTreeMap::new(),
    )?;
    source.insert_for_spec::<AdminTag>(
        tenant.clone(),
        Uuid::from_u128(30),
        other_subject,
        at,
        BTreeMap::new(),
    )?;

    let all = source.active_for_subject(&tenant, &subject)?;
    assert_eq!(
        all.iter()
            .map(|active| active.keepsake().id())
            .collect::<Vec<_>>(),
        vec![Uuid::from_u128(10), Uuid::from_u128(20)]
    );

    let by_ids = source.active_for_subject_by_ids(
        &tenant,
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
    let by_keys = source.active_for_subject_by_keys(&tenant, &subject, &keys)?;
    assert_eq!(by_keys.len(), 1);
    assert_eq!(by_keys[0].relation().id, TrustedTag::ID);

    assert!(
        source
            .active_for_subject_by_ids(&tenant, &subject, &[])?
            .is_empty()
    );
    assert!(
        source
            .active_for_subject_by_keys(&tenant, &subject, &[])?
            .is_empty()
    );
    Ok(())
}

#[test]
fn in_memory_providers_isolate_same_ids_by_tenant() -> TestResult<()> {
    let source = InMemoryActiveRelations::empty();
    let subject = SubjectRef::new("account", "acct_123")?;
    let tenant_a = TenantId::new("tenant-a")?;
    let tenant_b = TenantId::new("tenant-b")?;
    let at = ts("2026-01-01T00:00:00Z")?;

    source.insert_for_spec::<TrustedTag>(
        tenant_a.clone(),
        Uuid::from_u128(10),
        subject.clone(),
        at,
        BTreeMap::new(),
    )?;
    source.insert_for_spec::<TrustedTag>(
        tenant_b.clone(),
        Uuid::from_u128(10),
        subject.clone(),
        at,
        BTreeMap::new(),
    )?;

    assert_eq!(source.active_for_subject(&tenant_a, &subject)?.len(), 1);
    assert_eq!(source.active_for_subject(&tenant_b, &subject)?.len(), 1);

    let store = InMemoryKeepsakeStore::empty();
    let mut first = apply_command(Uuid::from_u128(20), subject, TrustedTag::ID, at)?;
    first.tenant_id = tenant_a.clone();
    let mut second = first.clone();
    second.tenant_id = tenant_b.clone();
    store.apply(&first)?;
    store.apply(&second)?;
    assert!(store.get(&tenant_a, first.id)?.is_some());
    assert!(store.get(&tenant_b, second.id)?.is_some());
    Ok(())
}

#[test]
fn inserts_active_for_spec_with_explicit_id_time_and_empty_metadata() -> TestResult<()> {
    let source = InMemoryActiveRelations::empty();
    let subject = SubjectRef::new("account", "acct_123")?;
    let at = ts("2026-01-01T00:00:00Z")?;

    let tenant = tenant()?;
    source.insert_active_for_spec::<TrustedTag>(
        tenant.clone(),
        0xaaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa,
        subject.clone(),
        at,
    )?;

    let active = source.active_for_subject(&tenant, &subject)?;
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
            tenant()?,
            Uuid::from_u128(0xbbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb),
            subject.clone(),
            at,
        )
        .with_attribute("ticket", "case-1")
        .with_attributes([("source", "fixture")]),
    )?;

    let active = source.active_for_subject(&tenant()?, &subject)?;
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

    assert_eq!(
        store.active_for_subject(&command.tenant_id, &subject)?,
        vec![keepsake]
    );
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
    let relation = RelationDefinition::from_spec::<TrustedTag>(tenant()?, at)?;
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
fn keepsake_store_rejects_new_apply_for_disabled_relation_without_writing() -> TestResult<()> {
    let store = InMemoryKeepsakeStore::empty();
    let subject = SubjectRef::new("account", "acct_123")?;
    let at = ts("2026-01-01T00:00:00Z")?;
    let relation = RelationDefinition::disabled(
        tenant()?,
        TrustedTag::ID,
        TrustedTag::KEY.to_relation_key()?,
        ExpiryPolicy::ManualOnly,
    )?;
    let command = apply_command(Uuid::from_u128(100), subject.clone(), relation.id, at)?;

    let error = store.apply_with_relation(&command, &relation);

    assert!(matches!(
        error,
        Err(InMemoryKeepsakeStoreError::Keepsake(
            KeepsakeError::RelationDisabled { relation_id }
        )) if relation_id == relation.id
    ));
    assert!(store.get(&command.tenant_id, command.id)?.is_none());
    assert!(
        store
            .active_for_subject(&command.tenant_id, &subject)?
            .is_empty()
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
    let revoke = RevokeKeepsake::new(
        keepsake.tenant_id().clone(),
        keepsake.id(),
        ts("2026-01-02T00:00:00Z")?,
        context()?,
    );

    let revoked = store.revoke(&revoke)?;

    assert!(revoked.is_revoked());
    assert!(
        store
            .active_for_subject(&revoke.tenant_id, &subject)?
            .is_empty()
    );
    Ok(())
}

#[test]
fn keepsake_store_rejects_revoking_already_terminal_keepsake() -> TestResult<()> {
    let store = InMemoryKeepsakeStore::empty();
    let subject = SubjectRef::new("account", "acct_123")?;
    let at = ts("2026-01-01T00:00:00Z")?;
    let command = apply_command(Uuid::from_u128(100), subject, TrustedTag::ID, at)?;
    let keepsake = store.apply(&command)?;
    let revoke = RevokeKeepsake::new(
        keepsake.tenant_id().clone(),
        keepsake.id(),
        ts("2026-01-02T00:00:00Z")?,
        context()?,
    );

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

    assert_eq!(store.get(&command.tenant_id, id)?, None);
    let keepsake = store.apply(&command)?;
    assert_eq!(store.get(&command.tenant_id, id)?, Some(keepsake));
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
    provider.insert_snapshot(
        keepsake.tenant_id().clone(),
        keepsake.id(),
        snapshot.clone(),
    )?;

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
        tenant()?,
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
    provider.insert_snapshot(keepsake.tenant_id().clone(), keepsake.id(), snapshot)?;

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
