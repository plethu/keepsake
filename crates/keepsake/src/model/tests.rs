use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::ExpiryPolicy;

use super::*;

type TestResult<T> = std::result::Result<T, TestError>;

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),

    #[error(transparent)]
    Keepsake(#[from] KeepsakeError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

fn ts(value: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

fn record(expiry: ExpiryPolicy, state: LifecycleState) -> TestResult<KeepsakeRecord> {
    Ok(KeepsakeRecord {
        id: Uuid::nil(),
        subject: SubjectRef::new("user", "u_1")?,
        relation_id: Uuid::from_u128(1),
        state,
        expires_at: expiry.timed_expiry(),
        expiry,
        applied_at: ts("2026-01-01T00:00:00Z")?,
        fulfilled_at: None,
        revoked_at: None,
        metadata: BTreeMap::new(),
    })
}

#[test]
fn relation_definition_enabled_and_disabled_helpers_set_state() -> TestResult<()> {
    let key = RelationKey::new("tag", "trusted")?;
    let enabled = RelationDefinition::enabled(Uuid::nil(), key.clone(), ExpiryPolicy::ManualOnly)?;
    let disabled = RelationDefinition::disabled(Uuid::nil(), key, ExpiryPolicy::ManualOnly)?;

    assert!(enabled.enabled);
    assert!(!disabled.enabled);
    Ok(())
}

#[test]
fn relation_key_components_validate_independently() {
    assert_eq!(
        RelationKind::new(" ").map_err(|error| error.to_string()),
        Err("relation.kind must not be empty".to_owned())
    );
    assert_eq!(
        RelationName::new(" ").map_err(|error| error.to_string()),
        Err("relation.name must not be empty".to_owned())
    );
}

#[test]
fn relation_key_components_format_for_logs_and_labels() -> TestResult<()> {
    let kind = RelationKind::new("sanction")?;
    let name = RelationName::new("mute_24h")?;

    assert_eq!(kind.as_ref(), "sanction");
    assert_eq!(kind.to_string(), "sanction");
    assert_eq!(name.as_ref(), "mute_24h");
    assert_eq!(name.to_string(), "mute_24h");
    Ok(())
}

crate::relation_spec! {
    struct TrustedTag {
        id: 0;
        key: ("tag", "trusted");
        expiry(_at) => ExpiryPolicy::ManualOnly;
    }
}

crate::relation_spec! {
    struct DisabledTimedSanction {
        id: 0x018f_0000_0000_7000_8000_0000_0000_0003;
        key: ("sanction", "review_hold");
        enabled: false;
        expiry(at) => ExpiryPolicy::At { timestamp: at };
    }
}

#[test]
fn relation_definition_can_be_built_from_spec() -> TestResult<()> {
    let definition = RelationDefinition::from_spec::<TrustedTag>(
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .map(|timestamp| timestamp.with_timezone(&Utc))?,
    )?;

    assert_eq!(definition.id, Uuid::nil());
    assert_eq!(definition.key.kind(), "tag");
    assert_eq!(definition.key.name(), "trusted");
    assert!(definition.enabled);
    assert_eq!(definition.expiry, ExpiryPolicy::ManualOnly);
    Ok(())
}

#[test]
fn relation_spec_macro_supports_disabled_timed_specs() -> TestResult<()> {
    let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .map(|timestamp| timestamp.with_timezone(&Utc))?;
    let definition = RelationDefinition::from_spec::<DisabledTimedSanction>(at)?;

    assert_eq!(
        definition.id,
        Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0003)
    );
    assert_eq!(definition.key.kind(), "sanction");
    assert_eq!(definition.key.name(), "review_hold");
    assert!(!definition.enabled);
    assert_eq!(definition.expiry, ExpiryPolicy::At { timestamp: at });
    Ok(())
}

#[test]
fn applied_keepsake_exposes_common_accessors() -> TestResult<()> {
    let relation = RelationDefinition::enabled(
        Uuid::from_u128(1),
        RelationKey::new("tag", "trusted")?,
        ExpiryPolicy::At {
            timestamp: ts("2026-01-02T00:00:00Z")?,
        },
    )?;
    let keepsake = Keepsake::applied(
        Uuid::from_u128(2),
        SubjectRef::new("user", "u_1")?,
        &relation,
        ts("2026-01-01T00:00:00Z")?,
        BTreeMap::from([("source".to_owned(), "test".to_owned())]),
    )?;

    assert_eq!(keepsake.id(), Uuid::from_u128(2));
    assert_eq!(keepsake.subject().id, "u_1");
    assert_eq!(keepsake.relation_id(), relation.id);
    assert_eq!(keepsake.state(), LifecycleState::Applied);
    assert!(matches!(keepsake.lifecycle(), KeepsakeLifecycle::Applied));
    assert!(keepsake.is_active());
    assert!(!keepsake.is_revoked());
    assert!(!keepsake.is_expired());
    assert_eq!(keepsake.expires_at(), Some(ts("2026-01-02T00:00:00Z")?));
    assert_eq!(keepsake.ended_at(), None);
    assert_eq!(keepsake.revoked_at(), None);
    assert_eq!(keepsake.expired_at(), None);
    assert_eq!(keepsake.fulfilled_at(), None);
    assert_eq!(
        keepsake.metadata().get("source").map(String::as_str),
        Some("test")
    );
    Ok(())
}

#[test]
fn valid_flat_records_convert_to_typed_lifecycles() -> TestResult<()> {
    let mut revoked = record(ExpiryPolicy::ManualOnly, LifecycleState::Revoked)?;
    revoked.revoked_at = Some(ts("2026-01-03T00:00:00Z")?);
    let revoked = Keepsake::try_from(revoked)?;
    assert!(revoked.is_revoked());
    assert_eq!(revoked.ended_at(), Some(ts("2026-01-03T00:00:00Z")?));

    let timed_expiry = Keepsake::try_from(record(
        ExpiryPolicy::At {
            timestamp: ts("2026-01-02T00:00:00Z")?,
        },
        LifecycleState::Expired,
    )?)?;
    assert_eq!(timed_expiry.state(), LifecycleState::Expired);
    assert_eq!(timed_expiry.expired_at(), Some(ts("2026-01-02T00:00:00Z")?));
    assert_eq!(timed_expiry.fulfilled_at(), None);

    let mut fulfilled = record(
        ExpiryPolicy::WhenFulfilled {
            policy: crate::policy::FulfillmentPolicy::CounterAtLeast {
                key: "messages_sent".to_owned(),
                threshold: 3,
            },
        },
        LifecycleState::Expired,
    )?;
    fulfilled.fulfilled_at = Some(ts("2026-01-04T00:00:00Z")?);
    let fulfilled = Keepsake::try_from(fulfilled)?;
    assert_eq!(fulfilled.expired_at(), Some(ts("2026-01-04T00:00:00Z")?));
    assert_eq!(fulfilled.fulfilled_at(), Some(ts("2026-01-04T00:00:00Z")?));
    Ok(())
}

#[test]
fn invalid_flat_records_are_rejected() -> TestResult<()> {
    let mut manual_expired = record(ExpiryPolicy::ManualOnly, LifecycleState::Expired)?;
    assert!(Keepsake::try_from(manual_expired.clone()).is_err());

    let mut revoked_with_fulfilled = record(ExpiryPolicy::ManualOnly, LifecycleState::Revoked)?;
    revoked_with_fulfilled.revoked_at = Some(ts("2026-01-03T00:00:00Z")?);
    revoked_with_fulfilled.fulfilled_at = Some(ts("2026-01-03T00:00:00Z")?);
    assert!(Keepsake::try_from(revoked_with_fulfilled).is_err());

    let mut applied_with_terminal = record(ExpiryPolicy::ManualOnly, LifecycleState::Applied)?;
    applied_with_terminal.revoked_at = Some(ts("2026-01-03T00:00:00Z")?);
    assert!(Keepsake::try_from(applied_with_terminal).is_err());

    let mut fulfilled_with_expires = record(
        ExpiryPolicy::WhenFulfilled {
            policy: crate::policy::FulfillmentPolicy::CounterAtLeast {
                key: "messages_sent".to_owned(),
                threshold: 3,
            },
        },
        LifecycleState::Expired,
    )?;
    fulfilled_with_expires.expires_at = Some(ts("2026-01-02T00:00:00Z")?);
    fulfilled_with_expires.fulfilled_at = Some(ts("2026-01-03T00:00:00Z")?);
    assert!(Keepsake::try_from(fulfilled_with_expires).is_err());

    manual_expired.state = LifecycleState::Revoked;
    assert!(Keepsake::try_from(manual_expired).is_err());
    Ok(())
}

#[test]
fn serde_uses_flat_record_shape_and_validates_on_read() -> TestResult<()> {
    let valid = record(ExpiryPolicy::ManualOnly, LifecycleState::Applied)?;
    let json = serde_json::to_string(&valid)?;
    let keepsake = serde_json::from_str::<Keepsake>(&json)?;
    let round_trip = serde_json::to_value(&keepsake)?;

    assert_eq!(keepsake.state(), LifecycleState::Applied);
    assert_eq!(round_trip, serde_json::to_value(&valid)?);

    let mut invalid = valid;
    invalid.revoked_at = Some(ts("2026-01-03T00:00:00Z")?);
    let json = serde_json::to_string(&invalid)?;
    assert!(serde_json::from_str::<Keepsake>(&json).is_err());
    Ok(())
}
