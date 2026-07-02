use chrono::DateTime;
use keepsake::SubjectRef;
use sqlx::postgres::PgPoolOptions;

use super::support::parse_state;
use super::*;

fn ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    Chrono(#[from] chrono::ParseError),

    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),

    #[error(transparent)]
    Repository(#[from] RepositoryError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[tokio::test]
async fn timestamp_scoped_repository_reuses_explicit_timestamp() -> Result<(), TestError> {
    let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/keepsake")?;
    let repo = KeepsakeRepository::new(pool);
    let at = ts("2026-01-02T00:00:00Z")?;
    let timed_repo = repo.at(at);

    assert_eq!(timed_repo.timestamp(), at);
    Ok(())
}

#[tokio::test]
async fn active_relations_for_subject_by_keys_short_circuits_empty_keys() -> Result<(), TestError> {
    let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/keepsake")?;
    let repo = KeepsakeRepository::new(pool);
    let subject = SubjectRef::new("account", "acct_123")?;

    let active = repo
        .active_relations_for_subject_by_keys(&subject, &[])
        .await?;

    assert!(active.is_empty());
    Ok(())
}

#[test]
fn membership_cursor_serializes_for_api_boundaries() -> RepositoryResult<()> {
    let cursor = MembershipCursor {
        subject_kind: "account".to_owned(),
        subject_id: "acct_123".to_owned(),
        keepsake_id: Uuid::nil(),
    };

    let encoded = serde_json::to_string(&cursor)?;
    let decoded = serde_json::from_str::<MembershipCursor>(&encoded)?;

    assert_eq!(decoded, cursor);
    Ok(())
}

#[test]
fn timed_expiry_candidate_serializes_with_stable_field_names() -> Result<(), TestError> {
    let candidate = TimedExpiryCandidate {
        keepsake_id: Uuid::nil(),
        relation_id: Uuid::nil(),
        subject_kind: "account".to_owned(),
        subject_id: "acct_123".to_owned(),
        due_at: ts("2026-01-02T00:00:00Z")?,
    };

    let encoded = serde_json::to_value(&candidate)?;

    assert_eq!(
        encoded,
        serde_json::json!({
            "keepsake_id": "00000000-0000-0000-0000-000000000000",
            "relation_id": "00000000-0000-0000-0000-000000000000",
            "subject_kind": "account",
            "subject_id": "acct_123",
            "due_at": "2026-01-02T00:00:00Z"
        })
    );
    assert_eq!(
        serde_json::from_value::<TimedExpiryCandidate>(encoded)?,
        candidate
    );
    Ok(())
}

#[test]
fn fulfilled_expiry_candidate_serializes_with_stable_field_names() -> Result<(), TestError> {
    let candidate = FulfilledExpiryCandidate {
        keepsake_id: Uuid::nil(),
        relation_id: Uuid::nil(),
        subject_kind: "account".to_owned(),
        subject_id: "acct_123".to_owned(),
        expiry_policy: keepsake::ExpiryPolicy::WhenFulfilled {
            policy: keepsake::FulfillmentPolicy::CounterAtLeast {
                key: "steps".to_owned(),
                threshold: 3,
            },
        },
    };

    let encoded = serde_json::to_value(&candidate)?;

    assert_eq!(
        encoded,
        serde_json::json!({
            "keepsake_id": "00000000-0000-0000-0000-000000000000",
            "relation_id": "00000000-0000-0000-0000-000000000000",
            "subject_kind": "account",
            "subject_id": "acct_123",
            "expiry_policy": {
                "type": "when_fulfilled",
                "policy": {
                    "type": "counter_at_least",
                    "key": "steps",
                    "threshold": 3
                }
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<FulfilledExpiryCandidate>(encoded)?,
        candidate
    );
    Ok(())
}

#[test]
fn parse_state_rejects_unknown_values() {
    let error = parse_state("archived".to_owned())
        .map(|_| ())
        .map_err(|error| error.to_string());

    assert_eq!(error, Err("unknown lifecycle state archived".to_owned()));
}
