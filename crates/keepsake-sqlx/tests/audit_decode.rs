//! Contract tests for typed Keepsake audit history decoding.

use std::error::Error;

use chrono::{DateTime, Utc};
use keepsake::{
    ActorRef, AuditContext, AuditDecision, AuditEvent, AuditEventId, AuditEventType, KeepsakeId,
    RelationId, SubjectRef,
};
use keepsake_sqlx::{AuditEventDecodeError, DovecoteAuditConfig, decode_audit_event};

type TestResult<T> = Result<T, Box<dyn Error>>;

fn audit_event() -> TestResult<AuditEvent> {
    Ok(AuditEvent {
        id: AuditEventId::from_uuid(uuid::Uuid::nil()),
        event_type: AuditEventType::Apply,
        at: DateTime::parse_from_rfc3339("2023-11-14T22:13:20.123456Z")?.with_timezone(&Utc),
        actor: ActorRef::new("system", "test")?,
        keepsake_id: KeepsakeId::nil(),
        subject: SubjectRef::new("account", "acct-1")?,
        relation_id: RelationId::nil(),
        decision: AuditDecision::Applied {
            duplicate_prevented: false,
        },
        context: AuditContext::default(),
    })
}

fn event_time(event: &AuditEvent) -> Result<time::OffsetDateTime, time::error::ComponentRange> {
    time::OffsetDateTime::from_unix_timestamp(event.at.timestamp())?
        .replace_nanosecond(event.at.timestamp_subsec_nanos())
}

fn stored_event(
    event_id: &str,
    source: &str,
    stream: &str,
    event_type: &str,
    time: time::OffsetDateTime,
    payload: Vec<u8>,
) -> TestResult<dovecote::StoredEvent> {
    Ok(dovecote::NewEvent::builder(
        dovecote::StreamName::new(stream)?,
        dovecote::EventId::new(event_id)?,
        dovecote::EventSource::new(source)?,
        dovecote::EventType::new(event_type)?,
    )
    .time(time)
    .datacontenttype(dovecote::ContentType::new("application/json")?)
    .data(dovecote::EventData::json(payload)?)
    .build()?
    .into_stored()?)
}

fn valid_stored(
    config: &DovecoteAuditConfig,
    event: &AuditEvent,
) -> TestResult<dovecote::StoredEvent> {
    stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        event_time(event)?,
        serde_json::to_vec(event)?,
    )
}

#[test]
fn decoder_projects_a_current_event() -> TestResult<()> {
    let config = DovecoteAuditConfig::new("https://tests.invalid/keepsake")?;
    let event = audit_event()?;
    let stored = valid_stored(&config, &event)?;

    assert_eq!(decode_audit_event(&config, &stored)?, event);
    Ok(())
}

#[test]
fn decoder_validates_the_complete_current_envelope() -> TestResult<()> {
    let config = DovecoteAuditConfig::new("https://tests.invalid/keepsake")?;
    let event = audit_event()?;
    let payload = serde_json::to_vec(&event)?;
    let expected_id = format!("keepsake-audit-{}", event.id.as_uuid());
    let cases = [
        (
            "source",
            "keepsake-audit",
            config.event_type(),
            "keepsake-audit-00000000-0000-0000-0000-000000000000",
            "https://other.invalid/keepsake",
            event_time(&event)?,
        ),
        (
            "stream",
            "other-stream",
            config.event_type(),
            expected_id.as_str(),
            config.source(),
            event_time(&event)?,
        ),
        (
            "type",
            config.stream(),
            "keepsake.other",
            expected_id.as_str(),
            config.source(),
            event_time(&event)?,
        ),
        (
            "event identity",
            config.stream(),
            config.event_type(),
            "keepsake-audit-00000000-0000-0000-0000-000000000001",
            config.source(),
            event_time(&event)?,
        ),
        (
            "occurrence time",
            config.stream(),
            config.event_type(),
            expected_id.as_str(),
            config.source(),
            event_time(&event)? + time::Duration::SECOND,
        ),
    ];

    for (field, stream, event_type, event_id, source, occurred_at) in cases {
        let stored = stored_event(
            event_id,
            source,
            stream,
            event_type,
            occurred_at,
            payload.clone(),
        )?;
        assert!(matches!(
            decode_audit_event(&config, &stored),
            Err(AuditEventDecodeError::InvalidEnvelope { field: actual }) if actual == field
        ));
    }
    Ok(())
}

#[test]
fn decoder_reports_both_migrated_identity_shapes() -> TestResult<()> {
    let config = DovecoteAuditConfig::new("https://tests.invalid/keepsake")?;
    let event = audit_event()?;
    for event_id in ["keepsake-outbox-7", "keepsake-audit-legacy-9"] {
        let stored = stored_event(
            event_id,
            config.source(),
            config.stream(),
            config.event_type(),
            event_time(&event)?,
            br#"{"legacy":true}"#.to_vec(),
        )?;
        assert!(matches!(
            decode_audit_event(&config, &stored),
            Err(AuditEventDecodeError::LegacyEvent { event_id: id }) if id == event_id
        ));
    }
    Ok(())
}
