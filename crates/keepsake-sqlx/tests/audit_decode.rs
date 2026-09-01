//! Contract tests for typed Keepsake audit history decoding.

use std::error::Error;

use keepsake::{
    AUDIT_PAYLOAD_SCHEMA_VERSION, ActorRef, AuditContext, AuditDecision, AuditEvent, AuditEventId,
    AuditEventType, KeepsakeId, RelationId, SubjectRef, TenantId,
};
use keepsake_sqlx::{AuditEventDecodeError, DovecoteAuditConfig, decode_audit_event};
use time::OffsetDateTime;

type TestResult<T> = Result<T, Box<dyn Error>>;

fn audit_event() -> TestResult<AuditEvent> {
    Ok(AuditEvent {
        schema_version: AUDIT_PAYLOAD_SCHEMA_VERSION,
        tenant_id: TenantId::new("tenant-test")?,
        id: AuditEventId::from_uuid(uuid::Uuid::nil()),
        event_type: AuditEventType::Apply,
        at: OffsetDateTime::parse(
            "2023-11-14T22:13:20.123456Z",
            &time::format_description::well_known::Rfc3339,
        )?,
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
    time::OffsetDateTime::from_unix_timestamp(event.at.unix_timestamp())?
        .replace_nanosecond(event.at.nanosecond())
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

fn paged_event(
    tenant_id: dovecote::TenantId,
    event: dovecote::StoredEvent,
    occurred_at: time::OffsetDateTime,
) -> TestResult<dovecote::PagedEvent> {
    Ok(dovecote::PagedEvent::new(
        tenant_id,
        dovecote::RowId::new(1)?,
        event,
        occurred_at,
        dovecote::DeliverySnapshot::pending(occurred_at, dovecote::AttemptCount::new(0)?, None)?,
    )?)
}

fn valid_paged(
    config: &DovecoteAuditConfig,
    event: &AuditEvent,
) -> TestResult<dovecote::PagedEvent> {
    let occurred_at = event_time(event)?;
    let stored = stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        occurred_at,
        serde_json::to_vec(event)?,
    )?;
    paged_event(
        dovecote::TenantId::new(event.tenant_id.as_str())?,
        stored,
        occurred_at,
    )
}

#[test]
fn decoder_projects_a_current_event() -> TestResult<()> {
    let config = DovecoteAuditConfig::new("https://tests.invalid/keepsake")?;
    let event = audit_event()?;
    let paged = valid_paged(&config, &event)?;

    assert_eq!(decode_audit_event(&config, &paged)?, event);
    Ok(())
}

#[test]
fn decoder_requires_an_explicit_supported_payload_schema() -> TestResult<()> {
    let config = DovecoteAuditConfig::new("https://tests.invalid/keepsake")?;
    let event = audit_event()?;
    let current = serde_json::to_value(&event)?;
    assert_eq!(
        current
            .get("schema_version")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(AUDIT_PAYLOAD_SCHEMA_VERSION))
    );

    let current_page = valid_paged(&config, &event)?;
    assert_eq!(decode_audit_event(&config, &current_page)?, event);

    let mut explicit_v3 = current.clone();
    explicit_v3["schema_version"] = serde_json::json!(3);
    let stored = stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        event_time(&event)?,
        serde_json::to_vec(&explicit_v3)?,
    )?;
    let paged = paged_event(
        dovecote::TenantId::new("tenant-test")?,
        stored,
        event_time(&event)?,
    )?;
    assert!(matches!(
        decode_audit_event(&config, &paged),
        Err(AuditEventDecodeError::LegacyPayload { schema_version: 3 })
    ));

    let mut omitted_v3 = current.clone();
    omitted_v3
        .as_object_mut()
        .ok_or_else(|| std::io::Error::other("audit event did not serialize as an object"))?
        .remove("schema_version");
    let stored = stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        event_time(&event)?,
        serde_json::to_vec(&omitted_v3)?,
    )?;
    let paged = paged_event(
        dovecote::TenantId::new("tenant-test")?,
        stored,
        event_time(&event)?,
    )?;
    assert!(matches!(
        decode_audit_event(&config, &paged),
        Err(AuditEventDecodeError::LegacyPayload { schema_version: 3 })
    ));

    let mut unknown = current;
    unknown["schema_version"] = serde_json::json!(99);
    let stored = stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        event_time(&event)?,
        serde_json::to_vec(&unknown)?,
    )?;
    let paged = paged_event(
        dovecote::TenantId::new("tenant-test")?,
        stored,
        event_time(&event)?,
    )?;
    assert!(matches!(
        decode_audit_event(&config, &paged),
        Err(AuditEventDecodeError::UnknownPayloadVersion { schema_version: 99 })
    ));

    let future_shape = serde_json::json!({"schema_version": 99});
    let stored = stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        event_time(&event)?,
        serde_json::to_vec(&future_shape)?,
    )?;
    let paged = paged_event(
        dovecote::TenantId::new("tenant-test")?,
        stored,
        event_time(&event)?,
    )?;
    assert!(matches!(
        decode_audit_event(&config, &paged),
        Err(AuditEventDecodeError::UnknownPayloadVersion { schema_version: 99 })
    ));
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
        let paged = paged_event(dovecote::TenantId::new("tenant-test")?, stored, occurred_at)?;
        assert!(matches!(
            decode_audit_event(&config, &paged),
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
        let paged = paged_event(
            dovecote::TenantId::new("tenant-test")?,
            stored,
            event_time(&event)?,
        )?;
        assert!(matches!(
            decode_audit_event(&config, &paged),
            Err(AuditEventDecodeError::LegacyEvent { event_id: id }) if id == event_id
        ));
    }
    Ok(())
}

#[test]
fn decoder_rejects_storage_payload_tenant_mismatch() -> TestResult<()> {
    let config = DovecoteAuditConfig::new("https://tests.invalid/keepsake")?;
    let event = audit_event()?;
    let occurred_at = event_time(&event)?;
    let stored = stored_event(
        &format!("keepsake-audit-{}", event.id.as_uuid()),
        config.source(),
        config.stream(),
        config.event_type(),
        occurred_at,
        serde_json::to_vec(&event)?,
    )?;
    let paged = paged_event(
        dovecote::TenantId::new("tenant-other")?,
        stored,
        occurred_at,
    )?;

    assert!(matches!(
        decode_audit_event(&config, &paged),
        Err(AuditEventDecodeError::TenantMismatch {
            storage_tenant,
            payload_tenant,
        }) if storage_tenant == "tenant-other" && payload_tenant == "tenant-test"
    ));
    Ok(())
}
