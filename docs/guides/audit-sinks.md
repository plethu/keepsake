# Audit events

Audit is durable history. Logging is diagnostic. Metrics are aggregate health
signals. Keepsake keeps these concerns separate through its typed event model
and `AuditSink` trait.

In the core crate, `AuditEvent` is a validated, deterministic Rust value.
`AuditEventId` is generated before persistence and must be retained when a
caller retries the same logical operation. `AuditContext` carries application
values such as an operator id, ticket id, reason code, tenant id, or request
id. The core crate does not choose a product vocabulary or tracing stack.

The SQLx adapter serializes the complete event to exact JSON bytes and enqueues
one CloudEvents-compatible [Dovecote](https://github.com/plethu/dovecote) event
in the same transaction as the Keepsake mutation. Its defaults are:

- stream: `keepsake-audit`;
- type: `keepsake.audit_event_recorded`;
- content type: `application/json`.

The application must configure a stable absolute source URI. The event id is
`keepsake-audit-<audit event id>`. Consumers deduplicate at the tenant-scoped
Dovecote `(tenant_id, source, event_id)` boundary because delivery remains at
least once. A transport projection must preserve tenant routing alongside the
CloudEvents `(source, id)` pair.

Current Keepsake 4.0 payloads include the explicit
`schema_version: keepsake::AUDIT_PAYLOAD_SCHEMA_VERSION` discriminator (4).
The ordinary decoder accepts only that version. A payload that omits the field
or declares version 3 is returned as `AuditEventDecodeError::LegacyPayload` and
must go through an application-owned legacy decoder; an unknown version is
returned as `UnknownPayloadVersion`. The decoder also preserves the historical
outer-id rejection for `keepsake-outbox-N` and `keepsake-audit-legacy-N`, so a
changed timestamp or field shape cannot silently reinterpret legacy data.

## Reading history and publishing

Keepsake leaves SQL history, delivery state, and claim operations to Dovecote.
It has no second audit table or outbox.

Use the selected Dovecote SQLx adapter for live paging or a finite snapshot.
Pass each stored event through Keepsake's decoder so a page cannot silently
mix another source, stream, event type, or payload contract into typed history:

```rust
let config = keepsake_sqlx::DovecoteAuditConfig::new("https://example.invalid/keepsake")?;
let page = dovecote_sqlx_postgres::page(&pool, after_row_id, limit).await?;
for stored in page {
    let event = keepsake_sqlx::decode_audit_event(&config, &stored)?;
    // Use `stored.delivery()` for delivery state, not a Keepsake table.
}
```

The decoder recognizes migrated v1 outer identities such as
`keepsake-outbox-42` and `keepsake-audit-legacy-42`, but returns a typed
`LegacyEvent` error for them. Those payloads predate the current
`AuditEventId` field and require application-specific legacy handling; they must
not be cast to a current event by dropping or inventing identity data.

For a stable complete-history read, begin a Dovecote snapshot and call
`next_page` until it returns an empty page, then finish it. Publication workers
claim, finalize, retry, or quarantine through Dovecote's token-fenced APIs;
transport clients remain application-owned.

`InMemoryAuditSink` remains useful for core tests and non-SQL consumers. It is
not a second SQL persistence model and is not used by `keepsake-sqlx`.
