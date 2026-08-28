# Audit events

Audit is durable history. Logging is diagnostic. Metrics are aggregate health
signals. Keepsake keeps these concerns separate through its typed event model
and `AuditSink` trait.

In the core crate, `AuditEvent` is a validated, deterministic Rust value.
`AuditEventId` is generated before persistence and must be retained when a
caller retries the same logical operation. `AuditContext` carries application
values such as an operator id, ticket id, reason code, tenant id, or request
id. The core crate does not choose a product vocabulary or tracing stack.

The 2.0 SQLx adapter serializes the complete event to exact JSON bytes and
enqueues one CloudEvents-compatible Dovecote event in the same SQL transaction
as the Keepsake mutation. Its defaults are:

- stream: `keepsake-audit`;
- type: `keepsake.audit_event_recorded`;
- content type: `application/json`.

The application must configure a stable absolute source URI. The event id is
`keepsake-audit-<audit event id>`. Consumers deduplicate at the CloudEvents
`(source, id)` boundary because delivery remains at least once.

## Reading history and publishing

Keepsake does not maintain SQL audit tables, normalized context rows, a second
outbox, or project-specific claim/ack/release methods. Those 1.x APIs are not
part of the 2.0 SQLx surface.

Use the selected Dovecote SQLx adapter for live paging or a finite snapshot.
Pass each stored event through Keepsake's decoder so a page cannot silently
mix another source, stream, event type, or payload contract into typed history:

```rust
let config = keepsake_sqlx::DovecoteAuditConfig::new("https://example.invalid/keepsake")?;
let page = dovecote_sqlx_postgres::page(&pool, after_row_id, limit).await?;
for stored in page {
    let event = keepsake_sqlx::decode_audit_event(&config, stored.event())?;
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
