# Keepsake 2.0 API removal audit

This is the intentional 1.x-to-2.0 SQLx API boundary. The core typed audit
model remains; only the duplicate SQL persistence and delivery surface is
removed.

Removed from `keepsake-sqlx`:

- `AuditEventRecord` and `AuditCursor`;
- `AuditOutboxRecord` and `AuditOutboxCursor`;
- `append_audit_event`;
- `audit_events_for_keepsake` and `audit_events_for_relation`;
- `audit_outbox`;
- `claim_audit_outbox`, `ack_audit_outbox`, and `release_audit_outbox`;
- Keepsake-owned audit tables, context-attribute tables, and outbox tables from
  the clean 2.0 baseline.

Retained in the core crate:

- `AuditEvent`, `AuditEventId`, `AuditEventType`, `AuditDecision`, and
  `AuditContext`;
- `AuditSink` and in-memory testing support;
- typed lifecycle commands and relation-state queries.

Replacement: use the selected Dovecote SQLx adapter for event live/snapshot
paging and delivery claims. Decode event data as `keepsake::AuditEvent`; do not
recreate Keepsake-specific SQL filters or delivery state.
