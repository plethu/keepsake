# Versioning

Keepsake 6.0 provides caller-owned relation lifecycle
and effective authorization integration. It retains the v4 database track
and numeric JSON payload schema version 4. Crate major versions and durable
schema versions are separate contracts.

## SQLx adapter 6.1.0

`keepsake-sqlx` 6.1.0 adds tenant-scoped exact assignment reads and
relation-scoped timed expiry on PostgreSQL, MySQL and SQLite, including
caller-owned transaction variants. Existing signatures, features and the Rust
1.94 MSRV are unchanged. The `keepsake` core remains at 6.0.0; no database
migration or persisted-data rewrite is required.

This adapter-only minor release uses the `keepsake-sqlx-v6.1.0` Git tag.
Applications using the new methods should require `keepsake-sqlx = "6.1"`.

## Upgrading from published 5.0

This release is a major API change, not a database-format rewrite.
`ApplyKeepsake` literals add `expiry: None` to use the definition policy;
constructors already provide that default. Use `with_expiry` for an individually
assigned deadline. `AuditEvent` literals add `command: None` for historical or
system occurrences; new lifecycle storage writes capture complete typed commands.
`AppliedKeepsake.replayed` distinguishes a committed retry from a new application
or prevented duplicate. Do not repeat business effects when it is true.

Current domain and audit schemas remain version 4. Existing published migration
bytes are unchanged, terminal/delivered rows remain history, and existing version-5
payloads remain readable. Old events without complete command evidence cannot be
used as exact receipts: the SQL adapter returns `ReceiptEvidenceUnavailable`.
Keep their original bytes and route recovery through the application's existing
records. Do not backfill invented commands or republish historical notifications.

See [transactional lifecycle](../reference/transactional-lifecycle.md) for
transaction methods, backend isolation, expiry reconciliation and retry identity.

## Upgrading from 4.0

Replace `schema_version: AUDIT_PAYLOAD_SCHEMA_VERSION` or `schema_version: 4`
in `AuditEvent` literals with
`schema_version: AuditPayloadSchemaVersion::CURRENT`. Import
`keepsake::AuditPayloadSchemaVersion` where those events are constructed.

`AuditEvent.schema_version` can no longer hold a legacy or future version.
The numeric `AUDIT_PAYLOAD_SCHEMA_VERSION` constant remains available for
routing raw payloads before decoding. Valid existing v4 JSON is unchanged.
Existing valid v4 databases need no new migration; run `check_schema()` before
serving requests to detect incomplete identifier constraints.

## Semver

- **Major**: breaking changes to public API types, command semantics, storage
  record layout, or migration ordering. Keepsake 4.0 replaces public
  `chrono::DateTime<Utc>` with `time::OffsetDateTime` and adds the v4
  identifier/schema contract. The historical Keepsake 2.0 release removed the
  maintained SQL audit repositories, audit history cursors, outbox paging, and
  claim/ack/release methods.
- **Minor**: additive API, new query helpers, new migrations that existing code
  can ignore until adopted.
- **Patch**: bug fixes and non-breaking schema corrections.

Use the core version required by the SQLx adapter: `keepsake-sqlx` 6.1
retains its `keepsake` 6.0 dependency. Additive adapter releases need not
republish an unchanged core. Select the matching Dovecote adapter and apply
both schemas before deploying code that depends on the audit contract.

## Database upgrades

Follow the [migration guide](migrations.md) for clean installation or an
existing database. It owns track selection, schema ordering, tenant activation,
and history import. Never edit or reorder published migrations.
