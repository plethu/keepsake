# Versioning

Keepsake 6.0 provides caller-owned relation lifecycle
and effective authorization integration. It retains the v4 database track
and numeric JSON payload schema version 4. Crate major versions and durable
schema versions are separate contracts.

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
The source dependency graph and executable consumer can be verified locally;
package publication and registry resolution are separate release gates. Verify
the package versions used by your service before deployment.

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

Pin `keepsake` and `keepsake-sqlx` to the same release. Select the matching
Dovecote adapter and apply both schemas before deploying code that depends on
the new audit contract.

## Upgrade checklist

- Read the changelog for API changes, new migration files, changed indexes, and
  required ordering.
- For new databases, apply the clean 4.0 domain baseline, v4 contract, and
  Dovecote schema.
- For existing clean v3 databases with their SQLx baseline receipt, run
  `repo.migrate()` to apply the additive v4 track. For operator-managed tenant
  activation, use `repo.upgrade_identifier_contract()` instead. Resolve the migration's incompatible-row preflight before deploying
  4.0 writers; do not edit historical v3 SQL.
- For 1.x databases, select `upgrade_migrate()` explicitly and complete the
  documented history import before deploying the historical 2.0 writers.
- Never edit or reorder published historical migrations.
- Test request paths and workers that use changed query helpers.

Embedded migrations define each track's required domain schema. Your service
decides when and how to apply it; Dovecote migrations are selected from the
matching Dovecote SQLx adapter. The adapter refuses a track mismatch rather
than guessing or dropping tables.
