# Changelog

## Unreleased

- Added `audit_events_for_keepsake` and `audit_events_for_relation` read helpers
  to the SQLx adapter (Postgres, MySQL, SQLite) with keyset pagination via
  `AuditCursor`, returning `AuditEventRecord`s with hydrated context attributes.
- Added `AuditEventType::from_storage_label` as the inverse of `as_str`.
- Batched audit context attribute writes into a single statement per event
  instead of one statement per attribute.
- Indexed the fulfillment expiry sweep: partial indexes on Postgres and SQLite,
  and a stored generated column plus index on MySQL.

## 0.5.1 - 2026-06-20

- Fixed MySQL lifecycle check constraints so the SQLx migration applies on
  MySQL 8.4.
- Added Docker-backed MySQL integration coverage to `make test-db`.

## 0.4.1 - 2026-06-20

- Added `ActiveRelationSeed` and `insert_active_for_spec` behind the `test`
  feature for deterministic typed relation seeding in adapter tests and
  examples.
- Documented in-memory relation seeding with explicit timestamps, relation
  instance ids, and optional metadata.

## 0.4.0 - 2026-06-20

- Added `ActiveRelationSource` as the canonical async read-side adapter seam for
  active relation lookups.
- Added `DynActiveRelationSource` as an explicit erased boundary for application
  composition, while keeping generic `S: ActiveRelationSource` as the primary
  integration shape.
- Moved `ActiveRelation` into the core crate with constructor-enforced
  keepsake/relation invariants and re-exported it from `keepsake-sqlx`.
- Added SQLx bounded active relation lookup by relation ids for typed
  `RelationSpec` integrations.
- Added `InMemoryActiveRelations` behind the core `test` feature for downstream
  adapter tests.
- Aligned `keepsake-sqlx` with SQLx 0.9.0 and raised the workspace Rust version
  to 1.94.
- Documented multi-tenant `SubjectRef` conventions and bounded active relation
  read paths.

## 0.3.0 - 2026-06-19

- Added typed audit event categories and audit-specific decisions.
- Added audited SQLx apply/revoke command helpers that write lifecycle and audit
  rows atomically.
- Added `append_audit_event` as an explicit SQLx escape hatch for audit events
  that do not have a built-in repository command.
- Made the SQLx mutation API command-first by replacing unaudited convenience
  mutation helpers with `apply(&ApplyKeepsake)` and `revoke(&RevokeKeepsake)`.
- Split large repository, model, and integration-test modules into smaller
  responsibility-focused files.
- Clarified audit and command documentation around the command-first SQLx API.

## 0.2.0 - 2026-06-19

- Added typed keepsake lifecycle invariants with flat serde/storage records,
  lifecycle accessors, fulfillment snapshots, and relation-spec helpers.
- Added SQL persistence guards for lifecycle state, terminal timestamps,
  subject validation, and deterministic expiry behavior.
- Split core model, SQL repository, and Postgres integration tests into focused
  private modules while preserving public API paths.
- Refreshed README, docs-site, examples, and crate versions for the 0.2.0
  release surface.

## 0.1.0 - 2026-06-18

- Initial workspace scaffold with core lifecycle model, SQLx/Postgres adapter,
  Docker-backed database test wiring, examples, and Starlight documentation.
