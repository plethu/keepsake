# Changelog

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
