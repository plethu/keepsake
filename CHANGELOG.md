# Changelog

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
