# Changelog

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
