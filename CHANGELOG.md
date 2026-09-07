# Changelog

## 6.0.0 - 2026-09-07

- SQLite counter overflow is rejected without changing its value or observation time.
- SQLite legacy schema checks identify extra triggers by their exact owning table.
- Static relation keys reject the same Unicode edge whitespace as runtime keys.
- PostgreSQL examples keep connection errors out of external diagnostics.
- Strengthened public error documentation, lint/feature coverage and documentation
  gates; separated schema verification and test-provider responsibilities.

- Added caller-owned lifecycle, reconciliation and fulfillment transactions,
  scoped observations, conditional writes and complete command replay receipts.
- Added per-assignment expiry overrides and explicit authoritative-time effective
  relation evaluation. Persisted expiry reconciliation remains separate.
- **Breaking Rust API:** apply commands accept an expiry override; audit events
  carry optional typed command evidence; apply receipts identify committed replay.
  SQL transaction revokes return typed receipts and reconciliation returns IDs.
- Published migration bytes and domain/audit schema version 4 are unchanged.
  Existing history stays readable; old occurrences lacking command evidence
  cannot provide strict replay receipts. See the
  [transactional lifecycle contract](docs/reference/transactional-lifecycle.md).

## 5.0.0 - 2026-09-07

- **Breaking Rust API:** `AuditEvent.schema_version` now uses the opaque
  `AuditPayloadSchemaVersion` type. Use `AuditPayloadSchemaVersion::CURRENT`
  in event literals instead of a numeric value. Serialization always emits
  schema version 4; existing v4 audit payloads and databases remain compatible.
- Schema checks now compare complete v4 identifier predicates and SQLite
  trigger definitions, rejecting weakened or inactive constraints. Published
  migration files are unchanged; existing valid v4 schemas need no migration.
- The in-memory keepsake store rejects fresh applies to disabled relations
  without changing stored state.
- Fixed the quickstart and test-helper examples and added them to doctest
  coverage. `just test` now forwards Cargo test filters and runner arguments.
- Added pinned cargo-machete checks to the project gate and removed unused
  direct UUID dependencies from the Postgres examples.

## 4.0.0 - 2026-09-01

- **Breaking core API:** replaced public and internal `chrono::DateTime<Utc>`
  with `time::OffsetDateTime`; RFC3339 serde wire timestamps remain stable,
  while SQLx and Dovecote persistence use canonical UTC microseconds.
- **Breaking identifier contract:** tenant, relation, subject, actor, and
  fulfillment identifiers are nonempty, have no leading or trailing Unicode
  whitespace, and are at most 191 UTF-8 bytes. Values are byte-preserving and
  case-sensitive; no Unicode normalization is performed. Constructors, Serde,
  and SQL write boundaries enforce the contract.
- Added additive v4 SQL migration tracks; historical v3 artifacts remain
  byte-for-byte unchanged. All backends preflight existing rows with the exact
  Rust contract and add database-level byte/edge checks; MySQL and MariaDB
  additionally require explicit binary `utf8mb4_bin` collation.
- **Breaking durable audit contract:** current Dovecote payloads carry an
  explicit `schema_version = 4`. v3 or missing-version payloads require an
  explicit legacy decoder path, unknown versions are rejected, and historical
  outer IDs remain a typed legacy path.

## 3.0.1 - 2026-09-01

- Canonicalised lifecycle occurrence timestamps to microseconds before both
  SQL persistence and Dovecote event construction, so exact retries remain
  idempotent on every supported backend.
- Canonicalised timed-expiry policies at SQL write and read boundaries so
  nanosecond inputs and previously stored policy JSON remain compatible with
  microsecond database columns on every backend.
- Prevented relation upserts on every backend from mutating an existing row
  when a stable id is reused with a different natural key, returning a typed
  identity conflict instead.
- Enforced tenant ownership on PostgreSQL relation upserts and revalidated
  relation definitions at every SQL write boundary.
- Closed Serde construction paths that could create empty relation keys or
  invalid fulfillment policies without running their domain validators.
- Removed sibling-worktree paths from Dovecote dependencies while retaining
  compatibility with the published Dovecote 0.2 series.
- Replaced the bespoke lock-and-map TTL cache with the optional Moka 0.12
  implementation while retaining the existing `cache` feature and public
  configuration API.
- Split the monolithic cross-dialect schema verifier into backend-owned
  `PostgreSQL`, `MySQL`/`MariaDB`, and `SQLite` modules and corrected its stale
  schema-version diagnostic.

## 3.0.0 - 2026-08-28

- **Breaking SQLx API:** Keepsake SQLx moved to the tenant-scoped 3.0
  repository contract. PostgreSQL, MySQL, and SQLite clean installs use the
  tenant-aware v3 baseline; the v2 upgrade requires an explicit operator
  mapping, with no inferred tenant.
- **Breaking core API:** added mandatory, validated domain-owned `TenantId`
  values to Keepsake identities, commands, audit events, and provider query
  boundaries. In-memory provider keys and lookups now remain tenant-scoped;
  the matching SQLx/Dovecote migration and adapter track is required before
  deploying tenant-aware persistence.

## 2.1.0 - 2026-08-28

- Fixed MySQL relation upserts with an enabled relation cache so natural-key
  and id lookups return the freshly persisted enabled and expiry state.
- Added a backend-independent, envelope-validating Dovecote decoder for typed
  Keepsake audit history. Migrated v1 identities are reported as an explicit
  legacy outcome and are not reinterpreted as current events.
- Made both PostgreSQL examples install and check the Dovecote schema before
  their first audited write, and raised the Dovecote dependency floor to 0.1.1.

## 2.0.0 - 2026-08-27

- Moved SQL audit persistence and delivery to Dovecote. Keepsake lifecycle
  mutations enqueue one validated event in the same backend transaction.
- Added `AuditEventId`, generated before persistence and reusable across
  retries, and preserved authoritative occurrence timestamps in the event and
  CloudEvents `time` field.
- Removed the maintained Keepsake SQL audit tables, normalized context rows,
  audit history cursors, outbox paging, and claim/ack/release APIs. Typed audit
  history is read by decoding Dovecote live or snapshot pages.
- Added explicit clean-install and historical-upgrade migration tracks. The
  upgrade track retains old audit tables for reconciliation and rollback and
  never drops them automatically.
- Constructors now require an application-owned stable absolute audit source.

## 1.2.0 - 2026-08-27

- Added backend-specific, opt-in Dovecote migration bridge features.
- Added forward-only bridge bookkeeping migrations while preserving every
  historical migration artifact unchanged.
- Added atomic dual-write commands, persisted source/event identity and exact
  payload ledgers, and bounded high-water history import/reconciliation.
- Added bridge publisher identity and acknowledgement APIs with database-time
  claim fencing, exact replay idempotency, and typed ownership/conflict errors.

## 1.1.0 - 2026-07-12

- Added `evaluate_active`, a non-breaking evaluator entry point that accepts a
  validated `ActiveRelation`.
- Fixed SQLite and MySQL builds when `fulfillment-counters` is disabled.
- Changed bounded SQLite and MySQL active-relation reads to apply id and key
  filters in SQL instead of loading every active relation for the subject.
- Added SQLx feature-matrix checks and automated Postgres/MySQL integration
  gates to CI, including concurrent MySQL apply coverage.
- Documented SQLite/MySQL installation, feature selection, and contention
  behavior.

## 1.0.1 - 2026-07-09

- Gate `filter_active_relations_by_ids` and `filter_active_relations_by_keys`
  behind the `mysql` and `sqlite` features so default Postgres builds are
  warning-free.

## 1.0.0 - 2026-07-09

First stable release. Semver applies to the public Rust API and to schema
expectations in `keepsake-sqlx` from this version onward.

- Added a SQLx audit outbox for Postgres, SQLite, and MySQL. Every SQL audit
  write now creates an outbox row in the same transaction, including apply,
  duplicate apply, revoke, expiry helpers, and explicit `append_audit_event`.
- Added `AuditOutboxRecord`, `AuditOutboxCursor`, and repository helpers for
  cursor export, claim/lease, acknowledgement, and release.
- Moved human documentation from the Astro docs site into [`docs/`](docs/README.md).
- CI runs on pull requests via GitHub Actions.

## 0.6.0 - 2026-06-23

- Added `audit_events_for_keepsake` and `audit_events_for_relation` read helpers
  to the SQLx adapter (Postgres, MySQL, SQLite) with keyset pagination via
  `AuditCursor`, returning `AuditEventRecord`s with hydrated context attributes.
- Added `AuditEventType::from_storage_label` as the inverse of `as_str`.
- Batched audit context attribute writes into a single statement per event
  instead of one statement per attribute.
- Indexed the fulfillment expiry sweep: partial indexes on Postgres and SQLite,
  and a stored generated column plus index on MySQL.
- Added `RevokeBySubject` and `revoke_by_subject`, revoking the active keepsake
  for a `(subject, relation)` pair and returning the revoked keepsake id.
- Added `increment_counter_projection`, an atomic database-side counter
  increment that returns the new value, avoiding the read-modify-write race in
  `upsert_counter_projection`.
- Persisted checklist fulfillment state via a new `keepsake_fulfillment_checklist`
  table and `upsert_checklist_projection`, so `when_fulfilled` policies with a
  `checklist_complete` rule are evaluated by the expiry sweep instead of only in
  application code.

## 0.5.1 - 2026-06-20

- Fixed MySQL lifecycle check constraints so the SQLx migration applies on
  MySQL 8.4.
- Added Docker-backed MySQL integration coverage to `mise run test-db`.

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
- Refreshed README, docs, examples, and crate versions for the 0.2.0 release
  surface.

## 0.1.0 - 2026-06-18

- Initial workspace scaffold with core lifecycle model, SQLx/Postgres adapter,
  Docker-backed database test wiring, examples, and documentation.
