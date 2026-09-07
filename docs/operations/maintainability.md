# Maintainability contract

Keepsake's public API, persistence formats and verification commands have separate
owners. Rust API changes follow package semver; published migration artifacts and
immutable audit occurrences retain their original bytes. The current source major
is 6, following published 5. This alignment does not introduce another major.

| Criterion | Enforced or reviewed boundary |
| --- | --- |
| Setup and commands | Pinned mise tools, familiar just recipes, one gate script shared with CI |
| Effective quality coverage | Inherited strict lints, feature matrix, structural rules, API docs, TOML and spelling |
| Exceptions | Two existing workspace lint allowances; test/production distinctions explained below; no local suppression attributes |
| Cohesion and names | Backend catalogs, predicate checks, audit construction and in-memory provider owners are separate; public domain names remain stable |
| Ecosystem reuse | SQLx, Serde, time, UUID and Dovecote own commodity infrastructure; catalog comparison has a narrow compatibility purpose |
| Models and errors | Validated identities, typed state/observations, explicit fallible APIs and documented rollback obligations |
| Async and unsafe | No synchronous mutex guard across I/O; caller-owned transaction effects are explicit; unsafe code forbidden |
| Evolution | Pending major 6, unchanged published SQL and domain schema 4, historical upgrade and immutable-history regression tests |

## Commands and enforcement

Install pinned tools with `mise install`. `mise run check` delegates to `just check`
and one canonical script; CI calls the same entry point. `mise run fmt` deliberately
formats Rust and TOML. Acceptance only checks formatting. A focused test uses
`mise exec -- just test <filter> -- --nocapture`; live PostgreSQL/MySQL contracts use
`mise run test-db`, against disposable databases.

The workspace and every example inherit the same lint policy. All, pedantic,
nursery and cargo groups are denied, together with the selected panic, unfinished
code, indexing, string slicing, arithmetic, conversion, absolute-path and attribute
hygiene restrictions. Public error documentation is checked throughout; unreachable public items are
checked in production. The feature matrix checks independent PostgreSQL, SQLite and MySQL builds
with and without cache/migrations, and the no-feature build remains supported.
Excessive nesting is checked at the configured threshold of four structural blocks;
unsafe code is forbidden across the workspace.

Three distinctions are deliberate:

- Existing tests may assert and index expected fixture results. Indexing is checked
  in production, while `allow-indexing-slicing-in-tests` preserves the pre-alignment
  test behavior. Existing direct panic, unwrap and expect bans remain in force;
  no new permission for those macros was added.
- `panic_in_result_fn` is enforced in a separate production library/binary pass.
  Rust tests use `Result` to propagate setup failures and assertions for behavioral
  failures. Applying this restriction to those tests would reject their ordinary
harness contract. The normal all-target pass still checks them under the previous
panic restrictions.
- `unreachable_pub` is enforced in the production pass. In private integration-test
  fixture modules it conflicts with nursery's `redundant_pub_crate`: a plain public
  helper is unreachable outside the private test module, while narrowing it to the
  crate produces the opposite lint. The test namespaces retain their existing
  private ownership instead of being exposed to satisfy a lint.

`module_name_repetitions` remains allowed for established public names such as
`RelationDefinition` and `KeepsakeRepository`; module moves must not force consumers
to rename domain types. `multiple_crate_versions` remains allowed because the
supported SQLx/Dovecote backend dependency graph contains compatible parallel
versions. Dependency advisories, licenses, sources, bans and unused direct
packages are checked separately by cargo-deny and cargo-machete. These are explicit
existing ecosystem/API choices, not permission to add local suppression attributes.

Strict rustdoc, TOML formatting/validation and spelling are part of the canonical
gate. Required missing tools fail with an installation instruction. Published
package resolution and real database tests remain separate evidence from a local
workspace build.

## Ownership and models

The pure core owns lifecycle validation and effective-state evaluation. SQLx owns
database transactions, migrations and backend access. Keepsake owns scoped relation
observations and lifecycle writes; Dovecote owns the only maintained audit and
delivery store. Applications own clock authority, authorization, business records,
identity mapping and notification content.

In-memory provider implementations are test capabilities, split by active-relation
seeding/reads, lifecycle storage and fulfillment snapshots. Their public re-exports
remain stable. Shared mutexes have concrete collection owners and poisoned-lock
errors; synchronous guards are not held across asynchronous I/O.

Audit decoding and canonical lifecycle occurrence construction have distinct
internal modules. Scoped evidence exposes its limitations: retaining a snapshot
alone does not preserve freshness, and cached reads cannot authorize a protected
write. Effective time failures are typed rather than converted to permission or
indefinite restriction. Public fallible operations document validation, storage,
replay and rollback outcomes at their own boundary.

Backend errors retain their typed SQLx sources for application recovery. Callers
must choose what to expose in external diagnostics: driver errors and intentional
`Debug` formatting of application records can contain private input. The executable
examples emit a fixed failure diagnostic instead of printing connection errors.

## Persistence and commodity infrastructure

SQLx migrations execute published artifacts and retain real migration receipts.
Keepsake's catalog verifier checks additional domain constraints, indexes,
identifier rules and migration-track identity. These checks cannot be replaced by
SQLx's migration ledger alone: the ledger does not prove the current catalog still
matches an applied migration. Catalog declarations, constraints/index checks,
identifier validation and upgrade preflight are separate responsibilities.

The small SQL-expression normalization boundary compares the published schema's
known predicates across backend catalog renderings. It is not an SQL execution
engine or general SQL parser. Replacing it with a general parser would still need
backend-specific canonical rendering rules, while potentially changing accepted
historical spellings. Its parsing failures must reject schema acceptance; migration
bytes remain unchanged. Backend schema corruption and historical upgrade tests
are the regression evidence for this boundary.

SQLite extra-trigger checks use the catalog's exact owning table, rather than
inferring ownership from SQL text. This catches triggers attached to protected
tables with quoted names while leaving unrelated application tables alone.

Serde owns JSON encoding, time owns timestamp representation, UUID owns event and
assignment identifiers, and Dovecote owns outbox concurrency. No private serializer,
broker, workflow engine or second audit repository is introduced. Lifecycle table
locking and full-history observation comparisons are intentional costs: their
contention and O(history) memory behavior are documented in the transaction guide.
A generic repository framework would hide these distinct backend guarantees.

SQLite counter increments explicitly reject arithmetic promotion out of the signed
64-bit integer range. SQLite otherwise converts overflowing integer addition into
REAL storage. The guarded update returns `RepositoryError::CounterOverflow` and
preserves both value and observation timestamp. This is a write-contract fix using
the existing schema, not a rewrite of historical migration SQL.

## Verification boundaries

Run the canonical gate after any alignment change and run the live database lane
when catalog or query ownership changes. An independent review must inspect the
actual final source and consumer composition, including rollback, replay,
re-application, deadline expiry and delivery history. A passing source dependency
build does not establish registry availability; publication has a separate gate.
See [relation lifecycle verification](relation-lifecycle-verification.md) for the
executed backend, upgrade, example and package checks.
