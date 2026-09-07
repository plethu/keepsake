# Relation lifecycle verification

This records local implementation evidence for the unpublished Keepsake 6.0.0
source. Keepsake 5.0.0 is already published. No release or publication was
performed. The [transaction contract](../reference/transactional-lifecycle.md)
defines the caller's obligations and backend limits.

## Performed checks

- `scripts/check-project-gates.sh`: formatting, structural rules, workspace
  strict Clippy, all seven SQL feature combinations, no-default-feature tests,
  SQLite integration tests, advisory/license/ban/source checks, workspace tests,
  documentation tests and unused dependencies passed. The final alignment run
  also passed the selected arithmetic/bounds restrictions, production panic and
  visibility checks, strict rustdoc, TOML validation/formatting and spelling.
- PostgreSQL 17.11: the complete ignored PostgreSQL target passed 48 tests.
  The final transaction target passed two tests, including absence contention,
  stale observations, exact receipts, rollback, reapplication and effective expiry.
- MySQL 8.4.11: the complete ignored MySQL target passed 33 tests. The final
  transaction target passed two tests, including real lock contention and scoped
  fulfillment evidence. Its fixture resets make this target materially slower
  than PostgreSQL or SQLite.
- MySQL Innovation 26.7.0 and MariaDB 11.8.6: each passed both new lifecycle
  transaction tests and the published-v3 identifier upgrade fixture on separate
  disposable databases. These are focused new-contract results, not reruns of
  every historical backend test.
- SQLite: the canonical suite passed 36 tests, including published-v3 identifier
  upgrade and historical schema-4 occurrence recovery refusal. The
  transaction target passed three tests, including wrong-schema rejection for
  missing-id revocation, fulfillment reads and both expiry batches. Tests use the
  SQLx bundled SQLite 3.46.0, including a file-backed WAL contention fixture.
- `cargo package -p keepsake -p keepsake-sqlx --allow-dirty` assembled and built
  both version-6 archives. Cargo staged Keepsake 6.0.0 in its temporary local
  registry to verify the dependent SQL package. This proves those local archives
  compose; it does not prove resolution against a published Keepsake 6.0.0.

The core suite has 56 tests, the SQL unit suite 28, audit decoding five and the
Dovecote SQLite integration suite 14. Workspace execution deliberately ignores
live backend cases; the explicit database commands above supply that evidence.
The guide has two executable documentation examples and SQL audit decoding has
one. No mocks stand in for the rollback or locking tests.
The alignment added runtime evidence for SQLite counter overflow and underflow
without mutation, exact SQLite trigger ownership, and const/runtime relation-key
whitespace parity across every Unicode scalar.
After the quality-audit setup-order fix, `cargo run -p postgres-tags` and
`cargo run -p postgres-sanctions` each passed against a fresh isolated database.
Their final failure-boundary checks also passed: invalid connection input produced
only the fixed diagnostic and failure exit status, without private input details.

## Durable compatibility

All historical migration artifacts remain byte-for-byte unchanged. Domain and
audit schema versions remain 4. New audit occurrences include optional typed
command evidence; old occurrences remain readable and are never re-emitted merely
because of an upgrade. Strict recovery of an old occurrence without that evidence
returns `ReceiptEvidenceUnavailable` rather than guessing request equivalence.

The supported `upgrade_identifier_contract` method applies the existing published
4000 artifact after explicit tenant activation without replaying or inventing the
3000 clean-baseline receipt. PostgreSQL's representative old-track upgrade test
preserves assignment metadata, counters and checklist evidence. SQLite and MySQL
fixtures additionally preserve revoked history without creating delivery work and
reject a dirty migration receipt. See the [migration procedure](migrations.md)
for writer fencing and MySQL DDL recovery requirements.

## Remaining limits

The full historical PostgreSQL/MySQL targets ran on PostgreSQL 17.11 and
MySQL 8.4.11. Innovation and MariaDB received the focused new-contract coverage
above. The historical MariaDB 1.x migration restriction remains unchanged.

PostgreSQL composition deliberately uses coarse table locks and requires READ
COMMITTED. MySQL composition requires REPEATABLE READ and forbids per-transaction
isolation overrides; its check validates the configured session setting, not a
privileged server inspection of a hidden override. SQLite serializes writers.
Deadlock or lock-timeout recovery restarts the complete outer transaction.
Observation evidence reads retained assignment history, so cost grows with history
and callers must not delete or rewrite it while observations or receipts can be
reused. These tradeoffs are explicit; there is no weaker cached authorization
substitute for the protected transaction path.
