# Transactional relation lifecycle

Keepsake owns persisted assignments, per-assignment expiry and lifecycle history.
Gatekeep consumes scoped effective facts; Dovecote owns immutable audits and delivery.
Applications own identity mapping, policy, business records and notification wording.

The initial gap was transaction-owning apply/revoke operations, no stale-observation
precondition, and persisted `applied` reads used as presence. The implemented
composition path uses `apply_in_transaction`, `revoke_in_transaction`,
`revoke_by_subject_in_transaction`, reconciliation and fulfillment methods, plus
`observe_in_transaction` / `revalidate_in_transaction`. Convenience methods own
their transactions and delegate to those same implementations.

Caller transactions own commit and rollback. Any error or cancellation requires
rolling back the entire transaction; callers must never commit partial work.
Authenticate and revalidate current authority before invoking replay or exposing
its receipt. Mandatory lifecycle audits stay in the existing Dovecote stream.

Observation evidence retains the relation definition and complete assignment
history (including terminal identities). This intentionally trades memory and
query cost for exact ABA detection without a second authoritative lifecycle
store. It cannot be forged through public fields or deserialization. An empty
history is distinct from a history with a terminal assignment. A new assignment
must use a fresh id. Current means the unique applied row, never an arbitrary
historical row. Revalidation holds database locks through the protected business
write and outer commit. Cached reads provide no such guarantee.

PostgreSQL requires READ COMMITTED and takes relation-definition and assignment
table locks in that order. This coarse lock fences absent insertions and all
existing mutation paths, including reconciliation; it limits concurrency across
tenants. SQLite takes a write reservation before reading; BEGIN IMMEDIATE is
recommended, and a failed deferred transaction upgrade requires whole-operation
retry. MySQL uses the relation definition row followed by assignment rows with
locking current reads, requiring REPEATABLE READ for Dovecote composition. For multiple
relations acquire observations in sorted relation-id order before any lifecycle
or business write; retry deadlocks/serialization failures as whole transactions.
Relation definitions must exist before observing absence. Business operations
must use observations for every policy-relevant scope; application policy still
owns directional pairs and already-admitted session behavior.

No migration bytes need change: per-assignment expiry and terminal history already
exist. The new public command fields require the next major package version.

## Receipt and upgrade boundaries

Keepsake 6 changes public command and receipt shapes; version 5 was published.
Its domain schema remains version 4. All published migration bytes are retained;
there is no DDL migration and no history is copied or re-emitted. Run the existing
full `check_schema` before serving, including constraint/index and Dovecote checks.
Transactional observations, fulfillment reads and expiry batches additionally verify the backend and domain schema
track on their own connection, without acquiring another pooled connection.

New audit occurrences carry the complete typed command in an optional additive
schema-4 field. Historical occurrences without this field remain readable and
retain their original immutable bytes and delivery state; they cannot supply a
strict command replay receipt. Such replay returns `ReceiptEvidenceUnavailable` instead of
inventing missing identity evidence. New conflicting command content, including
metadata, deadline, requested assignment id or actor, returns `CommandConflict`.

`AppliedKeepsake.replayed` and `RevokedKeepsake.replayed` distinguish exact
committed receipt recovery from new mutations. Replaying a successful revoke
never revokes a later re-application. A transactional revoke with no active assignment and no
committed command returns `MissingActiveAssignment`; it commits no receipt and
does not reserve the command identity. Authenticate and revalidate current
application authority before exposing any receipt. Conditional exact replay
returns its committed result even if the original lifecycle observation is now
stale; only a new mutation must match that observation.

Do not delete or rewrite terminal assignment history or immutable audit history
while observations or receipts may be reused. Full-history evidence has query
and memory cost proportional to assignment count; applications needing archival
must establish a separate versioned invalidation contract before pruning. SQL
occurrence and copied expiry timestamps retain the existing microsecond
canonicalization contract. Applications should provide deadlines at that precision.

Convenience revokes preserve their existing false/None no-op result by rolling
back that empty attempt. These no-op results are not committed command receipts.


## Fulfillment and reconciliation composition

`fulfillment_snapshot_in_transaction` returns `FulfillmentEvidence` bound to the
tenant and assignment identity. Gatekeep rejects evidence for another assignment.
It reads projection evidence on the caller
connection and fences updates until commit. Observe relation scopes first;
PostgreSQL then locks counter and checklist projection tables in that order,
SQLite reserves the writer, and MySQL locks the parent assignment followed by
projection rows. Missing projection entries remain missing evidence, not zero or
completed values. The application must supply evidence freshness explicitly.

`expire_due_timed_in_transaction` and `expire_due_fulfilled_in_transaction`
return the assignment IDs actually transitioned and audited in that transaction.
Use those IDs to enqueue required application notification intents before commit.
Convenience workers retain their count return. A zero-effect retry creates no
new event. Batch reconciliation is separate from effective expiry at the deadline.

PostgreSQL checks the actual transaction's READ COMMITTED mode. MySQL checks
configured session REPEATABLE READ, matching Dovecote; do not override isolation
for an individual transaction. SQL's session variable cannot prove a per-transaction
override without additional server privileges/instrumentation. Relation locking
reads still fence lifecycle and fulfillment changes under READ COMMITTED, but
that mode is not advertised for the full Dovecote composition.


A command's retry identity is `(tenant, configured event source, audit_id)`.
Keep the application-owned event source stable across retries and upgrades;
changing it creates a different event namespace and requires an explicit data
migration strategy. `CommandContext.idempotency_key` is audited application
context, not the deduplication key. Assignment IDs identify lifecycle generations;
they must be fresh for re-application and are independently protected by the
existing primary key.


Reconciliation batches have no separate command identity. For exact unknown-commit
recovery of a batch's returned IDs, store those IDs in the application's existing
operation receipt alongside notification intents in the same transaction. Retrying
a committed batch itself returns no newly transitioned IDs. Apply and revoke
commands provide their own exact immutable command receipts.
