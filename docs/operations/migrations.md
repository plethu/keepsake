# Migrations

## Tenant-aware Keepsake 3 migration

The core tenant contract is a breaking API boundary and is not satisfied by
adding a `tenant_id` field only in application code. The coordinated Keepsake
3 SQLx and tenant-capable Dovecote release must install non-null tenant
columns, tenant-prefixed indexes, tenant-aware uniqueness, and matching
foreign-key invariants. Every read, mutation, expiry sweep, audit event, and
delivery operation must bind the tenant explicitly.

Published 1.x and 2.x migrations remain immutable. A v2-to-v3 upgrade must
first stop or fence writers, inventory all rows, and supply an explicit
operator-reviewed mapping from each existing row to a tenant. The migration
must refuse incomplete mappings and must never invent a default tenant. Keep
the mapping and reconciliation evidence with the deployment record.

Relation definitions are tenant-owned in the new contract, so their natural
key uniqueness is scoped by tenant. Clean installs use the backend-specific
baselines under `migrations/v3/{postgres,mysql,sqlite}/`.

For an existing Keepsake 2 database on any supported backend, call
`prepare_tenant_upgrade`, apply an operator-reviewed mapping to every nullable
`tenant_id` column, and then call `activate_tenant_upgrade`. The SQL artifacts
are under `migrations/upgrade/v2_to_v3/{postgres,mysql,sqlite}/`. Activation
refuses incomplete mappings and changes the domain keys and active indexes to
tenant-leading composite forms; it never invents a tenant. MySQL separate
databases and SQLite file-per-tenant deployments provide the clearest physical
boundaries for regulated workloads; use shared schemas only with equivalent
operational controls and verified tenant isolation tests.

Do not enable tenant-aware writers until the Keepsake and Dovecote schema
checks pass, cross-tenant isolation tests pass on the selected backend, and
rollback/backup procedures have been rehearsed. The current 2.x clean baseline
and historical upgrade paths below describe the pre-tenant schema and remain
valid for existing installations.

Keepsake 2.0 has an explicit clean-install track and an explicit historical
upgrade track. The tracks share the domain schema but are not interchangeable.
The clean track contains no Keepsake audit or outbox tables: Dovecote owns that
schema and must be installed separately with the selected SQLx adapter.

## New installation

1. Install the Keepsake 3.0 clean baseline with `repo.migrate()`.
2. Install the matching Dovecote schema with its backend adapter.
3. Construct the repository with an application-owned absolute source URI.
4. Call `repo.check_schema()` before accepting writes.

```rust
let repo = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
repo.migrate().await?;
// Dovecote's migration is installed by dovecote-sqlx-postgres (or the
// matching SQLite/MySQL adapter).
repo.check_schema().await?;
let tenant = keepsake::TenantId::new("account-group-a")?;
let account = repo.for_tenant(tenant);
```

`migrate()` refuses a database marked as the legacy track. It does not drop
unknown tables, create inert legacy audit tables, or silently guess which
schema the operator intended.

Dovecote's MySQL/MariaDB schema creates validation triggers. The migration
account needs trigger DDL authority; with MySQL binary logging enabled, an
administrator may also need to enable `log_bin_trust_function_creators` for
schema installation. Ordinary Keepsake operations do not require that server
setting after the schema is installed.

## Existing 1.x installation

Use this path only when the database already contains the published Keepsake
1.x migrations:

1. Stop writers and legacy publishers during a maintenance window, or deploy
   the separately documented 1.x bridge before beginning a rolling migration.
2. Install and check Dovecote.
3. Call `repo.upgrade_migrate()` explicitly. This runs the historical Keepsake
   migrations without editing their files or dropping their tables.
4. Import complete audit history into Dovecote, including delivered events,
   using the Dovecote migration importer.
5. Reconcile counts, event identities, exact payload bytes where they existed,
   and delivery state before deploying 2.0 writers.
6. Call `repo.activate_upgrade()` only after reconciliation succeeds. This
   explicit activation marks the legacy domain schema as 2.0; a partially
   imported database cannot pass the normal 2.0 schema check.

`activate_upgrade()` requires an importer-owned row in
`keepsake_upgrade_evidence` with `evidence_id = 1`,
`evidence_schema_version = 1`, `provenance = 'keepsake-dovecote-importer'`,
`source_schema = 'keepsake-sqlx-1.1'`, the configured source, stream
`keepsake-audit`, non-negative complete audit and outbox high-water marks, a non-empty
versioned codec identifier, zero missing/extra/state/digest deltas, and zero
active claims. The final 1.x bridge importer is the sole evidence producer:
call its `finalize_upgrade_reconciliation()` only after the independent
audit-only and outbox scans have completed. It rereads both persisted
high-waters, checks the current source maxima and Dovecote rows, and inserts
the singleton evidence row atomically. The 2.0 adapter has no public evidence
writer; it only validates and consumes that row. Activation validates the
complete row again before writing the 2.0 marker. These fields are contract
checks, not an unforgeable proof of which database principal wrote the row.
The machine gate is the complete scan plus zero reconciliation deltas. It also
checks the installed Dovecote schema.

Zero-history activation is valid: an empty legacy audit source records
`audit_high_water = 0`, `outbox_high_water = 0`, and zero counts after the
importer proves that the complete scan found no rows. A fresh empty legacy
database still cannot be activated by
merely creating a marker.

The importer evidence schema contains these required fields in addition
to the reconciliation counts: `evidence_schema_version` (currently `1`),
`provenance` (currently `keepsake-dovecote-importer`), and `source_schema`
(currently `keepsake-sqlx-1.1`). `codec_version` names the versioned project
codec used for reconstructed historical payloads. The activation reader
selects the single row with `evidence_id = 1` and rejects missing or changed
contract fields before writing the 2.0 activation marker. `upgrade_migrate()`
installs this small evidence table in the additive bridge upgrade-track migration;
the clean baseline does not create it and the published historical migration
files remain byte-identical. The upgrade track embeds the exact bridge
bookkeeping migration from the final 1.x line.
That preserves SQLx migration history when a bridge-enabled database is
upgraded; those bridge tables are inert in 2.0 and remain only as
rollback/reconciliation material.

The old Keepsake audit and outbox tables remain read-only migration material
through the rollback window. Keepsake 2.0 has no active SQL accessors for them;
use Dovecote live or snapshot paging for audit reads and Dovecote delivery
operations for publication. Drop the old tables only through a later,
operator-controlled cleanup after rollback and reconciliation are no longer
needed.

## Rolling bridge

The bridge is an additive 1.x feature and is not part of this clean 2.0
constructor. Its writer transaction records the legacy row and equivalent
pending Dovecote event together. Legacy publication remains the sole publisher
until cutover. A bounded high-water importer catches rows produced by old
writers, and active legacy claims must be completed, expired, or explicitly
fenced before their state is imported.

The recommended rolling sequence is:

1. Install Dovecote and run `check_schema`.
2. Deploy the opt-in bridge while legacy publication remains enabled.
3. Run repeated bounded complete-history reconciliation.
4. Prove every writer is bridge-aware or 2.0-capable, then fence new
   legacy-only rows.
5. Finish or fence active claims and run the final high-water pass.
6. Require zero reconciliation delta and matching state/count/digest evidence.
7. Stop the legacy publisher, switch ownership to Dovecote, and deploy 2.0.
8. Keep legacy tables read-only through the rollback window.

At-least-once cutover can produce one transport duplicate: a legacy publisher
may publish before acknowledgement and Dovecote may publish after cutover.
Both carry the same tenant-scoped Dovecote `(tenant_id, source, event_id)`, so
consumers must preserve tenant routing and deduplicate that identity.
Deployments without consumer deduplication must use the maintenance-window
path.

## Historical migration rules

Published migrations are immutable. Verify their bytes before and after an
upgrade. Never edit a historical migration to remove the old audit tables.
Prefer legacy outbox payload bytes as the source of truth; reconstruct older
rows only through the project's declared versioned codec, and record that the
payload was reconstructed rather than byte-preserved. Legacy source rows stay
available for rollback and reconciliation.

The backend catalog corruption test is intentionally ignored because it alters
the configured database while it proves that `check_schema()` rejects a
weakened column, index, or invariant. Run it alone against a disposable
database, with its backend URL explicitly set:

```text
DATABASE_URL=postgres://... cargo test -p keepsake-sqlx --test postgres --features postgres-tests catalog_check_rejects_changed_column_index_and_constraint -- --ignored --test-threads=1
MYSQL_DATABASE_URL=mysql://... cargo test -p keepsake-sqlx --test mysql --features mysql-tests catalog_check_rejects_changed_column_index_and_constraint -- --ignored --test-threads=1
```

The PostgreSQL ignored tests acquire a database-scoped advisory lock on their
first schema reset and hold that session until the test process exits. This
means overlapping PostgreSQL `cargo test` or `just test-db-postgres` commands
against the same URL wait for one another instead of resetting each other's
fixtures. Keep `--test-threads=1` so the destructive reset and migration-track
selection remain ordered within each process; a crashed process releases the
PostgreSQL session lock automatically.

The same ignored backend targets include `upgrade_track_activates_after_importer_evidence`;
run that test separately against PostgreSQL 17.11 and MySQL 8.4/Innovation
26.7. It exercises the historical upgrade migrations, additive Dovecote
installation, importer evidence, activation, and the final catalog check.

MariaDB 11.8 supports the clean v3 baseline and the tenant-aware runtime
schema. The published 1.x MySQL migration bytes are immutable and contain a
conditional generated column over `CHAR(36)` identifiers, which MariaDB cannot
replay. Existing MariaDB installations on that historical track must use an
operator-owned export/rebuild into the forward v3 baseline; do not edit or
re-run the published migration files on MariaDB.
