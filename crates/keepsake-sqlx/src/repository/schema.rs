//! Database schema preflight and runtime shape verification.
//!
//! Migration runners need a deliberately small preflight: an empty database is
//! a valid target for the runner. `check_schema`, in contrast, is a runtime
//! gate and verifies the complete domain catalog before it verifies Dovecote.

#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use std::fmt::Display;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use super::{RepositoryError, RepositoryResult};

#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
#[derive(Debug, Clone, Copy)]
pub(super) struct PersistedIdentifier {
    pub(super) table: &'static str,
    pub(super) column: &'static str,
    pub(super) field: &'static str,
}

#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub(super) const PERSISTED_IDENTIFIERS: &[PersistedIdentifier] = &[
    PersistedIdentifier {
        table: "keepsake_relation_definitions",
        column: "tenant_id",
        field: "tenant_id",
    },
    PersistedIdentifier {
        table: "keepsake_relation_definitions",
        column: "kind",
        field: "relation.kind",
    },
    PersistedIdentifier {
        table: "keepsake_relation_definitions",
        column: "key",
        field: "relation.name",
    },
    PersistedIdentifier {
        table: "keepsakes",
        column: "tenant_id",
        field: "tenant_id",
    },
    PersistedIdentifier {
        table: "keepsakes",
        column: "subject_kind",
        field: "subject.kind",
    },
    PersistedIdentifier {
        table: "keepsakes",
        column: "subject_id",
        field: "subject.id",
    },
    PersistedIdentifier {
        table: "keepsake_fulfillment_counters",
        column: "tenant_id",
        field: "tenant_id",
    },
    PersistedIdentifier {
        table: "keepsake_fulfillment_counters",
        column: "key",
        field: "fulfillment.key",
    },
    PersistedIdentifier {
        table: "keepsake_fulfillment_checklist",
        column: "tenant_id",
        field: "tenant_id",
    },
    PersistedIdentifier {
        table: "keepsake_fulfillment_checklist",
        column: "item",
        field: "fulfillment.list_key",
    },
];

/// Keep migration preflight memory bounded even when an installation contains
/// a large historical relation catalogue.
#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub(super) const IDENTIFIER_SCAN_BATCH_SIZE: i64 = 256;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
fn mismatch(detail: impl Into<String>) -> RepositoryError {
    RepositoryError::BackendMismatch {
        expected: "complete Keepsake domain schema for the selected track",
        actual: detail.into(),
    }
}

#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub(super) fn validate_persisted_identifier_bytes(
    identifier: PersistedIdentifier,
    row: impl Display,
    bytes: &[u8],
) -> RepositoryResult<()> {
    let value = str::from_utf8(bytes).map_err(|error| {
        mismatch(format!(
            "v4 identifier migration preflight rejected {}.{} row {}: invalid UTF-8 ({error})",
            identifier.table, identifier.column, row
        ))
    })?;
    keepsake::validate_persisted_identifier(identifier.field, value).map_err(|error| {
        mismatch(format!(
            "v4 identifier migration preflight rejected {}.{} row {}: {error}",
            identifier.table, identifier.column, row
        ))
    })
}

#[cfg(all(
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
pub(super) fn persisted_identifier_type_mismatch(
    identifier: PersistedIdentifier,
    row: impl Display,
    actual_type: &str,
) -> RepositoryError {
    mismatch(format!(
        "v4 identifier migration preflight rejected {}.{} row {}: expected text, found {actual_type}",
        identifier.table, identifier.column, row
    ))
}

#[cfg(all(test, feature = "mysql"))]
mod mysql_normalization_tests;

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
mod postgres_artifact_tests;

#[cfg(all(test, feature = "sqlite", feature = "migrations"))]
mod sqlite_artifact_tests;

#[cfg(all(
    test,
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
mod persisted_identifier_tests;

#[cfg(all(feature = "postgres", feature = "migrations"))]
const PG_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/postgres/2000_clean_baseline.sql");

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
const PG_V3_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v3/postgres/3000_clean_baseline.sql");

#[cfg(feature = "postgres")]
const PG_V4_IDENTIFIER_ARTIFACT: &str =
    include_str!("../../migrations/v4/postgres/4000_identifier_contract.sql");

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
const PG_V3_UPGRADE_ACTIVATE_ARTIFACT: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/postgres/activate.sql");

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
const PG_V3_UPGRADE_PREPARE_ARTIFACT: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/postgres/prepare.sql");

#[cfg(all(feature = "postgres", feature = "migrations"))]
const PG_UPGRADE_ARTIFACT: &str = concat!(
    include_str!("../../migrations/postgres/0001_init.sql"),
    include_str!("../../migrations/postgres/0002_lifecycle_invariants.sql"),
    include_str!("../../migrations/postgres/0003_schema_metadata.sql"),
    include_str!("../../migrations/postgres/0004_fulfillment_expiry_index.sql"),
    include_str!("../../migrations/postgres/0005_fulfillment_checklist.sql"),
    include_str!("../../migrations/postgres/0006_audit_outbox.sql"),
    include_str!("../../migrations/postgres/0007_dovecote_bridge.sql"),
);

#[cfg(all(feature = "mysql", feature = "migrations"))]
const MYSQL_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/mysql/2000_clean_baseline.sql");

#[cfg(feature = "mysql")]
const MYSQL_V3_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v3/mysql/3000_clean_baseline.sql");

#[cfg(feature = "mysql")]
const MYSQL_V4_IDENTIFIER_ARTIFACT: &str =
    include_str!("../../migrations/v4/mysql/4000_identifier_contract.sql");

#[cfg(feature = "sqlite")]
const SQLITE_V4_IDENTIFIER_ARTIFACT: &str =
    include_str!("../../migrations/v4/sqlite/4000_identifier_contract.sql");

#[cfg(all(test, feature = "mysql"))]
const MYSQL_V3_UPGRADE_ACTIVATE_ARTIFACT: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/mysql/activate.sql");

#[cfg(all(feature = "mysql", feature = "migrations"))]
const MYSQL_UPGRADE_ARTIFACT: &str = concat!(
    include_str!("../../migrations/mysql/0001_init.sql"),
    include_str!("../../migrations/mysql/0002_lifecycle_invariants.sql"),
    include_str!("../../migrations/mysql/0003_fulfillment_expiry_index.sql"),
    include_str!("../../migrations/mysql/0004_fulfillment_checklist.sql"),
    include_str!("../../migrations/mysql/0005_audit_outbox.sql"),
    include_str!("../../migrations/mysql/0006_dovecote_bridge.sql"),
);

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const SQLITE_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/sqlite/2000_clean_baseline.sql");

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const SQLITE_UPGRADE_ARTIFACT: &str = concat!(
    include_str!("../../migrations/sqlite/0001_init.sql"),
    include_str!("../../migrations/sqlite/0002_lifecycle_invariants.sql"),
    include_str!("../../migrations/sqlite/0003_fulfillment_expiry_index.sql"),
    include_str!("../../migrations/sqlite/0004_fulfillment_checklist.sql"),
    include_str!("../../migrations/sqlite/0005_audit_outbox.sql"),
    include_str!("../../migrations/sqlite/0006_dovecote_bridge.sql"),
);

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const CLEAN_TABLES: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const UPGRADE_TABLES: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
    "keepsake_audit_events",
    "keepsake_audit_context_attributes",
    "keepsake_audit_outbox",
];

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const CLEAN_INDEXES: &[(&str, &str)] = &[
    ("index", "keepsakes_active_subject_lookup"),
    ("index", "keepsakes_active_relation_membership"),
    ("index", "keepsakes_due_timed_expiry"),
    ("index", "keepsake_fulfillment_counter_scan"),
    ("index", "keepsakes_due_fulfilled_expiry"),
    ("index", "keepsake_fulfillment_checklist_scan"),
    ("unique index", "keepsakes_one_active_relation_per_subject"),
];

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const UPGRADE_INDEXES: &[(&str, &str)] = &[
    ("index", "keepsakes_active_subject_lookup"),
    ("index", "keepsakes_active_relation_membership"),
    ("index", "keepsakes_due_timed_expiry"),
    ("index", "keepsake_fulfillment_counter_scan"),
    ("index", "keepsakes_due_fulfilled_expiry"),
    ("index", "keepsake_fulfillment_checklist_scan"),
    ("unique index", "keepsakes_one_active_relation_per_subject"),
    ("index", "keepsake_audit_by_keepsake"),
    ("index", "keepsake_audit_by_relation"),
    ("index", "keepsake_audit_context_attribute_lookup"),
    ("index", "keepsake_audit_outbox_export"),
    ("index", "keepsake_audit_outbox_claim"),
];

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const CLEAN_TRIGGERS: &[(&str, &str)] = &[
    ("trigger", "keepsakes_clean_invariants_insert"),
    ("trigger", "keepsakes_clean_invariants_update"),
];

#[cfg(all(feature = "sqlite", feature = "migrations"))]
const UPGRADE_TRIGGERS: &[(&str, &str)] = &[
    ("trigger", "keepsakes_expiry_policy_projection_insert"),
    ("trigger", "keepsakes_expiry_policy_projection_update"),
    ("trigger", "keepsakes_lifecycle_timestamps_insert"),
    ("trigger", "keepsakes_lifecycle_timestamps_update"),
];

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

// PostgreSQL and MySQL use information_schema/pg_catalog rather than relying
// on object counts. Each backend module keeps its catalog comparisons close to
// the schema dialect it verifies.

#[cfg(feature = "mysql")]
pub(super) use mysql::mysql_runtime_schema_check;
#[cfg(all(feature = "mysql", feature = "migrations"))]
pub(super) use mysql::{
    mysql_clean_schema_preflight, mysql_upgrade_schema_check, mysql_upgrade_schema_preflight,
};
#[cfg(all(test, feature = "mysql"))]
use mysql::{mysql_default_matches, mysql_is_generated_extra, mysql_v3_referential_action_matches};
#[cfg(feature = "mysql")]
use postgres::mysql_catalog_check_matches;
#[cfg(feature = "postgres")]
pub(super) use postgres::postgres_runtime_schema_check;
#[cfg(all(feature = "postgres", feature = "migrations"))]
pub(super) use postgres::{
    postgres_clean_schema_preflight, postgres_upgrade_schema_check,
    postgres_upgrade_schema_preflight,
};
#[cfg(feature = "sqlite")]
pub(super) use sqlite::sqlite_runtime_schema_check;
#[cfg(all(feature = "sqlite", feature = "migrations"))]
pub(super) use sqlite::{
    sqlite_clean_schema_preflight, sqlite_upgrade_schema_check, sqlite_upgrade_schema_preflight,
};

#[cfg(all(test, any(feature = "postgres", feature = "mysql")))]
mod predicate_parsing_tests;

mod sql;
#[cfg(any(
    feature = "mysql",
    all(feature = "postgres", any(feature = "migrations", test))
))]
use sql::artifact_check_expression;
#[cfg(all(feature = "sqlite", feature = "migrations"))]
use sql::artifact_object_sql;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use sql::compact_sql;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sql::default_sql;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sql::identifier_check_from_artifact;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sql::identifier_check_matches;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sql::normalize_check_expression;
#[cfg(feature = "mysql")]
use sql::normalize_mysql_generated_expression;
#[cfg(any(
    feature = "postgres",
    all(feature = "sqlite", feature = "migrations"),
    all(test, feature = "mysql")
))]
use sql::normalize_sql;
#[cfg(all(test, any(feature = "postgres", feature = "mysql")))]
use sql::strip_sql_outer_groups;
