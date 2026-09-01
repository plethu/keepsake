//! `MySQL` and `MariaDB` schema verification.

use super::{
    MYSQL_CLEAN_ARTIFACT, MYSQL_UPGRADE_ARTIFACT, MYSQL_V3_CLEAN_ARTIFACT, RepositoryError,
    RepositoryResult, artifact_check_expression, compact_sql, default_sql, mismatch,
    mysql_catalog_check_matches, normalize_check_expression, normalize_mysql_generated_expression,
};
use crate::repository::backend::KeepsakeSqlxBackend;

#[cfg(feature = "mysql")]
#[derive(Debug, Clone, Copy)]
struct MySqlColumn<'a> {
    table: &'a str,
    name: &'a str,
    column_type: &'a str,
    nullable: bool,
    default: Option<&'a str>,
    auto_increment: bool,
    generated: Option<&'a str>,
}

#[cfg(feature = "mysql")]
const MYSQL_CLEAN_COLUMNS: &[MySqlColumn<'_>] = &[
    MySqlColumn {
        table: "keepsake_schema_metadata",
        name: "key",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_schema_metadata",
        name: "value",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "kind",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "key",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "enabled",
        column_type: "tinyint(1)",
        nullable: false,
        default: Some("1"),
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "expiry_policy",
        column_type: "json",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "created_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "updated_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "subject_kind",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "subject_id",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "relation_id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "state",
        column_type: "varchar(16)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "expiry_policy",
        column_type: "json",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "applied_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "expires_at",
        column_type: "datetime(6)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "fulfilled_at",
        column_type: "datetime(6)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "revoked_at",
        column_type: "datetime(6)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "metadata",
        column_type: "json",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "created_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "updated_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "active_relation_key",
        column_type: "char(36)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: Some("case when state = 'applied' then relation_id end"),
    },
    MySqlColumn {
        table: "keepsakes",
        name: "fulfillment_pending",
        column_type: "tinyint(4)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: Some(
            "case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end",
        ),
    },
    MySqlColumn {
        table: "keepsake_fulfillment_counters",
        name: "keepsake_id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_counters",
        name: "key",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_counters",
        name: "value",
        column_type: "bigint",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_counters",
        name: "observed_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_checklist",
        name: "keepsake_id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_checklist",
        name: "item",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_checklist",
        name: "complete",
        column_type: "tinyint(1)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_checklist",
        name: "observed_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
];

#[cfg(feature = "mysql")]
const MYSQL_V3_TENANT_COLUMNS: &[MySqlColumn<'_>] = &[
    MySqlColumn {
        table: "keepsake_relation_definitions",
        name: "tenant_id",
        column_type: "varbinary(255)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsakes",
        name: "tenant_id",
        column_type: "varbinary(255)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_counters",
        name: "tenant_id",
        column_type: "varbinary(255)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_fulfillment_checklist",
        name: "tenant_id",
        column_type: "varbinary(255)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
];

#[cfg(feature = "mysql")]
fn mysql_v3_clean_columns() -> Vec<MySqlColumn<'static>> {
    // MariaDB does not permit a conditional generated expression to read a
    // CHAR column. The v3 baseline therefore uses VARCHAR for UUID text
    // columns, preserving the wire representation while keeping the
    // active-relation projection portable across both MySQL families.
    MYSQL_CLEAN_COLUMNS
        .iter()
        .copied()
        .map(|mut column| {
            if column.column_type == "char(36)" {
                column.column_type = "varchar(36)";
            }
            column
        })
        .collect()
}

#[cfg(feature = "mysql")]
const MYSQL_LEGACY_COLUMNS: &[MySqlColumn<'_>] = &[
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "id",
        column_type: "bigint",
        nullable: false,
        default: None,
        auto_increment: true,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "keepsake_id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "relation_id",
        column_type: "char(36)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "subject_kind",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "subject_id",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "actor_kind",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "actor_id",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "event_type",
        column_type: "varchar(64)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "decision",
        column_type: "json",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "occurred_at",
        column_type: "datetime(6)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_events",
        name: "recorded_at",
        column_type: "datetime(6)",
        nullable: false,
        default: Some("current_timestamp(6)"),
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_context_attributes",
        name: "audit_event_id",
        column_type: "bigint",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_context_attributes",
        name: "key",
        column_type: "varchar(191)",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_context_attributes",
        name: "value",
        column_type: "text",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "id",
        column_type: "bigint",
        nullable: false,
        default: None,
        auto_increment: true,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "audit_event_id",
        column_type: "bigint",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "event_type",
        column_type: "text",
        nullable: false,
        default: Some("keepsake.audit_event_recorded"),
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "payload",
        column_type: "json",
        nullable: false,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "claimed_by",
        column_type: "text",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "claimed_until",
        column_type: "timestamp(6)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "delivered_at",
        column_type: "timestamp(6)",
        nullable: true,
        default: None,
        auto_increment: false,
        generated: None,
    },
    MySqlColumn {
        table: "keepsake_audit_outbox",
        name: "created_at",
        column_type: "timestamp(6)",
        nullable: false,
        default: Some("current_timestamp(6)"),
        auto_increment: false,
        generated: None,
    },
];

#[cfg(feature = "mysql")]
pub(super) fn mysql_default_matches(actual: Option<&str>, expected: Option<&str>) -> bool {
    match (actual, expected) {
        (None, None) => true,
        // MySQL exposes the implicit NULL default of a nullable generated
        // column as the string `NULL` through some information_schema/SQLx
        // combinations. A quoted 'NULL' remains a real default.
        (Some(actual), None) if actual.trim().eq_ignore_ascii_case("null") => true,
        (Some(actual), Some(expected)) => default_sql(actual) == default_sql(expected),
        _ => false,
    }
}

#[cfg(feature = "mysql")]
pub(super) fn mysql_is_generated_extra(extra: &str) -> bool {
    let extra = extra.to_ascii_lowercase();
    extra.contains("stored generated")
        || extra.contains("virtual generated")
        || extra.contains("generated always")
}

#[cfg(feature = "mysql")]
fn mysql_catalog_type_matches(actual: &str, expected: &str) -> bool {
    // MySQL-family servers disagree about display widths and JSON's catalog
    // representation. Display widths do not change an integer's storage or
    // range, while accepting a different integer family would weaken the
    // schema contract. MariaDB exposes JSON as LONGTEXT and installs a
    // json_valid CHECK; that CHECK is validated with the other constraints.
    let actual = actual.to_ascii_lowercase();
    let expected = expected.to_ascii_lowercase();
    if expected == "json" {
        return actual == "json" || actual == "longtext";
    }

    let actual_family = actual.split('(').next().unwrap_or(&actual);
    let expected_family = expected.split('(').next().unwrap_or(&expected);
    if matches!(
        expected_family,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
    ) {
        return actual_family == expected_family;
    }
    actual == expected
}

#[cfg(feature = "mysql")]
fn mysql_expected_check_expression(activated_upgrade: bool, name: &str) -> Option<String> {
    let artifact = if activated_upgrade {
        MYSQL_UPGRADE_ARTIFACT
    } else {
        MYSQL_CLEAN_ARTIFACT
    };
    let marker = match name {
        "state" if activated_upgrade => "state varchar(16) not null check",
        "state" => "constraint keepsakes_state_check check",
        "keepsakes_expiry_policy_projection" => {
            "constraint keepsakes_expiry_policy_projection check"
        }
        "keepsakes_lifecycle_timestamps" => "constraint keepsakes_lifecycle_timestamps check",
        _ => return None,
    };
    artifact_check_expression(artifact, marker)
}

#[cfg(feature = "mysql")]
fn mysql_v3_expected_check_expression(name: &str) -> Option<String> {
    let marker = match name {
        "state" => "constraint keepsakes_state_check check",
        "keepsakes_expiry_policy_projection" => {
            "constraint keepsakes_expiry_policy_projection check"
        }
        "keepsakes_lifecycle_timestamps" => "constraint keepsakes_lifecycle_timestamps check",
        _ => return None,
    };
    artifact_check_expression(MYSQL_V3_CLEAN_ARTIFACT, marker)
}

#[cfg(feature = "mysql")]
async fn mysql_columns_check<'a>(
    pool: &sqlx::MySqlPool,
    expected: &[MySqlColumn<'a>],
) -> RepositoryResult<Vec<(&'a str, &'a str)>> {
    use sqlx::Row;

    let mut json_longtext_columns = Vec::new();
    for item in expected {
        let row = sqlx::query("select column_type as column_type, is_nullable as is_nullable, column_default as column_default, extra as extra, generation_expression as generation_expression from information_schema.columns where table_schema = database() and table_name = ? and column_name = ?")
            .bind(item.table)
            .bind(item.name)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| mismatch(format!("missing column {}.{}", item.table, item.name)))?;
        let column_type: String = row.try_get("column_type")?;
        let nullable: String = row.try_get("is_nullable")?;
        let default: Option<String> = row.try_get("column_default")?;
        let extra: String = row.try_get("extra")?;
        // MySQL's information_schema reports the empty generation expression
        // as a nullable blank value on some SQLx/server combinations. Treat
        // that representation as absent; a real generated column still has
        // a non-empty expression and is checked below.
        let generation: Option<String> = row
            .try_get::<Option<String>, _>("generation_expression")?
            .filter(|expression| !expression.trim().is_empty());
        let generated_matches = item.generated.map_or_else(
            || generation.is_none() && !mysql_is_generated_extra(&extra),
            |expression| {
                mysql_is_generated_extra(&extra)
                    && generation.as_deref().is_some_and(|actual| {
                        normalize_mysql_generated_expression(actual)
                            == normalize_mysql_generated_expression(expression)
                    })
            },
        );
        let type_matches = mysql_catalog_type_matches(&column_type, item.column_type);
        let nullable_matches = (nullable == "YES") == item.nullable;
        let default_matches = mysql_default_matches(default.as_deref(), item.default);
        let auto_increment_matches =
            extra.to_ascii_lowercase().contains("auto_increment") == item.auto_increment;
        if !type_matches
            || !nullable_matches
            || !default_matches
            || !auto_increment_matches
            || !generated_matches
        {
            let default_class = |value: Option<&str>| match value {
                None => "absent",
                Some(value) if value.trim().eq_ignore_ascii_case("null") => "NULL",
                Some(_) => "value",
            };

            let actual_generation = generation
                .as_deref()
                .map_or_else(|| "absent".to_owned(), normalize_mysql_generated_expression);
            let expected_generation = item
                .generated
                .map_or_else(|| "absent".to_owned(), normalize_mysql_generated_expression);
            return Err(mismatch(format!(
                "column {}.{} has unexpected catalog semantics: type actual={column_type:?} expected={:?} match={type_matches}; nullable actual={nullable:?} expected={} match={nullable_matches}; default actual={} expected={} match={default_matches}; extra={extra:?} auto_increment_match={auto_increment_matches}; generation actual={actual_generation:?} expected={expected_generation:?} match={generated_matches}",
                item.table,
                item.name,
                item.column_type,
                item.nullable,
                default_class(default.as_deref()),
                default_class(item.default),
            )));
        }

        if item.column_type == "json" && column_type.eq_ignore_ascii_case("longtext") {
            json_longtext_columns.push((item.table, item.name));
        }
    }
    Ok(json_longtext_columns)
}

#[cfg(feature = "mysql")]
#[allow(clippy::too_many_lines)]
async fn mysql_catalog_shape_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    let tables: &[&str] = if activated_upgrade {
        &[
            "keepsake_schema_metadata",
            "keepsake_relation_definitions",
            "keepsakes",
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_checklist",
            "keepsake_audit_events",
            "keepsake_audit_context_attributes",
            "keepsake_audit_outbox",
        ]
    } else {
        &[
            "keepsake_schema_metadata",
            "keepsake_relation_definitions",
            "keepsakes",
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_checklist",
        ]
    };

    let expected: Vec<MySqlColumn<'_>> = MYSQL_CLEAN_COLUMNS
        .iter()
        .chain(
            activated_upgrade
                .then_some(MYSQL_LEGACY_COLUMNS)
                .into_iter()
                .flatten(),
        )
        .copied()
        .collect();
    let table_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name in (?,?,?,?,?,?,?,?)").bind(tables.first().unwrap_or(&"")).bind(tables.get(1).unwrap_or(&"")).bind(tables.get(2).unwrap_or(&"")).bind(tables.get(3).unwrap_or(&"")).bind(tables.get(4).unwrap_or(&"")).bind(tables.get(5).unwrap_or(&"")).bind(tables.get(6).unwrap_or(&"")).bind(tables.get(7).unwrap_or(&"")).fetch_one(pool).await?;
    if table_count != i64::try_from(tables.len()).unwrap_or(i64::MAX) {
        return Err(mismatch(format!(
            "expected {} domain tables, found {table_count}",
            tables.len()
        )));
    }

    if !activated_upgrade {
        let legacy = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name in ('keepsake_audit_events','keepsake_audit_context_attributes','keepsake_audit_outbox')").fetch_one(pool).await?;
        if legacy != 0 {
            return Err(mismatch("clean track contains legacy audit tables"));
        }
    }

    let json_longtext_columns = mysql_columns_check(pool, &expected).await?;

    let column_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.columns where table_schema = database() and table_name in (?,?,?,?,?,?,?,?)").bind(tables.first().unwrap_or(&"")).bind(tables.get(1).unwrap_or(&"")).bind(tables.get(2).unwrap_or(&"")).bind(tables.get(3).unwrap_or(&"")).bind(tables.get(4).unwrap_or(&"")).bind(tables.get(5).unwrap_or(&"")).bind(tables.get(6).unwrap_or(&"")).bind(tables.get(7).unwrap_or(&"")).fetch_one(pool).await?;
    if column_count != i64::try_from(expected.len()).unwrap_or(i64::MAX) {
        return Err(mismatch("domain tables contain unexpected columns"));
    }

    let server_version = sqlx::query_scalar::<_, String>("select version()")
        .fetch_one(pool)
        .await?;
    let maria_db = server_version.to_ascii_lowercase().contains("mariadb");
    mysql_constraints_check(pool, activated_upgrade, &json_longtext_columns, maria_db).await?;
    mysql_indexes_check(pool, activated_upgrade).await?;
    let trigger_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.triggers where trigger_schema = database() and event_object_table in (?,?,?,?,?)").bind("keepsake_schema_metadata").bind("keepsake_relation_definitions").bind("keepsakes").bind("keepsake_fulfillment_counters").bind("keepsake_fulfillment_checklist").fetch_one(pool).await?;
    if trigger_count != 0 {
        return Err(mismatch(
            "unexpected trigger mutates a Keepsake invariant table",
        ));
    }
    Ok(())
}

#[cfg(feature = "mysql")]
#[allow(clippy::too_many_lines)]
async fn mysql_constraints_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
    json_longtext_columns: &[(&str, &str)],
    maria_db: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let tables: &[&str] = if activated_upgrade {
        &[
            "keepsake_schema_metadata",
            "keepsake_relation_definitions",
            "keepsakes",
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_checklist",
            "keepsake_audit_events",
            "keepsake_audit_context_attributes",
            "keepsake_audit_outbox",
        ]
    } else {
        &[
            "keepsake_schema_metadata",
            "keepsake_relation_definitions",
            "keepsakes",
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_checklist",
        ]
    };
    // CHECK constraints are queried separately below: MariaDB adds a
    // json_valid CHECK for every JSON-as-LONGTEXT column, while Oracle
    // MySQL represents those columns as native JSON without the extra row.
    let rows = sqlx::query("select table_name as table_name, constraint_name as constraint_name, constraint_type as constraint_type from information_schema.table_constraints where constraint_schema = database() and table_name in (?,?,?,?,?,?,?,?) and constraint_type in ('PRIMARY KEY','UNIQUE','FOREIGN KEY')").bind(tables.first().unwrap_or(&"")).bind(tables.get(1).unwrap_or(&"")).bind(tables.get(2).unwrap_or(&"")).bind(tables.get(3).unwrap_or(&"")).bind(tables.get(4).unwrap_or(&"")).bind(tables.get(5).unwrap_or(&"")).bind(tables.get(6).unwrap_or(&"")).bind(tables.get(7).unwrap_or(&"")).fetch_all(pool).await?;
    let expected: &[(&str, &str, &str)] = if activated_upgrade {
        &[
            ("keepsake_schema_metadata", "PRIMARY", "PRIMARY KEY"),
            ("keepsake_relation_definitions", "PRIMARY", "PRIMARY KEY"),
            ("keepsake_relation_definitions", "kind", "UNIQUE"),
            ("keepsakes", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsakes",
                "keepsakes_one_active_relation_per_subject",
                "UNIQUE",
            ),
            ("keepsakes", "keepsakes_relation_fk", "FOREIGN KEY"),
            ("keepsake_fulfillment_counters", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_fulfillment_counters",
                "keepsake_fulfillment_counters_keepsake_fk",
                "FOREIGN KEY",
            ),
            ("keepsake_fulfillment_checklist", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_fulfillment_checklist",
                "keepsake_fulfillment_checklist_keepsake_fk",
                "FOREIGN KEY",
            ),
            ("keepsake_audit_events", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_audit_context_attributes",
                "PRIMARY",
                "PRIMARY KEY",
            ),
            (
                "keepsake_audit_context_attributes",
                "keepsake_audit_context_attributes_event_fk",
                "FOREIGN KEY",
            ),
            ("keepsake_audit_outbox", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_audit_outbox",
                "keepsake_audit_outbox_event_fk",
                "FOREIGN KEY",
            ),
        ]
    } else {
        &[
            ("keepsake_schema_metadata", "PRIMARY", "PRIMARY KEY"),
            ("keepsake_relation_definitions", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_relation_definitions",
                "keepsake_relation_definitions_kind_key_unique",
                "UNIQUE",
            ),
            ("keepsakes", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsakes",
                "keepsakes_one_active_relation_per_subject",
                "UNIQUE",
            ),
            ("keepsakes", "keepsakes_relation_fk", "FOREIGN KEY"),
            ("keepsake_fulfillment_counters", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_fulfillment_counters",
                "keepsake_fulfillment_counters_keepsake_fk",
                "FOREIGN KEY",
            ),
            ("keepsake_fulfillment_checklist", "PRIMARY", "PRIMARY KEY"),
            (
                "keepsake_fulfillment_checklist",
                "keepsake_fulfillment_checklist_keepsake_fk",
                "FOREIGN KEY",
            ),
        ]
    };

    if rows.len() != expected.len() {
        return Err(mismatch(
            "primary, foreign, unique, or check constraint count differs",
        ));
    }

    for row in rows {
        let table: String = row.try_get("table_name")?;
        let name: String = row.try_get("constraint_name")?;
        let kind: String = row.try_get("constraint_type")?;
        if !expected
            .iter()
            .any(|(t, n, k)| *t == table && *n == name && *k == kind)
        {
            return Err(mismatch(format!(
                "unexpected or altered constraint {table}.{name}"
            )));
        }
    }

    let foreign_keys: &[(&str, &str, &str, &str, &str)] = &[
        (
            "keepsakes",
            "keepsakes_relation_fk",
            "relation_id",
            "keepsake_relation_definitions",
            "id",
        ),
        (
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_counters_keepsake_fk",
            "keepsake_id",
            "keepsakes",
            "id",
        ),
        (
            "keepsake_fulfillment_checklist",
            "keepsake_fulfillment_checklist_keepsake_fk",
            "keepsake_id",
            "keepsakes",
            "id",
        ),
    ];
    for (table, constraint, column, referenced_table, referenced_column) in foreign_keys {
        let row = sqlx::query(
            "select column_name as column_name, referenced_table_name as referenced_table_name, referenced_column_name as referenced_column_name from information_schema.key_column_usage where constraint_schema = database() and table_name = ? and constraint_name = ? and ordinal_position = 1",
        )
        .bind(table)
        .bind(constraint)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing foreign key {table}.{constraint}")))?;
        let actual_column: String = row.try_get("column_name")?;
        let actual_table: Option<String> = row.try_get("referenced_table_name")?;
        let actual_ref_column: Option<String> = row.try_get("referenced_column_name")?;
        if actual_column != *column
            || actual_table.as_deref() != Some(*referenced_table)
            || actual_ref_column.as_deref() != Some(*referenced_column)
        {
            return Err(mismatch(format!(
                "foreign key {table}.{constraint} references the wrong column"
            )));
        }
    }

    if activated_upgrade {
        for (table, constraint) in [
            (
                "keepsake_audit_context_attributes",
                "keepsake_audit_context_attributes_event_fk",
            ),
            ("keepsake_audit_outbox", "keepsake_audit_outbox_event_fk"),
        ] {
            let row = sqlx::query(
                "select column_name as column_name, referenced_table_name as referenced_table_name, referenced_column_name as referenced_column_name from information_schema.key_column_usage where constraint_schema = database() and table_name = ? and constraint_name = ? and ordinal_position = 1",
            )
            .bind(table)
            .bind(constraint)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| mismatch(format!("missing foreign key {table}.{constraint}")))?;
            let column: String = row.try_get("column_name")?;
            let referenced_table: Option<String> = row.try_get("referenced_table_name")?;
            let referenced_column: Option<String> = row.try_get("referenced_column_name")?;
            if column != "audit_event_id"
                || referenced_table.as_deref() != Some("keepsake_audit_events")
                || referenced_column.as_deref() != Some("id")
            {
                return Err(mismatch(format!(
                    "foreign key {table}.{constraint} references the wrong column"
                )));
            }
        }
    }

    let expected_actions = [
        ("keepsakes", "keepsakes_relation_fk", "NO ACTION"),
        (
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_counters_keepsake_fk",
            "CASCADE",
        ),
        (
            "keepsake_fulfillment_checklist",
            "keepsake_fulfillment_checklist_keepsake_fk",
            "CASCADE",
        ),
    ];
    for (table, constraint, expected_action) in expected_actions {
        let action = sqlx::query_scalar::<_, String>(
            "select delete_rule as delete_rule from information_schema.referential_constraints where constraint_schema = database() and table_name = ? and constraint_name = ?",
        )
        .bind(table)
        .bind(constraint)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing referential action {table}.{constraint}")))?;
        if action != expected_action {
            return Err(mismatch(format!(
                "foreign key {table}.{constraint} has delete action {action}"
            )));
        }
    }

    if activated_upgrade {
        for constraint in [
            "keepsake_audit_context_attributes_event_fk",
            "keepsake_audit_outbox_event_fk",
        ] {
            let action = sqlx::query_scalar::<_, String>(
                "select delete_rule as delete_rule from information_schema.referential_constraints where constraint_schema = database() and constraint_name = ?",
            )
            .bind(constraint)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| mismatch(format!("missing referential action {constraint}")))?;
            if action != "CASCADE" {
                return Err(mismatch(format!(
                    "foreign key {constraint} has delete action {action}"
                )));
            }
        }
    }

    let check_query = if maria_db {
        "select tc.table_name as table_name, tc.constraint_name as constraint_name, cc.check_clause as check_clause from information_schema.table_constraints tc join information_schema.check_constraints cc on cc.constraint_schema = tc.constraint_schema and cc.table_name = tc.table_name and cc.constraint_name = tc.constraint_name where tc.constraint_schema = database() and tc.table_name in (?,?,?,?,?,?,?,?) and tc.constraint_type = 'CHECK'"
    } else {
        "select tc.table_name as table_name, tc.constraint_name as constraint_name, cc.check_clause as check_clause from information_schema.table_constraints tc join information_schema.check_constraints cc on cc.constraint_schema = tc.constraint_schema and cc.constraint_name = tc.constraint_name where tc.constraint_schema = database() and tc.table_name in (?,?,?,?,?,?,?,?) and tc.constraint_type = 'CHECK'"
    };

    let checks = sqlx::query(check_query)
        .bind(tables.first().unwrap_or(&""))
        .bind(tables.get(1).unwrap_or(&""))
        .bind(tables.get(2).unwrap_or(&""))
        .bind(tables.get(3).unwrap_or(&""))
        .bind(tables.get(4).unwrap_or(&""))
        .bind(tables.get(5).unwrap_or(&""))
        .bind(tables.get(6).unwrap_or(&""))
        .bind(tables.get(7).unwrap_or(&""))
        .fetch_all(pool)
        .await?;
    let historical_state_name = if maria_db { "state" } else { "keepsakes_chk_1" };

    let expected_checks = [
        (
            "keepsakes",
            if activated_upgrade {
                historical_state_name
            } else {
                "keepsakes_state_check"
            },
            "state",
        ),
        (
            "keepsakes",
            "keepsakes_expiry_policy_projection",
            "keepsakes_expiry_policy_projection",
        ),
        (
            "keepsakes",
            "keepsakes_lifecycle_timestamps",
            "keepsakes_lifecycle_timestamps",
        ),
    ];
    let mut found_checks = std::collections::BTreeSet::new();
    let mut json_checks = std::collections::BTreeSet::new();
    for row in checks {
        let table: String = row.try_get("table_name")?;
        let name: String = row.try_get("constraint_name")?;
        let clause: String = row.try_get("check_clause")?;
        let named_check = expected_checks
            .iter()
            .find(|(expected_table, expected_name, _)| {
                *expected_table == table && *expected_name == name
            });
        if let Some((_, _, logical_name)) = named_check {
            let expected_clause = mysql_expected_check_expression(activated_upgrade, logical_name)
                .ok_or_else(|| mismatch(format!("missing migration CHECK {table}.{name}")))?;
            if !mysql_catalog_check_matches(
                activated_upgrade,
                &name,
                &clause,
                &expected_clause,
                maria_db,
            ) {
                return Err(mismatch(format!(
                    "CHECK constraint {table}.{name} definition differs: actual={:?} expected={:?}",
                    normalize_check_expression(&clause),
                    normalize_check_expression(&expected_clause)
                )));
            }

            found_checks.insert(name.clone());
        }

        let compact_clause = compact_sql(&clause);
        let json_check = json_longtext_columns.iter().find(|(json_table, column)| {
            *json_table == table
                && compact_clause == format!("json_valid({})", column.to_ascii_lowercase())
        });
        if let Some((json_table, column)) = json_check {
            json_checks.insert(format!("{json_table}.{column}"));
        }

        if named_check.is_none() && json_check.is_none() {
            return Err(mismatch(format!(
                "unexpected or altered CHECK constraint {table}.{name}"
            )));
        }
    }

    if found_checks.len() != expected_checks.len()
        || json_longtext_columns
            .iter()
            .any(|(table, column)| !json_checks.contains(&format!("{table}.{column}")))
    {
        return Err(mismatch("Keepsake CHECK constraint definitions differ"));
    }

    Ok(())
}

#[cfg(feature = "mysql")]
#[allow(clippy::too_many_lines)]
#[allow(clippy::excessive_nesting)]
async fn mysql_indexes_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let relation_unique_name = if activated_upgrade {
        "kind"
    } else {
        "keepsake_relation_definitions_kind_key_unique"
    };
    let expected: &[(&str, &str, bool, &[&str])] = &[
        ("PRIMARY", "keepsake_schema_metadata", true, &["key"]),
        ("PRIMARY", "keepsake_relation_definitions", true, &["id"]),
        (
            relation_unique_name,
            "keepsake_relation_definitions",
            true,
            &["kind", "key"],
        ),
        ("PRIMARY", "keepsakes", true, &["id"]),
        (
            "keepsakes_one_active_relation_per_subject",
            "keepsakes",
            true,
            &["subject_kind", "subject_id", "active_relation_key"],
        ),
        (
            "keepsakes_active_subject_lookup",
            "keepsakes",
            false,
            &["subject_kind", "subject_id", "relation_id", "id"],
        ),
        (
            "keepsakes_active_relation_membership",
            "keepsakes",
            false,
            &["relation_id", "subject_kind", "subject_id", "id"],
        ),
        (
            "keepsakes_due_timed_expiry",
            "keepsakes",
            false,
            &[
                "expires_at",
                "relation_id",
                "subject_kind",
                "subject_id",
                "id",
            ],
        ),
        (
            "keepsake_fulfillment_counter_scan",
            "keepsake_fulfillment_counters",
            false,
            &["key", "value", "keepsake_id"],
        ),
        (
            "keepsakes_due_fulfilled_expiry",
            "keepsakes",
            false,
            &[
                "fulfillment_pending",
                "relation_id",
                "subject_kind",
                "subject_id",
                "id",
            ],
        ),
        (
            "keepsake_fulfillment_checklist_scan",
            "keepsake_fulfillment_checklist",
            false,
            &["item", "complete", "keepsake_id"],
        ),
        (
            "PRIMARY",
            "keepsake_fulfillment_counters",
            true,
            &["keepsake_id", "key"],
        ),
        (
            "PRIMARY",
            "keepsake_fulfillment_checklist",
            true,
            &["keepsake_id", "item"],
        ),
    ];
    for (name, table, unique, columns) in expected {
        let rows = sqlx::query("select non_unique as non_unique, seq_in_index as seq_in_index, column_name as column_name, sub_part as sub_part from information_schema.statistics where table_schema = database() and table_name = ? and index_name = ? order by seq_in_index").bind(table).bind(name).fetch_all(pool).await?;
        if rows.len() != columns.len() {
            return Err(mismatch(format!("index {name} column count differs")));
        }

        for (offset, row) in rows.iter().enumerate() {
            let non_unique: i64 = row.try_get("non_unique")?;
            let actual: Option<String> = row.try_get("column_name")?;
            let sub_part: Option<i64> = row.try_get("sub_part")?;
            if (non_unique == 0) != *unique
                || actual.as_deref() != Some(columns[offset])
                || sub_part.is_some()
            {
                return Err(mismatch(format!(
                    "index {name} columns or uniqueness differ"
                )));
            }
        }
    }

    if activated_upgrade {
        for (name, table, unique, columns) in [
            ("PRIMARY", "keepsake_audit_events", true, &["id"][..]),
            (
                "PRIMARY",
                "keepsake_audit_context_attributes",
                true,
                &["audit_event_id", "key"][..],
            ),
            ("PRIMARY", "keepsake_audit_outbox", true, &["id"][..]),
            (
                "keepsake_audit_by_keepsake",
                "keepsake_audit_events",
                false,
                &["keepsake_id", "occurred_at", "id"][..],
            ),
            (
                "keepsake_audit_by_relation",
                "keepsake_audit_events",
                false,
                &["relation_id", "occurred_at", "id"][..],
            ),
            (
                "keepsake_audit_context_attribute_lookup",
                "keepsake_audit_context_attributes",
                false,
                &["key", "value", "audit_event_id"][..],
            ),
            (
                "keepsake_audit_outbox_export",
                "keepsake_audit_outbox",
                false,
                &["id"][..],
            ),
            (
                "keepsake_audit_outbox_claim",
                "keepsake_audit_outbox",
                false,
                &["delivered_at", "claimed_until", "id"][..],
            ),
        ] {
            let rows = sqlx::query("select non_unique as non_unique, column_name as column_name, sub_part as sub_part from information_schema.statistics where table_schema = database() and table_name = ? and index_name = ? order by seq_in_index").bind(table).bind(name).fetch_all(pool).await?;
            if rows.len() != columns.len() {
                return Err(mismatch(format!("index {name} column count differs")));
            }

            for (offset, row) in rows.iter().enumerate() {
                let non_unique: i64 = row.try_get("non_unique")?;
                let actual: Option<String> = row.try_get("column_name")?;
                let sub_part: Option<i64> = row.try_get("sub_part")?;
                let expected_sub_part = (name == "keepsake_audit_context_attribute_lookup"
                    && columns[offset] == "value")
                    .then_some(191);
                if (non_unique == 0) != unique
                    || actual.as_deref() != Some(columns[offset])
                    || sub_part != expected_sub_part
                {
                    return Err(mismatch(format!(
                        "index {name} columns or uniqueness differ"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(all(feature = "mysql", feature = "migrations"))]
pub(in crate::repository) async fn mysql_upgrade_schema_check(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    mysql_catalog_shape_check(pool, true).await
}

#[cfg(feature = "mysql")]
#[allow(clippy::too_many_lines)]
async fn mysql_v3_domain_shape_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    let expected_tables = [
        "keepsake_schema_metadata",
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ];
    let v3_clean_columns = mysql_v3_clean_columns();
    let expected: Vec<MySqlColumn<'_>> = v3_clean_columns
        .iter()
        .chain(MYSQL_V3_TENANT_COLUMNS.iter())
        .copied()
        .collect();
    let json_longtext_columns = mysql_columns_check(pool, &expected).await?;
    let column_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.columns where table_schema = database() and table_name in (?,?,?,?,?,?,?,?)")
        .bind(expected_tables.first().unwrap_or(&""))
        .bind(expected_tables.get(1).unwrap_or(&""))
        .bind(expected_tables.get(2).unwrap_or(&""))
        .bind(expected_tables.get(3).unwrap_or(&""))
        .bind(expected_tables.get(4).unwrap_or(&""))
        .bind(expected_tables.get(5).unwrap_or(&""))
        .bind(expected_tables.get(6).unwrap_or(&""))
        .bind(expected_tables.get(7).unwrap_or(&""))
        .fetch_one(pool)
        .await?;
    if column_count != i64::try_from(expected.len()).unwrap_or(i64::MAX) {
        return Err(mismatch("domain tables contain unexpected columns"));
    }

    let server_version = sqlx::query_scalar::<_, String>("select version()")
        .fetch_one(pool)
        .await?;
    // Validate key topology before CHECK expressions so a partially damaged
    // catalog reports the actionable PK/index/FK drift directly.
    mysql_v3_indexes_check(pool).await?;
    mysql_v3_foreign_keys_check(pool).await?;
    mysql_v3_constraints_check(
        pool,
        &json_longtext_columns,
        server_version.to_ascii_lowercase().contains("mariadb"),
    )
    .await?;

    let tenant_columns = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = database() and column_name = 'tenant_id' and is_nullable = 'NO' and table_name in (?,?,?,?)",
    )
    .bind("keepsake_relation_definitions")
    .bind("keepsakes")
    .bind("keepsake_fulfillment_counters")
    .bind("keepsake_fulfillment_checklist")
    .fetch_one(pool)
    .await?;
    if tenant_columns != 4 {
        return Err(mismatch(format!(
            "v3 schema has {tenant_columns} of 4 tenant_id columns"
        )));
    }

    let tenant_checks = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.table_constraints where constraint_schema = database() and constraint_type = 'CHECK' and constraint_name in (?,?,?,?,?,?,?,?)",
    )
    .bind("keepsake_relation_definitions_tenant_size")
    .bind("keepsake_relation_definitions_tenant_nonempty")
    .bind("keepsakes_tenant_size")
    .bind("keepsakes_tenant_nonempty")
    .bind("keepsake_fulfillment_counter_tenant_size")
    .bind("keepsake_fulfillment_counter_tenant_nonempty")
    .bind("keepsake_fulfillment_checklist_tenant_size")
    .bind("keepsake_fulfillment_checklist_tenant_nonempty")
    .fetch_one(pool)
    .await?;
    if tenant_checks != 8 {
        return Err(mismatch("v3 tenant size/non-empty checks are incomplete"));
    }

    Ok(())
}

#[cfg(feature = "mysql")]
#[allow(clippy::too_many_lines)]
async fn mysql_v3_indexes_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    use sqlx::Row;

    let expected: &[(&str, &str, bool, &[&str])] = &[
        (
            "keepsake_relation_definitions",
            "PRIMARY",
            true,
            &["tenant_id", "id"],
        ),
        (
            "keepsake_relation_definitions",
            "keepsake_relation_definitions_tenant_key",
            true,
            &["tenant_id", "kind", "key"],
        ),
        (
            "keepsake_relation_definitions",
            "keepsake_relation_definitions_tenant_key_idx",
            false,
            &["tenant_id", "kind", "key", "id"],
        ),
        ("keepsakes", "PRIMARY", true, &["tenant_id", "id"]),
        (
            "keepsakes",
            "keepsakes_one_active_relation_per_subject",
            true,
            &[
                "tenant_id",
                "subject_kind",
                "subject_id",
                "active_relation_key",
            ],
        ),
        (
            "keepsakes",
            "keepsakes_active_subject_lookup",
            false,
            &[
                "tenant_id",
                "subject_kind",
                "subject_id",
                "relation_id",
                "id",
            ],
        ),
        (
            "keepsakes",
            "keepsakes_active_relation_membership",
            false,
            &[
                "tenant_id",
                "relation_id",
                "subject_kind",
                "subject_id",
                "id",
            ],
        ),
        (
            "keepsakes",
            "keepsakes_due_timed_expiry",
            false,
            &[
                "tenant_id",
                "expires_at",
                "relation_id",
                "subject_kind",
                "subject_id",
                "id",
            ],
        ),
        (
            "keepsakes",
            "keepsakes_due_fulfilled_expiry",
            false,
            &[
                "tenant_id",
                "fulfillment_pending",
                "relation_id",
                "subject_kind",
                "subject_id",
                "id",
            ],
        ),
        (
            "keepsake_fulfillment_counters",
            "PRIMARY",
            true,
            &["tenant_id", "keepsake_id", "key"],
        ),
        (
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_counter_scan",
            false,
            &["tenant_id", "key", "value", "keepsake_id"],
        ),
        (
            "keepsake_fulfillment_checklist",
            "PRIMARY",
            true,
            &["tenant_id", "keepsake_id", "item"],
        ),
        (
            "keepsake_fulfillment_checklist",
            "keepsake_fulfillment_checklist_scan",
            false,
            &["tenant_id", "item", "complete", "keepsake_id"],
        ),
    ];

    let tables = [
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ];
    let names = sqlx::query_as::<_, (String, String)>(
        "select distinct table_name, index_name from information_schema.statistics where table_schema = database() and table_name in (?,?,?,?)",
    )
    .bind(tables[0])
    .bind(tables[1])
    .bind(tables[2])
    .bind(tables[3])
    .fetch_all(pool)
    .await?;
    if names.len() != expected.len()
        || names.iter().any(|(table, name)| {
            !expected
                .iter()
                .any(|(expected_table, expected_name, _, _)| {
                    *expected_table == table && *expected_name == name
                })
        })
    {
        return Err(mismatch(format!("v3 domain index set differs: {names:?}")));
    }

    for (table, name, unique, columns) in expected {
        let rows = sqlx::query(
            "select non_unique as non_unique, cast(seq_in_index as signed) as seq_in_index, column_name as column_name, sub_part as sub_part from information_schema.statistics where table_schema = database() and table_name = ? and index_name = ? order by seq_in_index",
        )
        .bind(table)
        .bind(name)
        .fetch_all(pool)
        .await?;
        if rows.len() != columns.len() {
            return Err(mismatch(format!(
                "index {table}.{name} column count differs"
            )));
        }

        for (offset, row) in rows.iter().enumerate() {
            let non_unique: i64 = row.try_get("non_unique")?;
            let seq_in_index: i64 = row.try_get("seq_in_index")?;
            let column: String = row.try_get("column_name")?;
            let sub_part: Option<i64> = row.try_get("sub_part")?;
            if seq_in_index != i64::try_from(offset + 1).unwrap_or(i64::MAX)
                || (non_unique == 0) != *unique
                || column != columns[offset]
                || sub_part.is_some()
            {
                return Err(mismatch(format!(
                    "index {table}.{name} columns or uniqueness differ"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "mysql")]
pub(super) fn mysql_v3_referential_action_matches(expected: &str, actual: &str) -> bool {
    actual == expected || (expected == "NO ACTION" && actual == "RESTRICT")
}

#[cfg(feature = "mysql")]
#[allow(clippy::type_complexity)]
async fn mysql_v3_foreign_keys_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    use sqlx::Row;

    let expected: &[(&str, &str, &[(&str, &str)], &str)] = &[
        (
            "keepsakes",
            "keepsakes_relation_fk",
            &[("tenant_id", "tenant_id"), ("relation_id", "id")],
            "NO ACTION",
        ),
        (
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_counter_keepsake_fk",
            &[("tenant_id", "tenant_id"), ("keepsake_id", "id")],
            "CASCADE",
        ),
        (
            "keepsake_fulfillment_checklist",
            "keepsake_fulfillment_checklist_keepsake_fk",
            &[("tenant_id", "tenant_id"), ("keepsake_id", "id")],
            "CASCADE",
        ),
    ];

    let tables = [
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ];
    let names = sqlx::query_as::<_, (String, String)>(
        "select distinct table_name, constraint_name from information_schema.key_column_usage where constraint_schema = database() and referenced_table_name is not null and table_name in (?,?,?,?)",
    )
    .bind(tables[0])
    .bind(tables[1])
    .bind(tables[2])
    .bind(tables[3])
    .fetch_all(pool)
    .await?;
    if names.len() != expected.len()
        || names.iter().any(|(table, name)| {
            !expected
                .iter()
                .any(|(expected_table, expected_name, _, _)| {
                    *expected_table == table && *expected_name == name
                })
        })
    {
        return Err(mismatch(format!(
            "v3 domain foreign-key set differs: {names:?}"
        )));
    }

    for (table, name, columns, delete_rule) in expected {
        let rows = sqlx::query(
            "select cast(kcu.ordinal_position as signed) as ordinal_position, kcu.column_name as column_name, kcu.referenced_table_name as referenced_table_name, kcu.referenced_column_name as referenced_column_name, rc.delete_rule as delete_rule, rc.update_rule as update_rule from information_schema.key_column_usage kcu join information_schema.referential_constraints rc on rc.constraint_schema = kcu.constraint_schema and rc.table_name = kcu.table_name and rc.constraint_name = kcu.constraint_name where kcu.constraint_schema = database() and kcu.table_name = ? and kcu.constraint_name = ? order by kcu.ordinal_position",
        )
        .bind(table)
        .bind(name)
        .fetch_all(pool)
        .await?;
        if rows.len() != columns.len() {
            return Err(mismatch(format!(
                "foreign key {table}.{name} column count differs"
            )));
        }

        for (offset, row) in rows.iter().enumerate() {
            let ordinal: i64 = row.try_get("ordinal_position")?;
            let local: String = row.try_get("column_name")?;
            let referenced_table: Option<String> = row.try_get("referenced_table_name")?;
            let referenced_column: Option<String> = row.try_get("referenced_column_name")?;
            let actual_rule: String = row.try_get("delete_rule")?;
            let actual_update_rule: String = row.try_get("update_rule")?;
            let (expected_local, expected_referenced) = columns[offset];
            if ordinal != i64::try_from(offset + 1).unwrap_or(i64::MAX)
                || local != expected_local
                || referenced_table.as_deref()
                    != Some(if *table == "keepsakes" {
                        "keepsake_relation_definitions"
                    } else {
                        "keepsakes"
                    })
                || referenced_column.as_deref() != Some(expected_referenced)
            {
                return Err(mismatch(format!(
                    "foreign key {table}.{name} columns or target differ"
                )));
            }

            if !mysql_v3_referential_action_matches(delete_rule, &actual_rule) {
                return Err(mismatch(format!(
                    "foreign key {table}.{name} delete action differs: actual={actual_rule}, expected={delete_rule}"
                )));
            }

            if !mysql_v3_referential_action_matches("NO ACTION", &actual_update_rule) {
                return Err(mismatch(format!(
                    "foreign key {table}.{name} update action differs: actual={actual_update_rule}, expected=NO ACTION"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "mysql")]
#[allow(clippy::too_many_lines)]
async fn mysql_v3_constraints_check(
    pool: &sqlx::MySqlPool,
    json_longtext_columns: &[(&str, &str)],
    maria_db: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let tables = [
        "keepsake_schema_metadata",
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ];
    let check_query = if maria_db {
        "select tc.table_name as table_name, tc.constraint_name as constraint_name, cc.check_clause as check_clause from information_schema.table_constraints tc join information_schema.check_constraints cc on cc.constraint_schema = tc.constraint_schema and cc.table_name = tc.table_name and cc.constraint_name = tc.constraint_name where tc.constraint_schema = database() and tc.table_name in (?,?,?,?,?,?,?,?) and tc.constraint_type = 'CHECK'"
    } else {
        "select tc.table_name as table_name, tc.constraint_name as constraint_name, cc.check_clause as check_clause from information_schema.table_constraints tc join information_schema.check_constraints cc on cc.constraint_schema = tc.constraint_schema and cc.constraint_name = tc.constraint_name where tc.constraint_schema = database() and tc.table_name in (?,?,?,?,?,?,?,?) and tc.constraint_type = 'CHECK'"
    };
    let checks = sqlx::query(check_query)
        .bind(tables.first().unwrap_or(&""))
        .bind(tables.get(1).unwrap_or(&""))
        .bind(tables.get(2).unwrap_or(&""))
        .bind(tables.get(3).unwrap_or(&""))
        .bind(tables.get(4).unwrap_or(&""))
        .bind(tables.get(5).unwrap_or(&""))
        .bind(tables.get(6).unwrap_or(&""))
        .bind(tables.get(7).unwrap_or(&""))
        .fetch_all(pool)
        .await?;
    let expected_checks = [
        ("keepsakes", "keepsakes_state_check", "state"),
        (
            "keepsakes",
            "keepsakes_expiry_policy_projection",
            "keepsakes_expiry_policy_projection",
        ),
        (
            "keepsakes",
            "keepsakes_lifecycle_timestamps",
            "keepsakes_lifecycle_timestamps",
        ),
    ];
    let tenant_checks = [
        (
            "keepsake_relation_definitions_tenant_size",
            "octet_length(tenant_id)<=255",
        ),
        (
            "keepsake_relation_definitions_tenant_nonempty",
            "octet_length(tenant_id)>0",
        ),
        ("keepsakes_tenant_size", "octet_length(tenant_id)<=255"),
        ("keepsakes_tenant_nonempty", "octet_length(tenant_id)>0"),
        (
            "keepsake_fulfillment_counter_tenant_size",
            "octet_length(tenant_id)<=255",
        ),
        (
            "keepsake_fulfillment_counter_tenant_nonempty",
            "octet_length(tenant_id)>0",
        ),
        (
            "keepsake_fulfillment_checklist_tenant_size",
            "octet_length(tenant_id)<=255",
        ),
        (
            "keepsake_fulfillment_checklist_tenant_nonempty",
            "octet_length(tenant_id)>0",
        ),
    ];
    let mut found_checks = std::collections::BTreeSet::new();
    let mut found_tenant_checks = std::collections::BTreeSet::new();
    let mut found_json_checks = std::collections::BTreeSet::new();
    for row in checks {
        let table: String = row.try_get("table_name")?;
        let name: String = row.try_get("constraint_name")?;
        let clause: String = row.try_get("check_clause")?;
        if let Some((_, _, logical_name)) =
            expected_checks
                .iter()
                .find(|(expected_table, expected_name, _)| {
                    *expected_table == table && *expected_name == name
                })
        {
            let expected_clause = mysql_v3_expected_check_expression(logical_name)
                .ok_or_else(|| mismatch(format!("missing migration CHECK {table}.{name}")))?;
            if !mysql_catalog_check_matches(false, &name, &clause, &expected_clause, maria_db) {
                return Err(mismatch(format!(
                    "CHECK constraint {table}.{name} definition differs"
                )));
            }
            found_checks.insert(name.clone());
            continue;
        }

        if let Some((tenant_name, fragment)) = tenant_checks
            .iter()
            .find(|(expected_name, _)| *expected_name == name)
        {
            let normalized = normalize_check_expression(&clause);
            // MySQL deparses OCTET_LENGTH as its equivalent LENGTH function;
            // MariaDB commonly preserves the source spelling.
            let mysql_fragment = fragment.replace("octet_length", "length");
            if !normalized.contains(fragment) && !normalized.contains(&mysql_fragment) {
                return Err(mismatch(format!(
                    "tenant CHECK constraint {table}.{name} definition differs"
                )));
            }
            found_tenant_checks.insert(*tenant_name);
            continue;
        }

        let json_check = json_longtext_columns.iter().find(|(json_table, column)| {
            *json_table == table
                && compact_sql(&clause) == format!("json_valid({})", column.to_ascii_lowercase())
        });
        if let Some((json_table, column)) = json_check {
            found_json_checks.insert(format!("{json_table}.{column}"));
            continue;
        }

        return Err(mismatch(format!(
            "unexpected or altered CHECK constraint {table}.{name}"
        )));
    }

    if found_checks.len() != expected_checks.len()
        || found_tenant_checks.len() != tenant_checks.len()
        || json_longtext_columns
            .iter()
            .any(|(table, column)| !found_json_checks.contains(&format!("{table}.{column}")))
    {
        return Err(mismatch("Keepsake v3 CHECK constraint definitions differ"));
    }
    Ok(())
}

#[cfg(feature = "mysql")]
pub(in crate::repository) async fn mysql_runtime_schema_check(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let metadata_exists = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'",
    )
    .fetch_one(pool)
    .await?;
    if metadata_exists == 0 {
        return Err(mismatch("missing Keepsake schema metadata table"));
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'backend'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if backend.as_deref() != Some(super::super::MySqlBackend::NAME) {
        return Err(mismatch(format!(
            "missing or incorrect MySQL backend marker: {backend:?}"
        )));
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if track.as_deref() == Some("3") {
        mysql_v3_domain_shape_check(pool).await?;
        return Ok(());
    }

    if track.as_deref() != Some("2") {
        return Err(RepositoryError::BackendMismatch {
            expected: "2.0 active schema",
            actual: "schema is not activated for the 2.0 API".to_owned(),
        });
    }

    let has_legacy = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_audit_events'").fetch_one(pool).await? != 0;
    mysql_catalog_shape_check(pool, has_legacy).await
}

#[cfg(feature = "mysql")]
#[cfg(feature = "migrations")]
pub(in crate::repository) async fn mysql_clean_schema_preflight(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let has_domain = sqlx::query_scalar::<_, Option<String>>("select table_name from information_schema.tables where table_schema = database() and table_name = 'keepsake_relation_definitions'").fetch_optional(pool).await?.flatten().is_some();
    if !has_domain {
        return mysql_schema_preflight(pool).await;
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("3") => mysql_v3_domain_shape_check(pool).await,
        Some("2") => {
            let legacy = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_audit_events'").fetch_one(pool).await? != 0;
            if legacy {
                return Err(RepositoryError::BackendMismatch {
                    expected: "2.0 clean track",
                    actual: "activated upgrade track".to_owned(),
                });
            }
            Err(RepositoryError::BackendMismatch {
                expected: "3.0 clean track",
                actual: "2.0 clean track; run the explicit tenant upgrade route".to_owned(),
            })
        }
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: "3.0 clean track",
            actual: actual.to_owned(),
        }),
        None => Err(RepositoryError::BackendMismatch {
            expected: "3.0 clean track",
            actual: "legacy schema; call upgrade_migrate".to_owned(),
        }),
    }
}

#[cfg(feature = "mysql")]
#[cfg(feature = "migrations")]
pub(in crate::repository) async fn mysql_upgrade_schema_preflight(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let has_metadata = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'").fetch_one(pool).await? > 0;
    if !has_metadata {
        return mysql_schema_preflight(pool).await;
    }

    let has_v2 = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten()
    .is_some_and(|value| value == "2");
    if has_v2 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "2.0 clean track".to_owned(),
        });
    }

    let has_v3 = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten()
    .is_some_and(|value| value == "3");
    if has_v3 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "3.0 clean track".to_owned(),
        });
    }
    mysql_schema_preflight(pool).await
}

#[cfg(feature = "mysql")]
#[cfg(feature = "migrations")]
pub(in crate::repository) async fn mysql_schema_preflight(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>("select table_name from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'").fetch_optional(pool).await?.flatten();
    if metadata.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            "select value from keepsake_schema_metadata where `key` = 'backend'",
        )
        .fetch_optional(pool)
        .await?
        .flatten();
        return match backend.as_deref() {
            Some(super::super::MySqlBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: super::super::MySqlBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = database()",
    )
    .fetch_one(pool)
    .await?;
    if existing == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: super::super::MySqlBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}
