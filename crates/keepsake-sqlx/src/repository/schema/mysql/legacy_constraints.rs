//! Constraints retained by the pre-tenant clean and activated upgrade tracks.

#[cfg(feature = "migrations")]
use super::constraints_catalog::MYSQL_CONSTRAINTS_CHECK_EXPECTED_ACTIONS;
#[cfg(feature = "migrations")]
use super::constraints_catalog::MYSQL_CONSTRAINTS_CHECK_EXPECTED_CLEAN;
#[cfg(feature = "migrations")]
use super::constraints_catalog::MYSQL_CONSTRAINTS_CHECK_EXPECTED_UPGRADE;
#[cfg(feature = "migrations")]
use super::constraints_catalog::MYSQL_CONSTRAINTS_CHECK_FOREIGN_KEYS;
#[cfg(feature = "migrations")]
use super::constraints_catalog::MYSQL_CONSTRAINTS_CHECK_TABLES_CLEAN;
#[cfg(feature = "migrations")]
use super::constraints_catalog::MYSQL_CONSTRAINTS_CHECK_TABLES_UPGRADE;
#[cfg(feature = "migrations")]
use crate::repository::schema::MYSQL_CLEAN_ARTIFACT;
#[cfg(feature = "migrations")]
use crate::repository::schema::MYSQL_UPGRADE_ARTIFACT;

use crate::repository::schema::RepositoryResult;

use crate::repository::schema::artifact_check_expression;

use crate::repository::schema::compact_sql;

use crate::repository::schema::mismatch;

use crate::repository::schema::mysql_catalog_check_matches;

use crate::repository::schema::normalize_check_expression;

use std::collections::BTreeSet;

#[cfg(feature = "migrations")]
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

#[cfg(feature = "migrations")]
pub(super) async fn mysql_constraints_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
    json_longtext_columns: &[(&str, &str)],
    maria_db: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let tables: &[&str] = if activated_upgrade {
        MYSQL_CONSTRAINTS_CHECK_TABLES_UPGRADE
    } else {
        MYSQL_CONSTRAINTS_CHECK_TABLES_CLEAN
    };
    // CHECK constraints are queried separately below: MariaDB adds a
    // json_valid CHECK for every JSON-as-LONGTEXT column, while Oracle
    // MySQL represents those columns as native JSON without the extra row.
    let rows = sqlx::query("select table_name as table_name, constraint_name as constraint_name, constraint_type as constraint_type from information_schema.table_constraints where constraint_schema = database() and table_name in (?,?,?,?,?,?,?,?) and constraint_type in ('PRIMARY KEY','UNIQUE','FOREIGN KEY')").bind(tables.first().unwrap_or(&"")).bind(tables.get(1).unwrap_or(&"")).bind(tables.get(2).unwrap_or(&"")).bind(tables.get(3).unwrap_or(&"")).bind(tables.get(4).unwrap_or(&"")).bind(tables.get(5).unwrap_or(&"")).bind(tables.get(6).unwrap_or(&"")).bind(tables.get(7).unwrap_or(&"")).fetch_all(pool).await?;
    let expected: &[(&str, &str, &str)] = if activated_upgrade {
        MYSQL_CONSTRAINTS_CHECK_EXPECTED_UPGRADE
    } else {
        MYSQL_CONSTRAINTS_CHECK_EXPECTED_CLEAN
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

    mysql_legacy_foreign_key_targets_check(pool, activated_upgrade).await?;

    mysql_legacy_referential_actions_check(pool, activated_upgrade).await?;

    mysql_legacy_check_predicates(pool, activated_upgrade, json_longtext_columns, maria_db).await?;

    Ok(())
}

#[cfg(feature = "migrations")]
async fn mysql_legacy_foreign_key_targets_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let foreign_keys = MYSQL_CONSTRAINTS_CHECK_FOREIGN_KEYS;
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

    Ok(())
}

#[cfg(feature = "migrations")]
async fn mysql_legacy_referential_actions_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    let expected_actions = MYSQL_CONSTRAINTS_CHECK_EXPECTED_ACTIONS;
    for (table, constraint, expected_action) in expected_actions {
        let action = sqlx::query_scalar::<_, String>(
            "select delete_rule as delete_rule from information_schema.referential_constraints where constraint_schema = database() and table_name = ? and constraint_name = ?",
        )
        .bind(table)
        .bind(constraint)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing referential action {table}.{constraint}")))?;
        if action != *expected_action {
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

    Ok(())
}

#[cfg(feature = "migrations")]
async fn mysql_legacy_check_predicates(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
    json_longtext_columns: &[(&str, &str)],
    maria_db: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let tables = if activated_upgrade {
        MYSQL_CONSTRAINTS_CHECK_TABLES_UPGRADE
    } else {
        MYSQL_CONSTRAINTS_CHECK_TABLES_CLEAN
    };
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
    let expected_checks = legacy_lifecycle_checks(activated_upgrade, maria_db);
    let mut found_checks = BTreeSet::new();
    let mut json_checks = BTreeSet::new();
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

const fn legacy_lifecycle_checks(
    activated_upgrade: bool,
    maria_db: bool,
) -> [(&'static str, &'static str, &'static str); 3] {
    let historical_state_name = if maria_db { "state" } else { "keepsakes_chk_1" };

    [
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
    ]
}
