//! `MySQL` and `MariaDB` catalog constraints and tenant isolation checks.

use super::constraints_catalog::MYSQL_V3_CONSTRAINTS_CHECK_EXPECTED_CHECKS;
use super::constraints_catalog::MYSQL_V3_CONSTRAINTS_CHECK_IDENTIFIER_CHECKS;
use super::constraints_catalog::MYSQL_V3_CONSTRAINTS_CHECK_TABLES;
use super::constraints_catalog::MYSQL_V3_CONSTRAINTS_CHECK_TENANT_CHECKS;
use super::constraints_catalog::MYSQL_V3_FOREIGN_KEYS_CHECK_EXPECTED;
use super::constraints_catalog::TenantForeignKey;

use crate::repository::schema::MYSQL_V3_CLEAN_ARTIFACT;
use crate::repository::schema::MYSQL_V4_IDENTIFIER_ARTIFACT;
use crate::repository::schema::RepositoryResult;
use crate::repository::schema::artifact_check_expression;
use crate::repository::schema::compact_sql;
use crate::repository::schema::identifier_check_from_artifact;
use crate::repository::schema::identifier_check_matches;
use crate::repository::schema::mismatch;
use crate::repository::schema::mysql_catalog_check_matches;
use crate::repository::schema::normalize_check_expression;
use sqlx::mysql::MySqlRow;
use std::collections::BTreeSet;

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

pub(in crate::repository::schema) fn mysql_v3_referential_action_matches(
    expected: &str,
    actual: &str,
) -> bool {
    actual == expected || (expected == "NO ACTION" && actual == "RESTRICT")
}

pub(super) async fn mysql_v3_foreign_keys_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    use sqlx::Row;

    let expected = MYSQL_V3_FOREIGN_KEYS_CHECK_EXPECTED;

    let names = sqlx::query_as::<_, (String, String)>(
        "select distinct table_name, constraint_name from information_schema.key_column_usage where constraint_schema = database() and referenced_table_name is not null and table_name in (?,?,?,?)",
    )
    .bind("keepsake_relation_definitions")
    .bind("keepsakes")
    .bind("keepsake_fulfillment_counters")
    .bind("keepsake_fulfillment_checklist")
    .fetch_all(pool)
    .await?;
    if names.len() != expected.len()
        || names.iter().any(|(table, name)| {
            !expected
                .iter()
                .any(|expected| expected.table == table && expected.name == name)
        })
    {
        return Err(mismatch(format!(
            "v3 domain foreign-key set differs: {names:?}"
        )));
    }

    for TenantForeignKey {
        table,
        name,
        columns,
        delete_rule,
    } in expected
    {
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

        for ((row, expected_column), position) in rows.iter().zip(columns.iter()).zip(1_i64..) {
            let ordinal: i64 = row.try_get("ordinal_position")?;
            let local: String = row.try_get("column_name")?;
            let referenced_table: Option<String> = row.try_get("referenced_table_name")?;
            let referenced_column: Option<String> = row.try_get("referenced_column_name")?;
            let actual_rule: String = row.try_get("delete_rule")?;
            let actual_update_rule: String = row.try_get("update_rule")?;
            let (expected_local, expected_referenced) = *expected_column;
            if ordinal != position
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

pub(super) async fn mysql_v3_constraints_check(
    pool: &sqlx::MySqlPool,
    json_longtext_columns: &[(&str, &str)],
    maria_db: bool,
    identifier_contract: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let checks = mysql_tenant_check_rows(pool, maria_db).await?;
    let expected_checks = MYSQL_V3_CONSTRAINTS_CHECK_EXPECTED_CHECKS;
    let tenant_checks = MYSQL_V3_CONSTRAINTS_CHECK_TENANT_CHECKS;
    let identifier_checks = MYSQL_V3_CONSTRAINTS_CHECK_IDENTIFIER_CHECKS;
    let mut found_checks = BTreeSet::new();
    let mut found_tenant_checks = BTreeSet::new();
    let mut found_identifier_checks = BTreeSet::new();
    let mut found_json_checks = BTreeSet::new();
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

        if identifier_checks.contains(&name.as_str()) {
            mysql_identifier_predicate_check(&row, &table, &name, &clause, identifier_contract)?;
            found_identifier_checks.insert(name.clone());
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
        || found_identifier_checks.len()
            != if identifier_contract {
                identifier_checks.len()
            } else {
                0
            }
        || json_longtext_columns
            .iter()
            .any(|(table, column)| !found_json_checks.contains(&format!("{table}.{column}")))
    {
        return Err(mismatch("Keepsake v3 CHECK constraint definitions differ"));
    }
    Ok(())
}

async fn mysql_tenant_check_rows(
    pool: &sqlx::MySqlPool,
    maria_db: bool,
) -> RepositoryResult<Vec<MySqlRow>> {
    let tables = MYSQL_V3_CONSTRAINTS_CHECK_TABLES;
    let check_query = if maria_db {
        "select tc.table_name as table_name, tc.constraint_name as constraint_name, cc.check_clause as check_clause, 'YES' as enforced from information_schema.table_constraints tc join information_schema.check_constraints cc on cc.constraint_schema = tc.constraint_schema and cc.table_name = tc.table_name and cc.constraint_name = tc.constraint_name where tc.constraint_schema = database() and tc.table_name in (?,?,?,?,?,?,?,?) and tc.constraint_type = 'CHECK'"
    } else {
        "select tc.table_name as table_name, tc.constraint_name as constraint_name, cc.check_clause as check_clause, tc.enforced as enforced from information_schema.table_constraints tc join information_schema.check_constraints cc on cc.constraint_schema = tc.constraint_schema and cc.constraint_name = tc.constraint_name where tc.constraint_schema = database() and tc.table_name in (?,?,?,?,?,?,?,?) and tc.constraint_type = 'CHECK'"
    };
    sqlx::query(check_query)
        .bind(tables.first().unwrap_or(&""))
        .bind(tables.get(1).unwrap_or(&""))
        .bind(tables.get(2).unwrap_or(&""))
        .bind(tables.get(3).unwrap_or(&""))
        .bind(tables.get(4).unwrap_or(&""))
        .bind(tables.get(5).unwrap_or(&""))
        .bind(tables.get(6).unwrap_or(&""))
        .bind(tables.get(7).unwrap_or(&""))
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

fn mysql_identifier_predicate_check(
    row: &MySqlRow,
    table: &str,
    name: &str,
    clause: &str,
    identifier_contract: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let expected = identifier_check_from_artifact(MYSQL_V4_IDENTIFIER_ARTIFACT, table, name);
    // MySQL deparses OCTET_LENGTH as LENGTH; both count bytes in MySQL
    // and MariaDB. PostgreSQL must retain OCTET_LENGTH instead.
    let matches = expected.is_some_and(|expected| {
        identifier_check_matches(
            &clause.replace("octet_length", "length"),
            &expected.replace("octet_length", "length"),
        )
    });
    if !identifier_contract || !matches || row.try_get::<String, _>("enforced")? != "YES" {
        return Err(mismatch(format!(
            "identifier CHECK constraint {table}.{name} definition differs"
        )));
    }
    Ok(())
}
