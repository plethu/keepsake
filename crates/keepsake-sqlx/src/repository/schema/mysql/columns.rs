//! `MySQL` and `MariaDB` column type, default, and generation checks.

use super::catalog::MySqlColumn;
use crate::repository::schema::RepositoryResult;
use crate::repository::schema::default_sql;
use crate::repository::schema::mismatch;
use crate::repository::schema::normalize_mysql_generated_expression;

pub(in crate::repository::schema) fn mysql_default_matches(
    actual: Option<&str>,
    expected: Option<&str>,
) -> bool {
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

pub(in crate::repository::schema) fn mysql_is_generated_extra(extra: &str) -> bool {
    let extra = extra.to_ascii_lowercase();
    extra.contains("stored generated")
        || extra.contains("virtual generated")
        || extra.contains("generated always")
}

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

pub(super) async fn mysql_columns_check<'a>(
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
