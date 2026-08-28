//! Database schema preflight and runtime shape verification.
//!
//! Migration runners need a deliberately small preflight: an empty database is
//! a valid target for the runner. `check_schema`, in contrast, is a runtime
//! gate and verifies the complete domain catalog before it verifies Dovecote.

#![cfg_attr(
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite")),
    allow(dead_code)
)]

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use super::backend::KeepsakeSqlxBackend;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use super::{RepositoryError, RepositoryResult};

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
fn mismatch(detail: impl Into<String>) -> RepositoryError {
    RepositoryError::BackendMismatch {
        expected: "complete Keepsake 2.0 domain schema",
        actual: detail.into(),
    }
}

fn normalize_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    for line in sql.lines() {
        let line = line.split_once("--").map_or(line, |(line, _)| line);
        if !line.trim().is_empty() {
            if !normalized.is_empty() {
                normalized.push(' ');
            }
            normalized.push_str(line.trim());
        }
    }
    normalized
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn default_sql(sql: &str) -> String {
    normalize_sql(sql)
        .replace("_utf8mb4", "")
        .replace("_utf8mb3", "")
        .replace("\\'", "'")
        .trim_matches('(')
        .trim_matches(')')
        .trim_matches('\'')
        .to_owned()
}

fn compact_sql(sql: &str) -> String {
    normalize_sql(sql)
        .replace([' ', '\n', '`'], "")
        .replace("_utf8mb4", "")
        .replace("_utf8mb3", "")
        .replace("_latin1", "")
        .replace("\\'", "'")
}

#[cfg(feature = "mysql")]
#[allow(clippy::excessive_nesting)]
fn normalize_mysql_generated_expression(expression: &str) -> String {
    let mut normalized = compact_sql(expression).replace("\\'", "'");
    // MySQL's generated-column deparser wraps the whole CASE and its WHEN
    // predicate, and makes the implicit ELSE NULL explicit. Those are
    // equivalent representations of the migration's expression, while the
    // function calls and predicates inside them remain byte-for-byte strict.
    while normalized.starts_with('(') && normalized.ends_with(')') {
        let mut depth = 0usize;
        let mut wraps_entire_expression = true;
        for (offset, character) in normalized.char_indices() {
            match character {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && offset != normalized.len() - 1 {
                        wraps_entire_expression = false;
                        break;
                    }
                }
                _ => {}
            }
        }

        if !wraps_entire_expression {
            break;
        }
        normalized = normalized[1..normalized.len() - 1].to_owned();
    }

    if let Some(predicate) = normalized.strip_prefix("casewhen(") {
        normalized = format!("casewhen{}", predicate.replacen(")then", "then", 1));
    }

    while normalized.starts_with("casewhen(") {
        normalized = normalized.replacen("casewhen(", "casewhen", 1);
    }
    // The catalog may also parenthesize one atomic predicate inside the CASE
    // condition. These deparser boundaries surround atomic predicates; no
    // parentheses containing an AND/OR expression are removed.
    normalized
        .replace(")and(", "and")
        .replace(")then", "then")
        .replace("elsenullend", "end")
}

#[cfg(all(test, feature = "mysql"))]
mod mysql_normalization_tests {
    use super::{
        artifact_check_expression, normalize_check_expression, normalize_mysql_generated_expression,
    };

    #[test]
    fn check_normalization_preserves_outer_close_after_in_list() {
        let source = "check(coalesce(x in ('a', 'b'), false))";
        assert_eq!(
            normalize_check_expression(source),
            "coalesce(x=any(array['a','b']),false)"
        );
    }

    #[test]
    fn all_mysql_check_artifacts_are_extractable() {
        assert!(
            artifact_check_expression(
                super::MYSQL_CLEAN_ARTIFACT,
                "constraint keepsakes_state_check check"
            )
            .is_some()
        );
        for marker in [
            "constraint keepsakes_state_check check",
            "constraint keepsakes_expiry_policy_projection check",
            "constraint keepsakes_lifecycle_timestamps check",
        ] {
            assert!(artifact_check_expression(super::MYSQL_V3_CLEAN_ARTIFACT, marker).is_some());
        }
        assert!(
            artifact_check_expression(
                super::MYSQL_UPGRADE_ARTIFACT,
                "state varchar(16) not null check"
            )
            .is_some()
        );
        for marker in [
            "constraint keepsakes_expiry_policy_projection check",
            "constraint keepsakes_lifecycle_timestamps check",
        ] {
            assert!(artifact_check_expression(super::MYSQL_CLEAN_ARTIFACT, marker).is_some());
            assert!(artifact_check_expression(super::MYSQL_UPGRADE_ARTIFACT, marker).is_some());
        }
    }

    #[test]
    fn v3_mysql_identifier_shape_is_mariadb_compatible() {
        let clean = super::normalize_sql(super::MYSQL_V3_CLEAN_ARTIFACT);
        assert!(!clean.contains(" id char(36)"));
        assert!(!clean.contains(" relation_id char(36)"));
        assert!(!clean.contains(" keepsake_id char(36)"));
        assert_eq!(clean.matches("varchar(36)").count(), 6);

        let activation = super::normalize_sql(super::MYSQL_V3_UPGRADE_ACTIVATE_ARTIFACT);
        for fragment in [
            "modify id varchar(36) not null",
            "modify id varchar(36) not null, modify relation_id varchar(36) not null",
            "modify active_relation_key varchar(36) generated always",
            "modify keepsake_id varchar(36) not null",
        ] {
            assert!(
                activation.contains(fragment),
                "missing activation fragment: {fragment}"
            );
        }
    }

    #[test]
    fn generated_case_deparser_forms_compare_equal() {
        assert_eq!(
            normalize_mysql_generated_expression(
                "(case when (`state` = _utf8mb4'applied') then `relation_id` else NULL end)"
            ),
            normalize_mysql_generated_expression(
                "case when state = 'applied' then relation_id end"
            )
        );
        assert_eq!(
            normalize_mysql_generated_expression(
                r"(case when (`state` = _utf8mb4\'applied\') then `relation_id` else NULL end)"
            ),
            normalize_mysql_generated_expression(
                "case when state = 'applied' then relation_id end"
            )
        );
    }

    #[test]
    fn fulfillment_generated_case_keeps_predicate_semantics() {
        assert_eq!(
            normalize_mysql_generated_expression(
                "(case when (`state` = _utf8mb4'applied' and json_unquote(json_extract(`expiry_policy`, '$.type')) = _utf8mb4'when_fulfilled') then 1 else NULL end)"
            ),
            normalize_mysql_generated_expression(
                "case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end"
            )
        );
        assert_eq!(
            normalize_mysql_generated_expression(
                "case when (state = 'applied') and (json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled') then 1 else NULL end"
            ),
            normalize_mysql_generated_expression(
                "case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end"
            )
        );
    }

    #[test]
    fn generated_null_default_is_absent_but_quoted_null_is_not() {
        assert!(super::mysql_default_matches(Some("NULL"), None));
        assert!(!super::mysql_default_matches(Some("'NULL'"), None));
        assert!(!super::mysql_is_generated_extra("DEFAULT_GENERATED"));
        assert!(super::mysql_is_generated_extra("STORED GENERATED"));
    }

    #[test]
    fn v3_referential_actions_reject_update_cascade() {
        assert!(super::mysql_v3_referential_action_matches(
            "NO ACTION",
            "NO ACTION"
        ));
        assert!(super::mysql_v3_referential_action_matches(
            "NO ACTION",
            "RESTRICT"
        ));
        assert!(!super::mysql_v3_referential_action_matches(
            "NO ACTION",
            "CASCADE"
        ));
        assert!(!super::mysql_v3_referential_action_matches(
            "CASCADE",
            "NO ACTION"
        ));
    }
}

#[cfg(all(test, feature = "postgres"))]
mod postgres_artifact_tests {
    use super::artifact_check_expression;

    #[test]
    fn all_postgres_check_artifacts_are_extractable() {
        assert!(
            artifact_check_expression(
                super::PG_CLEAN_ARTIFACT,
                "constraint keepsakes_state_check check"
            )
            .is_some()
        );
        assert!(
            artifact_check_expression(super::PG_UPGRADE_ARTIFACT, "state text not null check")
                .is_some()
        );
        for marker in [
            "constraint keepsakes_expiry_policy_projection check",
            "constraint keepsakes_lifecycle_timestamps check",
        ] {
            assert!(artifact_check_expression(super::PG_CLEAN_ARTIFACT, marker).is_some());
            assert!(artifact_check_expression(super::PG_UPGRADE_ARTIFACT, marker).is_some());
        }
    }

    #[test]
    fn v3_postgres_tenant_contract_requires_nonempty_c_collated_columns() {
        let clean = super::normalize_sql(super::PG_V3_CLEAN_ARTIFACT);
        assert_eq!(
            clean
                .matches("tenant_id text collate \"c\" not null")
                .count(),
            4
        );
        let activation = super::normalize_sql(super::PG_V3_UPGRADE_ACTIVATE_ARTIFACT);
        assert_eq!(
            activation
                .matches("alter column tenant_id type text collate \"c\"")
                .count(),
            4
        );
        let prepare = super::normalize_sql(super::PG_V3_UPGRADE_PREPARE_ARTIFACT);
        assert_eq!(
            prepare
                .matches("add column tenant_id text collate \"c\"")
                .count(),
            4
        );
        for marker in [
            "keepsake_relation_definitions_tenant_nonempty",
            "keepsakes_tenant_nonempty",
            "keepsake_fulfillment_counter_tenant_nonempty",
            "keepsake_fulfillment_checklist_tenant_nonempty",
        ] {
            let marker = format!("constraint {marker} check");
            assert!(artifact_check_expression(super::PG_V3_CLEAN_ARTIFACT, &marker).is_some());
            assert!(
                artifact_check_expression(super::PG_V3_UPGRADE_ACTIVATE_ARTIFACT, &marker)
                    .is_some()
            );
        }
    }
}

#[cfg(feature = "postgres")]
const PG_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/postgres/2000_clean_baseline.sql");

#[cfg(all(test, feature = "postgres"))]
const PG_V3_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v3/postgres/3000_clean_baseline.sql");

#[cfg(all(test, feature = "postgres"))]
const PG_V3_UPGRADE_ACTIVATE_ARTIFACT: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/postgres/activate.sql");

#[cfg(all(test, feature = "postgres"))]
const PG_V3_UPGRADE_PREPARE_ARTIFACT: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/postgres/prepare.sql");

#[cfg(feature = "postgres")]
const PG_UPGRADE_ARTIFACT: &str = concat!(
    include_str!("../../migrations/postgres/0001_init.sql"),
    include_str!("../../migrations/postgres/0002_lifecycle_invariants.sql"),
    include_str!("../../migrations/postgres/0003_schema_metadata.sql"),
    include_str!("../../migrations/postgres/0004_fulfillment_expiry_index.sql"),
    include_str!("../../migrations/postgres/0005_fulfillment_checklist.sql"),
    include_str!("../../migrations/postgres/0006_audit_outbox.sql"),
    include_str!("../../migrations/postgres/0007_dovecote_bridge.sql"),
);

#[cfg(feature = "mysql")]
const MYSQL_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/mysql/2000_clean_baseline.sql");

#[cfg(feature = "mysql")]
const MYSQL_V3_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v3/mysql/3000_clean_baseline.sql");

#[cfg(all(test, feature = "mysql"))]
const MYSQL_V3_UPGRADE_ACTIVATE_ARTIFACT: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/mysql/activate.sql");

#[cfg(feature = "mysql")]
const MYSQL_UPGRADE_ARTIFACT: &str = concat!(
    include_str!("../../migrations/mysql/0001_init.sql"),
    include_str!("../../migrations/mysql/0002_lifecycle_invariants.sql"),
    include_str!("../../migrations/mysql/0003_fulfillment_expiry_index.sql"),
    include_str!("../../migrations/mysql/0004_fulfillment_checklist.sql"),
    include_str!("../../migrations/mysql/0005_audit_outbox.sql"),
    include_str!("../../migrations/mysql/0006_dovecote_bridge.sql"),
);

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn artifact_check_expression(artifact: &str, marker: &str) -> Option<String> {
    let artifact = normalize_sql(artifact);
    let lower = artifact.to_ascii_lowercase();
    let marker_start = lower.find(&normalize_sql(marker))?;
    let check_start = lower[marker_start..].find("check")? + marker_start;
    let open = lower[check_start..].find('(')? + check_start;
    let mut depth = 0usize;
    let mut quoted = false;
    for (offset, character) in artifact[open..].char_indices() {
        if character == '\'' {
            quoted = !quoted;
            continue;
        }

        if quoted {
            continue;
        }

        match character {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(artifact[open..=open + offset].to_owned());
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
#[allow(clippy::excessive_nesting)]
fn normalize_check_expression(expression: &str) -> String {
    let mut normalized = compact_sql(expression);
    if normalized.starts_with("check(") {
        normalized = normalized["check".len()..].to_owned();
    }

    while normalized.starts_with('(') && normalized.ends_with(')') {
        let mut depth = 0usize;
        let mut wraps_entire_expression = true;
        for (offset, character) in normalized.char_indices() {
            match character {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && offset != normalized.len() - 1 {
                        wraps_entire_expression = false;
                        break;
                    }
                }
                _ => {}
            }
        }

        if !wraps_entire_expression {
            break;
        }
        normalized = normalized[1..normalized.len() - 1].to_owned();
    }
    // PostgreSQL's deparser spells an IN list as = ANY (ARRAY[...]) and
    // annotates string literals with their inferred text type. The migration
    // source is canonicalized to the same representation below.
    for cast in ["::text", "::timestamptz", "::timestampwithtimezone"] {
        normalized = normalized.replace(cast, "");
    }
    normalize_in_list(&normalized)
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
#[allow(clippy::excessive_nesting)]
fn normalize_in_list(expression: &str) -> String {
    let mut normalized = expression.to_owned();
    while let Some(in_offset) = normalized.find("in(") {
        let mut left_start = in_offset;
        if left_start > 0 && normalized.as_bytes()[left_start - 1] as char == ')' {
            let mut depth = 0usize;
            while left_start > 0 {
                left_start -= 1;
                match normalized.as_bytes()[left_start] as char {
                    ')' => depth += 1,
                    '(' if depth == 1 => break,
                    '(' => depth -= 1,
                    _ => {}
                }
            }
        } else {
            while left_start > 0 {
                let character = normalized.as_bytes()[left_start - 1] as char;
                if character.is_ascii_alphanumeric() || matches!(character, '_' | '\'' | '-' | '>')
                {
                    left_start -= 1;
                } else {
                    break;
                }
            }
        }

        if left_start == in_offset {
            break;
        }

        let mut depth = 0usize;
        let close = normalized[in_offset + 3..]
            .char_indices()
            .find_map(|(offset, character)| match character {
                '(' => {
                    depth += 1;
                    None
                }
                ')' if depth == 0 => Some(in_offset + 3 + offset),
                ')' => {
                    depth -= 1;
                    None
                }
                _ => None,
            });
        let Some(close) = close else { break };

        let left = &normalized[left_start..in_offset];
        let values = &normalized[in_offset + 3..close];
        let replacement = format!("{left}=any(array[{values}])");
        normalized.replace_range(left_start..=close, &replacement);
    }

    normalized
}

#[cfg(feature = "sqlite")]
fn artifact_object_sql<'a>(artifact: &'a str, kind: &str, name: &str) -> Option<&'a str> {
    let lower = artifact.to_ascii_lowercase();
    let marker = format!("create {kind} {name}");
    let start = lower.find(&marker)?;
    let remainder = &lower[start..];
    let end = if kind == "trigger" {
        remainder
            .find("\nend;")
            .map(|offset| offset + "\nend;".len())?
    } else {
        remainder.find(';').map(|offset| offset + 1)?
    };
    Some(&artifact[start..start + end])
}

#[cfg(feature = "sqlite")]
const SQLITE_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/sqlite/2000_clean_baseline.sql");

#[cfg(feature = "sqlite")]
const SQLITE_UPGRADE_ARTIFACT: &str = concat!(
    include_str!("../../migrations/sqlite/0001_init.sql"),
    include_str!("../../migrations/sqlite/0002_lifecycle_invariants.sql"),
    include_str!("../../migrations/sqlite/0003_fulfillment_expiry_index.sql"),
    include_str!("../../migrations/sqlite/0004_fulfillment_checklist.sql"),
    include_str!("../../migrations/sqlite/0005_audit_outbox.sql"),
    include_str!("../../migrations/sqlite/0006_dovecote_bridge.sql"),
);

#[cfg(feature = "sqlite")]
const CLEAN_TABLES: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

#[cfg(feature = "sqlite")]
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

#[cfg(feature = "sqlite")]
const CLEAN_INDEXES: &[(&str, &str)] = &[
    ("index", "keepsakes_active_subject_lookup"),
    ("index", "keepsakes_active_relation_membership"),
    ("index", "keepsakes_due_timed_expiry"),
    ("index", "keepsake_fulfillment_counter_scan"),
    ("index", "keepsakes_due_fulfilled_expiry"),
    ("index", "keepsake_fulfillment_checklist_scan"),
    ("unique index", "keepsakes_one_active_relation_per_subject"),
];

#[cfg(feature = "sqlite")]
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

#[cfg(feature = "sqlite")]
const CLEAN_TRIGGERS: &[(&str, &str)] = &[
    ("trigger", "keepsakes_clean_invariants_insert"),
    ("trigger", "keepsakes_clean_invariants_update"),
];

#[cfg(feature = "sqlite")]
const UPGRADE_TRIGGERS: &[(&str, &str)] = &[
    ("trigger", "keepsakes_expiry_policy_projection_insert"),
    ("trigger", "keepsakes_expiry_policy_projection_update"),
    ("trigger", "keepsakes_lifecycle_timestamps_insert"),
    ("trigger", "keepsakes_lifecycle_timestamps_update"),
];

#[cfg(feature = "sqlite")]
#[allow(clippy::too_many_lines)]
async fn sqlite_domain_shape_check(
    pool: &sqlx::SqlitePool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let artifact = if activated_upgrade {
        SQLITE_UPGRADE_ARTIFACT
    } else {
        SQLITE_CLEAN_ARTIFACT
    };
    let expected_tables = if activated_upgrade {
        UPGRADE_TABLES
    } else {
        CLEAN_TABLES
    };
    let expected_indexes = if activated_upgrade {
        UPGRADE_INDEXES
    } else {
        CLEAN_INDEXES
    };
    let expected_triggers = if activated_upgrade {
        UPGRADE_TRIGGERS
    } else {
        CLEAN_TRIGGERS
    };

    for table in expected_tables {
        let row = sqlx::query("select sql from sqlite_master where type = 'table' and name = ?")
            .bind(table)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| mismatch(format!("missing table {table}")))?;
        let actual: String = row.try_get("sql")?;
        let expected = artifact_object_sql(artifact, "table", table)
            .ok_or_else(|| mismatch(format!("migration artifact lacks table {table}")))?;
        if normalize_sql(&actual) != normalize_sql(expected) {
            return Err(mismatch(format!(
                "table {table} definition differs from migration"
            )));
        }
    }

    for (kind, index) in expected_indexes {
        let row = sqlx::query("select sql from sqlite_master where type = 'index' and name = ?")
            .bind(index)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| mismatch(format!("missing index {index}")))?;
        let actual: String = row.try_get("sql")?;
        let expected = artifact_object_sql(artifact, kind, index)
            .ok_or_else(|| mismatch(format!("migration artifact lacks index {index}")))?;
        if normalize_sql(&actual) != normalize_sql(expected) {
            return Err(mismatch(format!(
                "index {index} definition differs from migration"
            )));
        }
    }

    for (kind, trigger) in expected_triggers {
        let row = sqlx::query("select sql from sqlite_master where type = 'trigger' and name = ?")
            .bind(trigger)
            .fetch_optional(pool)
            .await?
            .ok_or_else(|| mismatch(format!("missing trigger {trigger}")))?;
        let actual: String = row.try_get("sql")?;
        let expected = artifact_object_sql(artifact, kind, trigger)
            .ok_or_else(|| mismatch(format!("migration artifact lacks trigger {trigger}")))?;
        if normalize_sql(&actual) != normalize_sql(expected) {
            return Err(mismatch(format!(
                "trigger {trigger} definition differs from migration"
            )));
        }
    }

    // An application may own unrelated SQLite objects, but an additional
    // trigger attached to an invariant table can silently change lifecycle
    // semantics. Reject those while leaving unrelated application objects
    // alone.
    let rows = sqlx::query(
        "select name, sql from sqlite_master where type = 'trigger' and name not like 'sqlite_%'",
    )
    .fetch_all(pool)
    .await?;
    let expected_names: std::collections::BTreeSet<&str> =
        expected_triggers.iter().map(|(_, name)| *name).collect();
    for row in rows {
        let name: String = row.try_get("name")?;
        let sql: String = row.try_get("sql")?;
        let compact = compact_sql(&sql);
        if !expected_names.contains(name.as_str())
            && [
                "onkeepsakes",
                "onkeepsakerelationdefinitions",
                "onkeepsakefulfillmentcounters",
                "onkeepsakefulfillmentchecklist",
            ]
            .iter()
            .any(|table| compact.contains(table))
        {
            return Err(mismatch(format!(
                "unexpected trigger {name} mutates a Keepsake invariant table"
            )));
        }
    }

    // The clean track must not retain the active legacy SQL audit model. The
    // activated upgrade track deliberately expects these tables and validates
    // their definitions above.
    if !activated_upgrade {
        for table in [
            "keepsake_audit_events",
            "keepsake_audit_context_attributes",
            "keepsake_audit_outbox",
        ] {
            let exists = sqlx::query_scalar::<_, i64>(
                "select count(*) from sqlite_master where type = 'table' and name = ?",
            )
            .bind(table)
            .fetch_one(pool)
            .await?;
            if exists != 0 {
                return Err(mismatch(format!(
                    "clean track contains legacy table {table}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
pub(super) async fn sqlite_upgrade_schema_check(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    sqlite_domain_shape_check(pool, true).await
}

#[cfg(feature = "sqlite")]
#[allow(clippy::too_many_lines)]
async fn sqlite_v3_domain_shape_check(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    use sqlx::Row;

    for table in [
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ] {
        let columns = match table {
            "keepsake_relation_definitions" => {
                sqlx::query("pragma table_info(keepsake_relation_definitions)")
                    .fetch_all(pool)
                    .await?
            }
            "keepsakes" => {
                sqlx::query("pragma table_info(keepsakes)")
                    .fetch_all(pool)
                    .await?
            }
            "keepsake_fulfillment_counters" => {
                sqlx::query("pragma table_info(keepsake_fulfillment_counters)")
                    .fetch_all(pool)
                    .await?
            }
            "keepsake_fulfillment_checklist" => {
                sqlx::query("pragma table_info(keepsake_fulfillment_checklist)")
                    .fetch_all(pool)
                    .await?
            }
            _ => unreachable!("table list is static"),
        };
        let tenant = columns
            .iter()
            .find(|row| row.try_get::<String, _>("name").ok().as_deref() == Some("tenant_id"))
            .ok_or_else(|| mismatch(format!("{table} lacks tenant_id")))?;
        if tenant.try_get::<i64, _>("notnull")? != 1 {
            return Err(mismatch(format!("{table}.tenant_id is nullable")));
        }
        let definition = sqlx::query_scalar::<_, Option<String>>(
            "select sql from sqlite_master where type = 'table' and name = ?",
        )
        .bind(table)
        .fetch_optional(pool)
        .await?
        .flatten()
        .ok_or_else(|| mismatch(format!("missing v3 table {table}")))?;
        if !compact_sql(&definition).contains("length(cast(tenant_idasblob))>0") {
            return Err(mismatch(format!(
                "{table}.tenant_id is not non-empty constrained"
            )));
        }
    }

    for index in [
        "keepsake_relation_definitions_tenant_key",
        "keepsakes_one_active_relation_per_subject",
        "keepsakes_active_subject_lookup",
        "keepsakes_active_relation_membership",
        "keepsakes_due_timed_expiry",
        "keepsakes_due_fulfilled_expiry",
        "keepsake_fulfillment_counter_scan",
        "keepsake_fulfillment_checklist_scan",
    ] {
        let sql = sqlx::query_scalar::<_, Option<String>>(
            "select sql from sqlite_master where type = 'index' and name = ?",
        )
        .bind(index)
        .fetch_optional(pool)
        .await?
        .flatten()
        .ok_or_else(|| mismatch(format!("missing v3 index {index}")))?;
        if !compact_sql(&sql).contains("(tenant_id,") {
            return Err(mismatch(format!("v3 index {index} is not tenant-leading")));
        }
    }

    for trigger in [
        "keepsakes_clean_invariants_insert",
        "keepsakes_clean_invariants_update",
    ] {
        let sql = sqlx::query_scalar::<_, Option<String>>(
            "select sql from sqlite_master where type = 'trigger' and name = ?",
        )
        .bind(trigger)
        .fetch_optional(pool)
        .await?
        .flatten()
        .ok_or_else(|| mismatch(format!("missing v3 trigger {trigger}")))?;
        let compact = compact_sql(&sql);
        if !compact.contains("raise(abort,'keepsakes_clean_invariants')") {
            return Err(mismatch(format!("v3 trigger {trigger} definition differs")));
        }
    }

    for (table, message) in [
        (
            "keepsakes",
            "keepsakes relation foreign key is not tenant-composite",
        ),
        (
            "keepsake_fulfillment_counters",
            "counter foreign key is not tenant-composite",
        ),
        (
            "keepsake_fulfillment_checklist",
            "checklist foreign key is not tenant-composite",
        ),
    ] {
        let foreign_keys = match table {
            "keepsakes" => sqlx::query("pragma foreign_key_list(keepsakes)"),
            "keepsake_fulfillment_counters" => {
                sqlx::query("pragma foreign_key_list(keepsake_fulfillment_counters)")
            }
            "keepsake_fulfillment_checklist" => {
                sqlx::query("pragma foreign_key_list(keepsake_fulfillment_checklist)")
            }
            _ => unreachable!("table list is static"),
        }
        .fetch_all(pool)
        .await?;
        if !foreign_keys.iter().any(|row| {
            row.try_get::<String, _>("from").ok().as_deref() == Some("tenant_id")
                && row.try_get::<String, _>("to").ok().as_deref() == Some("tenant_id")
        }) {
            return Err(mismatch(message));
        }
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
pub(super) async fn sqlite_runtime_schema_check(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    let metadata_exists = sqlx::query_scalar::<_, i64>(
        "select count(*) from sqlite_master where type = 'table' and name = 'keepsake_schema_metadata'",
    )
    .fetch_one(pool)
    .await?;
    if metadata_exists == 0 {
        return Err(mismatch("missing Keepsake schema metadata table"));
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'backend'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if backend.as_deref() != Some(super::SqliteBackend::NAME) {
        return Err(mismatch(format!(
            "missing or incorrect SQLite backend marker: {backend:?}"
        )));
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if track.as_deref() == Some("3") {
        sqlite_v3_domain_shape_check(pool).await?;
        return Ok(());
    }
    if track.as_deref() != Some("2") {
        return Err(RepositoryError::BackendMismatch {
            expected: "2.0 active schema",
            actual: "schema is not activated for the 2.0 API".to_owned(),
        });
    }

    let has_legacy = sqlx::query_scalar::<_, i64>(
        "select count(*) from sqlite_master where type = 'table' and name = 'keepsake_audit_events'",
    )
    .fetch_one(pool)
    .await?
        != 0;
    sqlite_domain_shape_check(pool, has_legacy).await
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "migrations")]
pub(super) async fn sqlite_clean_schema_preflight(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    let has_domain = sqlx::query_scalar::<_, Option<String>>(
        "select name from sqlite_master where type = 'table' and name = 'keepsake_relation_definitions'",
    )
    .fetch_optional(pool)
    .await?
    .flatten()
    .is_some();
    if !has_domain {
        return sqlite_schema_preflight(pool).await;
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("3") => sqlite_v3_domain_shape_check(pool).await,
        Some("2") => {
            let has_legacy = sqlx::query_scalar::<_, i64>(
                "select count(*) from sqlite_master where type = 'table' and name = 'keepsake_audit_events'",
            )
            .fetch_one(pool)
            .await?
                != 0;
            if has_legacy {
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

#[cfg(feature = "sqlite")]
#[cfg(feature = "migrations")]
pub(super) async fn sqlite_upgrade_schema_preflight(
    pool: &sqlx::SqlitePool,
) -> RepositoryResult<()> {
    let has_metadata = sqlx::query_scalar::<_, i64>(
        "select count(*) from sqlite_master where type = 'table' and name = 'keepsake_schema_metadata'",
    )
    .fetch_one(pool)
    .await?
        > 0;
    if !has_metadata {
        return sqlite_schema_preflight(pool).await;
    }

    let has_v2 = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
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
        "select value from keepsake_schema_metadata where key = 'api_track'",
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
    sqlite_schema_preflight(pool).await
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "migrations")]
pub(super) async fn sqlite_schema_preflight(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        "select name from sqlite_master where type = 'table' and name = 'keepsake_schema_metadata'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();

    if metadata_table.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            "select value from keepsake_schema_metadata where key = 'backend'",
        )
        .fetch_optional(pool)
        .await?
        .flatten();
        return match backend.as_deref() {
            Some(super::SqliteBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: super::SqliteBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing_tables = sqlx::query_scalar::<_, i64>(
        "select count(*) from sqlite_master where type = 'table' and name not like 'sqlite_%'",
    )
    .fetch_one(pool)
    .await?;
    if existing_tables == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: super::SqliteBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}

// PostgreSQL and MySQL use information_schema/pg_catalog rather than relying
// on object counts. The compact helpers below keep the semantic comparisons
// readable while preserving the backend-specific catalog queries.

#[cfg(feature = "postgres")]
#[derive(Debug, Clone, Copy)]
struct PgColumn<'a> {
    table: &'a str,
    name: &'a str,
    data_type: &'a str,
    udt_name: &'a str,
    nullable: bool,
    default: Option<&'a str>,
    sequence: bool,
}

#[cfg(feature = "postgres")]
const PG_CLEAN_COLUMNS: &[PgColumn<'_>] = &[
    PgColumn {
        table: "keepsake_schema_metadata",
        name: "key",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_schema_metadata",
        name: "value",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "kind",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "key",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "enabled",
        data_type: "boolean",
        udt_name: "bool",
        nullable: false,
        default: Some("true"),
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "expiry_policy",
        data_type: "jsonb",
        udt_name: "jsonb",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "created_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: Some("now()"),
        sequence: false,
    },
    PgColumn {
        table: "keepsake_relation_definitions",
        name: "updated_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: Some("now()"),
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "subject_kind",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "subject_id",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "relation_id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "state",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "expiry_policy",
        data_type: "jsonb",
        udt_name: "jsonb",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "applied_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "expires_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: true,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "fulfilled_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: true,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "revoked_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: true,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "metadata",
        data_type: "jsonb",
        udt_name: "jsonb",
        nullable: false,
        default: Some("'{}'::jsonb"),
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "created_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: Some("now()"),
        sequence: false,
    },
    PgColumn {
        table: "keepsakes",
        name: "updated_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: Some("now()"),
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_counters",
        name: "keepsake_id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_counters",
        name: "key",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_counters",
        name: "value",
        data_type: "bigint",
        udt_name: "int8",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_counters",
        name: "observed_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_checklist",
        name: "keepsake_id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_checklist",
        name: "item",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_checklist",
        name: "complete",
        data_type: "boolean",
        udt_name: "bool",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_fulfillment_checklist",
        name: "observed_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: None,
        sequence: false,
    },
];

#[cfg(feature = "postgres")]
const PG_LEGACY_COLUMNS: &[PgColumn<'_>] = &[
    PgColumn {
        table: "keepsake_audit_events",
        name: "id",
        data_type: "bigint",
        udt_name: "int8",
        nullable: false,
        default: Some("nextval"),
        sequence: true,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "keepsake_id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "relation_id",
        data_type: "uuid",
        udt_name: "uuid",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "subject_kind",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "subject_id",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "actor_kind",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "actor_id",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "event_type",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "decision",
        data_type: "jsonb",
        udt_name: "jsonb",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "occurred_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_events",
        name: "recorded_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: Some("now()"),
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_context_attributes",
        name: "audit_event_id",
        data_type: "bigint",
        udt_name: "int8",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_context_attributes",
        name: "key",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_context_attributes",
        name: "value",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "id",
        data_type: "bigint",
        udt_name: "int8",
        nullable: false,
        default: Some("nextval"),
        sequence: true,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "audit_event_id",
        data_type: "bigint",
        udt_name: "int8",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "event_type",
        data_type: "text",
        udt_name: "text",
        nullable: false,
        default: Some("'keepsake.audit_event_recorded'"),
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "payload",
        data_type: "jsonb",
        udt_name: "jsonb",
        nullable: false,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "claimed_by",
        data_type: "text",
        udt_name: "text",
        nullable: true,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "claimed_until",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: true,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "delivered_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: true,
        default: None,
        sequence: false,
    },
    PgColumn {
        table: "keepsake_audit_outbox",
        name: "created_at",
        data_type: "timestamp with time zone",
        udt_name: "timestamptz",
        nullable: false,
        default: Some("now()"),
        sequence: false,
    },
];

#[cfg(feature = "postgres")]
fn pg_default_matches(actual: Option<&str>, expected: Option<&str>, sequence: bool) -> bool {
    match (actual, expected, sequence) {
        (Some(actual), Some("nextval") | None, true) => {
            normalize_sql(actual).starts_with("nextval(")
        }
        (Some(actual), Some(expected), false) => {
            let actual = normalize_sql(actual);
            let expected = normalize_sql(expected);
            let actual = actual.strip_suffix("::text").unwrap_or(&actual);
            default_sql(actual) == default_sql(&expected)
        }
        (None, None, false) => true,
        _ => false,
    }
}

#[cfg(feature = "postgres")]
#[allow(clippy::too_many_lines)]
async fn postgres_catalog_shape_check(
    pool: &sqlx::PgPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let expected_tables: &[&str] = if activated_upgrade {
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
    let expected_columns: Vec<PgColumn<'_>> = PG_CLEAN_COLUMNS
        .iter()
        .chain(
            activated_upgrade
                .then_some(PG_LEGACY_COLUMNS)
                .into_iter()
                .flatten(),
        )
        .copied()
        .collect();

    let table_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and table_name = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if table_count != i64::try_from(expected_tables.len()).unwrap_or(i64::MAX) {
        return Err(mismatch(format!(
            "expected {} domain tables, found {table_count}",
            expected_tables.len()
        )));
    }

    if !activated_upgrade {
        let legacy_count = sqlx::query_scalar::<_, i64>(
            "select count(*) from information_schema.tables where table_schema = 'public' and table_name in ('keepsake_audit_events','keepsake_audit_context_attributes','keepsake_audit_outbox')",
        )
        .fetch_one(pool)
        .await?;
        if legacy_count != 0 {
            return Err(mismatch("clean track contains legacy audit tables"));
        }
    }

    for expected in &expected_columns {
        let row = sqlx::query(
            "select data_type, udt_name, is_nullable, column_default, is_identity, is_generated, generation_expression from information_schema.columns where table_schema = 'public' and table_name = $1 and column_name = $2",
        )
        .bind(expected.table)
        .bind(expected.name)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing column {}.{}", expected.table, expected.name)))?;
        let data_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let nullable: String = row.try_get("is_nullable")?;
        let default: Option<String> = row.try_get("column_default")?;
        let identity: String = row.try_get("is_identity")?;
        let generated: String = row.try_get("is_generated")?;
        let generation_expression: Option<String> = row.try_get("generation_expression")?;
        let default_matches =
            pg_default_matches(default.as_deref(), expected.default, expected.sequence);
        if data_type != expected.data_type
            || udt_name != expected.udt_name
            || (nullable == "YES") != expected.nullable
            || identity != "NO"
            || generated != "NEVER"
            || generation_expression.is_some()
            || !default_matches
        {
            return Err(mismatch(format!(
                "column {}.{} has unexpected catalog semantics: type actual={data_type:?}/{udt_name:?} expected={:?}/{:?}; nullable actual={nullable:?} expected={}; identity={identity:?}; generated={generated:?}; generation_present={}; default actual={default:?} expected={:?} sequence={} match={default_matches}",
                expected.table,
                expected.name,
                expected.data_type,
                expected.udt_name,
                expected.nullable,
                generation_expression.is_some(),
                expected.default,
                expected.sequence,
            )));
        }
    }

    let column_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if column_count != i64::try_from(expected_columns.len()).unwrap_or(i64::MAX) {
        return Err(mismatch("domain tables contain unexpected columns"));
    }

    pg_constraints_check(pool, activated_upgrade).await?;
    pg_indexes_check(pool, activated_upgrade).await?;
    let trigger_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from pg_trigger t join pg_class c on c.oid = t.tgrelid join pg_namespace n on n.oid = c.relnamespace where not t.tgisinternal and n.nspname = 'public' and c.relname = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if trigger_count != 0 {
        return Err(mismatch(
            "unexpected trigger mutates a Keepsake invariant table",
        ));
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn pg_expected_check_expression(activated_upgrade: bool, name: &str) -> Option<String> {
    let artifact = if activated_upgrade {
        PG_UPGRADE_ARTIFACT
    } else {
        PG_CLEAN_ARTIFACT
    };
    let marker = match name {
        "keepsakes_state_check" if activated_upgrade => "state text not null check",
        "keepsakes_state_check" => "constraint keepsakes_state_check check",
        "keepsakes_expiry_policy_projection" => {
            "constraint keepsakes_expiry_policy_projection check"
        }
        "keepsakes_lifecycle_timestamps" => "constraint keepsakes_lifecycle_timestamps check",
        _ => return None,
    };
    artifact_check_expression(artifact, marker)
}

#[cfg(feature = "postgres")]
fn pg_catalog_check_matches(
    activated_upgrade: bool,
    name: &str,
    actual: &str,
    expected: &str,
) -> bool {
    let actual = normalize_check_expression(actual);
    if name == "keepsakes_expiry_policy_projection" {
        return actual
            == "coalesce(((expiry_policy->>'type')=any(array['manual_only','at','when_fulfilled']))and((expiry_policy->>'type')='at'andexpires_atisnotnulland((expiry_policy->>'timestamp'))=expires_ator((expiry_policy->>'type')=any(array['manual_only','when_fulfilled']))andexpires_atisnull),false)";
    }

    if name == "keepsakes_lifecycle_timestamps" && !activated_upgrade {
        return actual
            == "coalesce(state='applied'andrevoked_atisnullandfulfilled_atisnullorstate='revoked'andrevoked_atisnotnullandfulfilled_atisnullorstate='expired'andrevoked_atisnulland((expiry_policy->>'type')='at'andexpires_atisnotnullandfulfilled_atisnullor(expiry_policy->>'type')='when_fulfilled'andfulfilled_atisnotnullandexpires_atisnull),false)";
    }

    if name == "keepsakes_lifecycle_timestamps" && activated_upgrade {
        return actual
            == "coalesce(((expiry_policy->>'type')=any(array['manual_only','at','when_fulfilled']))and(state='applied'andrevoked_atisnullandfulfilled_atisnullorstate='revoked'andrevoked_atisnotnullandfulfilled_atisnullorstate='expired'andrevoked_atisnulland((expiry_policy->>'type')='at'andexpires_atisnotnullandfulfilled_atisnullor(expiry_policy->>'type')='when_fulfilled'andfulfilled_atisnotnullandexpires_atisnull)),false)";
    }

    let _ = activated_upgrade;
    actual == normalize_check_expression(expected)
}

#[cfg(feature = "mysql")]
fn mysql_catalog_check_matches(
    activated_upgrade: bool,
    name: &str,
    actual: &str,
    expected: &str,
    maria_db: bool,
) -> bool {
    let actual = normalize_check_expression(actual);
    if maria_db && name == "keepsakes_expiry_policy_projection" {
        // MariaDB deparses redundant boolean grouping around AND/OR terms.
        // Keep this equivalent form explicit so the verifier remains strict
        // about the expression while accepting the server's canonical output.
        return actual
            == "(json_unquote(json_extract(expiry_policy,'$.type'))=any(array['manual_only','at','when_fulfilled'])and(json_unquote(json_extract(expiry_policy,'$.type'))='at'andexpires_atisnotnullandcast(replace(replace(json_unquote(json_extract(expiry_policy,'$.timestamp')),'t',''),'z','')asdatetime(6))=expires_atorjson_unquote(json_extract(expiry_policy,'$.type'))=any(array['manual_only','when_fulfilled'])andexpires_atisnull))istrue";
    }
    if maria_db && name == "keepsakes_lifecycle_timestamps" && !activated_upgrade {
        return actual
            == "(state='applied'andrevoked_atisnullandfulfilled_atisnullorstate='revoked'andrevoked_atisnotnullandfulfilled_atisnullorstate='expired'andrevoked_atisnulland(json_unquote(json_extract(expiry_policy,'$.type'))='at'andexpires_atisnotnullandfulfilled_atisnullorjson_unquote(json_extract(expiry_policy,'$.type'))='when_fulfilled'andfulfilled_atisnotnullandexpires_atisnull))istrue";
    }
    if name == "keepsakes_expiry_policy_projection" {
        return actual
            == "((json_unquote(json_extract(expiry_policy,'$.type'))=any(array['manual_only','at','when_fulfilled']))and(((json_unquote(json_extract(expiry_policy,'$.type'))='at')and(expires_atisnotnull)and(cast(replace(replace(json_unquote(json_extract(expiry_policy,'$.timestamp')),'t',''),'z','')asdatetime(6))=expires_at))or((json_unquote(json_extract(expiry_policy,'$.type'))=any(array['manual_only','when_fulfilled']))and(expires_atisnull))))istrue";
    }

    if name == "keepsakes_lifecycle_timestamps" && !activated_upgrade {
        return actual
            == "(((state='applied')and(revoked_atisnull)and(fulfilled_atisnull))or((state='revoked')and(revoked_atisnotnull)and(fulfilled_atisnull))or((state='expired')and(revoked_atisnull)and(((json_unquote(json_extract(expiry_policy,'$.type'))='at')and(expires_atisnotnull)and(fulfilled_atisnull))or((json_unquote(json_extract(expiry_policy,'$.type'))='when_fulfilled')and(fulfilled_atisnotnull)and(expires_atisnull)))))istrue";
    }

    if name == "keepsakes_lifecycle_timestamps" && activated_upgrade {
        return actual
            == "((json_unquote(json_extract(expiry_policy,'$.type'))=any(array['manual_only','at','when_fulfilled']))and(((state='applied')and(revoked_atisnull)and(fulfilled_atisnull))or((state='revoked')and(revoked_atisnotnull)and(fulfilled_atisnull))or((state='expired')and(revoked_atisnull)and(((json_unquote(json_extract(expiry_policy,'$.type'))='at')and(expires_atisnotnull)and(fulfilled_atisnull))or((json_unquote(json_extract(expiry_policy,'$.type'))='when_fulfilled')and(fulfilled_atisnotnull)and(expires_atisnull))))))istrue";
    }

    let _ = activated_upgrade;
    actual == normalize_check_expression(expected)
}

#[cfg(feature = "postgres")]
#[allow(clippy::too_many_lines)]
#[allow(clippy::excessive_nesting)]
async fn pg_constraints_check(
    pool: &sqlx::PgPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let expected: &[(&str, &str, &str)] = if activated_upgrade {
        &[
            ("keepsake_schema_metadata", "p", "primary key (key)"),
            ("keepsake_relation_definitions", "p", "primary key (id)"),
            ("keepsake_relation_definitions", "u", "unique (kind, key)"),
            ("keepsakes", "p", "primary key (id)"),
            (
                "keepsakes",
                "f",
                "foreign key (relation_id) references keepsake_relation_definitions(id)",
            ),
            ("keepsakes", "c", "keepsakes_state_check"),
            (
                "keepsake_fulfillment_counters",
                "p",
                "primary key (keepsake_id, key)",
            ),
            (
                "keepsake_fulfillment_counters",
                "f",
                "foreign key (keepsake_id) references keepsakes(id) on delete cascade",
            ),
            (
                "keepsake_fulfillment_checklist",
                "p",
                "primary key (keepsake_id, item)",
            ),
            (
                "keepsake_fulfillment_checklist",
                "f",
                "foreign key (keepsake_id) references keepsakes(id) on delete cascade",
            ),
            ("keepsakes", "c", "keepsakes_expiry_policy_projection"),
            ("keepsakes", "c", "keepsakes_lifecycle_timestamps"),
            ("keepsake_audit_events", "p", "primary key (id)"),
            (
                "keepsake_audit_context_attributes",
                "p",
                "primary key (audit_event_id, key)",
            ),
            (
                "keepsake_audit_context_attributes",
                "f",
                "foreign key (audit_event_id) references keepsake_audit_events(id) on delete cascade",
            ),
            ("keepsake_audit_outbox", "p", "primary key (id)"),
            (
                "keepsake_audit_outbox",
                "f",
                "foreign key (audit_event_id) references keepsake_audit_events(id) on delete cascade",
            ),
        ]
    } else {
        &[
            ("keepsake_schema_metadata", "p", "primary key (key)"),
            ("keepsake_relation_definitions", "p", "primary key (id)"),
            ("keepsake_relation_definitions", "u", "unique (kind, key)"),
            ("keepsakes", "p", "primary key (id)"),
            (
                "keepsakes",
                "f",
                "foreign key (relation_id) references keepsake_relation_definitions(id)",
            ),
            ("keepsakes", "c", "keepsakes_state_check"),
            (
                "keepsake_fulfillment_counters",
                "p",
                "primary key (keepsake_id, key)",
            ),
            (
                "keepsake_fulfillment_counters",
                "f",
                "foreign key (keepsake_id) references keepsakes(id) on delete cascade",
            ),
            (
                "keepsake_fulfillment_checklist",
                "p",
                "primary key (keepsake_id, item)",
            ),
            (
                "keepsake_fulfillment_checklist",
                "f",
                "foreign key (keepsake_id) references keepsakes(id) on delete cascade",
            ),
            ("keepsakes", "c", "keepsakes_expiry_policy_projection"),
            ("keepsakes", "c", "keepsakes_lifecycle_timestamps"),
        ]
    };
    let rows = sqlx::query("select c.relname, x.contype::text as contype, x.conname, pg_get_constraintdef(x.oid, true) as definition from pg_constraint x join pg_class c on c.oid = x.conrelid join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = any($1) and x.contype in ('p','u','f','c')")
        .bind(if activated_upgrade { &[
            "keepsake_schema_metadata", "keepsake_relation_definitions", "keepsakes", "keepsake_fulfillment_counters", "keepsake_fulfillment_checklist", "keepsake_audit_events", "keepsake_audit_context_attributes", "keepsake_audit_outbox",
        ][..] } else { &[
            "keepsake_schema_metadata", "keepsake_relation_definitions", "keepsakes", "keepsake_fulfillment_counters", "keepsake_fulfillment_checklist",
        ][..] })
        .fetch_all(pool).await?;
    if rows.len() != expected.len() {
        return Err(mismatch(
            "primary, foreign, unique, or check constraint count differs",
        ));
    }

    for row in rows {
        let table: String = row.try_get("relname")?;
        let kind: String = row.try_get("contype")?;
        let name: String = row.try_get("conname")?;
        let definition: String = row.try_get("definition")?;
        let found = expected
            .iter()
            .any(|(expected_table, expected_kind, expected_def)| {
                *expected_table == table
                    && *expected_kind == kind
                    && if kind == "c" {
                        name == *expected_def
                            && pg_expected_check_expression(activated_upgrade, &name).is_some_and(
                                |expected_expression| {
                                    pg_catalog_check_matches(
                                        activated_upgrade,
                                        &name,
                                        &definition,
                                        &expected_expression,
                                    )
                                },
                            )
                    } else {
                        normalize_sql(&definition) == normalize_sql(expected_def)
                    }
            });
        if !found {
            return Err(mismatch(format!(
                "unexpected or altered constraint {table}.{name}: kind={kind:?}, actual={:?}, expected={:?}",
                normalize_check_expression(&definition),
                expected
                    .iter()
                    .find(|(expected_table, expected_kind, expected_def)| {
                        *expected_table == table && *expected_kind == kind && *expected_def == name
                    })
                    .and_then(|(_, _, expected_def)| {
                        pg_expected_check_expression(activated_upgrade, expected_def)
                            .map(|expression| normalize_check_expression(&expression))
                    })
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
#[allow(clippy::too_many_lines)]
async fn pg_indexes_check(pool: &sqlx::PgPool, activated_upgrade: bool) -> RepositoryResult<()> {
    use sqlx::Row;
    let expected: &[(&str, &str, bool, &str, &str)] = &[
        (
            "keepsakes_one_active_relation_per_subject",
            "keepsakes",
            true,
            "subject_kind,subject_id,relation_id",
            "state='applied'",
        ),
        (
            "keepsakes_active_subject_lookup",
            "keepsakes",
            false,
            "subject_kind,subject_id,relation_id,id",
            "state='applied'",
        ),
        (
            "keepsakes_active_relation_membership",
            "keepsakes",
            false,
            "relation_id,subject_kind,subject_id,id",
            "state='applied'",
        ),
        (
            "keepsakes_due_timed_expiry",
            "keepsakes",
            false,
            "expires_at,relation_id,subject_kind,subject_id,id",
            "state='applied'andexpires_atisnotnull",
        ),
        (
            "keepsake_fulfillment_counter_scan",
            "keepsake_fulfillment_counters",
            false,
            "key,value,keepsake_id",
            "",
        ),
        (
            "keepsakes_due_fulfilled_expiry",
            "keepsakes",
            false,
            "relation_id,subject_kind,subject_id,id",
            "state='applied'andexpiry_policy->>'type'='when_fulfilled'",
        ),
        (
            "keepsake_fulfillment_checklist_scan",
            "keepsake_fulfillment_checklist",
            false,
            "item,complete,keepsake_id",
            "",
        ),
    ];
    let expected = expected
        .iter()
        .map(|(name, table, unique, columns, predicate)| {
            (*name, *table, *unique, *columns, *predicate)
        })
        .collect::<Vec<_>>();
    let mut expected = expected;
    if activated_upgrade {
        expected.extend([
            (
                "keepsake_audit_by_keepsake",
                "keepsake_audit_events",
                false,
                "keepsake_id,occurred_at,id",
                "",
            ),
            (
                "keepsake_audit_by_relation",
                "keepsake_audit_events",
                false,
                "relation_id,occurred_at,id",
                "",
            ),
            (
                "keepsake_audit_context_attribute_lookup",
                "keepsake_audit_context_attributes",
                false,
                "key,value,audit_event_id",
                "",
            ),
            (
                "keepsake_audit_outbox_export",
                "keepsake_audit_outbox",
                false,
                "id",
                "delivered_atisnull",
            ),
            (
                "keepsake_audit_outbox_claim",
                "keepsake_audit_outbox",
                false,
                "delivered_at,claimed_until,id",
                "",
            ),
        ]);
    }

    for (name, table, unique, columns, predicate) in expected {
        let row = sqlx::query("select ix.indisunique, pg_get_indexdef(ix.indexrelid, 0, true) as definition from pg_index ix join pg_class c on c.oid = ix.indexrelid join pg_namespace n on n.oid = c.relnamespace join pg_class table_class on table_class.oid = ix.indrelid where n.nspname = 'public' and c.relname = $1 and table_class.relname = $2")
            .bind(name).bind(table).fetch_optional(pool).await?
            .ok_or_else(|| mismatch(format!("missing index {name}")))?;
        let actual_unique: bool = row.try_get("indisunique")?;
        let definition: String = row.try_get("definition")?;
        let compact = compact_pg_index(&definition);
        let expected_prefix = format!("on{table}({columns})");
        let actual_predicate = compact
            .split_once("where")
            .map_or(String::new(), |(_, value)| value.replace(['(', ')'], ""));
        if actual_unique != unique
            || !compact.contains(&expected_prefix)
            || actual_predicate != predicate.replace(' ', "")
        {
            return Err(mismatch(format!(
                "index {name} columns, uniqueness, or predicate differ"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn compact_pg_index(definition: &str) -> String {
    compact_sql(definition)
        .replace("public.", "")
        .replace("usingbtree", "")
        .replace("::text", "")
}

#[cfg(all(feature = "postgres", feature = "migrations"))]
pub(super) async fn postgres_upgrade_schema_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    postgres_catalog_shape_check(pool, true).await
}

#[cfg(feature = "postgres")]
pub(super) async fn postgres_runtime_schema_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let metadata_exists = sqlx::query_scalar::<_, bool>(
        "select to_regclass('public.keepsake_schema_metadata') is not null",
    )
    .fetch_one(pool)
    .await?;
    if !metadata_exists {
        return Err(mismatch("missing Keepsake schema metadata table"));
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'backend'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if backend.as_deref() != Some(super::PostgresBackend::NAME) {
        return Err(mismatch(format!(
            "missing or incorrect PostgreSQL backend marker: {backend:?}"
        )));
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("3") => postgres_v3_runtime_schema_check(pool).await,
        Some("2") => Err(RepositoryError::BackendMismatch {
            expected: "3.0 active schema",
            actual: "schema is still on the 2.0 API track; run the explicit tenant upgrade route"
                .to_owned(),
        }),
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: "3.0 active schema",
            actual: format!("unsupported Keepsake API track {actual}"),
        }),
        None => Err(RepositoryError::BackendMismatch {
            expected: "3.0 active schema",
            actual: "schema is not activated for the 3.0 API".to_owned(),
        }),
    }
}

#[cfg(feature = "postgres")]
async fn postgres_v3_runtime_schema_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let expected_tables = [
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ];
    let table_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = 'public' and table_name = any($1)",
    )
    .bind(expected_tables.as_slice())
    .fetch_one(pool)
    .await?;
    let tenant_column_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1) and column_name = 'tenant_id'",
    )
    .bind(expected_tables.as_slice())
    .fetch_one(pool)
    .await?;
    let tenant_collation_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1) and column_name = 'tenant_id' and collation_name = 'C'",
    )
    .bind(expected_tables.as_slice())
    .fetch_one(pool)
    .await?;
    if table_count != 4 || tenant_column_count != 4 || tenant_collation_count != 4 {
        return Err(RepositoryError::BackendMismatch {
            expected: "complete Keepsake 3.0 tenant-aware PostgreSQL schema",
            actual: "missing tenant-owned Keepsake table, column, or C collation".to_owned(),
        });
    }

    postgres_v3_columns_check(pool).await?;

    let indexes = sqlx::query_scalar::<_, i64>(
        "select count(*) from pg_indexes where schemaname = 'public' and indexname = any($1)",
    )
    .bind([
        "keepsakes_one_active_relation_per_subject",
        "keepsakes_active_subject_lookup",
        "keepsakes_active_relation_membership",
        "keepsakes_due_timed_expiry",
        "keepsakes_due_fulfilled_expiry",
        "keepsake_fulfillment_counter_scan",
        "keepsake_fulfillment_checklist_scan",
    ])
    .fetch_one(pool)
    .await?;
    if indexes != 7 {
        return Err(RepositoryError::BackendMismatch {
            expected: "tenant-leading Keepsake 3.0 PostgreSQL indexes",
            actual: "one or more tenant-aware indexes are missing".to_owned(),
        });
    }
    postgres_v3_constraints_check(pool).await?;
    postgres_v3_indexes_check(pool).await?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn postgres_v3_columns_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    use sqlx::Row;

    // The v3 tables retain the v2 column types and add one tenant_id column to
    // each relation-owned table. Reuse the v2 catalog contract for the stable
    // columns, then verify the tenant columns separately because their C
    // collation is part of the identity-isolation contract.
    for expected in PG_CLEAN_COLUMNS {
        let row = sqlx::query(
            "select data_type, udt_name, is_nullable, column_default, is_identity, is_generated, generation_expression from information_schema.columns where table_schema = 'public' and table_name = $1 and column_name = $2",
        )
        .bind(expected.table)
        .bind(expected.name)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing column {}.{}", expected.table, expected.name)))?;
        let data_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let nullable: String = row.try_get("is_nullable")?;
        let default: Option<String> = row.try_get("column_default")?;
        let identity: String = row.try_get("is_identity")?;
        let generated: String = row.try_get("is_generated")?;
        let generation_expression: Option<String> = row.try_get("generation_expression")?;
        if data_type != expected.data_type
            || udt_name != expected.udt_name
            || (nullable == "YES") != expected.nullable
            || identity != "NO"
            || generated != "NEVER"
            || generation_expression.is_some()
            || !pg_default_matches(default.as_deref(), expected.default, expected.sequence)
        {
            return Err(mismatch(format!(
                "column {}.{} has unexpected v3 catalog semantics",
                expected.table, expected.name
            )));
        }
    }

    for table in [
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ] {
        let row = sqlx::query(
            "select data_type, udt_name, is_nullable, column_default, collation_name, is_identity, is_generated, generation_expression from information_schema.columns where table_schema = 'public' and table_name = $1 and column_name = 'tenant_id'",
        )
        .bind(table)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing column {table}.tenant_id")))?;
        let data_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let nullable: String = row.try_get("is_nullable")?;
        let default: Option<String> = row.try_get("column_default")?;
        let collation: Option<String> = row.try_get("collation_name")?;
        let identity: String = row.try_get("is_identity")?;
        let generated: String = row.try_get("is_generated")?;
        let generation_expression: Option<String> = row.try_get("generation_expression")?;
        if data_type != "text"
            || udt_name != "text"
            || nullable != "NO"
            || default.is_some()
            || collation.as_deref() != Some("C")
            || identity != "NO"
            || generated != "NEVER"
            || generation_expression.is_some()
        {
            return Err(mismatch(format!(
                "column {table}.tenant_id has unexpected v3 catalog semantics"
            )));
        }
    }

    let column_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1)",
    )
    .bind([
        "keepsake_schema_metadata",
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ])
    .fetch_one(pool)
    .await?;
    let expected_count = i64::try_from(PG_CLEAN_COLUMNS.len() + 4).unwrap_or(i64::MAX);
    if column_count != expected_count {
        return Err(mismatch("v3 domain tables contain unexpected columns"));
    }
    Ok(())
}

#[cfg(feature = "postgres")]
#[allow(clippy::too_many_lines)]
async fn postgres_v3_constraints_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    use sqlx::Row;

    // Compare catalog definitions rather than only constraint names. Names
    // generated for composite unique constraints are PostgreSQL-version
    // details, while the tenant-leading columns and foreign-key pairs are the
    // actual isolation contract.
    let rows = sqlx::query(
        "select c.relname, x.contype::text as contype, x.conname, pg_get_constraintdef(x.oid, true) as definition from pg_constraint x join pg_class c on c.oid = x.conrelid join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = any($1) and x.contype in ('p','u','f','c')",
    )
    .bind([
        "keepsake_schema_metadata",
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ])
    .fetch_all(pool)
    .await?;
    if rows.len() != 20 {
        return Err(RepositoryError::BackendMismatch {
            expected: "complete Keepsake 3.0 PostgreSQL constraints",
            actual: "primary, foreign, unique, or check constraint count differs".to_owned(),
        });
    }

    let expected = [
        ("keepsake_schema_metadata", "p", "primary key (key)"),
        (
            "keepsake_relation_definitions",
            "p",
            "primary key (tenant_id, id)",
        ),
        (
            "keepsake_relation_definitions",
            "u",
            "unique (tenant_id, kind, key)",
        ),
        ("keepsakes", "p", "primary key (tenant_id, id)"),
        (
            "keepsakes",
            "f",
            "foreign key (tenant_id, relation_id) references keepsake_relation_definitions(tenant_id, id)",
        ),
        (
            "keepsake_fulfillment_counters",
            "p",
            "primary key (tenant_id, keepsake_id, key)",
        ),
        (
            "keepsake_fulfillment_counters",
            "f",
            "foreign key (tenant_id, keepsake_id) references keepsakes(tenant_id, id) on delete cascade",
        ),
        (
            "keepsake_fulfillment_checklist",
            "p",
            "primary key (tenant_id, keepsake_id, item)",
        ),
        (
            "keepsake_fulfillment_checklist",
            "f",
            "foreign key (tenant_id, keepsake_id) references keepsakes(tenant_id, id) on delete cascade",
        ),
    ];

    for row in rows {
        let table: String = row.try_get("relname")?;
        let kind: String = row.try_get("contype")?;
        let name: String = row.try_get("conname")?;
        let definition: String = row.try_get("definition")?;
        let normalized = normalize_sql(&definition).replace("public.", "");
        let matches = if kind == "c" {
            let check = normalize_check_expression(&definition);
            match name.as_str() {
                "keepsake_relation_definitions_tenant_size"
                | "keepsake_relation_definitions_tenant_nonempty"
                | "keepsakes_tenant_size"
                | "keepsakes_tenant_nonempty"
                | "keepsake_fulfillment_counter_tenant_size"
                | "keepsake_fulfillment_counter_tenant_nonempty"
                | "keepsake_fulfillment_checklist_tenant_size"
                | "keepsake_fulfillment_checklist_tenant_nonempty" => match name.as_str() {
                    "keepsake_relation_definitions_tenant_nonempty"
                    | "keepsakes_tenant_nonempty"
                    | "keepsake_fulfillment_counter_tenant_nonempty"
                    | "keepsake_fulfillment_checklist_tenant_nonempty" => {
                        check.contains("octet_length(tenant_id)>0")
                    }
                    _ => check.contains("octet_length(tenant_id)<=255"),
                },
                "keepsakes_state_check" => {
                    check.contains("state=any(array['applied','revoked','expired'])")
                }
                "keepsakes_expiry_policy_projection" => {
                    check.contains(
                        "(expiry_policy->>'type')=any(array['manual_only','at','when_fulfilled'])",
                    ) && check.contains("expires_atisnotnull")
                        && check.contains("expires_atisnull")
                }
                "keepsakes_lifecycle_timestamps" => {
                    check.contains("state='applied'andrevoked_atisnullandfulfilled_atisnull")
                        && check
                            .contains("state='revoked'andrevoked_atisnotnullandfulfilled_atisnull")
                        && check.contains("state='expired'andrevoked_atisnull")
                        && check.contains("(expiry_policy->>'type')='at'")
                        && check.contains("(expiry_policy->>'type')='when_fulfilled'")
                }
                _ => false,
            }
        } else {
            expected
                .iter()
                .any(|(expected_table, expected_kind, definition)| {
                    *expected_table == table
                        && *expected_kind == kind
                        && normalize_sql(definition) == normalized
                })
        };
        if !matches {
            return Err(RepositoryError::BackendMismatch {
                expected: "complete Keepsake 3.0 PostgreSQL constraints",
                actual: format!("unexpected or altered constraint {table}.{name}"),
            });
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
async fn postgres_v3_indexes_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    use sqlx::Row;

    let expected = [
        (
            "keepsakes_one_active_relation_per_subject",
            "keepsakes",
            true,
            "tenant_id,subject_kind,subject_id,relation_id",
            "state='applied'",
        ),
        (
            "keepsakes_active_subject_lookup",
            "keepsakes",
            false,
            "tenant_id,subject_kind,subject_id,relation_id,id",
            "state='applied'",
        ),
        (
            "keepsakes_active_relation_membership",
            "keepsakes",
            false,
            "tenant_id,relation_id,subject_kind,subject_id,id",
            "state='applied'",
        ),
        (
            "keepsakes_due_timed_expiry",
            "keepsakes",
            false,
            "tenant_id,expires_at,relation_id,subject_kind,subject_id,id",
            "state='applied'andexpires_atisnotnull",
        ),
        (
            "keepsakes_due_fulfilled_expiry",
            "keepsakes",
            false,
            "tenant_id,relation_id,subject_kind,subject_id,id",
            "state='applied'andexpiry_policy->>'type'='when_fulfilled'",
        ),
        (
            "keepsake_fulfillment_counter_scan",
            "keepsake_fulfillment_counters",
            false,
            "tenant_id,key,value,keepsake_id",
            "",
        ),
        (
            "keepsake_fulfillment_checklist_scan",
            "keepsake_fulfillment_checklist",
            false,
            "tenant_id,item,complete,keepsake_id",
            "",
        ),
    ];

    for (name, table, unique, columns, predicate) in expected {
        let row = sqlx::query(
            "select ix.indisunique, pg_get_indexdef(ix.indexrelid, 0, true) as definition from pg_index ix join pg_class c on c.oid = ix.indexrelid join pg_namespace n on n.oid = c.relnamespace join pg_class table_class on table_class.oid = ix.indrelid where n.nspname = 'public' and c.relname = $1 and table_class.relname = $2",
        )
        .bind(name)
        .bind(table)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing index {name}")))?;
        let actual_unique: bool = row.try_get("indisunique")?;
        let definition: String = row.try_get("definition")?;
        let compact = compact_pg_index(&definition);
        let expected_prefix = format!("on{table}({columns})");
        let actual_predicate = compact
            .split_once("where")
            .map_or(String::new(), |(_, value)| value.replace(['(', ')'], ""));
        if actual_unique != unique
            || !compact.contains(&expected_prefix)
            || actual_predicate != predicate
        {
            return Err(mismatch(format!(
                "index {name} columns, uniqueness, or predicate differ"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
#[cfg(feature = "migrations")]
pub(super) async fn postgres_clean_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let has_domain = sqlx::query_scalar::<_, bool>(
        "select to_regclass('public.keepsake_relation_definitions') is not null",
    )
    .fetch_one(pool)
    .await?;
    if !has_domain {
        return postgres_schema_preflight(pool).await;
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("2") => {
            let legacy = sqlx::query_scalar::<_, bool>(
                "select to_regclass('public.keepsake_audit_events') is not null",
            )
            .fetch_one(pool)
            .await?;
            if legacy {
                return Err(RepositoryError::BackendMismatch {
                    expected: "2.0 clean track",
                    actual: "activated upgrade track".to_owned(),
                });
            }
            postgres_catalog_shape_check(pool, false).await
        }
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: "2.0 clean track",
            actual: actual.to_owned(),
        }),
        None => Err(RepositoryError::BackendMismatch {
            expected: "2.0 clean track",
            actual: "legacy schema; call upgrade_migrate".to_owned(),
        }),
    }
}

#[cfg(feature = "postgres")]
#[cfg(feature = "migrations")]
pub(super) async fn postgres_upgrade_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>(
        "select to_regclass('public.keepsake_schema_metadata')::text",
    )
    .fetch_one(pool)
    .await?;
    let has_v2 = if metadata.is_some() {
        sqlx::query_scalar::<_, bool>(
            "select exists (select 1 from keepsake_schema_metadata where key = 'api_track' and value = '2')",
        )
        .fetch_one(pool)
        .await?
    } else {
        false
    };
    if has_v2 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "2.0 clean track".to_owned(),
        });
    }
    let has_v3 = if metadata.is_some() {
        sqlx::query_scalar::<_, bool>(
            "select exists (select 1 from keepsake_schema_metadata where key = 'api_track' and value = '3')",
        )
        .fetch_one(pool)
        .await?
    } else {
        false
    };
    if has_v3 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "3.0 clean track".to_owned(),
        });
    }
    postgres_schema_preflight(pool).await
}

#[cfg(feature = "postgres")]
#[cfg(feature = "migrations")]
pub(super) async fn postgres_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>(
        "select to_regclass('public.keepsake_schema_metadata')::text",
    )
    .fetch_one(pool)
    .await?;
    if metadata.is_none() {
        return postgres_unmarked_schema_preflight(pool).await;
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'backend'",
    )
    .fetch_one(pool)
    .await?;
    match backend.as_deref() {
        Some(super::PostgresBackend::NAME) | None => Ok(()),
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: super::PostgresBackend::NAME,
            actual: actual.to_owned(),
        }),
    }
}

#[cfg(feature = "postgres")]
#[cfg(feature = "migrations")]
async fn postgres_unmarked_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE'").fetch_one(pool).await?;
    if count == 0 {
        return Ok(());
    }

    let known = sqlx::query_scalar::<_, bool>("select to_regclass('public.keepsake_relation_definitions') is not null and to_regclass('public.keepsakes') is not null and to_regclass('public._sqlx_migrations') is not null").fetch_one(pool).await?;
    if !known {
        return Err(RepositoryError::BackendMismatch {
            expected: super::PostgresBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        });
    }

    let migrations = sqlx::query_scalar::<_, i64>(
        "select count(*) from _sqlx_migrations where version in (1,2)",
    )
    .fetch_one(pool)
    .await?;
    if migrations == 2 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: super::PostgresBackend::NAME,
            actual: "unmarked unknown migration history".to_owned(),
        })
    }
}

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
fn mysql_default_matches(actual: Option<&str>, expected: Option<&str>) -> bool {
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
fn mysql_is_generated_extra(extra: &str) -> bool {
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
pub(super) async fn mysql_upgrade_schema_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
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
fn mysql_v3_referential_action_matches(expected: &str, actual: &str) -> bool {
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
pub(super) async fn mysql_runtime_schema_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
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
    if backend.as_deref() != Some(super::MySqlBackend::NAME) {
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
pub(super) async fn mysql_clean_schema_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
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
pub(super) async fn mysql_upgrade_schema_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
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
pub(super) async fn mysql_schema_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>("select table_name from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'").fetch_optional(pool).await?.flatten();
    if metadata.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            "select value from keepsake_schema_metadata where `key` = 'backend'",
        )
        .fetch_optional(pool)
        .await?
        .flatten();
        return match backend.as_deref() {
            Some(super::MySqlBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: super::MySqlBackend::NAME,
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
            expected: super::MySqlBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}
