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
    row: impl std::fmt::Display,
    bytes: &[u8],
) -> RepositoryResult<()> {
    let value = std::str::from_utf8(bytes).map_err(|error| {
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
    row: impl std::fmt::Display,
    actual_type: &str,
) -> RepositoryError {
    mismatch(format!(
        "v4 identifier migration preflight rejected {}.{} row {}: expected text, found {actual_type}",
        identifier.table, identifier.column, row
    ))
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
    fn v4_mysql_identifier_contract_is_explicit_and_binary() {
        let contract = super::normalize_sql(super::MYSQL_V4_IDENTIFIER_ARTIFACT);
        assert_eq!(contract.matches("collate utf8mb4_bin").count(), 10);
        for marker in [
            "keepsake_relation_definitions_identifier_contract",
            "keepsakes_identifier_contract",
            "keepsake_fulfillment_counter_identifier_contract",
            "keepsake_fulfillment_checklist_identifier_contract",
        ] {
            assert!(contract.contains(marker));
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

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
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

    #[test]
    fn v4_postgres_identifier_contract_is_byte_bounded() {
        let contract = super::normalize_sql(super::PG_V4_IDENTIFIER_ARTIFACT);
        assert_eq!(contract.matches("collate \"c\"").count(), 10);
        assert_eq!(contract.matches("<= 191").count(), 10);
        for marker in [
            "keepsake_relation_definitions_identifier_contract",
            "keepsakes_identifier_contract",
            "keepsake_fulfillment_counter_identifier_contract",
            "keepsake_fulfillment_checklist_identifier_contract",
        ] {
            assert!(contract.contains(marker));
        }
    }
}

#[cfg(all(test, feature = "sqlite", feature = "migrations"))]
mod sqlite_artifact_tests {
    #[test]
    fn v4_sqlite_identifier_contract_covers_every_domain_table() {
        let contract = super::normalize_sql(super::SQLITE_V4_IDENTIFIER_ARTIFACT);
        let compact = super::compact_sql(super::SQLITE_V4_IDENTIFIER_ARTIFACT);
        for table in [
            "keepsake_relation_definitions",
            "keepsakes",
            "keepsake_fulfillment_counters",
            "keepsake_fulfillment_checklist",
        ] {
            assert!(contract.contains(&format!(
                "create trigger {table}_identifier_contract_insert"
            )));
            assert!(contract.contains(&format!(
                "create trigger {table}_identifier_contract_update"
            )));
        }
        assert_eq!(
            contract
                .matches("raise(abort, 'keepsake_identifier_contract')")
                .count(),
            8
        );
        assert!(compact.contains("length(cast(new.tenant_idasblob))<=191"));
        assert!(contract.contains("update keepsake_schema_metadata set value = '4'"));
    }
}

#[cfg(all(
    test,
    feature = "migrations",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
mod persisted_identifier_tests {
    use super::{PERSISTED_IDENTIFIERS, validate_persisted_identifier_bytes};

    #[test]
    fn byte_reader_matches_core_identifier_contract() {
        let identifier = PERSISTED_IDENTIFIERS[0];
        let valid = format!("{}a", "é".repeat(95));
        assert_eq!(valid.len(), 191);
        assert!(validate_persisted_identifier_bytes(identifier, "row-1", valid.as_bytes()).is_ok());

        for (value, expected) in [
            ("", "must not be empty"),
            ("\u{2003}tenant", "leading or trailing whitespace"),
            ("tenant\u{2003}", "leading or trailing whitespace"),
            ("tenant\u{007f}", "control character"),
            ("tenant\u{fdd0}", "noncharacter"),
        ] {
            let result = validate_persisted_identifier_bytes(identifier, "row-2", value.as_bytes());
            assert!(result.is_err(), "{value:?} should be rejected");
            if let Err(error) = result {
                assert!(error.to_string().contains(expected), "{error}");
            }
        }

        let too_long = "é".repeat(96);
        let result = validate_persisted_identifier_bytes(identifier, "row-3", too_long.as_bytes());
        assert!(result.is_err(), "byte length should be bounded");
        if let Err(error) = result {
            assert!(error.to_string().contains("192 UTF-8 bytes"), "{error}");
        }

        let result = validate_persisted_identifier_bytes(identifier, "row-4", &[0xff]);
        assert!(result.is_err(), "invalid UTF-8 should be rejected");
        if let Err(error) = result {
            assert!(error.to_string().contains("invalid UTF-8"), "{error}");
        }
    }
}

#[cfg(all(feature = "postgres", feature = "migrations"))]
const PG_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v2/postgres/2000_clean_baseline.sql");

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
const PG_V3_CLEAN_ARTIFACT: &str =
    include_str!("../../migrations/v3/postgres/3000_clean_baseline.sql");

#[cfg(all(test, feature = "postgres", feature = "migrations"))]
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

#[cfg(all(test, feature = "mysql"))]
const MYSQL_V4_IDENTIFIER_ARTIFACT: &str =
    include_str!("../../migrations/v4/mysql/4000_identifier_contract.sql");

#[cfg(all(test, feature = "sqlite", feature = "migrations"))]
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

#[cfg(any(feature = "mysql", all(feature = "postgres", feature = "migrations")))]
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

#[cfg(all(feature = "sqlite", feature = "migrations"))]
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
