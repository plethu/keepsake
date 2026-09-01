//! `SQLite` schema verification.

#[cfg(feature = "migrations")]
use super::{
    CLEAN_INDEXES, CLEAN_TABLES, CLEAN_TRIGGERS, IDENTIFIER_SCAN_BATCH_SIZE, PERSISTED_IDENTIFIERS,
    SQLITE_CLEAN_ARTIFACT, SQLITE_UPGRADE_ARTIFACT, UPGRADE_INDEXES, UPGRADE_TABLES,
    UPGRADE_TRIGGERS, artifact_object_sql, normalize_sql, persisted_identifier_type_mismatch,
    validate_persisted_identifier_bytes,
};
use super::{RepositoryError, RepositoryResult, compact_sql, mismatch};
use crate::repository::backend::KeepsakeSqlxBackend;

#[cfg(all(feature = "sqlite", feature = "migrations"))]
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
pub(in crate::repository) async fn sqlite_upgrade_schema_check(
    pool: &sqlx::SqlitePool,
) -> RepositoryResult<()> {
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
async fn sqlite_v4_domain_shape_check(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    sqlite_v3_domain_shape_check(pool).await?;
    for trigger in [
        "keepsake_relation_definitions_identifier_contract_insert",
        "keepsake_relation_definitions_identifier_contract_update",
        "keepsakes_identifier_contract_insert",
        "keepsakes_identifier_contract_update",
        "keepsake_fulfillment_counters_identifier_contract_insert",
        "keepsake_fulfillment_counters_identifier_contract_update",
        "keepsake_fulfillment_checklist_identifier_contract_insert",
        "keepsake_fulfillment_checklist_identifier_contract_update",
    ] {
        let definition = sqlx::query_scalar::<_, Option<String>>(
            "select sql from sqlite_master where type = 'trigger' and name = ?",
        )
        .bind(trigger)
        .fetch_optional(pool)
        .await?
        .flatten()
        .ok_or_else(|| mismatch(format!("missing v4 identifier trigger {trigger}")))?;
        let compact = compact_sql(&definition);
        if !compact.contains("length(cast(new.")
            || !compact.contains("asblob))<=191")
            || !compact.contains("trim(new.")
            || !compact.contains("raise(abort,'keepsake_identifier_contract')")
        {
            return Err(mismatch(format!(
                "v4 identifier trigger {trigger} definition differs"
            )));
        }
    }
    Ok(())
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn sqlite_v4_identifier_preflight(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    // SQLite's declared TEXT affinity does not prevent a legacy row from
    // carrying a BLOB, integer, real, or NULL. Preserve the raw bytes for
    // text/BLOB values, reject non-text values explicitly, and then apply the
    // exact Rust identifier contract (including Unicode edge whitespace,
    // controls, and noncharacters).
    for identifier in PERSISTED_IDENTIFIERS {
        sqlite_scan_identifier(pool, *identifier).await?;
    }
    Ok(())
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
async fn sqlite_scan_identifier(
    pool: &sqlx::SqlitePool,
    identifier: super::PersistedIdentifier,
) -> RepositoryResult<()> {
    let query = format!(
        "select rowid as __keepsake_row, typeof(\"{}\") as __keepsake_type, case when typeof(\"{}\") in ('text', 'blob') then cast(\"{}\" as blob) end as __keepsake_value from \"{}\" order by rowid limit {IDENTIFIER_SCAN_BATCH_SIZE} offset ?",
        identifier.column, identifier.column, identifier.column, identifier.table
    );
    let mut offset: i64 = 0;

    loop {
        let rows = sqlx::query(sqlx::AssertSqlSafe(query.clone()))
            .bind(offset)
            .fetch_all(pool)
            .await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            sqlite_validate_identifier_row(identifier, &row)?;
        }
        offset = offset
            .checked_add(IDENTIFIER_SCAN_BATCH_SIZE)
            .ok_or_else(|| mismatch("identifier preflight row offset overflow"))?;
    }

    Ok(())
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
fn sqlite_validate_identifier_row(
    identifier: super::PersistedIdentifier,
    row: &sqlx::sqlite::SqliteRow,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let row_locator: i64 = row.try_get("__keepsake_row")?;
    let value_type: String = row.try_get("__keepsake_type")?;
    let value: Option<Vec<u8>> = row.try_get("__keepsake_value")?;
    let row_label = row_locator.to_string();
    let Some(value) = value else {
        return Err(persisted_identifier_type_mismatch(
            identifier,
            row_label,
            &value_type,
        ));
    };

    if value_type != "text" {
        if std::str::from_utf8(&value).is_err() {
            return validate_persisted_identifier_bytes(identifier, row_label, &value);
        }

        return Err(persisted_identifier_type_mismatch(
            identifier,
            row_label,
            &value_type,
        ));
    }

    validate_persisted_identifier_bytes(identifier, row_label, &value)
}

#[cfg(feature = "sqlite")]
pub(in crate::repository) async fn sqlite_runtime_schema_check(
    pool: &sqlx::SqlitePool,
) -> RepositoryResult<()> {
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
    if backend.as_deref() != Some(super::super::SqliteBackend::NAME) {
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
    if track.as_deref() == Some("4") {
        sqlite_v4_domain_shape_check(pool).await?;
        return Ok(());
    }

    if track.as_deref() == Some("3") {
        return Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is still on the 3.0 API track; run migrate to activate the 4.0 schema"
                .to_owned(),
        });
    }

    if track.as_deref() == Some("2") {
        return Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is still on the 2.0 API track; run the explicit tenant upgrade route"
                .to_owned(),
        });
    }
    Err(RepositoryError::BackendMismatch {
        expected: "4.0 active schema",
        actual: "schema is not activated for the 4.0 API".to_owned(),
    })
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "migrations")]
pub(in crate::repository) async fn sqlite_clean_schema_preflight(
    pool: &sqlx::SqlitePool,
) -> RepositoryResult<()> {
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
        Some("4") => sqlite_v4_domain_shape_check(pool).await,
        Some("3") => {
            sqlite_v3_domain_shape_check(pool).await?;
            sqlite_v4_identifier_preflight(pool).await
        }
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
pub(in crate::repository) async fn sqlite_upgrade_schema_preflight(
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
pub(in crate::repository) async fn sqlite_schema_preflight(
    pool: &sqlx::SqlitePool,
) -> RepositoryResult<()> {
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
            Some(super::super::SqliteBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: super::super::SqliteBackend::NAME,
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
            expected: super::super::SqliteBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}
