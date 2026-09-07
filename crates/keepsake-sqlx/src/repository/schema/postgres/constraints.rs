//! `PostgreSQL` catalog constraints and tenant isolation checks.

#[cfg(all(feature = "postgres", feature = "migrations"))]
use super::constraints_catalog::PG_CONSTRAINTS_CHECK_EXPECTED_CLEAN;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use super::constraints_catalog::PG_CONSTRAINTS_CHECK_EXPECTED_UPGRADE;
#[cfg(feature = "postgres")]
use super::constraints_catalog::POSTGRES_V3_CONSTRAINTS_CHECK_EXPECTED;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use crate::repository::schema::PG_CLEAN_ARTIFACT;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use crate::repository::schema::PG_UPGRADE_ARTIFACT;
#[cfg(feature = "postgres")]
use crate::repository::schema::PG_V4_IDENTIFIER_ARTIFACT;
#[cfg(feature = "postgres")]
use crate::repository::schema::RepositoryError;
#[cfg(feature = "postgres")]
use crate::repository::schema::RepositoryResult;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use crate::repository::schema::artifact_check_expression;
#[cfg(feature = "postgres")]
use crate::repository::schema::identifier_check_from_artifact;
#[cfg(feature = "postgres")]
use crate::repository::schema::identifier_check_matches;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use crate::repository::schema::mismatch;
use crate::repository::schema::normalize_check_expression;
#[cfg(feature = "postgres")]
use crate::repository::schema::normalize_sql;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgRow;

#[cfg(all(feature = "postgres", feature = "migrations"))]
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

#[cfg(all(feature = "postgres", feature = "migrations"))]
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
pub(in crate::repository::schema) fn mysql_catalog_check_matches(
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

#[cfg(all(feature = "postgres", feature = "migrations"))]
pub(super) async fn pg_constraints_check(
    pool: &sqlx::PgPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let expected: &[(&str, &str, &str)] = if activated_upgrade {
        PG_CONSTRAINTS_CHECK_EXPECTED_UPGRADE
    } else {
        PG_CONSTRAINTS_CHECK_EXPECTED_CLEAN
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
        let check_matches = kind == "c"
            && pg_expected_check_expression(activated_upgrade, &name).is_some_and(
                |expected_expression| {
                    pg_catalog_check_matches(
                        activated_upgrade,
                        &name,
                        &definition,
                        &expected_expression,
                    )
                },
            );
        let found = expected
            .iter()
            .any(|(expected_table, expected_kind, expected_def)| {
                if *expected_table != table || *expected_kind != kind {
                    return false;
                }

                if kind == "c" {
                    return name == *expected_def && check_matches;
                }
                normalize_sql(&definition) == normalize_sql(expected_def)
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
pub(super) async fn postgres_v3_constraints_check(
    pool: &sqlx::PgPool,
    identifier_contract: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;

    // Compare catalog definitions rather than only constraint names. Names
    // generated for composite unique constraints are PostgreSQL-version
    // details, while the tenant-leading columns and foreign-key pairs are the
    // actual isolation contract.
    let rows = sqlx::query(
        "select c.relname, x.contype::text as contype, x.conname, x.convalidated, pg_get_constraintdef(x.oid, true) as definition from pg_constraint x join pg_class c on c.oid = x.conrelid join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relname = any($1) and x.contype in ('p','u','f','c')",
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
    let expected_constraint_count = if identifier_contract { 24 } else { 20 };

    if rows.len() != expected_constraint_count {
        return Err(RepositoryError::BackendMismatch {
            expected: "complete Keepsake 3.0 PostgreSQL constraints",
            actual: "primary, foreign, unique, or check constraint count differs".to_owned(),
        });
    }

    let expected = POSTGRES_V3_CONSTRAINTS_CHECK_EXPECTED;

    for row in rows {
        let table: String = row.try_get("relname")?;
        let kind: String = row.try_get("contype")?;
        let name: String = row.try_get("conname")?;
        let definition: String = row.try_get("definition")?;
        let normalized = normalize_sql(&definition).replace("public.", "");
        let matches = match kind.as_str() {
            "c" => pg_tenant_check_matches(&row, &table, &name, &definition, identifier_contract)?,

            _ => expected
                .iter()
                .any(|(expected_table, expected_kind, definition)| {
                    *expected_table == table
                        && *expected_kind == kind
                        && normalize_sql(definition) == normalized
                }),
        };

        if matches {
            continue;
        }

        return Err(RepositoryError::BackendMismatch {
            expected: "complete Keepsake 3.0 PostgreSQL constraints",
            actual: format!("unexpected or altered constraint {table}.{name}"),
        });
    }

    Ok(())
}

#[cfg(feature = "postgres")]
fn pg_tenant_check_matches(
    row: &PgRow,
    table: &str,
    name: &str,
    definition: &str,
    identifier_contract: bool,
) -> RepositoryResult<bool> {
    use sqlx::Row;
    Ok(match name {
        "keepsake_relation_definitions_tenant_size"
        | "keepsake_relation_definitions_tenant_nonempty"
        | "keepsakes_tenant_size"
        | "keepsakes_tenant_nonempty"
        | "keepsake_fulfillment_counter_tenant_size"
        | "keepsake_fulfillment_counter_tenant_nonempty"
        | "keepsake_fulfillment_checklist_tenant_size"
        | "keepsake_fulfillment_checklist_tenant_nonempty" => match name {
            "keepsake_relation_definitions_tenant_nonempty"
            | "keepsakes_tenant_nonempty"
            | "keepsake_fulfillment_counter_tenant_nonempty"
            | "keepsake_fulfillment_checklist_tenant_nonempty" => {
                normalize_check_expression(definition).contains("octet_length(tenant_id)>0")
            }

            _ => normalize_check_expression(definition).contains("octet_length(tenant_id)<=255"),
        },

        "keepsake_relation_definitions_identifier_contract"
        | "keepsakes_identifier_contract"
        | "keepsake_fulfillment_counter_identifier_contract"
        | "keepsake_fulfillment_checklist_identifier_contract" => {
            identifier_contract
                && row.try_get::<bool, _>("convalidated")?
                && identifier_check_from_artifact(PG_V4_IDENTIFIER_ARTIFACT, table, name)
                    .is_some_and(|expected| identifier_check_matches(definition, &expected))
        }

        "keepsakes_state_check" => normalize_check_expression(definition)
            .contains("state=any(array['applied','revoked','expired'])"),

        "keepsakes_expiry_policy_projection" => {
            let check = normalize_check_expression(definition);
            check.contains(
                "(expiry_policy->>'type')=any(array['manual_only','at','when_fulfilled'])",
            ) && check.contains("expires_atisnotnull")
                && check.contains("expires_atisnull")
        }

        "keepsakes_lifecycle_timestamps" => {
            let check = normalize_check_expression(definition);
            check.contains("state='applied'andrevoked_atisnullandfulfilled_atisnull")
                && check.contains("state='revoked'andrevoked_atisnotnullandfulfilled_atisnull")
                && check.contains("state='expired'andrevoked_atisnull")
                && check.contains("(expiry_policy->>'type')='at'")
                && check.contains("(expiry_policy->>'type')='when_fulfilled'")
        }

        _ => false,
    })
}
