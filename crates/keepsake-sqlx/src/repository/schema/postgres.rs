//! `PostgreSQL` schema verification.

#[cfg(all(feature = "mysql", not(feature = "postgres")))]
use super::normalize_check_expression;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use super::{PG_CLEAN_ARTIFACT, PG_UPGRADE_ARTIFACT, artifact_check_expression};
#[cfg(feature = "postgres")]
use super::{
    RepositoryError, RepositoryResult, compact_sql, default_sql, mismatch,
    normalize_check_expression, normalize_sql,
};
#[cfg(feature = "postgres")]
use crate::repository::backend::KeepsakeSqlxBackend;

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

#[cfg(all(feature = "postgres", feature = "migrations"))]
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

#[cfg(all(feature = "postgres", feature = "migrations"))]
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
pub(super) fn mysql_catalog_check_matches(
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

#[cfg(all(feature = "postgres", feature = "migrations"))]
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
pub(in crate::repository) async fn postgres_upgrade_schema_check(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
    postgres_catalog_shape_check(pool, true).await
}

#[cfg(feature = "postgres")]
pub(in crate::repository) async fn postgres_runtime_schema_check(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
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
    if backend.as_deref() != Some(super::super::PostgresBackend::NAME) {
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
pub(in crate::repository) async fn postgres_clean_schema_preflight(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
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
pub(in crate::repository) async fn postgres_upgrade_schema_preflight(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
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
pub(in crate::repository) async fn postgres_schema_preflight(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
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
        Some(super::super::PostgresBackend::NAME) | None => Ok(()),
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: super::super::PostgresBackend::NAME,
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
            expected: super::super::PostgresBackend::NAME,
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
            expected: super::super::PostgresBackend::NAME,
            actual: "unmarked unknown migration history".to_owned(),
        })
    }
}
