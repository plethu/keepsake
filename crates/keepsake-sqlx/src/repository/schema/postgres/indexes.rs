//! `PostgreSQL` index columns, uniqueness, and predicate checks.

#[cfg(feature = "migrations")]
use super::indexes_catalog::PG_INDEXES_CHECK_EXPECTED;
use crate::repository::schema::RepositoryResult;
use crate::repository::schema::compact_sql;
use crate::repository::schema::mismatch;

#[cfg(feature = "migrations")]
pub(super) async fn pg_indexes_check(
    pool: &sqlx::PgPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    use sqlx::Row;
    let expected = PG_INDEXES_CHECK_EXPECTED;
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

fn compact_pg_index(definition: &str) -> String {
    compact_sql(definition)
        .replace("public.", "")
        .replace("usingbtree", "")
        .replace("::text", "")
}

pub(super) async fn postgres_v3_indexes_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
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
