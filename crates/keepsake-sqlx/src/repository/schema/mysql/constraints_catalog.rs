//! Expected constraints in published schema tracks.

#[cfg(feature = "migrations")]
pub(super) const MYSQL_CONSTRAINTS_CHECK_TABLES_CLEAN: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

#[cfg(feature = "migrations")]
pub(super) const MYSQL_CONSTRAINTS_CHECK_TABLES_UPGRADE: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
    "keepsake_audit_events",
    "keepsake_audit_context_attributes",
    "keepsake_audit_outbox",
];

#[cfg(feature = "migrations")]
pub(super) const MYSQL_CONSTRAINTS_CHECK_EXPECTED_CLEAN: &[(&str, &str, &str)] = &[
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
];

#[cfg(feature = "migrations")]
pub(super) const MYSQL_CONSTRAINTS_CHECK_EXPECTED_UPGRADE: &[(&str, &str, &str)] = &[
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
];

#[cfg(feature = "migrations")]
pub(super) const MYSQL_CONSTRAINTS_CHECK_FOREIGN_KEYS: &[(&str, &str, &str, &str, &str)] = &[
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

#[cfg(feature = "migrations")]
pub(super) const MYSQL_CONSTRAINTS_CHECK_EXPECTED_ACTIONS: &[(&str, &str, &str)] = &[
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

pub(super) const MYSQL_V3_FOREIGN_KEYS_CHECK_EXPECTED: &[TenantForeignKey] = &[
    TenantForeignKey {
        table: "keepsakes",
        name: "keepsakes_relation_fk",
        columns: &[("tenant_id", "tenant_id"), ("relation_id", "id")],
        delete_rule: "NO ACTION",
    },
    TenantForeignKey {
        table: "keepsake_fulfillment_counters",
        name: "keepsake_fulfillment_counter_keepsake_fk",
        columns: &[("tenant_id", "tenant_id"), ("keepsake_id", "id")],
        delete_rule: "CASCADE",
    },
    TenantForeignKey {
        table: "keepsake_fulfillment_checklist",
        name: "keepsake_fulfillment_checklist_keepsake_fk",
        columns: &[("tenant_id", "tenant_id"), ("keepsake_id", "id")],
        delete_rule: "CASCADE",
    },
];

pub(super) const MYSQL_V3_CONSTRAINTS_CHECK_TABLES: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

pub(super) const MYSQL_V3_CONSTRAINTS_CHECK_EXPECTED_CHECKS: &[(&str, &str, &str)] = &[
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

pub(super) const MYSQL_V3_CONSTRAINTS_CHECK_TENANT_CHECKS: &[(&str, &str)] = &[
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

pub(super) const MYSQL_V3_CONSTRAINTS_CHECK_IDENTIFIER_CHECKS: &[&str] = &[
    "keepsake_relation_definitions_identifier_contract",
    "keepsakes_identifier_contract",
    "keepsake_fulfillment_counter_identifier_contract",
    "keepsake_fulfillment_checklist_identifier_contract",
];

pub(super) struct TenantForeignKey {
    pub(super) table: &'static str,
    pub(super) name: &'static str,
    pub(super) columns: &'static [(&'static str, &'static str)],
    pub(super) delete_rule: &'static str,
}
