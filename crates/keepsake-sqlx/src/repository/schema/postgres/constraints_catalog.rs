//! Expected constraints in published schema tracks.

#[cfg(feature = "migrations")]
pub(super) const PG_CONSTRAINTS_CHECK_EXPECTED_CLEAN: &[(&str, &str, &str)] = &[
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
];

#[cfg(feature = "migrations")]
pub(super) const PG_CONSTRAINTS_CHECK_EXPECTED_UPGRADE: &[(&str, &str, &str)] = &[
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
];

pub(super) const POSTGRES_V3_CONSTRAINTS_CHECK_EXPECTED: &[(&str, &str, &str)] = &[
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
