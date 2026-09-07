//! Expected indexes in published schema tracks.

pub(super) const MYSQL_V3_INDEXES_CHECK_EXPECTED: &[(&str, &str, bool, &[&str])] = &[
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

#[cfg(feature = "migrations")]
pub(super) const LEGACY_INDEXES: &[(&str, &str, bool, &[&str])] = &[
    ("PRIMARY", "keepsake_schema_metadata", true, &["key"]),
    ("PRIMARY", "keepsake_relation_definitions", true, &["id"]),
    (
        "keepsake_relation_definitions_kind_key_unique",
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

#[cfg(feature = "migrations")]
pub(super) const LEGACY_AUDIT_INDEXES: &[(&str, &str, bool, &[&str])] = &[
    ("PRIMARY", "keepsake_audit_events", true, &["id"]),
    (
        "PRIMARY",
        "keepsake_audit_context_attributes",
        true,
        &["audit_event_id", "key"],
    ),
    ("PRIMARY", "keepsake_audit_outbox", true, &["id"]),
    (
        "keepsake_audit_by_keepsake",
        "keepsake_audit_events",
        false,
        &["keepsake_id", "occurred_at", "id"],
    ),
    (
        "keepsake_audit_by_relation",
        "keepsake_audit_events",
        false,
        &["relation_id", "occurred_at", "id"],
    ),
    (
        "keepsake_audit_context_attribute_lookup",
        "keepsake_audit_context_attributes",
        false,
        &["key", "value", "audit_event_id"],
    ),
    (
        "keepsake_audit_outbox_export",
        "keepsake_audit_outbox",
        false,
        &["id"],
    ),
    (
        "keepsake_audit_outbox_claim",
        "keepsake_audit_outbox",
        false,
        &["delivered_at", "claimed_until", "id"],
    ),
];
