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
            artifact_check_expression(super::PG_V3_UPGRADE_ACTIVATE_ARTIFACT, &marker).is_some()
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
