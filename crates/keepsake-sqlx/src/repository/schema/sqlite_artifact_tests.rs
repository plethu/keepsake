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
