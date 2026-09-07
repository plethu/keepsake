use super::{artifact_check_expression, identifier_check_matches, strip_sql_outer_groups};

#[test]
fn identifier_matching_rejects_unbalanced_catalog_groups() {
    let expected = "check (octet_length(tenant_id) > 0 and tenant_id = trim(tenant_id))";
    assert!(identifier_check_matches(
        "check ((octet_length(tenant_id) > 0) and (tenant_id = trim(tenant_id)))",
        expected
    ));
    for invalid in [
        "check ((octet_length(tenant_id) > 0 and tenant_id = trim(tenant_id))",
        "check (octet_length(tenant_id) > 0 and tenant_id = trim(tenant_id)))",
    ] {
        assert!(!identifier_check_matches(invalid, expected));
    }
}

#[test]
fn check_groups_ignore_parentheses_inside_literals() {
    assert_eq!(
        artifact_check_expression(
            "constraint example check (value = ')(')",
            "constraint example check"
        ),
        Some("(value = ')(')".to_owned())
    );
    assert_eq!(strip_sql_outer_groups("((value = ')'))"), "value = ')'");
    assert_eq!(
        artifact_check_expression(
            "constraint example check ((value = 'unterminated)",
            "constraint example check"
        ),
        None
    );
}
