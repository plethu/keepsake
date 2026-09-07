#[cfg(feature = "migrations")]
use super::artifact_check_expression;
use super::{normalize_check_expression, normalize_mysql_generated_expression};

#[test]
fn check_normalization_preserves_outer_close_after_in_list() {
    let source = "check(coalesce(x in ('a', 'b'), false))";
    assert_eq!(
        normalize_check_expression(source),
        "coalesce(x=any(array['a','b']),false)"
    );
}

#[test]
#[cfg(feature = "migrations")]
fn all_mysql_check_artifacts_are_extractable() {
    assert!(
        artifact_check_expression(
            super::MYSQL_CLEAN_ARTIFACT,
            "constraint keepsakes_state_check check"
        )
        .is_some()
    );
    for marker in [
        "constraint keepsakes_state_check check",
        "constraint keepsakes_expiry_policy_projection check",
        "constraint keepsakes_lifecycle_timestamps check",
    ] {
        assert!(artifact_check_expression(super::MYSQL_V3_CLEAN_ARTIFACT, marker).is_some());
    }
    assert!(
        artifact_check_expression(
            super::MYSQL_UPGRADE_ARTIFACT,
            "state varchar(16) not null check"
        )
        .is_some()
    );
    for marker in [
        "constraint keepsakes_expiry_policy_projection check",
        "constraint keepsakes_lifecycle_timestamps check",
    ] {
        assert!(artifact_check_expression(super::MYSQL_CLEAN_ARTIFACT, marker).is_some());
        assert!(artifact_check_expression(super::MYSQL_UPGRADE_ARTIFACT, marker).is_some());
    }
}

#[test]
fn v3_mysql_identifier_shape_is_mariadb_compatible() {
    let clean = super::normalize_sql(super::MYSQL_V3_CLEAN_ARTIFACT);
    assert!(!clean.contains(" id char(36)"));
    assert!(!clean.contains(" relation_id char(36)"));
    assert!(!clean.contains(" keepsake_id char(36)"));
    assert_eq!(clean.matches("varchar(36)").count(), 6);

    let activation = super::normalize_sql(super::MYSQL_V3_UPGRADE_ACTIVATE_ARTIFACT);
    for fragment in [
        "modify id varchar(36) not null",
        "modify id varchar(36) not null, modify relation_id varchar(36) not null",
        "modify active_relation_key varchar(36) generated always",
        "modify keepsake_id varchar(36) not null",
    ] {
        assert!(
            activation.contains(fragment),
            "missing activation fragment: {fragment}"
        );
    }
}

#[test]
fn v4_mysql_identifier_contract_is_explicit_and_binary() {
    let contract = super::normalize_sql(super::MYSQL_V4_IDENTIFIER_ARTIFACT);
    assert_eq!(contract.matches("collate utf8mb4_bin").count(), 10);
    for marker in [
        "keepsake_relation_definitions_identifier_contract",
        "keepsakes_identifier_contract",
        "keepsake_fulfillment_counter_identifier_contract",
        "keepsake_fulfillment_checklist_identifier_contract",
    ] {
        assert!(contract.contains(marker));
    }
}

#[test]
fn generated_case_deparser_forms_compare_equal() {
    assert_eq!(
        normalize_mysql_generated_expression(
            "(case when (`state` = _utf8mb4'applied') then `relation_id` else NULL end)"
        ),
        normalize_mysql_generated_expression("case when state = 'applied' then relation_id end")
    );
    assert_eq!(
        normalize_mysql_generated_expression(
            r"(case when (`state` = _utf8mb4\'applied\') then `relation_id` else NULL end)"
        ),
        normalize_mysql_generated_expression("case when state = 'applied' then relation_id end")
    );
}

#[test]
fn fulfillment_generated_case_keeps_predicate_semantics() {
    assert_eq!(
        normalize_mysql_generated_expression(
            "(case when (`state` = _utf8mb4'applied' and json_unquote(json_extract(`expiry_policy`, '$.type')) = _utf8mb4'when_fulfilled') then 1 else NULL end)"
        ),
        normalize_mysql_generated_expression(
            "case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end"
        )
    );
    assert_eq!(
        normalize_mysql_generated_expression(
            "case when (state = 'applied') and (json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled') then 1 else NULL end"
        ),
        normalize_mysql_generated_expression(
            "case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end"
        )
    );
}

#[test]
fn generated_null_default_is_absent_but_quoted_null_is_not() {
    assert!(super::mysql_default_matches(Some("NULL"), None));
    assert!(!super::mysql_default_matches(Some("'NULL'"), None));
    assert!(!super::mysql_is_generated_extra("DEFAULT_GENERATED"));
    assert!(super::mysql_is_generated_extra("STORED GENERATED"));
}

#[test]
fn v3_referential_actions_reject_update_cascade() {
    assert!(super::mysql_v3_referential_action_matches(
        "NO ACTION",
        "NO ACTION"
    ));
    assert!(super::mysql_v3_referential_action_matches(
        "NO ACTION",
        "RESTRICT"
    ));
    assert!(!super::mysql_v3_referential_action_matches(
        "NO ACTION",
        "CASCADE"
    ));
    assert!(!super::mysql_v3_referential_action_matches(
        "CASCADE",
        "NO ACTION"
    ));
}
