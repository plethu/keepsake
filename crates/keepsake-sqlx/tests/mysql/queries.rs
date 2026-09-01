use super::support::*;

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_bounded_relation_reads_filter_in_the_database() -> TestResult<()> {
    backend_cases::bounded_relation_reads_filter_in_the_database::<MySqlHarness>().await
}

#[tokio::test]
#[ignore = "requires docker mysql; run `mise run test-db`"]
async fn mysql_identifier_contract_round_trips_case_and_unicode() -> TestResult<()> {
    backend_cases::identifier_contract_round_trips_case_and_unicode::<MySqlHarness>().await
}
