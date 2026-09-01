use super::support::*;

#[tokio::test]
async fn sqlite_bounded_relation_reads_filter_in_the_database() -> TestResult<()> {
    backend_cases::bounded_relation_reads_filter_in_the_database::<SqliteHarness>().await
}

#[tokio::test]
async fn sqlite_identifier_contract_round_trips_case_and_unicode() -> TestResult<()> {
    backend_cases::identifier_contract_round_trips_case_and_unicode::<SqliteHarness>().await
}
