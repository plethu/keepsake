use super::support::*;

#[tokio::test]
#[ignore = "requires docker mysql; run `make test-db`"]
async fn mysql_bounded_relation_reads_filter_in_the_database() -> TestResult<()> {
    backend_cases::bounded_relation_reads_filter_in_the_database::<MySqlHarness>().await
}
