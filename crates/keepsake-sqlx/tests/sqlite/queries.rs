use super::support::*;

#[tokio::test]
async fn sqlite_bounded_relation_reads_filter_in_the_database() -> TestResult<()> {
    backend_cases::bounded_relation_reads_filter_in_the_database::<SqliteHarness>().await
}
