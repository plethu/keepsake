mod expiry;
#[cfg(feature = "fulfillment-counters")]
mod fulfillment;
mod lifecycle;
mod observation;
mod query;
mod receipt;
mod relation;
mod rows;

pub(super) async fn require_repeatable_read(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> super::RepositoryResult<()> {
    let isolation: String = sqlx::query_scalar("select @@transaction_isolation")
        .fetch_one(&mut **tx)
        .await?;
    if !isolation.eq_ignore_ascii_case("REPEATABLE-READ") {
        return Err(super::RepositoryError::UnsupportedIsolation);
    }
    Ok(())
}
