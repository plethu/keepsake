//! Timed sanction example.

use chrono::{Duration, Utc};
use keepsake::{ExpiryPolicy, SubjectRef};
use keepsake_sqlx::{KeepsakeRepository, RepositoryError};
use sqlx::PgPool;

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    Env(#[from] std::env::VarError),

    #[error(transparent)]
    Keepsake(#[from] keepsake::KeepsakeError),

    #[error(transparent)]
    Repository(#[from] RepositoryError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

keepsake::relation_spec! {
    struct Mute24hSanction {
        id: 0x018f_0000_0000_7000_8000_0000_0000_0002;
        key: ("sanction", "mute_24h");
        expiry(at) => ExpiryPolicy::At {
            timestamp: at + Duration::hours(24),
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool);
    repo.migrate().await?;
    let now = Utc::now();
    let repo = repo.at(now);

    repo.upsert_relation_spec::<Mute24hSanction>().await?;

    let subject = SubjectRef::new("user", "user_123")?;
    let applied = repo
        .apply_spec_without_metadata::<Mute24hSanction>(&subject)
        .await?;

    println!("{}", applied.keepsake.id());
    Ok(())
}
