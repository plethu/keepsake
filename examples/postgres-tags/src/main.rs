//! Minimal tag assignment example.

use chrono::Utc;
use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef};
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
    struct TrustedTag {
        id: 0x018f_0000_0000_7000_8000_0000_0000_0001;
        key: ("tag", "trusted");
        expiry(_at) => ExpiryPolicy::ManualOnly;
    }
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool);
    repo.migrate().await?;
    let now = Utc::now();
    let timed_repo = repo.at(now);

    timed_repo.upsert_relation_spec::<TrustedTag>().await?;

    let subject = SubjectRef::new("account", "acct_123")?;
    let command = ApplyKeepsake::for_spec::<TrustedTag>(
        subject,
        now,
        CommandContext::new(ActorRef::new("system", "example")?),
    );
    let applied = repo.apply(&command).await?;

    println!("{}", applied.keepsake.id());
    Ok(())
}
