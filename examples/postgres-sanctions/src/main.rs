//! Timed sanction example.

use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef, TenantId};
use keepsake_sqlx::{KeepsakeRepository, RepositoryError};
use sqlx::{PgPool, raw_sql};
use time::{Duration, OffsetDateTime};

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

async fn install_dovecote_schema(pool: &PgPool) -> Result<(), ExampleError> {
    let installed: bool =
        sqlx::query_scalar("SELECT to_regclass('public.dovecote_schema') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if !installed {
        // Fresh databases need the Dovecote schema before the first audited
        // write. Existing databases are checked below and are not rewritten.
        raw_sql(dovecote_sqlx_postgres::MIGRATIONS[0].sql())
            .execute(pool)
            .await?;
    }
    Ok(())
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
    install_dovecote_schema(&pool).await?;
    let repo = KeepsakeRepository::new(pool, "https://example.invalid/keepsake")?;
    repo.migrate().await?;
    repo.check_schema().await?;
    let tenant_id = TenantId::new("example-tenant")?;
    let scoped_repo = repo.for_tenant(tenant_id.clone());
    let now = OffsetDateTime::now_utc();
    let timed_repo = scoped_repo.at(now);

    timed_repo.upsert_relation_spec::<Mute24hSanction>().await?;

    let subject = SubjectRef::new("user", "user_123")?;
    let command = ApplyKeepsake::for_spec::<Mute24hSanction>(
        tenant_id,
        subject,
        now,
        CommandContext::new(ActorRef::new("system", "example")?),
    );
    let applied = scoped_repo.apply(&command).await?;

    println!("{}", applied.keepsake.id());
    Ok(())
}
