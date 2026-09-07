//! Minimal tag assignment example.

use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef, TenantId};
use keepsake_sqlx::{KeepsakeRepository, RepositoryError};
use sqlx::{PgPool, raw_sql};
use std::env;
use std::process::ExitCode;
use time::OffsetDateTime;

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    Env(#[from] env::VarError),

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
        for migration in dovecote_sqlx_postgres::MIGRATIONS {
            raw_sql(migration.sql()).execute(pool).await?;
        }
    }
    Ok(())
}

keepsake::relation_spec! {
    struct TrustedTag {
        id: 0x018f_0000_0000_7000_8000_0000_0000_0001;
        key: ("tag", "trusted");
        expiry(_at) => ExpiryPolicy::ManualOnly;
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    if run().await.is_ok() {
        ExitCode::SUCCESS
    } else {
        // Driver errors may contain connection input. Keep diagnostics safe;
        // the typed error remains available to an application-owned boundary.
        eprintln!("Keepsake example failed; check database configuration and schema");
        ExitCode::FAILURE
    }
}

async fn run() -> Result<(), ExampleError> {
    let database_url = env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    let repo = KeepsakeRepository::new(pool.clone(), "https://example.invalid/keepsake")?;
    repo.migrate().await?;
    install_dovecote_schema(&pool).await?;
    repo.check_schema().await?;
    let tenant_id = TenantId::new("example-tenant")?;
    let scoped_repo = repo.for_tenant(tenant_id.clone());
    let now = OffsetDateTime::now_utc();
    let timed_repo = scoped_repo.at(now);

    timed_repo.upsert_relation_spec::<TrustedTag>().await?;

    let subject = SubjectRef::new("account", "acct_123")?;
    let command = ApplyKeepsake::for_spec::<TrustedTag>(
        tenant_id,
        subject,
        now,
        CommandContext::new(ActorRef::new("system", "example")?),
    );
    let applied = scoped_repo.apply(&command).await?;

    println!("{}", applied.keepsake.id());
    Ok(())
}
