use super::support::*;
use keepsake::{
    ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, LifecycleState, RelationDefinition,
    RelationKey, RevokeBySubject, SubjectRef,
};
use keepsake_sqlx::RepositoryError;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable MySQL database"]
async fn caller_transaction_replay_fencing_and_deadline() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let tenant = MySqlHarness::tenant()?;
    let at = ts("2026-01-01T00:00:00Z")?;
    let deadline = ts("2026-01-01T00:01:00.123456Z")?;
    let relation = RelationDefinition::enabled(
        tenant.clone(),
        Uuid::now_v7(),
        RelationKey::new("restriction", "admission")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, at).await?;
    let subject = SubjectRef::new("directed-pair", "a:b")?;
    let command = ApplyKeepsake::new(
        tenant,
        subject.clone(),
        relation.id,
        at,
        CommandContext::new(ActorRef::new("account", "a")?),
    )
    .with_expiry(ExpiryPolicy::At {
        timestamp: ts("2026-01-01T00:01:00.123456789Z")?,
    });
    sqlx::query("create table transaction_business (id integer primary key)")
        .execute(&pool)
        .await?;
    let mut tx = pool.begin().await?;
    let absent = repo
        .observe_in_transaction(&mut tx, &subject, relation.id)
        .await?;
    assert!(absent.history().is_empty());
    let applied = repo
        .apply_if_current_in_transaction(&mut tx, &absent, &command)
        .await?;
    assert!(!applied.replayed);
    sqlx::query("insert into transaction_business (id) values (1)")
        .execute(&mut *tx)
        .await?;
    tx.rollback().await?;
    for query in [
        "select count(*) from keepsakes",
        "select count(*) from dovecote_events",
        "select count(*) from transaction_business",
    ] {
        let count: i64 = sqlx::query_scalar(query).fetch_one(&pool).await?;
        assert_eq!(count, 0);
    }

    let mut tx = pool.begin().await?;
    let applied = repo
        .apply_if_current_in_transaction(&mut tx, &absent, &command)
        .await?;
    sqlx::query("insert into transaction_business (id) values (1)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let mut tx = pool.begin().await?;
    let replay = repo
        .apply_if_current_in_transaction(&mut tx, &absent, &command)
        .await?;
    assert!(replay.replayed);
    assert_eq!(applied.keepsake, replay.keepsake);
    assert_eq!(applied.duplicate_prevented, replay.duplicate_prevented);
    let active = repo
        .observe_in_transaction(&mut tx, &subject, relation.id)
        .await?;
    assert_eq!(active.history().len(), 1);
    let effective = active
        .active_relation()?
        .ok_or(RepositoryError::MissingActiveAssignment)?;
    assert_eq!(
        keepsake::effective_state(
            keepsake::ObservationTime::Authoritative(deadline),
            &effective,
            None
        ),
        Ok(LifecycleState::Expired)
    );
    assert_eq!(effective.keepsake().state(), LifecycleState::Applied);
    assert!(matches!(
        repo.revalidate_in_transaction(&mut tx, &absent).await,
        Err(RepositoryError::StaleObservation)
    ));
    tx.rollback().await?;

    reject_scope_substitution(&repo, &pool, &command, &absent).await?;
    verify_fulfillment_evidence_scope(&repo, &pool, &command).await?;
    verify_terminal_receipts(&repo, &pool, &command, &active, &applied, &absent).await?;
    verify_reconciliation_transaction(&repo, &pool, relation.id, deadline).await
}

async fn verify_terminal_receipts(
    repo: &keepsake_sqlx::TenantSqlxKeepsakeRepository<'_, keepsake_sqlx::MySqlBackend>,
    pool: &sqlx::Pool<sqlx::MySql>,
    command: &ApplyKeepsake,
    active: &keepsake_sqlx::RelationObservation,
    applied: &keepsake_sqlx::AppliedKeepsake,
    absent: &keepsake_sqlx::RelationObservation,
) -> TestResult<()> {
    let mut changed = command.clone();
    changed
        .metadata
        .insert("private".to_owned(), "must not leak".to_owned());
    assert!(matches!(
        repo.apply(&changed).await,
        Err(RepositoryError::CommandConflict)
    ));
    let revoke = RevokeBySubject::new(
        command.tenant_id.clone(),
        command.subject.clone(),
        command.relation_id,
        command.at,
        command.context.clone(),
    );
    let mut tx = pool.begin().await?;
    let first_revoke = repo
        .revoke_if_current_in_transaction(&mut tx, active, &revoke)
        .await?;
    assert!(!first_revoke.replayed);
    tx.commit().await?;
    let mut tx = pool.begin().await?;
    let revoked = repo
        .observe_in_transaction(&mut tx, &command.subject, command.relation_id)
        .await?;
    assert!(revoked.active_relation()?.is_none());
    assert!(matches!(
        repo.revalidate_in_transaction(&mut tx, absent).await,
        Err(RepositoryError::StaleObservation)
    ));
    tx.rollback().await?;
    let mut reapply = command.clone();
    reapply.id = Uuid::now_v7();
    reapply.audit_id = keepsake::AuditEventId::new();
    let fresh = repo.apply(&reapply).await?;
    assert_ne!(fresh.keepsake.id(), applied.keepsake.id());
    let mut tx = pool.begin().await?;
    let replay_revoke = repo
        .revoke_if_current_in_transaction(&mut tx, active, &revoke)
        .await?;
    assert!(replay_revoke.replayed);
    assert_eq!(replay_revoke.keepsake_id, applied.keepsake.id());
    let current = repo
        .observe_in_transaction(&mut tx, &command.subject, command.relation_id)
        .await?;
    assert_eq!(
        current.active_relation()?.map(|row| row.keepsake().id()),
        Some(fresh.keepsake.id())
    );
    assert!(matches!(
        repo.revalidate_in_transaction(&mut tx, active).await,
        Err(RepositoryError::StaleObservation)
    ));
    tx.rollback().await?;
    let terminal_replay = repo.apply(command).await?;
    assert!(terminal_replay.replayed);
    assert_eq!(terminal_replay.keepsake, applied.keepsake);
    let count: i64 = sqlx::query_scalar("select count(*) from dovecote_events")
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 3);
    sqlx::query("drop table transaction_business")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable MySQL database"]
async fn absent_observation_fences_a_concurrent_apply() -> TestResult<()> {
    let (repo, pool) = MySqlHarness::repo().await?;
    let tenant = MySqlHarness::tenant()?;
    let at = ts("2026-01-01T00:00:00Z")?;
    let relation = RelationDefinition::enabled(
        tenant.clone(),
        Uuid::now_v7(),
        RelationKey::new("block", "social")?,
        ExpiryPolicy::ManualOnly,
    )?;
    repo.upsert_relation(&relation, at).await?;
    let subject = SubjectRef::new("directed-pair", "a:b")?;
    let command = ApplyKeepsake::new(
        tenant,
        subject.clone(),
        relation.id,
        at,
        CommandContext::new(ActorRef::new("account", "a")?),
    );
    let mut protected = pool.begin().await?;
    let absent = repo
        .observe_in_transaction(&mut protected, &subject, relation.id)
        .await?;
    assert!(absent.active_relation()?.is_none());
    let mut concurrent = pool.begin().await?;
    sqlx::query("set session innodb_lock_wait_timeout = 1")
        .execute(&mut *concurrent)
        .await?;
    let blocked = repo.apply_in_transaction(&mut concurrent, &command).await;
    assert!(blocked.is_err());
    concurrent.rollback().await?;
    repo.revalidate_in_transaction(&mut protected, &absent)
        .await?;
    protected.commit().await?;
    repo.apply(&command).await?;
    let mut stale = pool.begin().await?;
    assert!(matches!(
        repo.revalidate_in_transaction(&mut stale, &absent).await,
        Err(RepositoryError::StaleObservation)
    ));
    stale.rollback().await?;
    let mut connection = pool.acquire().await?;
    sqlx::query("set session transaction isolation level read committed")
        .execute(&mut *connection)
        .await?;
    let mut unsupported = sqlx::Connection::begin(&mut *connection).await?;
    assert!(matches!(
        repo.observe_in_transaction(&mut unsupported, &subject, relation.id)
            .await,
        Err(RepositoryError::UnsupportedIsolation)
    ));
    unsupported.rollback().await?;
    sqlx::query("set session transaction isolation level repeatable read")
        .execute(&mut *connection)
        .await?;

    Ok(())
}

async fn reject_scope_substitution(
    repo: &keepsake_sqlx::TenantSqlxKeepsakeRepository<'_, keepsake_sqlx::MySqlBackend>,
    pool: &sqlx::Pool<sqlx::MySql>,
    command: &ApplyKeepsake,
    expected: &keepsake_sqlx::RelationObservation,
) -> TestResult<()> {
    let mut other_direction = command.clone();
    other_direction.subject = SubjectRef::new("directed-pair", "b:a")?;
    let mut other_relation = command.clone();
    other_relation.relation_id = Uuid::now_v7();
    let mut other_tenant = command.clone();
    other_tenant.tenant_id = keepsake::TenantId::new("other-tenant")?;
    for changed in [other_direction, other_relation, other_tenant] {
        let mut tx = pool.begin().await?;
        assert!(matches!(
            repo.apply_if_current_in_transaction(&mut tx, expected, &changed)
                .await,
            Err(RepositoryError::StaleObservation)
        ));
        tx.rollback().await?;
    }
    Ok(())
}

async fn verify_reconciliation_transaction(
    repo: &keepsake_sqlx::TenantSqlxKeepsakeRepository<'_, keepsake_sqlx::MySqlBackend>,
    pool: &sqlx::Pool<sqlx::MySql>,
    relation_id: Uuid,
    deadline: time::OffsetDateTime,
) -> TestResult<()> {
    let mut tx = pool.begin().await?;
    assert_eq!(
        repo.expire_due_timed_for_relation_in_transaction(&mut tx, relation_id, deadline, 10)
            .await?
            .len(),
        1
    );
    let count: i64 = sqlx::query_scalar("select count(*) from dovecote_events")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 4);
    tx.rollback().await?;
    let count: i64 = sqlx::query_scalar("select count(*) from dovecote_events")
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 3);
    let mut tx = pool.begin().await?;
    assert_eq!(
        repo.expire_due_timed_for_relation_in_transaction(&mut tx, relation_id, deadline, 10)
            .await?
            .len(),
        1
    );
    tx.commit().await?;
    assert_eq!(repo.expire_due_timed(deadline, 10).await?, 0);
    let count: i64 = sqlx::query_scalar("select count(*) from dovecote_events")
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 4);
    Ok(())
}

async fn verify_fulfillment_evidence_scope(
    repo: &keepsake_sqlx::TenantSqlxKeepsakeRepository<'_, keepsake_sqlx::MySqlBackend>,
    pool: &sqlx::Pool<sqlx::MySql>,
    command: &ApplyKeepsake,
) -> TestResult<()> {
    let mut tx = pool.begin().await?;
    repo.observe_in_transaction(&mut tx, &command.subject, command.relation_id)
        .await?;
    let evidence = repo
        .fulfillment_snapshot_in_transaction(&mut tx, command.id)
        .await?;
    assert_eq!(evidence.tenant_id(), &command.tenant_id);
    assert_eq!(evidence.keepsake_id(), command.id);
    assert!(evidence.snapshot().counters.is_empty());
    assert!(evidence.snapshot().checklist.is_empty());
    tx.rollback().await?;
    Ok(())
}
