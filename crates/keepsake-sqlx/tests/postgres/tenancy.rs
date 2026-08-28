use super::support::*;

#[tokio::test]
#[ignore = "requires docker postgres; run `mise run test-db`"]
async fn tenant_handles_isolate_same_relation_and_keepsake_ids() -> TestResult<()> {
    let root = repo().await?;
    let tenant_a = TenantId::new("tenant-a")?;
    let tenant_b = TenantId::new("tenant-b")?;
    let repo_a = root.for_tenant(tenant_a.clone());
    let repo_b = root.for_tenant(tenant_b.clone());
    let relation_id = Uuid::now_v7();
    let relation_key = RelationKey::new("tag", "same-key")?;
    let at = ts("2026-01-01T00:00:00Z")?;

    let relation_a = RelationDefinition::enabled(
        tenant_a.clone(),
        relation_id,
        relation_key.clone(),
        ExpiryPolicy::ManualOnly,
    )?;
    let relation_b = RelationDefinition::enabled(
        tenant_b.clone(),
        relation_id,
        relation_key,
        ExpiryPolicy::ManualOnly,
    )?;
    repo_a.upsert_relation(&relation_a, at).await?;
    repo_b.upsert_relation(&relation_b, at).await?;

    let subject = SubjectRef::new("account", "same-subject")?;
    let keepsake_id = Uuid::now_v7();
    let mut command_a = ApplyKeepsake::new(
        tenant_a.clone(),
        subject.clone(),
        relation_id,
        at,
        test_context("tenant-a")?,
    );
    command_a.id = keepsake_id;
    command_a.audit_id = keepsake::AuditEventId::deterministic(b"tenant-a-audit");
    let mut command_b = ApplyKeepsake::new(
        tenant_b.clone(),
        subject.clone(),
        relation_id,
        at,
        test_context("tenant-b")?,
    );
    command_b.id = keepsake_id;
    command_b.audit_id = keepsake::AuditEventId::deterministic(b"tenant-b-audit");

    let applied_a = repo_a.apply(&command_a).await?;
    let applied_b = repo_b.apply(&command_b).await?;
    assert_eq!(applied_a.keepsake.id(), keepsake_id);
    assert_eq!(applied_b.keepsake.id(), keepsake_id);
    assert_eq!(applied_a.keepsake.tenant_id(), &tenant_a);
    assert_eq!(applied_b.keepsake.tenant_id(), &tenant_b);
    assert_eq!(repo_a.active_for_subject(&subject).await?.len(), 1);
    assert_eq!(repo_b.active_for_subject(&subject).await?.len(), 1);
    assert_eq!(repo_a.relation_by_id(relation_id).await?, Some(relation_a));
    assert_eq!(repo_b.relation_by_id(relation_id).await?, Some(relation_b));

    assert!(matches!(
        repo_b.apply(&command_a).await,
        Err(RepositoryError::TenantScopeMismatch)
    ));
    assert!(matches!(
        repo_b.active_relations_for_subject(&subject).await,
        Ok(rows) if rows.len() == 1 && rows[0].keepsake().tenant_id() == &tenant_b
    ));
    assert!(matches!(
        ActiveRelationSource::active_relations_for_subject(&repo_b, &tenant_a, &subject).await,
        Err(RepositoryError::TenantScopeMismatch)
    ));
    Ok(())
}
