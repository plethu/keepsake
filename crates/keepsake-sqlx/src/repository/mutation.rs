use keepsake::{ApplyKeepsake, Keepsake, RelationDefinition, RelationId, RevokeKeepsake};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::audit::record_audit_event_tx;
use super::support::{apply_event, revoke_event};
use super::{
    AppliedKeepsake, AppliedKeepsakeRow, AppliedKeepsakeWriteRow, KeepsakeRepository,
    RelationCache, RelationRow, RepositoryError, RepositoryResult,
};

impl<C> KeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Applies a command idempotently and records its audit event atomically.
    ///
    /// If an active keepsake already exists for the subject and relation, the existing
    /// row is returned with `duplicate_prevented` set to true, even if the relation
    /// has since been disabled. Disabled relations reject new non-duplicate applies.
    pub async fn apply(&self, command: &ApplyKeepsake) -> RepositoryResult<AppliedKeepsake> {
        command.subject.validate()?;
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let relation = relation_for_share_tx(&mut tx, command.relation_id).await?;
        let metadata = serde_json::to_value(&command.metadata)?;

        let applied = sqlx::query_as::<_, AppliedKeepsakeWriteRow>(
            r"
            insert into keepsakes
                (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at, expires_at, metadata, created_at, updated_at)
            select
                $1,
                $2,
                $3,
                r.id,
                'applied',
                r.expiry_policy,
                $4,
                case
                    when r.expiry_policy->>'type' = 'at'
                    then (r.expiry_policy->>'timestamp')::timestamptz
                    else null
                end,
                $5,
                $4,
                $4
            from keepsake_relation_definitions r
            where r.id = $6
            on conflict (subject_kind, subject_id, relation_id) where state = 'applied'
            do update set updated_at = keepsakes.updated_at
            returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                expires_at, fulfilled_at, revoked_at, metadata, (xmax <> 0) as duplicate_prevented
            ",
        )
        .bind(command.id)
        .bind(&command.subject.kind)
        .bind(&command.subject.id)
        .bind(command.at)
        .bind(metadata)
        .bind(command.relation_id)
        .fetch_one(&mut *tx)
        .await?;

        if !relation.enabled && !applied.duplicate_prevented {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        let (keepsake, duplicate_prevented) = applied.try_into_parts()?;
        let event = apply_event(command, &keepsake, duplicate_prevented);
        record_audit_event_tx(&mut tx, &event).await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented,
        })
    }

    /// Revokes an active keepsake from a command and records its audit event atomically.
    pub async fn revoke(&self, command: &RevokeKeepsake) -> RepositoryResult<bool> {
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let revoked = revoke_tx(&mut tx, command.keepsake_id, command.at).await?;
        if let Some(keepsake) = &revoked {
            let event = revoke_event(command, keepsake);
            record_audit_event_tx(&mut tx, &event).await?;
        }
        tx.commit().await?;
        Ok(revoked.is_some())
    }
}

async fn revoke_tx(
    tx: &mut Transaction<'_, Postgres>,
    keepsake_id: Uuid,
    at: chrono::DateTime<chrono::Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query_as::<_, AppliedKeepsakeRow>(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = $2, updated_at = $2
        where id = $1 and state = 'applied'
        returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(keepsake_id)
    .bind(at)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(AppliedKeepsakeRow::try_into_keepsake).transpose()
}

async fn relation_for_share_tx(
    tx: &mut Transaction<'_, Postgres>,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query_as::<_, RelationRow>(
        r"
        select id, kind, key, enabled, expiry_policy
        from keepsake_relation_definitions
        where id = $1
        for share
        ",
    )
    .bind(relation_id)
    .fetch_one(&mut **tx)
    .await?;
    row.try_into_relation()
}
