use chrono::{DateTime, Utc};
use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, RelationDefinition, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
use sqlx::{MySql, Transaction};
use uuid::Uuid;

use crate::repository::support::{apply_event, expires_at, revoke_by_subject_event, revoke_event};
use crate::repository::{
    AppliedKeepsake, MySqlKeepsakeRepository, RelationCache, RepositoryError, RepositoryResult,
};

use super::audit::record_audit_event_tx;
use super::rows::{keepsake_from_row, naive_timestamp, relation_from_row};

impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Applies a command idempotently and records its audit event atomically.
    pub async fn apply(&self, command: &ApplyKeepsake) -> RepositoryResult<AppliedKeepsake> {
        command.subject.validate()?;
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let relation = relation_for_update_tx(&mut tx, command.relation_id).await?;
        if let Some(existing) =
            active_keepsake_for_subject_relation_tx(&mut tx, &command.subject, command.relation_id)
                .await?
        {
            record_audit_event_tx(&mut tx, &apply_event(command, &existing, true)).await?;
            tx.commit().await?;
            return Ok(AppliedKeepsake {
                keepsake: existing,
                duplicate_prevented: true,
            });
        }

        if !relation.enabled {
            return Err(RepositoryError::RelationDisabled {
                relation_id: command.relation_id,
            });
        }

        sqlx::query(
            r"
            insert into keepsakes
                (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                 expires_at, metadata, created_at, updated_at)
            values (?, ?, ?, ?, 'applied', ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(command.id.to_string())
        .bind(command.subject.kind())
        .bind(command.subject.id())
        .bind(command.relation_id.to_string())
        .bind(serde_json::to_value(&relation.expiry)?)
        .bind(naive_timestamp(command.at))
        .bind(expires_at(&relation.expiry).map(naive_timestamp))
        .bind(serde_json::to_value(&command.metadata)?)
        .bind(naive_timestamp(command.at))
        .bind(naive_timestamp(command.at))
        .execute(&mut *tx)
        .await?;

        let keepsake = keepsake_by_id_tx(&mut tx, command.id).await?.ok_or(
            RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            },
        )?;
        record_audit_event_tx(&mut tx, &apply_event(command, &keepsake, false)).await?;
        tx.commit().await?;
        Ok(AppliedKeepsake {
            keepsake,
            duplicate_prevented: false,
        })
    }

    /// Revokes an active keepsake from a command and records its audit event atomically.
    pub async fn revoke(&self, command: &RevokeKeepsake) -> RepositoryResult<bool> {
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let revoked = revoke_tx(&mut tx, command.keepsake_id, command.at).await?;
        if let Some(keepsake) = &revoked {
            record_audit_event_tx(&mut tx, &revoke_event(command, keepsake)).await?;
        }
        tx.commit().await?;
        Ok(revoked.is_some())
    }

    /// Revokes the active keepsake for a subject and relation pair.
    ///
    /// Returns the revoked keepsake id, or `None` when no active keepsake exists
    /// for the pair. The active uniqueness invariant guarantees at most one match.
    pub async fn revoke_by_subject(
        &self,
        command: &RevokeBySubject,
    ) -> RepositoryResult<Option<KeepsakeId>> {
        command.subject.validate()?;
        command.context.validate()?;

        let mut tx = self.pool.begin().await?;
        let revoked =
            revoke_by_subject_tx(&mut tx, &command.subject, command.relation_id, command.at)
                .await?;
        let revoked_id = revoked.as_ref().map(Keepsake::id);
        if let Some(keepsake) = &revoked {
            record_audit_event_tx(&mut tx, &revoke_by_subject_event(command, keepsake)).await?;
        }
        tx.commit().await?;
        Ok(revoked_id)
    }
}

pub(super) async fn relation_for_update_tx(
    tx: &mut Transaction<'_, MySql>,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query(
        r"
        select id, kind, `key`, enabled, expiry_policy
        from keepsake_relation_definitions
        where id = ?
        for update
        ",
    )
    .bind(relation_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    relation_from_row(&row)
}

pub(super) async fn active_keepsake_for_subject_relation_tx(
    tx: &mut Transaction<'_, MySql>,
    subject: &SubjectRef,
    relation_id: RelationId,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where subject_kind = ? and subject_id = ? and relation_id = ? and state = 'applied'
        for update
        ",
    )
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn keepsake_by_id_tx(
    tx: &mut Transaction<'_, MySql>,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where id = ?
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_tx(
    tx: &mut Transaction<'_, MySql>,
    keepsake_id: Uuid,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let result = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?, updated_at = ?
        where id = ? and state = 'applied'
        ",
    )
    .bind(naive_timestamp(at))
    .bind(naive_timestamp(at))
    .bind(keepsake_id.to_string())
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() == 0 {
        return Ok(None);
    }
    keepsake_by_id_tx(tx, keepsake_id).await
}

pub(super) async fn revoke_by_subject_tx(
    tx: &mut Transaction<'_, MySql>,
    subject: &SubjectRef,
    relation_id: RelationId,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where subject_kind = ? and subject_id = ? and relation_id = ? and state = 'applied'
        for update
        ",
    )
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    let Some(keepsake) = row.as_ref().map(keepsake_from_row).transpose()? else {
        return Ok(None);
    };
    revoke_tx(tx, keepsake.id(), at).await
}
