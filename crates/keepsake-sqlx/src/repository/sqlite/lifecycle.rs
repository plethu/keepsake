use chrono::{DateTime, Utc};
use keepsake::{
    ApplyKeepsake, Keepsake, KeepsakeId, RelationDefinition, RelationId, RevokeBySubject,
    RevokeKeepsake, SubjectRef,
};
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

use crate::repository::support::{apply_event, expires_at, revoke_by_subject_event, revoke_event};
use crate::repository::{
    AppliedKeepsake, RelationCache, RepositoryError, RepositoryResult, SqliteKeepsakeRepository,
};

use super::audit::record_audit_event_tx;
use super::rows::{format_timestamp, keepsake_from_row, relation_from_row};

impl<C> SqliteKeepsakeRepository<C>
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

        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let metadata = serde_json::to_string(&command.metadata)?;
        let expires_at_column = expires_at(&relation.expiry).map(format_timestamp);
        let at = format_timestamp(command.at);
        let result = sqlx::query(
            r"
            insert into keepsakes
                (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
                 expires_at, metadata, created_at, updated_at)
            values (?1, ?2, ?3, ?4, 'applied', ?5, ?6, ?7, ?8, ?6, ?6)
            on conflict (subject_kind, subject_id, relation_id) where state = 'applied'
            do nothing
            ",
        )
        .bind(command.id.to_string())
        .bind(command.subject.kind())
        .bind(command.subject.id())
        .bind(command.relation_id.to_string())
        .bind(expiry_policy)
        .bind(&at)
        .bind(expires_at_column)
        .bind(metadata)
        .execute(&mut *tx)
        .await?;

        let (keepsake, duplicate_prevented) = if result.rows_affected() == 0 {
            let existing = active_keepsake_for_subject_relation_tx(
                &mut tx,
                &command.subject,
                command.relation_id,
            )
            .await?
            .ok_or(RepositoryError::RelationDefinitionMissing {
                relation_id: command.relation_id,
            })?;
            (existing, true)
        } else {
            let keepsake = keepsake_by_id_tx(&mut tx, command.id).await?.ok_or(
                RepositoryError::RelationDefinitionMissing {
                    relation_id: command.relation_id,
                },
            )?;
            (keepsake, false)
        };

        record_audit_event_tx(
            &mut tx,
            &apply_event(command, &keepsake, duplicate_prevented),
        )
        .await?;
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
    tx: &mut Transaction<'_, Sqlite>,
    relation_id: RelationId,
) -> RepositoryResult<RelationDefinition> {
    let row = sqlx::query(
        r"
        select id, kind, key, enabled, expiry_policy
        from keepsake_relation_definitions
        where id = ?1
        ",
    )
    .bind(relation_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    relation_from_row(&row)
}

pub(super) async fn active_keepsake_for_subject_relation_tx(
    tx: &mut Transaction<'_, Sqlite>,
    subject: &SubjectRef,
    relation_id: RelationId,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where subject_kind = ?1 and subject_id = ?2 and relation_id = ?3 and state = 'applied'
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
    tx: &mut Transaction<'_, Sqlite>,
    keepsake_id: Uuid,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        select id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        from keepsakes
        where id = ?1
        ",
    )
    .bind(keepsake_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_tx(
    tx: &mut Transaction<'_, Sqlite>,
    keepsake_id: Uuid,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?2, updated_at = ?2
        where id = ?1 and state = 'applied'
        returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(keepsake_id.to_string())
    .bind(format_timestamp(at))
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}

pub(super) async fn revoke_by_subject_tx(
    tx: &mut Transaction<'_, Sqlite>,
    subject: &SubjectRef,
    relation_id: RelationId,
    at: DateTime<Utc>,
) -> RepositoryResult<Option<Keepsake>> {
    let row = sqlx::query(
        r"
        update keepsakes
        set state = 'revoked', revoked_at = ?4, updated_at = ?4
        where subject_kind = ?1 and subject_id = ?2 and relation_id = ?3 and state = 'applied'
        returning id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
            expires_at, fulfilled_at, revoked_at, metadata
        ",
    )
    .bind(subject.kind())
    .bind(subject.id())
    .bind(relation_id.to_string())
    .bind(format_timestamp(at))
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(keepsake_from_row).transpose()
}
