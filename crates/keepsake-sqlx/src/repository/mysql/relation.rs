use chrono::{DateTime, Utc};
use keepsake::{RelationDefinition, RelationId, RelationKey, RelationSpec};

use crate::repository::{
    MySqlKeepsakeRepository, RelationCache, RepositoryError, RepositoryResult,
};

use super::rows::{naive_timestamp, relation_from_row};

impl<C> MySqlKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Inserts or updates a relation definition by its natural relation key.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition> {
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (id, kind, `key`, enabled, expiry_policy, created_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?)
            on duplicate key update
                enabled = values(enabled),
                expiry_policy = values(expiry_policy),
                updated_at = values(updated_at)
            ",
        )
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(naive_timestamp(at))
        .bind(naive_timestamp(at))
        .execute(&self.pool)
        .await?;

        let relation = self.relation_by_key(&relation.key).await?.ok_or(
            RepositoryError::RelationDefinitionMissing {
                relation_id: relation.id,
            },
        )?;
        self.relation_cache.remove_by_id(relation.id).await;
        Ok(relation)
    }

    /// Inserts or updates a typed relation spec by its natural relation key.
    pub async fn upsert_relation_spec<Spec>(
        &self,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition>
    where
        Spec: RelationSpec,
    {
        let relation = RelationDefinition::from_spec::<Spec>(at)?;
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            r"
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = ? and `key` = ?
            for update
            ",
        )
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = existing {
            let stored = relation_from_row(&row)?;
            if stored.id != relation.id {
                return Err(RepositoryError::RelationSpecIdMismatch {
                    kind: relation.key.kind().to_owned(),
                    name: relation.key.name().to_owned(),
                    expected_relation_id: relation.id,
                    stored_relation_id: stored.id,
                });
            }
            sqlx::query(
                r"
                update keepsake_relation_definitions
                set enabled = ?, expiry_policy = ?, updated_at = ?
                where id = ?
                ",
            )
            .bind(relation.enabled)
            .bind(serde_json::to_value(&relation.expiry)?)
            .bind(naive_timestamp(at))
            .bind(relation.id.to_string())
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                r"
                insert into keepsake_relation_definitions
                    (id, kind, `key`, enabled, expiry_policy, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?)
                ",
            )
            .bind(relation.id.to_string())
            .bind(relation.key.kind())
            .bind(relation.key.name())
            .bind(relation.enabled)
            .bind(serde_json::to_value(&relation.expiry)?)
            .bind(naive_timestamp(at))
            .bind(naive_timestamp(at))
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query(
            r"
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = ?
            ",
        )
        .bind(relation.id.to_string())
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        let relation = relation_from_row(&row)?;
        self.relation_cache.remove_by_id(relation.id).await;
        Ok(relation)
    }

    /// Looks up a relation definition by stable id.
    pub async fn relation_by_id(
        &self,
        relation_id: RelationId,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_id(relation_id).await {
            return Ok(Some(relation));
        }

        let row = sqlx::query(
            r"
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = ?
            ",
        )
        .bind(relation_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let relation = row.map(|row| relation_from_row(&row)).transpose()?;
        if let Some(relation) = &relation {
            self.relation_cache.store(relation).await;
        }
        Ok(relation)
    }

    /// Looks up a relation definition by its natural relation key.
    pub async fn relation_by_key(
        &self,
        key: &RelationKey,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_key(key).await {
            return Ok(Some(relation));
        }

        let row = sqlx::query(
            r"
            select id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = ? and `key` = ?
            ",
        )
        .bind(key.kind())
        .bind(key.name())
        .fetch_optional(&self.pool)
        .await?;
        let relation = row.map(|row| relation_from_row(&row)).transpose()?;
        if let Some(relation) = &relation {
            self.relation_cache.store(relation).await;
        }
        Ok(relation)
    }

    /// Enables or disables a relation.
    pub async fn set_relation_enabled(
        &self,
        relation_id: RelationId,
        enabled: bool,
        at: DateTime<Utc>,
    ) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_relation_definitions
            set enabled = ?, updated_at = ?
            where id = ?
            ",
        )
        .bind(enabled)
        .bind(naive_timestamp(at))
        .bind(relation_id.to_string())
        .execute(&self.pool)
        .await?;
        let changed = result.rows_affected() == 1;
        if changed {
            self.relation_cache.remove_by_id(relation_id).await;
        }
        Ok(changed)
    }
}
