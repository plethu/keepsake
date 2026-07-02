use chrono::{DateTime, Utc};
use keepsake::{RelationDefinition, RelationId, RelationKey, RelationSpec};

use crate::repository::support::parse_uuid;
use crate::repository::{
    RelationCache, RepositoryError, RepositoryResult, SqliteKeepsakeRepository,
};

use super::rows::{format_timestamp, relation_from_row};

impl<C> SqliteKeepsakeRepository<C>
where
    C: RelationCache,
{
    /// Inserts or updates a relation definition by its natural relation key.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition> {
        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let row = sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?6)
            on conflict (kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = ?6
            returning id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(format_timestamp(at))
        .fetch_one(&self.pool)
        .await?;
        let relation = relation_from_row(&row)?;
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
        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?6)
            on conflict (kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = ?6
            where keepsake_relation_definitions.id = excluded.id
            returning id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(format_timestamp(at))
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let stored_relation_id = sqlx::query_scalar::<_, String>(
                r"
                select id
                from keepsake_relation_definitions
                where kind = ?1 and key = ?2
                ",
            )
            .bind(relation.key.kind())
            .bind(relation.key.name())
            .fetch_one(&mut *tx)
            .await?;
            return Err(RepositoryError::RelationSpecIdMismatch {
                kind: relation.key.kind().to_owned(),
                name: relation.key.name().to_owned(),
                expected_relation_id: relation.id,
                stored_relation_id: parse_uuid(&stored_relation_id)?,
            });
        };

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
            select id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where id = ?1
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
            select id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where kind = ?1 and key = ?2
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
            set enabled = ?2, updated_at = ?3
            where id = ?1
            ",
        )
        .bind(relation_id.to_string())
        .bind(enabled)
        .bind(format_timestamp(at))
        .execute(&self.pool)
        .await?;
        let changed = result.rows_affected() == 1;
        if changed {
            self.relation_cache.remove_by_id(relation_id).await;
        }
        Ok(changed)
    }
}
