use chrono::{DateTime, Utc};
use keepsake::{RelationDefinition, RelationId, RelationKey, RelationSpec};

use crate::repository::support::parse_uuid;
use crate::repository::{
    RelationCache, RepositoryError, RepositoryResult, SqliteBackend, TenantSqlxKeepsakeRepository,
};

use super::rows::{format_timestamp, relation_from_row};

impl<C> TenantSqlxKeepsakeRepository<'_, SqliteBackend, C>
where
    C: RelationCache,
{
    /// Inserts or updates a relation definition by its natural relation key.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: DateTime<Utc>,
    ) -> RepositoryResult<RelationDefinition> {
        if relation.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }

        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let row = sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
            on conflict (tenant_id, kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = ?7
            returning tenant_id, id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(format_timestamp(at))
        .fetch_one(self.pool)
        .await?;
        let relation = relation_from_row(&row)?;
        self.relation_cache
            .remove_by_id(&self.tenant_id, relation.id)
            .await;
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
        let relation = RelationDefinition::from_spec::<Spec>(self.tenant_id.clone(), at)?;
        let expiry_policy = serde_json::to_string(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
            on conflict (tenant_id, kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = ?7
            where keepsake_relation_definitions.id = excluded.id
            returning tenant_id, id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(self.tenant_id.as_str())
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
                where tenant_id = ?1 and kind = ?2 and key = ?3
                ",
            )
            .bind(self.tenant_id.as_str())
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
        self.relation_cache
            .remove_by_id(&self.tenant_id, relation.id)
            .await;
        Ok(relation)
    }

    /// Looks up a relation definition by stable id.
    pub async fn relation_by_id(
        &self,
        relation_id: RelationId,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self
            .relation_cache
            .get_by_id(&self.tenant_id, relation_id)
            .await
        {
            return Ok(Some(relation));
        }

        let row = sqlx::query(
            r"
            select tenant_id, id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ?1 and id = ?2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation_id.to_string())
        .fetch_optional(self.pool)
        .await?;
        let relation = row.map(|row| relation_from_row(&row)).transpose()?;
        if let Some(relation) = &relation {
            self.relation_cache.store(&self.tenant_id, relation).await;
        }
        Ok(relation)
    }

    /// Looks up a relation definition by its natural relation key.
    pub async fn relation_by_key(
        &self,
        key: &RelationKey,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        if let Some(relation) = self.relation_cache.get_by_key(&self.tenant_id, key).await {
            return Ok(Some(relation));
        }

        let row = sqlx::query(
            r"
            select tenant_id, id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ?1 and kind = ?2 and key = ?3
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(key.kind())
        .bind(key.name())
        .fetch_optional(self.pool)
        .await?;
        let relation = row.map(|row| relation_from_row(&row)).transpose()?;
        if let Some(relation) = &relation {
            self.relation_cache.store(&self.tenant_id, relation).await;
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
            set enabled = ?3, updated_at = ?4
            where tenant_id = ?1 and id = ?2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation_id.to_string())
        .bind(enabled)
        .bind(format_timestamp(at))
        .execute(self.pool)
        .await?;
        let changed = result.rows_affected() == 1;
        if changed {
            self.relation_cache
                .remove_by_id(&self.tenant_id, relation_id)
                .await;
        }
        Ok(changed)
    }
}
