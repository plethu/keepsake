use chrono::{DateTime, Utc};
use keepsake::{RelationDefinition, RelationId, RelationKey, RelationSpec};

use crate::repository::support::{canonical_relation, canonical_timestamp};
use crate::repository::{
    MySqlBackend, RelationCache, RepositoryError, RepositoryResult, TenantSqlxKeepsakeRepository,
};

use super::rows::{naive_timestamp, relation_from_row};

impl<C> TenantSqlxKeepsakeRepository<'_, MySqlBackend, C>
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
        relation.validate()?;
        let relation = canonical_relation(relation);
        let at = canonical_timestamp(at);
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let existing_by_id = sqlx::query(
            r"
            select tenant_id, id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ? and id = ?
            for update
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(relation.id.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(row) = existing_by_id {
            let stored = relation_from_row(&row)?;
            if stored.key != relation.key {
                return Err(RepositoryError::RelationIdentityConflict {
                    relation_id: relation.id,
                    stored_kind: stored.key.kind().to_owned(),
                    stored_name: stored.key.name().to_owned(),
                    incoming_kind: relation.key.kind().to_owned(),
                    incoming_name: relation.key.name().to_owned(),
                });
            }
        }

        sqlx::query(
            r"
            insert into keepsake_relation_definitions
                (tenant_id, id, kind, `key`, enabled, expiry_policy, created_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on duplicate key update
                enabled = if(kind = values(kind) and `key` = values(`key`), values(enabled), enabled),
                expiry_policy = if(kind = values(kind) and `key` = values(`key`), values(expiry_policy), expiry_policy),
                updated_at = if(kind = values(kind) and `key` = values(`key`), values(updated_at), updated_at)
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(relation.id.to_string())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(naive_timestamp(at))
        .bind(naive_timestamp(at))
        .execute(&mut *tx)
        .await?;

        let row = sqlx::query(
            r"
            select tenant_id, id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ? and kind = ? and `key` = ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(stored_relation) = row.map(|row| relation_from_row(&row)).transpose()? else {
            let stored_row = sqlx::query(
                r"
                select tenant_id, id, kind, `key`, enabled, expiry_policy
                from keepsake_relation_definitions
                where tenant_id = ? and id = ?
                ",
            )
            .bind(self.tenant_id.as_str().as_bytes())
            .bind(relation.id.to_string())
            .fetch_one(&mut *tx)
            .await?;
            let stored = relation_from_row(&stored_row)?;
            return Err(RepositoryError::RelationIdentityConflict {
                relation_id: relation.id,
                stored_kind: stored.key.kind().to_owned(),
                stored_name: stored.key.name().to_owned(),
                incoming_kind: relation.key.kind().to_owned(),
                incoming_name: relation.key.name().to_owned(),
            });
        };
        tx.commit().await?;
        let relation = stored_relation;
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
        let relation = canonical_relation(&RelationDefinition::from_spec::<Spec>(
            self.tenant_id.clone(),
            at,
        )?);
        let at = canonical_timestamp(at);
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            r"
            select tenant_id, id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ? and kind = ? and `key` = ?
            for update
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
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
                where tenant_id = ? and id = ?
                ",
            )
            .bind(relation.enabled)
            .bind(serde_json::to_value(&relation.expiry)?)
            .bind(naive_timestamp(at))
            .bind(self.tenant_id.as_str().as_bytes())
            .bind(relation.id.to_string())
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                r"
                insert into keepsake_relation_definitions
                    (tenant_id, id, kind, `key`, enabled, expiry_policy, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                ",
            )
            .bind(self.tenant_id.as_str().as_bytes())
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
            select tenant_id, id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ? and id = ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(relation.id.to_string())
        .fetch_one(&mut *tx)
        .await?;
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
            select tenant_id, id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ? and id = ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
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
            select tenant_id, id, kind, `key`, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = ? and kind = ? and `key` = ?
            ",
        )
        .bind(self.tenant_id.as_str().as_bytes())
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
            set enabled = ?, updated_at = ?
            where tenant_id = ? and id = ?
            ",
        )
        .bind(enabled)
        .bind(naive_timestamp(at))
        .bind(self.tenant_id.as_str().as_bytes())
        .bind(relation_id.to_string())
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
