use keepsake::{RelationDefinition, RelationId, RelationKey, RelationSpec};
use time::OffsetDateTime;
use uuid::Uuid;

use super::PostgresBackend;
use super::support::{canonical_relation, canonical_timestamp};
use super::{
    RelationCache, RelationRow, RepositoryError, RepositoryResult, TenantSqlxKeepsakeRepository,
};

impl<C> TenantSqlxKeepsakeRepository<'_, PostgresBackend, C>
where
    C: RelationCache,
{
    /// Inserts or updates a relation definition by its natural relation key.
    ///
    /// If a relation already exists for the same kind/name, its stable id is preserved and
    /// the returned definition contains the existing id.
    pub async fn upsert_relation(
        &self,
        relation: &RelationDefinition,
        at: OffsetDateTime,
    ) -> RepositoryResult<RelationDefinition> {
        if relation.tenant_id != self.tenant_id {
            return Err(RepositoryError::TenantScopeMismatch);
        }

        relation.validate()?;
        let relation = canonical_relation(relation);
        let at = canonical_timestamp(at);
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let existing_by_id = sqlx::query_as::<_, RelationRow>(
            r"
            select tenant_id, id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = $1 and id = $2
            for update
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation.id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(row) = existing_by_id {
            let stored = row.try_into_relation()?;
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

        let row = sqlx::query_as::<_, RelationRow>(
            r"
            insert into keepsake_relation_definitions
                (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values ($1, $2, $3, $4, $5, $6, $7, $7)
            on conflict (tenant_id, kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = $7
            returning tenant_id, id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation.id)
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(at)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        let relation = row.try_into_relation()?;
        self.relation_cache
            .remove_by_id(&self.tenant_id, relation.id)
            .await;
        Ok(relation)
    }

    /// Inserts or updates a typed relation spec by its natural relation key.
    pub async fn upsert_relation_spec<Spec>(
        &self,
        at: OffsetDateTime,
    ) -> RepositoryResult<RelationDefinition>
    where
        Spec: RelationSpec,
    {
        let relation = canonical_relation(&RelationDefinition::from_spec::<Spec>(
            self.tenant_id.clone(),
            at,
        )?);
        let at = canonical_timestamp(at);
        let expiry_policy = serde_json::to_value(&relation.expiry)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            insert into keepsake_relation_definitions
                (tenant_id, id, kind, key, enabled, expiry_policy, created_at, updated_at)
            values ($1, $2, $3, $4, $5, $6, $7, $7)
            on conflict (tenant_id, kind, key) do update set
                enabled = excluded.enabled,
                expiry_policy = excluded.expiry_policy,
                updated_at = $7
            where keepsake_relation_definitions.tenant_id = excluded.tenant_id
              and keepsake_relation_definitions.id = excluded.id
            returning tenant_id, id, kind, key, enabled, expiry_policy
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation.id)
        .bind(relation.key.kind())
        .bind(relation.key.name())
        .bind(relation.enabled)
        .bind(expiry_policy)
        .bind(at)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let stored_relation_id = sqlx::query_scalar::<_, Uuid>(
                r"
                select id
                from keepsake_relation_definitions
                where tenant_id = $1 and kind = $2 and key = $3
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
                stored_relation_id,
            });
        };

        tx.commit().await?;
        let relation = row.try_into_relation()?;
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

        let relation = self.fetch_relation_by_id(relation_id).await?;
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

        let relation = self.fetch_relation_by_key(key).await?;
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
        at: OffsetDateTime,
    ) -> RepositoryResult<bool> {
        let result = sqlx::query(
            r"
            update keepsake_relation_definitions
            set enabled = $3, updated_at = $4
            where tenant_id = $1 and id = $2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation_id)
        .bind(enabled)
        .bind(at)
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

    async fn fetch_relation_by_id(
        &self,
        relation_id: RelationId,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            select tenant_id, id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = $1 and id = $2
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(relation_id)
        .fetch_optional(self.pool)
        .await?;

        row.map(RelationRow::try_into_relation).transpose()
    }

    async fn fetch_relation_by_key(
        &self,
        key: &RelationKey,
    ) -> RepositoryResult<Option<RelationDefinition>> {
        let row = sqlx::query_as::<_, RelationRow>(
            r"
            select tenant_id, id, kind, key, enabled, expiry_policy
            from keepsake_relation_definitions
            where tenant_id = $1 and kind = $2 and key = $3
            ",
        )
        .bind(self.tenant_id.as_str())
        .bind(key.kind())
        .bind(key.name())
        .fetch_optional(self.pool)
        .await?;

        row.map(RelationRow::try_into_relation).transpose()
    }
}
