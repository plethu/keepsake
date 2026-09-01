use keepsake::{RelationDefinition, RelationId, RelationKey, TenantId};

use std::fmt::Debug;
#[cfg(feature = "cache")]
use std::time::Duration;

/// Adapter for relation definition caching.
#[async_trait::async_trait]
pub trait RelationCache: Send + Sync + Debug {
    /// Gets a cached relation by stable id.
    async fn get_by_id(
        &self,
        tenant_id: &TenantId,
        relation_id: RelationId,
    ) -> Option<RelationDefinition>;

    /// Gets a cached relation by natural relation key.
    async fn get_by_key(
        &self,
        tenant_id: &TenantId,
        key: &RelationKey,
    ) -> Option<RelationDefinition>;

    /// Stores or refreshes a relation definition.
    async fn store(&self, tenant_id: &TenantId, relation: &RelationDefinition);

    /// Removes cached entries for a relation id.
    async fn remove_by_id(&self, tenant_id: &TenantId, relation_id: RelationId);
}

/// Relation cache implementation that never stores entries.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRelationCache;

#[async_trait::async_trait]
impl RelationCache for NoopRelationCache {
    async fn get_by_id(
        &self,
        _tenant_id: &TenantId,
        _relation_id: RelationId,
    ) -> Option<RelationDefinition> {
        None
    }

    async fn get_by_key(
        &self,
        _tenant_id: &TenantId,
        _key: &RelationKey,
    ) -> Option<RelationDefinition> {
        None
    }

    async fn store(&self, _tenant_id: &TenantId, _relation: &RelationDefinition) {}

    async fn remove_by_id(&self, _tenant_id: &TenantId, _relation_id: RelationId) {}
}

/// Configuration for local in-process relation definition caching.
#[cfg(feature = "cache")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRelationCacheConfig {
    /// Time before a cached relation definition must be refreshed from Postgres.
    pub ttl: Duration,
}

#[cfg(feature = "cache")]
impl LocalRelationCacheConfig {
    /// Creates a local relation cache configuration.
    #[must_use]
    pub const fn new(ttl: Duration) -> Self {
        Self { ttl }
    }
}

/// Local in-process relation definition cache.
#[cfg(feature = "cache")]
#[derive(Debug, Clone)]
pub struct LocalRelationCache {
    by_id: moka::sync::Cache<(TenantId, RelationId), RelationDefinition>,
    by_key: moka::sync::Cache<(TenantId, RelationKey), RelationDefinition>,
}

#[cfg(feature = "cache")]
impl LocalRelationCache {
    /// Creates a local in-process relation definition cache.
    #[must_use]
    pub fn new(config: LocalRelationCacheConfig) -> Self {
        Self {
            by_id: moka::sync::Cache::builder()
                .time_to_live(config.ttl)
                .build(),
            by_key: moka::sync::Cache::builder()
                .time_to_live(config.ttl)
                .build(),
        }
    }
}

#[cfg(feature = "cache")]
#[async_trait::async_trait]
impl RelationCache for LocalRelationCache {
    async fn get_by_id(
        &self,
        tenant_id: &TenantId,
        relation_id: RelationId,
    ) -> Option<RelationDefinition> {
        self.by_id.get(&(tenant_id.clone(), relation_id))
    }

    async fn get_by_key(
        &self,
        tenant_id: &TenantId,
        key: &RelationKey,
    ) -> Option<RelationDefinition> {
        self.by_key.get(&(tenant_id.clone(), key.clone()))
    }

    async fn store(&self, tenant_id: &TenantId, relation: &RelationDefinition) {
        self.by_id
            .insert((tenant_id.clone(), relation.id), relation.clone());
        self.by_key
            .insert((tenant_id.clone(), relation.key.clone()), relation.clone());
    }

    async fn remove_by_id(&self, tenant_id: &TenantId, relation_id: RelationId) {
        let id_key = (tenant_id.clone(), relation_id);
        let relation_key = self
            .by_id
            .get(&id_key)
            .map(|relation| relation.key)
            .or_else(|| {
                self.by_key.iter().find_map(|(cache_key, relation)| {
                    let (cached_tenant, cached_key) = cache_key.as_ref();
                    (cached_tenant == tenant_id && relation.id == relation_id)
                        .then(|| cached_key.clone())
                })
            });
        if let Some(relation_key) = relation_key {
            self.by_key.invalidate(&(tenant_id.clone(), relation_key));
        }
        self.by_id.invalidate(&id_key);
    }
}

#[cfg(all(test, feature = "cache"))]
mod tests {
    use super::*;
    use keepsake::ExpiryPolicy;
    use std::thread;
    use uuid::Uuid;

    #[tokio::test]
    async fn local_cache_expires_entries_after_ttl() -> keepsake::Result<()> {
        let relation = RelationDefinition::enabled(
            keepsake::TenantId::new("tenant-test")?,
            Uuid::nil(),
            RelationKey::new("tag", "trusted")?,
            ExpiryPolicy::ManualOnly,
        )?;
        let cache =
            LocalRelationCache::new(LocalRelationCacheConfig::new(Duration::from_millis(1)));
        cache.store(&relation.tenant_id, &relation).await;
        thread::sleep(Duration::from_millis(5));

        assert_eq!(
            cache.get_by_id(&relation.tenant_id, relation.id).await,
            None
        );
        assert_eq!(
            cache.get_by_key(&relation.tenant_id, &relation.key).await,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn remove_by_id_invalidates_a_surviving_key_entry() -> keepsake::Result<()> {
        let relation = RelationDefinition::enabled(
            keepsake::TenantId::new("tenant-test")?,
            Uuid::nil(),
            RelationKey::new("tag", "trusted")?,
            ExpiryPolicy::ManualOnly,
        )?;
        let cache = LocalRelationCache::new(LocalRelationCacheConfig::new(Duration::from_mins(1)));
        cache.store(&relation.tenant_id, &relation).await;
        cache
            .by_id
            .invalidate(&(relation.tenant_id.clone(), relation.id));

        cache.remove_by_id(&relation.tenant_id, relation.id).await;

        assert_eq!(
            cache.get_by_key(&relation.tenant_id, &relation.key).await,
            None
        );
        Ok(())
    }
}
