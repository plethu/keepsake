use keepsake::{RelationDefinition, RelationId, RelationKey};

#[cfg(feature = "cache")]
use std::collections::BTreeMap;
use std::fmt::Debug;
#[cfg(feature = "cache")]
use std::sync::{Arc, RwLock};
#[cfg(feature = "cache")]
use std::time::{Duration, Instant};

/// Adapter for relation definition caching.
#[async_trait::async_trait]
pub trait RelationCache: Send + Sync + Debug {
    /// Gets a cached relation by stable id.
    async fn get_by_id(&self, relation_id: RelationId) -> Option<RelationDefinition>;

    /// Gets a cached relation by natural relation key.
    async fn get_by_key(&self, key: &RelationKey) -> Option<RelationDefinition>;

    /// Stores or refreshes a relation definition.
    async fn store(&self, relation: &RelationDefinition);

    /// Removes cached entries for a relation id.
    async fn remove_by_id(&self, relation_id: RelationId);
}

/// Relation cache implementation that never stores entries.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRelationCache;

#[async_trait::async_trait]
impl RelationCache for NoopRelationCache {
    async fn get_by_id(&self, _relation_id: RelationId) -> Option<RelationDefinition> {
        None
    }

    async fn get_by_key(&self, _key: &RelationKey) -> Option<RelationDefinition> {
        None
    }

    async fn store(&self, _relation: &RelationDefinition) {}

    async fn remove_by_id(&self, _relation_id: RelationId) {}
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
    config: LocalRelationCacheConfig,
    // Local cache handles may be cloned or shared across repository clones.
    // Locks protect a small in-process map and are never held across `.await`.
    // Cross-pod invalidation belongs in another `RelationCache` adapter.
    state: Arc<RwLock<LocalRelationCacheState>>,
}

#[cfg(feature = "cache")]
impl LocalRelationCache {
    /// Creates a local in-process relation definition cache.
    #[must_use]
    pub fn new(config: LocalRelationCacheConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(LocalRelationCacheState::default())),
        }
    }
}

#[cfg(feature = "cache")]
#[async_trait::async_trait]
impl RelationCache for LocalRelationCache {
    async fn get_by_id(&self, relation_id: RelationId) -> Option<RelationDefinition> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.by_id.get(&relation_id).cloned())
            .and_then(CacheEntry::fresh_relation)
    }

    async fn get_by_key(&self, key: &RelationKey) -> Option<RelationDefinition> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.by_key.get(key).cloned())
            .and_then(CacheEntry::fresh_relation)
    }

    async fn store(&self, relation: &RelationDefinition) {
        let entry = CacheEntry {
            relation: relation.clone(),
            expires_at: Instant::now() + self.config.ttl,
        };
        if let Ok(mut state) = self.state.write() {
            state.by_id.insert(relation.id, entry.clone());
            state.by_key.insert(relation.key.clone(), entry);
        }
    }

    async fn remove_by_id(&self, relation_id: RelationId) {
        if let Ok(mut state) = self.state.write()
            && let Some(entry) = state.by_id.remove(&relation_id)
        {
            state.by_key.remove(&entry.relation.key);
        }
    }
}

#[cfg(feature = "cache")]
#[derive(Debug, Default)]
struct LocalRelationCacheState {
    by_id: BTreeMap<RelationId, CacheEntry>,
    by_key: BTreeMap<RelationKey, CacheEntry>,
}

#[cfg(feature = "cache")]
#[derive(Debug, Clone)]
struct CacheEntry {
    relation: RelationDefinition,
    expires_at: Instant,
}

#[cfg(feature = "cache")]
impl CacheEntry {
    fn fresh_relation(self) -> Option<RelationDefinition> {
        (Instant::now() <= self.expires_at).then_some(self.relation)
    }
}

#[cfg(all(test, feature = "cache"))]
mod tests {
    use super::*;
    use keepsake::ExpiryPolicy;
    use std::thread;
    use uuid::Uuid;

    #[test]
    fn cache_entry_expires_after_ttl() -> keepsake::Result<()> {
        let relation = RelationDefinition::enabled(
            Uuid::nil(),
            RelationKey::new("tag", "trusted")?,
            ExpiryPolicy::ManualOnly,
        )?;
        let entry = CacheEntry {
            relation,
            expires_at: Instant::now() + Duration::from_millis(1),
        };

        thread::sleep(Duration::from_millis(5));

        assert_eq!(entry.fresh_relation(), None);
        Ok(())
    }
}
