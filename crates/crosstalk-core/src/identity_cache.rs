//! Cached identity resolution: per-run memo plus the store's `identity_cache` table.
//!
//! Phase 3 step 6. `crosstalk-matching` stays free of core dependencies (the
//! dependency runs core -> matching), so the persistence lives here: [`CachedIdentities`]
//! pairs matching's in-memory [`IdentityCache`] with the store rows in
//! [`Store::identity_cache_get`]/[`Store::identity_cache_put`]. Resolution order is
//! memo, then store, then compute-and-store; rows are namespaced by
//! [`UnifiedEngine::identity_version`] (merged data + decisions + engine version +
//! the settings identities depend on: normalisation toggles and custom aliases), so
//! a data, rule or settings change misses every old row without any explicit
//! clearing, and [`CachedIdentities::prune_stale`] drops them.
//!
//! Concurrency: the memo is a mutex-held map locked only per lookup/insert, and the
//! store serializes its own connection, so rayon threads may resolve through one
//! `CachedIdentities`. A store read happens once per distinct input per run (memo
//! first), a write only when the identity was computed.

use anyhow::Result;
use crosstalk_matching::engine::UnifiedEngine;
use crosstalk_matching::identity::{
    resolve_with, FamilyMember, Identity, IdentityCache, ResolveContext,
};
use std::sync::Arc;

use crate::store::Store;

/// One resolver for a whole run: memoized in memory, persisted in the store.
#[derive(Debug, Clone)]
pub struct CachedIdentities {
    cache: IdentityCache,
    store: Arc<Store>,
}

impl CachedIdentities {
    pub fn new(engine: Arc<UnifiedEngine>, store: Arc<Store>) -> Self {
        Self {
            cache: IdentityCache::new(engine),
            store,
        }
    }

    pub fn data_version(&self) -> &str {
        self.cache.data_version()
    }

    /// Resolve `name`, reading memo, then store, then the engine (writing through).
    ///
    /// Same input yields the same identity regardless of call order or thread. A
    /// store row that no longer parses is re-resolved rather than failing.
    pub fn resolve(&self, name: &str, ctx: &ResolveContext) -> Identity {
        let key = IdentityCache::input_key(name, ctx);
        if let Some(hit) = self.cache.memoized(&key) {
            return hit;
        }
        if let Ok(Some(json)) = self
            .store
            .identity_cache_get(&key, self.cache.data_version())
        {
            if let Some(identity) = IdentityCache::identity_from_json(&json) {
                self.cache.memoize(key, identity.clone());
                return identity;
            }
        }
        let identity = resolve_with(self.cache.engine(), name, ctx);
        self.cache.memoize(key.clone(), identity.clone());
        let json = IdentityCache::identity_json(&identity);
        let _ = self
            .store
            .identity_cache_put(&key, self.cache.data_version(), &json);
        identity
    }

    /// Resolve many names at once and leave them memoized and persisted: one store
    /// read pass for the names this run has not seen, the misses resolved on rayon, and
    /// ONE write transaction for what was computed. The batch twin of
    /// [`resolve`](Self::resolve) (same keys, same rows, same identities) for a run
    /// warming every channel and family member: per-name autocommits would cost one
    /// fsync each, serialized on the store's connection. Blocking: call it off the
    /// async runtime.
    pub fn warm<'a>(&self, names: impl IntoIterator<Item = (&'a str, &'a ResolveContext)>) {
        use rayon::prelude::*;
        let mut seen = std::collections::HashSet::new();
        let pending: Vec<(String, &str, &ResolveContext)> = names
            .into_iter()
            .filter_map(|(name, ctx)| {
                let key = IdentityCache::input_key(name, ctx);
                (self.cache.memoized(&key).is_none() && seen.insert(key.clone()))
                    .then_some((key, name, ctx))
            })
            .collect();
        if pending.is_empty() {
            return;
        }
        let keys: Vec<String> = pending.iter().map(|(key, _, _)| key.clone()).collect();
        let stored = self
            .store
            .identity_cache_get_many(&keys, self.cache.data_version())
            .unwrap_or_default();
        let computed: Vec<(String, Identity, bool)> = pending
            .par_iter()
            .map(|(key, name, ctx)| {
                if let Some(identity) = stored
                    .get(key)
                    .and_then(|json| IdentityCache::identity_from_json(json))
                {
                    return (key.clone(), identity, false);
                }
                (
                    key.clone(),
                    resolve_with(self.cache.engine(), name, ctx),
                    true,
                )
            })
            .collect();
        let mut writes = Vec::new();
        for (key, identity, fresh) in computed {
            if fresh {
                writes.push((key.clone(), IdentityCache::identity_json(&identity)));
            }
            self.cache.memoize(key, identity);
        }
        let _ = self
            .store
            .identity_cache_put_many(&writes, self.cache.data_version());
    }

    /// Rank candidates into the best identity family: the family from the engine's
    /// one ranker, each member's identity through [`CachedIdentities::resolve`]
    /// (`ctx` describes the query, as in `crosstalk_matching::identity::rank_with`).
    pub fn rank(
        &self,
        query: &str,
        candidates: &[String],
        ctx: &ResolveContext,
    ) -> Vec<FamilyMember> {
        let candidate_ctx = ResolveContext::default();
        self.cache
            .engine()
            .rank_in(query, ctx.country_hint.as_deref(), candidates)
            .into_iter()
            .map(|entry| FamilyMember {
                identity: self.resolve(&candidates[entry.index], &candidate_ctx),
                index: entry.index,
                score: entry.score,
                method: entry.method,
            })
            .collect()
    }

    /// Drop rows from older data versions. Returns rows removed.
    pub fn prune_stale(&self) -> Result<u64> {
        self.store.identity_cache_prune(self.cache.data_version())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_matching::engine::EngineConfig;

    fn setup() -> (CachedIdentities, tempfile::TempDir) {
        let engine = Arc::new(UnifiedEngine::new(&EngineConfig::default()));
        let dir = tempfile::tempdir().expect("dir");
        let store = Arc::new(Store::open(dir.path().join("test.db")).expect("store"));
        (CachedIdentities::new(engine, store), dir)
    }

    #[test]
    fn warming_in_bulk_writes_the_same_rows_resolve_reads() {
        let (cached, _dir) = setup();
        let ctx = ResolveContext::default();
        cached.resolve("CNN", &ctx);
        let names = ["ESPN HD", "US: ESPN", "CNN", "ESPN HD"];
        cached.warm(names.iter().map(|name| (*name, &ctx)));
        for name in names {
            let key = IdentityCache::input_key(name, &ctx);
            let json = cached
                .store
                .identity_cache_get(&key, cached.data_version())
                .expect("read")
                .expect("row");
            let direct = resolve_with(cached.cache.engine(), name, &ctx);
            assert_eq!(
                IdentityCache::identity_from_json(&json),
                Some(direct.clone())
            );
            // A fresh resolver on the same store reads the warmed row.
            let rerun =
                CachedIdentities::new(Arc::clone(cached.cache.engine()), Arc::clone(&cached.store));
            assert_eq!(rerun.resolve(name, &ctx), direct);
        }
    }

    #[test]
    fn resolve_writes_through_and_reads_back() {
        let (cached, _dir) = setup();
        let ctx = ResolveContext::default();
        let first = cached.resolve("US: ESPN HD", &ctx);
        let key = IdentityCache::input_key("US: ESPN HD", &ctx);
        let row = cached
            .store
            .identity_cache_get(&key, cached.data_version())
            .expect("read")
            .expect("row written");
        let back = IdentityCache::identity_from_json(&row).expect("parses");
        assert_eq!(first, back);
        // Second call hits memo/store and agrees.
        assert_eq!(cached.resolve("US: ESPN HD", &ctx), first);
    }

    #[test]
    fn a_settings_change_never_reads_old_rows() {
        let (cached, dir) = setup();
        let ctx = ResolveContext::default();
        let plain = cached.resolve("Cable News", &ctx);
        assert_eq!(plain.canonical_name, "Cable News");
        // Same store, a custom alias added: new namespace, fresh resolution.
        let mut custom_aliases = std::collections::HashMap::new();
        custom_aliases.insert("CNN".to_string(), vec!["Cable News".to_string()]);
        let engine = Arc::new(UnifiedEngine::new(&EngineConfig {
            custom_aliases,
            ..EngineConfig::default()
        }));
        let reopened = CachedIdentities::new(engine, Arc::clone(&cached.store));
        assert_ne!(reopened.data_version(), cached.data_version());
        assert_eq!(reopened.resolve("Cable News", &ctx).canonical_name, "CNN");
        assert_eq!(reopened.prune_stale().expect("prune"), 1);
        drop(dir);
    }

    #[test]
    fn rank_attaches_cached_identities() {
        let (cached, _dir) = setup();
        let candidates = vec![
            "UK: CNN".to_string(),
            "US: CNN HD".to_string(),
            "CNN".to_string(),
        ];
        let family = cached.rank("US: CNN", &candidates, &ResolveContext::default());
        let names: Vec<&str> = family
            .iter()
            .map(|member| candidates[member.index].as_str())
            .collect();
        assert_eq!(names, ["US: CNN HD", "CNN"]);
        for member in &family {
            let key =
                IdentityCache::input_key(&candidates[member.index], &ResolveContext::default());
            assert!(cached
                .store
                .identity_cache_get(&key, cached.data_version())
                .expect("read")
                .is_some());
        }
    }

    #[test]
    fn stale_versions_are_never_read_and_prune_drops_them() {
        let (cached, _dir) = setup();
        let ctx = ResolveContext::default();
        let identity = cached.resolve("ESPN", &ctx);
        let key = IdentityCache::input_key("ESPN", &ctx);
        cached
            .store
            .identity_cache_put(
                &key,
                "old-version",
                &IdentityCache::identity_json(&identity),
            )
            .expect("put");
        assert!(cached
            .store
            .identity_cache_get(&key, "old-version")
            .expect("read")
            .is_some());
        // The live version misses (different namespace), then resolves + stores.
        let live_miss = cached
            .store
            .identity_cache_get(&key, cached.data_version())
            .expect("read");
        assert!(live_miss.is_some(), "resolve stored the live version");
        let removed = cached.prune_stale().expect("prune");
        assert_eq!(removed, 1);
    }
}
