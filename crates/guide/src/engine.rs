//! Guide's view of the unified matching layer (phase 3 step 8, goal reset).
//!
//! One ranker, one selection rule, one cache: every query (channel) and every candidate
//! (EPG entry) resolves through the [`UnifiedEngine`] + identity cache
//! ([`CachedIdentities`]: store-backed and version-keyed when the run has a store, an
//! in-memory [`IdentityCache`] otherwise), ranking is the engine's `rank` (best identity
//! family), and selection is the family's members in rank order — the run's walk then
//! assigns the first member that carries programme data. The one cut is `[matching]`
//! sensitivity in `crosstalk.toml` ([`MatchingConfig::threshold`]); guide's own
//! thresholds are retired.
//!
//! The AI gate still applies to borderline channels (see `matching.rs`): a channel with
//! no family at the cut is re-ranked by the same engine at the `[ai.verify]` floor
//! ([`RunEngine::shortlist_engine`]) so the verify band and rescue candidates reach the
//! chain. The engine's callsign anchor (C06/D04) decides callsign agreement at family level, and Janitor's
//! standalone structural callsign/state/city/network score (D06, sole implementation,
//! no evidence) is retired — it never reaches a family on its own.

use crosstalk_core::config::{AiVerifyConfig, MatchingConfig};
use crosstalk_core::identity_cache::CachedIdentities;
use crosstalk_core::store::Store;
use crosstalk_matching::engine::{EngineIndex, EngineRanked, UnifiedEngine};
use crosstalk_matching::identity::{Identity, IdentityCache, ResolveContext};
use std::sync::Arc;

/// The report preamble line naming the decision cut a run used, e.g.
/// `strict, cut 95 out of 100, higher is stricter`: the cut lives in `[matching]`, not in
/// `guide.toml`, so the guide schema alone would leave it out of the CSV.
pub fn cut_setting(matching: &MatchingConfig) -> (&'static str, String) {
    (
        "Matching Sensitivity ([matching] in crosstalk.toml)",
        format!(
            "{}, cut {} out of 100, higher is stricter",
            matching.sensitivity.trim(),
            matching.threshold()
        ),
    )
}

/// The unified stack one guide run scores and resolves with.
#[derive(Debug, Clone)]
pub struct RunEngine {
    engine: Arc<UnifiedEngine>,
    cached: Option<CachedIdentities>,
    memory: IdentityCache,
}

impl RunEngine {
    /// Build the run's engine from the host `[matching]` section, store-backed when the
    /// run has a store to persist identities in.
    pub fn for_config(matching: &MatchingConfig, store: Option<Store>) -> Self {
        let engine = Arc::new(UnifiedEngine::new(&matching.engine_config()));
        let cached = store.map(|store| CachedIdentities::new(Arc::clone(&engine), Arc::new(store)));
        let memory = IdentityCache::new(Arc::clone(&engine));
        Self {
            engine,
            cached,
            memory,
        }
    }

    /// The one decision cut in force (D02/T10 `janitor-95`).
    pub fn threshold(&self) -> u8 {
        self.engine.threshold()
    }

    pub fn engine(&self) -> &Arc<UnifiedEngine> {
        &self.engine
    }

    /// The cut the AI shortlist ranks at, when the `[ai.verify]` gate can fire at all:
    /// the lowest score the gate would offer (the rescue floor with rescue on, else the
    /// verify band's low end), and only when it sits below the decision cut — a band at
    /// or above the cut is all confident families, which the model never re-checks.
    pub fn shortlist_cut(&self, ai: &AiVerifyConfig) -> Option<u8> {
        if !ai.enabled {
            return None;
        }
        let floor = if ai.rescue {
            ai.rescue_floor.min(ai.verify_band_low)
        } else {
            ai.verify_band_low
        };
        (floor < self.threshold()).then_some(floor)
    }

    /// The run's engine re-cut at [`shortlist_cut`](Self::shortlist_cut): the same
    /// ranker over the same index, keeping the sub-cut candidates the gate offers
    /// (verify band and rescue). `None` when the gate cannot fire.
    pub fn shortlist_engine(&self, ai: &AiVerifyConfig) -> Option<Arc<UnifiedEngine>> {
        self.shortlist_cut(ai)
            .map(|cut| Arc::new(self.engine.with_threshold(cut)))
    }

    /// Resolve one name through the shared cache (memo, then store, then the engine).
    ///
    /// Same input yields the same identity whether guide or the engine directly resolves
    /// it: resolution is a pure function of (input, version), and the store row is
    /// namespaced by [`UnifiedEngine::identity_version`] (merged data + decisions +
    /// engine version + identity-affecting settings).
    pub fn resolve(&self, name: &str, ctx: &ResolveContext) -> Identity {
        match &self.cached {
            Some(cached) => cached.resolve(name, ctx),
            None => self.memory.resolve(name, ctx),
        }
    }

    /// Normalize a whole candidate list once, for every query to share.
    pub fn build_index(&self, names: &[String]) -> EngineIndex {
        EngineIndex::build(names, &self.engine, &ResolveContext::default())
    }

    /// Persist the identity of every query and every family member in one batch, so
    /// another app resolving the same name reads this run's row.
    ///
    /// One store read pass plus ONE write transaction, resolved on rayon off the async
    /// runtime ([`CachedIdentities::warm`]): at 20k channels and their families a
    /// per-name autocommit would cost one fsync each. The run itself scores on the
    /// engine index and never reads these identities back, so without a store there is
    /// nothing to persist and this is a no-op. `skip[i]` leaves query `i` (and its
    /// family) out, for channels the run does not match.
    pub async fn warm_families(
        &self,
        queries: &[String],
        families: &[Vec<EngineRanked>],
        candidates: &[String],
        skip: &[bool],
    ) {
        let Some(cached) = self.cached.clone() else {
            return;
        };
        let mut names: Vec<String> = Vec::new();
        for (ordinal, query) in queries.iter().enumerate() {
            if skip.get(ordinal).copied().unwrap_or(false) {
                continue;
            }
            names.push(query.clone());
            for member in families.get(ordinal).into_iter().flatten() {
                if let Some(name) = candidates.get(member.index) {
                    names.push(name.clone());
                }
            }
        }
        // Queries and EPG entries both resolve without a hint (no lineup country or
        // category in guide), so one context serves every name.
        let _ = tokio::task::spawn_blocking(move || {
            let ctx = ResolveContext::default();
            cached.warm(names.iter().map(|name| (name.as_str(), &ctx)));
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matching() -> MatchingConfig {
        MatchingConfig::default()
    }

    #[test]
    fn the_default_cut_is_strict() {
        // D02/T10 `janitor-95`: one threshold, and it ships at 95.
        let engine = RunEngine::for_config(&matching(), None);
        assert_eq!(engine.threshold(), 95);
    }

    #[test]
    fn the_same_channel_resolves_to_the_same_identity_through_any_cache() {
        // Cross-app consistency: guide (store-backed), a second store-backed resolver on
        // the same store, the in-memory fallback and the bare engine must all agree —
        // resolution is a pure function of (input, version), never of who asks.
        let store = Store::in_memory().expect("store");
        let guided = RunEngine::for_config(&matching(), Some(store.clone()));
        let ctx = ResolveContext::default();
        let via_guide = guided.resolve("US: ESPN HD", &ctx);

        let engine = UnifiedEngine::new(&matching().engine_config());
        let direct = crosstalk_matching::identity::resolve_with(&engine, "US: ESPN HD", &ctx);
        assert_eq!(via_guide, direct);

        // A second resolver on the same store reads the row the first wrote.
        let rerun = CachedIdentities::new(Arc::new(engine), Arc::new(store.clone()));
        assert_eq!(rerun.resolve("US: ESPN HD", &ctx), via_guide);

        // Without a store the memo still agrees.
        let memory = RunEngine::for_config(&matching(), None);
        assert_eq!(memory.resolve("US: ESPN HD", &ctx), via_guide);

        // The version namespaces the rows: a rule change misses every old identity.
        assert!(guided.engine().identity_version().contains("-eng:2"));
    }

    #[tokio::test]
    async fn warming_families_persists_every_query_and_member() {
        let store = Store::in_memory().expect("store");
        let guided = RunEngine::for_config(&matching(), Some(store.clone()));
        let candidates = vec![
            "ESPN".to_string(),
            "US: ESPN HD".to_string(),
            "ESPN 2".to_string(),
        ];
        let index = guided.build_index(&candidates);
        let queries = vec!["ESPN HD".to_string(), "CNN".to_string()];
        let families: Vec<Vec<EngineRanked>> = queries
            .iter()
            .map(|query| guided.engine().rank_index(query, &index))
            .collect();
        assert!(!families[0].is_empty());
        guided
            .warm_families(&queries, &families, &candidates, &[false, true])
            .await;
        let cached = |name: &str| {
            let key = IdentityCache::input_key(name, &ResolveContext::default());
            store
                .identity_cache_get(&key, &guided.engine().identity_version())
                .expect("read")
                .is_some()
        };
        assert!(cached("ESPN HD"), "the query is cached");
        for member in &families[0] {
            assert!(
                cached(&candidates[member.index]),
                "{} was cached",
                candidates[member.index]
            );
        }
        assert!(!cached("CNN"), "a skipped query is not");
    }

    #[test]
    fn the_shortlist_engine_follows_the_ai_policy() {
        let engine = RunEngine::for_config(&matching(), None);
        let mut ai = AiVerifyConfig::default();
        assert_eq!(
            engine.shortlist_cut(&ai),
            None,
            "the gate is off by default"
        );
        ai.enabled = true;
        assert_eq!(
            engine.shortlist_cut(&ai),
            Some(ai.rescue_floor.min(ai.verify_band_low))
        );
        ai.rescue = false;
        assert_eq!(engine.shortlist_cut(&ai), Some(ai.verify_band_low));
        let shortlist = engine.shortlist_engine(&ai).expect("engine");
        assert_eq!(shortlist.threshold(), ai.verify_band_low);
        // A band at or above the cut is all confident families: nothing to shortlist.
        let mut low = matching();
        low.sensitivity = "low".to_string();
        let engine = RunEngine::for_config(&low, None);
        assert_eq!(engine.shortlist_cut(&ai), None);
    }
}
