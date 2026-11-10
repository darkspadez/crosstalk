//! The unified run engine: one ranker, one selection rule, one cache, shared by apps.
//!
//! Phase 3 step 9 (goal reset): [`RunEngine`] generalises guide's step-8 run engine
//! (`crates/guide/src/engine.rs`) into the host, used by every migrated app (guide,
//! streams, lineup). One ranker (the [`UnifiedEngine`]'s best identity family), one
//! selection rule (the family's members in rank order at the one `[matching]` cut),
//! one cache (store-backed and version-keyed when the run has a store, in-memory
//! otherwise).
//!
//! The AI contract is the same everywhere (step-8 review): a channel with an EMPTY
//! family is the borderline case. It is re-ranked by the same engine and index at the
//! gate's floor ([`RunEngine::shortlist_engine`]: [`UnifiedEngine::with_threshold`] +
//! [`UnifiedEngine::shortlist_index_in`] via [`match_run::score_shortlists`]), and
//! only a model accept can promote a sub-cut candidate — the walk still accepts
//! nothing below the cut.

use crate::config::{AiVerifyConfig, MatchingConfig};
use crate::identity_cache::CachedIdentities;
use crate::store::Store;
use crosstalk_matching::engine::{EngineIndex, EngineRanked, UnifiedEngine};
use crosstalk_matching::identity::{Identity, IdentityCache, ResolveContext};
use std::sync::Arc;

/// The report preamble line naming the decision cut a run used, e.g.
/// `strict, cut 95 out of 100, higher is stricter`: the cut lives in `[matching]`,
/// not in any one app's settings file, so an app schema alone would leave it out of
/// the CSV.
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

/// The unified stack one run scores and resolves with, shared by every migrated app.
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
        Self::with_engine(
            Arc::new(UnifiedEngine::new(&matching.engine_config())),
            store,
        )
    }

    /// The same, around an engine the run already built from `[matching]` (an app
    /// that ranks before it gates must not build, and load the merged data, twice).
    pub fn with_engine(engine: Arc<UnifiedEngine>, store: Option<Store>) -> Self {
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
    /// Same input yields the same identity whether guide, streams or the engine
    /// directly resolves it: resolution is a pure function of (input, version), and
    /// the store row is namespaced by [`UnifiedEngine::identity_version`] (merged
    /// data + decisions + engine version + identity-affecting settings).
    pub fn resolve(&self, name: &str, ctx: &ResolveContext) -> Identity {
        match &self.cached {
            Some(cached) => cached.resolve(name, ctx),
            None => self.memory.resolve(name, ctx),
        }
    }

    /// Normalize a whole candidate list once, for every query to share.
    ///
    /// CPU-bound (parallel inside, E-S3): an async caller uses
    /// [`build_index_blocking`](Self::build_index_blocking) instead.
    pub fn build_index(&self, names: &[String]) -> EngineIndex {
        EngineIndex::build(names, &self.engine, &ResolveContext::default())
    }

    /// [`build_index`](Self::build_index) on a blocking thread, so a 50k-name index
    /// never stalls the async runtime (E-S3).
    pub async fn build_index_blocking(&self, names: &[String]) -> anyhow::Result<EngineIndex> {
        let engine = Arc::clone(&self.engine);
        let names = names.to_vec();
        tokio::task::spawn_blocking(move || {
            EngineIndex::build(&names, &engine, &ResolveContext::default())
        })
        .await
        .map_err(|error| anyhow::anyhow!("building the candidate index panicked: {error}"))
    }

    /// Resolve every name once in a batch and leave it memoized: one store read pass,
    /// the misses on rayon, ONE write transaction ([`CachedIdentities::warm`]). After
    /// this, [`resolve`](Self::resolve) on those names is a memo hit, so a rayon stage
    /// never takes the store lock or pays an fsync per name (E-B3). Without a store
    /// there is nothing to batch: the memo fills on first resolve. Blocking.
    pub fn warm(&self, names: &[(String, ResolveContext)]) {
        if let Some(cached) = &self.cached {
            cached.warm(names.iter().map(|(name, ctx)| (name.as_str(), ctx)));
        }
    }

    /// Warm `names`, then run `work` with this engine on a blocking thread (E-B3): the
    /// rayon stage of an app never runs on the async runtime, and every resolve inside
    /// it reads the memo the warm filled.
    pub async fn resolve_stage<T, F>(
        &self,
        names: Vec<(String, ResolveContext)>,
        work: F,
    ) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&RunEngine) -> T + Send + 'static,
    {
        let engine = self.clone();
        tokio::task::spawn_blocking(move || {
            engine.warm(&names);
            work(&engine)
        })
        .await
        .map_err(|error| anyhow::anyhow!("the resolve stage panicked: {error}"))
    }

    /// Persist the identity of every query and every family member in one batch, so
    /// another app resolving the same name reads this run's row.
    ///
    /// One store read pass plus ONE write transaction, resolved on rayon off the async
    /// runtime ([`CachedIdentities::warm`]): at 20k channels and their families a
    /// per-name autocommit would cost one fsync each. The run itself scores on the
    /// engine index and never reads these identities back, so without a store this is
    /// a no-op. `skip[i]` leaves query `i` (and its family) out, for channels the run
    /// does not match.
    pub async fn warm_families(
        &self,
        queries: &[String],
        families: &[Vec<EngineRanked>],
        candidates: &[String],
        skip: &[bool],
    ) {
        self.start_warming_families(queries, families, candidates, skip)
            .finish()
            .await;
    }

    /// [`warm_families`](Self::warm_families), started in the background (E-B2): the
    /// run never reads these identities back, so it starts the warm as soon as the
    /// families exist, carries on with its network stages (programme checks, the AI
    /// gate, the writes) while the warm runs, and only awaits [`Warming::finish`] before
    /// it returns, so the rows are in the store when the run ends, as before.
    pub fn start_warming_families(
        &self,
        queries: &[String],
        families: &[Vec<EngineRanked>],
        candidates: &[String],
        skip: &[bool],
    ) -> Warming {
        let Some(cached) = self.cached.clone() else {
            return Warming(None);
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
        // Queries and family members both resolve without a hint (no lineup country or
        // category in guide/streams), so one context serves every name.
        Warming(Some(tokio::task::spawn_blocking(move || {
            let ctx = ResolveContext::default();
            cached.warm(names.iter().map(|name| (name.as_str(), &ctx)));
        })))
    }
}

/// An identity warm running in the background ([`RunEngine::start_warming_families`]).
///
/// Dropping it detaches the warm (a cancelled run still persists what it resolved);
/// [`finish`](Self::finish) waits for it.
#[must_use = "await `finish` so the identities are stored before the run ends"]
pub struct Warming(Option<tokio::task::JoinHandle<()>>);

impl Warming {
    /// Wait for the warm to land in the store.
    pub async fn finish(self) {
        if let Some(handle) = self.0 {
            let _ = handle.await;
        }
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
        // Cross-app consistency: guide, streams, a second store-backed resolver on the
        // same store, the in-memory fallback and the bare engine must all agree —
        // resolution is a pure function of (input, version), never of who asks.
        let store = Store::in_memory().expect("store");
        let run = RunEngine::for_config(&matching(), Some(store.clone()));
        let ctx = ResolveContext::default();
        let via_run = run.resolve("US: ESPN HD", &ctx);

        let engine = UnifiedEngine::new(&matching().engine_config());
        let direct = crosstalk_matching::identity::resolve_with(&engine, "US: ESPN HD", &ctx);
        assert_eq!(via_run, direct);

        // A second resolver on the same store reads the row the first wrote.
        let rerun = CachedIdentities::new(Arc::new(engine), Arc::new(store.clone()));
        assert_eq!(rerun.resolve("US: ESPN HD", &ctx), via_run);

        // Without a store the memo still agrees.
        let memory = RunEngine::for_config(&matching(), None);
        assert_eq!(memory.resolve("US: ESPN HD", &ctx), via_run);

        // The version namespaces the rows: a rule change misses every old identity.
        // `-eng:7`: a language-word parenthetical stays in the key (N16, after C08's `-eng:6`).
        assert!(run.engine().identity_version().contains("-eng:7"));
    }

    #[tokio::test]
    async fn warming_families_persists_every_query_and_member() {
        let store = Store::in_memory().expect("store");
        let run = RunEngine::for_config(&matching(), Some(store.clone()));
        let candidates = vec![
            "ESPN".to_string(),
            "US: ESPN HD".to_string(),
            "ESPN 2".to_string(),
        ];
        let index = run.build_index(&candidates);
        let queries = vec!["ESPN HD".to_string(), "CNN".to_string()];
        let families: Vec<Vec<EngineRanked>> = queries
            .iter()
            .map(|query| run.engine().rank_index(query, &index))
            .collect();
        assert!(!families[0].is_empty());
        run.warm_families(&queries, &families, &candidates, &[false, true])
            .await;
        let cached = |name: &str| {
            let key = IdentityCache::input_key(name, &ResolveContext::default());
            store
                .identity_cache_get(&key, &run.engine().identity_version())
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
