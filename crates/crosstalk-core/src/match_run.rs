//! The matching walk: rank, threshold, tie-break, and validate one channel's candidates.
//!
//! Port of the walk inside `Plugin._find_best_epg_match`: the highest-scoring candidate that
//! both clears the threshold and carries what the app needs is the one that wins. The
//! candidates are ranked by `crosstalk-matching`; this module walks that order, one channel
//! at a time, and only the check itself is app-specific.
//!
//! Guide walks EPG entries looking for programme data, streams will walk streams looking for a
//! live one, lineup will walk entries looking for a logo. The order and the tie-break are the
//! same for all of them, which is why they live here rather than in each app.
//!
//! The parallel scoring stage in front of the walk is shared too ([`score_names`]): every app
//! scores a list of query names against the same kind of candidate index, and the work scales
//! with cores rather than with the async runtime.

use crate::dispatcharr::EpgEntry;
use crate::jobs::{JobContext, ProgressEvent};
use anyhow::{Context, Result};
use crosstalk_matching::pipeline::{CandidateIndex, Matcher, Scored};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Channels per progress tick in the scoring stage. At 20k channels the stage runs for
/// seconds, which is long enough to go silent otherwise.
const PROGRESS_EVERY: usize = 500;

/// A candidate the engine can rank and validate.
///
/// Guide's candidate is an EPG entry; streams' will be a stream, lineup's a lineup entry.
pub trait Candidate {
    fn id(&self) -> i64;
    fn name(&self) -> &str;

    /// Extra fields the AI gate sends about this candidate, so the model can tell two
    /// same-name candidates apart: an EPG entry's `tvg_id` and source, say.
    ///
    /// Empty by default, because most candidates need no more than their name.
    fn request_fields(&self) -> Vec<(&'static str, serde_json::Value)> {
        Vec::new()
    }
}

/// An EPG entry as a candidate: its name ranks it, the walk probes it for programme data.
///
/// Lives here rather than in the gate so the walk is self-contained: matching needs the name,
/// probing needs the entry, and the model half only adds wording on top.
impl Candidate for EpgEntry {
    fn id(&self) -> i64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn request_fields(&self) -> Vec<(&'static str, serde_json::Value)> {
        let mut fields = vec![("tvg_id", serde_json::json!(self.tvg_id))];
        if let Some(source) = self.epg_source {
            fields.push(("epg_source", serde_json::json!(source)));
        }
        fields
    }
}

/// The app-specific half of a walk: does this candidate actually carry something usable?
///
/// Guide asks Dispatcharr whether the entry has programme data; streams will ask whether the
/// stream is alive. A failed check is "no", never an error: one broken candidate must not fail
/// a run that can still match the rest.
#[async_trait::async_trait]
pub trait CandidateProbe<C: Candidate>: Send + Sync {
    async fn carries_data(&self, candidate: &C) -> bool;
}

/// A probe for callers with nothing to check: always carries data.
///
/// Pass this with [`WalkRules::validate`] off, where the walk never consults the probe but
/// still needs one to be generic over. A caller that actually validates something implements
/// [`CandidateProbe`] for its own check instead.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoProbe;

#[async_trait::async_trait]
impl<C: Candidate + Sync> CandidateProbe<C> for NoProbe {
    async fn carries_data(&self, _candidate: &C) -> bool {
        true
    }
}

/// How a walk decides what wins.
#[derive(Debug, Clone, Copy)]
pub struct WalkRules {
    /// The lowest score that may win.
    pub threshold: u8,
    /// Consult the probe at all. When off, the first candidate that clears the threshold wins
    /// without a check — the plugin's `allow_epg_without_programs` escape hatch.
    pub validate: bool,
}

/// What one channel's walk found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalkResult {
    /// Position in the channel's ranked list that won, if any.
    pub chosen: Option<usize>,
    /// Score of the last candidate rejected for carrying no data, if any.
    pub best_rejected: Option<u8>,
    /// The model's pick and its score, when it accepted a name whose every same-name
    /// candidate is empty.
    pub ai_empty: Option<(String, u8)>,
}

impl WalkResult {
    /// The "no result" value, which a cancelled walk returns.
    fn empty() -> Self {
        Self::default()
    }
}

/// One channel's validation walk: the first ranked candidate that clears the threshold AND
/// carries the data the probe asks for wins.
///
/// An accepted AI pick replaces the heuristic order: every same-name candidate is validated in
/// ranked order and the first live duplicate wins, even when the exact picked candidate is
/// empty. When every same-name copy is empty the channel is left unmatched with an `ai_empty`
/// result saying exactly that — there is no fallback to other heuristic candidates, which by
/// construction all scored below the threshold (the model only runs when nothing cleared it),
/// so falling back would contradict the threshold itself.
///
/// Serial per channel and dependency-free across channels, so a caller runs one of these per
/// channel concurrently and resolves the winners in channel order.
pub async fn walk<C, P>(
    context: &JobContext,
    candidates: &[C],
    ranked: &[Scored],
    accepted: Option<usize>,
    rules: WalkRules,
    probe: &P,
) -> WalkResult
where
    C: Candidate + Sync,
    P: CandidateProbe<C>,
{
    // Every probe call below is preceded by a cancel check so a cancel lands within one
    // in-flight round. The empty result is the "no result" value: the caller's batch check
    // turns it into an abort.
    if context.is_cancelled() {
        return WalkResult::empty();
    }

    if let Some(position) = accepted {
        let picked_name = ranked.get(position).and_then(|candidate| {
            candidates
                .get(candidate.index)
                .map(|entry| entry.name().to_string())
        });
        if let Some(picked_name) = picked_name {
            for (rank_position, candidate) in ranked.iter().enumerate() {
                let Some(entry) = candidates.get(candidate.index) else {
                    continue;
                };
                if entry.name() != picked_name {
                    continue;
                }
                let has = if !rules.validate {
                    true
                } else {
                    if context.is_cancelled() {
                        return WalkResult::empty();
                    }
                    probe.carries_data(entry).await
                };
                if has {
                    return WalkResult {
                        chosen: Some(rank_position),
                        ..WalkResult::empty()
                    };
                }
            }
            let score = ranked.get(position).map(|candidate| candidate.score);
            return WalkResult {
                chosen: None,
                best_rejected: score,
                ai_empty: score.map(|score| (picked_name, score)),
            };
        }
        return WalkResult::empty();
    }

    let mut best_rejected = None;
    for (rank_position, candidate) in ranked.iter().enumerate() {
        if context.is_cancelled() {
            return WalkResult::empty();
        }
        let Some(entry) = candidates.get(candidate.index) else {
            continue;
        };
        if candidate.score < rules.threshold {
            continue;
        }
        if !rules.validate {
            return WalkResult {
                chosen: Some(rank_position),
                best_rejected,
                ai_empty: None,
            };
        }
        if context.is_cancelled() {
            return WalkResult::empty();
        }
        if probe.carries_data(entry).await {
            return WalkResult {
                chosen: Some(rank_position),
                best_rejected,
                ai_empty: None,
            };
        }
        best_rejected = Some(candidate.score);
    }
    WalkResult {
        chosen: None,
        best_rejected,
        ai_empty: None,
    }
}

/// The score bands the plugins report matches in, highest first.
///
/// The boundaries are the plugin's own; each app supplies its own noun, because the wording
/// differs ("Excellent match" against "Excellent replacement match") and the reports are
/// compared against the plugin's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceBand {
    Excellent,
    High,
    Good,
    Moderate,
    Low,
}

impl ConfidenceBand {
    pub fn of(score: u8) -> Self {
        if score >= 95 {
            ConfidenceBand::Excellent
        } else if score >= 85 {
            ConfidenceBand::High
        } else if score >= 75 {
            ConfidenceBand::Good
        } else if score >= 60 {
            ConfidenceBand::Moderate
        } else {
            ConfidenceBand::Low
        }
    }
}

/// Where the parallel scoring stage publishes progress.
pub struct ScoreProgress<'a> {
    /// The run that receives the progress events.
    pub context: &'a JobContext,
    /// The run's progress label, for example `guide.match`.
    pub action: &'a str,
    /// The phase, for example `scoring channels`.
    pub phase: &'a str,
}

/// Score every query name against the candidate index, in parallel.
///
/// Runs on a blocking thread with rayon, so a big scope never stalls the async runtime. Order
/// is preserved on collect, so the result is in query order. `progress` is optional because a
/// short stage (heal scores only the broken channels) is not worth a progress bar.
pub async fn score_names(
    names: &[String],
    matcher: Arc<Matcher>,
    index: Arc<CandidateIndex>,
    tags: &[String],
    progress: Option<ScoreProgress<'_>>,
) -> Result<Vec<Vec<Scored>>> {
    let total = names.len();
    let names = names.to_vec();
    let tags = tags.to_vec();
    let progress = progress.map(|progress| {
        (
            progress.context.clone(),
            progress.action.to_string(),
            progress.phase.to_string(),
        )
    });

    tokio::task::spawn_blocking(move || {
        use rayon::prelude::*;
        let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
        let counter = AtomicUsize::new(0);
        // Published `current` must be monotonic: rayon workers finish out of order, so a
        // later-finishing worker with a smaller `done` must not move the bar backwards. Only
        // the largest value so far is published.
        let max_sent = AtomicUsize::new(0);
        names
            .par_iter()
            .map(|name| {
                let ranked = matcher.score_candidates(name, &index, &tag_refs, None);
                if let Some((context, action, phase)) = progress.as_ref() {
                    let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if done % PROGRESS_EVERY == 0 || done == total {
                        let prev = max_sent.fetch_max(done, Ordering::Relaxed);
                        if done > prev {
                            context.progress(&ProgressEvent::new(
                                action.as_str(),
                                done as u64,
                                total as u64,
                                phase.as_str(),
                            ));
                        }
                    }
                }
                ranked
            })
            .collect()
    })
    .await
    .context("the scoring stage panicked")
}

/// Whether an AI gate would ask the model about this channel's best candidate.
///
/// Pure, so it stays serial: the requests that follow are what fan out.
pub fn needs_gate(best_score: Option<u8>, threshold: u8) -> bool {
    !best_score.is_some_and(|score| score >= threshold)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::VecSink;

    /// A candidate with a name and a flag, standing in for whatever an app matches.
    struct Entry {
        id: i64,
        name: String,
        live: bool,
    }

    impl Candidate for Entry {
        fn id(&self) -> i64 {
            self.id
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// Answer from the candidate's own flag, and count the calls.
    struct FlagProbe {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl CandidateProbe<Entry> for FlagProbe {
        async fn carries_data(&self, candidate: &Entry) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst);
            candidate.live
        }
    }

    fn probe() -> FlagProbe {
        FlagProbe {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn entry(id: i64, name: &str, live: bool) -> Entry {
        Entry {
            id,
            name: name.to_string(),
            live,
        }
    }

    fn scored(score: u8, index: usize) -> Scored {
        Scored {
            index,
            score,
            components: vec!["Fuzzy"],
        }
    }

    fn rules(threshold: u8) -> WalkRules {
        WalkRules {
            threshold,
            validate: true,
        }
    }

    #[tokio::test]
    async fn the_best_live_candidate_wins() {
        let context = JobContext::for_tests(Default::default());
        let candidates = vec![entry(1, "A", true), entry(2, "B", false)];
        let ranked = vec![scored(100, 0), scored(90, 1)];
        let walked = walk(&context, &candidates, &ranked, None, rules(85), &probe()).await;
        assert_eq!(walked.chosen, Some(0));
    }

    #[tokio::test]
    async fn a_candidate_below_the_threshold_is_never_checked() {
        let context = JobContext::for_tests(Default::default());
        let probe = probe();
        let candidates = vec![entry(1, "A", true)];
        let ranked = vec![scored(70, 0)];
        let walked = walk(&context, &candidates, &ranked, None, rules(85), &probe).await;
        assert_eq!(walked, WalkResult::empty());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_dead_candidate_is_rejected_and_reported() {
        let context = JobContext::for_tests(Default::default());
        let candidates = vec![entry(1, "A", false)];
        let ranked = vec![scored(99, 0)];
        let walked = walk(&context, &candidates, &ranked, None, rules(85), &probe()).await;
        assert_eq!(walked.chosen, None);
        assert_eq!(walked.best_rejected, Some(99));
    }

    #[tokio::test]
    async fn validation_off_wins_without_a_probe() {
        let context = JobContext::for_tests(Default::default());
        let probe = probe();
        let candidates = vec![entry(1, "A", false)];
        let ranked = vec![scored(99, 0)];
        let walked = walk(
            &context,
            &candidates,
            &ranked,
            None,
            WalkRules {
                threshold: 85,
                validate: false,
            },
            &probe,
        )
        .await;
        assert_eq!(walked.chosen, Some(0));
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_accepted_pick_falls_through_to_a_live_same_name_duplicate() {
        let context = JobContext::for_tests(Default::default());
        let candidates = vec![entry(1, "Same", false), entry(2, "Same", true)];
        let ranked = vec![scored(90, 0), scored(80, 1)];
        // The model picked position 0, which is empty; the duplicate at 1 is live.
        let walked = walk(&context, &candidates, &ranked, Some(0), rules(95), &probe()).await;
        assert_eq!(walked.chosen, Some(1));
        assert_eq!(walked.ai_empty, None);
    }

    #[tokio::test]
    async fn an_accepted_pick_with_no_live_duplicate_is_an_ai_empty_result() {
        let context = JobContext::for_tests(Default::default());
        let candidates = vec![entry(1, "Same", false), entry(2, "Same", false)];
        let ranked = vec![scored(90, 0), scored(80, 1)];
        let walked = walk(&context, &candidates, &ranked, Some(0), rules(95), &probe()).await;
        assert_eq!(walked.chosen, None);
        assert_eq!(walked.ai_empty, Some(("Same".to_string(), 90)));
        assert_eq!(walked.best_rejected, Some(90));
    }

    #[tokio::test]
    async fn a_cancelled_walk_validates_nothing() {
        let context = JobContext::for_tests(Default::default());
        context.cancel();
        let probe = probe();
        let candidates = vec![entry(1, "A", true)];
        let ranked = vec![scored(100, 0)];
        let walked = walk(&context, &candidates, &ranked, None, rules(85), &probe).await;
        assert_eq!(walked, WalkResult::empty());
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn confidence_bands_follow_the_plugins_boundaries() {
        assert_eq!(ConfidenceBand::of(100), ConfidenceBand::Excellent);
        assert_eq!(ConfidenceBand::of(95), ConfidenceBand::Excellent);
        assert_eq!(ConfidenceBand::of(94), ConfidenceBand::High);
        assert_eq!(ConfidenceBand::of(85), ConfidenceBand::High);
        assert_eq!(ConfidenceBand::of(84), ConfidenceBand::Good);
        assert_eq!(ConfidenceBand::of(75), ConfidenceBand::Good);
        assert_eq!(ConfidenceBand::of(74), ConfidenceBand::Moderate);
        assert_eq!(ConfidenceBand::of(60), ConfidenceBand::Moderate);
        assert_eq!(ConfidenceBand::of(59), ConfidenceBand::Low);
    }

    #[test]
    fn only_a_channel_without_a_confident_match_needs_the_gate() {
        assert!(!needs_gate(Some(95), 95));
        assert!(!needs_gate(Some(100), 95));
        assert!(needs_gate(Some(94), 95));
        assert!(needs_gate(None, 95));
    }

    #[tokio::test]
    async fn scoring_stays_in_query_order_and_does_not_report_without_a_progress_sink() {
        let sink = Arc::new(VecSink::new());
        let _context = JobContext::new(
            Arc::new(crate::config::CrosstalkConfig::default()),
            sink.clone(),
        );
        let matcher = Arc::new(Matcher::with_shipped_defaults());
        let names = vec!["ESPN".to_string(), "CNN".to_string()];
        let index = Arc::new(CandidateIndex::build(
            &["ESPN HD".to_string(), "CNN".to_string()],
            matcher.normalizer(),
            matcher.known(),
        ));
        let scored = score_names(&names, Arc::clone(&matcher), index, &matcher.tags(), None)
            .await
            .expect("scored");
        assert_eq!(scored.len(), 2);
        assert_eq!(scored[0][0].index, 0);
        assert_eq!(scored[1][0].index, 1);
        assert!(sink.events().is_empty(), "no progress was requested");
    }
}
