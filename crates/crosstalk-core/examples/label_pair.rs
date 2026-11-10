//! Corpus labeller for `scripts/corpus/build.py --ai` (plan step 5).
//!
//! Reads `{"query", "candidate"}` JSON objects, one per line, on stdin; asks the
//! `[ai.verify]` chain whether each pair names the same television channel; and
//! writes one `{"query", "candidate", "probability", "decided", "provider",
//! "model"}` JSON object per line, in input order, on stdout. `decided` is true
//! when the deciding link's probability reached `accept_threshold` or fell to
//! `reject_threshold`; the labeller relabels only decided pairs.
//!
//! The chain is the operator's own: configuration loads from `--config-dir`
//! (default `./config`) with `CROSSTALK_`-prefixed environment overrides, so
//! keys arrive as `CROSSTALK_AI__...` overrides and are never written to disk.
//! The request is [`crosstalk_core::ai::yes_no_request`] and the answer is read
//! by [`AiService::decide_yes_no`]: the same entry point the runtime gate uses,
//! so the labels the corpus gets are the labels the apps would get.
//!
//! `--fake <mode>` replaces every configured provider with an in-process stub
//! (no network) for tests: `same-if-normalized-equal` answers 0.95 when the
//! two names fold equal after lowercasing and stripping non-alphanumerics,
//! 0.05 otherwise. `scripts/corpus/build.py --check` and CI never touch the
//! network; the `fake_labels_pairs` test below pins the wiring.
//!
//! A failing link escalates to the next one; a pair no link settles keeps a
//! null probability, and the Python side keeps its majority label.
use std::collections::BTreeMap;
use std::io::{BufRead, Write};

use crosstalk_core::ai::{self, AiService, Answer, Decider, DecisionRequest, DecisionResponse};
use crosstalk_core::config::{AiChainEntry, CrosstalkConfig};

#[derive(Debug, serde::Deserialize)]
struct Job {
    query: String,
    candidate: String,
}

#[derive(Debug, serde::Serialize)]
struct Answer_ {
    query: String,
    candidate: String,
    probability: Option<f64>,
    decided: bool,
    provider: String,
    model: String,
}

/// Whether a yes-probability settles the question at the chain's thresholds.
fn settled(probability: Option<f64>, accept_at: f64, reject_at: f64) -> bool {
    probability.is_some_and(|p| p >= accept_at || p <= reject_at)
}

/// In-process stand-in for a provider: no network, deterministic.
/// Answers from normalized equality, so `--fake same-if-normalized-equal`
/// labels exactly what a trivial string gate would.
struct FakeDecider {
    name: String,
}

fn fold(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect()
}

#[async_trait::async_trait]
impl Decider for FakeDecider {
    fn provider(&self) -> &str {
        &self.name
    }

    fn calibrated(&self) -> bool {
        true
    }

    async fn decide(&self, request: &DecisionRequest) -> anyhow::Result<DecisionResponse> {
        let state = request.state.clone();
        let first = state.get("first").and_then(|v| v.as_str()).unwrap_or("");
        let second = state.get("second").and_then(|v| v.as_str()).unwrap_or("");
        let probability = if fold(first) == fold(second) {
            0.95
        } else {
            0.05
        };
        let mut answers = BTreeMap::new();
        for id in request.questions.keys() {
            answers.insert(id.clone(), Answer::Noul { noul: probability });
        }
        Ok(DecisionResponse {
            model: request.model.clone(),
            answers,
            input_tokens: 0,
            output_tokens: 0,
            latency_ms: 0,
            calibrated: true,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config_dir = String::from("config");
    let mut fake: Option<String> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config-dir" => {
                config_dir = args.next().unwrap_or_else(|| {
                    eprintln!("label_pair: --config-dir needs a value");
                    std::process::exit(2);
                });
            }
            "--fake" => {
                fake = Some(args.next().unwrap_or_else(|| {
                    eprintln!("label_pair: --fake needs a mode");
                    std::process::exit(2);
                }));
            }
            other => {
                eprintln!("label_pair: unknown argument {other:?}");
                std::process::exit(2);
            }
        }
    }

    let config = CrosstalkConfig::load(config_dir)?;
    let verify = &config.ai.verify;
    let chain: Vec<AiChainEntry> = verify.effective_chain();
    if chain.is_empty() {
        eprintln!("label_pair: no [ai.verify] chain and no provider/model pair");
        std::process::exit(2);
    }

    let service = match fake.as_deref() {
        Some("same-if-normalized-equal") => AiService::with_deciders(
            chain
                .iter()
                .map(|entry| {
                    (
                        entry.provider.clone(),
                        std::sync::Arc::new(FakeDecider {
                            name: entry.provider.clone(),
                        }) as std::sync::Arc<dyn Decider>,
                    )
                })
                .collect(),
        ),
        Some(other) => {
            eprintln!("label_pair: unknown --fake mode {other:?}");
            std::process::exit(2);
        }
        None => AiService::from_config(&config),
    };
    let missing: Vec<&str> = chain
        .iter()
        .map(|entry| entry.provider.as_str())
        .filter(|provider| !service.has(provider))
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "label_pair: no API key for provider(s) {}; refusing rather than labelling blind",
            missing.join(", ")
        );
        std::process::exit(2);
    }

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let job: Job = serde_json::from_str(&line)?;
        let request = ai::yes_no_request(&chain[0], &job.query, &job.candidate, "names");
        let outcome = service
            .decide_yes_no(
                &chain,
                &request,
                "same",
                verify.accept_threshold,
                verify.reject_threshold,
                || false,
            )
            .await;
        let answer = Answer_ {
            query: job.query,
            candidate: job.candidate,
            probability: outcome.probability,
            decided: settled(
                outcome.probability,
                verify.accept_threshold,
                verify.reject_threshold,
            ),
            provider: outcome.provider,
            model: outcome.model,
        };
        writeln!(out, "{}", serde_json::to_string(&answer)?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> (AiService, Vec<AiChainEntry>) {
        let chain = vec![AiChainEntry::new("fake", "fold")];
        let service = AiService::with_decider(
            "fake",
            std::sync::Arc::new(FakeDecider {
                name: "fake".to_string(),
            }),
        );
        (service, chain)
    }

    #[tokio::test]
    async fn fake_labels_pairs_without_a_network() {
        let (service, chain) = service();
        // Normalized-equal names are the same channel...
        let request = ai::yes_no_request(&chain[0], "HBO", "hbo", "names");
        let outcome = service
            .decide_yes_no(&chain, &request, "same", 0.8, 0.3, || false)
            .await;
        assert_eq!(outcome.probability, Some(0.95));
        assert_eq!(outcome.provider, "fake");
        // ...and different names are not.
        let request = ai::yes_no_request(&chain[0], "HBO", "HBO Family", "names");
        let outcome = service
            .decide_yes_no(&chain, &request, "same", 0.8, 0.3, || false)
            .await;
        assert_eq!(outcome.probability, Some(0.05));
    }

    #[test]
    fn only_a_settled_probability_relabels() {
        assert!(settled(Some(0.95), 0.8, 0.3));
        assert!(settled(Some(0.05), 0.8, 0.3));
        assert!(!settled(Some(0.5), 0.8, 0.3));
        assert!(!settled(None, 0.8, 0.3));
    }
}
