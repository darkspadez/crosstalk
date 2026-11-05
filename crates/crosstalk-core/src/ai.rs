//! The AI layer: a provider-agnostic way to ask a model yes/no and which-of-these questions.
//!
//! TypeSafe's System One API is not OpenAI-shaped. It takes a `state` and a set of typed
//! questions, and answers all of them in parallel with calibrated probabilities — which is
//! exactly the shape a matcher needs, since "is this the same channel?" is a yes/no decision
//! and a wrong yes is a silently wrong guide. An OpenAI-compatible endpoint can be made to
//! answer the same questions, but only by coaxing a chat model into JSON and reading the
//! numbers back, so its probabilities are weaker.
//!
//! A sub-app never talks HTTP: it builds a [`DecisionRequest`] and gets a
//! [`DecisionResponse`]. That keeps the choice of provider, the retry policy and the audit
//! trail in one place.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::config::{AiProviderConfig, CrosstalkConfig};

/// One question type. The three the matcher uses map onto TypeSafe's primitives directly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Question {
    /// "Is this statement true?" Answers with a probability in `0..=1`.
    Noul {
        instructions: String,
        /// What a yes and a no mean, when the boundary needs spelling out. Without it the
        /// model reads the instruction literally, which is where wrong answers come from.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
    /// "Which of these?" Answers with the chosen option, its probability and a confidence.
    Choice {
        instructions: String,
        /// Option name -> description. The names and descriptions both go to the model.
        criteria: BTreeMap<String, String>,
    },
    /// "How much?" Answers with a position and a confidence.
    Score {
        instructions: String,
        /// Ordered levels, lowest first.
        criteria: Vec<String>,
    },
}

impl Question {
    pub fn noul(instructions: impl Into<String>) -> Self {
        Question::Noul {
            instructions: instructions.into(),
            criteria: None,
        }
    }

    pub fn noul_with(
        instructions: impl Into<String>,
        yes: impl Into<String>,
        no: impl Into<String>,
    ) -> Self {
        Question::Noul {
            instructions: instructions.into(),
            criteria: Some(NoulCriteria {
                r#true: yes.into(),
                r#false: no.into(),
            }),
        }
    }

    pub fn choice(instructions: impl Into<String>, options: BTreeMap<String, String>) -> Self {
        Question::Choice {
            instructions: instructions.into(),
            criteria: options,
        }
    }

    pub fn score(instructions: impl Into<String>, levels: Vec<String>) -> Self {
        Question::Score {
            instructions: instructions.into(),
            criteria: levels,
        }
    }
}

/// What a yes and a no mean for a noul.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoulCriteria {
    pub r#true: String,
    pub r#false: String,
}

/// One request: a state, and the questions to ask about it.
///
/// Serializable so the audit trail can store exactly what was asked, which is the only way to
/// explain an AI decision months later.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRequest {
    /// The provider name from `[ai.providers]`.
    pub provider: String,
    /// The model name, as the provider spells it.
    pub model: String,
    /// The state: a string, or a JSON object with named fields.
    pub state: serde_json::Value,
    /// Question id -> question. The id is not sent to the model; it names the answer.
    pub questions: BTreeMap<String, Question>,
    /// What the question is about, e.g. `"epg"`, `"streams"` or `"names"`. A chat-shaped
    /// backend turns it into a task prefix for its prompt; a decision-shaped backend ignores
    /// it. `None` asks the generic "same channel?" question. Stored with the audit row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
}

impl DecisionRequest {
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        state: serde_json::Value,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            state,
            questions: BTreeMap::new(),
            task: None,
        }
    }

    pub fn ask(mut self, id: impl Into<String>, question: Question) -> Self {
        self.questions.insert(id.into(), question);
        self
    }

    /// Name what the question is about, so a chat-shaped backend can tailor its prompt
    /// without new plumbing: `"epg"`, `"streams"`, `"names"`, or a sentence.
    pub fn with_task(mut self, task: impl Into<String>) -> Self {
        let task = task.into();
        self.task = if task.trim().is_empty() {
            None
        } else {
            Some(task)
        };
        self
    }
}

/// One answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Answer {
    /// Probability that the statement is true.
    Noul { noul: f64 },
    /// The chosen option, its probability distribution, and how peaked it is.
    Choice {
        choice: String,
        confidence: f64,
        probabilities: BTreeMap<String, f64>,
    },
    /// A position on the levels, with its distribution.
    Score {
        score: f64,
        confidence: f64,
        probabilities: BTreeMap<String, f64>,
    },
}

impl Answer {
    /// The yes-probability of a noul answer.
    pub fn noul(&self) -> Option<f64> {
        match self {
            Answer::Noul { noul } => Some(*noul),
            _ => None,
        }
    }

    pub fn choice(&self) -> Option<(&str, f64)> {
        match self {
            Answer::Choice {
                choice, confidence, ..
            } => Some((choice.as_str(), *confidence)),
            _ => None,
        }
    }
}

/// The answers to one request, plus what it cost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionResponse {
    /// The model that actually answered, which may differ from the alias that was asked for.
    pub model: String,
    pub answers: BTreeMap<String, Answer>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub latency_ms: i64,
    /// Whether the backend can report calibrated probabilities. A chat model asked for a
    /// number is not the same as a decision model returning one, and the UI says so.
    pub calibrated: bool,
}

impl DecisionResponse {
    pub fn noul(&self, id: &str) -> Option<f64> {
        self.answers.get(id).and_then(Answer::noul)
    }

    pub fn choice(&self, id: &str) -> Option<(&str, f64)> {
        self.answers.get(id).and_then(Answer::choice)
    }
}

/// What a caller needs to make a yes/no decision: the probability and the confidence behind it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict {
    /// Probability that the candidate is the same channel.
    pub probability: f64,
    /// Confidence in the chosen option, or `None` when the backend cannot report one.
    pub confidence: Option<f64>,
}

impl Verdict {
    /// Whether to accept, given the app's thresholds.
    pub fn accept(&self, accept_at: f64, min_confidence: f64) -> bool {
        self.probability >= accept_at
            && self
                .confidence
                .map(|value| value >= min_confidence)
                .unwrap_or(true)
    }

    /// Whether to drop outright.
    pub fn reject(&self, reject_at: f64) -> bool {
        self.probability <= reject_at
    }
}

/// One link's turn at the question: what it answered, or why it could not.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainAttempt {
    /// The chain link's provider, as named in `[ai.providers]`.
    pub provider: String,
    /// The chain link's model.
    pub model: String,
    /// The answer, when the call worked. `None` when it failed.
    pub response: Option<DecisionResponse>,
    /// Why the call failed, when it did.
    pub error: Option<String>,
}

impl ChainAttempt {
    /// Whether this attempt settled anything: an answer arrived, failed or not.
    /// (Every recorded attempt arrived at an answer or an error; the helper exists so
    /// audit rendering reads plainly.)
    pub fn failed(&self) -> bool {
        self.response.is_none()
    }
}

/// What the chain did with one question: every link asked, in order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChainOutcome {
    /// One entry per link asked, in chain order. The last entry with a response decided.
    pub attempts: Vec<ChainAttempt>,
    /// True when cancellation stopped the chain before a link settled the question.
    pub cancelled: bool,
}

impl ChainOutcome {
    /// The attempt that decided, if any: the last one that got a response.
    pub fn deciding(&self) -> Option<&ChainAttempt> {
        self.attempts
            .iter()
            .rev()
            .find(|attempt| attempt.response.is_some())
    }

    /// The deciding response, if any.
    pub fn response(&self) -> Option<&DecisionResponse> {
        self.deciding()
            .and_then(|attempt| attempt.response.as_ref())
    }

    /// Who decided: the deciding link's provider and model.
    pub fn decided_by(&self) -> Option<(&str, &str)> {
        self.deciding()
            .map(|attempt| (attempt.provider.as_str(), attempt.model.as_str()))
    }

    /// The yes-probability of one noul question in the deciding response.
    pub fn noul(&self, id: &str) -> Option<f64> {
        self.response().and_then(|response| response.noul(id))
    }

    /// Whether the chain escalated: more than one link was asked.
    pub fn escalated(&self) -> bool {
        self.attempts.len() > 1
    }
}

/// The chain's answer to one yes/no question: the probability and who decided it.
#[derive(Debug, Clone, PartialEq)]
pub struct YesNoOutcome {
    /// The yes-probability from the deciding link, if it answered the question.
    pub probability: Option<f64>,
    /// The deciding link's provider. Empty when nothing answered.
    pub provider: String,
    /// The deciding link's model. Empty when nothing answered.
    pub model: String,
    /// Every link asked, in chain order.
    pub attempts: Vec<ChainAttempt>,
    /// True when cancellation stopped the chain before a link settled the question.
    pub cancelled: bool,
}

/// Build the "are these the same channel?" request the corpus labeller asks.
///
/// One noul question, id `"same"`, over the two names: the smallest question the chain
/// can answer, and the one [`AiService::decide_yes_no`] reads back.
///
/// `entry` addresses the request at the chain's first link; `decide_yes_no` re-addresses
/// it per link, so any link of `chain` works here. `task` names the naming domain (the
/// labeller passes `"names"`); it only tunes a chat-shaped backend's prompt.
pub fn yes_no_request(
    entry: &crate::config::AiChainEntry,
    first: &str,
    second: &str,
    task: &str,
) -> DecisionRequest {
    DecisionRequest::new(
        entry.provider.clone(),
        entry.model.clone(),
        serde_json::json!({
            "first": first,
            "second": second,
        }),
    )
    .ask(
        "same",
        Question::noul_with(
            format!("Do {first:?} and {second:?} refer to the same television channel?"),
            "The same channel, ignoring only quality and language tags",
            "A different channel: a numbered sibling, a time-shifted feed, a regional \
             variant, or a different network",
        ),
    )
    .with_task(task)
}

/// Answers questions.
#[async_trait::async_trait]
pub trait Decider: Send + Sync {
    /// The provider name this decider serves.
    fn provider(&self) -> &str;

    /// Whether answers carry calibrated probabilities.
    fn calibrated(&self) -> bool;

    async fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse>;
}

/// Everything the AI layer knows how to build.
pub struct AiService {
    deciders: BTreeMap<String, std::sync::Arc<dyn Decider>>,
}

impl std::fmt::Debug for AiService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiService")
            .field("providers", &self.deciders.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl AiService {
    /// Build one decider per configured provider.
    ///
    /// A provider with no API key is skipped with a warning rather than failing the daemon:
    /// an install that has not set up AI should still match channels.
    pub fn from_config(config: &CrosstalkConfig) -> Self {
        let mut deciders: BTreeMap<String, std::sync::Arc<dyn Decider>> = BTreeMap::new();
        for (name, provider) in &config.ai.providers {
            let built: Result<std::sync::Arc<dyn Decider>> = match provider.kind.as_str() {
                "typesafe" => build_typesafe(name, provider),
                "openai_compat" => build_openai(name, provider),
                other => {
                    tracing::warn!("ai.providers.{name}: unknown kind {other:?}; skipping it");
                    continue;
                }
            };
            match built {
                Ok(decider) => {
                    deciders.insert(name.clone(), decider);
                }
                Err(error) => {
                    tracing::warn!("ai.providers.{name}: {error:#}; skipping it");
                }
            }
        }
        Self { deciders }
    }

    /// A service with one decider, for tests.
    pub fn with_decider(name: impl Into<String>, decider: std::sync::Arc<dyn Decider>) -> Self {
        Self::with_deciders(vec![(name.into(), decider)])
    }

    /// A service with several deciders, for chain tests: one per chain link.
    pub fn with_deciders(deciders: Vec<(String, std::sync::Arc<dyn Decider>)>) -> Self {
        Self {
            deciders: deciders.into_iter().collect(),
        }
    }

    pub fn has(&self, provider: &str) -> bool {
        self.deciders.contains_key(provider)
    }

    pub fn provider_names(&self) -> Vec<&str> {
        self.deciders.keys().map(String::as_str).collect()
    }

    /// Ask one provider.
    pub async fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
        let decider = self.deciders.get(&request.provider).with_context(|| {
            format!(
                "no AI provider named {:?}; configured: {}",
                request.provider,
                if self.deciders.is_empty() {
                    "none".to_string()
                } else {
                    self.deciders.keys().cloned().collect::<Vec<_>>().join(", ")
                }
            )
        })?;
        decider.decide(request).await
    }

    /// Ask each chain link in order until one settles the question.
    ///
    /// The same [`DecisionRequest`] goes to every link, with only the provider and model
    /// rewritten to the link's own: an escalation is a second opinion on the same question,
    /// not a new question. A link that errors hands the question on; a link whose answer
    /// `settled` calls undecided does too. A confident answer is final and later links are
    /// never asked, so the first link that can decide is the only one that spends anything.
    ///
    /// `settled` answers whether a response decides the question — accept or confident
    /// reject — because "undecided" depends on the caller's thresholds, which this layer
    /// does not know. `cancelled` is checked between links, never during one: an in-flight
    /// request runs to its answer, and the chain stops after it.
    pub async fn decide_chain(
        &self,
        chain: &[crate::config::AiChainEntry],
        request: &DecisionRequest,
        mut settled: impl FnMut(&DecisionResponse) -> bool,
        cancelled: impl Fn() -> bool,
    ) -> ChainOutcome {
        let mut outcome = ChainOutcome {
            attempts: Vec::new(),
            cancelled: false,
        };
        for (position, entry) in chain.iter().enumerate() {
            if position > 0 && cancelled() {
                outcome.cancelled = true;
                break;
            }
            let mut addressed = request.clone();
            addressed.provider = entry.provider.clone();
            addressed.model = entry.model.clone();
            match self.decide(&addressed).await {
                Ok(response) => {
                    let done = settled(&response);
                    outcome.attempts.push(ChainAttempt {
                        provider: entry.provider.clone(),
                        model: entry.model.clone(),
                        response: Some(response),
                        error: None,
                    });
                    if done {
                        break;
                    }
                }
                Err(error) => {
                    outcome.attempts.push(ChainAttempt {
                        provider: entry.provider.clone(),
                        model: entry.model.clone(),
                        response: None,
                        error: Some(redact_error(&format!("{error:#}"))),
                    });
                }
            }
        }
        outcome
    }

    /// Ask the chain one yes/no question and read the answer with the gate's own rule.
    ///
    /// This is the labeller's entry point: given two names, build the request with
    /// [`yes_no_request`], ask, and read the yes-probability and the model that decided it
    /// off the outcome. `question` is the noul question id to read.
    ///
    /// One call is one pair; a labeller with thousands of pairs fans out itself, bounded by
    /// [`crate::ai_gate::fanout_bound_for`]. Each provider's client still holds its own
    /// `max_concurrency` permits and retries rate limits and 5xx, so a wide fan-out cannot
    /// overrun a slow link, and only the pairs the cheap first link cannot settle reach the
    /// later ones. Cancellation is checked between links; the caller stops issuing pairs.
    pub async fn decide_yes_no(
        &self,
        chain: &[crate::config::AiChainEntry],
        request: &DecisionRequest,
        question: &str,
        accept_at: f64,
        reject_at: f64,
        cancelled: impl Fn() -> bool,
    ) -> YesNoOutcome {
        let outcome = self
            .decide_chain(
                chain,
                request,
                |response| {
                    // A noul answer carries no confidence, and `Verdict::accept` treats a
                    // missing confidence as "no evidence against": the probability alone
                    // decides, exactly as the gate reads a yes-probability.
                    match response.noul(question) {
                        Some(probability) => {
                            let verdict = Verdict {
                                probability,
                                confidence: None,
                            };
                            verdict.accept(accept_at, 0.0) || verdict.reject(reject_at)
                        }
                        // A missing answer is not evidence either way: escalate.
                        None => false,
                    }
                },
                cancelled,
            )
            .await;
        let deciding = outcome.deciding();
        YesNoOutcome {
            probability: deciding.and_then(|attempt| {
                attempt
                    .response
                    .as_ref()
                    .and_then(|response| response.noul(question))
            }),
            provider: deciding
                .map(|attempt| attempt.provider.clone())
                .unwrap_or_default(),
            model: deciding
                .map(|attempt| attempt.model.clone())
                .unwrap_or_default(),
            attempts: outcome.attempts,
            cancelled: outcome.cancelled,
        }
    }
}

/// Scrub an error message before it reaches the audit trail: URLs lose their userinfo and
/// query string (where proxies carry tokens) and bearer tokens are masked. The bearer key
/// itself is sent as a header and never formatted into an error, but a provider's echoed
/// body or an operator's `base_url` can still carry one.
pub fn redact_error(text: &str) -> String {
    static URL: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static BEARER: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let url = URL.get_or_init(|| {
        regex::Regex::new(r#"(?i)\b(https?://)(?:[^\s/@?#"']*@)?([^\s?#"']*)(?:\?[^\s#"']*)?"#)
            .expect("valid URL pattern")
    });
    let bearer = BEARER.get_or_init(|| {
        regex::Regex::new(r#"(?i)\bbearer\s+[^\s"',}]+"#).expect("valid bearer pattern")
    });
    let text = url.replace_all(text, "$1$2");
    bearer.replace_all(&text, "Bearer [redacted]").into_owned()
}

fn build_typesafe(name: &str, provider: &AiProviderConfig) -> Result<std::sync::Arc<dyn Decider>> {
    let client = typesafe::TypeSafeClient::new(provider)
        .with_context(|| format!("building the TypeSafe client for {name}"))?;
    Ok(std::sync::Arc::new(client))
}

fn build_openai(name: &str, provider: &AiProviderConfig) -> Result<std::sync::Arc<dyn Decider>> {
    let client = openai::OpenAiCompatClient::new(provider)
        .with_context(|| format!("building the OpenAI-compatible client for {name}"))?;
    Ok(std::sync::Arc::new(client))
}

/// The TypeSafe System One backend: native, calibrated, one request per decision.
pub mod typesafe {
    use super::*;
    use std::sync::Arc;

    /// The client. Cheap to clone; the connection pool and the concurrency cap are shared.
    #[derive(Debug, Clone)]
    pub struct TypeSafeClient {
        http: reqwest::Client,
        base: String,
        api_key: String,
        permits: Arc<tokio::sync::Semaphore>,
    }

    impl TypeSafeClient {
        /// The endpoint this client posts to.
        pub fn base(&self) -> &str {
            &self.base
        }

        pub fn new(config: &AiProviderConfig) -> Result<Self> {
            let api_key = config.resolved_api_key().with_context(|| {
                "no api_key or api_key_env resolved for this provider".to_string()
            })?;
            let base = if config.base_url.trim().is_empty() {
                "https://api.typesafe.ai/v1".to_string()
            } else {
                config.base_url.trim().trim_end_matches('/').to_string()
            };
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(config.timeout_secs.max(1)))
                .user_agent(concat!("crosstalk/", env!("CARGO_PKG_VERSION")))
                .build()
                .context("building the HTTP client")?;
            Ok(Self {
                http,
                base,
                api_key,
                // The public endpoint rate-limits around eight concurrent requests.
                permits: Arc::new(tokio::sync::Semaphore::new(config.max_concurrency.max(1))),
            })
        }
    }

    #[derive(Serialize)]
    struct RequestBody<'a> {
        model: &'a str,
        state: &'a serde_json::Value,
        questions: &'a BTreeMap<String, Question>,
    }

    #[derive(Deserialize)]
    struct AnswerBody {
        model: String,
        answers: BTreeMap<String, RawAnswer>,
        #[serde(default)]
        usage: Usage,
    }

    #[derive(Deserialize, Default)]
    struct Usage {
        #[serde(default)]
        input_tokens: i64,
        #[serde(default)]
        output_tokens: i64,
    }

    #[derive(Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum RawAnswer {
        Noul {
            noul: f64,
        },
        Choice {
            choice: String,
            #[serde(default)]
            confidence: f64,
            #[serde(default)]
            probabilities: BTreeMap<String, f64>,
        },
        Score {
            #[serde(default)]
            score: f64,
            #[serde(default)]
            confidence: f64,
            #[serde(default)]
            probabilities: BTreeMap<String, f64>,
        },
    }

    #[async_trait::async_trait]
    impl Decider for TypeSafeClient {
        fn provider(&self) -> &str {
            "typesafe"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| anyhow::anyhow!("the AI concurrency semaphore was closed"))?;

            let url = format!("{}/systemone", self.base);
            let body = RequestBody {
                model: &request.model,
                state: &request.state,
                questions: &request.questions,
            };

            let started = std::time::Instant::now();
            let mut attempt = 0u32;
            let response = loop {
                attempt += 1;
                let sent = self
                    .http
                    .post(&url)
                    .bearer_auth(&self.api_key)
                    .json(&body)
                    .send()
                    .await;

                match sent {
                    Ok(response) => {
                        let status = response.status();
                        // 429 and 529 are the documented back-off statuses; 5xx is worth one
                        // more try.
                        let retryable = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || status.as_u16() == 529
                            || status.is_server_error();
                        if retryable && attempt < 3 {
                            let wait = response
                                .headers()
                                .get(reqwest::header::RETRY_AFTER)
                                .and_then(|value| value.to_str().ok())
                                .and_then(|value| value.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or(Duration::from_millis(250 * 2u64.pow(attempt - 1)));
                            tokio::time::sleep(wait).await;
                            continue;
                        }
                        if !status.is_success() {
                            let text = response.text().await.unwrap_or_default();
                            bail!(
                                "TypeSafe returned {status} for {url}: {}",
                                truncate(&text, 300)
                            );
                        }
                        break response;
                    }
                    Err(error) => {
                        if attempt < 3 && (error.is_connect() || error.is_timeout()) {
                            tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(attempt - 1)))
                                .await;
                            continue;
                        }
                        return Err(error).with_context(|| format!("POST {url}"));
                    }
                }
            };

            let parsed: AnswerBody = response
                .json()
                .await
                .with_context(|| format!("POST {url} returned an unexpected body"))?;

            let mut answers = BTreeMap::new();
            for (id, answer) in parsed.answers {
                let converted = match answer {
                    RawAnswer::Noul { noul } => Answer::Noul { noul },
                    RawAnswer::Choice {
                        choice,
                        confidence,
                        probabilities,
                    } => Answer::Choice {
                        choice,
                        confidence,
                        probabilities,
                    },
                    RawAnswer::Score {
                        score,
                        confidence,
                        probabilities,
                    } => Answer::Score {
                        score,
                        confidence,
                        probabilities,
                    },
                };
                answers.insert(id, converted);
            }

            Ok(DecisionResponse {
                model: parsed.model,
                answers,
                input_tokens: parsed.usage.input_tokens,
                output_tokens: parsed.usage.output_tokens,
                latency_ms: started.elapsed().as_millis() as i64,
                calibrated: true,
            })
        }
    }
}

/// The OpenAI-compatible backend: asks a chat model for a JSON object and reads it back.
pub mod openai {
    use super::*;
    use std::sync::Arc;

    /// The client.
    #[derive(Debug, Clone)]
    pub struct OpenAiCompatClient {
        http: reqwest::Client,
        base: String,
        api_key: String,
        permits: Arc<tokio::sync::Semaphore>,
    }

    impl OpenAiCompatClient {
        pub fn new(config: &AiProviderConfig) -> Result<Self> {
            let base = if config.base_url.trim().is_empty() {
                bail!("base_url is required for an openai_compat provider");
            } else {
                config.base_url.trim().trim_end_matches('/').to_string()
            };
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(config.timeout_secs.max(1)))
                .user_agent(concat!("crosstalk/", env!("CARGO_PKG_VERSION")))
                .build()
                .context("building the HTTP client")?;
            Ok(Self {
                http,
                base,
                api_key: config.resolved_api_key().unwrap_or_default(),
                permits: Arc::new(tokio::sync::Semaphore::new(config.max_concurrency.max(1))),
            })
        }
    }

    /// The JSON shape asked of the model. It answers every question with one number, so the
    /// whole reply is a flat object and a chat model has little room to wander.
    #[derive(Debug, Deserialize)]
    struct Reply {
        /// Question id -> answer. A noul's answer is a probability; a choice's answer is the
        /// option name.
        #[serde(default)]
        answers: BTreeMap<String, serde_json::Value>,
        #[serde(default)]
        confidences: BTreeMap<String, f64>,
    }

    /// Strip markdown code fences before parsing: chat models wrap the JSON object in
    /// ```json ... ``` even when told to reply with nothing else, and sometimes add
    /// prose around it. Without this the reply below would fail to parse and a working
    /// answer would be lost.
    pub(super) fn strip_code_fences(content: &str) -> &str {
        let trimmed = content.trim();
        // Find the first fence anywhere, so leading prose is dropped too; the language
        // tag (`json`, `JSON`, `jsonc`, ...) lives on the fence's own line.
        let unmarked = match trimmed.find("```") {
            Some(start) => {
                let after = &trimmed[start + 3..];
                match after.find('\n') {
                    Some(newline) => &after[newline + 1..],
                    // A single-line fence with no newline: strip a bare language tag.
                    None => after.trim_start_matches(|cell: char| cell.is_ascii_alphabetic()),
                }
            }
            None => trimmed,
        };
        // A lone closing fence is not valid JSON either; drop it when it is the tail.
        let unmarked = unmarked.trim();
        unmarked.strip_suffix("```").unwrap_or(unmarked).trim()
    }

    /// Build the instruction sent as the system message.
    ///
    /// The request's task, when set, becomes the first line: a chat model told "you are
    /// matching a channel to a guide listing" answers a different question than one told
    /// it is comparing two names, and the generic wording below covers neither precisely.
    /// Unknown tasks are passed through verbatim rather than rejected, so a new caller does
    /// not need a prompt change to be understood.
    pub(super) fn system_prompt(request: &DecisionRequest) -> String {
        let mut lines = Vec::new();
        if let Some(task) = request
            .task
            .as_deref()
            .map(str::trim)
            .filter(|task| !task.is_empty())
        {
            lines.push(task_prefix(task).to_string());
            lines.push(String::new());
        }
        lines.push(
            "You answer questions about whether two channel names refer to the same television \
             channel. Reply with a JSON object and nothing else: {\"answers\": {<id>: <value>}, \
             \"confidences\": {<id>: <number 0-1>}}. For a yes/no question the value is the \
             probability that the answer is yes, from 0 to 1. For a choice question the value \
             is the exact option name. Treat numbered siblings (ESPN and ESPN2), time-shifted \
             feeds (+1) and regional feeds (East, West, Pacific) as DIFFERENT channels."
                .to_string(),
        );
        lines.push(String::new());
        lines.push("Questions:".to_string());
        for (id, question) in &request.questions {
            match question {
                Question::Noul {
                    instructions,
                    criteria,
                } => {
                    lines.push(format!("- {id} (yes/no): {instructions}"));
                    if let Some(criteria) = criteria {
                        lines.push(format!("    yes means: {}", criteria.r#true));
                        lines.push(format!("    no means: {}", criteria.r#false));
                    }
                }
                Question::Choice {
                    instructions,
                    criteria,
                } => {
                    let options: Vec<String> = criteria
                        .iter()
                        .map(|(name, description)| format!("{name}: {description}"))
                        .collect();
                    lines.push(format!(
                        "- {id} (choose one of {}): {instructions}",
                        options.join("; ")
                    ));
                }
                Question::Score {
                    instructions,
                    criteria,
                } => {
                    lines.push(format!(
                        "- {id} (score 0 to {}): {instructions}",
                        criteria.len().saturating_sub(1)
                    ));
                }
            }
        }
        lines.join("\n")
    }

    /// The per-task instruction prefix for the system prompt: what the question is about,
    /// in the words a chat model needs first. Unknown tasks pass through verbatim.
    pub(super) fn task_prefix(task: &str) -> std::borrow::Cow<'_, str> {
        match task {
            "epg" => std::borrow::Cow::Borrowed(
                "You are matching a Dispatcharr channel to a guide (EPG) listing entry.",
            ),
            "streams" => std::borrow::Cow::Borrowed(
                "You are matching a Dispatcharr channel to an IPTV stream.",
            ),
            "names" => std::borrow::Cow::Borrowed(
                "You are deciding whether two channel names refer to the same television channel.",
            ),
            other => std::borrow::Cow::Owned(format!("Task: {other}.")),
        }
    }

    #[async_trait::async_trait]
    impl Decider for OpenAiCompatClient {
        fn provider(&self) -> &str {
            "openai_compat"
        }

        fn calibrated(&self) -> bool {
            // A chat model asked for a probability produces a number, not a calibrated one.
            false
        }

        async fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| anyhow::anyhow!("the AI concurrency semaphore was closed"))?;

            let url = format!("{}/chat/completions", self.base);
            let body = serde_json::json!({
                "model": request.model,
                "messages": [
                    {"role": "system", "content": system_prompt(request)},
                    {"role": "user", "content": match &request.state {
                        serde_json::Value::String(text) => text.clone(),
                        other => other.to_string(),
                    }},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0,
            });

            let started = std::time::Instant::now();
            // The same back-off as the TypeSafe client: a corpus labeller asking thousands
            // of questions should ride out a hosted endpoint's rate limit, not fail the pair.
            let mut attempt = 0u32;
            let response = loop {
                attempt += 1;
                let mut sent = self.http.post(&url).json(&body);
                if !self.api_key.is_empty() {
                    sent = sent.bearer_auth(&self.api_key);
                }
                match sent.send().await {
                    Ok(response) => {
                        let status = response.status();
                        let retryable = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || status.as_u16() == 529
                            || status.is_server_error();
                        if retryable && attempt < 3 {
                            let wait = response
                                .headers()
                                .get(reqwest::header::RETRY_AFTER)
                                .and_then(|value| value.to_str().ok())
                                .and_then(|value| value.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or(Duration::from_millis(250 * 2u64.pow(attempt - 1)))
                                .min(Duration::from_secs(30));
                            tokio::time::sleep(wait).await;
                            continue;
                        }
                        break response;
                    }
                    Err(error) => {
                        if attempt < 3 && (error.is_connect() || error.is_timeout()) {
                            tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(attempt - 1)))
                                .await;
                            continue;
                        }
                        return Err(error).with_context(|| format!("POST {url}"));
                    }
                }
            };

            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if !status.is_success() {
                bail!(
                    "the OpenAI-compatible endpoint returned {status} for {url}: {}",
                    truncate(&text, 300)
                );
            }

            #[derive(Deserialize)]
            struct ChatResponse {
                #[serde(default)]
                model: String,
                #[serde(default)]
                choices: Vec<Choice>,
                #[serde(default)]
                usage: Usage,
            }
            #[derive(Deserialize)]
            struct Choice {
                #[serde(default)]
                message: Message,
            }
            #[derive(Deserialize, Default)]
            struct Message {
                #[serde(default)]
                content: String,
            }
            #[derive(Deserialize, Default)]
            struct Usage {
                #[serde(default)]
                prompt_tokens: i64,
                #[serde(default)]
                completion_tokens: i64,
            }

            let parsed: ChatResponse = serde_json::from_str(&text)
                .with_context(|| format!("POST {url} returned an unexpected body"))?;
            let content = parsed
                .choices
                .first()
                .map(|choice| choice.message.content.clone())
                .unwrap_or_default();
            let reply: Reply =
                serde_json::from_str(strip_code_fences(&content)).with_context(|| {
                    format!(
                        "the model did not answer with the expected JSON object: {}",
                        truncate(&content, 300)
                    )
                })?;

            let mut answers = BTreeMap::new();
            for (id, question) in &request.questions {
                let Some(value) = reply.answers.get(id) else {
                    // A missing answer is not fatal: the caller treats the question as
                    // undecided, which is safer than inventing one.
                    continue;
                };
                let confidence = reply.confidences.get(id).copied();
                let answer = match question {
                    Question::Noul { .. } => match value.as_f64() {
                        Some(probability) => Answer::Noul {
                            noul: probability.clamp(0.0, 1.0),
                        },
                        None => continue,
                    },
                    Question::Choice { criteria, .. } => {
                        let Some(chosen) = value.as_str() else {
                            continue;
                        };
                        if !criteria.contains_key(chosen) {
                            continue;
                        }
                        // A chat backend gives no distribution, so it is reported as a point
                        // mass on the chosen option — and the response stays marked
                        // `calibrated: false`, because a chat model asked for a number is not
                        // a decision model returning one. A missing confidence is 0.0, not
                        // 1.0 and not a "neutral" 0.5: an absent number must fail the
                        // `min_confidence` floor rather than auto-pass it (0.5 would clear a
                        // default 0.5 floor).
                        let mut probabilities = BTreeMap::new();
                        for name in criteria.keys() {
                            probabilities
                                .insert(name.clone(), if name == chosen { 1.0 } else { 0.0 });
                        }
                        Answer::Choice {
                            choice: chosen.to_string(),
                            confidence: confidence.unwrap_or(0.0),
                            probabilities,
                        }
                    }
                    Question::Score { criteria, .. } => match value.as_f64() {
                        Some(score) => Answer::Score {
                            score: score.clamp(0.0, criteria.len().saturating_sub(1) as f64),
                            // Same failing default as a choice: an absent confidence is not
                            // evidence of certainty.
                            confidence: confidence.unwrap_or(0.0),
                            probabilities: BTreeMap::new(),
                        },
                        None => continue,
                    },
                };
                answers.insert(id.clone(), answer);
            }

            Ok(DecisionResponse {
                model: if parsed.model.is_empty() {
                    request.model.clone()
                } else {
                    parsed.model
                },
                answers,
                input_tokens: parsed.usage.prompt_tokens,
                output_tokens: parsed.usage.completion_tokens,
                latency_ms: started.elapsed().as_millis() as i64,
                calibrated: false,
            })
        }
    }
}

fn truncate(text: &str, limit: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= limit {
        return trimmed.to_string();
    }
    let cut: String = trimmed.chars().take(limit).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn provider(base_url: &str) -> AiProviderConfig {
        AiProviderConfig {
            kind: "typesafe".to_string(),
            base_url: base_url.to_string(),
            api_key: "test-key".to_string(),
            api_key_env: String::new(),
            timeout_secs: 5,
            max_concurrency: 2,
        }
    }

    fn openai_provider(base_url: &str) -> AiProviderConfig {
        AiProviderConfig {
            kind: "openai_compat".to_string(),
            base_url: base_url.to_string(),
            api_key: "test-key".to_string(),
            api_key_env: String::new(),
            timeout_secs: 5,
            max_concurrency: 2,
        }
    }

    fn request(provider: &str) -> DecisionRequest {
        let mut options = BTreeMap::new();
        options.insert("espn".to_string(), "ESPN".to_string());
        options.insert("none".to_string(), "none of them".to_string());

        DecisionRequest::new(
            provider,
            "jev-latest",
            serde_json::json!({"channel": "ESPN HD", "candidate": "ESPN"}),
        )
        .ask(
            "same",
            Question::noul_with("Is this the same channel?", "same", "different"),
        )
        .ask("best", Question::choice("Which candidate?", options))
    }

    #[tokio::test]
    async fn typesafe_answers_questions_with_probabilities() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .and(header("Authorization", "Bearer test-key"))
            .and(body_partial_json(serde_json::json!({
                "model": "jev-latest",
                "questions": {
                    "same": {"type": "noul", "instructions": "Is this the same channel?"},
                    "best": {"type": "choice", "instructions": "Which candidate?"}
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "jev-1.14.0",
                "answers": {
                    "same": {"type": "noul", "noul": 0.93},
                    "best": {
                        "type": "choice",
                        "choice": "espn",
                        "confidence": 0.88,
                        "probabilities": {"espn": 0.88, "none": 0.12}
                    }
                },
                "usage": {"input_tokens": 420, "output_tokens": 12}
            })))
            .mount(&server)
            .await;

        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "typesafe".to_string(),
                    provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("typesafe")).await.expect("answers");

        assert_eq!(response.model, "jev-1.14.0", "the versioned id is reported");
        assert_eq!(response.noul("same"), Some(0.93));
        assert_eq!(response.choice("best"), Some(("espn", 0.88)));
        assert_eq!(response.input_tokens, 420);
        assert!(response.calibrated);
    }

    #[tokio::test]
    async fn typesafe_retries_a_rate_limit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "jev-latest",
                "answers": {"same": {"type": "noul", "noul": 0.7}},
                "usage": {"input_tokens": 1, "output_tokens": 1}
            })))
            .mount(&server)
            .await;

        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "typesafe".to_string(),
                    provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("typesafe")).await.expect("retried");
        assert_eq!(response.noul("same"), Some(0.7));
    }

    #[tokio::test]
    async fn typesafe_surfaces_a_permanent_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("{\"detail\":\"bad key\"}"))
            .mount(&server)
            .await;
        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "typesafe".to_string(),
                    provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let error = service.decide(&request("typesafe")).await.expect_err("401");
        let message = format!("{error:#}");
        assert!(message.contains("401"), "{message}");
        assert!(message.contains("bad key"), "{message}");
    }

    #[tokio::test]
    async fn openai_compat_reads_a_json_reply() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("Authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-local",
                "choices": [{"message": {"content": "{\"answers\": {\"same\": 0.81, \"best\": \"espn\"}, \"confidences\": {\"best\": 0.66}}"}}],
                "usage": {"prompt_tokens": 300, "completion_tokens": 20}
            })))
            .mount(&server)
            .await;

        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "local".to_string(),
                    openai_provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("local")).await.expect("answers");

        assert_eq!(response.model, "gpt-local");
        assert_eq!(response.noul("same"), Some(0.81));
        assert_eq!(response.choice("best"), Some(("espn", 0.66)));
        // And it says the numbers are not calibrated.
        assert!(!response.calibrated);
    }

    #[tokio::test]
    async fn openai_compat_retries_a_rate_limit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-local",
                "choices": [{"message": {"content": "{\"answers\": {\"same\": 0.7}}"}}],
            })))
            .mount(&server)
            .await;

        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "local".to_string(),
                    openai_provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("local")).await.expect("retried");
        assert_eq!(response.noul("same"), Some(0.7));
    }

    #[tokio::test]
    async fn openai_compat_defaults_a_missing_confidence_to_zero() {
        // No "confidences" map at all: the choice must not report certainty it was
        // never given. 0.0 fails the `min_confidence` floor instead of auto-passing
        // it — the old 0.5 default cleared a default 0.5 floor — while the point-mass
        // probabilities stay marked `calibrated: false`.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "m",
                "choices": [{"message": {"content": "{\"answers\": {\"same\": 0.9, \"best\": \"espn\"}}"}}],
                "usage": {}
            })))
            .mount(&server)
            .await;
        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "local".to_string(),
                    openai_provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("local")).await.expect("answers");
        assert_eq!(response.noul("same"), Some(0.9));
        assert_eq!(
            response.choice("best"),
            Some(("espn", 0.0)),
            "an unreported confidence fails the floor, it is not certain"
        );
        assert!(!response.calibrated);
    }

    #[tokio::test]
    async fn openai_compat_parses_a_fenced_json_reply() {
        // Chat models wrap the object in markdown fences even when told not to.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "m",
                "choices": [{"message": {"content": "```json\n{\"answers\": {\"same\": 0.81, \"best\": \"espn\"}, \"confidences\": {\"best\": 0.66}}\n```"}}],
                "usage": {}
            })))
            .mount(&server)
            .await;
        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "local".to_string(),
                    openai_provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("local")).await.expect("answers");
        assert_eq!(response.noul("same"), Some(0.81));
        assert_eq!(response.choice("best"), Some(("espn", 0.66)));
    }

    #[test]
    fn code_fences_are_stripped_before_parsing() {
        assert_eq!(
            super::openai::strip_code_fences("```json\n{\"a\": 1}\n```"),
            "{\"a\": 1}"
        );
        assert_eq!(
            super::openai::strip_code_fences("```\n{\"a\": 1}\n```"),
            "{\"a\": 1}"
        );
        assert_eq!(
            super::openai::strip_code_fences("```JSON\n{\"a\": 1}\n```"),
            "{\"a\": 1}",
            "the language tag case does not matter"
        );
        assert_eq!(
            super::openai::strip_code_fences("```jsonc\n{\"a\": 1}\n```"),
            "{\"a\": 1}",
            "jsonc is fenced the same way"
        );
        assert_eq!(
            super::openai::strip_code_fences(
                "Here is the object you asked for:\n```json\n{\"a\": 1}\n```"
            ),
            "{\"a\": 1}",
            "prose before the fence is dropped"
        );
        assert_eq!(
            super::openai::strip_code_fences("  {\"a\": 1}  "),
            "{\"a\": 1}",
            "a bare object passes through untouched"
        );
    }

    #[tokio::test]
    async fn openai_compat_rejects_an_option_that_was_not_offered() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "m",
                "choices": [{"message": {"content": "{\"answers\": {\"best\": \"something else\"}}"}}],
                "usage": {}
            })))
            .mount(&server)
            .await;
        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "local".to_string(),
                    openai_provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let response = service.decide(&request("local")).await.expect("answers");
        assert!(
            response.choice("best").is_none(),
            "an invented option is dropped"
        );
    }

    #[tokio::test]
    async fn openai_compat_rejects_a_non_json_reply() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "m",
                "choices": [{"message": {"content": "Sure! I think they are the same."}}],
                "usage": {}
            })))
            .mount(&server)
            .await;
        let service = AiService::from_config(&CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([(
                    "local".to_string(),
                    openai_provider(&format!("{}/v1", server.uri())),
                )]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        });
        let error = service
            .decide(&request("local"))
            .await
            .expect_err("not json");
        assert!(format!("{error:#}").contains("expected JSON"));
    }

    #[test]
    fn an_unknown_provider_lists_the_known_ones() {
        let service = AiService::with_decider("only", std::sync::Arc::new(StubDecider));
        let error = futures_lite_block(service.decide(&request("other"))).expect_err("unknown");
        let message = format!("{error:#}");
        assert!(message.contains("no AI provider named"), "{message}");
        assert!(message.contains("only"), "{message}");
    }

    /// Run a future to completion without a runtime, for the one test that needs it.
    fn futures_lite_block<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    struct StubDecider;

    #[async_trait::async_trait]
    impl Decider for StubDecider {
        fn provider(&self) -> &str {
            "only"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
            Ok(DecisionResponse {
                model: request.model.clone(),
                answers: BTreeMap::new(),
                input_tokens: 0,
                output_tokens: 0,
                latency_ms: 0,
                calibrated: true,
            })
        }
    }

    #[test]
    fn verdict_applies_the_thresholds() {
        let confident = Verdict {
            probability: 0.9,
            confidence: Some(0.8),
        };
        assert!(confident.accept(0.8, 0.5));
        assert!(!confident.accept(0.95, 0.5));
        assert!(!confident.reject(0.3));

        let unsure = Verdict {
            probability: 0.9,
            confidence: Some(0.2),
        };
        assert!(!unsure.accept(0.8, 0.5), "low confidence is not accepted");

        let no_confidence = Verdict {
            probability: 0.9,
            confidence: None,
        };
        assert!(
            no_confidence.accept(0.8, 0.5),
            "a backend without confidence still counts"
        );

        let borderline = Verdict {
            probability: 0.5,
            confidence: Some(0.9),
        };
        assert!(!borderline.accept(0.8, 0.5));
        assert!(!borderline.reject(0.3));
    }

    #[test]
    fn a_provider_without_a_key_is_skipped_not_fatal() {
        let config = CrosstalkConfig {
            ai: crate::config::AiConfig {
                providers: BTreeMap::from([
                    (
                        "nokey".to_string(),
                        AiProviderConfig {
                            kind: "typesafe".to_string(),
                            base_url: "https://api.typesafe.ai/v1".to_string(),
                            api_key: String::new(),
                            api_key_env: String::new(),
                            ..AiProviderConfig::default()
                        },
                    ),
                    (
                        "weird".to_string(),
                        AiProviderConfig {
                            kind: "telepathy".to_string(),
                            ..AiProviderConfig::default()
                        },
                    ),
                ]),
                ..crate::config::AiConfig::default()
            },
            ..CrosstalkConfig::default()
        };
        let service = AiService::from_config(&config);
        assert!(service.provider_names().is_empty());
    }

    #[test]
    fn an_openai_provider_needs_a_base_url() {
        let error = openai::OpenAiCompatClient::new(&AiProviderConfig {
            kind: "openai_compat".to_string(),
            base_url: String::new(),
            ..AiProviderConfig::default()
        })
        .expect_err("needs a url");
        assert!(format!("{error}").contains("base_url"));
    }

    #[test]
    fn the_typesafe_base_url_defaults_to_the_public_endpoint() {
        let client = typesafe::TypeSafeClient::new(&AiProviderConfig {
            kind: "typesafe".to_string(),
            base_url: String::new(),
            api_key: "k".to_string(),
            api_key_env: String::new(),
            ..AiProviderConfig::default()
        })
        .expect("client");
        assert_eq!(client.base(), "https://api.typesafe.ai/v1");
    }

    /// A decider scripted per link: answers the `same` question with a fixed probability —
    /// or omits it, or fails — and counts its calls so escalation is observable.
    struct ScriptDecider {
        probability: Option<f64>,
        fail: bool,
        model: String,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Decider for ScriptDecider {
        fn provider(&self) -> &str {
            "script"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(&self, _request: &DecisionRequest) -> Result<DecisionResponse> {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("the endpoint is down");
            }
            let mut answers = BTreeMap::new();
            if let Some(probability) = self.probability {
                answers.insert("same".to_string(), Answer::Noul { noul: probability });
            }
            Ok(DecisionResponse {
                model: self.model.clone(),
                answers,
                input_tokens: 1,
                output_tokens: 1,
                latency_ms: 1,
                calibrated: true,
            })
        }
    }

    fn script(
        probability: Option<f64>,
        fail: bool,
        model: &str,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> std::sync::Arc<dyn Decider> {
        std::sync::Arc::new(ScriptDecider {
            probability,
            fail,
            model: model.to_string(),
            calls,
        })
    }

    fn chain() -> Vec<crate::config::AiChainEntry> {
        vec![
            crate::config::AiChainEntry::new("first", "jev-latest"),
            crate::config::AiChainEntry::new("second", "kimi-k3"),
        ]
    }

    fn same_request() -> DecisionRequest {
        DecisionRequest::new("first", "jev-latest", serde_json::json!({"a": "ESPN"}))
            .ask("same", Question::noul("Same channel?"))
    }

    /// The gate's rule for one yes/no answer: accept or confident reject settles.
    fn settled_yes_no(accept_at: f64, reject_at: f64) -> impl FnMut(&DecisionResponse) -> bool {
        move |response: &DecisionResponse| match response.noul("same") {
            Some(probability) => probability >= accept_at || probability <= reject_at,
            None => false,
        }
    }

    #[tokio::test]
    async fn a_confident_first_answer_stops_the_chain() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let first_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(
                    Some(0.95),
                    false,
                    "jev-1",
                    std::sync::Arc::clone(&first_calls),
                ),
            ),
            (
                "second".to_string(),
                script(
                    Some(0.95),
                    false,
                    "kimi",
                    std::sync::Arc::clone(&second_calls),
                ),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(outcome.attempts.len(), 1);
        assert!(!outcome.cancelled);
        assert!(!outcome.escalated());
        assert_eq!(outcome.decided_by(), Some(("first", "jev-latest")));
        assert_eq!(outcome.noul("same"), Some(0.95));
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0, "never asked");
    }

    #[tokio::test]
    async fn an_undecided_first_answer_escalates() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let first_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(
                    Some(0.5),
                    false,
                    "jev-1",
                    std::sync::Arc::clone(&first_calls),
                ),
            ),
            (
                "second".to_string(),
                script(
                    Some(0.95),
                    false,
                    "kimi",
                    std::sync::Arc::clone(&second_calls),
                ),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(outcome.attempts.len(), 2);
        assert!(outcome.escalated());
        assert_eq!(outcome.decided_by(), Some(("second", "kimi-k3")));
        assert_eq!(outcome.noul("same"), Some(0.95));
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_confident_reject_is_final() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(Some(0.1), false, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(
                    Some(0.95),
                    false,
                    "kimi",
                    std::sync::Arc::clone(&second_calls),
                ),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(outcome.attempts.len(), 1);
        assert_eq!(outcome.decided_by(), Some(("first", "jev-latest")));
        assert_eq!(second_calls.load(Ordering::SeqCst), 0, "a reject is final");
    }

    #[tokio::test]
    async fn a_missing_answer_escalates() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(None, false, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(
                    Some(0.2),
                    false,
                    "kimi",
                    std::sync::Arc::clone(&second_calls),
                ),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(
            outcome.attempts.len(),
            2,
            "a missing answer is not evidence"
        );
        assert_eq!(outcome.decided_by(), Some(("second", "kimi-k3")));
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_call_escalates() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(Some(0.95), true, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(
                    Some(0.9),
                    false,
                    "kimi",
                    std::sync::Arc::clone(&second_calls),
                ),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(outcome.attempts.len(), 2);
        assert!(outcome.attempts[0].failed());
        assert!(outcome.attempts[0]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("down"));
        assert_eq!(outcome.decided_by(), Some(("second", "kimi-k3")));
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn all_undecided_records_every_attempt() {
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(Some(0.5), false, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(Some(0.6), false, "kimi", Default::default()),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(outcome.attempts.len(), 2);
        assert!(!outcome.cancelled);
        // The last answer stands, even undecided: the caller reads the probability and
        // applies its own thresholds.
        assert_eq!(outcome.decided_by(), Some(("second", "kimi-k3")));
        assert_eq!(outcome.noul("same"), Some(0.6));
    }

    #[tokio::test]
    async fn all_failed_leaves_nothing_decided() {
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(None, true, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(None, true, "kimi", Default::default()),
            ),
        ]);
        let outcome = service
            .decide_chain(&chain(), &same_request(), settled_yes_no(0.8, 0.3), || {
                false
            })
            .await;
        assert_eq!(outcome.attempts.len(), 2);
        assert!(outcome.deciding().is_none());
        assert!(outcome.decided_by().is_none());
        assert!(outcome.attempts.iter().all(|attempt| attempt.failed()));
    }

    #[tokio::test]
    async fn cancellation_stops_the_chain_between_links() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let cancel = std::sync::Arc::new(AtomicBool::new(false));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(Some(0.5), false, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(
                    Some(0.95),
                    false,
                    "kimi",
                    std::sync::Arc::clone(&second_calls),
                ),
            ),
        ]);
        // The first link leaves the question undecided and the operator cancels while its
        // answer lands: the second link is never asked.
        let stop = std::sync::Arc::clone(&cancel);
        let outcome = service
            .decide_chain(
                &chain(),
                &same_request(),
                |response: &DecisionResponse| {
                    stop.store(true, Ordering::SeqCst);
                    settled_yes_no(0.8, 0.3)(response)
                },
                || cancel.load(Ordering::SeqCst),
            )
            .await;
        assert!(outcome.cancelled);
        assert_eq!(outcome.attempts.len(), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_empty_chain_asks_nothing() {
        let service = AiService::with_deciders(vec![]);
        let outcome = service
            .decide_chain(&[], &same_request(), settled_yes_no(0.8, 0.3), || false)
            .await;
        assert!(outcome.attempts.is_empty());
        assert!(!outcome.cancelled);
        assert!(outcome.deciding().is_none());
    }

    #[tokio::test]
    async fn the_labeller_gets_a_probability_and_the_deciding_model() {
        // `yes_no_request` + `decide_yes_no`: given two names, ask the chain "same
        // channel?" and read back the yes-probability and who decided it.
        let service = AiService::with_deciders(vec![
            (
                "first".to_string(),
                script(Some(0.5), false, "jev-1", Default::default()),
            ),
            (
                "second".to_string(),
                script(Some(0.91), false, "kimi", Default::default()),
            ),
        ]);
        let request = super::yes_no_request(&chain()[0], "ESPN HD", "ESPN", "names");
        assert_eq!(request.task.as_deref(), Some("names"));
        assert!(request.questions.contains_key("same"));
        let answered = service
            .decide_yes_no(&chain(), &request, "same", 0.8, 0.3, || false)
            .await;
        assert_eq!(answered.probability, Some(0.91));
        assert_eq!(answered.provider, "second");
        assert_eq!(answered.model, "kimi-k3");
        assert_eq!(answered.attempts.len(), 2);
        assert!(!answered.cancelled);
    }

    #[test]
    fn the_task_becomes_the_chat_prompts_first_line() {
        let base = same_request();
        let plain = super::openai::system_prompt(&base);
        assert!(
            !plain
                .lines()
                .next()
                .unwrap_or_default()
                .starts_with("You are matching"),
            "no task, no prefix"
        );
        for (task, expected) in [
            (
                "epg",
                "You are matching a Dispatcharr channel to a guide (EPG) listing entry.",
            ),
            (
                "streams",
                "You are matching a Dispatcharr channel to an IPTV stream.",
            ),
            (
                "names",
                "You are deciding whether two channel names refer to the same television channel.",
            ),
        ] {
            let tasked = same_request().with_task(task);
            let prompt = super::openai::system_prompt(&tasked);
            assert_eq!(prompt.lines().next().unwrap_or_default(), expected);
            assert!(
                prompt.contains("You answer questions about"),
                "{task}: the generic wording stays"
            );
        }
        let custom = same_request().with_task("logos");
        assert_eq!(
            super::openai::system_prompt(&custom)
                .lines()
                .next()
                .unwrap_or_default(),
            "Task: logos."
        );
        assert_eq!(same_request().with_task("  ").task, None, "blank is unset");
    }

    #[test]
    fn audit_errors_lose_url_tokens_and_bearer_keys() {
        let raw = "POST https://user:pa55@proxy.example/v1/chat/completions?api_key=sk-123&x=1: \
                   401 {\"error\": \"bad header Bearer sk-live-abc\"}";
        let clean = super::redact_error(raw);
        assert!(!clean.contains("sk-123"), "{clean}");
        assert!(!clean.contains("pa55"), "{clean}");
        assert!(!clean.contains("sk-live-abc"), "{clean}");
        assert!(
            clean.contains("https://proxy.example/v1/chat/completions"),
            "{clean}"
        );
        assert_eq!(
            super::redact_error("the endpoint is down"),
            "the endpoint is down",
            "plain errors pass through untouched"
        );
    }
}
