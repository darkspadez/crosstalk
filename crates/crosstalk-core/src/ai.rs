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
        }
    }

    pub fn ask(mut self, id: impl Into<String>, question: Question) -> Self {
        self.questions.insert(id.into(), question);
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
        let mut deciders = BTreeMap::new();
        deciders.insert(name.into(), decider);
        Self { deciders }
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
    fn system_prompt(request: &DecisionRequest) -> String {
        let mut lines = vec![
            "You answer questions about whether two channel names refer to the same television \
             channel. Reply with a JSON object and nothing else: {\"answers\": {<id>: <value>}, \
             \"confidences\": {<id>: <number 0-1>}}. For a yes/no question the value is the \
             probability that the answer is yes, from 0 to 1. For a choice question the value \
             is the exact option name. Treat numbered siblings (ESPN and ESPN2), time-shifted \
             feeds (+1) and regional feeds (East, West, Pacific) as DIFFERENT channels."
                .to_string(),
            String::new(),
            "Questions:".to_string(),
        ];
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
            let mut sent = self.http.post(&url).json(&body);
            if !self.api_key.is_empty() {
                sent = sent.bearer_auth(&self.api_key);
            }
            let response = sent.send().await.with_context(|| format!("POST {url}"))?;

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
}
