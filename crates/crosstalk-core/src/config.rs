//! Configuration: one host file plus one file per sub-app.
//!
//! The split follows how the options are used. Anything the HOST owns — the Dispatcharr
//! connection, the AI providers, the HTTP listener, the thread pool, the export directory —
//! lives in `crosstalk.toml`. Anything every sub-app must agree on also lives in
//! `crosstalk.toml`:
//!
//!   * `[matching]`: the normalisation toggles, the enabled country databases, the custom
//!     aliases. Two sub-apps disagreeing about whether `[HD]` is stripped would produce two
//!     different answers for the same channel;
//!   * `[scope]`: which profiles, groups and ignored groups a run may touch;
//!   * `[exports]`: how long CSV reports are kept;
//!   * `[ai.verify]`: when a borderline match is worth asking a model about. The endpoints
//!     and keys stay under `[ai.providers]`; the policy is what every app shares.
//!
//! A sub-app's own thresholds and filters live in its own file.
//!
//! Secrets can be written inline or named through `*_env`, which is what a container
//! deployment wants. Loading never logs a secret value.
//!
//! Write-back goes through `toml_edit` so comments and ordering in the operator's file
//! survive an edit made in the web UI; a round trip through `toml` would silently drop
//! every comment in the file.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Accept a TOML string, or a scalar figment inferred a type for.
///
/// An environment override is untyped text, and figment guesses: `CROSSTALK_TOKEN=123456`
/// arrives as a number, which would fail to deserialize into a `String` and take the whole
/// configuration down. Reading it back through this keeps a digits-only or `true`-shaped
/// secret working.
fn string_or_scalar<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(match value {
        serde_json::Value::String(text) => text,
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        other => other.to_string(),
    })
}

/// Where the daemon listens, and how it authenticates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Interface to bind. Localhost by default, so an unconfigured daemon is not exposed.
    pub bind: String,
    pub port: u16,
    /// Bearer token required for every request when set. Empty means "no token", which is
    /// only safe while `bind` is a loopback address.
    #[serde(deserialize_with = "string_or_scalar")]
    pub token: String,
    /// Read the token from this environment variable instead of the file.
    #[serde(deserialize_with = "string_or_scalar")]
    pub token_env: String,
    /// Reverse proxies allowed to name the client: CIDR ranges (`10.0.0.0/8`, `::1/128`) or bare
    /// addresses. Only a request whose socket peer is in one of these has its
    /// `X-Forwarded-For` / `X-Real-IP` / `X-Forwarded-Proto` / `X-Forwarded-Host` honoured;
    /// everyone else's forwarding headers are ignored. Non-empty requires a token.
    pub trusted_proxies: Vec<String>,
    /// The URL browsers use to reach the UI (`https://crosstalk.example.com`), when a proxy
    /// rewrites `Host`. A POST's `Origin` must match it. Empty means "work it out from the
    /// trusted proxy's `X-Forwarded-*` headers, else `Host`".
    #[serde(deserialize_with = "string_or_scalar")]
    pub public_url: String,
    /// Always mark the session cookie `Secure`. It is also marked whenever a trusted proxy
    /// says the request arrived over https.
    pub secure_cookies: bool,
    /// Always send `Strict-Transport-Security`. It is also sent whenever a trusted proxy says
    /// the request arrived over https.
    pub hsts: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 8730,
            token: String::new(),
            token_env: "CROSSTALK_TOKEN".to_string(),
            trusted_proxies: Vec::new(),
            public_url: String::new(),
            secure_cookies: false,
            hsts: false,
        }
    }
}

/// The shortest token a reachable daemon accepts: 32 bytes, the length of
/// `openssl rand -hex 16` and well past anything guessable at the limiter's rate.
pub const MIN_TOKEN_BYTES: usize = 32;

impl ServerConfig {
    /// The token to require, resolved from the environment first.
    pub fn resolved_token(&self) -> Option<String> {
        resolve_secret(&self.token, &self.token_env)
    }

    /// Whether the bind address is a loopback address.
    pub fn is_loopback(&self) -> bool {
        crate::net::is_loopback_bind(&self.bind)
    }

    /// The trusted proxy ranges. Entries that do not parse are dropped here; [`Self::security_errors`]
    /// reports them, and the daemon refuses to start (or to reload) while any remain.
    pub fn trusted_cidrs(&self) -> Vec<crate::net::Cidr> {
        self.trusted_proxies
            .iter()
            .filter_map(|entry| crate::net::Cidr::parse(entry).ok())
            .collect()
    }

    /// `public_url` as a normalised origin (`https://host:443`), when it is set and valid.
    pub fn public_origin(&self) -> Option<String> {
        let url = self.public_url.trim();
        if url.is_empty() {
            None
        } else {
            crate::net::url_origin(url)
        }
    }

    /// What makes this listener unsafe to run, as errors: the daemon refuses to start with any,
    /// and a settings save refuses to swap such a configuration in.
    ///
    /// * every `trusted_proxies` entry must parse as an address or CIDR range;
    /// * trusted proxies mean the daemon is reachable from elsewhere, so a token is required;
    /// * a token on a reachable daemon (non-loopback bind, or behind a proxy) is at least
    ///   [`MIN_TOKEN_BYTES`] long, because it is the only thing between the internet and an
    ///   apply;
    /// * `public_url`, when set, is an `http(s)://host[:port]` URL.
    pub fn security_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if let Err(bad) = crate::net::parse_cidrs(&self.trusted_proxies) {
            for error in bad {
                errors.push(format!(
                    "server.trusted_proxies: {error} (use an address like 10.0.0.2 or a range \
                     like 172.16.0.0/12)"
                ));
            }
        }
        let token = self.resolved_token();
        let proxied = !self.trusted_proxies.is_empty();
        if proxied && token.is_none() {
            errors.push(
                "server.trusted_proxies is set but no token is configured: a daemon behind a \
                 reverse proxy is reachable from outside this host, so set server.token or \
                 server.token_env"
                    .to_string(),
            );
        }
        if let Some(token) = &token {
            if token.len() < MIN_TOKEN_BYTES && (proxied || !self.is_loopback()) {
                errors.push(format!(
                    "server.token is {} bytes; a daemon reachable from outside this host (a \
                     non-loopback bind or trusted_proxies) needs at least {MIN_TOKEN_BYTES}. \
                     Generate one with `openssl rand -hex 32`.",
                    token.len()
                ));
            }
        }
        if !self.public_url.trim().is_empty() && self.public_origin().is_none() {
            errors.push(format!(
                "server.public_url {:?} must be an http:// or https:// URL with a host",
                self.public_url.trim()
            ));
        }
        errors
    }
}

/// How to reach Dispatcharr.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DispatcharrConfig {
    /// Base URL, without the `/api` suffix: `http://dispatcharr:9191`.
    #[serde(deserialize_with = "string_or_scalar")]
    pub base_url: String,
    /// API key, sent as `X-API-Key`. Preferred: it does not expire.
    #[serde(deserialize_with = "string_or_scalar")]
    pub api_key: String,
    #[serde(deserialize_with = "string_or_scalar")]
    pub api_key_env: String,
    /// JWT fallback when no API key is configured.
    #[serde(deserialize_with = "string_or_scalar")]
    pub username: String,
    #[serde(deserialize_with = "string_or_scalar")]
    pub password: String,
    #[serde(deserialize_with = "string_or_scalar")]
    pub password_env: String,
    /// Per-request timeout.
    pub timeout_secs: u64,
    /// Cap on concurrent HTTP requests, for the programme-data checks, the AI calls and
    /// lineup's channel writes (lineup has no write-concurrency knob of its own).
    pub concurrency: usize,
}

impl Default for DispatcharrConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:9191".to_string(),
            api_key: String::new(),
            api_key_env: "CROSSTALK_DISPATCHARR_API_KEY".to_string(),
            username: String::new(),
            password: String::new(),
            password_env: "CROSSTALK_DISPATCHARR_PASSWORD".to_string(),
            timeout_secs: 30,
            concurrency: 8,
        }
    }
}

impl DispatcharrConfig {
    pub fn resolved_api_key(&self) -> Option<String> {
        resolve_secret(&self.api_key, &self.api_key_env)
    }

    pub fn resolved_password(&self) -> Option<String> {
        resolve_secret(&self.password, &self.password_env)
    }

    /// The API root every request is built from.
    pub fn api_root(&self) -> String {
        let base = self.base_url.trim().trim_end_matches('/');
        if base.to_lowercase().ends_with("/api") {
            base.to_string()
        } else {
            format!("{base}/api")
        }
    }

    /// Whether enough is configured to authenticate at all.
    pub fn has_credentials(&self) -> bool {
        self.resolved_api_key().is_some()
            || (!self.username.trim().is_empty() && self.resolved_password().is_some())
    }
}

/// One AI provider endpoint. No model names here: a sub-app names the model, the host owns
/// the connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AiProviderConfig {
    /// `typesafe` (native System One) or `openai_compat`.
    pub kind: String,
    #[serde(deserialize_with = "string_or_scalar")]
    pub base_url: String,
    #[serde(deserialize_with = "string_or_scalar")]
    pub api_key: String,
    #[serde(deserialize_with = "string_or_scalar")]
    pub api_key_env: String,
    pub timeout_secs: u64,
    pub max_concurrency: usize,
}

impl Default for AiProviderConfig {
    fn default() -> Self {
        // Zero here would mean a zero timeout and a zero-permit semaphore: a request that
        // never starts and never times out. `#[serde(default)]` takes missing fields from
        // this impl, so a provider written without these keys still gets usable values.
        Self {
            kind: "typesafe".to_string(),
            base_url: String::new(),
            api_key: String::new(),
            api_key_env: String::new(),
            timeout_secs: 30,
            max_concurrency: 8,
        }
    }
}

impl AiProviderConfig {
    pub fn resolved_api_key(&self) -> Option<String> {
        resolve_secret(&self.api_key, &self.api_key_env)
    }
}

/// Every configured AI provider, keyed by the name a sub-app refers to, plus the policy each
/// sub-app shares.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AiConfig {
    pub providers: BTreeMap<String, AiProviderConfig>,
    /// When a borderline match is worth a request, and how to read the answer.
    ///
    /// Under `[ai.verify]` rather than flattened into `[ai]` so the connection details
    /// (`[ai.providers.<name>]`) stay apart from the matching policy, and so a provider can
    /// never be mistaken for a policy key.
    pub verify: AiVerifyConfig,
}

impl AiConfig {
    pub fn provider(&self, name: &str) -> Option<&AiProviderConfig> {
        self.providers.get(name)
    }
}

/// One link of the `[ai.verify]` escalation chain: which provider answers, and which model.
///
/// ```toml
/// [ai.verify]
/// chain = [{provider = "typesafe", model = "jev-latest"}, {provider = "llm", model = "kimi-k3"}]
/// ```
///
/// The `provider` names are the keys of `[ai.providers.<name>]`, so every link is validated
/// against the configured endpoints.
///
/// Both fields are required: a link missing its model must not quietly inherit
/// `jev-latest` and send it to an endpoint that has never heard of it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AiChainEntry {
    /// Which entry under `[ai.providers]` answers this link.
    pub provider: String,
    /// Model name, as the provider names it.
    pub model: String,
}

/// The defaults the single-pair shorthand falls back to, and what a `provider`/`model` left
/// untouched reads as when the chain is set. Validation refuses a non-default pair alongside
/// a chain; with no way to tell an explicitly written default from an inherited one, a pair
/// left at these values reads as "not set".
pub const DEFAULT_VERIFY_PROVIDER: &str = "typesafe";
pub const DEFAULT_VERIFY_MODEL: &str = "jev-latest";

impl Default for AiChainEntry {
    fn default() -> Self {
        Self {
            provider: DEFAULT_VERIFY_PROVIDER.to_string(),
            model: DEFAULT_VERIFY_MODEL.to_string(),
        }
    }
}

impl AiChainEntry {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
        }
    }
}

/// Parse `[ai.verify] chain`: either inline tables in the file, or — for
/// `CROSSTALK_AI__VERIFY__CHAIN` — a JSON string holding the same array.
///
/// An environment override arrives as untyped text, and figment has no syntax for a list of
/// tables, so the JSON string is the only way to set the chain without the file. A value
/// that parses as neither is a load error naming the field, not a silent empty chain.
fn deserialize_ai_chain<'de, D>(deserializer: D) -> Result<Vec<AiChainEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, SeqAccess, Visitor};

    struct ChainVisitor;

    impl<'de> Visitor<'de> for ChainVisitor {
        type Value = Vec<AiChainEntry>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .write_str("a list of {provider, model} tables, or a JSON string holding that list")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str(trimmed).map_err(|error| {
                E::custom(format!(
                    "ai.verify.chain: not a JSON list of {{provider, model}}: {error}"
                ))
            })
        }

        fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
            self.visit_str(&value)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut chain = Vec::new();
            while let Some(entry) = seq.next_element()? {
                chain.push(entry);
            }
            Ok(chain)
        }
    }

    deserializer.deserialize_any(ChainVisitor)
}

/// When the AI verifier is asked, and how its answers are read: `[ai.verify]`.
///
/// This was `guide.toml`'s `[ai]` block. It moved to the host file because verifying a
/// borderline match is the same decision for every sub-app that matches names, and because
/// the endpoint and key it refers to were already host-owned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct AiVerifyConfig {
    /// Ask the model about borderline or unmatched candidates.
    pub enabled: bool,
    /// Which entry under `[ai.providers]` in `crosstalk.toml` to use.
    pub provider: String,
    /// Model name, as the provider names it: `jev-latest`, `clef`, and so on.
    pub model: String,
    /// The escalation chain: asked in order, each link handling what the earlier ones could
    /// not decide. Empty means "the `provider`/`model` pair above, as one link": every
    /// existing file and every `CROSSTALK_AI__VERIFY__...` override keeps working, and setting
    /// both the pair and the chain is a validation error.
    #[serde(default, deserialize_with = "deserialize_ai_chain")]
    pub chain: Vec<AiChainEntry>,
    /// Score band, inclusive, in which a heuristic result is treated as uncertain.
    pub verify_band_low: u8,
    pub verify_band_high: u8,
    /// Ask the model about candidates when no heuristic result cleared the band.
    pub rescue: bool,
    /// Lowest heuristic score worth showing the model during a rescue.
    pub rescue_floor: u8,
    /// How many candidates to put in one request.
    pub shortlist: usize,
    /// Accept a candidate when the model's yes-probability reaches this.
    pub accept_threshold: f64,
    /// Drop a candidate outright at or below this.
    pub reject_threshold: f64,
    /// Minimum choice confidence needed to act on the model's pick.
    pub min_confidence: f64,
}

impl AiVerifyConfig {
    /// The links the gate actually asks, in order: the chain when set, otherwise the
    /// `provider`/`model` pair above as a single link.
    pub fn effective_chain(&self) -> Vec<AiChainEntry> {
        if self.chain.is_empty() {
            vec![AiChainEntry {
                provider: self.provider.clone(),
                model: self.model.clone(),
            }]
        } else {
            self.chain.clone()
        }
    }

    /// Whether the shorthand pair carries a non-default value while the chain is also set.
    /// That combination is refused by validation: with no way to tell an explicitly written
    /// default from an inherited one, a pair left at its defaults reads as "not set".
    pub fn has_shorthand_and_chain(&self) -> bool {
        !self.chain.is_empty()
            && (self.provider != DEFAULT_VERIFY_PROVIDER || self.model != DEFAULT_VERIFY_MODEL)
    }
}

impl Default for AiVerifyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: DEFAULT_VERIFY_PROVIDER.to_string(),
            model: DEFAULT_VERIFY_MODEL.to_string(),
            chain: Vec::new(),
            verify_band_low: 80,
            verify_band_high: 94,
            rescue: true,
            rescue_floor: 60,
            shortlist: 5,
            accept_threshold: 0.8,
            reject_threshold: 0.3,
            min_confidence: 0.5,
        }
    }
}

/// Which channels a run may touch: `[scope]`, shared by every sub-app.
///
/// These were `channel_profile_name`, `selected_groups` and `ignore_groups` in
/// `guide.toml`. They moved to the host file because "which channels am I responsible for"
/// is the same question for every sub-app, and answering it twice lets two apps disagree
/// about the same install.
///
/// Deliberately without a per-app override: no app needs one yet, and an optional `[scope]`
/// table in an app's file would mean the form, the CSV preamble and the validation each had
/// to know about a second place to look. Adding it later is a decision per app, not a
/// default.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ScopeConfig {
    /// Profile names to scope to, comma-separated. The first one is used by actions that
    /// need exactly one.
    pub profiles: String,
    /// Only process channels in these groups, comma-separated. Supports `*` and `?`
    /// wildcards. Cannot be combined with `ignore_groups`.
    pub groups: String,
    /// Exclude channels in these groups, applied after `groups`.
    pub ignore_groups: String,
}

/// How long CSV reports are kept: `[exports]`, shared by every sub-app.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ExportsConfig {
    /// Delete this app's exports older than this many days. 0 keeps everything.
    pub retention_days: u32,
}

/// Name-comparison settings shared by every sub-app.
///
/// These are in the host file, not a sub-app file, because a sub-app that normalised
/// names differently would match a different candidate for the same channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MatchingConfig {
    /// Strip `[HD]`, `[4K]`, `(Backup)` and friends before comparing.
    pub ignore_quality_tags: bool,
    /// Strip East/West/Pacific/Central/Mountain/Atlantic.
    pub ignore_regional_tags: bool,
    /// Strip country prefixes such as `US:` and `[CA]`.
    pub ignore_geographic_tags: bool,
    /// Strip other parenthesised tags.
    pub ignore_misc_tags: bool,
    /// Country database codes the matcher may draw names from: any of the merged data
    /// set's countries (`crosstalk_matching::data::merged::merged_codes`: AU, BR, CA, DE,
    /// DO, ES, FR, IN, MX, NL, NO, PE, PL, PT, UK, US). Empty means "the default set",
    /// which is every merged database when only one exists, otherwise the US one.
    pub enabled_databases: Vec<String>,
    /// Custom aliases, a table of `name -> [aliases]`. An entry REPLACES the built-in list
    /// for that key rather than adding to it.
    pub custom_aliases: BTreeMap<String, Vec<String>>,
    /// The one decision cut every app matches at (goal reset, D02/T10 `janitor-95`):
    /// a sensitivity preset (`strict`/`high`/`medium`/`low` = 95/90/80/70) or a bare
    /// number (`"80"`). Guide's own `match_confidence_threshold` /
    /// `heal_confidence_threshold` are retired in favour of this: one ranker, one cut.
    pub sensitivity: String,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            ignore_quality_tags: true,
            ignore_regional_tags: true,
            ignore_geographic_tags: true,
            ignore_misc_tags: true,
            enabled_databases: Vec::new(),
            custom_aliases: BTreeMap::new(),
            sensitivity: "strict".to_string(),
        }
    }
}

impl MatchingConfig {
    /// The normalisation flags this configuration describes.
    pub fn normalize_flags(&self) -> crosstalk_matching::normalize::NormalizeFlags {
        crosstalk_matching::normalize::NormalizeFlags {
            ignore_quality: self.ignore_quality_tags,
            ignore_regional: self.ignore_regional_tags,
            ignore_geographic: self.ignore_geographic_tags,
            ignore_misc: self.ignore_misc_tags,
            ..crosstalk_matching::normalize::NormalizeFlags::default()
        }
    }

    /// These settings in the matcher crate's own terms.
    ///
    /// `crosstalk-matching` cannot depend on this crate (the dependency runs the other way),
    /// so the host hands its values over rather than the matcher reading the file.
    ///
    /// Oracle tooling only (streams' Stream-Mapparr profile, under `cfg(test)` /
    /// `--features oracle`): production builds its engine from [`Self::engine_config`].
    pub fn matcher_config(&self) -> crosstalk_matching::MatcherConfig {
        crosstalk_matching::MatcherConfig {
            flags: self.normalize_flags(),
            databases: self.databases(),
            custom_aliases: self.custom_aliases.clone(),
            alias_country: None,
        }
    }

    /// The decision cut in force: the `[matching] sensitivity` preset resolved to 0-100
    /// (unknown names fall back to the decided 95; see `sensitivity_preset`).
    pub fn threshold(&self) -> u8 {
        crosstalk_matching::engine::sensitivity_preset(&self.sensitivity)
    }

    /// Whether `sensitivity` names a preset or a bare 0-100 number.
    pub fn sensitivity_is_valid(sensitivity: &str) -> bool {
        let trimmed = sensitivity.trim().to_lowercase();
        if ["low", "medium", "high", "strict"].contains(&trimmed.as_str()) {
            return true;
        }
        trimmed.parse::<u8>().is_ok_and(|cut| cut <= 100)
    }

    /// The unified engine a migrated sub-app runs with (goal reset, step 8+):
    /// the one normalizer, ladder, alias set and country rule, deciding at
    /// [`threshold`](Self::threshold).
    pub fn engine_config(&self) -> crosstalk_matching::engine::EngineConfig {
        crosstalk_matching::engine::EngineConfig {
            threshold: self.threshold(),
            enabled_countries: self.databases(),
            custom_aliases: self
                .custom_aliases
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            flags: self.normalize_flags(),
        }
    }

    /// Which country databases to load, out of the MERGED data set (the one table
    /// every app reads; phase 3 review).
    ///
    /// An explicit list wins. An empty list falls back to the plugin's rule: every
    /// database when only one ships, otherwise the United States one.
    pub fn databases(&self) -> Vec<String> {
        let available = crosstalk_matching::data::merged::merged_codes();
        if !self.enabled_databases.is_empty() {
            // Deduplicated, restricted to codes that actually ship, and sorted, which is
            // what `_select_enabled_databases` does: a stored code for a database that no
            // longer ships must not be reported as loaded forever.
            let mut codes: Vec<String> = self
                .enabled_databases
                .iter()
                .map(|code| code.trim().to_uppercase())
                .filter(|code| code.is_empty() || available.contains(&code.as_str()))
                .filter(|code| !code.is_empty())
                .collect();
            codes.sort();
            codes.dedup();
            return codes;
        }
        if available.len() == 1 {
            available.iter().map(|code| code.to_string()).collect()
        } else {
            vec!["US".to_string()]
        }
    }
}

/// One `[schedules]` entry.
///
/// A plain cron expression is all most entries need. An entry that carries a window runs for a
/// bounded stretch and checkpoints its progress, so a job too big for one night continues where
/// it stopped ([`crate::window`]):
///
/// ```toml
/// [schedules]
/// "guide.match_apply" = "0 4 * * *"
/// "streams.probe" = { cron = "0 23 * * *", window = "4h" }
/// "streams.probe_far" = { cron = "0 1 * * *", until = "06:00", timezone = "America/New_York" }
/// ```
///
/// The cron expression stays UTC, which is what the scheduler fires on; only `until` is read in
/// `timezone`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleEntry {
    /// `"guide.match_apply" = "0 4 * * *"`.
    Cron(String),
    /// `{ cron = "...", window = "4h" }`.
    Windowed(ScheduleTable),
}

/// The table form of a [`ScheduleEntry`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleTable {
    /// The cron expression: five or six fields, UTC.
    pub cron: String,
    /// How long one firing may run, for example `"4h"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// The wall-clock time of day the run must stop at, for example `"06:00"`. Wraps past
    /// midnight when it has already passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// The zone `until` is read in. Omitted means UTC, like the cron expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

impl ScheduleEntry {
    /// A plain cron entry. The shorthand every caller that only wants an expression uses.
    pub fn cron(expression: impl Into<String>) -> Self {
        Self::Cron(expression.into())
    }

    /// The cron expression, whichever shape the entry has.
    pub fn cron_expression(&self) -> &str {
        match self {
            Self::Cron(expression) => expression,
            Self::Windowed(table) => &table.cron,
        }
    }

    /// The window this entry arms, or `None` for a plain cron entry.
    ///
    /// `Err` is a window an operator mistyped; [`CrosstalkConfig::validate`] reports it before
    /// the daemon arms anything.
    pub fn window(&self) -> Result<Option<crate::window::Window>> {
        match self {
            Self::Cron(_) => Ok(None),
            Self::Windowed(table) => crate::window::Window::parse(
                table.window.as_deref(),
                table.until.as_deref(),
                table.timezone.as_deref(),
            ),
        }
    }

    /// Whether the entry runs inside a window at all.
    pub fn is_windowed(&self) -> bool {
        matches!(self, Self::Windowed(table) if table.window.is_some() || table.until.is_some())
    }

    /// The window in words, for the dashboard and the run report. `None` when there is none.
    pub fn window_description(&self) -> Option<String> {
        self.window().ok().flatten().map(|window| window.describe())
    }
}

impl From<&str> for ScheduleEntry {
    fn from(expression: &str) -> Self {
        Self::Cron(expression.to_string())
    }
}

impl From<String> for ScheduleEntry {
    fn from(expression: String) -> Self {
        Self::Cron(expression)
    }
}

impl std::fmt::Display for ScheduleEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.cron_expression())?;
        if let Some(window) = self.window_description() {
            write!(f, " ({window})")?;
        }
        Ok(())
    }
}

/// Deserialize a schedule entry as either a string or a table.
///
/// Hand-written rather than `#[serde(untagged)]` so a typo inside the table names the field it
/// did not understand instead of "data did not match any variant".
impl<'de> Deserialize<'de> for ScheduleEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct EntryVisitor;

        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = ScheduleEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "a cron expression string, or a table with cron, window, until and timezone",
                )
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(ScheduleEntry::Cron(value.to_string()))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(ScheduleEntry::Cron(value))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let table = ScheduleTable::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(ScheduleEntry::Windowed(table))
            }
        }

        deserializer.deserialize_any(EntryVisitor)
    }
}

impl Serialize for ScheduleEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Cron(expression) => serializer.serialize_str(expression),
            Self::Windowed(table) => table.serialize(serializer),
        }
    }
}

/// Where one workflow step reads its channels from: a scope override for that step.
///
/// A bare name (`scope = "lineup"`) is a named scope: the channels one app offers under that
/// name, resolved through the [`crate::subapp::Registry`] at run time (a table there would
/// pin the step to today's group names; a name follows the app's lineup instead). A table
/// (`scope = { groups = "DIRECTV: *" }`) is an inline [`ScopeConfig`] that REPLACES the
/// host `[scope]` for that step. No `scope` key means the step runs under the host `[scope]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowStepScope {
    /// A named scope one app provides (`lineup`, or a test's fake scope).
    Named(String),
    /// An inline scope table, replacing the host `[scope]` for this step.
    Table(ScopeConfig),
}

impl WorkflowStepScope {
    /// How the override reads in the dashboard, the run page and the CLI.
    pub fn describe(&self) -> String {
        match self {
            Self::Named(name) => format!("scope {name:?}"),
            Self::Table(scope) => {
                let mut parts = Vec::new();
                if !scope.profiles.trim().is_empty() {
                    parts.push(format!("profiles {:?}", scope.profiles));
                }
                if !scope.groups.trim().is_empty() {
                    parts.push(format!("groups {:?}", scope.groups));
                }
                if !scope.ignore_groups.trim().is_empty() {
                    parts.push(format!("ignore_groups {:?}", scope.ignore_groups));
                }
                if parts.is_empty() {
                    "scope {} (every channel)".to_string()
                } else {
                    format!("scope {}", parts.join(", "))
                }
            }
        }
    }
}

/// Deserialize a scope override as a bare name or an inline table.
///
/// Hand-written rather than `#[serde(untagged)]` so a mistyped table key names the field it
/// did not understand instead of "data did not match any variant".
impl<'de> Deserialize<'de> for WorkflowStepScope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct ScopeVisitor;

        impl<'de> Visitor<'de> for ScopeVisitor {
            type Value = WorkflowStepScope;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a named scope, or an inline scope table")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(WorkflowStepScope::Named(value.to_string()))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(WorkflowStepScope::Named(value))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let scope = ScopeConfig::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(WorkflowStepScope::Table(scope))
            }
        }

        deserializer.deserialize_any(ScopeVisitor)
    }
}

impl Serialize for WorkflowStepScope {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Named(name) => serializer.serialize_str(name),
            Self::Table(scope) => scope.serialize(serializer),
        }
    }
}

/// Whether a `continue_on_failure` flag reads as unset, for the save's shape.
///
/// A table step written without the flag reloads as a table step without it; spelling
/// `continue_on_failure = false` everywhere would bury the steps that do carry on.
fn continue_unset(value: &bool) -> bool {
    !value
}

/// The table form of a workflow step: an action plus its per-step settings.
///
/// ```toml
/// [workflows.full_sync]
/// steps = [
///   "lineup.sync_apply",
///   { action = "streams.match_apply", scope = "lineup" },
///   { action = "guide.match_apply", scope = { groups = "DIRECTV: *" }, continue_on_failure = true },
/// ]
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowStepDetail {
    /// The `<app>.<action>` to run, never another workflow.
    pub action: String,
    /// Where the step reads its channels from, instead of the host `[scope]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<WorkflowStepScope>,
    /// Carry on past this step when it fails. A cancelled or paused step still stops the
    /// workflow; the workflow itself is recorded failed when any step failed.
    #[serde(default, skip_serializing_if = "continue_unset")]
    pub continue_on_failure: bool,
}

/// One entry of a workflow's `steps` list: today a bare `"<app>.<action>"` string, or a
/// table carrying that action plus a scope override and a failure policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowStepEntry {
    /// `"streams.probe"`: the shape every existing file uses.
    Simple(String),
    /// `{ action = "streams.match_apply", scope = ..., continue_on_failure = ... }`.
    Detailed(WorkflowStepDetail),
}

impl WorkflowStepEntry {
    /// The `<app>.<action>` this entry runs.
    pub fn action(&self) -> &str {
        match self {
            Self::Simple(action) => action,
            Self::Detailed(detail) => &detail.action,
        }
    }

    /// The step's scope override, when it carries one.
    pub fn scope(&self) -> Option<&WorkflowStepScope> {
        match self {
            Self::Simple(_) => None,
            Self::Detailed(detail) => detail.scope.as_ref(),
        }
    }

    /// Whether the workflow continues past this step's failure.
    pub fn continue_on_failure(&self) -> bool {
        match self {
            Self::Simple(_) => false,
            Self::Detailed(detail) => detail.continue_on_failure,
        }
    }

    /// The entry as written, for an error or a display that names the step.
    pub fn display(&self) -> String {
        match self {
            Self::Simple(action) => action.clone(),
            Self::Detailed(detail) => {
                let mut text = detail.action.clone();
                if let Some(scope) = &detail.scope {
                    text.push_str(&format!(" [{}]", scope.describe()));
                }
                if detail.continue_on_failure {
                    text.push_str(" [continue on failure]");
                }
                text
            }
        }
    }
}

impl From<&str> for WorkflowStepEntry {
    fn from(action: &str) -> Self {
        Self::Simple(action.to_string())
    }
}

impl From<String> for WorkflowStepEntry {
    fn from(action: String) -> Self {
        Self::Simple(action)
    }
}

impl From<WorkflowStepDetail> for WorkflowStepEntry {
    fn from(detail: WorkflowStepDetail) -> Self {
        Self::Detailed(detail)
    }
}

/// Deserialize a step as a bare action string or an action table.
///
/// Hand-written rather than `#[serde(untagged)]` so a table with a mistyped key names the
/// key instead of falling back to "expected a string".
impl<'de> Deserialize<'de> for WorkflowStepEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct EntryVisitor;

        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = WorkflowStepEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "a \"<app>.<action>\" string, or a table with action, scope and \
                     continue_on_failure",
                )
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(WorkflowStepEntry::Simple(value.to_string()))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(WorkflowStepEntry::Simple(value))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let detail =
                    WorkflowStepDetail::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(WorkflowStepEntry::Detailed(detail))
            }
        }

        deserializer.deserialize_any(EntryVisitor)
    }
}

impl Serialize for WorkflowStepEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            // A bare string stays a bare string: a save round-trips the shape the operator
            // wrote (comments survive through `toml_edit`; the shape survives here).
            Self::Simple(action) => serializer.serialize_str(action),
            Self::Detailed(detail) => detail.serialize(serializer),
        }
    }
}

/// One `[workflows.<name>]` entry: a named sequence of actions the host runs in order.
///
/// ```toml
/// [workflows.nightly]
/// steps = ["streams.probe", "streams.sort_apply"]
/// ```
///
/// A step is an `<app>.<action>` like a schedule key, or a table carrying that action plus a
/// scope override and a failure policy (see [`WorkflowStepDetail`]). A step may NOT be another
/// workflow: the sequence is a flat list, so there is no recursion to guard against at run
/// time. Steps run through the same [`crate::runner::JobRunner`] a manual or scheduled action
/// does, which is why the scheduler's dry-run rule holds for them too: a `Read` step is a dry
/// run and a writing step really writes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowConfig {
    /// The steps, in order: a bare action string, or a table with the action plus its
    /// scope override and failure policy.
    pub steps: Vec<WorkflowStepEntry>,
    /// Start each step's backlog from the beginning when this workflow begins a new pass.
    ///
    /// Off by default, which is the resume-friendly choice: a `probe` that paused at its window's
    /// end continues where it stopped, then the next step runs. On, every step of a new pass
    /// discards its saved progress — what an operator wants after widening the scope. A pass that
    /// RESUMES a paused workflow never discards anything, whatever this says.
    #[serde(default)]
    pub fresh: bool,
}

/// The `[hooks]` table: what Dispatcharr Connect events trigger, and how deliveries are paced.
///
/// ```toml
/// [hooks]
/// debounce_secs = 60
/// rate_per_minute = 60
/// m3u_refresh = ["streams.match_apply"]
/// epg_refresh = ["workflow.nightly"]
/// ```
///
/// The event names are open-ended (`m3u_refresh`, `epg_refresh`, `m3u_error`, … — whatever the
/// operator subscribes to in Dispatcharr), so they are the table's unknown keys rather than a
/// fixed list. A target is an `<app>.<action>` or a `workflow.<name>`, validated at load like a
/// schedule key.
#[derive(Debug, Clone, PartialEq)]
pub struct HooksConfig {
    /// How long one event must be quiet before its targets run.
    ///
    /// Several M3U accounts finishing a refresh within a few seconds is one logical refresh, not
    /// six matches: the timer restarts on every delivery of the same event and the run happens
    /// once, after the burst settles. Zero means "run on the first delivery".
    pub debounce_secs: f64,
    /// The longest a burst may keep postponing its run, in seconds.
    ///
    /// Without a bound, deliveries that keep arriving inside the debounce window (an account
    /// refreshing every 30 s against a 60 s debounce) would restart the timer forever and the
    /// targets would never run. Unset means five debounce windows; the value is capped at
    /// [`HooksConfig::MAX_SETTLE_CAP_SECS`].
    pub max_settle_secs: Option<f64>,
    /// Deliveries served per client address per minute, for the HTTP route.
    ///
    /// A hook delivery starts real work, so an unauthenticated flood (or a Connect integration
    /// configured twice) must not start a run per request. Zero disables the limit.
    pub rate_per_minute: u32,
    /// Event name to what it triggers, in the order the hook fires them.
    pub events: BTreeMap<String, Vec<String>>,
}

impl Default for HooksConfig {
    fn default() -> Self {
        Self {
            // The burst window several M3U accounts need to finish: long enough to collapse a
            // refresh of a dozen accounts, short enough that a nightly match still runs promptly.
            debounce_secs: 60.0,
            max_settle_secs: None,
            rate_per_minute: 60,
            events: BTreeMap::new(),
        }
    }
}

impl HooksConfig {
    /// The ceiling on `max_settle_secs`: a quarter of an hour.
    pub const MAX_SETTLE_CAP_SECS: f64 = 900.0;

    /// How long after a burst's FIRST delivery its targets run at the latest: `max_settle_secs`,
    /// or five debounce windows when unset, capped at fifteen minutes.
    pub fn max_settle(&self) -> std::time::Duration {
        let debounce = self.debounce().as_secs_f64();
        let secs = match self.max_settle_secs {
            Some(secs) if secs.is_finite() && secs >= 0.0 => secs,
            _ => debounce * 5.0,
        };
        std::time::Duration::from_secs_f64(secs.min(Self::MAX_SETTLE_CAP_SECS))
    }

    /// The debounce window as a duration.
    pub fn debounce(&self) -> std::time::Duration {
        let millis = if self.debounce_secs.is_finite() && self.debounce_secs > 0.0 {
            // Saturating rather than casting blindly: a config can say 1e20.
            (self.debounce_secs * 1000.0).min(u64::MAX as f64) as u64
        } else {
            0
        };
        std::time::Duration::from_millis(millis)
    }

    /// The targets one event name triggers, if it is configured.
    pub fn targets(&self, event: &str) -> &[String] {
        self.events.get(event).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Deserialize `[hooks]` as a table whose known keys are the pacing settings and whose other
/// keys are event names.
///
/// Hand-written rather than a `#[serde(flatten)]` map: flattening would feed `debounce_secs` to
/// the event map as well (serde's flatten sees every key), so the scalar settings would fail to
/// parse as a list of targets. Writing the visitor also names the offending key on a type error.
impl<'de> Deserialize<'de> for HooksConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor};

        struct HooksVisitor;

        impl<'de> Visitor<'de> for HooksVisitor {
            type Value = HooksConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "a table of debounce_secs, max_settle_secs, rate_per_minute and event name \
                     -> [targets]",
                )
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut config = HooksConfig::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "debounce_secs" => config.debounce_secs = map.next_value()?,
                        "max_settle_secs" => config.max_settle_secs = Some(map.next_value()?),
                        "rate_per_minute" => config.rate_per_minute = map.next_value()?,
                        _ => {
                            let targets: Vec<String> = map.next_value().map_err(|error| {
                                serde::de::Error::custom(format!(
                                    "hooks.{key}: expected a list of targets: {error}"
                                ))
                            })?;
                            config.events.insert(key, targets);
                        }
                    }
                }
                Ok(config)
            }
        }

        deserializer.deserialize_map(HooksVisitor)
    }
}

impl Serialize for HooksConfig {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let settle = usize::from(self.max_settle_secs.is_some());
        let mut map = serializer.serialize_map(Some(2 + settle + self.events.len()))?;
        map.serialize_entry("debounce_secs", &self.debounce_secs)?;
        if let Some(secs) = self.max_settle_secs {
            map.serialize_entry("max_settle_secs", &secs)?;
        }
        map.serialize_entry("rate_per_minute", &self.rate_per_minute)?;
        for (event, targets) in &self.events {
            map.serialize_entry(event, targets)?;
        }
        map.end()
    }
}

/// Filesystem and thread-pool settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobsConfig {
    /// Worker threads for the matching stage. 0 means "one per core".
    pub worker_threads: usize,
    /// Where CSV reports are written.
    pub exports_dir: String,
    /// Where the SQLite database lives.
    pub data_dir: String,
    /// Days of run history to keep. 0 keeps everything. Pruned after each run, keeping
    /// at least the newest 10 runs per app so recent history survives even on a busy
    /// install.
    #[serde(default = "default_run_history_days")]
    pub run_history_days: u32,
}

fn default_run_history_days() -> u32 {
    15
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            worker_threads: 0,
            exports_dir: "exports".to_string(),
            data_dir: "data".to_string(),
            run_history_days: default_run_history_days(),
        }
    }
}

impl JobsConfig {
    /// The worker count to use, resolving 0 to the available parallelism.
    pub fn resolved_worker_threads(&self) -> usize {
        if self.worker_threads > 0 {
            self.worker_threads
        } else {
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
        }
    }
}

/// The host configuration file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CrosstalkConfig {
    pub server: ServerConfig,
    pub dispatcharr: DispatcharrConfig,
    pub ai: AiConfig,
    pub matching: MatchingConfig,
    /// What a run may touch. Shared, so every sub-app selects the same channels.
    pub scope: ScopeConfig,
    pub exports: ExportsConfig,
    pub jobs: JobsConfig,
    /// Cron expressions per `<app>.<action>`, for example `guide.match_apply = "0 4 * * *"`,
    /// or a table that adds a window: `{ cron = "0 23 * * *", window = "4h" }`. A workflow is
    /// scheduled the same way, as `workflow.<name>`.
    pub schedules: BTreeMap<String, ScheduleEntry>,
    /// Named step sequences: `[workflows.nightly] steps = ["streams.probe", ...]`.
    pub workflows: BTreeMap<String, WorkflowConfig>,
    /// What Dispatcharr Connect events trigger: `[hooks] m3u_refresh = ["streams.match_apply"]`.
    pub hooks: HooksConfig,
    /// Directory the configuration was loaded from. Not part of the file.
    #[serde(skip)]
    pub config_dir: PathBuf,
    /// How many environment overrides were applied when loading. Non-zero means the value
    /// in memory is not the value in the file, so it must never be written back.
    #[serde(skip)]
    pub env_overrides: usize,
}

impl CrosstalkConfig {
    /// Load `<dir>/crosstalk.toml`, applying `CROSSTALK_`-prefixed environment overrides.
    ///
    /// A missing file yields the defaults with a warning rather than an error: the daemon
    /// has to start to be configured, and the web UI is where most people will set things
    /// up.
    pub fn load(dir: impl AsRef<Path>) -> Result<Self> {
        Self::load_prefixed(dir, "CROSSTALK_")
    }

    /// The configuration exactly as the file holds it, with no environment overrides.
    ///
    /// This is what must be SAVED. Writing the effective configuration would copy every
    /// secret the environment supplied into the file, which is the opposite of why those
    /// secrets are in the environment.
    pub fn load_file(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let path = dir.join("crosstalk.toml");
        let mut config = if path.exists() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            toml::from_str::<Self>(&text).with_context(|| format!("parsing {}", path.display()))?
        } else {
            Self::default()
        };
        config.config_dir = dir;
        config.env_overrides = 0;
        Ok(config)
    }

    /// [`CrosstalkConfig::load`] with a caller-chosen environment prefix.
    ///
    /// Tests use a unique prefix because environment variables are process-global, and a
    /// test that sets `CROSSTALK_SERVER__PORT` would otherwise rewrite the port for every
    /// other test running in parallel.
    pub fn load_prefixed(dir: impl AsRef<Path>, env_prefix: &str) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let path = dir.join("crosstalk.toml");
        let mut config = if path.exists() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            toml::from_str::<Self>(&text).with_context(|| format!("parsing {}", path.display()))?
        } else {
            Self::default()
        };
        config.config_dir = dir;
        config.apply_env_overrides(env_prefix)?;
        Ok(config)
    }

    /// Apply `CROSSTALK_`-prefixed overrides such as `CROSSTALK_SERVER__PORT=9000`.
    ///
    /// A double underscore separates nested keys, which keeps a container deployment from
    /// having to bind-mount a file to change one value.
    fn apply_env_overrides(&mut self, env_prefix: &str) -> Result<()> {
        let before = toml::to_string(self).unwrap_or_default();
        use figment::providers::Env;
        use figment::Figment;

        // Read the environment as a TOML overlay, then keep only the subtrees that name a
        // real top-level section. The same prefix namespace also carries secrets consumed
        // through `*_env` indirection (`CROSSTALK_TOKEN`, `CROSSTALK_DISPATCHARR_API_KEY`),
        // which are not config paths: with unknown fields now rejected, letting them reach
        // the merge would fail every load in a container deployment. Unknown keys under a
        // real section still fail loudly when the overlay is deserialised below.
        let overlay: toml::Table = Figment::from(Env::prefixed(env_prefix).split("__"))
            .extract()
            .map_err(|error| anyhow::anyhow!("invalid environment override: {error}"))?;
        const SECTIONS: [&str; 10] = [
            "server",
            "dispatcharr",
            "ai",
            "matching",
            "scope",
            "exports",
            "jobs",
            "schedules",
            "workflows",
            "hooks",
        ];
        let overlay: toml::Table = overlay
            .into_iter()
            .filter(|(key, _)| SECTIONS.contains(&key.as_str()))
            .collect();

        let mut base: toml::Table =
            toml::from_str(&before).context("serialising the configuration for overrides")?;
        merge_toml_overlay(&mut base, overlay);
        let merged: Self = base.try_into().map_err(|error| {
            // A malformed override should not silently do nothing.
            anyhow::anyhow!("invalid environment override: {error}")
        })?;
        let dir = std::mem::take(&mut self.config_dir);
        let overrides = self.env_overrides;
        *self = merged;
        self.config_dir = dir;
        let after = toml::to_string(self).unwrap_or_default();
        self.env_overrides = overrides + usize::from(before != after);
        Ok(())
    }

    /// Absolute path of the exports directory.
    pub fn exports_dir(&self) -> PathBuf {
        self.resolve_dir(&self.jobs.exports_dir)
    }

    /// Absolute path of the data directory.
    pub fn data_dir(&self) -> PathBuf {
        self.resolve_dir(&self.jobs.data_dir)
    }

    fn resolve_dir(&self, value: &str) -> PathBuf {
        let path = Path::new(value);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.config_dir.join(path)
        }
    }

    /// Problems an operator needs to fix, as human-readable strings.
    ///
    /// Warnings are separated from errors so the UI can show "this will not work" and
    /// "this is a bad idea" differently.
    pub fn validate(&self) -> ConfigReport {
        let mut report = ConfigReport::default();

        if self.dispatcharr.base_url.trim().is_empty() {
            report
                .errors
                .push("dispatcharr.base_url is empty".to_string());
        } else if !(self.dispatcharr.base_url.trim().starts_with("http://")
            || self.dispatcharr.base_url.trim().starts_with("https://"))
        {
            report
                .errors
                .push("dispatcharr.base_url must start with http:// or https://".to_string());
        }
        if !self.dispatcharr.has_credentials() {
            report.errors.push(
                "no Dispatcharr credentials: set dispatcharr.api_key or username/password"
                    .to_string(),
            );
        }
        if self.dispatcharr.concurrency == 0 {
            report
                .errors
                .push("dispatcharr.concurrency must be at least 1".to_string());
        }
        if url_is_plaintext_to_remote(&self.dispatcharr.base_url) {
            report.warnings.push(format!(
                "dispatcharr.base_url uses plaintext http:// to a non-loopback host {}: the \
                 API key travels in cleartext. Prefer https:// or keep Dispatcharr on this host.",
                url_host(&self.dispatcharr.base_url)
            ));
        }

        if self.server.port == 0 {
            report.errors.push("server.port must not be 0".to_string());
        }
        report.errors.extend(self.server.security_errors());
        if !self.server.is_loopback() && self.server.resolved_token().is_none() {
            report.warnings.push(format!(
                "server.bind is {} with no token: anyone who can reach the port can run \
                 apply actions. Set server.token or server.token_env.",
                self.server.bind
            ));
        }

        for (name, provider) in &self.ai.providers {
            if provider.kind != "typesafe" && provider.kind != "openai_compat" {
                report.errors.push(format!(
                    "ai.providers.{name}.kind must be typesafe or openai_compat, got {:?}",
                    provider.kind
                ));
            }
            // An environment override is lowercased on its way in, so a mixed-case provider
            // name cannot be reached by `CROSSTALK_AI__PROVIDERS__<NAME>__...`.
            if name != &name.to_lowercase() {
                report.errors.push(format!(
                    "ai.providers.{name}: provider names must be lowercase so environment \
                     overrides can address them"
                ));
            }
            if provider.base_url.trim().is_empty() {
                report
                    .errors
                    .push(format!("ai.providers.{name}.base_url is empty"));
            }
            if provider.timeout_secs == 0 {
                report.errors.push(format!(
                    "ai.providers.{name}.timeout_secs must be at least 1"
                ));
            }
            if provider.max_concurrency == 0 {
                report.errors.push(format!(
                    "ai.providers.{name}.max_concurrency must be at least 1"
                ));
            }
            if provider.resolved_api_key().is_none() && provider.kind == "typesafe" {
                // A local openai_compat endpoint may need no key at all; a hosted one does.
                report.warnings.push(format!(
                    "ai.providers.{name} has no api_key; AI features that use it will be skipped"
                ));
            }
            if url_is_plaintext_to_remote(&provider.base_url) {
                report.warnings.push(format!(
                    "ai.providers.{name}.base_url uses plaintext http:// to a non-loopback host {}: \
                     the API key travels in cleartext. Prefer https:// or keep the endpoint on this host.",
                    url_host(&provider.base_url)
                ));
            }
        }

        if !MatchingConfig::sensitivity_is_valid(&self.matching.sensitivity) {
            report.errors.push(format!(
                "matching.sensitivity is {:?}, which is not a preset (strict/high/medium/low) \
                 or a 0-100 number; the matcher would silently decide at 95. Pick one of the \
                 presets or write the cut as a number, e.g. sensitivity = \"high\"",
                self.matching.sensitivity
            ));
        }

        let available = crosstalk_matching::data::merged::merged_codes();
        for code in &self.matching.enabled_databases {
            let upper = code.trim().to_uppercase();
            if !available.contains(&upper.as_str()) {
                report.warnings.push(format!(
                    "matching.enabled_databases lists {code:?}, which is not in the merged \
                     data set (available: {})",
                    available.join(", ")
                ));
            }
        }

        // The scope rules were the guide app's own until the values moved here; they are
        // still configuration mistakes an operator can fix, not empty results to explain.
        if !self.scope.groups.trim().is_empty() && !self.scope.ignore_groups.trim().is_empty() {
            report.errors.push(
                "scope.groups and scope.ignore_groups cannot both be set: use one or the other"
                    .to_string(),
            );
        }

        let verify = &self.ai.verify;
        if verify.verify_band_low > verify.verify_band_high {
            report.errors.push(format!(
                "ai.verify.verify_band_low ({}) is above ai.verify.verify_band_high ({})",
                verify.verify_band_low, verify.verify_band_high
            ));
        }
        if verify.reject_threshold >= verify.accept_threshold {
            report.errors.push(format!(
                "ai.verify.reject_threshold ({}) must be below ai.verify.accept_threshold ({})",
                verify.reject_threshold, verify.accept_threshold
            ));
        }
        if verify.rescue_floor > verify.verify_band_low {
            report.errors.push(format!(
                "ai.verify.rescue_floor ({}) is above ai.verify.verify_band_low ({}), so a \
                 rescued channel would be one the band already covers",
                verify.rescue_floor, verify.verify_band_low
            ));
        }
        if verify.shortlist == 0 {
            report
                .errors
                .push("ai.verify.shortlist must be at least 1".to_string());
        }
        if verify.enabled && verify.provider.trim().is_empty() {
            report
                .errors
                .push("ai.verify.enabled is on but ai.verify.provider is empty".to_string());
        }
        if verify.enabled && verify.model.trim().is_empty() {
            report
                .errors
                .push("ai.verify.enabled is on but ai.verify.model is empty".to_string());
        }
        if verify.has_shorthand_and_chain() {
            report.errors.push(
                "ai.verify.chain is set and ai.verify.provider/model is also set (in the \
                 file or a CROSSTALK_AI__VERIFY__PROVIDER/MODEL override): use the chain or \
                 the single provider/model pair, not both (leave provider and model at their \
                 defaults to use the chain)"
                    .to_string(),
            );
        }
        // An explicit chain names its endpoints link by link, so a typo'd provider is a
        // validation error rather than a run that silently degrades to heuristic-only. The
        // shorthand pair keeps its old behaviour (a warning at ask time, heuristic-only):
        // refusing it here would turn every existing enabled-without-provider install from
        // a warning into a failed run.
        if verify.enabled && !verify.chain.is_empty() {
            for (position, entry) in verify.chain.iter().enumerate() {
                if entry.provider.trim().is_empty() {
                    report
                        .errors
                        .push(format!("ai.verify.chain[{position}].provider is empty"));
                } else if !self.ai.providers.contains_key(entry.provider.trim()) {
                    report.errors.push(format!(
                        "ai.verify.chain[{position}].provider names {:?}, which has no \
                         [ai.providers.{}] entry",
                        entry.provider,
                        entry.provider.trim()
                    ));
                }
                if entry.model.trim().is_empty() {
                    report
                        .errors
                        .push(format!("ai.verify.chain[{position}].model is empty"));
                }
            }
        }

        for (key, entry) in &self.schedules {
            if !key.contains('.') {
                report.errors.push(format!(
                    "schedules.{key} must be <app>.<action>, e.g. guide.match_apply"
                ));
            }
            // A workflow is scheduled under its own name space. Naming one that does not exist
            // would arm nothing, which is exactly the typo this catches: the scheduler skips
            // it and `crosstalk validate` says why.
            if let Some(name) = key.strip_prefix("workflow.") {
                if !self.workflows.contains_key(name) {
                    report
                        .errors
                        .push(format!("schedules.{key} names no [workflows.{name}] entry"));
                }
            }
            // Parsed, not merely counted: a five-field expression with a misspelled weekday
            // would otherwise validate and then never fire.
            if let Err(error) = crate::scheduler::parse_schedule(entry.cron_expression()) {
                report.errors.push(format!("schedules.{key}: {error:#}"));
            }
            // A window is parsed here for the same reason: "4" or a misspelled zone would
            // otherwise arm a schedule whose runs never pause, or never end.
            if let Err(error) = entry.window() {
                report.errors.push(format!("schedules.{key}: {error:#}"));
            }
        }

        // Workflows: every step must be a step and not another workflow. The registered-action
        // and named-scope checks need the registry, so they live in
        // `Registry::unknown_workflow_steps` and `Registry::unknown_workflow_scopes`; what can
        // be checked here is the shape, which is what a step naming a workflow gets wrong.
        // Every error names the step's index, so a ten-step workflow's typo is found at once.
        for (name, workflow) in &self.workflows {
            if workflow.steps.is_empty() {
                report.errors.push(format!("workflows.{name} has no steps"));
            }
            for (index, step) in workflow.steps.iter().enumerate() {
                let action = step.action();
                if action.trim().starts_with("workflow.") {
                    report.errors.push(format!(
                        "workflows.{name}.steps[{index}]: {action} is a workflow, and a workflow \
                         step must be an <app>.<action>: workflows do not nest"
                    ));
                } else if !action.trim().contains('.') {
                    report.errors.push(format!(
                        "workflows.{name}.steps[{index}]: {action} must be <app>.<action>, e.g. \
                         streams.probe"
                    ));
                }
                match step.scope() {
                    Some(WorkflowStepScope::Named(scope)) if scope.trim().is_empty() => {
                        report.errors.push(format!(
                            "workflows.{name}.steps[{index}]: the scope name is empty"
                        ));
                    }
                    Some(WorkflowStepScope::Table(scope))
                        if !scope.groups.trim().is_empty()
                            && !scope.ignore_groups.trim().is_empty() =>
                    {
                        report.errors.push(format!(
                            "workflows.{name}.steps[{index}]: scope.groups and \
                             scope.ignore_groups cannot both be set: use one or the other"
                        ));
                    }
                    Some(_) | None => {}
                }
            }
        }

        // Hooks: the route turns every delivery into real work, so a nonsense pacing value or a
        // target that is not an <app>.<action> / workflow.<name> is an error rather than a
        // surprise at 03:00.
        if !self.hooks.debounce_secs.is_finite() || self.hooks.debounce_secs < 0.0 {
            report.errors.push(format!(
                "hooks.debounce_secs must be a number of seconds (0 runs on the first \
                 delivery), got {}",
                self.hooks.debounce_secs
            ));
        }
        if let Some(secs) = self.hooks.max_settle_secs {
            if !secs.is_finite() || secs < 0.0 {
                report.errors.push(format!(
                    "hooks.max_settle_secs must be a number of seconds, got {secs}"
                ));
            } else if secs > HooksConfig::MAX_SETTLE_CAP_SECS {
                report.warnings.push(format!(
                    "hooks.max_settle_secs is {secs}; bursts settle after at most {} s",
                    HooksConfig::MAX_SETTLE_CAP_SECS
                ));
            }
        }
        for (event, targets) in &self.hooks.events {
            if targets.is_empty() {
                report.warnings.push(format!(
                    "hooks.{event} lists no targets: the event is delivered and ignored"
                ));
            }
            for target in targets {
                let is_workflow = target
                    .strip_prefix("workflow.")
                    .is_some_and(|name| self.workflows.contains_key(name));
                if !is_workflow && !target.contains('.') {
                    report.errors.push(format!(
                        "hooks.{event}: {target} must be <app>.<action> or workflow.<name>"
                    ));
                }
            }
        }

        report
    }

    /// Write the config back to `path`, preserving comments.
    ///
    /// Refuses a configuration that had environment overrides applied: those values came
    /// from the environment, and writing them here would put a secret in a file.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        if self.env_overrides > 0 {
            anyhow::bail!(
                "refusing to save a configuration that had environment overrides applied: \
                 save a `load_file` configuration instead"
            );
        }
        save_with_comments(path.as_ref(), self)
    }
}

/// Whether a base URL sends secrets in cleartext: `http://` to a non-loopback host.
///
/// Loopback stays quiet (the daemon itself defaults to `http://localhost:9191`), and so
/// does anything that is not even an `http://` URL — the scheme check in `validate`
/// already errors on those. Only a plaintext scheme aimed off-host warns.
fn url_is_plaintext_to_remote(base_url: &str) -> bool {
    let lower = base_url.trim().to_lowercase();
    if !lower.starts_with("http://") {
        return false;
    }
    !is_loopback_host(&url_host(base_url))
}

/// The host portion of a base URL, lowercased, without port or brackets.
fn url_host(base_url: &str) -> String {
    let lower = base_url.trim().to_lowercase();
    let rest = lower
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(lower.as_str());
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // Drop userinfo (`user:pass@host`): the host is what the loopback check needs.
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if let Some(bracketed) = authority.strip_prefix('[') {
        return bracketed.split(']').next().unwrap_or("").to_string();
    }
    authority.split(':').next().unwrap_or("").to_string()
}

/// Whether a URL host is this machine: `localhost`, `127.0.0.0/8`, or `::1`.
fn is_loopback_host(host: &str) -> bool {
    if host == "localhost" || host == "::1" {
        return true;
    }
    let parts: Vec<&str> = host.split('.').collect();
    parts.len() == 4
        && parts[0] == "127"
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

/// The daemon's live configuration: one value shared by the runner, the scheduler and
/// the web UI, swapped wholesale when the operator saves host settings.
///
/// A run snapshots the config when it starts ([`SharedConfig::snapshot`]) and builds its
/// [`crate::jobs::JobContext`] from that copy, so saving settings mid-run changes the
/// NEXT run, never the one already in flight. Schedules re-arm from the same swap.
#[derive(Debug, Clone)]
pub struct SharedConfig {
    inner: std::sync::Arc<std::sync::RwLock<CrosstalkConfig>>,
}

impl SharedConfig {
    pub fn new(config: CrosstalkConfig) -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(config)),
        }
    }

    /// A point-in-time copy. Runs take one at start; request handlers take one per request.
    pub fn snapshot(&self) -> CrosstalkConfig {
        self.read().clone()
    }

    /// Read the live value. Recovers from a poisoned lock rather than panicking: a poisoned
    /// config would take the whole daemon down on the next request, long after whatever
    /// panicked while holding it.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, CrosstalkConfig> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Swap in a freshly loaded config, for example after the settings page saved the file.
    pub fn replace(&self, config: CrosstalkConfig) {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = config;
    }

    /// Where the configuration was loaded from. Needed to reload after a save without
    /// snapshotting the whole value.
    pub fn config_dir(&self) -> std::path::PathBuf {
        self.read().config_dir.clone()
    }
}

impl From<CrosstalkConfig> for SharedConfig {
    fn from(config: CrosstalkConfig) -> Self {
        Self::new(config)
    }
}

/// The host's settings form: every option the UI edits in `crosstalk.toml`.
///
/// These live in the host file, so they need a schema of their own; without one the UI has
/// nothing to render and the CSV preamble cannot report them under the names operators know.
pub fn host_schema() -> crate::subapp::SettingsSchema {
    crate::subapp::SettingsSchema::new([shared_fields(), matching_fields()].concat())
}

/// The shared half of [`host_schema`]: scope, export retention and the AI policy.
///
/// The CSV preamble prints these with every app's report, because they describe the run as
/// much as the app's own thresholds do.
pub fn shared_schema() -> crate::subapp::SettingsSchema {
    crate::subapp::SettingsSchema::new(shared_fields())
}

/// `[scope]`, `[exports]` and `[ai.verify]`, as form fields.
fn shared_fields() -> Vec<crate::subapp::FieldSpec> {
    use crate::subapp::{FieldKind, FieldSpec, FieldValue};

    vec![
        FieldSpec::section(
            "_section_scope",
            "Scope",
            "Which channels a run may touch. Every sub-app asks the same question, so the \
             answer lives here once: profiles first, then the group filter. An app's own \
             source filter (the guide's EPG sources, for example) stays in that app's file.",
        ),
        FieldSpec::new(
            "scope.profiles",
            "_section_scope",
            "Channel Profile Names",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. All, Favorites")
        .with_help(
            "Comma-separated profile names. Empty means every profile for a scope filter, but \
             the guide's 'Clear EPG from Hidden Channels' refuses to run without a name, \
             because 'no profile' cannot mean 'the disabled channels of every profile'. It \
             uses every name given, unions their disabled channels, and matches names \
             exactly.",
        ),
        FieldSpec::new(
            "scope.groups",
            "_section_scope",
            "Channel Groups",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. DTV: All, DISH: All")
        .with_help(
            "Only process channels in these groups. Empty includes every group. Supports * \
             and ? wildcards, case-insensitively. Cannot be combined with Ignore Groups.",
        ),
        FieldSpec::new(
            "scope.ignore_groups",
            "_section_scope",
            "Ignore Groups",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. PPV Live Events, 24/7 Streams")
        .with_help(
            "Exclude channels in these groups, after the Channel Groups filter. Supports * \
             and ? wildcards, case-insensitively.",
        ),
        FieldSpec::section(
            "_section_exports",
            "CSV Exports",
            "Age-based tidying of the CSV files runs write into the shared exports \
             directory. Off unless a number of days is set, and the file a run has just \
             written is always kept.",
        ),
        FieldSpec::new(
            "exports.retention_days",
            "_section_exports",
            "Delete CSV Exports Older Than (Days)",
            FieldKind::Number,
            FieldValue::Number(0.0),
        )
        .with_help(
            "0 keeps every file, which is the default. Only files named <app>_*.csv are \
             touched, because the exports directory is shared with other apps.",
        ),
        FieldSpec::section(
            "_section_ai",
            "AI Verification",
            "The heuristics decide first and the model only promotes: it is asked about \
             results in the uncertain band below, or about candidates for a channel the \
             heuristics could not match at all. A heuristic result at or above an app's own \
             confidence threshold is never re-checked or vetoed by the model. An AI-accepted \
             pick is still validated for programme data: when every same-name copy is empty \
             the channel is left unmatched with a reason saying so, and there is no fallback \
             to other candidates. A sub-app may still decide never to consult the model; the \
             guide app's heal never does. The endpoints and keys live under [ai.providers] \
             in this same file.",
        ),
        FieldSpec::new(
            "ai.verify.enabled",
            "_section_ai",
            "Enable AI Verification",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help("When off, matching is purely heuristic and no request is ever made."),
        FieldSpec::new(
            "ai.verify.provider",
            "_section_ai",
            "AI Provider",
            FieldKind::Text,
            FieldValue::Text("typesafe".to_string()),
        )
        .with_placeholder("typesafe")
        .with_help("Name of an [ai.providers.<name>] entry in crosstalk.toml."),
        FieldSpec::new(
            "ai.verify.model",
            "_section_ai",
            "AI Model",
            FieldKind::Text,
            FieldValue::Text("jev-latest".to_string()),
        )
        .with_placeholder("jev-latest")
        .with_help("Model name as the provider spells it. Leave at the defaults when the chain below is set: setting both is refused.")
        ,
        FieldSpec::new(
            "ai.verify.chain",
            "_section_ai",
            "AI Verify Chain (JSON)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder(r#"[{"provider": "typesafe", "model": "jev-latest"}, {"provider": "llm", "model": "kimi-k3"}]"#)
        .with_help(
            "Escalation chain as a JSON list of {provider, model} links, asked in order: a \
             link that cannot decide hands the same question to the next. Empty means the \
             provider/model pair above, as one link. Each provider must name an \
             [ai.providers.<name>] entry in this file. The CROSSTALK_AI__VERIFY__CHAIN \
             environment override takes the same JSON."
        ),
        FieldSpec::new(
            "ai.verify.verify_band_low",
            "_section_ai",
            "Verify Band Low",
            FieldKind::Number,
            FieldValue::Number(80.0),
        )
        .with_help(
            "A heuristic score in this band, both ends inclusive, is uncertain enough to ask \
             the model about.",
        ),
        FieldSpec::new(
            "ai.verify.verify_band_high",
            "_section_ai",
            "Verify Band High",
            FieldKind::Number,
            FieldValue::Number(94.0),
        )
        .with_help(
            "Scores above this are not offered to the model. Scores at or above the app's \
             own confidence threshold are never offered either, whatever the band says: the \
             model promotes inside the band but never re-checks or vetoes a heuristic win.",
        ),
        FieldSpec::new(
            "ai.verify.rescue",
            "_section_ai",
            "Rescue Unmatched Channels",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Offer the model a shortlist when no heuristic result reached the band, but some \
             candidate scored at or above the rescue floor.",
        ),
        FieldSpec::new(
            "ai.verify.rescue_floor",
            "_section_ai",
            "Rescue Floor",
            FieldKind::Number,
            FieldValue::Number(60.0),
        )
        .with_help("Lowest heuristic score worth showing the model during a rescue."),
        FieldSpec::new(
            "ai.verify.shortlist",
            "_section_ai",
            "Shortlist Size",
            FieldKind::Number,
            FieldValue::Number(5.0),
        )
        .with_help("How many candidates go into one request. More candidates cost tokens."),
        FieldSpec::new(
            "ai.verify.accept_threshold",
            "_section_ai",
            "AI Accept Threshold",
            FieldKind::Number,
            FieldValue::Number(0.8),
        )
        .with_help(
            "0-1. A candidate is accepted when the model's yes-probability reaches this and \
             its choice agrees. An accepted pick is still validated for programme data: \
             when every same-name copy is empty the channel is left unmatched and the \
             reason says so.",
        ),
        FieldSpec::new(
            "ai.verify.reject_threshold",
            "_section_ai",
            "AI Reject Threshold",
            FieldKind::Number,
            FieldValue::Number(0.3),
        )
        .with_help("0-1. At or below this the candidate is dropped outright."),
        FieldSpec::new(
            "ai.verify.min_confidence",
            "_section_ai",
            "AI Minimum Confidence",
            FieldKind::Number,
            FieldValue::Number(0.5),
        )
        .with_help(
            "0-1. Below this the model's pick is treated as undecided and the channel is left \
             unmatched.",
        ),
    ]
}

/// `[matching]`, as form fields.
fn matching_fields() -> Vec<crate::subapp::FieldSpec> {
    use crate::subapp::{FieldKind, FieldSpec, FieldValue};

    vec![
        FieldSpec::section(
            "_section_threshold",
            "Decision Threshold",
            "The ONE cut every app matches at (guide assigns, streams attaches, lineup \
             names). A sensitivity preset — strict/high/medium/low (95/90/80/70) — or a \
             bare number. Guide's own per-action thresholds are retired in favour of this.",
        ),
        FieldSpec::new(
            "matching.sensitivity",
            "_section_threshold",
            "Matching Sensitivity",
            FieldKind::Text,
            FieldValue::Text("strict".to_string()),
        )
        .with_placeholder("strict")
        .with_help(
            "strict/high/medium/low (95/90/80/70) or a 0-100 number. Higher is stricter: \
             candidates below the cut never match, whatever the AI says.",
        ),
        FieldSpec::section(
            "_section_normalization",
            "Normalization Toggles",
            "Which kinds of tag are stripped from channel and guide names before they are \
             compared. This affects matching only: channel names in Dispatcharr are never \
             rewritten by these settings.",
        ),
        FieldSpec::new(
            "matching.ignore_quality_tags",
            "_section_normalization",
            "Ignore Quality Tags",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Strip [HD], [4K], [UHD], [SD], (Backup) and similar before comparing."),
        FieldSpec::new(
            "matching.ignore_regional_tags",
            "_section_normalization",
            "Ignore Regional Tags",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Strip East/West/Pacific/Central/Mountain/Atlantic. The regional differentiation \
             filter still runs when a name explicitly carries a regional marker.",
        ),
        FieldSpec::new(
            "matching.ignore_geographic_tags",
            "_section_normalization",
            "Ignore Geographic Prefixes",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Strip country-code prefixes such as 'US:' and '[CA]'."),
        FieldSpec::new(
            "matching.ignore_misc_tags",
            "_section_normalization",
            "Ignore Miscellaneous Tags",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Strip single-letter tags like (A), and other parenthesised noise."),
        FieldSpec::section(
            "_section_databases",
            "Channel Databases",
            "Which shipped reference lists the matcher may draw names from. These are not \
             your Dispatcharr channels. Enabling a country you do not carry costs accuracy \
             rather than gaining coverage, because it adds names to match against.",
        ),
        FieldSpec::new(
            "matching.enabled_databases",
            "_section_databases",
            "Enabled Databases",
            FieldKind::Text,
            FieldValue::Text("US".to_string()),
        )
        .with_placeholder("US, UK, CA")
        .with_help("Comma-separated country codes. Empty uses the default set."),
        FieldSpec::section(
            "_section_aliases",
            "Custom Aliases",
            "Alternate names for channels whose names differ between your provider and the \
             guide. A key REPLACES the built-in list for that name rather than adding to it, \
             so include the built-in variants you still want.",
        ),
        FieldSpec::new(
            "matching.custom_aliases",
            "_section_aliases",
            "Custom Channel Aliases (JSON)",
            FieldKind::JsonObject,
            FieldValue::Json(std::collections::BTreeMap::new()),
        )
        .with_placeholder("{\"FOX News Channel\": [\"FOX NEWS HD\", \"FoxNews\"]}"),
    ]
}

/// The chain as the settings form holds it: empty when unset (so the preamble prints
/// "(not set)"), otherwise the compact JSON the form parses back verbatim.
fn chain_form_value(chain: &[AiChainEntry]) -> String {
    if chain.is_empty() {
        String::new()
    } else {
        serde_json::to_string(chain).unwrap_or_default()
    }
}

/// Parse the chain back from the settings form. Blank clears it to the shorthand; anything
/// else must be the JSON list the form value was rendered from.
fn parse_chain_form_value(text: &str) -> Result<Vec<AiChainEntry>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(trimmed).with_context(|| {
        "ai.verify.chain must be a JSON list like ".to_string()
            + r#"[{"provider": "typesafe", "model": "jev-latest"}]"#
    })
}

/// The host settings as form values, for the settings page and the CSV preamble.
pub fn host_values(config: &CrosstalkConfig) -> crate::subapp::SettingsValues {
    let mut values = shared_values(config);
    values.insert(
        "matching.ignore_quality_tags",
        crate::subapp::FieldValue::Boolean(config.matching.ignore_quality_tags),
    );
    values.insert(
        "matching.ignore_regional_tags",
        crate::subapp::FieldValue::Boolean(config.matching.ignore_regional_tags),
    );
    values.insert(
        "matching.ignore_geographic_tags",
        crate::subapp::FieldValue::Boolean(config.matching.ignore_geographic_tags),
    );
    values.insert(
        "matching.ignore_misc_tags",
        crate::subapp::FieldValue::Boolean(config.matching.ignore_misc_tags),
    );
    values.insert(
        "matching.sensitivity",
        crate::subapp::FieldValue::Text(config.matching.sensitivity.clone()),
    );
    values.insert(
        "matching.enabled_databases",
        crate::subapp::FieldValue::Text(config.matching.databases().join(", ")),
    );
    values.insert(
        "matching.custom_aliases",
        crate::subapp::FieldValue::Json(config.matching.custom_aliases.clone()),
    );
    values
}

/// The shared settings as form values: the ones every app's report prints.
pub fn shared_values(config: &CrosstalkConfig) -> crate::subapp::SettingsValues {
    use crate::subapp::{FieldValue, SettingsValues};

    let mut values = SettingsValues::new();
    values.insert(
        "scope.profiles",
        FieldValue::Text(config.scope.profiles.clone()),
    );
    values.insert(
        "scope.groups",
        FieldValue::Text(config.scope.groups.clone()),
    );
    values.insert(
        "scope.ignore_groups",
        FieldValue::Text(config.scope.ignore_groups.clone()),
    );
    values.insert(
        "exports.retention_days",
        FieldValue::Number(f64::from(config.exports.retention_days)),
    );

    let verify = &config.ai.verify;
    values.insert("ai.verify.enabled", FieldValue::Boolean(verify.enabled));
    values.insert(
        "ai.verify.provider",
        FieldValue::Text(verify.provider.clone()),
    );
    values.insert("ai.verify.model", FieldValue::Text(verify.model.clone()));
    values.insert(
        "ai.verify.chain",
        FieldValue::Text(chain_form_value(&verify.chain)),
    );
    values.insert(
        "ai.verify.verify_band_low",
        FieldValue::Number(f64::from(verify.verify_band_low)),
    );
    values.insert(
        "ai.verify.verify_band_high",
        FieldValue::Number(f64::from(verify.verify_band_high)),
    );
    values.insert("ai.verify.rescue", FieldValue::Boolean(verify.rescue));
    values.insert(
        "ai.verify.rescue_floor",
        FieldValue::Number(f64::from(verify.rescue_floor)),
    );
    values.insert(
        "ai.verify.shortlist",
        FieldValue::Number(verify.shortlist as f64),
    );
    values.insert(
        "ai.verify.accept_threshold",
        FieldValue::Number(verify.accept_threshold),
    );
    values.insert(
        "ai.verify.reject_threshold",
        FieldValue::Number(verify.reject_threshold),
    );
    values.insert(
        "ai.verify.min_confidence",
        FieldValue::Number(verify.min_confidence),
    );
    values
}

/// Host form values -> `config`, in place.
///
/// Fields the form did not submit are left alone, so a partial POST (a test, a script) cannot
/// silently reset the values it never mentioned. The bounds are the ones the form's own
/// number inputs enforced when these fields lived in `guide.toml`.
pub fn apply_host_values(
    config: &mut CrosstalkConfig,
    values: &crate::subapp::SettingsValues,
) -> Result<()> {
    use crate::subapp::{clamp_u8, clamp_unit, whole_number};

    if let Some(text) = values.text("scope.profiles") {
        config.scope.profiles = text.to_string();
    }
    if let Some(text) = values.text("scope.groups") {
        config.scope.groups = text.to_string();
    }
    if let Some(text) = values.text("scope.ignore_groups") {
        config.scope.ignore_groups = text.to_string();
    }
    if let Some(number) = values.number("exports.retention_days") {
        config.exports.retention_days = whole_number("exports.retention_days", number, 0, 3_650)?;
    }

    if let Some(value) = values.bool("ai.verify.enabled") {
        config.ai.verify.enabled = value;
    }
    if let Some(value) = values.bool("ai.verify.rescue") {
        config.ai.verify.rescue = value;
    }
    if let Some(text) = values.text("ai.verify.provider") {
        config.ai.verify.provider = text.to_string();
    }
    if let Some(text) = values.text("ai.verify.model") {
        config.ai.verify.model = text.to_string();
    }
    if let Some(text) = values.text("ai.verify.chain") {
        config.ai.verify.chain = parse_chain_form_value(text)?;
    }
    if let Some(number) = values.number("ai.verify.verify_band_low") {
        config.ai.verify.verify_band_low = clamp_u8(number)?;
    }
    if let Some(number) = values.number("ai.verify.verify_band_high") {
        config.ai.verify.verify_band_high = clamp_u8(number)?;
    }
    if let Some(number) = values.number("ai.verify.rescue_floor") {
        config.ai.verify.rescue_floor = clamp_u8(number)?;
    }
    if let Some(number) = values.number("ai.verify.shortlist") {
        config.ai.verify.shortlist = whole_number("ai.verify.shortlist", number, 1, 255)? as usize;
    }
    if let Some(number) = values.number("ai.verify.accept_threshold") {
        config.ai.verify.accept_threshold = clamp_unit(number)?;
    }
    if let Some(number) = values.number("ai.verify.reject_threshold") {
        config.ai.verify.reject_threshold = clamp_unit(number)?;
    }
    if let Some(number) = values.number("ai.verify.min_confidence") {
        config.ai.verify.min_confidence = clamp_unit(number)?;
    }

    if let Some(text) = values.text("matching.sensitivity") {
        config.matching.sensitivity = text.to_string();
    }
    if let Some(value) = values.bool("matching.ignore_quality_tags") {
        config.matching.ignore_quality_tags = value;
    }
    if let Some(value) = values.bool("matching.ignore_regional_tags") {
        config.matching.ignore_regional_tags = value;
    }
    if let Some(value) = values.bool("matching.ignore_geographic_tags") {
        config.matching.ignore_geographic_tags = value;
    }
    if let Some(value) = values.bool("matching.ignore_misc_tags") {
        config.matching.ignore_misc_tags = value;
    }
    if let Some(text) = values.text("matching.enabled_databases") {
        config.matching.enabled_databases = text
            .split([',', '\n'])
            .map(str::trim)
            .filter(|code| !code.is_empty())
            .map(str::to_string)
            .collect();
    }
    if let Some(map) = values.json("matching.custom_aliases") {
        config.matching.custom_aliases = map.clone();
    }
    Ok(())
}

/// Result of [`CrosstalkConfig::validate`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConfigReport {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ConfigReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Serialise `value` into `path`, keeping any comment and ordering already in the file.
///
/// `toml_edit` is used rather than `toml::to_string` because an operator's comments are
/// the only explanation of why a value is what it is, and a UI save must not erase them.
pub fn save_with_comments<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    // Serialise with the `toml` crate, which emits real `[section]` headers, then re-parse
    // so `toml_edit` can merge into the file that is already there. Going through
    // `toml_edit::ser` directly produces inline tables, which are valid TOML but rewrite an
    // operator's whole file onto a handful of lines.
    let rendered =
        toml::to_string_pretty(value).with_context(|| format!("serialising {}", path.display()))?;
    let table: toml_edit::DocumentMut = rendered
        .parse()
        .with_context(|| format!("re-parsing serialised config for {}", path.display()))?;

    let existing = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };

    // A file that does not parse is left alone rather than replaced: overwriting it would
    // destroy the comments and values the operator wrote, and they would not know why.
    let mut document: toml_edit::DocumentMut = if existing.trim().is_empty() {
        toml_edit::DocumentMut::new()
    } else {
        existing.parse().with_context(|| {
            format!(
                "{} is not valid TOML; refusing to overwrite it. Fix the file, then save again",
                path.display()
            )
        })?
    };

    merge_document(&mut document, table);

    // Write through a temporary file in the same directory, so an interrupted write cannot
    // leave a half-written configuration behind. The temporary file is created 0600: these
    // files can inline the UI token and the Dispatcharr admin key, so the pre-rename
    // contents must never be group- or world-readable. The name carries the process id
    // plus a per-save counter: pid alone is shared by every thread of this process, so
    // two concurrent saves from the same daemon would otherwise truncate each other.
    // Creation uses `create_new`: with a pid+counter name a collision means something is
    // genuinely wrong (a stale temp, a recycled pid), and silently truncating it would
    // destroy evidence. On collision the save retries once with the next counter value.
    static SAVE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(directory)
        .with_context(|| format!("creating {}", directory.display()))?;
    let temp_name = |counter: u64| {
        directory.join(format!(
            ".{}.tmp.{}.{}",
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| "config".to_string()),
            std::process::id(),
            counter,
        ))
    };
    let mut temporary = temp_name(SAVE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    // `create_new` fails when the name already exists rather than truncating it: with a
    // pid+counter name a collision means something is genuinely wrong, and silently
    // reusing the file would destroy evidence. Retry once with the next counter value.
    let mut file = match open_temp(&temporary) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            temporary = temp_name(SAVE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
            open_temp(&temporary).with_context(|| format!("writing {}", temporary.display()))?
        }
        Err(error) => {
            return Err(error).with_context(|| format!("writing {}", temporary.display()));
        }
    };
    // Every failure from here on removes the temp file: these files can inline the UI
    // token and the Dispatcharr key, so a half-written temp must never be left behind.
    let text = document.to_string();
    if let Err(error) = write_temp(&mut file, &temporary, &text) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    drop(file);
    if let Err(error) = std::fs::rename(&temporary, path)
        .with_context(|| format!("renaming {} to {}", temporary.display(), path.display()))
    {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    // Best effort: fsync the directory so the rename itself survives a crash.
    if let Ok(dir) = std::fs::File::open(directory) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Create the save's temporary file, failing when the name is already taken.
fn open_temp(temporary: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(temporary)?;
    // `mode` only applies at creation, and `create_new` always creates, so the file is
    // 0600 from birth; the explicit chmod below is belt and braces for platforms where
    // the option is ignored.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

/// Write the serialised document and fsync it, so a crash cannot lose a reported save.
fn write_temp(file: &mut std::fs::File, temporary: &Path, text: &str) -> Result<()> {
    use std::io::Write;
    file.write_all(text.as_bytes())
        .with_context(|| format!("writing {}", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing {}", temporary.display()))?;
    Ok(())
}

/// Copy every value from `source` into `target`, keeping `target`'s comments and key order.
///
/// Keys are matched by path, so a comment above `[server] port = 8730` survives a change
/// to the port.
fn merge_document(target: &mut toml_edit::DocumentMut, source: toml_edit::DocumentMut) {
    // `prune = false` at the root: unknown top-level keys are rejected at load
    // (`deny_unknown_fields`), so by the time a save merges, a key the structs do not
    // know about could only have appeared in the file between load and save. Deleting
    // it here would destroy something the operator just wrote, so the root keeps what
    // it does not recognise. Nested tables are pruned, because those are the ones the
    // form can actually edit.
    merge_table(target.as_table_mut(), source.as_table(), false);
}

fn merge_table(target: &mut toml_edit::Table, source: &toml_edit::Table, prune: bool) {
    // Inside a table the source describes, the source is authoritative: a key that is no
    // longer in it was deleted in memory, and leaving it behind would make a removal
    // impossible from the UI.
    if prune {
        let stale: Vec<String> = target
            .iter()
            .filter(|(key, _)| !source.contains_key(key))
            .map(|(key, _)| key.to_string())
            .collect();
        for key in stale {
            target.remove(&key);
        }
    }

    for (key, source_item) in source {
        match (target.get_mut(key), source_item) {
            (Some(toml_edit::Item::Table(target_table)), toml_edit::Item::Table(source_table)) => {
                merge_table(target_table, source_table, true);
            }
            (Some(target_item), source_item) => {
                let decoration = target_item.as_value().map(|value| value.decor().clone());
                let mut replacement = source_item.clone();
                if let (Some(decoration), Some(value)) = (decoration, replacement.as_value_mut()) {
                    *value.decor_mut() = decoration;
                }
                *target_item = replacement;
            }
            (None, _) => {
                target.insert(key, source_item.clone());
            }
        }
    }
}

/// Deep-merge an environment overlay into a TOML table, for [`CrosstalkConfig`].
///
/// Tables merge key by key so `CROSSTALK_SERVER__PORT` changes one value; anything else
/// (including arrays) replaces, which is how figment's own merge treats them.
fn merge_toml_overlay(base: &mut toml::Table, overlay: toml::Table) {
    for (key, value) in overlay {
        match (base.get_mut(&key), value) {
            (Some(toml::Value::Table(existing)), toml::Value::Table(patch)) => {
                merge_toml_overlay(existing, patch);
            }
            (Some(existing), patch) => {
                *existing = patch;
            }
            (None, patch) => {
                base.insert(key, patch);
            }
        }
    }
}

/// A secret from the file, or from the environment variable it names.
///
/// The environment wins, so a token can be rotated without touching the file. Empty values
/// count as unset: a blank `api_key` line should not look like a configured key.
fn resolve_secret(inline: &str, env_name: &str) -> Option<String> {
    let inline = inline.trim();
    if !inline.is_empty() {
        return Some(inline.to_string());
    }
    let env_name = env_name.trim();
    if env_name.is_empty() {
        return None;
    }
    let value = std::env::var(env_name).ok()?;
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subapp::{FieldValue, SettingsValues};

    fn write(dir: &Path, name: &str, text: &str) {
        std::fs::write(dir.join(name), text).expect("write");
    }

    #[test]
    fn missing_file_yields_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        assert_eq!(config.server.port, 8730);
        assert!(config.server.is_loopback());
        assert!(config.matching.ignore_quality_tags);
    }

    #[test]
    fn file_values_are_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[server]
bind = "0.0.0.0"
port = 9000
token = "secret"

[dispatcharr]
base_url = "http://dispatcharr:9191"
api_key = "abc123"

[matching]
ignore_quality_tags = false
enabled_databases = ["US", "UK"]

[matching.custom_aliases]
"Fox News Channel" = ["FNC"]

[schedules]
"guide.match_apply" = "0 4 * * *"
"#,
        );
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        assert_eq!(config.server.port, 9000);
        assert!(!config.server.is_loopback());
        assert_eq!(
            config.dispatcharr.api_url_root(),
            "http://dispatcharr:9191/api"
        );
        assert_eq!(
            config.dispatcharr.resolved_api_key().as_deref(),
            Some("abc123")
        );
        assert!(!config.matching.ignore_quality_tags);
        // Sorted, like the plugin's own `enabled.sort()`.
        assert_eq!(config.matching.databases(), vec!["UK", "US"]);
        assert_eq!(
            config.matching.custom_aliases.get("Fox News Channel"),
            Some(&vec!["FNC".to_string()])
        );
        assert_eq!(
            config
                .schedules
                .get("guide.match_apply")
                .map(ScheduleEntry::cron_expression),
            Some("0 4 * * *")
        );
    }

    impl DispatcharrConfig {
        fn api_url_root(&self) -> String {
            self.api_root()
        }
    }

    #[test]
    fn environment_overrides_win() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "crosstalk.toml", "[server]\nport = 9000\n");
        // A unique prefix keeps this out of every other test's way: environment variables
        // are process-global and the suite runs in parallel.
        std::env::set_var("CROSSTALK_TESTENV_SERVER__PORT", "9123");
        let config =
            CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTENV_").expect("load");
        std::env::remove_var("CROSSTALK_TESTENV_SERVER__PORT");
        assert_eq!(config.server.port, 9123);
    }

    #[test]
    fn secrets_resolve_from_the_environment() {
        let provider = AiProviderConfig {
            api_key: String::new(),
            api_key_env: "CROSSTALK_TEST_AI_KEY".to_string(),
            ..AiProviderConfig::default()
        };
        assert!(provider.resolved_api_key().is_none());
        std::env::set_var("CROSSTALK_TEST_AI_KEY", "sk-test");
        assert_eq!(provider.resolved_api_key().as_deref(), Some("sk-test"));
        std::env::remove_var("CROSSTALK_TEST_AI_KEY");

        // A blank inline value is unset, not an empty key.
        let blank = AiProviderConfig {
            api_key: "   ".to_string(),
            api_key_env: String::new(),
            ..AiProviderConfig::default()
        };
        assert!(blank.resolved_api_key().is_none());
    }

    #[test]
    fn api_root_is_idempotent() {
        let config = DispatcharrConfig {
            base_url: "http://host:9191/api/".to_string(),
            ..DispatcharrConfig::default()
        };
        assert_eq!(config.api_root(), "http://host:9191/api");
    }

    #[test]
    fn validate_reports_missing_credentials_and_open_binds() {
        let mut config = CrosstalkConfig::default();
        let report = config.validate();
        assert!(!report.is_ok(), "defaults have no credentials");
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("credentials")));
        assert!(
            report.warnings.is_empty(),
            "loopback bind is fine without a token"
        );

        config.dispatcharr.api_key = "abc".to_string();
        config.server.bind = "0.0.0.0".to_string();
        let report = config.validate();
        assert!(report.is_ok(), "{:?}", report.errors);
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("no token"));

        config.server.token = "t".to_string();
        assert!(config.validate().warnings.is_empty());
    }

    #[test]
    fn a_reachable_daemon_needs_a_long_token() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "abc".to_string();
        config.server.token_env = String::new();
        config.server.token = "short".to_string();
        // Loopback with no proxy: only this host can guess, so any token will do.
        assert!(config.server.security_errors().is_empty());

        config.server.bind = "0.0.0.0".to_string();
        let errors = config.validate().errors;
        assert!(
            errors.iter().any(|error| error.contains("at least 32")),
            "{errors:?}"
        );

        // Behind a proxy on loopback the token is just as exposed.
        config.server.bind = "127.0.0.1".to_string();
        config.server.trusted_proxies = vec!["127.0.0.1".to_string()];
        assert!(!config.server.security_errors().is_empty());

        config.server.token = "x".repeat(MIN_TOKEN_BYTES);
        assert!(config.server.security_errors().is_empty());
    }

    #[test]
    fn trusted_proxies_need_a_token_and_valid_ranges() {
        let mut config = CrosstalkConfig::default();
        config.server.token_env = String::new();
        config.server.trusted_proxies = vec!["10.0.0.0/8".to_string()];
        let errors = config.server.security_errors();
        assert!(
            errors.iter().any(|error| error.contains("no token")),
            "{errors:?}"
        );

        config.server.token = "y".repeat(MIN_TOKEN_BYTES);
        config.server.trusted_proxies = vec!["10.0.0.0/8".to_string(), "proxy".to_string()];
        let errors = config.server.security_errors();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("server.trusted_proxies"), "{errors:?}");
        assert!(errors[0].contains("proxy"), "{errors:?}");
        assert_eq!(config.server.trusted_cidrs().len(), 1);

        config.server.trusted_proxies = vec!["10.0.0.0/8".to_string(), "fd00::/8".to_string()];
        assert!(config.server.security_errors().is_empty());

        config.server.public_url = "crosstalk.example.com".to_string();
        assert!(!config.server.security_errors().is_empty());
        config.server.public_url = "https://crosstalk.example.com/".to_string();
        assert!(config.server.security_errors().is_empty());
        assert_eq!(
            config.server.public_origin().as_deref(),
            Some("https://crosstalk.example.com:443")
        );
    }

    #[test]
    fn the_server_table_reads_the_proxy_settings() {
        let config: CrosstalkConfig = toml::from_str(
            "[server]\nbind = \"127.0.0.1\"\ntrusted_proxies = [\"172.16.0.0/12\"]\n\
             public_url = \"https://ui.example\"\nsecure_cookies = true\nhsts = true\n",
        )
        .expect("parse");
        assert_eq!(config.server.trusted_proxies, vec!["172.16.0.0/12"]);
        assert!(config.server.secure_cookies);
        assert!(config.server.hsts);
        assert_eq!(config.server.public_url, "https://ui.example");
    }

    #[test]
    fn validate_warns_on_plaintext_http_to_a_remote_host() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "abc".to_string();

        // Loopback over http is the default and stays quiet.
        for quiet in [
            "http://localhost:9191",
            "http://127.0.0.1:9191",
            "http://127.0.0.2:9191",
            "http://[::1]:9191",
            "https://dispatcharr.lan:9191",
        ] {
            config.dispatcharr.base_url = quiet.to_string();
            let report = config.validate();
            assert!(
                report.warnings.iter().all(|w| !w.contains("cleartext")),
                "{quiet}: {:?}",
                report.warnings
            );
        }

        // Plaintext off-host warns, naming the host.
        for loud in [
            "http://dispatcharr:9191",
            "http://192.168.1.5:9191/",
            "http://dispatcharr.lan:9191/api",
        ] {
            config.dispatcharr.base_url = loud.to_string();
            let report = config.validate();
            assert!(
                report.warnings.iter().any(|w| w.contains("cleartext")),
                "{loud}: {:?}",
                report.warnings
            );
        }
        config.dispatcharr.base_url = "http://192.168.1.5:9191".to_string();
        let report = config.validate();
        assert!(
            report.warnings.iter().any(|w| w.contains("192.168.1.5")),
            "{:?}",
            report.warnings
        );
    }

    #[test]
    fn validate_warns_on_plaintext_ai_endpoints_off_host() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "abc".to_string();
        config.ai.providers.insert(
            "remote".to_string(),
            AiProviderConfig {
                kind: "openai_compat".to_string(),
                base_url: "http://gpu-box:8000/v1".to_string(),
                ..AiProviderConfig::default()
            },
        );
        let report = config.validate();
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.contains("ai.providers.remote")),
            "{:?}",
            report.warnings
        );

        // A loopback AI endpoint stays quiet.
        config
            .ai
            .providers
            .get_mut("remote")
            .expect("provider")
            .base_url = "http://127.0.0.1:11434/v1".to_string();
        let report = config.validate();
        assert!(
            report
                .warnings
                .iter()
                .all(|w| !w.contains("ai.providers.remote")),
            "{:?}",
            report.warnings
        );
    }

    #[test]
    fn url_hosts_parse_with_ports_brackets_and_userinfo() {
        assert_eq!(url_host("http://dispatcharr:9191"), "dispatcharr");
        assert_eq!(url_host("http://dispatcharr:9191/api"), "dispatcharr");
        assert_eq!(url_host("http://[::1]:9191"), "::1");
        assert_eq!(url_host("http://user:pass@lan:9191"), "lan");
        assert_eq!(url_host("https://AI.EXAMPLE:443/v1"), "ai.example");
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("dispatcharr"));
        assert!(!is_loopback_host(""));
    }

    #[test]
    fn validate_catches_bad_provider_kind_and_cron() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "abc".to_string();
        config.ai.providers.insert(
            "bad".to_string(),
            AiProviderConfig {
                kind: "openai".to_string(),
                base_url: "https://example.invalid".to_string(),
                api_key: "k".to_string(),
                ..AiProviderConfig::default()
            },
        );
        config
            .schedules
            .insert("guide".to_string(), ScheduleEntry::cron("every day"));
        let report = config.validate();
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("typesafe or openai_compat")));
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("<app>.<action>")));
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("cron expression")));
    }

    #[test]
    fn database_fallback_matches_the_plugin() {
        let config = MatchingConfig::default();
        // Twelve databases ship, so the plugin's rule picks the US one.
        assert_eq!(config.databases(), vec!["US".to_string()]);
        let config = MatchingConfig {
            enabled_databases: vec![
                "uk".to_string(),
                " us ".to_string(),
                "".to_string(),
                "ZZ".to_string(),
                "UK".to_string(),
            ],
            ..MatchingConfig::default()
        };
        // Uppercased, restricted to shipped codes, deduplicated, sorted.
        assert_eq!(config.databases(), vec!["UK".to_string(), "US".to_string()]);
    }

    #[test]
    fn an_effective_config_refuses_to_be_saved() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[dispatcharr]\napi_key = \"\"\n",
        );
        std::env::set_var("CROSSTALK_TESTSAVE_DISPATCHARR__API_KEY", "from-env");
        let effective =
            CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTSAVE_").expect("load");
        std::env::remove_var("CROSSTALK_TESTSAVE_DISPATCHARR__API_KEY");
        assert_eq!(effective.env_overrides, 1);
        assert_eq!(
            effective.dispatcharr.resolved_api_key().as_deref(),
            Some("from-env")
        );

        // Writing it back would put the environment's secret in the file.
        let error = effective
            .save(dir.path().join("crosstalk.toml"))
            .expect_err("refuses");
        assert!(format!("{error}").contains("environment overrides"));
        let text = std::fs::read_to_string(dir.path().join("crosstalk.toml")).expect("read");
        assert!(!text.contains("from-env"), "{text}");
    }

    #[test]
    fn load_file_ignores_the_environment() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[dispatcharr]\napi_key = \"file-key\"\n",
        );
        std::env::set_var("CROSSTALK_FILEONLY_DISPATCHARR__API_KEY", "env-key");
        let file_only =
            CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_FILEONLY_").expect("load");
        std::env::remove_var("CROSSTALK_FILEONLY_DISPATCHARR__API_KEY");
        assert_eq!(file_only.env_overrides, 1);

        let plain = CrosstalkConfig::load_file(dir.path()).expect("load_file");
        assert_eq!(plain.env_overrides, 0);
        assert_eq!(
            plain.dispatcharr.resolved_api_key().as_deref(),
            Some("file-key")
        );
        // And it saves.
        plain
            .save(dir.path().join("crosstalk.toml"))
            .expect("saves");
    }

    #[test]
    fn saving_removes_entries_deleted_in_memory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[ai.providers.typesafe]
kind = "typesafe"
base_url = "https://api.typesafe.ai/v1"

[matching.custom_aliases]
"ESPN" = ["ESPN US"]

[schedules]
"guide.match_apply" = "0 4 * * *"
"#,
        );

        let mut config = CrosstalkConfig::load_file(dir.path()).expect("load");
        config.ai.providers.clear();
        config.matching.custom_aliases.clear();
        config.schedules.clear();
        config.save(&path).expect("save");

        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("reload");
        assert!(reloaded.ai.providers.is_empty());
        assert!(reloaded.matching.custom_aliases.is_empty());
        assert!(reloaded.schedules.is_empty());
    }

    #[test]
    fn a_schedule_entry_is_a_cron_string_or_a_table_with_a_window() {
        // Two shapes, one key space: the short form stays what it was, so every existing
        // crosstalk.toml keeps working.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[schedules]
"guide.match_apply" = "0 4 * * *"
"streams.probe" = { cron = "0 23 * * *", window = "4h" }
"streams.probe_early" = { cron = "0 1 * * *", until = "06:00" }
"streams.probe_ny" = { cron = "0 1 * * *", until = "06:00", timezone = "America/New_York" }
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");

        let plain = &config.schedules["guide.match_apply"];
        assert_eq!(plain.cron_expression(), "0 4 * * *");
        assert!(!plain.is_windowed());
        assert_eq!(plain.window().expect("window"), None);

        let probe = &config.schedules["streams.probe"];
        assert!(probe.is_windowed());
        assert_eq!(probe.cron_expression(), "0 23 * * *");
        let window = probe.window().expect("window").expect("a duration window");
        assert_eq!(
            window.describe(),
            "4h from start",
            "the dashboard and reports name the window in words"
        );

        let early = &config.schedules["streams.probe_early"];
        assert_eq!(
            early.window_description().as_deref(),
            Some("until 06:00 (UTC)"),
            "`until` defaults to UTC, like the cron expression"
        );
        let ny = &config.schedules["streams.probe_ny"];
        assert_eq!(
            ny.window_description().as_deref(),
            Some("until 06:00 (America/New_York)")
        );
        // The file has no Dispatcharr credentials, which is the only complaint: the schedule
        // entries themselves must all read.
        assert!(
            config
                .validate()
                .errors
                .iter()
                .all(|error| error.contains("credentials")),
            "{:?}",
            config.validate().errors
        );
    }

    #[test]
    fn a_bad_window_is_a_validation_error_that_names_the_entry() {
        // A window that does not parse would arm a run that never pauses: refuse it at load
        // time rather than let a "4" mean four seconds.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[schedules]
"streams.probe" = { cron = "0 23 * * *", window = "4" }
"streams.sort" = { cron = "0 23 * * *", window = "4h", until = "06:00" }
"streams.mirror" = { cron = "0 23 * * *", until = "06:00", timezone = "Mars/Olympus" }
"streams.visibility" = { cron = "not a cron", window = "4h" }
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        let errors = config.validate().errors;
        for (entry, expected) in [
            ("schedules.streams.probe", "duration"),
            ("schedules.streams.sort", "not both"),
            ("schedules.streams.mirror", "not an IANA time zone"),
            ("schedules.streams.visibility", "cron expression"),
        ] {
            assert!(
                errors
                    .iter()
                    .any(|error| error.starts_with(entry) && error.contains(expected)),
                "{entry} should report {expected:?}: {errors:?}"
            );
        }
    }

    #[test]
    fn a_windowed_schedule_survives_a_save_with_comments() {
        // The web UI's host save rewrites the file through `toml_edit`: a table-valued schedule
        // entry must come back as a table, or saving an unrelated setting would silently turn
        // every windowed run into an unbounded one.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"# operator note
[schedules]
"guide.match_apply" = "0 4 * * *"
"streams.probe" = { cron = "0 23 * * *", window = "4h" }
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        config.save(&path).expect("save");
        let text = std::fs::read_to_string(&path).expect("read back");
        assert!(text.contains("# operator note"), "comments survive: {text}");

        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("reload");
        assert_eq!(
            reloaded.schedules["streams.probe"]
                .window_description()
                .as_deref(),
            Some("4h from start")
        );
        assert_eq!(
            reloaded.schedules["guide.match_apply"].cron_expression(),
            "0 4 * * *"
        );
    }

    #[test]
    fn a_misspelled_key_inside_a_schedule_table_names_the_field() {
        // `untagged` would say "data did not match any variant", which tells an operator
        // nothing about the typo they made.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[schedules]\n\"streams.probe\" = { cron = \"0 1 * * *\", windoww = \"4h\" }\n",
        );
        let error = CrosstalkConfig::load_file(dir.path()).expect_err("rejects");
        let text = format!("{error:#}");
        assert!(text.contains("windoww"), "{text}");
    }

    #[test]
    fn a_workflow_is_steps_and_an_optional_fresh_flag() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[workflows.nightly]
steps = ["streams.probe", "streams.sort_apply"]

[workflows.rebuild]
steps = ["streams.match_apply"]
fresh = true

[schedules]
"workflow.nightly" = { cron = "0 23 * * *", window = "4h" }
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        assert_eq!(
            config.workflows["nightly"].steps,
            vec!["streams.probe".into(), "streams.sort_apply".into()]
        );
        assert!(!config.workflows["nightly"].fresh, "off by default");
        assert!(config.workflows["rebuild"].fresh);
        // A workflow is scheduled like an action, under its own name space, and the schedule
        // entry names an existing workflow, so nothing here is an error.
        assert!(config.schedules.contains_key("workflow.nightly"));
        assert!(
            config
                .validate()
                .errors
                .iter()
                .all(|error| error.contains("credentials")),
            "{:?}",
            config.validate().errors
        );
    }

    #[test]
    fn workflow_steps_parse_as_strings_or_tables() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[workflows.full_sync]
steps = [
  "lineup.sync_apply",
  { action = "streams.match_apply", scope = "lineup" },
  { action = "guide.match_apply", scope = { groups = "DIRECTV: *" }, continue_on_failure = true },
]
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        let steps = &config.workflows["full_sync"].steps;
        assert_eq!(steps.len(), 3);
        // The old string form is unchanged.
        assert_eq!(steps[0], WorkflowStepEntry::from("lineup.sync_apply"));
        // Nothing about the steps is an error (credentials aside: the temp file has none).
        assert!(
            config
                .validate()
                .errors
                .iter()
                .filter(|error| !error.contains("credentials"))
                .all(|error| !error.contains("workflows.full_sync")),
            "no step-shape error: {:?}",
            config.validate().errors
        );
        match (&steps[1], &steps[2]) {
            (WorkflowStepEntry::Detailed(first), WorkflowStepEntry::Detailed(second)) => {
                assert_eq!(first.action, "streams.match_apply");
                assert_eq!(
                    first.scope,
                    Some(WorkflowStepScope::Named("lineup".to_string()))
                );
                assert!(!first.continue_on_failure);
                assert_eq!(second.action, "guide.match_apply");
                assert_eq!(
                    second.scope,
                    Some(WorkflowStepScope::Table(ScopeConfig {
                        groups: "DIRECTV: *".to_string(),
                        ..ScopeConfig::default()
                    }))
                );
                assert!(second.continue_on_failure);
            }
            other => panic!("tables parse as tables: {other:?}"),
        }
    }

    #[test]
    fn workflow_step_validation_names_the_bad_step() {
        let mut config = CrosstalkConfig::default();
        config.workflows.insert(
            "nightly".to_string(),
            WorkflowConfig {
                steps: vec![
                    "streams.probe".into(),
                    WorkflowStepEntry::Detailed(WorkflowStepDetail {
                        action: "guide.match_apply".to_string(),
                        scope: Some(WorkflowStepScope::Table(ScopeConfig {
                            groups: "A".to_string(),
                            ignore_groups: "B".to_string(),
                            ..ScopeConfig::default()
                        })),
                        continue_on_failure: false,
                    }),
                    WorkflowStepEntry::Detailed(WorkflowStepDetail {
                        action: "streams.sort_apply".to_string(),
                        scope: Some(WorkflowStepScope::Named(String::new())),
                        continue_on_failure: false,
                    }),
                ],
                fresh: false,
            },
        );
        let errors = config.validate().errors;
        assert!(
            errors.iter().any(|error| error
                .contains("workflows.nightly.steps[1]: scope.groups and scope.ignore_groups")),
            "{errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|error| error.contains("workflows.nightly.steps[2]: the scope name")),
            "{errors:?}"
        );
    }

    #[test]
    fn workflow_steps_keep_their_shape_through_a_save() {
        // A string stays a string and a table stays a table, with the operator's comment
        // intact: the save merges values, it does not re-render the file.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"# operator note
[workflows.full_sync]
steps = [
  "lineup.sync_apply",
  { action = "guide.match_apply", scope = { groups = "DIRECTV: *" }, continue_on_failure = true },
]
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        config.save(&path).expect("save");
        let text = std::fs::read_to_string(&path).expect("read back");
        assert!(text.contains("# operator note"), "comments survive: {text}");
        assert!(
            text.contains("\"lineup.sync_apply\""),
            "the string step stays a string: {text}"
        );
        assert!(
            text.contains("continue_on_failure = true"),
            "the table step stays a table: {text}"
        );
        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("reload");
        assert_eq!(
            reloaded.workflows["full_sync"].steps,
            config.workflows["full_sync"].steps
        );
    }

    #[test]
    fn workflow_steps_also_parse_as_an_array_of_tables() {
        // TOML forbids newlines inside an inline table, so a long step reads better as
        // `[[workflows.<name>.steps]]`. Every step is then a table; a save keeps them valid.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"[workflows.full_sync]
fresh = false

[[workflows.full_sync.steps]]
action = "lineup.sync_apply"

[[workflows.full_sync.steps]]
action = "guide.match_apply"
continue_on_failure = true
scope = { groups = "DIRECTV: *" }

[[workflows.full_sync.steps]]
action = "streams.match_apply"
scope = "lineup"
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        let steps = &config.workflows["full_sync"].steps;
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].action(), "lineup.sync_apply");
        assert!(!steps[0].continue_on_failure());
        assert!(steps[1].continue_on_failure());
        assert_eq!(
            steps[1].scope(),
            Some(&WorkflowStepScope::Table(ScopeConfig {
                groups: "DIRECTV: *".to_string(),
                ..ScopeConfig::default()
            }))
        );
        assert_eq!(
            steps[2].scope(),
            Some(&WorkflowStepScope::Named("lineup".to_string()))
        );

        config.save(&path).expect("save");
        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("reload");
        assert_eq!(reloaded.workflows["full_sync"].steps, *steps);
    }

    #[test]
    fn a_mistyped_step_key_is_named() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"[workflows.full_sync]
steps = [{ action = "guide.match_apply", continue_on_fail = true }]
"#,
        );
        let error = CrosstalkConfig::load_file(dir.path()).expect_err("unknown key");
        assert!(
            format!("{error:#}").contains("continue_on_fail"),
            "the error names the key: {error:#}"
        );
    }

    #[test]
    fn a_workflow_step_cannot_be_a_workflow() {
        // A step naming a workflow would be a cycle (or at least a sequence inside a
        // sequence), and this port has neither: a flat list of actions is the whole feature.
        let mut config = CrosstalkConfig::default();
        config.workflows.insert(
            "nightly".to_string(),
            WorkflowConfig {
                steps: vec!["workflow.other".into()],
                fresh: false,
            },
        );
        config.workflows.insert(
            "other".to_string(),
            WorkflowConfig {
                steps: vec!["workflow.nightly".into()],
                fresh: false,
            },
        );
        let errors = config.validate().errors;
        assert!(
            errors.iter().any(|error| error.contains("do not nest")),
            "{errors:?}"
        );
    }

    #[test]
    fn an_empty_or_malformed_workflow_is_a_validation_error() {
        let mut config = CrosstalkConfig::default();
        config.workflows.insert(
            "empty".to_string(),
            WorkflowConfig {
                steps: Vec::new(),
                fresh: false,
            },
        );
        config.workflows.insert(
            "shapeless".to_string(),
            WorkflowConfig {
                steps: vec!["probe".into()],
                fresh: false,
            },
        );
        let errors = config.validate().errors;
        assert!(
            errors
                .iter()
                .any(|error| error.contains("workflows.empty has no steps")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|error| error
                .contains("workflows.shapeless.steps[0]: probe must be <app>.<action>")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_schedule_naming_a_missing_workflow_is_an_error() {
        let mut config = CrosstalkConfig::default();
        config.schedules.insert(
            "workflow.nightly".to_string(),
            ScheduleEntry::cron("0 23 * * *"),
        );
        let errors = config.validate().errors;
        assert!(
            errors
                .iter()
                .any(|error| error.contains("names no [workflows.nightly] entry")),
            "{errors:?}"
        );
    }

    #[test]
    fn hooks_are_pacing_settings_plus_one_key_per_event() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[workflows.nightly]
steps = ["streams.probe"]

[hooks]
debounce_secs = 30
rate_per_minute = 12
m3u_refresh = ["streams.match_apply"]
epg_refresh = ["workflow.nightly", "streams.sort_apply"]
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        assert_eq!(config.hooks.debounce_secs, 30.0);
        assert_eq!(config.hooks.rate_per_minute, 12);
        assert_eq!(
            config.hooks.targets("epg_refresh"),
            [
                "workflow.nightly".to_string(),
                "streams.sort_apply".to_string()
            ]
        );
        // An event the file does not name has no targets; unknown event names are the normal
        // case, so none of this is an error.
        assert!(config.hooks.targets("channel_start").is_empty());
        assert_eq!(config.hooks.debounce(), std::time::Duration::from_secs(30));
        assert!(
            config
                .validate()
                .errors
                .iter()
                .all(|error| error.contains("credentials")),
            "{:?}",
            config.validate().errors
        );
    }

    #[test]
    fn a_malformed_hook_target_or_pacing_is_a_validation_error() {
        let mut config = CrosstalkConfig::default();
        config.hooks.debounce_secs = -1.0;
        config
            .hooks
            .events
            .insert("m3u_refresh".to_string(), vec!["typo".to_string()]);
        config
            .hooks
            .events
            .insert("epg_refresh".to_string(), Vec::new());
        let errors = config.validate().errors;
        assert!(
            errors
                .iter()
                .any(|error| error.contains("hooks.debounce_secs")),
            "{errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|error| error.contains("hooks.m3u_refresh: typo must be <app>.<action>")),
            "{errors:?}"
        );
        // An event with no targets is a warning: the delivery is accepted and ignored.
        assert!(
            config
                .validate()
                .warnings
                .iter()
                .any(|warning| warning.contains("hooks.epg_refresh lists no targets")),
            "{:?}",
            config.validate().warnings
        );
    }

    #[test]
    fn hooks_and_workflows_survive_a_save_with_comments() {
        // The web UI's host save rewrites the file through `toml_edit`: the event keys must come
        // back as event keys (not under `[hooks.events]`), and the workflow steps as steps.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"# operator note
[workflows.nightly]
steps = ["streams.probe", "streams.sort_apply"]

[hooks]
m3u_refresh = ["streams.match_apply"]
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        config.save(&path).expect("save");
        let text = std::fs::read_to_string(&path).expect("read back");
        assert!(text.contains("# operator note"), "comments survive: {text}");
        assert!(!text.contains("[hooks.events]"), "events stay flat: {text}");

        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("reload");
        assert_eq!(
            reloaded.workflows["nightly"].steps,
            vec!["streams.probe".into(), "streams.sort_apply".into()]
        );
        assert_eq!(
            reloaded.hooks.targets("m3u_refresh"),
            ["streams.match_apply".to_string()]
        );
        assert!(reloaded.hooks.debounce_secs > 0.0, "the default survives");
    }

    #[test]
    fn a_malformed_hook_value_names_the_event() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[hooks]\nm3u_refresh = \"streams.match_apply\"\n",
        );
        let error = CrosstalkConfig::load_file(dir.path()).expect_err("rejects");
        assert!(format!("{error:#}").contains("m3u_refresh"), "{error:#}");
    }

    #[test]
    fn environment_overrides_reach_hooks_and_workflows() {
        // The README promises every value is overridable as CROSSTALK_<SECTION>__<KEY>: the
        // new top-level tables are sections too, so the filter must let them through.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[workflows.nightly]\nsteps = [\"streams.probe\"]\n",
        );
        std::env::set_var("CROSSTALK_TESTHOOKS_HOOKS__RATE_PER_MINUTE", "12");
        std::env::set_var("CROSSTALK_TESTHOOKS_WORKFLOWS__NIGHTLY__FRESH", "true");
        let config =
            CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTHOOKS_").expect("load");
        std::env::remove_var("CROSSTALK_TESTHOOKS_HOOKS__RATE_PER_MINUTE");
        std::env::remove_var("CROSSTALK_TESTHOOKS_WORKFLOWS__NIGHTLY__FRESH");
        assert_eq!(config.hooks.rate_per_minute, 12);
        assert!(config.workflows["nightly"].fresh);
        assert_eq!(
            config.env_overrides, 1,
            "overrides applied, so save still refuses"
        );
        assert!(config.save(dir.path().join("crosstalk.toml")).is_err());
    }

    #[test]
    fn unknown_top_level_keys_are_rejected_loudly() {
        // Strict configs fail on typos instead of silently falling back to defaults: an
        // unknown section is an error that names the key, not a value that is kept.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "# operator note\n[future_section]\nthing = 1\n",
        );
        let error = CrosstalkConfig::load_file(dir.path()).expect_err("rejects unknown keys");
        assert!(
            format!("{error:#}").contains("future_section"),
            "the error names the key: {error:#}"
        );
    }

    #[test]
    fn unknown_nested_keys_are_rejected_loudly() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "crosstalk.toml", "[server]\nportt = 9999\n");
        let error = CrosstalkConfig::load(dir.path()).expect_err("rejects the typo");
        assert!(
            format!("{error:#}").contains("portt"),
            "the error names the key: {error:#}"
        );

        write(
            dir.path(),
            "crosstalk.toml",
            "[matching]\ncheck_hourss = 99\n",
        );
        let error = CrosstalkConfig::load(dir.path()).expect_err("rejects the typo");
        assert!(
            format!("{error:#}").contains("check_hourss"),
            "the error names the key: {error:#}"
        );
    }

    #[test]
    fn saving_adds_the_ai_policy_alongside_the_providers() {
        // `[ai.verify]` is a sibling of the provider tables, so a save has to add a table
        // into a table that already has sub-tables and still produce valid TOML.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            "[ai.providers.typesafe]\nkind = \"typesafe\"\nbase_url = \"https://api.typesafe.ai/v1\"\n",
        );

        let mut config = CrosstalkConfig::load_file(dir.path()).expect("load");
        config.ai.verify.enabled = true;
        config.ai.verify.model = "jev-1.14.0".to_string();
        config.scope.profiles = "All".to_string();
        config.exports.retention_days = 30;
        config.save(&path).expect("save");

        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("[ai.verify]"), "{text}");
        assert!(text.contains("[ai.providers.typesafe]"), "{text}");
        assert!(text.contains("[scope]"), "{text}");
        assert!(text.contains("[exports]"), "{text}");

        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("the written file parses");
        assert!(reloaded.ai.verify.enabled);
        assert_eq!(reloaded.ai.verify.model, "jev-1.14.0");
        assert_eq!(reloaded.scope.profiles, "All");
        assert_eq!(reloaded.exports.retention_days, 30);
        assert!(reloaded.ai.providers.contains_key("typesafe"));
    }

    #[test]
    fn saving_refuses_a_file_that_does_not_parse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        std::fs::write(&path, "# keep me\n[server\nport = 1\n").expect("seed");
        let error = CrosstalkConfig::default().save(&path).expect_err("refuses");
        assert!(format!("{error}").contains("not valid TOML"));
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("# keep me"), "{text}");
    }

    #[test]
    fn a_numeric_secret_in_the_environment_still_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "crosstalk.toml", "[server]\nport = 1\n");
        std::env::set_var("CROSSTALK_TESTDIGITS_SERVER__TOKEN", "123456");
        let config = CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTDIGITS_")
            .expect("a digits-only token must not be read as a number");
        std::env::remove_var("CROSSTALK_TESTDIGITS_SERVER__TOKEN");
        assert_eq!(config.server.resolved_token().as_deref(), Some("123456"));
    }

    #[test]
    fn the_verify_chain_defaults_to_the_shorthand_pair() {
        // Old files name one provider and model and no chain: that pair is the one-link
        // chain, so every existing crosstalk.toml keeps working untouched.
        let verify = AiVerifyConfig::default();
        assert!(verify.chain.is_empty());
        assert_eq!(
            verify.effective_chain(),
            vec![AiChainEntry::new("typesafe", "jev-latest")]
        );
        assert!(!verify.has_shorthand_and_chain());

        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[ai.verify]\nprovider = \"local\"\nmodel = \"m\"\n",
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        assert_eq!(
            config.ai.verify.effective_chain(),
            vec![AiChainEntry::new("local", "m")]
        );
    }

    #[test]
    fn the_verify_chain_parses_from_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[dispatcharr]
api_key = "k"

[ai.providers.typesafe]
kind = "typesafe"
base_url = "https://api.typesafe.ai/v1"
api_key = "k"

[ai.providers.llm]
kind = "openai_compat"
base_url = "http://gpu-box:8000/v1"

[ai.verify]
enabled = true
chain = [{provider = "typesafe", model = "jev-latest"}, {provider = "llm", model = "kimi-k3"}]
"#,
        );
        let config = CrosstalkConfig::load_file(dir.path()).expect("load");
        assert_eq!(
            config.ai.verify.effective_chain(),
            vec![
                AiChainEntry::new("typesafe", "jev-latest"),
                AiChainEntry::new("llm", "kimi-k3"),
            ]
        );
        // The chain replaces the pair: the pair's own values are untouched defaults.
        assert_eq!(config.ai.verify.provider, "typesafe");
        assert!(config.validate().is_ok(), "{:?}", config.validate().errors);

        // And it survives a save with the operator's comments.
        config
            .save(dir.path().join("crosstalk.toml"))
            .expect("save");
        let reloaded = CrosstalkConfig::load_file(dir.path()).expect("reload");
        assert_eq!(reloaded.ai.verify.effective_chain().len(), 2);
    }

    #[test]
    fn setting_both_the_pair_and_the_chain_is_refused() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "k".to_string();
        config.ai.verify.enabled = true;
        config.ai.verify.provider = "local".to_string();
        config.ai.verify.model = "m".to_string();
        config.ai.verify.chain = vec![AiChainEntry::new("typesafe", "jev-latest")];
        let report = config.validate();
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("ai.verify.chain is set")
                    && error.contains("not both")),
            "{report:?}"
        );

        // A pair left at its defaults reads as "not set": the chain alone is fine.
        config.ai.verify.provider = DEFAULT_VERIFY_PROVIDER.to_string();
        config.ai.verify.model = DEFAULT_VERIFY_MODEL.to_string();
        assert!(
            !config
                .validate()
                .errors
                .iter()
                .any(|error| error.contains("ai.verify.chain is set")),
            "{:?}",
            config.validate().errors
        );
    }

    #[test]
    fn a_chain_link_naming_no_provider_is_refused() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "k".to_string();
        config.ai.verify.enabled = true;
        config.ai.verify.chain = vec![
            AiChainEntry::new("typesafe", "jev-latest"),
            AiChainEntry::new("llm", ""),
        ];
        let report = config.validate();
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("ai.verify.chain[0].provider")
                    && error.contains("no [ai.providers.typesafe] entry")),
            "{report:?}"
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("ai.verify.chain[1].model is empty")),
            "{report:?}"
        );

        // Disabled, the chain is not checked: an operator can stage it before the keys.
        config.ai.verify.enabled = false;
        assert!(
            config
                .validate()
                .errors
                .iter()
                .all(|error| !error.contains("ai.verify.chain[")),
            "{:?}",
            config.validate().errors
        );
    }

    #[test]
    fn the_chain_comes_from_the_environment_as_json() {
        // Figment has no syntax for a list of tables, so the override is the JSON string
        // holding the same array the file would hold as tables.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[ai.verify]\nenabled = true\n",
        );
        std::env::set_var(
            "CROSSTALK_TESTCHAIN_AI__VERIFY__CHAIN",
            r#"[{"provider": "typesafe", "model": "jev-latest"}, {"provider": "llm", "model": "kimi-k3"}]"#,
        );
        let config =
            CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTCHAIN_").expect("load");
        std::env::remove_var("CROSSTALK_TESTCHAIN_AI__VERIFY__CHAIN");
        assert_eq!(config.env_overrides, 1);
        assert_eq!(
            config.ai.verify.effective_chain(),
            vec![
                AiChainEntry::new("typesafe", "jev-latest"),
                AiChainEntry::new("llm", "kimi-k3"),
            ]
        );

        // And an effective config still refuses to be saved, secrets or not.
        let error = config
            .save(dir.path().join("crosstalk.toml"))
            .expect_err("refuses");
        assert!(format!("{error}").contains("environment overrides"));
    }

    #[test]
    fn a_chain_link_missing_its_model_is_a_load_error() {
        // Defaulting the model would send `jev-latest` to an LLM endpoint.
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[ai.verify]\nchain = [{provider = \"llm\"}]\n",
        );
        let error = CrosstalkConfig::load_file(dir.path()).expect_err("rejects");
        assert!(format!("{error:#}").contains("model"), "{error:#}");
    }

    #[test]
    fn a_garbled_chain_override_is_a_load_error_naming_the_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[ai.verify]\nenabled = true\n",
        );
        std::env::set_var(
            "CROSSTALK_TESTCHAINBAD_AI__VERIFY__CHAIN",
            "not json at all",
        );
        let error = CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTCHAINBAD_")
            .expect_err("rejects");
        std::env::remove_var("CROSSTALK_TESTCHAINBAD_AI__VERIFY__CHAIN");
        assert!(
            format!("{error:#}").contains("ai.verify.chain"),
            "{error:#}"
        );
    }

    #[test]
    fn the_chain_round_trips_through_the_settings_form() {
        // The host page edits the chain as a JSON text field: values -> form -> values is
        // the identity, and blank clears back to the shorthand.
        let mut config = CrosstalkConfig::default();
        config.ai.verify.chain = vec![AiChainEntry::new("llm", "kimi-k3")];
        let values = shared_values(&config);
        assert_eq!(
            values.text("ai.verify.chain"),
            Some(r#"[{"provider":"llm","model":"kimi-k3"}]"#)
        );

        let mut applied = CrosstalkConfig::default();
        apply_host_values(&mut applied, &values).expect("apply");
        assert_eq!(applied.ai.verify.chain, config.ai.verify.chain);

        let mut blank = SettingsValues::new();
        blank.insert("ai.verify.chain", FieldValue::Text(String::new()));
        apply_host_values(&mut applied, &blank).expect("clear");
        assert!(applied.ai.verify.chain.is_empty());
        assert_eq!(
            applied.ai.verify.effective_chain(),
            vec![AiChainEntry::new("typesafe", "jev-latest")]
        );

        let mut bad = SettingsValues::new();
        bad.insert("ai.verify.chain", FieldValue::Text("[{oops}]".to_string()));
        let error = apply_host_values(&mut applied, &bad).expect_err("rejects");
        assert!(format!("{error:#}").contains("ai.verify.chain"));
    }

    #[test]
    fn provider_names_must_be_lowercase_for_env_overrides() {
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "k".to_string();
        config.ai.providers.insert(
            "TypeSafe".to_string(),
            AiProviderConfig {
                base_url: "https://api.typesafe.ai/v1".to_string(),
                api_key: "k".to_string(),
                ..AiProviderConfig::default()
            },
        );
        let report = config.validate();
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("must be lowercase")));
    }

    #[test]
    fn provider_numeric_defaults_are_usable() {
        let provider = AiProviderConfig::default();
        assert_eq!(provider.timeout_secs, 30);
        assert_eq!(provider.max_concurrency, 8);

        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "k".to_string();
        config.ai.providers.insert(
            "zeroed".to_string(),
            AiProviderConfig {
                base_url: "https://example.invalid".to_string(),
                api_key: "k".to_string(),
                timeout_secs: 0,
                max_concurrency: 0,
                ..AiProviderConfig::default()
            },
        );
        let report = config.validate();
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("timeout_secs")));
        assert!(report
            .errors
            .iter()
            .any(|error| error.contains("max_concurrency")));
    }

    #[test]
    fn the_host_schema_covers_every_shared_and_matching_option() {
        use crate::subapp::FieldValue;
        let schema = host_schema();
        let ids: Vec<&str> = schema.editable().map(|field| field.id).collect();
        for expected in [
            "matching.sensitivity",
            "matching.ignore_quality_tags",
            "matching.ignore_regional_tags",
            "matching.ignore_geographic_tags",
            "matching.ignore_misc_tags",
            "matching.enabled_databases",
            "matching.custom_aliases",
            "scope.profiles",
            "scope.groups",
            "scope.ignore_groups",
            "exports.retention_days",
            "ai.verify.enabled",
            "ai.verify.provider",
            "ai.verify.model",
            "ai.verify.verify_band_low",
            "ai.verify.verify_band_high",
            "ai.verify.rescue",
            "ai.verify.rescue_floor",
            "ai.verify.shortlist",
            "ai.verify.accept_threshold",
            "ai.verify.reject_threshold",
            "ai.verify.min_confidence",
        ] {
            assert!(ids.contains(&expected), "{expected} is not on the form");
        }
        // Every field carries a default of the right shape, so the form can render.
        for field in schema.editable() {
            let matches = matches!(
                (&field.kind, &field.default),
                (crate::subapp::FieldKind::Text, FieldValue::Text(_))
                    | (crate::subapp::FieldKind::Number, FieldValue::Number(_))
                    | (crate::subapp::FieldKind::Boolean, FieldValue::Boolean(_))
                    | (crate::subapp::FieldKind::JsonObject, FieldValue::Json(_))
            );
            assert!(matches, "{} has a mismatched default", field.id);
            assert!(
                schema
                    .sections()
                    .any(|section| Some(section.id) == field.section),
                "{} points at an unknown section",
                field.id
            );
        }
    }

    #[test]
    fn the_shared_schema_is_the_subset_the_reports_print() {
        let shared: Vec<&str> = shared_schema().editable().map(|field| field.id).collect();
        assert!(shared.contains(&"scope.profiles"));
        assert!(shared.contains(&"exports.retention_days"));
        assert!(shared.contains(&"ai.verify.enabled"));
        assert!(!shared.contains(&"matching.ignore_quality_tags"));
    }

    #[test]
    fn the_shared_settings_load_from_the_host_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            r#"
[scope]
profiles = "All, Favorites"
groups = "DTV: *"
ignore_groups = ""

[exports]
retention_days = 7

[ai.verify]
enabled = true
provider = "typesafe"
model = "jev-1.14.0"
verify_band_low = 70
verify_band_high = 90
rescue_floor = 50
shortlist = 3
accept_threshold = 0.9
reject_threshold = 0.1
min_confidence = 0.6
"#,
        );
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        assert_eq!(config.scope.profiles, "All, Favorites");
        assert_eq!(config.scope.groups, "DTV: *");
        assert!(config.scope.ignore_groups.is_empty());
        assert_eq!(config.exports.retention_days, 7);
        assert!(config.ai.verify.enabled);
        assert_eq!(config.ai.verify.model, "jev-1.14.0");
        assert_eq!(config.ai.verify.verify_band_low, 70);
        assert_eq!(config.ai.verify.shortlist, 3);
        assert_eq!(config.ai.verify.min_confidence, 0.6);
        // The providers are still their own table, untouched by the policy.
        assert!(config.ai.providers.is_empty());
    }

    #[test]
    fn the_shared_settings_round_trip_through_the_form_values() {
        let config = CrosstalkConfig::default();
        let mut rebuilt = CrosstalkConfig::default();
        apply_host_values(&mut rebuilt, &host_values(&config)).expect("apply");
        assert_eq!(rebuilt.scope, config.scope);
        assert_eq!(rebuilt.exports, config.exports);
        assert_eq!(rebuilt.ai.verify, config.ai.verify);
        assert_eq!(rebuilt.matching.sensitivity, config.matching.sensitivity);
        assert_eq!(
            rebuilt.matching.custom_aliases,
            config.matching.custom_aliases
        );

        let mut values = SettingsValues::new();
        values.insert("scope.profiles", FieldValue::Text("All".to_string()));
        values.insert("exports.retention_days", FieldValue::Number(30.0));
        values.insert("ai.verify.verify_band_high", FieldValue::Number(88.0));
        let mut config = CrosstalkConfig::default();
        apply_host_values(&mut config, &values).expect("apply");
        assert_eq!(config.scope.profiles, "All");
        assert_eq!(config.exports.retention_days, 30);
        assert_eq!(config.ai.verify.verify_band_high, 88);
        // A field the form did not submit is left alone.
        assert_eq!(config.ai.verify.verify_band_low, 80);
    }

    #[test]
    fn the_sensitivity_names_the_one_decision_cut() {
        // D02/T10 `janitor-95`: the default cut is strict (95); presets and bare numbers
        // resolve through the engine's ladder.
        let config = CrosstalkConfig::default();
        assert_eq!(config.matching.sensitivity, "strict");
        assert_eq!(config.matching.threshold(), 95);
        assert!(
            CrosstalkConfig::default()
                .validate()
                .errors
                .iter()
                .all(|error| !error.contains("matching.sensitivity")),
            "{:?}",
            CrosstalkConfig::default().validate().errors
        );

        for (sensitivity, cut) in [
            ("strict", 95),
            ("high", 90),
            ("medium", 80),
            ("low", 70),
            ("80", 80),
        ] {
            let mut config = CrosstalkConfig::default();
            config.matching.sensitivity = sensitivity.to_string();
            // (The default config has no Dispatcharr credentials, so scope this to the
            // setting under test rather than the whole report.)
            assert!(
                config
                    .validate()
                    .errors
                    .iter()
                    .all(|error| !error.contains("matching.sensitivity")),
                "{sensitivity}"
            );
            assert_eq!(config.matching.threshold(), cut, "{sensitivity}");
            assert_eq!(config.matching.engine_config().threshold, cut);
        }
        // Unknown names and out-of-range numbers fail validation instead of silently
        // deciding at 95.
        for bad in ["fuzzy", "150"] {
            let mut config = CrosstalkConfig::default();
            config.matching.sensitivity = bad.to_string();
            let report = config.validate();
            assert!(!report.is_ok());
            assert!(
                report
                    .errors
                    .iter()
                    .any(|error| error.contains("matching.sensitivity")),
                "{bad}: {report:?}"
            );
        }
    }

    #[test]
    fn applying_host_values_enforces_the_old_bounds() {
        let mut config = CrosstalkConfig::default();

        let mut values = SettingsValues::new();
        values.insert("exports.retention_days", FieldValue::Number(6.5));
        let error = apply_host_values(&mut config, &values).expect_err("rejects");
        assert!(format!("{error}").contains("whole number"), "{error}");

        let mut values = SettingsValues::new();
        values.insert("exports.retention_days", FieldValue::Number(-1.0));
        assert!(format!(
            "{}",
            apply_host_values(&mut config, &values).expect_err("rejects")
        )
        .contains("between"),);

        let mut values = SettingsValues::new();
        values.insert("ai.verify.accept_threshold", FieldValue::Number(1.5));
        assert!(apply_host_values(&mut config, &values).is_err());

        let mut values = SettingsValues::new();
        values.insert("ai.verify.shortlist", FieldValue::Number(0.0));
        assert!(apply_host_values(&mut config, &values).is_err());
    }

    #[test]
    fn the_moved_settings_are_validated_in_the_host_file() {
        // These rules were `guide.toml`'s; the values moved, so the checks moved with them.
        let mut config = CrosstalkConfig::default();
        config.dispatcharr.api_key = "k".to_string();
        assert!(config.validate().is_ok(), "{:?}", config.validate().errors);

        config.scope.groups = "A".to_string();
        config.scope.ignore_groups = "B".to_string();
        config.ai.verify.verify_band_low = 95;
        config.ai.verify.verify_band_high = 80;
        config.ai.verify.reject_threshold = 0.9;
        config.ai.verify.accept_threshold = 0.5;
        config.ai.verify.shortlist = 0;
        config.ai.verify.enabled = true;
        config.ai.verify.provider = String::new();
        let errors = config.validate().errors;
        for expected in [
            "scope.groups and scope.ignore_groups",
            "ai.verify.verify_band_low",
            "ai.verify.reject_threshold",
            "ai.verify.shortlist",
            "ai.verify.provider is empty",
        ] {
            assert!(
                errors.iter().any(|error| error.contains(expected)),
                "{expected} not reported: {errors:?}"
            );
        }
    }

    #[test]
    fn ai_verify_defaults_match_the_plan() {
        let verify = AiVerifyConfig::default();
        assert!(!verify.enabled);
        assert_eq!(verify.provider, "typesafe");
        assert_eq!(verify.model, "jev-latest");
        assert_eq!(verify.verify_band_low, 80);
        assert_eq!(verify.verify_band_high, 94);
        assert!(verify.rescue);
        assert_eq!(verify.rescue_floor, 60);
        assert_eq!(verify.shortlist, 5);
        assert_eq!(verify.accept_threshold, 0.8);
        assert_eq!(verify.reject_threshold, 0.3);
        assert_eq!(verify.min_confidence, 0.5);
    }

    #[test]
    fn the_shared_tables_have_environment_overrides() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "crosstalk.toml",
            "[scope]\nprofiles = \"All\"\n",
        );
        std::env::set_var("CROSSTALK_TESTSCOPE_SCOPE__PROFILES", "All, Favorites");
        std::env::set_var("CROSSTALK_TESTSCOPE_EXPORTS__RETENTION_DAYS", "9");
        std::env::set_var("CROSSTALK_TESTSCOPE_AI__VERIFY__ENABLED", "true");
        let config =
            CrosstalkConfig::load_prefixed(dir.path(), "CROSSTALK_TESTSCOPE_").expect("load");
        std::env::remove_var("CROSSTALK_TESTSCOPE_SCOPE__PROFILES");
        std::env::remove_var("CROSSTALK_TESTSCOPE_EXPORTS__RETENTION_DAYS");
        std::env::remove_var("CROSSTALK_TESTSCOPE_AI__VERIFY__ENABLED");

        assert_eq!(config.scope.profiles, "All, Favorites");
        assert_eq!(config.exports.retention_days, 9);
        assert!(config.ai.verify.enabled);
        assert_eq!(config.env_overrides, 1, "the value differs from the file");
    }

    #[test]
    fn run_history_is_kept_for_15_days_by_default() {
        assert_eq!(JobsConfig::default().run_history_days, 15);
        // An old file without the key still loads, and inherits the 15-day default.
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "crosstalk.toml", "[jobs]\nworker_threads = 2\n");
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        assert_eq!(config.jobs.run_history_days, 15);
        // Explicit zero keeps everything.
        write(
            dir.path(),
            "crosstalk.toml",
            "[jobs]\nrun_history_days = 0\n",
        );
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        assert_eq!(config.jobs.run_history_days, 0);
    }

    #[test]
    fn worker_threads_resolve_zero_to_the_core_count() {
        let config = JobsConfig::default();
        assert_eq!(
            config.resolved_worker_threads(),
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
        );
        let config = JobsConfig {
            worker_threads: 3,
            ..JobsConfig::default()
        };
        assert_eq!(config.resolved_worker_threads(), 3);
    }

    #[test]
    fn save_preserves_comments_and_updates_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        write(
            dir.path(),
            "crosstalk.toml",
            "# The port the UI listens on\n[server]\nport = 8730\n\n# How to reach Dispatcharr\n[dispatcharr]\nbase_url = \"http://old:9191\"\n",
        );

        let mut config = CrosstalkConfig::load(dir.path()).expect("load");
        config.server.port = 9100;
        config.dispatcharr.base_url = "http://new:9191".to_string();
        config.save(&path).expect("save");

        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("# The port the UI listens on"), "{text}");
        assert!(text.contains("# How to reach Dispatcharr"), "{text}");
        assert!(text.contains("port = 9100"), "{text}");
        assert!(text.contains("http://new:9191"), "{text}");

        // And the written file loads back to the same values.
        let reloaded = CrosstalkConfig::load(dir.path()).expect("reload");
        assert_eq!(reloaded.server.port, 9100);
    }

    #[test]
    fn save_creates_a_missing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("new.toml");
        CrosstalkConfig::default().save(&path).expect("save");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("[server]"), "{text}");
    }

    #[cfg(unix)]
    #[test]
    fn saved_files_are_owner_only() {
        // These files can inline the UI token and the Dispatcharr admin key.
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        CrosstalkConfig::default().save(&path).expect("save");
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "saved with mode {:o}", mode & 0o777);
        // No temporary file is left behind in the directory.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("list")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn concurrent_saves_to_the_same_path_all_succeed() {
        // Two settings POSTs can land at once; pid-only temp names would be shared by
        // every thread of the daemon and the saves would truncate/rename over each other.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.toml");
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let path = path.clone();
                std::thread::spawn(move || CrosstalkConfig::default().save(&path))
            })
            .collect();
        for handle in handles {
            handle
                .join()
                .expect("save thread")
                .expect("every concurrent save succeeds");
        }
        CrosstalkConfig::load_file(dir.path()).expect("the surviving file parses");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("list")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn directories_resolve_against_the_config_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        assert_eq!(config.exports_dir(), dir.path().join("exports"));
        assert_eq!(config.data_dir(), dir.path().join("data"));

        let absolute = CrosstalkConfig {
            jobs: JobsConfig {
                exports_dir: "/var/lib/crosstalk/exports".to_string(),
                ..JobsConfig::default()
            },
            ..CrosstalkConfig::default()
        };
        assert_eq!(
            absolute.exports_dir(),
            PathBuf::from("/var/lib/crosstalk/exports")
        );
    }
}
