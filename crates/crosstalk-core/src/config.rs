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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 8730,
            token: String::new(),
            token_env: "CROSSTALK_TOKEN".to_string(),
        }
    }
}

impl ServerConfig {
    /// The token to require, resolved from the environment first.
    pub fn resolved_token(&self) -> Option<String> {
        resolve_secret(&self.token, &self.token_env)
    }

    /// Whether the bind address is a loopback address.
    pub fn is_loopback(&self) -> bool {
        let bind = self.bind.trim();
        bind == "localhost" || bind.starts_with("127.") || bind == "::1" || bind == "[::1]"
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
    /// Cap on concurrent HTTP requests, for the programme-data checks and the AI calls.
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

impl Default for AiVerifyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "typesafe".to_string(),
            model: "jev-latest".to_string(),
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
    /// Country database codes the matcher may draw names from. Empty means "the default
    /// set", which is every shipped database when only one exists, otherwise the US one.
    pub enabled_databases: Vec<String>,
    /// Custom aliases, a table of `name -> [aliases]`. An entry REPLACES the built-in list
    /// for that key rather than adding to it.
    pub custom_aliases: BTreeMap<String, Vec<String>>,
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
        }
    }

    /// These settings in the matcher crate's own terms.
    ///
    /// `crosstalk-matching` cannot depend on this crate (the dependency runs the other way),
    /// so the host hands its values over rather than the matcher reading the file.
    pub fn matcher_config(&self) -> crosstalk_matching::MatcherConfig {
        crosstalk_matching::MatcherConfig {
            flags: self.normalize_flags(),
            databases: self.databases(),
            custom_aliases: self.custom_aliases.clone(),
        }
    }

    /// Build the matcher a sub-app runs with.
    ///
    /// Every app builds its matcher here, so the flags, aliases and callsigns a run uses are
    /// the ones the host file states and the profile's parity sweep guards.
    pub fn matcher(
        &self,
        profile: crosstalk_matching::MatchProfile,
    ) -> crosstalk_matching::Matcher {
        crosstalk_matching::Matcher::from_config(&self.matcher_config(), profile)
    }

    /// Which country databases to load.
    ///
    /// An explicit list wins. An empty list falls back to the plugin's rule: every
    /// database when only one ships, otherwise the United States one.
    pub fn databases(&self) -> Vec<String> {
        let available = crosstalk_matching::data::available_database_codes();
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
    /// Cron expressions per `<app>.<action>`, for example `guide.match_apply = "0 4 * * *"`.
    pub schedules: BTreeMap<String, String>,
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
        const SECTIONS: [&str; 8] = [
            "server",
            "dispatcharr",
            "ai",
            "matching",
            "scope",
            "exports",
            "jobs",
            "schedules",
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

        let available = crosstalk_matching::data::available_database_codes();
        for code in &self.matching.enabled_databases {
            let upper = code.trim().to_uppercase();
            if !available.contains(&upper.as_str()) {
                report.warnings.push(format!(
                    "matching.enabled_databases lists {code:?}, which is not shipped \
                     (available: {})",
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

        for (key, cron) in &self.schedules {
            if !key.contains('.') {
                report.errors.push(format!(
                    "schedules.{key} must be <app>.<action>, e.g. guide.match_apply"
                ));
            }
            // Parsed, not merely counted: a five-field expression with a misspelled weekday
            // would otherwise validate and then never fire.
            if let Err(error) = crate::scheduler::parse_schedule(cron) {
                report.errors.push(format!("schedules.{key}: {error:#}"));
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
        .with_help("Model name as the provider spells it."),
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
                .map(String::as_str),
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
            .insert("guide".to_string(), "every day".to_string());
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
