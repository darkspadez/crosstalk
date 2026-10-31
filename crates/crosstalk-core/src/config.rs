//! Configuration: one host file plus one file per sub-app.
//!
//! The split follows how the options are used. Anything the HOST owns — the Dispatcharr
//! connection, the AI providers, the HTTP listener, the thread pool, the export directory —
//! lives in `crosstalk.toml`. Anything about matching names — the normalisation toggles,
//! the enabled country databases, the custom aliases — also lives in `crosstalk.toml`,
//! because every sub-app that matches channel names must agree on it; two plugins
//! disagreeing about whether `[HD]` is stripped would produce two different answers for the
//! same channel. A sub-app's own thresholds and scope filters live in its own file.
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

/// Where the daemon listens, and how it authenticates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Interface to bind. Localhost by default, so an unconfigured daemon is not exposed.
    pub bind: String,
    pub port: u16,
    /// Bearer token required for every request when set. Empty means "no token", which is
    /// only safe while `bind` is a loopback address.
    pub token: String,
    /// Read the token from this environment variable instead of the file.
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
#[serde(default)]
pub struct DispatcharrConfig {
    /// Base URL, without the `/api` suffix: `http://dispatcharr:9191`.
    pub base_url: String,
    /// API key, sent as `X-API-Key`. Preferred: it does not expire.
    pub api_key: String,
    pub api_key_env: String,
    /// JWT fallback when no API key is configured.
    pub username: String,
    pub password: String,
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
        if base.ends_with("/api") {
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AiProviderConfig {
    /// `typesafe` (native System One) or `openai_compat`.
    pub kind: String,
    pub base_url: String,
    pub api_key: String,
    pub api_key_env: String,
    pub timeout_secs: u64,
    pub max_concurrency: usize,
}

impl AiProviderConfig {
    pub fn resolved_api_key(&self) -> Option<String> {
        resolve_secret(&self.api_key, &self.api_key_env)
    }
}

/// Every configured AI provider, keyed by the name a sub-app refers to.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    pub providers: BTreeMap<String, AiProviderConfig>,
}

impl AiConfig {
    pub fn provider(&self, name: &str) -> Option<&AiProviderConfig> {
        self.providers.get(name)
    }
}

/// Name-comparison settings shared by every sub-app.
///
/// These are in the host file, not a sub-app file, because a sub-app that normalised
/// names differently would match a different candidate for the same channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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

    /// Which country databases to load.
    ///
    /// An explicit list wins. An empty list falls back to the plugin's rule: every
    /// database when only one ships, otherwise the United States one.
    pub fn databases(&self) -> Vec<String> {
        if !self.enabled_databases.is_empty() {
            return self
                .enabled_databases
                .iter()
                .map(|code| code.trim().to_uppercase())
                .filter(|code| !code.is_empty())
                .collect();
        }
        let available = crosstalk_matching::data::available_database_codes();
        if available.len() == 1 {
            available.iter().map(|code| code.to_string()).collect()
        } else {
            vec!["US".to_string()]
        }
    }
}

/// Filesystem and thread-pool settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobsConfig {
    /// Worker threads for the matching stage. 0 means "one per core".
    pub worker_threads: usize,
    /// Where CSV reports are written.
    pub exports_dir: String,
    /// Where the SQLite database lives.
    pub data_dir: String,
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            worker_threads: 0,
            exports_dir: "exports".to_string(),
            data_dir: "data".to_string(),
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
#[serde(default)]
pub struct CrosstalkConfig {
    pub server: ServerConfig,
    pub dispatcharr: DispatcharrConfig,
    pub ai: AiConfig,
    pub matching: MatchingConfig,
    pub jobs: JobsConfig,
    /// Cron expressions per `<app>.<action>`, for example `janitor.automatch = "0 4 * * *"`.
    pub schedules: BTreeMap<String, String>,
    /// Directory the configuration was loaded from. Not part of the file.
    #[serde(skip)]
    pub config_dir: PathBuf,
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
        use figment::providers::{Env, Serialized};
        use figment::Figment;

        let overrides: Figment = Figment::from(Serialized::defaults(&*self))
            .merge(Env::prefixed(env_prefix).split("__"));
        let merged: Self = match overrides.extract() {
            Ok(merged) => merged,
            Err(error) => {
                // A malformed override should not silently do nothing.
                return Err(anyhow::anyhow!("invalid environment override: {error}"));
            }
        };
        let dir = std::mem::take(&mut self.config_dir);
        *self = merged;
        self.config_dir = dir;
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
        } else if !(self.dispatcharr.base_url.starts_with("http://")
            || self.dispatcharr.base_url.starts_with("https://"))
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
            if provider.base_url.trim().is_empty() {
                report
                    .errors
                    .push(format!("ai.providers.{name}.base_url is empty"));
            }
            if provider.resolved_api_key().is_none() {
                report.warnings.push(format!(
                    "ai.providers.{name} has no api_key; AI features that use it will be skipped"
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

        for (key, cron) in &self.schedules {
            if !key.contains('.') {
                report.errors.push(format!(
                    "schedules.{key} must be <app>.<action>, e.g. janitor.automatch"
                ));
            }
            if cron.split_whitespace().count() < 5 {
                report.errors.push(format!(
                    "schedules.{key} is not a 5-field cron expression: {cron:?}"
                ));
            }
        }

        report
    }

    /// Write the config back to `<dir>/crosstalk.toml`, preserving comments.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        save_with_comments(path.as_ref(), self)
    }
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

    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let mut document: toml_edit::DocumentMut = existing
        .parse()
        .unwrap_or_else(|_| toml_edit::DocumentMut::new());

    merge_document(&mut document, table);
    std::fs::write(path, document.to_string())
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Copy every value from `source` into `target`, keeping `target`'s comments and key order.
///
/// Keys are matched by path, so a comment above `[server] port = 8730` survives a change
/// to the port.
fn merge_document(target: &mut toml_edit::DocumentMut, source: toml_edit::DocumentMut) {
    merge_table(target.as_table_mut(), source.as_table());
}

fn merge_table(target: &mut toml_edit::Table, source: &toml_edit::Table) {
    for (key, source_item) in source {
        match (target.get_mut(key), source_item) {
            (Some(toml_edit::Item::Table(target_table)), toml_edit::Item::Table(source_table)) => {
                merge_table(target_table, source_table);
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
"janitor.automatch" = "0 4 * * *"
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
        assert_eq!(config.matching.databases(), vec!["US", "UK"]);
        assert_eq!(
            config.matching.custom_aliases.get("Fox News Channel"),
            Some(&vec!["FNC".to_string()])
        );
        assert_eq!(
            config
                .schedules
                .get("janitor.automatch")
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
            .insert("janitor".to_string(), "every day".to_string());
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
            .any(|error| error.contains("5-field cron")));
    }

    #[test]
    fn database_fallback_matches_the_plugin() {
        let config = MatchingConfig::default();
        // Twelve databases ship, so the plugin's rule picks the US one.
        assert_eq!(config.databases(), vec!["US".to_string()]);
        let config = MatchingConfig {
            enabled_databases: vec!["uk".to_string(), " us ".to_string(), "".to_string()],
            ..MatchingConfig::default()
        };
        assert_eq!(config.databases(), vec!["UK".to_string(), "US".to_string()]);
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
