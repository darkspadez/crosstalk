//! The plugin endpoints: what is installed, and running one action.
//!
//! This is how crosstalk talks to its own Dispatcharr-side bridge plugin (`crosstalk-bridge`):
//! Dispatcharr runs a plugin's action in-process, so a plugin is the only way to reach the ORM
//! for the writes the API cannot do. The client therefore needs two things — is the bridge
//! installed at a usable version, and can its action be invoked — and nothing else.

use super::*;
use anyhow::{bail, Context, Result};

impl DispatcharrClient {
    /// Every installed plugin.
    ///
    /// `GET /plugins/plugins/` answers `{"plugins": [...]}` (an object, not a list envelope),
    /// which is why this does not go through the shared list helper.
    pub async fn plugins(&self) -> Result<Vec<PluginInfo>> {
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            plugins: Vec<PluginInfo>,
        }

        let response: Response = self
            .get_json("/plugins/plugins/", &[])
            .await
            .context("listing plugins")?;
        Ok(response.plugins)
    }

    /// One installed plugin by key, or `None` when Dispatcharr does not know it.
    ///
    /// Presence and version are what decide whether an app offers the actions that need the
    /// bridge; a plugin listed with `missing: true` has a config row but no code, so it cannot
    /// run anything and is reported as absent here rather than as a stale version.
    pub async fn plugin(&self, key: &str) -> Result<Option<PluginInfo>> {
        Ok(self
            .plugins()
            .await?
            .into_iter()
            .find(|plugin| plugin.key == key && !plugin.missing))
    }

    /// Run one action on an installed plugin.
    ///
    /// `POST /plugins/plugins/<key>/run/` with `{"action": …, "params": {…}}` runs the
    /// plugin's `run(action, params, context)` in Dispatcharr itself and answers
    /// `{"success": true, "result": …}`. A plugin that is not installed (404), disabled (403)
    /// or that raises (500) answers `{"success": false, "error": …}`, and the error body is
    /// what the returned error carries.
    ///
    /// Not retried: an action is arbitrary code, and running a bridge write twice is not
    /// something the client can assume is safe.
    pub async fn run_plugin_action(
        &self,
        key: &str,
        action: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let path = plugin_run_path(key)?;
        if action.trim().is_empty() {
            bail!("running a plugin action needs an action id");
        }

        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            success: bool,
            #[serde(default)]
            result: serde_json::Value,
            #[serde(default)]
            error: Option<String>,
        }

        let body = serde_json::json!({ "action": action, "params": params });
        let response: Response = self
            .write_json(&reqwest::Method::POST, &path, Some(&body), Retry::Once)
            .await
            .with_context(|| format!("running {action:?} on the plugin {key:?}"))?;
        if !response.success {
            bail!(
                "the plugin {key:?} refused {action:?}: {}",
                crate::redact::urls_to_hosts(
                    response
                        .error
                        .unwrap_or_else(|| "no reason given".to_string())
                        .trim()
                )
            );
        }
        Ok(response.result)
    }
}

/// The run path for one plugin key.
///
/// The key is a single URL path segment upstream (`<str:key>`, sanitized when the plugin is
/// installed), so a key holding a slash or a query character would silently address a
/// different route. Rejecting it here keeps a typo from becoming a request to somewhere else.
fn plugin_run_path(key: &str) -> Result<String> {
    let valid = !key.is_empty()
        && key.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        });
    if !valid {
        bail!("{key:?} is not a valid plugin key (letters, digits, '-', '_' and '.' only)");
    }
    Ok(format!("/plugins/plugins/{key}/run/"))
}
