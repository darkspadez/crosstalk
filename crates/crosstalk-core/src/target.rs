//! Something the host can run: one sub-app action, or a named workflow.
//!
//! Schedules, hooks and the CLI all name work the same way — `<app>.<action>`
//! (`streams.probe`), or `workflow.<name>` for a step sequence ([`crate::workflow`]) — so the
//! three of them share one parse, one dry-run rule and one way to start the work. Without this
//! each caller would re-invent "is this a workflow? which app does it hold?" and the answers
//! would drift.
//!
//! `workflow` is a reserved app id: nothing may be registered under it (see
//! [`crate::subapp::Registry`]), which is what makes the prefix unambiguous.

use crate::config::CrosstalkConfig;
use crate::runner::{JobRunner, RunOptions, StartedRun};
use crate::subapp::{JobOutcome, Registry};
use anyhow::{bail, Context, Result};

/// The app id that stands for "a workflow" in a target or a schedule key.
pub const WORKFLOW_APP: &str = "workflow";

/// One thing the host can run.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Target {
    /// A sub-app action, as `<app>.<action>`.
    Action { app: String, action: String },
    /// A named workflow from `[workflows.<name>]`.
    Workflow { name: String },
}

impl Target {
    /// Parse `workflow.<name>` or `<app>.<action>`.
    ///
    /// A step of a workflow is NOT a target: it is parsed by [`crate::workflow::Step`], which
    /// refuses `workflow.*` because a workflow never nests. Here the workflow form is the point.
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if let Some(name) = text.strip_prefix("workflow.") {
            if name.is_empty() || name.contains('.') {
                bail!(
                    "{text:?} must be workflow.<name> (one name, no dot in it) or \
                     <app>.<action>"
                );
            }
            return Ok(Target::Workflow {
                name: name.to_string(),
            });
        }
        let (app, action) = text
            .split_once('.')
            .with_context(|| format!("{text:?} must be <app>.<action> or workflow.<name>"))?;
        if app.is_empty() || action.is_empty() {
            bail!("{text:?} must be <app>.<action> or workflow.<name>");
        }
        Ok(Target::Action {
            app: app.to_string(),
            action: action.to_string(),
        })
    }

    /// The key an operator wrote (`streams.probe`, `workflow.nightly`).
    pub fn key(&self) -> String {
        match self {
            Target::Action { app, action } => format!("{app}.{action}"),
            Target::Workflow { name } => format!("{WORKFLOW_APP}.{name}"),
        }
    }

    /// The app whose one-run-per-app slot this target takes: the workflow slot for a workflow,
    /// which is why two workflows never interleave.
    pub fn app(&self) -> &str {
        match self {
            Target::Action { app, .. } => app,
            Target::Workflow { .. } => WORKFLOW_APP,
        }
    }

    /// The action id, or the workflow name. What the run row records as its action.
    pub fn action(&self) -> &str {
        match self {
            Target::Action { action, .. } => action,
            Target::Workflow { name } => name,
        }
    }

    pub fn is_workflow(&self) -> bool {
        matches!(self, Target::Workflow { .. })
    }

    /// Whether running this target would change nothing.
    ///
    /// Derived from the target's own kind, never from the caller's hope: a `Read` action is a
    /// dry run, a writing action is a real run, and a workflow is a dry run only when every one
    /// of its steps is. That is what lets a schedule naming a preview stay a preview and a hook
    /// naming an apply really write.
    pub fn dry_run(&self, registry: &Registry, config: &CrosstalkConfig) -> bool {
        match self {
            Target::Action { app, action } => match registry.find_action(app, action) {
                Some((_app, action)) => !action.kind.touches_channels(),
                // An unknown action is a configuration error `crosstalk validate` reports; a
                // dry run is the safe reading of it here.
                None => true,
            },
            Target::Workflow { name } => crate::workflow::scheduled_dry_run(registry, config, name),
        }
    }

    /// Whether an unknown app/action/workflow is the reason this target cannot run.
    ///
    /// Validation asks this so it can report a typo once, in the words the operator's file uses.
    pub fn exists(&self, registry: &Registry, config: &CrosstalkConfig) -> bool {
        match self {
            Target::Action { app, action } => registry.find_action(app, action).is_some(),
            Target::Workflow { name } => config.workflows.contains_key(name.as_str()),
        }
    }

    /// Start this target and return at once, like any other run.
    pub fn start(
        &self,
        runner: &JobRunner,
        dry_run: bool,
        options: RunOptions,
    ) -> Result<StartedRun> {
        match self {
            Target::Action { app, action } => runner.start_with(app, action, dry_run, options),
            Target::Workflow { name } => crate::workflow::start(runner, name, dry_run, options),
        }
    }

    /// Run this target to completion, for the one-shot CLI and tests.
    pub async fn run_to_completion(
        &self,
        runner: &JobRunner,
        dry_run: bool,
        options: RunOptions,
    ) -> Result<JobOutcome> {
        match self {
            Target::Action { app, action } => {
                runner
                    .run_to_completion_with(app, action, dry_run, options)
                    .await
            }
            Target::Workflow { name } => {
                crate::workflow::run_to_completion(runner, name, dry_run, options).await
            }
        }
    }

    /// The workflows a configuration defines, as targets, for a UI that lists them.
    pub fn workflows(config: &CrosstalkConfig) -> Vec<Target> {
        config
            .workflows
            .keys()
            .map(|name| Target::Workflow { name: name.clone() })
            .collect()
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_parses_into_an_action_or_a_workflow() {
        assert_eq!(
            Target::parse("streams.match_apply").expect("action"),
            Target::Action {
                app: "streams".to_string(),
                action: "match_apply".to_string()
            }
        );
        assert_eq!(
            Target::parse("workflow.nightly").expect("workflow"),
            Target::Workflow {
                name: "nightly".to_string()
            }
        );
        // Whitespace from a hand-edited file is not a typo.
        assert!(Target::parse(" streams.probe ").is_ok());
    }

    #[test]
    fn the_keys_read_back_as_they_were_written() {
        for key in ["streams.probe", "workflow.nightly", "guide.match_apply"] {
            assert_eq!(Target::parse(key).expect("parses").key(), key);
        }
    }

    #[test]
    fn a_malformed_key_is_refused_with_the_shape_it_wants() {
        for key in [
            "",
            "typo",
            "streams.",
            ".probe",
            "workflow.",
            "workflow.a.b",
        ] {
            let error = Target::parse(key).expect_err(key);
            assert!(
                format!("{error:#}").contains("<app>.<action>"),
                "{key}: {error:#}"
            );
        }
    }

    #[test]
    fn a_workflow_holds_the_workflow_slot_and_an_action_its_own_app() {
        let workflow = Target::parse("workflow.nightly").expect("parses");
        assert_eq!(workflow.app(), "workflow");
        assert_eq!(workflow.action(), "nightly");
        assert!(workflow.is_workflow());

        let action = Target::parse("streams.sort_apply").expect("parses");
        assert_eq!(action.app(), "streams");
        assert_eq!(action.action(), "sort_apply");
        assert!(!action.is_workflow());
    }

    #[test]
    fn a_configuration_lists_its_workflows_as_targets() {
        let mut config = CrosstalkConfig::default();
        config.workflows.insert(
            "nightly".to_string(),
            crate::config::WorkflowConfig {
                steps: vec!["streams.probe".into()],
                fresh: false,
            },
        );
        assert_eq!(
            Target::workflows(&config),
            vec![Target::Workflow {
                name: "nightly".to_string()
            }]
        );
    }
}
