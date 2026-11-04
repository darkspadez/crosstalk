//! crosstalk: the host daemon for Dispatcharr tools, with `guide` as its first sub-app.
//!
//! Subcommands:
//!   * `serve`   — run the daemon (REST API, scheduler, web UI)
//!   * `run`     — run one sub-app action once, without the daemon
//!   * `validate` — check configuration against Dispatcharr
//!   * `exports clear` — delete the CSV reports the sub-apps wrote
//!   * `simulate-streams` — offline stream-to-channel match simulation (the parity oracle)

mod registry;
mod simulate;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use crosstalk_core::config::{CrosstalkConfig, SharedConfig};
use crosstalk_core::runner::JobRunner;
use crosstalk_core::store::Store;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "crosstalk",
    version,
    about = "Host daemon for Dispatcharr tools",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the daemon: REST API, scheduler and web UI.
    Serve(ServeArgs),
    /// Run one sub-app action once and exit.
    Run(RunArgs),
    /// Validate the configuration files and each sub-app's settings.
    Validate(ConfigArgs),
    /// Manage the CSV reports the sub-apps write.
    Exports(ExportsArgs),
    /// Offline stream-to-channel match simulation.
    #[command(name = "simulate-streams")]
    SimulateStreams(SimulateArgs),
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Directory holding crosstalk.toml and the per-app config files.
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct RunArgs {
    /// Sub-app id, for example `guide`.
    app: String,
    /// Action id, for example `match_apply`.
    action: String,
    /// Directory holding the configuration files.
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
    /// Change nothing; write the report only. `--preview` is accepted as a synonym.
    #[arg(long = "dry-run", alias = "preview")]
    dry_run: bool,
    /// Confirm an action that changes data. Without it, such an action is refused.
    #[arg(long, short = 'y')]
    yes: bool,
    /// Ignore and discard the progress a paused run saved, and start the backlog over.
    ///
    /// The default is to resume: a windowed job checkspoints where it stopped, so the next run
    /// continues. This is how an operator says "no, start from the beginning" — after changing
    /// the scope, for instance.
    #[arg(long)]
    fresh: bool,
}

#[derive(Args, Debug)]
struct ConfigArgs {
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct ExportsArgs {
    #[command(subcommand)]
    command: ExportsCommand,
}

#[derive(Subcommand, Debug)]
enum ExportsCommand {
    /// Delete the CSV reports the sub-apps wrote. Other files in the directory are untouched.
    Clear(ExportsClearArgs),
}

#[derive(Args, Debug)]
struct ExportsClearArgs {
    /// Directory holding the configuration files; the exports directory comes from it.
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
    /// Only this sub-app's reports, for example `guide`. Default: every sub-app's.
    #[arg(long)]
    app: Option<String>,
    /// Confirm the deletion. Without it, the command lists what it would delete and stops.
    #[arg(long, short = 'y')]
    yes: bool,
}

#[derive(Args, Debug)]
struct SimulateArgs {
    /// Channels CSV, for example a guide export.
    #[arg(long)]
    channels: PathBuf,
    /// Provider M3U.
    #[arg(long)]
    streams: PathBuf,
    /// Score a candidate must reach to count as passed.
    #[arg(long, default_value_t = 80)]
    threshold: u8,
    /// Rows written per stream to top_n.csv.
    #[arg(long = "top-n", default_value_t = 5)]
    top_n: usize,
    /// Width of the low-confidence band below the threshold.
    #[arg(long, default_value_t = 15)]
    margin: u8,
    /// Name of the channel-name column, when it cannot be auto-detected.
    #[arg(long = "channel-col")]
    channel_col: Option<String>,
    /// Only keep M3U entries whose group title matches this regex.
    #[arg(long = "group-filter")]
    group_filter: Option<String>,
    /// JSON file of custom aliases, merged over the built-in table.
    #[arg(long = "custom-aliases")]
    custom_aliases: Option<String>,
    /// Comma-separated country codes whose channel databases to load for callsigns.
    #[arg(long)]
    countries: Option<String>,
    /// Directory to write the report files into.
    #[arg(long, default_value = "tools/sim-out")]
    out: PathBuf,
    /// Do not strip quality tags before comparing.
    #[arg(long = "no-ignore-quality")]
    no_ignore_quality: bool,
    /// Do not strip regional tags before comparing.
    #[arg(long = "no-ignore-regional")]
    no_ignore_regional: bool,
    /// Do not strip geographic prefixes before comparing.
    #[arg(long = "no-ignore-geographic")]
    no_ignore_geographic: bool,
    /// Do not strip miscellaneous parenthesised tags before comparing.
    #[arg(long = "no-ignore-misc")]
    no_ignore_misc: bool,
}

fn main() -> Result<()> {
    install_logging();
    let cli = Cli::parse();

    match cli.command {
        Command::SimulateStreams(args) => {
            let tags = [
                !args.no_ignore_quality,
                !args.no_ignore_regional,
                !args.no_ignore_geographic,
                !args.no_ignore_misc,
            ];
            let options = simulate::SimulateOptions {
                channels: args.channels,
                streams: args.streams,
                threshold: args.threshold,
                top_n: args.top_n,
                margin: args.margin,
                channel_col: args.channel_col,
                group_filter: args.group_filter,
                custom_aliases: args.custom_aliases,
                countries: args.countries,
                out: args.out,
                tags,
            };
            match simulate::run(&options) {
                Ok(out) => {
                    println!("reports written to {}", simulate::display(&out));
                    Ok(())
                }
                Err(error) => {
                    // The Python tool exits 2 for every handled error.
                    eprintln!("error: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        Command::Serve(args) => serve(args),
        Command::Run(args) => run_once(args),
        Command::Validate(args) => validate(args),
        Command::Exports(args) => match args.command {
            ExportsCommand::Clear(args) => clear_exports(args),
        },
    }
}

fn serve(args: ServeArgs) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new().context("starting the async runtime")?;
    runtime.block_on(async move {
        // The live config: the runner, the scheduler and the web UI share it, and a host
        // settings save swaps it without a restart. Runs snapshot it at start, so a save
        // mid-run reaches the next run, never the running one.
        let config = SharedConfig::new(CrosstalkConfig::load(&args.config)?);
        let snapshot = config.snapshot();

        // Refuse to expose an unauthenticated listener: an apply action can rewrite a guide.
        let token = snapshot.server.resolved_token();
        if crosstalk_web::auth::requires_token(&snapshot.server.bind, token.as_deref()) {
            anyhow::bail!(
                "server.bind is {} with no token: set server.token or server.token_env before \
                 exposing the daemon. Anyone who can reach the port could run apply actions.",
                snapshot.server.bind
            );
        }

        let store = Store::open(snapshot.data_dir().join("crosstalk.db"))?;
        let registry = registry::builtin();
        let runner = JobRunner::new(registry.clone(), config.clone(), store.clone());

        // ONE runner: the scheduler arms jobs on the same instance the UI talks to, or a
        // scheduled run would be invisible to the UI and could race a UI run on the same app.
        // Behind a mutex shared with the web UI, so a settings save can re-arm schedules.
        let scheduler = Arc::new(tokio::sync::Mutex::new(
            crosstalk_core::scheduler::Scheduler::start(runner.clone(), &snapshot).await?,
        ));

        let state = crosstalk_web::AppState::new(registry, config, runner.clone(), store)
            .with_scheduler(Arc::clone(&scheduler));
        let app = crosstalk_web::router(state);

        let address = format!("{}:{}", snapshot.server.bind, snapshot.server.port);
        let listener = tokio::net::TcpListener::bind(&address)
            .await
            .with_context(|| format!("binding {address}"))?;

        println!(
            "crosstalk {} listening on http://{address}",
            crosstalk_core::HOST_VERSION
        );
        if token.is_none() {
            println!("no token configured: only a loopback listener may run without one");
        }
        for entry in crosstalk_core::scheduler::describe(&snapshot) {
            match &entry.next_run {
                Some(next) => println!("schedule {} next runs at {next}", entry.key),
                None => println!("schedule {} is not valid and will not run", entry.key),
            }
        }

        // With `ConnectInfo`: without it the login limiter's `ConnectInfo<SocketAddr>`
        // extractor is always `None` and every client shares the one fallback bucket, so ten
        // bad POSTs from anyone would lock out every browser login. Note the limit of this:
        // behind a reverse proxy every client arrives with the proxy's address, so the
        // limiter is effectively global there too — there is no trusted-proxy or
        // X-Forwarded-For support yet, so do not put a login-bearing listener behind one
        // expecting per-client limiting.
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving the daemon")?;

        // Stop the runs before the scheduler: cancelling first means no run dies
        // mid-`batch_set_epg` with the runtime torn down underneath it, and the drain
        // wait gives in-flight applies a chance to finish or cancel cleanly.
        runner.shutdown();
        wait_for_drain(&runner, std::time::Duration::from_secs(30)).await;

        scheduler.lock().await.shutdown().await;
        Ok(())
    })
}

/// Wait until no runs are in flight, or `timeout` elapses (logged, then shutdown proceeds).
///
/// The serve path calls this after [`JobRunner::shutdown`]: shutdown sets every cancel flag,
/// and this gives the runs a bounded window to observe it before the scheduler stops and the
/// runtime is torn down.
async fn wait_for_drain(runner: &JobRunner, timeout: std::time::Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !runner.running().is_empty() {
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                "shutting down with {} run(s) still active after {timeout:?}",
                runner.running().len()
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Install the log subscriber.
///
/// `RUST_LOG` sets the level (`info` by default, which is what an operator needs to see: the
/// port, the schedules, and anything a run went wrong on).
fn install_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| {
        EnvFilter::new(
            "info,crosstalk_core=info,crosstalk_web=info,crosstalk_guide=info,\
             crosstalk_streams=info",
        )
    });
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

/// Wait for Ctrl-C or SIGTERM, so a container stop is a clean shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("installing the Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing the SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    println!("shutting down");
}

fn run_once(args: RunArgs) -> Result<()> {
    let registry = registry::builtin();
    let config = CrosstalkConfig::load(&args.config)?;
    let (_, action) = registry
        .find_action(&args.app, &args.action)
        .with_context(|| {
            format!(
                "no action {:?} in sub-app {:?}; known apps: {}",
                args.action,
                args.app,
                registry
                    .iter()
                    .map(|app| app.id())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    // An apply action run with --dry-run may not honour the flag: it is a different entry
    // point. Redirect it to the preview action instead of trusting it to behave.
    let action = if args.dry_run {
        match preview_counterpart(action.id) {
            Some(preview_id) => registry
                .find_action(&args.app, preview_id)
                .map(|(_, preview)| preview)
                .unwrap_or(action),
            None => action,
        }
    } else {
        action
    };

    // `--dry-run` stands in for `--yes`: a dry run changes nothing, which is exactly what the
    // refusal's own wording offers ("or --dry-run to change nothing"). It matters for the one
    // writing action that is not a pair — `streams.probe`, whose `--dry-run` records health and
    // writes no stream stats — because there is no preview action to redirect it to.
    if action.kind.requires_confirmation() && !args.yes && !args.dry_run {
        // A one-shot run has no dialog to confirm in, so the flag has to stand in for it.
        println!(
            "{}\n{}",
            action.label,
            action.confirm.unwrap_or("This action changes data.")
        );
        anyhow::bail!(
            "refusing to run {:?} without --yes: it changes data. Add --yes to proceed, or \
             --dry-run to change nothing",
            action.id
        );
    }

    // Only the action's own id and dry-run flag matter from here; the run is recorded, so a
    // one-shot run shows up in the history and — for `scan` — leaves the result set the cleanup
    // actions select from. Without a store, `crosstalk run guide scan` followed by
    // `crosstalk run guide clear_missing_apply --yes` could never work.
    //
    // `open_shared`, never `open`: this is a second process on the daemon's file, and a plain
    // open would retire the daemon's live `running` rows out from under it. The store is
    // shared (WAL + busy_timeout), but the one-run-per-app guard is per process, so a live
    // row for this app here means the daemon (or another CLI) is mid-run: refuse rather
    // than run the same app twice. The reserve below enforces the same rule atomically for
    // the race this check cannot see.
    let store = Store::open_shared(config.data_dir().join("crosstalk.db"))?;
    let live = store.running_runs(&args.app)?;
    if let Some(current) = live.first() {
        anyhow::bail!(
            "{} already has {} #{} in progress (started {}); wait for it to finish or cancel \
             it. If no daemon is running, this row is stale from a crash and clears when the \
             daemon next starts.",
            args.app,
            current.action,
            current.id,
            current.started_at,
        );
    }
    let runner = JobRunner::new(registry, SharedConfig::new(config), store);
    let runtime = tokio::runtime::Runtime::new().context("starting the async runtime")?;
    let options = if args.fresh {
        crosstalk_core::runner::RunOptions::default().fresh()
    } else {
        crosstalk_core::runner::RunOptions::default()
    };
    let outcome = runtime.block_on(runner.run_to_completion_with(
        &args.app,
        action.id,
        args.dry_run,
        options,
    ))?;

    println!("{}", outcome.summary);
    if outcome.is_paused() {
        // A one-shot run has no deadline, so this cannot happen today; say so loudly rather
        // than reporting a partial run as a finished one if that ever changes.
        println!("  paused: this run stopped at its window's end and will resume");
    }
    for (key, value) in &outcome.counts {
        println!("  {key}: {value}");
    }
    if let Some(path) = &outcome.report_path {
        println!("  report: {}", path.display());
    }
    Ok(())
}

/// The preview action that stands in for an apply action.
///
/// Every writing action is a `<verb>_preview` / `<verb>_apply` pair, so `--dry-run` on an apply
/// is redirected to its preview rather than trusting the apply to honour the flag.
fn preview_counterpart(action: &str) -> Option<&'static str> {
    match action {
        "match_apply" => Some("match_preview"),
        "heal_apply" => Some("heal_preview"),
        "tag_missing_apply" => Some("tag_missing_preview"),
        "clear_missing_apply" => Some("clear_missing_preview"),
        "clear_hidden_apply" => Some("clear_hidden_preview"),
        "clear_by_regex_apply" => Some("clear_by_regex_preview"),
        "clear_groups_apply" => Some("clear_groups_preview"),
        "watchdog_apply" => Some("watchdog_preview"),
        "sort_apply" => Some("sort_preview"),
        "mirror_apply" => Some("mirror_preview"),
        "visibility_apply" => Some("visibility_preview"),
        "triage_apply" => Some("triage_preview"),
        "restore_apply" => Some("restore_preview"),
        _ => None,
    }
}

/// `exports clear`: the host feature that replaced each plugin's `clear_csv_exports`.
///
/// Only the registered sub-apps' own files are considered (by their export prefix), so a
/// directory shared with the plugins loses nothing that is not ours. Reports written in the
/// last couple of minutes are kept: a daemon run may be holding one, and this command cannot
/// ask the daemon — opening the run store would retire its in-flight runs.
fn clear_exports(args: ExportsClearArgs) -> Result<()> {
    let config = CrosstalkConfig::load(&args.config)?;
    let registry = registry::builtin();
    let all_prefixes: Vec<String> = registry.iter().map(|app| app.export_prefix()).collect();
    // The single-app filter is ownership against the whole registry, not a lone prefix
    // match: a nested id's files (a future `guide_extra_*` alongside `guide_*`) start with
    // both prefixes, and belong to the longest match.
    let wanted: Option<String> = match &args.app {
        Some(id) => {
            let app = registry.get(id).with_context(|| {
                format!(
                    "no sub-app {id:?}; known apps: {}",
                    registry
                        .iter()
                        .map(|app| app.id())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;
            Some(app.export_prefix())
        }
        None => None,
    };

    let directory = config.exports_dir();
    println!("exports: {}", directory.display());
    if !args.yes {
        // A one-shot command has no dialog to confirm in, so the flag stands in for it.
        let listed = crosstalk_core::csv_report::app_exports(&directory, &all_prefixes);
        let count = match &wanted {
            Some(prefix) => listed
                .iter()
                .filter(|file| {
                    crosstalk_core::csv_report::owning_prefix(&file.name, &all_prefixes)
                        == Some(prefix.as_str())
                })
                .count(),
            None => listed.len(),
        };
        anyhow::bail!(
            "refusing to delete {count} report file(s) without --yes. Add --yes to proceed."
        );
    }

    let summary = match &wanted {
        Some(prefix) => {
            crosstalk_core::csv_report::clear_app_exports(&directory, &all_prefixes, prefix)
        }
        None => crosstalk_core::csv_report::clear_exports(&directory, &all_prefixes),
    };
    println!("deleted: {}", summary.deleted);
    if summary.kept_recent > 0 {
        println!(
            "kept: {} written in the last {} minutes, in case a run is still using them",
            summary.kept_recent,
            crosstalk_core::csv_report::CLEAR_GRACE.as_secs() / 60
        );
    }
    if summary.failed > 0 {
        println!("failed: {} (the log says why)", summary.failed);
    }
    Ok(())
}

fn validate(args: ConfigArgs) -> Result<()> {
    let config = CrosstalkConfig::load(&args.config)?;
    let registry = registry::builtin();

    let report = config.validate();
    let mut failed = false;

    println!("configuration: {}", args.config.display());
    println!(
        "dispatcharr: {} (credentials {})",
        config.dispatcharr.api_root(),
        if config.dispatcharr.has_credentials() {
            "configured"
        } else {
            "MISSING"
        }
    );
    println!(
        "matching: databases {}, quality={} regional={} geographic={} misc={}",
        config.matching.databases().join(","),
        config.matching.ignore_quality_tags,
        config.matching.ignore_regional_tags,
        config.matching.ignore_geographic_tags,
        config.matching.ignore_misc_tags
    );
    println!("threads: {}", config.jobs.resolved_worker_threads());
    // The shared scope and export settings: they decide which channels a run may touch and
    // how much history it leaves behind, so `validate` should show them without opening the
    // file.
    println!(
        "scope: profiles [{}], groups [{}], ignore groups [{}]",
        config.scope.profiles, config.scope.groups, config.scope.ignore_groups
    );
    println!(
        "exports: kept {} day(s), 0 means every report",
        config.exports.retention_days
    );

    for warning in &report.warnings {
        println!("warning: {warning}");
    }
    for error in &report.errors {
        println!("error: {error}");
        failed = true;
    }

    for key in registry.unknown_schedules(&config) {
        println!("error: schedule {key:?} does not name a known <app>.<action>");
        failed = true;
    }
    for entry in crosstalk_core::scheduler::describe(&config) {
        // The window is what says whether the job finishes in one firing: a schedule whose
        // window is shorter than its work pauses every time and continues, which is a very
        // different thing from one that runs to the end.
        match &entry.window {
            Some(window) => println!("schedule {}: window {window}", entry.key),
            None => println!("schedule {}: no window (runs to the end)", entry.key),
        }
    }

    for app in registry.iter() {
        let path = app.settings_path(&config.config_dir);
        match app.validate_settings(&path) {
            Ok(()) => println!("app {}: settings valid ({})", app.id(), path.display()),
            Err(error) => {
                println!("error: app {} settings: {error:#}", app.id());
                failed = true;
            }
        }
    }

    if failed {
        anyhow::bail!("configuration has errors");
    }
    println!("ok");
    Ok(())
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    fn idle_runner() -> JobRunner {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = SharedConfig::new(CrosstalkConfig::load(dir.path()).expect("config"));
        let store = Store::in_memory().expect("store");
        JobRunner::new(registry::builtin(), config, store)
    }

    #[tokio::test]
    async fn drain_returns_immediately_when_nothing_runs() {
        let runner = idle_runner();
        assert!(runner.running().is_empty());
        // Would hang until the timeout if the loop condition were inverted.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            wait_for_drain(&runner, std::time::Duration::from_secs(30)),
        )
        .await
        .expect("an idle runner drains at once");
    }

    #[tokio::test]
    async fn shutdown_then_drain_refuses_new_runs() {
        let runner = idle_runner();
        runner.shutdown();
        wait_for_drain(&runner, std::time::Duration::from_secs(5)).await;
        assert!(runner.start("guide", "match_preview", true).is_err());
    }
}

#[cfg(test)]
mod cli_tests {
    use super::*;
    use std::path::Path;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("the command line parses")
    }

    fn clear_args(args: &[&str]) -> ExportsClearArgs {
        match parse(args).command {
            Command::Exports(ExportsArgs {
                command: ExportsCommand::Clear(args),
            }) => args,
            other => panic!("expected an exports clear command, got {other:?}"),
        }
    }

    #[test]
    fn exports_clear_takes_a_config_dir_an_app_and_yes() {
        let args = clear_args(&[
            "crosstalk",
            "exports",
            "clear",
            "--config",
            "/etc/crosstalk",
        ]);
        assert_eq!(args.config, Path::new("/etc/crosstalk"));
        assert_eq!(args.app, None);
        assert!(!args.yes, "a deletion is never the default");

        let args = clear_args(&[
            "crosstalk",
            "exports",
            "clear",
            "--config",
            "/etc/crosstalk",
            "--app",
            "guide",
            "--yes",
        ]);
        assert_eq!(args.app.as_deref(), Some("guide"));
        assert!(args.yes);
    }

    #[test]
    fn exports_clear_defaults_to_the_same_config_dir_as_run() {
        // `run` and `serve` default to `config` and honour the environment; a destructive
        // command that pointed somewhere else would be a trap.
        let args = clear_args(&["crosstalk", "exports", "clear"]);
        assert_eq!(args.config, Path::new("config"));
    }

    #[test]
    fn exports_clear_needs_the_clear_subcommand() {
        assert!(Cli::try_parse_from(["crosstalk", "exports"]).is_err());
        assert!(Cli::try_parse_from(["crosstalk", "exports", "nope"]).is_err());
    }

    /// A config directory with an exports subdirectory holding a backdated report.
    fn config_dir_with_exports() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let exports = dir.path().join("exports");
        std::fs::create_dir_all(&exports).expect("exports");
        let report = exports.join("guide_match_preview_20250101_000000_000.csv");
        std::fs::write(&report, "x").expect("write");
        let file = std::fs::File::options()
            .write(true)
            .open(&report)
            .expect("open for time");
        file.set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(86_400))
            .expect("set mtime");
        (dir, report)
    }

    #[test]
    fn clearing_without_yes_refuses_and_deletes_nothing() {
        let (dir, report) = config_dir_with_exports();
        let error = clear_exports(ExportsClearArgs {
            config: dir.path().to_path_buf(),
            app: None,
            yes: false,
        })
        .expect_err("refuses");
        assert!(format!("{error}").contains("--yes"), "{error:#}");
        assert!(report.exists(), "nothing was deleted");
    }

    #[test]
    fn clearing_with_yes_deletes_only_the_registered_apps_reports() {
        let (dir, report) = config_dir_with_exports();
        let exports = dir.path().join("exports");
        let foreign = exports.join("lineuparr_export.csv");
        std::fs::write(&foreign, "x").expect("write");

        clear_exports(ExportsClearArgs {
            config: dir.path().to_path_buf(),
            app: Some("guide".to_string()),
            yes: true,
        })
        .expect("clears");

        assert!(!report.exists(), "the guide's own report is deleted");
        assert!(foreign.exists(), "another program's file is untouched");
    }

    #[test]
    fn clearing_names_an_unknown_app() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = clear_exports(ExportsClearArgs {
            config: dir.path().to_path_buf(),
            app: Some("nope".to_string()),
            yes: true,
        })
        .expect_err("fails");
        assert!(
            format!("{error}").contains("known apps: guide, streams"),
            "{error:#}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_apply_action_has_its_preview_counterpart() {
        // The naming rule every sub-app follows: an action that writes is `<verb>_preview` /
        // `<verb>_apply`. `--dry-run` on an apply is redirected through this map, so a pair the
        // map does not know would silently run the apply and ignore the flag.
        for app in registry::builtin().iter() {
            let ids: Vec<&str> = app.actions().iter().map(|action| action.id).collect();
            for id in ids.iter().copied() {
                let Some(verb) = id.strip_suffix("_apply") else {
                    continue;
                };
                let expected = format!("{verb}_preview");
                let found = preview_counterpart(id)
                    .unwrap_or_else(|| panic!("{id} has no preview counterpart"));
                assert_eq!(found, expected);
                assert!(
                    ids.contains(&found),
                    "{found} is not an action of {}",
                    app.id()
                );
            }
        }
    }

    #[test]
    fn a_scheduled_cleanup_apply_is_a_real_run_and_a_scheduled_scan_is_a_read() {
        // The scheduler derives the dry-run flag from the action's kind, so a cleanup apply is
        // armed as a write and the scan as a read. Nothing else has to list the new actions.
        let runner = JobRunner::new(
            registry::builtin(),
            SharedConfig::new(CrosstalkConfig::default()),
            Store::in_memory().expect("store"),
        );
        for action in [
            "tag_missing_apply",
            "clear_missing_apply",
            "clear_hidden_apply",
            "clear_by_regex_apply",
            "clear_groups_apply",
            "watchdog_apply",
            "match_apply",
            "heal_apply",
        ] {
            assert!(
                !runner.scheduled_dry_run("guide", action),
                "{action} must be armed as a write"
            );
        }
        for action in [
            "scan",
            "tag_missing_preview",
            "clear_missing_preview",
            "clear_hidden_preview",
            "clear_by_regex_preview",
            "clear_groups_preview",
            "watchdog_preview",
        ] {
            assert!(
                runner.scheduled_dry_run("guide", action),
                "{action} must be armed as a read"
            );
        }
    }

    #[test]
    fn a_dry_run_never_needs_yes() {
        // The refusal's own wording offers `--dry-run` as the alternative, and for the one
        // writing action that is not a preview/apply pair (`streams.probe`) it is the only way
        // to run it from the CLI without `--yes`: a dry run records health and writes nothing.
        let registry = registry::builtin();
        let (_, probe) = registry
            .find_action("streams", "probe")
            .expect("streams.probe is an action");
        assert!(probe.kind.requires_confirmation(), "it writes stream stats");
        // The redirect map has no entry for it, so the action itself receives the flag.
        assert!(preview_counterpart(probe.id).is_none());
    }

    #[test]
    fn a_preview_is_not_redirected_anywhere() {
        // Only the writing half of a pair redirects; a preview is already a read.
        for app in registry::builtin().iter() {
            for action in app.actions() {
                if action.id.ends_with("_preview") {
                    assert!(preview_counterpart(action.id).is_none(), "{}", action.id);
                }
            }
        }
    }
}
