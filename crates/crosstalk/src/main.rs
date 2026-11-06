//! crosstalk: the host daemon for Dispatcharr tools, with `guide` as its first sub-app.
//!
//! Subcommands:
//!   * `serve`   — run the daemon (REST API, scheduler, web UI)
//!   * `run`     — run one sub-app action once, without the daemon
//!   * `validate` — check configuration against Dispatcharr
//!   * `exports clear` — delete the CSV reports the sub-apps wrote
//!   * `simulate-streams` — offline stream-to-channel match simulation (the unified engine;
//!     the Janitor oracle with `--features oracle --oracle janitor`)
//!   * `bench` — time the parts of a run, to decide whether REST is fast enough

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
    /// Undo a finished apply run: reverse the rows that are still in effect.
    Undo(UndoArgs),
    /// Time the parts of a sub-app's run: the plan phases, and the writes they would issue.
    Bench(BenchArgs),
    /// Offline stream-to-channel match simulation, on the unified engine.
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
    /// Sub-app id, for example `guide`, or `workflow` for a named workflow.
    app: String,
    /// Action id, for example `match_apply`, or the workflow's name after `workflow`.
    ///
    /// Omitted when the first argument already names a workflow: `run workflow.<name>`.
    action: Option<String>,
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
struct UndoArgs {
    /// The run to undo, for example `crosstalk undo 42`.
    run_id: i64,
    /// Change nothing; write the report only. `--preview` is accepted as a synonym.
    #[arg(long = "dry-run", alias = "preview")]
    dry_run: bool,
    /// Confirm an undo that changes data. Without it, an apply is refused.
    #[arg(long, short = 'y')]
    yes: bool,
    /// Directory holding the configuration files.
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct BenchArgs {
    #[command(subcommand)]
    target: BenchTarget,
}

#[derive(Subcommand, Debug)]
enum BenchTarget {
    /// Benchmark the streams sub-app: match, sort, mirror and visibility plans.
    Streams(StreamsBenchArgs),
}

/// One of `--synthetic` or `--live` must be chosen: a bench that silently measured nothing
/// would be worse than one that refuses.
#[derive(Args, Debug)]
struct StreamsBenchArgs {
    /// Directory holding the configuration files.
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
    /// Time a synthetic world built in memory. No network, no Dispatcharr.
    #[arg(long)]
    synthetic: bool,
    /// Time the plans against the Dispatcharr the configuration names. Read-only.
    #[arg(long)]
    live: bool,
    /// Synthetic channels to build (the plan's target size by default).
    #[arg(long, default_value_t = 20_000, requires = "synthetic")]
    channels: usize,
    /// Synthetic streams to build.
    #[arg(long, default_value_t = 50_000, requires = "synthetic")]
    streams: usize,
    /// Synthetic M3U accounts to build.
    #[arg(long, default_value_t = 100, requires = "synthetic")]
    accounts: usize,
    /// Read back the sampled channels' stream lists and rewrite them to that value, to time
    /// the two write patterns. Idempotent: nothing changes. Needs --yes.
    #[arg(long, requires = "live")]
    write_timing: bool,
    /// Channels to sample for --write-timing, comma-separated ids.
    #[arg(long = "channel-ids", requires = "write_timing", value_delimiter = ',')]
    channel_ids: Vec<i64>,
    /// Confirm the write timing's no-op rewrite.
    #[arg(long, short = 'y')]
    yes: bool,
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
    /// The decision cut: a stream passes as a member of the channel's best identity
    /// family at this score (the `[matching]` cut; 95 is `strict`, the default).
    #[arg(long, default_value_t = crosstalk_matching::DEFAULT_THRESHOLD)]
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
    /// Oracle builds only (`--features oracle`): run a retired plugin pipeline instead
    /// of the unified engine. `janitor` is EPG Janitor's `tools/match_sim.py`, ported
    /// byte for byte, for `scripts/sim_parity.sh`.
    #[cfg(feature = "oracle")]
    #[arg(long, value_parser = ["janitor"])]
    oracle: Option<String>,
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
                #[cfg(feature = "oracle")]
                matching: match args.oracle.as_deref() {
                    Some("janitor") => simulate::Matching::Janitor,
                    _ => simulate::Matching::Unified,
                },
                #[cfg(not(feature = "oracle"))]
                matching: simulate::Matching::Unified,
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
        Command::Bench(args) => match args.target {
            BenchTarget::Streams(args) => bench_streams(args),
        },
        Command::Validate(args) => validate(args),
        Command::Undo(args) => undo_once(args),
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
        // Workflows and hooks are configuration too, and a typo in either is silent until it
        // runs (or never does), so start-up says what the file actually describes.
        for workflow in crosstalk_core::workflow::describe(&snapshot) {
            match (&workflow.error, workflow.steps.is_empty()) {
                (Some(error), _) => println!("workflow {} will not run: {error}", workflow.name),
                (None, true) => println!("workflow {} has no steps", workflow.name),
                (None, false) => println!(
                    "workflow {}: {} step(s) — {}",
                    workflow.name,
                    workflow.steps.len(),
                    workflow.steps.join(" → ")
                ),
            }
        }
        for (event, targets) in crosstalk_core::hooks::describe(&snapshot.hooks) {
            println!(
                "hook {event} -> {} (debounced {} s)",
                if targets.is_empty() {
                    "nothing".to_string()
                } else {
                    targets.join(", ")
                },
                snapshot.hooks.debounce_secs
            );
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

/// The workflow a `run` invocation names, or `None` for an ordinary action run.
///
/// Two spellings, one target: `run workflow nightly` (the app-and-action shape) and
/// `run workflow.nightly` (the schedule-key shape, which is what a hook and a schedule write).
/// A bare `run <app>` with no second argument is refused here rather than reaching the action
/// lookup with a `None` that means nothing.
fn cli_workflow_name(args: &RunArgs) -> Result<Option<String>> {
    match (args.app.as_str(), args.action.as_deref()) {
        ("workflow", Some(name)) => Ok(Some(name.to_string())),
        ("workflow", None) => {
            anyhow::bail!("`run workflow` needs a workflow name: crosstalk run workflow <name>")
        }
        (key, None) => match key.strip_prefix("workflow.") {
            Some(name) if !name.is_empty() => Ok(Some(name.to_string())),
            _ => anyhow::bail!(
                "`run {key}` needs an action, for example `crosstalk run {key} <action>`, or \
                 name a workflow: `crosstalk run workflow.<name>`"
            ),
        },
        (_, Some(_)) => Ok(None),
    }
}

/// `run workflow <name>`: one workflow, once, without the daemon.
///
/// The same two refusals a writing action gets — `--yes` to confirm, and the cross-process
/// "already running" check — for the same reasons: a workflow writes and holds the `workflow`
/// slot, which the daemon's own workflow runs also hold.
fn run_workflow_once(args: RunArgs, name: &str) -> Result<()> {
    let registry = registry::builtin();
    let config = CrosstalkConfig::load(&args.config)?;
    let workflow = crosstalk_core::workflow::Workflow::from_config(&config, name)?;
    let writes = !crosstalk_core::workflow::scheduled_dry_run(&registry, &config, name);
    if writes && !args.yes && !args.dry_run {
        println!("workflow {name}: {} step(s)", workflow.steps.len());
        for step in workflow.labels() {
            println!("  {step}");
        }
        anyhow::bail!(
            "refusing to run workflow {name:?} without --yes: it changes data. Add --yes to \
             proceed, or --dry-run to run every writing step as its preview"
        );
    }

    let store = Store::open_shared(config.data_dir().join("crosstalk.db"))?;
    let live = store.running_runs(crosstalk_core::workflow::APP_ID)?;
    if let Some(current) = live.first() {
        anyhow::bail!(
            "a workflow is already running: workflow.{} #{} (started {}); wait for it to \
             finish or cancel it. If no daemon is running, this row is stale from a crash and \
             clears when the daemon next starts.",
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
    let outcome = runtime.block_on(crosstalk_core::workflow::run_to_completion(
        &runner,
        name,
        args.dry_run,
        options,
    ))?;

    println!("{}", outcome.summary);
    for (key, value) in &outcome.counts {
        println!("  {key}: {value}");
    }
    // The step runs, so a terminal operator can follow one to its report without the UI.
    if let Some(record) = runner
        .store()
        .recent_runs_for(crosstalk_core::workflow::APP_ID, 1)?
        .into_iter()
        .next()
    {
        println!("  workflow run: #{}", record.id);
        for step in runner.store().child_runs(record.id)? {
            println!(
                "    #{} {} {} — {}",
                step.id,
                if step.dry_run { "preview" } else { "apply" },
                step.action,
                step.summary.as_deref().unwrap_or(""),
            );
        }
    }
    Ok(())
}

fn run_once(args: RunArgs) -> Result<()> {
    // `run workflow <name>` and `run workflow.<name>` are the same target; both go through the
    // workflow executor, which records the workflow run and one run per step.
    if let Some(name) = cli_workflow_name(&args)? {
        return run_workflow_once(args, &name);
    }
    let Some(action_id) = args.action.clone() else {
        unreachable!("cli_workflow_name refuses a bare app id");
    };

    let registry = registry::builtin();
    let config = CrosstalkConfig::load(&args.config)?;
    let (_, action) = registry
        .find_action(&args.app, &action_id)
        .with_context(|| {
            format!(
                "no action {:?} in sub-app {:?}; known apps: {}",
                action_id,
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

/// `undo <run-id>`: reverse a finished apply run's rows that are still in effect.
///
/// A preview (`--dry-run`/`--preview`) plans and reports without writing; an apply needs
/// `--yes`, like every other write. The undo is itself a recorded, undoable run under the
/// target run's app with `parent_run_id` set to the undone run.
fn undo_once(args: UndoArgs) -> Result<()> {
    let config = CrosstalkConfig::load(&args.config)?;
    let registry = registry::builtin();

    // `open_shared`, never `open`: this is a second process on the daemon's file, and a plain
    // open would retire the daemon's live `running` rows out from under it (see `run_once`).
    let store = Store::open_shared(config.data_dir().join("crosstalk.db"))?;
    // The host's refusals first (a preview, a workflow run, a probe, a pruned run...): they
    // say why nothing can be undone, which beats asking for --yes on a run that can't be.
    let target = crosstalk_core::undo::check_target(&store, &registry, args.run_id)?;
    if !args.dry_run && !args.yes {
        println!(
            "undoing run #{} ({} {}) reverses its rows that are still in effect.",
            target.id, target.app, target.action
        );
        anyhow::bail!(
            "refusing to undo run #{} without --yes: it changes data. Add --yes to proceed, \
             or --dry-run to preview what it would reverse",
            args.run_id
        );
    }

    let live = store.running_runs(&target.app)?;
    if let Some(current) = live.first() {
        anyhow::bail!(
            "{} already has {} #{} in progress (started {}); wait for it to finish or cancel \
             it. If no daemon is running, this row is stale from a crash and clears when the \
             daemon next starts.",
            target.app,
            current.action,
            current.id,
            current.started_at,
        );
    }
    let runner = JobRunner::new(registry, SharedConfig::new(config), store);
    let runtime = tokio::runtime::Runtime::new().context("starting the async runtime")?;
    let outcome = runtime.block_on(runner.run_undo_to_completion_with(
        &target.app,
        args.run_id,
        args.dry_run,
        crosstalk_core::runner::RunOptions::default(),
    ))?;

    println!("{}", outcome.summary);
    for (key, value) in &outcome.counts {
        println!("  {key}: {value}");
    }
    if let Some(path) = &outcome.report_path {
        println!("  report: {}", path.display());
    }
    if args.dry_run {
        println!("preview only: rerun with --yes to apply the undo");
    }
    Ok(())
}

/// `bench streams`: the timing tool the plan promised, in two modes.
///
/// The prints are the report's own lines, so a test can assert on the same strings an
/// operator reads, and so the numbers a decision quotes are the ones this command wrote.
fn bench_streams(args: StreamsBenchArgs) -> Result<()> {
    if args.synthetic == args.live {
        anyhow::bail!(
            "choose exactly one mode: --synthetic times a world built in memory (no network), \
             --live times the Dispatcharr the configuration names"
        );
    }
    let config = CrosstalkConfig::load(&args.config)?;
    // The settings a run would use, so the numbers describe the configuration in front of the
    // operator and not a default nobody runs.
    let registry = registry::builtin();
    let app = registry
        .get(crosstalk_streams::APP_ID)
        .expect("the streams app is registered");
    let settings_path = app.settings_path(&config.config_dir);
    let settings = crosstalk_streams::settings::load(&settings_path)?;

    let mut report = if args.synthetic {
        let options = crosstalk_streams::bench::SyntheticOptions {
            channels: args.channels,
            streams: args.streams,
            accounts: args.accounts,
        };
        crosstalk_streams::bench::synthetic(&settings, &config.matching, &options)?
    } else {
        if args.write_timing && !args.yes {
            anyhow::bail!(
                "refusing --write-timing without --yes: it rewrites the sampled channels' \
                 stream lists (each read back just before, so nothing changes) to time \
                 the write path. Add --yes to proceed."
            );
        }
        let ids = if args.write_timing {
            if args.channel_ids.is_empty() {
                anyhow::bail!(
                    "--write-timing needs --channel-ids: name the channels to sample, for \
                     example --channel-ids 12,13,14"
                );
            }
            Some(args.channel_ids.as_slice())
        } else {
            None
        };
        let runtime = tokio::runtime::Runtime::new().context("starting the async runtime")?;
        runtime.block_on(crosstalk_streams::bench::live(&config, &settings, ids))?
    };
    report
        .notes
        .insert(0, format!("settings: {}", settings_path.display()));
    report.print();
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
        println!("error: schedule {key:?} does not name a known <app>.<action> or workflow.<name>");
        failed = true;
    }
    for (workflow, step) in registry.unknown_workflow_steps(&config) {
        println!("error: workflow {workflow:?} step {step:?} does not name a known <app>.<action>");
        failed = true;
    }
    for (event, target) in registry.unknown_hook_targets(&config) {
        println!("error: hooks.{event} target {target:?} does not name a known action or workflow");
        failed = true;
    }
    for workflow in crosstalk_core::workflow::describe(&config) {
        match (&workflow.error, workflow.steps.is_empty()) {
            (_, true) => println!("workflow {}: no steps", workflow.name),
            (Some(error), _) => println!("workflow {}: {error}", workflow.name),
            (None, _) => println!(
                "workflow {}: {} step(s){}",
                workflow.name,
                workflow.steps.len(),
                if workflow.fresh {
                    ", each new pass starts from the beginning"
                } else {
                    ""
                }
            ),
        }
    }
    for (event, targets) in crosstalk_core::hooks::describe(&config.hooks) {
        // A hook's pacing and targets are what decides whether a refresh storm becomes one run
        // or twenty, so `validate` says both rather than only that the table parsed.
        println!(
            "hook {event}: {} (debounced {} s)",
            if targets.is_empty() {
                "no targets".to_string()
            } else {
                targets.join(", ")
            },
            config.hooks.debounce_secs
        );
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

    fn run_args(args: &[&str]) -> RunArgs {
        match parse(args).command {
            Command::Run(args) => args,
            other => panic!("expected a run command, got {other:?}"),
        }
    }

    #[test]
    fn a_workflow_is_named_two_ways_and_an_action_one() {
        // `run workflow nightly` and `run workflow.nightly` are the same target; the CLI keeps
        // both spellings because the docs, the schedule keys and an operator's fingers disagree
        // about which is natural.
        let spaced = run_args(&["crosstalk", "run", "workflow", "nightly"]);
        assert_eq!(
            cli_workflow_name(&spaced).expect("parses"),
            Some("nightly".into())
        );

        let dotted = run_args(&["crosstalk", "run", "workflow.nightly"]);
        assert_eq!(
            cli_workflow_name(&dotted).expect("parses"),
            Some("nightly".into())
        );

        let action = run_args(&["crosstalk", "run", "guide", "match_preview"]);
        assert_eq!(cli_workflow_name(&action).expect("parses"), None);
    }

    #[test]
    fn a_bare_app_id_is_refused_with_the_shape_it_wants() {
        let bare = run_args(&["crosstalk", "run", "workflow"]);
        let error = cli_workflow_name(&bare).expect_err("refused");
        assert!(format!("{error}").contains("workflow <name>"), "{error}");

        let no_action = run_args(&["crosstalk", "run", "guide"]);
        let error = cli_workflow_name(&no_action).expect_err("refused");
        assert!(format!("{error}").contains("needs an action"), "{error}");
    }

    /// A config directory whose `crosstalk.toml` defines one workflow.
    ///
    /// The credentials are fake and never used: they are what makes the host configuration
    /// valid, so an offline step such as `streams.validate` — which reports configuration
    /// problems — has nothing to complain about.
    fn config_dir_with(steps: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let steps = steps
            .iter()
            .map(|step| format!("\"{step}\""))
            .collect::<Vec<_>>()
            .join(", ");
        std::fs::write(
            dir.path().join("crosstalk.toml"),
            format!(
                "[dispatcharr]\nbase_url = \"http://127.0.0.1:9191\"\napi_key = \"test\"\n\n\
                 [workflows.offline]\nsteps = [{steps}]\n"
            ),
        )
        .expect("write");
        dir
    }

    #[test]
    fn a_workflow_runs_once_from_the_cli() {
        // Both steps are offline, so this is a real end-to-end run without a Dispatcharr.
        let dir = config_dir_with(&["streams.validate", "streams.status"]);
        run_workflow_once(
            RunArgs {
                app: "workflow".to_string(),
                action: Some("offline".to_string()),
                config: dir.path().to_path_buf(),
                dry_run: false,
                yes: true,
                fresh: false,
            },
            "offline",
        )
        .expect("runs");

        let store =
            Store::open_shared(dir.path().join("data").join("crosstalk.db")).expect("store");
        let record = store
            .recent_runs_for(crosstalk_core::workflow::APP_ID, 1)
            .expect("runs")
            .into_iter()
            .next()
            .expect("a workflow run");
        assert_eq!(record.status, crosstalk_core::store::RunStatus::Succeeded);
        assert_eq!(store.child_runs(record.id).expect("steps").len(), 2);
    }

    #[test]
    fn a_writing_workflow_is_refused_without_yes() {
        // The same rule a writing action gets: a one-shot run has no dialog, so the flag stands
        // in for it — and nothing is recorded when it is missing.
        let dir = config_dir_with(&["streams.sort_apply"]);
        let error = run_workflow_once(
            RunArgs {
                app: "workflow".to_string(),
                action: Some("offline".to_string()),
                config: dir.path().to_path_buf(),
                dry_run: false,
                yes: false,
                fresh: false,
            },
            "offline",
        )
        .expect_err("refused");
        assert!(format!("{error}").contains("--yes"), "{error}");
        let store =
            Store::open_shared(dir.path().join("data").join("crosstalk.db")).expect("store");
        assert_eq!(store.run_count().expect("count"), 0, "nothing ran");
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

    fn bench_args(args: &[&str]) -> StreamsBenchArgs {
        match parse(args).command {
            Command::Bench(BenchArgs {
                target: BenchTarget::Streams(args),
            }) => args,
            other => panic!("expected a bench streams command, got {other:?}"),
        }
    }

    fn undo_args(args: &[&str]) -> UndoArgs {
        match parse(args).command {
            Command::Undo(args) => args,
            other => panic!("expected an undo command, got {other:?}"),
        }
    }

    #[test]
    fn undo_takes_a_run_id_and_defaults_to_applying() {
        // `crosstalk undo 42` reverses run 42; without --yes it is refused, like every
        // other write, and --preview/--dry-run previews instead.
        let args = undo_args(&["crosstalk", "undo", "42"]);
        assert_eq!(args.run_id, 42);
        assert!(!args.dry_run);
        assert!(!args.yes, "an undo is never the default");
        assert_eq!(args.config, Path::new("config"));

        let args = undo_args(&["crosstalk", "undo", "42", "--preview", "--yes"]);
        assert!(args.dry_run);
        let args = undo_args(&["crosstalk", "undo", "7", "--dry-run"]);
        assert!(args.dry_run);
        assert_eq!(args.run_id, 7);
    }

    #[test]
    fn undo_without_yes_is_refused_and_records_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("crosstalk.toml"), "").expect("seed");
        let seed = Store::open_shared(dir.path().join("data").join("crosstalk.db")).expect("store");
        let run_id = seed
            .start_run("guide", "match_apply", false)
            .expect("start");
        seed.finish_run(
            run_id,
            crosstalk_core::store::RunStatus::Succeeded,
            None,
            &serde_json::json!({}),
            None,
        )
        .expect("finish");
        seed.append_rows(
            run_id,
            &[crosstalk_core::store::RunRow {
                channel_id: 1,
                field: "epg_data_id".to_string(),
                before_value: Some("none".to_string()),
                after_value: Some("4".to_string()),
                status: "ok".to_string(),
                ..crosstalk_core::store::RunRow::default()
            }],
        )
        .expect("rows");
        let error = undo_once(UndoArgs {
            run_id,
            dry_run: false,
            yes: false,
            config: dir.path().to_path_buf(),
        })
        .expect_err("refused");
        assert!(format!("{error}").contains("--yes"), "{error}");
        assert_eq!(
            seed.run_count().expect("count"),
            1,
            "the target run is the only record: nothing started"
        );
    }

    #[test]
    fn undo_of_an_unknown_run_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("crosstalk.toml"), "").expect("seed");
        let error = undo_once(UndoArgs {
            run_id: 9999,
            dry_run: true,
            yes: true,
            config: dir.path().to_path_buf(),
        })
        .expect_err("fails");
        assert!(format!("{error}").contains("no run"), "{error:#}");
    }

    fn bare_bench_args(config: std::path::PathBuf) -> StreamsBenchArgs {
        StreamsBenchArgs {
            config,
            synthetic: false,
            live: false,
            channels: 20_000,
            streams: 50_000,
            accounts: 100,
            write_timing: false,
            channel_ids: Vec::new(),
            yes: false,
        }
    }

    #[test]
    fn bench_streams_takes_a_mode_and_the_synthetic_size() {
        let args = bench_args(&["crosstalk", "bench", "streams", "--synthetic"]);
        assert!(args.synthetic);
        assert!(!args.live);
        assert_eq!(args.channels, 20_000);
        assert_eq!(args.streams, 50_000);
        assert_eq!(args.accounts, 100);
        assert!(!args.write_timing, "a bench is read-only until asked");
        assert_eq!(args.config, Path::new("config"));

        let args = bench_args(&[
            "crosstalk",
            "bench",
            "streams",
            "--synthetic",
            "--channels",
            "500",
            "--streams",
            "1000",
            "--accounts",
            "5",
        ]);
        assert_eq!(args.channels, 500);
        assert_eq!(args.streams, 1_000);
        assert_eq!(args.accounts, 5);
    }

    #[test]
    fn the_synthetic_size_belongs_to_a_synthetic_run() {
        // A size on a live run would be read and silently ignored, which is how a report ends
        // up describing a world nobody measured.
        assert!(Cli::try_parse_from([
            "crosstalk",
            "bench",
            "streams",
            "--live",
            "--channels",
            "10"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "crosstalk",
            "bench",
            "streams",
            "--synthetic",
            "--channels",
            "10"
        ])
        .is_ok());
    }

    #[test]
    fn write_timing_needs_a_live_run_and_channel_ids() {
        assert!(Cli::try_parse_from([
            "crosstalk",
            "bench",
            "streams",
            "--synthetic",
            "--write-timing"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "crosstalk",
            "bench",
            "streams",
            "--live",
            "--channel-ids",
            "1"
        ])
        .is_err());
        let args = bench_args(&[
            "crosstalk",
            "bench",
            "streams",
            "--live",
            "--write-timing",
            "--channel-ids",
            "12,13",
            "--yes",
        ]);
        assert!(args.write_timing);
        assert!(args.yes);
        assert_eq!(args.channel_ids, vec![12, 13]);
    }

    #[test]
    fn a_bench_without_exactly_one_mode_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        for (synthetic, live) in [(false, false), (true, true)] {
            let error = bench_streams(StreamsBenchArgs {
                synthetic,
                live,
                ..bare_bench_args(dir.path().to_path_buf())
            })
            .expect_err("refused");
            assert!(format!("{error}").contains("exactly one mode"), "{error}");
        }
    }

    #[test]
    fn a_synthetic_bench_runs_end_to_end_from_the_cli() {
        // Every step is offline: this is a real CLI run against a world built in memory.
        let dir = tempfile::tempdir().expect("tempdir");
        bench_streams(StreamsBenchArgs {
            synthetic: true,
            channels: 20,
            streams: 100,
            accounts: 20,
            ..bare_bench_args(dir.path().to_path_buf())
        })
        .expect("runs");
    }

    #[test]
    fn write_timing_without_yes_is_refused_and_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = bench_streams(StreamsBenchArgs {
            live: true,
            write_timing: true,
            channel_ids: vec![1],
            ..bare_bench_args(dir.path().to_path_buf())
        })
        .expect_err("refused");
        assert!(format!("{error}").contains("--yes"), "{error}");
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

/// Cross-app consistency (phase 3 goal reset, step 9): one name must yield the same
/// identity and the same family in guide, streams and the shared cache, and match and
/// visibility must group the same channels together.
#[cfg(test)]
mod cross_app_consistency_tests {
    use super::Store;
    use crosstalk_matching::engine::UnifiedEngine;
    use crosstalk_matching::identity::ResolveContext;

    fn matching_config() -> crosstalk_core::config::MatchingConfig {
        crosstalk_core::config::MatchingConfig::default()
    }

    fn world(channels: &[&str], streams: &[&str]) -> crosstalk_core::World {
        use crosstalk_core::dispatcharr::{Channel, Stream, World};
        use serde_json::json;
        World {
            channels: channels
                .iter()
                .enumerate()
                .map(|(i, name)| Channel {
                    id: i as i64 + 1,
                    name: (*name).to_string(),
                    channel_number: Some(json!(i as i64 + 1)),
                    channel_group_id: Some(100),
                    streams: Vec::new(),
                    ..Channel::default()
                })
                .collect(),
            streams: streams
                .iter()
                .enumerate()
                .map(|(i, name)| Stream {
                    id: 20000 + i as i64,
                    name: (*name).to_string(),
                    m3u_account: Some(1),
                    channel_group: Some(100),
                    stream_stats: json!({"width": 1920, "height": 1080}),
                    ..Stream::default()
                })
                .collect(),
            groups: vec![crosstalk_core::dispatcharr::ChannelGroup {
                id: 100,
                name: "TVE".to_string(),
            }],
            m3u_accounts: vec![crosstalk_core::dispatcharr::M3uAccount {
                id: 1,
                name: "Main".to_string(),
                is_active: true,
                ..Default::default()
            }],
            ..World::default()
        }
    }

    #[test]
    fn one_name_resolves_to_one_identity_through_every_app_cache() {
        // Guide and streams share the host run engine and one store: the same input
        // resolves to the same identity no matter who asks, and the store row is one
        // version-keyed entry both apps read.
        let store = Store::in_memory().expect("store");
        let matching = matching_config();
        let guide_engine =
            crosstalk_core::engine_run::RunEngine::for_config(&matching, Some(store.clone()));
        let streams_engine =
            crosstalk_core::engine_run::RunEngine::for_config(&matching, Some(store.clone()));
        let ctx = ResolveContext::default();
        for name in ["US: ESPN HD", "BBC One", "Cinemax (WNCF)", "HBO West"] {
            assert_eq!(
                guide_engine.resolve(name, &ctx),
                streams_engine.resolve(name, &ctx),
                "{name}: guide and streams disagree"
            );
            assert_eq!(
                guide_engine.resolve(name, &ctx),
                crosstalk_matching::identity::resolve_with(
                    &UnifiedEngine::new(&matching.engine_config()),
                    name,
                    &ctx
                ),
                "{name}: the cache disagrees with a fresh resolve"
            );
        }
    }

    #[test]
    fn one_name_gets_the_same_epg_entry_family_and_stream_family() {
        // Identical candidate pools on both sides: guide's engine family over the
        // EPG-entry names and streams' attached stream family over the same names
        // must pick the same family (canonical names, in rank order).
        let matching = matching_config();
        let engine = UnifiedEngine::new(&matching.engine_config());
        let names: Vec<String> = ["ESPN", "ESPN HD", "ESPN 2", "CNN"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        let index = crosstalk_matching::engine::EngineIndex::build(
            &names,
            &engine,
            &ResolveContext::default(),
        );
        let guide_family: Vec<String> = engine
            .rank_index("US: ESPN HD", &index)
            .iter()
            .map(|member| names[member.index].clone())
            .collect();

        let world = world(&["US: ESPN HD"], &["ESPN", "ESPN HD", "ESPN 2", "CNN"]);
        let settings = crosstalk_streams::settings::StreamsSettings::default();
        let options = crosstalk_streams::matching::MatchOptions::new(&settings, &matching);
        let throughput = crosstalk_streams::quality::MemoryThroughputSource::new();
        let outcome = crosstalk_streams::matching::plan(
            &world,
            &crosstalk_core::config::ScopeConfig::default(),
            &settings,
            &options,
            &throughput,
        )
        .expect("plan");
        let by_name: std::collections::HashMap<i64, String> = world
            .streams
            .iter()
            .map(|stream| (stream.id, stream.name.clone()))
            .collect();
        let assignment = outcome
            .assignments
            .iter()
            .find(|row| row.channel_name == "US: ESPN HD")
            .expect("the channel is assigned");
        let stream_family: Vec<String> = assignment
            .after
            .iter()
            .map(|id| by_name[id].clone())
            .collect();
        assert_eq!(guide_family, stream_family, "guide and streams disagree");
    }

    #[test]
    fn match_and_visibility_group_the_same_channels_together() {
        // One grouping function serves both apps: the same inputs group the same
        // channels together in `match` and in `visibility`.
        let matching = matching_config();
        let world = world(
            &["ESPN", "ESPN HD", "CNN", "US: ESPN"],
            &["ESPN", "ESPN HD", "CNN"],
        );
        let settings = crosstalk_streams::settings::StreamsSettings::default();
        let options = crosstalk_streams::matching::MatchOptions::new(&settings, &matching);
        let throughput = crosstalk_streams::quality::MemoryThroughputSource::new();
        let outcome = crosstalk_streams::matching::plan(
            &world,
            &crosstalk_core::config::ScopeConfig::default(),
            &settings,
            &options,
            &throughput,
        )
        .expect("plan");
        let mut match_groups: Vec<Vec<i64>> = outcome
            .gate_groups
            .iter()
            .map(|group| {
                let mut members = group.member_ids.clone();
                members.sort_unstable();
                members
            })
            .collect();
        match_groups.sort();
        // Visibility groups through the same `build_groups`: rebuild it here over the
        // same channels and compare member sets.
        let engine = UnifiedEngine::new(&matching.engine_config());
        let mut groups = crosstalk_streams::matching::build_groups(
            &world.channels,
            &options.database,
            options.engine.normalizer(),
            &[],
            false,
            &std::collections::HashMap::new(),
            &engine,
        );
        for group in &mut groups {
            group.members.sort_unstable();
        }
        let mut members: Vec<Vec<i64>> = groups
            .iter()
            .map(|group| {
                group
                    .members
                    .iter()
                    .map(|position| world.channels[*position].id)
                    .collect()
            })
            .collect();
        for set in &mut members {
            set.sort_unstable();
        }
        members.sort();
        assert_eq!(match_groups, members, "match and visibility disagree");
    }
}
