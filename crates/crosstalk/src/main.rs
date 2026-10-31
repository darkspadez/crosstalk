//! crosstalk: the host daemon for Dispatcharr tools, with `janitor` as its first sub-app.
//!
//! Subcommands:
//!   * `serve`   — run the daemon (REST API, scheduler, web UI)
//!   * `run`     — run one sub-app action once, without the daemon
//!   * `validate` — check configuration against Dispatcharr
//!   * `simulate` — offline stream-to-channel match simulation (the parity oracle)

mod registry;
mod simulate;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::jobs::JobContext;
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
    /// Offline stream-to-channel match simulation.
    Simulate(SimulateArgs),
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Directory holding crosstalk.toml and the per-app config files.
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct RunArgs {
    /// Sub-app id, for example `janitor`.
    app: String,
    /// Action id, for example `automatch`.
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
}

#[derive(Args, Debug)]
struct ConfigArgs {
    #[arg(long, default_value = "config", env = "CROSSTALK_CONFIG_DIR")]
    config: PathBuf,
}

#[derive(Args, Debug)]
struct SimulateArgs {
    /// Channels CSV, for example a Janitor export.
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
        Command::Simulate(args) => {
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
    }
}

fn serve(args: ServeArgs) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new().context("starting the async runtime")?;
    runtime.block_on(async move {
        let config = Arc::new(CrosstalkConfig::load(&args.config)?);

        // Refuse to expose an unauthenticated listener: an apply action can rewrite a guide.
        let token = config.server.resolved_token();
        if crosstalk_web::auth::requires_token(&config.server.bind, token.as_deref()) {
            anyhow::bail!(
                "server.bind is {} with no token: set server.token or server.token_env before \
                 exposing the daemon. Anyone who can reach the port could run apply actions.",
                config.server.bind
            );
        }

        let store = Store::open(config.data_dir().join("crosstalk.db"))?;
        let registry = registry::builtin();
        let runner = JobRunner::new(registry.clone(), Arc::clone(&config), store.clone());

        // ONE runner: the scheduler arms jobs on the same instance the UI talks to, or a
        // scheduled run would be invisible to the UI and could race a UI run on the same app.
        let scheduler =
            crosstalk_core::scheduler::Scheduler::start(runner.clone(), &config).await?;

        let state = crosstalk_web::AppState::new(registry, Arc::clone(&config), runner, store);
        let app = crosstalk_web::router(state);

        let address = format!("{}:{}", config.server.bind, config.server.port);
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
        for entry in crosstalk_core::scheduler::describe(&config) {
            match &entry.next_run {
                Some(next) => println!("schedule {} next runs at {next}", entry.key),
                None => println!("schedule {} is not valid and will not run", entry.key),
            }
        }

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("serving the daemon")?;

        scheduler.shutdown().await;
        Ok(())
    })
}

/// Install the log subscriber.
///
/// `RUST_LOG` sets the level (`info` by default, which is what an operator needs to see: the
/// port, the schedules, and anything a run went wrong on).
fn install_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| {
        EnvFilter::new("info,crosstalk_core=info,crosstalk_web=info,janitor=info")
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
    let (app, action) = registry
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

    if action.kind.requires_confirmation() && !args.yes {
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

    // Only connect when the action needs Dispatcharr, so `validate` still works on a host
    // that cannot reach it yet.
    let context = JobContext::headless(config);
    let context = if action.needs_dispatcharr {
        context.connect()?
    } else {
        context
    };
    let runtime = tokio::runtime::Runtime::new().context("starting the async runtime")?;
    let outcome = runtime.block_on(app.run(action.id, &context, args.dry_run))?;

    println!("{}", outcome.summary);
    for (key, value) in &outcome.counts {
        println!("  {key}: {value}");
    }
    if let Some(path) = &outcome.report_path {
        println!("  report: {}", path.display());
    }
    Ok(())
}

/// The preview action that stands in for an apply action.
fn preview_counterpart(action: &str) -> Option<&'static str> {
    match action {
        "automatch_apply" => Some("automatch_preview"),
        "heal_apply" => Some("heal_preview"),
        _ => None,
    }
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
