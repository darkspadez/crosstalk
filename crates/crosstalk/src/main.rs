//! crosstalk: the host daemon for Dispatcharr tools, with `janitor` as its first sub-app.
//!
//! Subcommands:
//!   * `serve`   — run the daemon (REST API, scheduler, web UI)
//!   * `run`     — run one sub-app action once, without the daemon
//!   * `validate` — check configuration against Dispatcharr
//!   * `simulate` — offline stream-to-channel match simulation (the parity oracle)

mod simulate;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

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
    /// Validate configuration and Dispatcharr connectivity.
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
    /// Change nothing; write the report only.
    #[arg(long)]
    preview: bool,
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

fn serve(_args: ServeArgs) -> Result<()> {
    anyhow::bail!("the daemon is not built yet: this step of the rewrite covers the matcher only")
}

fn run_once(_args: RunArgs) -> Result<()> {
    anyhow::bail!("`crosstalk run` is not built yet: the daemon and sub-app registry come next")
}

fn validate(_args: ConfigArgs) -> Result<()> {
    anyhow::bail!("`crosstalk validate` is not built yet: it lands with the config loader")
}
