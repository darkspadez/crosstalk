//! Host services shared by every crosstalk sub-app.
//!
//! The split is deliberate: a sub-app decides WHAT to match and what to do with the result,
//! while the host owns everything with a connection, a file or a thread in it. That is what
//! lets two sub-apps share one Dispatcharr session, one AI provider table and one run store
//! without either knowing about the other.

pub mod ai;
pub mod ai_gate;
pub mod channel_ops;
pub mod config;
pub mod csv_report;
pub mod dispatcharr;
pub mod engine_run;
pub mod fetch_cache;
pub mod hooks;
pub mod identity_cache;
pub mod jobs;
pub mod limiter;
pub mod logos;
pub mod match_run;
pub mod naming;
pub mod net;
pub mod quality;
pub mod runner;
pub mod scheduler;
pub mod scope;
pub mod status;
pub mod store;
pub mod subapp;
pub mod target;
pub mod undo;
pub mod wildcard;
pub mod window;
pub mod workflow;

#[cfg(test)]
mod test_app;

pub use ai::{AiService, Answer, Decider, DecisionRequest, DecisionResponse, Question, Verdict};
pub use channel_ops::{
    apply, apply_with_writer, parse_stream_ids, plan, plan_with_options, rows_for_field, run_rows,
    write_ops_report, ApplyReport, Change, Field, ListedStream, Op, OpKind, Plan, PlanOptions,
    RenameKind, ReportSpec, RestStreamListWriter, RowStatus, SelectedChannel, Selection, SetValue,
    Skip, StreamListWriter, Value,
};
pub use config::{
    ConfigReport, CrosstalkConfig, HooksConfig, ScheduleEntry, ScheduleTable, SharedConfig,
    WorkflowConfig,
};
pub use csv_report::{
    app_exports, clear_app_exports, clear_exports, exports_to_clear, exports_to_delete,
    grace_doomed, is_our_export, owning_prefix, preamble, prune_exports, report_path, safe_cell,
    write_report, ClearSummary, ExportFile, Report, CLEAR_GRACE,
};
pub use dispatcharr::{
    Channel, ChannelCreate, ChannelEdit, ChannelGroup, ChannelProfile, ChannelRegexRename,
    CurrentProgram, DispatcharrClient, EpgEntry, EpgHorizon, EpgRefresh, EpgSource, EpgSourceEdit,
    EpgSourceState, HasPrograms, Logo, M3uAccount, PluginActionInfo, PluginInfo, ProgramWindow,
    QueuedTask, Set, Stream, StreamsToChannels, World, WorldKind, WorldKinds,
};
pub use fetch_cache::{FetchCache, Fetched, DEFAULT_SIZE_CAP, TV_LOGOS_TTL};
pub use hooks::Hooks;
pub use identity_cache::CachedIdentities;
pub use jobs::{JobContext, ProgressEvent, ProgressSink};
pub use limiter::{Limiter, LimiterConfig, Permit};
pub use logos::{
    build_logo_url, country_dir_for, match_channel_to_logo, match_station_logo, network_logo,
    normalize_channel_name, normalize_logo_filename, parse_station_name, resolve_logo,
    search_network_logo, search_tv_logos, split_timeshift, LogoQuery, LogoSource, LogoSources,
    ResolvedLogo, TvHit, TvLogosIndex, TvScored, COUNTRY_DIR_MAP, LOGO_MATCH_THRESHOLD,
    STATION_SUBDIRS, TV_LOGOS_BRANCH, TV_LOGOS_REPO, TV_LOGOS_TREE_URL,
};
pub use naming::{
    apply_prefix, apply_suffix, ota_fields, parse_network_affiliation, render_channel, render_ota,
    resolve_collisions, ChannelValues, CollisionStrategy, OtaFields, PlannedName, ResolvedName,
    CHANNEL_PLACEHOLDERS, OTA_PLACEHOLDERS,
};
pub use quality::{stream_format_of, QualityTier, IC_FORMAT_LABELS};
pub use runner::{ActiveRun, JobRunner, RunOptions, StartedRun, WorkflowSlot};
pub use scheduler::{ScheduleInfo, Scheduler};
pub use store::{AiDecision, RunRecord, RunRow, RunStatus, Store, Subject};
pub use subapp::{
    ActionKind, ActionSpec, FieldKind, FieldSpec, FieldValue, JobOutcome, OutcomeBuilder,
    OutcomeStatus, Registry, SettingsSchema, SettingsValues, SubApp,
};
pub use target::Target;
pub use wildcard::{expand_patterns, parse_tokens};
pub use window::{End as WindowEnd, Window};

pub const HOST_NAME: &str = "crosstalk";
pub const HOST_VERSION: &str = env!("CARGO_PKG_VERSION");
