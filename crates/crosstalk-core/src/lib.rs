//! Host services shared by every crosstalk sub-app.
//!
//! The split is deliberate: a sub-app decides WHAT to match and what to do with the result,
//! while the host owns everything with a connection, a file or a thread in it. That is what
//! lets two sub-apps share one Dispatcharr session, one AI provider table and one run store
//! without either knowing about the other.

pub mod ai;
pub mod config;
pub mod csv_report;
pub mod dispatcharr;
pub mod jobs;
pub mod runner;
pub mod scheduler;
pub mod store;
pub mod subapp;
pub mod wildcard;

pub use ai::{AiService, Answer, Decider, DecisionRequest, DecisionResponse, Question, Verdict};
pub use config::{ConfigReport, CrosstalkConfig};
pub use csv_report::{
    exports_to_delete, is_our_export, preamble, prune_exports, report_path, write_report,
    ExportFile,
};
pub use dispatcharr::{
    Channel, DispatcharrClient, EpgEntry, EpgSource, HasPrograms, ProgramWindow, World,
};
pub use jobs::{JobContext, ProgressEvent, ProgressSink};
pub use runner::{ActiveRun, JobRunner, StartedRun};
pub use scheduler::{ScheduleInfo, Scheduler};
pub use store::{AiDecision, RunRecord, RunRow, RunStatus, Store};
pub use subapp::{
    ActionKind, ActionSpec, FieldKind, FieldSpec, FieldValue, JobOutcome, Registry, SettingsSchema,
    SettingsValues, SubApp,
};
pub use wildcard::{expand_patterns, parse_tokens};

pub const HOST_NAME: &str = "crosstalk";
pub const HOST_VERSION: &str = env!("CARGO_PKG_VERSION");
