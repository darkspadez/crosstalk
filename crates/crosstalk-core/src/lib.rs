//! Host services shared by every crosstalk sub-app.
//!
//! The split is deliberate: a sub-app decides WHAT to match and what to do with the result,
//! while the host owns everything with a connection, a file or a thread in it. That is what
//! lets two sub-apps share one Dispatcharr session, one AI provider table and one run store
//! without either knowing about the other.

pub mod config;
pub mod jobs;
pub mod subapp;

pub use config::{ConfigReport, CrosstalkConfig};
pub use jobs::{JobContext, ProgressEvent, ProgressSink};
pub use subapp::{
    ActionKind, ActionSpec, FieldKind, FieldSpec, FieldValue, JobOutcome, Registry, SettingsSchema,
    SettingsValues, SubApp,
};

pub const HOST_NAME: &str = "crosstalk";
pub const HOST_VERSION: &str = env!("CARGO_PKG_VERSION");
