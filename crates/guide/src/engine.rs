//! Guide's view of the unified matching layer (phase 3 step 8, goal reset).
//!
//! Step 9 generalises the run engine into the host
//! ([`crosstalk_core::engine_run`]), used by every migrated app. This module keeps
//! guide's import paths stable: [`RunEngine`] and [`cut_setting`] are the host's.

pub use crosstalk_core::engine_run::{cut_setting, RunEngine};
