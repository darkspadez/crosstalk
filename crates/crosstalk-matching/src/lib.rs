//! Pure, IO-free channel-name matching.
//!
//! This crate is a faithful port of EPG Janitor's `fuzzy_matcher.py` and
//! `matching_core.py`. It holds no state beyond configuration and caches, performs
//! no IO, and depends only on the string it is given. Every regex table, substitution
//! order and guard is reproduced deliberately: parity with the Python implementation is
//! the contract, and deviations must be opt-in through `MatchOptions`.

pub mod alias;
pub mod callsign;
pub mod data;
pub mod normalize;
pub mod pipeline;
pub mod profile;
pub mod pyre;
pub mod similarity;
pub mod tokens;

pub use normalize::{NormalizeFlags, Normalizer};
pub use profile::{MatchProfile, MatcherConfig};

/// The fuzzy-matching floor used by Janitor (`FUZZY_MATCH_THRESHOLD` in `plugin.py`).
pub const FUZZY_MATCH_THRESHOLD: u8 = 85;

pub use pipeline::{CandidateIndex, MatchMethod, MatchOptions, Matcher, Ranked, Scored};
