//! Pure, IO-free channel-name matching.
//!
//! This crate is a faithful port of the matcher both plugins ship: the vendored
//! `matching_core.py` every plugin subclasses, EPG Janitor's `fuzzy_matcher.py` (which
//! overrides normalisation and the callsign ladder), and Stream-Mapparr's
//! `fuzzy_matcher.py` + `country.py`. It holds no state beyond configuration and caches,
//! performs no IO, and depends only on the strings it is given. Every regex table,
//! substitution order and guard is reproduced deliberately: parity with the Python
//! implementation is the contract, and the one engine's plugin-to-plugin differences are
//! flags on [`NormalizeFlags`], [`MatchOptions`], [`profile::MatchProfile`] and the alias
//! tables — see [`profile`] for the function-by-function diff.

pub mod alias;
pub mod callsign;
pub mod country;
pub mod data;
pub mod engine;
pub mod identity;
pub mod normalize;
pub mod pipeline;
pub mod profile;
pub mod pyre;
pub mod similarity;
pub mod tags;
pub mod tokens;

pub use engine::{EngineConfig, UnifiedEngine, DEFAULT_THRESHOLD, ENGINE_VERSION};
pub use identity::{Identity, IdentityCache, ResolveContext};
pub use normalize::{NormalizeFlags, Normalizer};
pub use profile::{MatchProfile, MatcherConfig};

/// The fuzzy-matching floor used by Janitor (`FUZZY_MATCH_THRESHOLD` in `plugin.py`).
pub const FUZZY_MATCH_THRESHOLD: u8 = 85;

pub use pipeline::{BestMatch, CandidateIndex, MatchMethod, MatchOptions, Matcher, Ranked, Scored};
