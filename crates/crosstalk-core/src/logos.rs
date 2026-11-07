//! The unified logo resolver: one tier walk ([`resolve_logo`]) for every app.
//!
//! Phase 3 step 7. Both plugins fetch tv-logos over the network and match
//! filenames with a ratio; Lineuparr's assignment order is the decided default
//! ([`LogoSource::DEFAULT_ORDER`]; an operator's `logo_sources` reorders it through
//! [`LogoSource::parse_order`]):
//!
//! 1. the channel's EPG icon (it is already curated for that exact entry);
//! 2. the Logo Manager: a logo whose NAME matches the channel name exactly,
//!    case-insensitively (`existing_logos_by_name`, `plugin.py:3193`);
//! 3. tv-logos: a station-format file for OTA names (callsign + network,
//!    [`match_station_logo`]), else the best fuzzy filename match
//!    ([`match_channel_to_logo`]);
//! 4. the default logo (lineup's configured fallback).
//!
//! Channel-Mapparr's order differs (station → fuzzy → EPG → network, and it
//! only consults EPG/network logos for channels with NO logo yet); the unified
//! order is Lineuparr's 3-tier fallback plus the default, decided for step 7.
//! The tiers, their order, the country search ([`search_tv_logos`]) and the
//! default safety net live here only; creating `Logo` rows is lineup's job (step
//! 16), which resolves through this module.
//!
//! # The tv-logos index
//!
//! Both plugins fetched per-country file lists; the unified resolver fetches
//! ONE recursive git tree
//! (`https://api.github.com/repos/tv-logo/tv-logos/git/trees/main?recursive=1`)
//! through [`crate::fetch_cache::FetchCache`] (24 h TTL, ETag revalidation,
//! offline stale use). [`TvLogosIndex::from_tree`] parses that payload into
//! files per DIRECTORY PATH under `countries/` (`united-states`,
//! `united-states/us-local`, `nordic/norway`), so every file keeps the exact
//! path its raw URL needs. Country files are a directory's direct children;
//! station files are the [`STATION_SUBDIRS`] directory's (CM's
//! `TV_LOGOS_STATION_DIRS`). Parsing is pure and tested on an inline fixture;
//! the network is never touched in tests.
//!
//! Measured 2026-11-05: the tree is 10,966 entries / 3.4 MiB, `truncated:
//! false` (GitHub truncates past 100,000 entries or 7 MB). A truncated tree is
//! still parsed, flagged on [`TvLogosIndex::truncated`] and warned about: the
//! files it does list are real, and per-directory fetches would cost one
//! request per country against the 60/h anonymous limit.
//!
//! # Fuzzy matching
//!
//! The normalization is Channel-Mapparr's (the newer of the two ports: noise
//! suffixes stripped from BOTH sides, quality stripped channel-side only, `+1`
//! → `plus` for timeshifts). The SIMILARITY is the unified engine's
//! ([`crosstalk_matching::similarity::calculate_similarity_chars`]), not
//! rapidfuzz/thefuzz `ratio` both plugins used at threshold ≥ 85: one matcher
//! means one notion of "looks like". The threshold stays 85 on the 0–100
//! scale; scores near the boundary may differ from the plugins', and step 16's
//! oracle report carries the diff.

use crosstalk_matching::similarity::calculate_similarity_chars;
use std::collections::HashMap;

/// The tv-logos repo both plugins read.
pub const TV_LOGOS_REPO: &str = "tv-logo/tv-logos";
/// The branch both plugins read.
pub const TV_LOGOS_BRANCH: &str = "main";
/// The one recursive-tree request the index is built from.
pub const TV_LOGOS_TREE_URL: &str =
    "https://api.github.com/repos/tv-logo/tv-logos/git/trees/main?recursive=1";

/// Fuzzy filename threshold, on the 0–100 scale (both plugins' `≥ 85`).
pub const LOGO_MATCH_THRESHOLD: u8 = 85;

/// Country code → tv-logos directory: Channel-Mapparr's map PLUS Lineuparr's.
///
/// The union is CM's set: LU's twelve (`US CA GB AU DE FR IT ES MX BR IN IE`)
/// are all in CM's sixteen, and CM adds `UK NL NO PL` (`UK`/`GB` both name
/// `united-kingdom`). One fix over CM: Norway lives at `nordic/norway` in the
/// repo (CM's `norway` does not exist, so CM never found a Norwegian logo). A
/// code with no directory disables the tv-logos tier for that country.
pub const COUNTRY_DIR_MAP: [(&str, &str); 16] = [
    ("US", "united-states"),
    ("CA", "canada"),
    ("UK", "united-kingdom"),
    ("GB", "united-kingdom"),
    ("AU", "australia"),
    ("DE", "germany"),
    ("FR", "france"),
    ("IT", "italy"),
    ("ES", "spain"),
    ("MX", "mexico"),
    ("BR", "brazil"),
    ("IN", "india"),
    ("IE", "ireland"),
    ("NL", "netherlands"),
    ("NO", "nordic/norway"),
    ("PL", "poland"),
];

/// Country directories whose station logos live in a subfolder the top-level
/// list skips (CM's `TV_LOGOS_STATION_DIRS`).
pub const STATION_SUBDIRS: [(&str, &str); 1] = [("united-states", "united-states/us-local")];

/// The tv-logos directory for a country code (`GB` intentional alias).
pub fn country_dir_for(code: &str) -> Option<&'static str> {
    let upper = code.trim().to_uppercase();
    COUNTRY_DIR_MAP
        .iter()
        .find(|(cc, _)| *cc == upper)
        .map(|(_, dir)| *dir)
}

/// The filename suffix tv-logos uses for a country code (`cnn-us.png` → `us`).
///
/// The code lowercased, except `GB`: the repo names British files `-uk`, and
/// CM's `cc.lower()` left `-gb` unstripped (so `bbc-one-uk` compared as
/// `bbc one uk` against `BBC One`).
pub fn country_suffix_for(code: &str) -> String {
    let lower = code.trim().to_lowercase();
    if lower == "gb" {
        "uk".to_string()
    } else {
        lower
    }
}

/// Percent-encode one URL path segment (RFC 3986 unreserved bytes kept).
///
/// tv-logos has filenames like `100%-news-ua.png` and Cyrillic/Turkish names
/// (`trt-genç-tr.png`); both plugins pasted them raw into the URL, which breaks
/// on `%`, `#`, `?` and spaces.
fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// A raw `raw.githubusercontent.com` logo URL (both plugins' `build_logo_url`).
///
/// `dir` is a directory path under `countries/` (`united-states/us-local`);
/// every segment of it and the filename are percent-encoded.
pub fn build_logo_url(repo: &str, branch: &str, dir: &str, filename: &str) -> String {
    let path = dir
        .split('/')
        .chain(std::iter::once(filename))
        .filter(|segment| !segment.is_empty())
        .map(encode_segment)
        .collect::<Vec<_>>()
        .join("/");
    format!("https://raw.githubusercontent.com/{repo}/{branch}/countries/{path}")
}

/// Where a resolved logo came from: the tiers [`resolve_logo`] consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogoSource {
    EpgIcon,
    LogoManager,
    TvLogos,
    /// The station's network logo (Channel-Mapparr's `network_logo` fallback).
    Network,
    Default,
}

impl LogoSource {
    /// The unified consult order: Lineuparr's (decided in step 7) EPG icon, Logo
    /// Manager, tv-logos, then Channel-Mapparr's network-logo fallback, which CM also
    /// consults last (`_assign_tv_logos`: station, fuzzy, EPG icon, network). The
    /// default logo is the safety net after these.
    pub const DEFAULT_ORDER: [LogoSource; 4] = [
        LogoSource::EpgIcon,
        LogoSource::LogoManager,
        LogoSource::TvLogos,
        LogoSource::Network,
    ];

    pub fn label(self) -> &'static str {
        match self {
            LogoSource::EpgIcon => "epg_icon",
            LogoSource::LogoManager => "logo_manager",
            LogoSource::TvLogos => "tv_logos",
            LogoSource::Network => "network",
            LogoSource::Default => "default",
        }
    }

    /// Parse an operator's source order (comma or newline separated, case-insensitive):
    /// `epg`, `logo_manager`, `tv-logos`, `network`, `default` (and their `_`/`-`
    /// spellings).
    ///
    /// An unknown token is an error naming the valid ones; repeats keep the first
    /// position; empty means [`DEFAULT_ORDER`](Self::DEFAULT_ORDER).
    pub fn parse_order(raw: &str) -> anyhow::Result<Vec<LogoSource>> {
        let mut order: Vec<LogoSource> = Vec::new();
        for token in raw.split([',', '\n']) {
            let token = token.trim().to_lowercase();
            if token.is_empty() {
                continue;
            }
            let source = match token.as_str() {
                "epg" | "epg_icon" | "epg-icon" => LogoSource::EpgIcon,
                "logo_manager" | "logo-manager" | "manager" => LogoSource::LogoManager,
                "tv-logos" | "tv_logos" | "tvlogos" => LogoSource::TvLogos,
                "network" | "network_logo" | "network-logo" => LogoSource::Network,
                "default" => LogoSource::Default,
                other => anyhow::bail!(
                    "unknown logo source {other:?}; use epg, logo_manager, tv-logos, network \
                     or default"
                ),
            };
            if !order.contains(&source) {
                order.push(source);
            }
        }
        if order.is_empty() {
            order = Self::DEFAULT_ORDER.to_vec();
        }
        Ok(order)
    }
}

/// A resolved logo: the URL to assign, the tier that produced it, and the tv-logos
/// file detail when that tier did.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLogo {
    pub url: String,
    pub source: LogoSource,
    pub tv_hit: Option<TvHit>,
}

/// The parsed recursive-tree index: image filenames per directory path.
#[derive(Debug, Clone, Default)]
pub struct TvLogosIndex {
    /// Directory path under `countries/` (`united-states`, `united-states/us-local`,
    /// `nordic/norway`) → its DIRECT image children (`cnn-us.png`), sorted.
    pub files: HashMap<String, Vec<String>>,
    /// GitHub's `truncated` flag: the tree listed only part of the repo.
    pub truncated: bool,
}

fn is_image(name: &str) -> bool {
    let lower = name.to_lowercase();
    [".png", ".svg", ".jpg", ".jpeg", ".gif", ".webp"]
        .iter()
        .any(|ext| lower.ends_with(ext))
}

impl TvLogosIndex {
    /// Parse the GitHub recursive-tree payload.
    ///
    /// Keeps `blob` entries under `countries/<dir…>/<file>` with image
    /// extensions, keyed by the directory path. A path with an empty, `.` or
    /// `..` segment, or a backslash, is skipped: tree paths are data, and a
    /// URL is built from them. Unknown shapes are skipped, never an error: a
    /// tree with an extra key is still an index.
    pub fn from_tree(payload: &serde_json::Value) -> Self {
        let mut index = TvLogosIndex {
            truncated: payload
                .get("truncated")
                .and_then(|t| t.as_bool())
                .unwrap_or(false),
            ..TvLogosIndex::default()
        };
        if index.truncated {
            tracing::warn!(
                "the tv-logos tree came back truncated; logos past the cut are not matched"
            );
        }
        let Some(tree) = payload.get("tree").and_then(|t| t.as_array()) else {
            return index;
        };
        for entry in tree {
            if entry.get("type").and_then(|t| t.as_str()) != Some("blob") {
                continue;
            }
            let Some(path) = entry.get("path").and_then(|p| p.as_str()) else {
                continue;
            };
            if path.contains('\\') {
                continue;
            }
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() < 3
                || parts[0] != "countries"
                || parts
                    .iter()
                    .any(|part| part.is_empty() || *part == "." || *part == "..")
            {
                continue;
            }
            let (filename, dirs) = parts[1..].split_last().expect("len >= 3");
            if !is_image(filename) {
                continue;
            }
            index
                .files
                .entry(dirs.join("/"))
                .or_default()
                .push((*filename).to_string());
        }
        for files in index.files.values_mut() {
            files.sort();
        }
        index
    }

    /// A directory's direct image children (country files for a country dir).
    pub fn files_for(&self, dir: &str) -> &[String] {
        self.files.get(dir).map(Vec::as_slice).unwrap_or_default()
    }

    /// The station directory of a country dir ([`STATION_SUBDIRS`]), if it has one.
    pub fn station_dir_for(dir: &str) -> Option<&'static str> {
        STATION_SUBDIRS
            .iter()
            .find(|(country, _)| *country == dir)
            .map(|(_, station)| *station)
    }

    /// The station files of a country dir (`united-states` → `us-local`'s files).
    pub fn station_files_for(&self, dir: &str) -> &[String] {
        Self::station_dir_for(dir)
            .map(|station| self.files_for(station))
            .unwrap_or_default()
    }
}

/// A broadcast-station name in the OTA format both plugins match by callsign:
/// `{NETWORK} - {STATE} {CITY} ({CALLSIGN})`, e.g. `CBS - AL Birmingham (WIAT)`.
///
/// Returns `(network, callsign)` lowercased; only the 3–4 letters kept
/// (`WBMA-LD` → `wbma`).
pub fn parse_station_name(channel_name: &str) -> Option<(String, String)> {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(r"^\s*([A-Za-z0-9]+)\s+-\s+.*\(([KW][A-Z]{2,3})(?:-[A-Z0-9]+)?\)\s*$")
            .expect("static station regex")
    });
    let caps = re.captures(channel_name)?;
    Some((
        caps.get(1)?.as_str().to_lowercase(),
        caps.get(2)?.as_str().to_lowercase(),
    ))
}

/// The tv-logos station file for a `(network, callsign)` pair (CM's
/// `match_station_logo`).
///
/// `us-local` files read `<network>-<channel>-<callsign>-us.png` (with
/// `abc-fox-kfbb` / `cbs-2-kalb-dt2` variations). A file qualifies when one of
/// its hyphen-separated words IS the callsign AND one IS the network (a
/// station that changed affiliation keeps its callsign; a file showing its old
/// network is the wrong logo). Among qualifiers, one without a `dt`
/// subchannel word wins, then one without a style word (`white`, `hz`,
/// `light`, `icon`), then the shortest name — so the choice never depends on
/// list order (`dark` is a style word too; the last tie-break is the name).
pub fn match_station_logo(station: &(String, String), local_files: &[String]) -> Option<String> {
    static EXT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let ext = EXT.get_or_init(|| regex::Regex::new(r"\.[a-z]+$").expect("static"));
    let (network, callsign) = station;
    let mut candidates: Vec<(&String, Vec<String>)> = Vec::new();
    for filename in local_files {
        let lowered = filename.to_lowercase();
        let words: Vec<String> = ext
            .replace(&lowered, "")
            .split('-')
            .map(str::to_string)
            .collect();
        if words.iter().any(|w| w == callsign) && words.iter().any(|w| w == network) {
            candidates.push((filename, words));
        }
    }
    // CM's `rank`: (subchannel `dt\d*`, style word, length, filename).
    candidates
        .into_iter()
        .min_by_key(|(filename, words)| {
            let subchannel = words.iter().any(|w| {
                w.strip_prefix("dt")
                    .is_some_and(|rest| rest.chars().all(|c| c.is_ascii_digit()))
            });
            let styled = words
                .iter()
                .any(|w| ["white", "hz", "light", "dark", "icon"].contains(&w.as_str()));
            (subchannel, styled, filename.len(), (*filename).clone())
        })
        .map(|(filename, _)| filename.clone())
}

fn noise_suffix_re() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"(?i)\s+(network|channel|tv)$").expect("static"))
}

fn quality_suffix_re() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"(?i)\s+(hd|sd|uhd|4k|fhd)$").expect("static"))
}

fn timeshift_re() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"(?i)\s*\+\s*1?\s*$").expect("static"))
}

fn strip_noise_suffixes(name: &str, quality: bool) -> String {
    let mut current = name.to_string();
    loop {
        let mut next = noise_suffix_re().replace(&current, "").to_string();
        if quality {
            next = quality_suffix_re().replace(&next, "").to_string();
        }
        if next == current {
            return current;
        }
        current = next;
    }
}

/// Normalize a tv-logos filename for comparison (CM's `normalize_logo_filename`).
pub fn normalize_logo_filename(filename: &str, country_suffix: &str) -> String {
    let stem = filename
        .rsplit_once('.')
        .map(|(s, _)| s)
        .unwrap_or(filename);
    let suffix = format!("-{country_suffix}");
    let stripped = stem
        .strip_suffix(suffix.as_str())
        .or_else(|| {
            let lower = stem.to_lowercase();
            let want = suffix.to_lowercase();
            lower.strip_suffix(want.as_str()).map(|_| {
                stem.get(..stem.len().saturating_sub(suffix.len()))
                    .unwrap_or(stem)
            })
        })
        .unwrap_or(stem);
    let mut name = stripped.replace('-', " ");
    name = name.split_whitespace().collect::<Vec<_>>().join(" ");
    name = name.to_lowercase();
    if let Some(base) = name.strip_suffix(" plus") {
        return format!("{} plus", strip_noise_suffixes(base, false));
    }
    strip_noise_suffixes(&name, false)
}

/// Normalize a channel name for comparison against logo filenames (CM's
/// `normalize_channel_name`).
///
/// One difference: punctuation becomes a SPACE here where CM's `[^\w\s]`
/// deletes it, so `Fox-News` reads `fox news` (tv-logos' own hyphen-to-space
/// form) rather than `foxnews`; the spaceless comparison in
/// [`logo_similarity`] covers the glued reading either way.
pub fn normalize_channel_name(name: &str) -> String {
    let mut out = name.to_lowercase().replace('&', " and ");
    out = out
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == ' ' {
                c
            } else {
                ' '
            }
        })
        .collect();
    out = out.split_whitespace().collect::<Vec<_>>().join(" ");
    strip_noise_suffixes(&out, true)
}

/// Split `ITV2 +1 HD` into `("ITV2 HD", true)`; anything else is `(name, false)`.
///
/// A noise suffix after the `+1` is moved in front of it first, so `+1 HD` is
/// recognized as well as `HD +1`.
pub fn split_timeshift(channel_name: &str) -> (String, bool) {
    static TAIL: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let tail =
        TAIL.get_or_init(|| regex::Regex::new(r"(?i)(\s+(hd|sd|uhd|4k|fhd))+$").expect("static"));
    let mut name = channel_name.trim().to_string();
    let quality_tail = tail.find(&name).map(|m| m.as_str().to_string());
    if quality_tail.is_some() {
        name = tail.replace(&name, "").to_string();
    }
    if let Some(m) = timeshift_re().find(&name) {
        let mut base = name[..m.start()].trim().to_string();
        if let Some(quality_tail) = quality_tail {
            base.push_str(&quality_tail);
        }
        return (base, true);
    }
    (channel_name.to_string(), false)
}

/// Similarity of two normalized names, 0–100, on the UNIFIED engine's metric.
///
/// CM's `_score`: the char similarity both spaced and space-removed (tv-logos
/// hyphenates between letters and digits — `e-4`, `film-4` — where channel
/// names do not), keeping the higher. The metric is
/// [`calculate_similarity_chars`](crosstalk_matching::similarity::calculate_similarity_chars);
/// both plugins used rapidfuzz/thefuzz `ratio` here instead (documented above).
pub fn logo_similarity(normalized_channel: &str, normalized_logo: &str) -> f64 {
    let spaced = calculate_similarity_chars(
        &normalized_channel.chars().collect::<Vec<_>>(),
        &normalized_logo.chars().collect::<Vec<_>>(),
        0.0,
    );
    if spaced >= 1.0 {
        return 100.0;
    }
    let joined = |text: &str| text.chars().filter(|c| *c != ' ').collect::<Vec<_>>();
    let flat =
        calculate_similarity_chars(&joined(normalized_channel), &joined(normalized_logo), 0.0);
    (spaced.max(flat) * 100.0).min(100.0)
}

/// The filename scoring highest against the channel name, or `None` below threshold.
fn best_logo(
    normalized_channel: &str,
    logo_filenames: &[String],
    country_suffix: &str,
    threshold: u8,
) -> Option<String> {
    if normalized_channel.is_empty() {
        return None;
    }
    let mut best: Option<(f64, &String)> = None;
    for filename in logo_filenames {
        let normalized_logo = normalize_logo_filename(filename, country_suffix);
        if normalized_logo.is_empty() {
            continue;
        }
        let score = logo_similarity(normalized_channel, &normalized_logo);
        let better = match &best {
            None => true,
            Some((top, _)) => score > *top,
        };
        if better {
            best = Some((score, filename));
            if score >= 100.0 {
                break;
            }
        }
    }
    match best {
        Some((score, filename)) if score >= f64::from(threshold) => Some(filename.clone()),
        _ => None,
    }
}

/// Match a channel name against tv-logos filenames (CM's `match_channel_to_logo`).
///
/// A timeshift channel (`ITV2 +1`) is matched first as `<name> plus` — how
/// tv-logos names those files — falling back to the plain channel's logo when
/// the repo has no `-plus` file for it.
pub fn match_channel_to_logo(
    channel_name: &str,
    logo_filenames: &[String],
    country_suffix: &str,
    threshold: u8,
) -> Option<String> {
    let (base_name, is_timeshift) = split_timeshift(channel_name);
    if is_timeshift {
        let base = normalize_channel_name(&base_name);
        if base.is_empty() {
            return None;
        }
        if let Some(found) = best_logo(
            &format!("{base} plus"),
            logo_filenames,
            country_suffix,
            threshold,
        ) {
            return Some(found);
        }
        return best_logo(&base, logo_filenames, country_suffix, threshold);
    }
    best_logo(
        &normalize_channel_name(channel_name),
        logo_filenames,
        country_suffix,
        threshold,
    )
}

/// One tv-logos hit: the URL to assign and how it was found.
#[derive(Debug, Clone, PartialEq)]
pub struct TvHit {
    pub url: String,
    pub file: String,
    /// The directory under `countries/` the file lives in (`united-states/us-local`).
    pub dir: String,
    /// The fuzzy score (0–100); `None` for a station-file hit (a callsign match, not a
    /// similarity).
    pub score: Option<f64>,
}

/// A tv-logos file the fuzzy tier scored (oracle evidence).
#[derive(Debug, Clone, PartialEq)]
pub struct TvScored {
    pub dir: String,
    pub file: String,
    pub score: f64,
    /// The normalized filename the score compares against.
    pub norm_file: String,
}

/// Search the tv-logos index for a name across country codes, in order.
///
/// Station-format names take the station file first — each code's station directory,
/// then its country directory (Channel-Mapparr's two-phase order across countries) —
/// else the best fuzzy filename match at `threshold`, the first country with a hit
/// winning. Returns the hit plus every file the fuzzy tier scored, so an oracle can
/// verify a similarity attribution both ways. A code with no directory is skipped.
pub fn search_tv_logos(
    name: &str,
    index: &TvLogosIndex,
    codes: &[String],
    threshold: u8,
) -> (Option<TvHit>, Vec<TvScored>) {
    search_tv(name, index, codes, threshold, true)
}

/// [`search_tv_logos`]; `collect` keeps every scored file (oracle evidence), which
/// [`resolve_logo`] skips — the hit alone is what a run needs.
fn search_tv(
    name: &str,
    index: &TvLogosIndex,
    codes: &[String],
    threshold: u8,
    collect: bool,
) -> (Option<TvHit>, Vec<TvScored>) {
    let mut scored: Vec<TvScored> = Vec::new();
    if let Some(station) = parse_station_name(name) {
        let dirs: Vec<&str> = codes
            .iter()
            .filter_map(|code| country_dir_for(code))
            .collect();
        let station_dirs = dirs
            .iter()
            .filter_map(|dir| TvLogosIndex::station_dir_for(dir))
            .chain(dirs.iter().copied());
        for dir in station_dirs {
            if let Some(file) = match_station_logo(&station, index.files_for(dir)) {
                return (
                    Some(TvHit {
                        url: build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, dir, &file),
                        file,
                        dir: dir.to_string(),
                        score: None,
                    }),
                    scored,
                );
            }
        }
    }
    let norm_channel = normalize_channel_name(name);
    for code in codes {
        let Some(dir) = country_dir_for(code) else {
            continue;
        };
        let suffix = country_suffix_for(code);
        let files = index.files_for(dir);
        if collect {
            for file in files {
                let norm_file = normalize_logo_filename(file, &suffix);
                if norm_file.is_empty() {
                    continue;
                }
                scored.push(TvScored {
                    dir: dir.to_string(),
                    file: file.clone(),
                    score: logo_similarity(&norm_channel, &norm_file),
                    norm_file,
                });
            }
        }
        if let Some((file, matched)) = match_zone_feed(name, files, &suffix, threshold) {
            // The plain name's similarity to the file (a timeshift matched as
            // `<name> plus` reports the plain reading, like the scored list).
            let score = Some(logo_similarity(
                &normalize_channel_name(&matched),
                &normalize_logo_filename(&file, &suffix),
            ));
            return (
                Some(TvHit {
                    url: build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, dir, &file),
                    file,
                    dir: dir.to_string(),
                    score,
                }),
                scored,
            );
        }
    }
    (None, scored)
}

/// [`match_channel_to_logo`] with the feed zone kept out of the competition: a zone
/// feed (`Showtime (W)`, `HBO East`, read by the engine's own
/// [`strip_zone_marker`](crosstalk_matching::country::strip_zone_marker)) shares its
/// brand's logo, so it matches on the zone-free name — unless the full name's file
/// itself names the zone (`Sportsnet West` is its own channel with its own file).
/// Returns the file and the name it matched on.
fn match_zone_feed(
    name: &str,
    files: &[String],
    suffix: &str,
    threshold: u8,
) -> Option<(String, String)> {
    let full = match_channel_to_logo(name, files, suffix, threshold);
    let Some(zone_free) = crosstalk_matching::country::strip_zone_marker(name) else {
        return full.map(|file| (file, name.to_string()));
    };
    let channel_words = normalize_channel_name(name);
    let names_the_zone = |file: &String| {
        normalize_logo_filename(file, suffix)
            .split(' ')
            .any(|word| {
                matches!(word, "west" | "east" | "pacific")
                    && channel_words.split(' ').any(|own| own == word)
            })
    };
    if let Some(file) = full.filter(names_the_zone) {
        return Some((file, name.to_string()));
    }
    if zone_free.is_empty() {
        return None;
    }
    match_channel_to_logo(&zone_free, files, suffix, threshold).map(|file| (file, zone_free))
}

/// The tv-logos file for a network name (`cbs`), or `None` (CM's `network_logo`):
/// `<network>-<suffix>.png`, then the white variant (`cbs-logo-white-us.png`, the
/// only CBS network logo the repo has). Exact filenames, no similarity.
pub fn network_logo(network: &str, files: &[String], country_suffix: &str) -> Option<String> {
    [
        format!("{network}-{country_suffix}.png"),
        format!("{network}-logo-white-{country_suffix}.png"),
    ]
    .into_iter()
    .find(|candidate| files.iter().any(|file| file == candidate))
}

/// The network-logo tier: a station-format name (`CBS - AL Birmingham (WIAT)`) takes
/// its network's logo from the first searched country that has one (CM's order). A
/// name that is not station-shaped has no network to look up.
pub fn search_network_logo(name: &str, index: &TvLogosIndex, codes: &[String]) -> Option<TvHit> {
    let (network, _) = parse_station_name(name)?;
    codes.iter().find_map(|code| {
        let dir = country_dir_for(code)?;
        let file = network_logo(&network, index.files_for(dir), &country_suffix_for(code))?;
        Some(TvHit {
            url: build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, dir, &file),
            file,
            dir: dir.to_string(),
            score: None,
        })
    })
}

/// What [`resolve_logo`] may consult for one run.
#[derive(Debug, Default)]
pub struct LogoSources<'a> {
    /// The consult order; empty means [`LogoSource::DEFAULT_ORDER`]. The default logo is
    /// consulted last even when the order does not name it (the safety net).
    pub order: Vec<LogoSource>,
    /// Logo Manager logo URLs by LOWER name (exact, case-insensitive).
    pub manager_by_name: HashMap<String, String>,
    /// The tv-logos tree index, when the run loaded it.
    pub tv_logos: Option<&'a TvLogosIndex>,
    /// The configured default logo URL.
    pub default_logo: Option<String>,
    /// Fuzzy threshold (default [`LOGO_MATCH_THRESHOLD`]).
    pub threshold: Option<u8>,
}

/// One channel as [`resolve_logo`] sees it.
#[derive(Debug, Default)]
pub struct LogoQuery<'a> {
    /// The channel name, for the Logo Manager's exact-name tier.
    pub name: &'a str,
    /// The name the tv-logos tier matches (an app passes the identity's canonical name,
    /// so a decorated name and its bare form take the same file).
    pub match_name: &'a str,
    /// The icon URL of the channel's assigned EPG entry, when it carries one.
    pub epg_icon: Option<&'a str>,
    /// ISO-2 countries whose tv-logos directories are searched, in order.
    pub countries: &'a [String],
    /// The channel already carries a logo: the network tier only fills a channel
    /// with none (CM's `current_logo_id is None`), so a replace run never swaps a
    /// logo for the generic network one.
    pub has_logo: bool,
}

/// Resolve a channel to a logo URL: the ONE tier walk every app uses.
///
/// The tiers run in [`LogoSources::order`] (default EPG icon → Logo Manager (exact
/// name) → tv-logos (station file for OTA names, else fuzzy, [`search_tv_logos`]) →
/// the station's network logo for a channel with no logo ([`search_network_logo`])),
/// then the default logo when the order did not name it. `None` only when every tier
/// misses. Creating the `Logo` row is the app's job.
pub fn resolve_logo(query: &LogoQuery<'_>, sources: &LogoSources<'_>) -> Option<ResolvedLogo> {
    let threshold = sources.threshold.unwrap_or(LOGO_MATCH_THRESHOLD);
    let order: &[LogoSource] = if sources.order.is_empty() {
        &LogoSource::DEFAULT_ORDER
    } else {
        &sources.order
    };
    let hit = |url: &str, source: LogoSource| {
        Some(ResolvedLogo {
            url: url.to_string(),
            source,
            tv_hit: None,
        })
    };
    let tiers = order
        .iter()
        .copied()
        .chain((!order.contains(&LogoSource::Default)).then_some(LogoSource::Default));
    for tier in tiers {
        let found = match tier {
            LogoSource::EpgIcon => query
                .epg_icon
                .filter(|icon| !icon.trim().is_empty())
                .and_then(|icon| hit(icon, tier)),
            LogoSource::LogoManager => sources
                .manager_by_name
                .get(&query.name.to_lowercase())
                .filter(|url| !url.trim().is_empty())
                .and_then(|url| hit(url.as_str(), tier)),
            LogoSource::TvLogos => sources.tv_logos.and_then(|index| {
                search_tv(query.match_name, index, query.countries, threshold, false)
                    .0
                    .map(|tv| ResolvedLogo {
                        url: tv.url.clone(),
                        source: tier,
                        tv_hit: Some(tv),
                    })
            }),
            LogoSource::Network => sources
                .tv_logos
                .filter(|_| !query.has_logo)
                .and_then(|index| search_network_logo(query.match_name, index, query.countries))
                .map(|tv| ResolvedLogo {
                    url: tv.url.clone(),
                    source: tier,
                    tv_hit: Some(tv),
                }),
            LogoSource::Default => sources
                .default_logo
                .as_deref()
                .filter(|url| !url.trim().is_empty())
                .and_then(|url| hit(url, tier)),
        };
        if found.is_some() {
            return found;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The recorded tree fixture shape: a handful of blobs across two dirs.
    fn fixture_tree() -> serde_json::Value {
        json!({
            "tree": [
                {"path": "countries/united-states/cnn-us.png", "type": "blob"},
                {"path": "countries/united-states/espn-us.png", "type": "blob"},
                {"path": "countries/united-states/food-network-us.png", "type": "blob"},
                {"path": "countries/united-states/itv-2-plus-uk.png", "type": "blob"},
                {"path": "countries/united-states/e-4-uk.png", "type": "blob"},
                {"path": "countries/united-states/notes.txt", "type": "blob"},
                {"path": "countries/united-states", "type": "tree"},
                {"path": "countries/united-states/us-local/cbs-42-wiat-us.png", "type": "blob"},
                {"path": "countries/united-states/us-local/cbs-2-kalb-dt2-us.png", "type": "blob"},
                {"path": "countries/united-states/us-local/nbc-5-kalb-us.png", "type": "blob"},
                {"path": "countries/united-kingdom/bbc-one-uk.png", "type": "blob"},
                {"path": "README.md", "type": "blob"},
            ]
        })
    }

    #[test]
    fn tree_parses_into_country_and_station_files() {
        let index = TvLogosIndex::from_tree(&fixture_tree());
        let mut us = index.files_for("united-states").to_vec();
        us.sort();
        assert_eq!(
            us,
            vec![
                "cnn-us.png",
                "e-4-uk.png",
                "espn-us.png",
                "food-network-us.png",
                "itv-2-plus-uk.png",
            ]
        );
        assert_eq!(
            index.station_files_for("united-states"),
            &[
                "cbs-2-kalb-dt2-us.png",
                "cbs-42-wiat-us.png",
                "nbc-5-kalb-us.png",
            ]
        );
        assert_eq!(
            index.files_for("united-kingdom"),
            &["bbc-one-uk.png".to_string()]
        );
        assert!(index.files_for("germany").is_empty());
    }

    #[test]
    fn union_map_covers_both_plugins() {
        // CM's sixteen (incl GB alias, NL/NO/PL); LU's twelve are a subset.
        for code in [
            "US", "CA", "UK", "GB", "AU", "DE", "FR", "IT", "ES", "MX", "BR", "IN", "IE",
        ] {
            assert!(country_dir_for(code).is_some(), "{code}");
        }
        assert_eq!(country_dir_for("NL"), Some("netherlands"));
        assert_eq!(country_dir_for("gb"), Some("united-kingdom"));
        assert_eq!(country_dir_for("ZZ"), None);
        assert_eq!(
            build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, "united-states", "cnn-us.png"),
            "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/cnn-us.png"
        );
    }

    #[test]
    fn station_files_match_by_callsign_and_network() {
        let index = TvLogosIndex::from_tree(&fixture_tree());
        let files = index.station_files_for("united-states").to_vec();
        // WIAT is the CBS affiliate: only the CBS file qualifies.
        let station = parse_station_name("CBS - AL Birmingham (WIAT)").expect("station");
        assert_eq!(station, ("cbs".to_string(), "wiat".to_string()));
        assert_eq!(
            match_station_logo(&station, &files).as_deref(),
            Some("cbs-42-wiat-us.png")
        );
        // KALB's CBS subchannel (dt2) vs its NBC feed: dt-free and network-matching wins.
        let dt = parse_station_name("CBS - LA Alexandria (KALB)").expect("station");
        assert_eq!(
            match_station_logo(&dt, &files).as_deref(),
            Some("cbs-2-kalb-dt2-us.png")
        );
        let nbc = parse_station_name("NBC - LA Alexandria (KALB)").expect("station");
        assert_eq!(
            match_station_logo(&nbc, &files).as_deref(),
            Some("nbc-5-kalb-us.png")
        );
        // Not a station name at all.
        assert_eq!(parse_station_name("ESPN HD"), None);
    }

    #[test]
    fn a_zone_feed_takes_its_brands_logo_unless_a_file_names_the_zone() {
        let index = TvLogosIndex::from_tree(&json!({"tree": [
            {"path": "countries/united-states/showtime-us.png", "type": "blob"},
            {"path": "countries/united-states/showtime-2-us.png", "type": "blob"},
            {"path": "countries/united-states/hbo-us.png", "type": "blob"},
            {"path": "countries/united-states/hbo-2-us.png", "type": "blob"},
            {"path": "countries/canada/sportsnet-ca.png", "type": "blob"},
            {"path": "countries/canada/sportsnet-west-ca.png", "type": "blob"}
        ]}));
        let file = |name: &str, code: &str| {
            search_tv_logos(name, &index, &[code.to_string()], LOGO_MATCH_THRESHOLD)
                .0
                .map(|hit| hit.file)
        };
        // `(W)` must not pull the West feed onto `showtime-2` (W vs 2, one char each).
        assert_eq!(
            file("Showtime (W)", "US").as_deref(),
            Some("showtime-us.png")
        );
        assert_eq!(
            file("Showtime (E)", "US").as_deref(),
            Some("showtime-us.png")
        );
        assert_eq!(file("HBO East", "US").as_deref(), Some("hbo-us.png"));
        assert_eq!(
            file("Showtime 2", "US").as_deref(),
            Some("showtime-2-us.png")
        );
        // A file naming the zone is that channel's own logo.
        assert_eq!(
            file("Sportsnet West", "CA").as_deref(),
            Some("sportsnet-west-ca.png")
        );
    }

    #[test]
    fn fuzzy_names_match_through_the_unified_similarity() {
        let files = vec![
            "cnn-us.png".to_string(),
            "food-network-us.png".to_string(),
            "e-4-uk.png".to_string(),
            "itv-2-plus-uk.png".to_string(),
        ];
        assert_eq!(
            match_channel_to_logo("CNN", &files, "us", LOGO_MATCH_THRESHOLD).as_deref(),
            Some("cnn-us.png")
        );
        assert_eq!(
            match_channel_to_logo("Food Network", &files, "us", LOGO_MATCH_THRESHOLD).as_deref(),
            Some("food-network-us.png")
        );
        // Timeshift first tries the `-plus` file.
        assert_eq!(
            match_channel_to_logo("ITV2 +1", &files, "uk", LOGO_MATCH_THRESHOLD).as_deref(),
            Some("itv-2-plus-uk.png")
        );
        // Hyphenated-between-letters-and-digits still matches (spaceless compare).
        assert_eq!(
            match_channel_to_logo("E4", &files, "uk", LOGO_MATCH_THRESHOLD).as_deref(),
            Some("e-4-uk.png")
        );
        // Nothing close: no match rather than a wrong logo.
        assert_eq!(
            match_channel_to_logo("Zzqxj Kbv", &files, "us", LOGO_MATCH_THRESHOLD),
            None
        );
    }

    fn us() -> Vec<String> {
        vec!["US".to_string()]
    }

    #[test]
    fn resolution_follows_epg_then_manager_then_tvlogos_then_default() {
        let index = TvLogosIndex::from_tree(&fixture_tree());
        let countries = us();
        let sources = LogoSources {
            manager_by_name: [("cnn".to_string(), "https://example.com/m.png".to_string())]
                .into_iter()
                .collect(),
            tv_logos: Some(&index),
            default_logo: Some("https://example.com/default.png".to_string()),
            ..LogoSources::default()
        };
        let query = |name: &'static str, epg_icon: Option<&'static str>| LogoQuery {
            name,
            match_name: name,
            epg_icon,
            countries: &countries,
            has_logo: false,
        };
        // EPG icon wins over everything.
        let hit = resolve_logo(&query("CNN", Some("https://example.com/epg.png")), &sources)
            .expect("epg");
        assert_eq!(
            (hit.url.as_str(), hit.source),
            ("https://example.com/epg.png", LogoSource::EpgIcon)
        );
        // Manager is exact-name, case-insensitive.
        let hit = resolve_logo(&query("CNN", None), &sources).expect("manager");
        assert_eq!(hit.source, LogoSource::LogoManager);
        let hit = resolve_logo(&query("CNN HD", None), &sources).expect("fuzzy");
        assert_eq!(hit.source, LogoSource::TvLogos);
        assert_eq!(
            hit.tv_hit.as_ref().map(|tv| tv.file.as_str()),
            Some("cnn-us.png")
        );
        // OTA names take the station file, under its OWN directory, with no score.
        let hit =
            resolve_logo(&query("CBS - AL Birmingham (WIAT)", None), &sources).expect("station");
        assert_eq!(
            hit.url,
            "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/us-local/cbs-42-wiat-us.png"
        );
        assert_eq!(hit.tv_hit.and_then(|tv| tv.score), None);
        // Default when nothing matches, even though the order does not name it.
        let hit = resolve_logo(&query("Zzqxj Kbv", None), &sources).expect("default");
        assert_eq!(hit.source, LogoSource::Default);
        // No default configured: None rather than a wrong logo.
        let bare = LogoSources {
            tv_logos: Some(&index),
            ..LogoSources::default()
        };
        assert_eq!(resolve_logo(&query("Zzqxj Kbv", None), &bare), None);
    }

    #[test]
    fn the_order_is_the_setting_and_parses_strictly() {
        let index = TvLogosIndex::from_tree(&fixture_tree());
        let countries = us();
        let sources = LogoSources {
            order: LogoSource::parse_order("tv-logos, epg").expect("order"),
            manager_by_name: [("cnn".to_string(), "https://example.com/m.png".to_string())]
                .into_iter()
                .collect(),
            tv_logos: Some(&index),
            ..LogoSources::default()
        };
        let query = LogoQuery {
            name: "CNN",
            match_name: "CNN",
            epg_icon: Some("https://example.com/epg.png"),
            countries: &countries,
            has_logo: false,
        };
        // tv-logos first: the file wins over the EPG icon; the manager is not consulted.
        let hit = resolve_logo(&query, &sources).expect("tv");
        assert_eq!(hit.source, LogoSource::TvLogos);
        assert_eq!(
            LogoSource::parse_order("").expect("empty"),
            LogoSource::DEFAULT_ORDER.to_vec()
        );
        assert_eq!(
            LogoSource::parse_order("Default, EPG, epg").expect("dedup"),
            vec![LogoSource::Default, LogoSource::EpgIcon]
        );
        let error = LogoSource::parse_order("epg, beacons").expect_err("rejects");
        assert!(format!("{error:#}").contains("logo_manager"), "{error:#}");
    }

    #[test]
    fn a_station_with_no_file_takes_its_network_logo_when_it_has_none() {
        let index = TvLogosIndex::from_tree(&json!({
            "tree": [
                {"path": "countries/united-states/cbs-logo-white-us.png", "type": "blob"},
                {"path": "countries/united-states/nbc-us.png", "type": "blob"},
                {"path": "countries/united-states/us-local/cbs-42-wiat-us.png", "type": "blob"},
            ]
        }));
        let countries = us();
        let sources = LogoSources {
            tv_logos: Some(&index),
            default_logo: Some("https://example.com/default.png".to_string()),
            ..LogoSources::default()
        };
        let query = |name: &'static str, has_logo: bool| LogoQuery {
            name,
            match_name: name,
            epg_icon: None,
            countries: &countries,
            has_logo,
        };
        // The station file still wins over the network logo.
        let hit =
            resolve_logo(&query("CBS - AL Birmingham (WIAT)", false), &sources).expect("station");
        assert_eq!(hit.source, LogoSource::TvLogos);
        // No station file: the network logo (CM's white CBS variant), before the default.
        let hit = resolve_logo(&query("CBS - KS Hays (KBSH)", false), &sources).expect("cbs");
        assert_eq!(hit.source, LogoSource::Network);
        assert!(
            hit.url.ends_with("/united-states/cbs-logo-white-us.png"),
            "{}",
            hit.url
        );
        let hit = resolve_logo(&query("NBC - OH Dayton (WDTN)", false), &sources).expect("nbc");
        assert!(
            hit.url.ends_with("/united-states/nbc-us.png"),
            "{}",
            hit.url
        );
        // A channel that has a logo never gets the generic network one.
        let hit = resolve_logo(&query("NBC - OH Dayton (WDTN)", true), &sources).expect("default");
        assert_eq!(hit.source, LogoSource::Default);
        // Not station-shaped, or a network with no file: no network logo.
        assert_eq!(search_network_logo("NBC News", &index, &countries), None);
        assert_eq!(
            search_network_logo("PBS - AK Fairbanks (KUAC)", &index, &countries),
            None
        );
        // `network` is a token of the order.
        assert_eq!(
            LogoSource::parse_order("network, default").expect("order"),
            vec![LogoSource::Network, LogoSource::Default]
        );
    }

    #[test]
    fn countries_are_searched_in_order_station_dirs_first() {
        let index = TvLogosIndex::from_tree(&fixture_tree());
        // A station name finds its us-local file even when US is the second code.
        let codes = vec!["GB".to_string(), "US".to_string()];
        let (hit, _) = search_tv_logos(
            "CBS - AL Birmingham (WIAT)",
            &index,
            &codes,
            LOGO_MATCH_THRESHOLD,
        );
        assert_eq!(
            hit.map(|hit| hit.dir).as_deref(),
            Some("united-states/us-local")
        );
        // Fuzzy: the first country with a hit wins, and every scored file is reported.
        let (hit, scored) = search_tv_logos("BBC One", &index, &codes, LOGO_MATCH_THRESHOLD);
        assert_eq!(hit.map(|hit| hit.dir).as_deref(), Some("united-kingdom"));
        assert!(scored.iter().all(|item| item.dir == "united-kingdom"));
        // An unknown code is skipped.
        let (hit, _) = search_tv_logos("CNN", &index, &["ZZ".to_string()], LOGO_MATCH_THRESHOLD);
        assert_eq!(hit, None);
    }

    #[test]
    fn urls_are_percent_encoded_and_tree_paths_are_checked() {
        assert_eq!(
            build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, "ukraine", "100%-news-ua.png"),
            "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/ukraine/100%25-news-ua.png"
        );
        assert_eq!(
            build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, "turkey", "trt-genç tr.png"),
            "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/turkey/trt-gen%C3%A7%20tr.png"
        );
        let index = TvLogosIndex::from_tree(&json!({
            "truncated": true,
            "tree": [
                {"path": "countries/../../etc/passwd.png", "type": "blob"},
                {"path": "countries/x//y.png", "type": "blob"},
                {"path": "countries/x\\y.png", "type": "blob"},
                {"path": "countries/nordic/norway/nrk1-no.png", "type": "blob"},
                {"path": "countries/united-kingdom/hd/bbc-one-hd-uk.png", "type": "blob"},
            ]
        }));
        assert!(index.truncated);
        let mut dirs: Vec<&String> = index.files.keys().collect();
        dirs.sort();
        assert_eq!(dirs, ["nordic/norway", "united-kingdom/hd"]);
        // Norway is under nordic/ in the repo (CM's `norway` never existed).
        assert_eq!(
            index.files_for(country_dir_for("NO").expect("NO")),
            &["nrk1-no.png".to_string()]
        );
        // Nested non-station dirs are not station files.
        assert!(index.station_files_for("united-kingdom").is_empty());
    }

    #[test]
    fn gb_uses_the_repos_uk_suffix() {
        assert_eq!(country_suffix_for("GB"), "uk");
        assert_eq!(country_suffix_for("us"), "us");
        let index = TvLogosIndex::from_tree(&fixture_tree());
        let countries = vec!["GB".to_string()];
        let sources = LogoSources {
            tv_logos: Some(&index),
            ..LogoSources::default()
        };
        let query = LogoQuery {
            name: "BBC One",
            match_name: "BBC One",
            epg_icon: None,
            countries: &countries,
            has_logo: false,
        };
        let hit = resolve_logo(&query, &sources).expect("bbc one");
        assert!(
            hit.url.ends_with("/united-kingdom/bbc-one-uk.png"),
            "{}",
            hit.url
        );
    }

    #[test]
    fn station_rank_is_cms() {
        let station = ("cbs".to_string(), "kxyz".to_string());
        // `dtv` is not a `dt\d*` subchannel word; `dark` is a style word.
        let files = vec![
            "cbs-4-kxyz-dark-us.png".to_string(),
            "cbs-4-kxyz-dtv-us.png".to_string(),
            "cbs-4-kxyz-dt2-us.png".to_string(),
        ];
        assert_eq!(
            match_station_logo(&station, &files).as_deref(),
            Some("cbs-4-kxyz-dtv-us.png")
        );
    }
}
