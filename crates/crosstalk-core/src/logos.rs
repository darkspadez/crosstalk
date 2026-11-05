//! The unified logo resolver: one source order for every app.
//!
//! Phase 3 step 7. Both plugins fetch tv-logos over the network and match
//! filenames with a ratio; Lineuparr's assignment order is the decided one:
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
//! Creating `Logo` rows is lineup's job (step 16); this module only resolves a
//! URL and names its source.
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

/// Where a resolved logo came from, in consult order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogoSource {
    EpgIcon,
    LogoManager,
    TvLogos,
    Default,
}

impl LogoSource {
    pub fn label(self) -> &'static str {
        match self {
            LogoSource::EpgIcon => "epg_icon",
            LogoSource::LogoManager => "logo_manager",
            LogoSource::TvLogos => "tv_logos",
            LogoSource::Default => "default",
        }
    }
}

/// A resolved logo: the URL to assign and the tier that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLogo {
    pub url: String,
    pub source: LogoSource,
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

/// What [`resolve_logo`] may consult.
#[derive(Debug, Default)]
pub struct LogoLookups<'a> {
    /// The channel's EPG icon URL, when its guide entry carries one.
    pub epg_icon: Option<&'a str>,
    /// Logo Manager logos by LOWER name (exact, case-insensitive).
    pub manager_by_name: HashMap<String, &'a str>,
    /// The tv-logos tree index, when the run fetched it.
    pub tv_logos: Option<TvLogosIndex>,
    /// Lineup's configured default logo URL.
    pub default_logo: Option<&'a str>,
    /// ISO-2 country choosing the tv-logos directory.
    pub country: Option<&'a str>,
    /// Fuzzy threshold (default [`LOGO_MATCH_THRESHOLD`]).
    pub threshold: Option<u8>,
}

/// Resolve a channel name to a logo URL, in the unified source order.
///
/// EPG icon → Logo Manager (exact name) → tv-logos (station file for OTA
/// names, else fuzzy) → default. `None` only when every tier misses (a lineup
/// with no default configured). Creating the `Logo` row is lineup's job.
pub fn resolve_logo(channel_name: &str, lookups: &LogoLookups<'_>) -> Option<ResolvedLogo> {
    if let Some(icon) = lookups.epg_icon.filter(|icon| !icon.trim().is_empty()) {
        return Some(ResolvedLogo {
            url: icon.to_string(),
            source: LogoSource::EpgIcon,
        });
    }
    if let Some(url) = lookups
        .manager_by_name
        .get(&channel_name.to_lowercase())
        .filter(|url| !url.trim().is_empty())
    {
        return Some(ResolvedLogo {
            url: (*url).to_string(),
            source: LogoSource::LogoManager,
        });
    }
    let threshold = lookups.threshold.unwrap_or(LOGO_MATCH_THRESHOLD);
    if let Some(index) = &lookups.tv_logos {
        if let Some(code) = lookups.country {
            if let Some(dir) = country_dir_for(code) {
                let suffix = country_suffix_for(code);
                if let Some(station) = parse_station_name(channel_name) {
                    // Station files first (their own dir), then the country's.
                    let station_dir = TvLogosIndex::station_dir_for(dir);
                    let mut found = station_dir.and_then(|station_dir| {
                        match_station_logo(&station, index.files_for(station_dir))
                            .map(|file| (station_dir, file))
                    });
                    if found.is_none() {
                        found = match_station_logo(&station, index.files_for(dir))
                            .map(|file| (dir, file));
                    }
                    if let Some((file_dir, file)) = found {
                        return Some(ResolvedLogo {
                            url: build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, file_dir, &file),
                            source: LogoSource::TvLogos,
                        });
                    }
                }
                if let Some(file) =
                    match_channel_to_logo(channel_name, index.files_for(dir), &suffix, threshold)
                {
                    return Some(ResolvedLogo {
                        url: build_logo_url(TV_LOGOS_REPO, TV_LOGOS_BRANCH, dir, &file),
                        source: LogoSource::TvLogos,
                    });
                }
            }
        }
    }
    lookups
        .default_logo
        .filter(|url| !url.trim().is_empty())
        .map(|url| ResolvedLogo {
            url: url.to_string(),
            source: LogoSource::Default,
        })
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

    #[test]
    fn resolution_follows_epg_then_manager_then_tvlogos_then_default() {
        let index = TvLogosIndex::from_tree(&fixture_tree());
        let base = LogoLookups {
            tv_logos: Some(index),
            country: Some("US"),
            default_logo: Some("https://example.com/default.png"),
            ..LogoLookups::default()
        };
        // EPG icon wins over everything.
        let epg = LogoLookups {
            epg_icon: Some("https://example.com/epg.png"),
            manager_by_name: [("cnn".to_string(), "https://example.com/m.png")]
                .into_iter()
                .collect(),
            ..base_shallow(&base)
        };
        let hit = resolve_logo("CNN", &epg).expect("epg");
        assert_eq!(
            (hit.url.as_str(), hit.source),
            ("https://example.com/epg.png", LogoSource::EpgIcon)
        );
        // Manager is exact-name, case-insensitive.
        let mut manager = base_shallow(&base);
        manager
            .manager_by_name
            .insert("cnn".to_string(), "https://example.com/m.png");
        let hit = resolve_logo("CNN", &manager).expect("manager");
        assert_eq!(hit.source, LogoSource::LogoManager);
        assert!(resolve_logo("CNN HD", &manager).expect("fuzzy").source == LogoSource::TvLogos);
        // OTA names take the station file, under its OWN directory.
        let hit = resolve_logo("CBS - AL Birmingham (WIAT)", &base).expect("station");
        assert_eq!(
            hit.url,
            "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/us-local/cbs-42-wiat-us.png"
        );
        // Default when nothing matches.
        let hit = resolve_logo("Zzqxj Kbv", &base).expect("default");
        assert_eq!(hit.source, LogoSource::Default);
        // No default configured: None rather than a wrong logo.
        let bare = LogoLookups {
            tv_logos: base.tv_logos.clone(),
            country: Some("US"),
            ..LogoLookups::default()
        };
        assert_eq!(resolve_logo("Zzqxj Kbv", &bare), None);
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
        let lookups = LogoLookups {
            tv_logos: Some(index),
            country: Some("GB"),
            ..LogoLookups::default()
        };
        let hit = resolve_logo("BBC One", &lookups).expect("bbc one");
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

    fn base_shallow<'a>(base: &LogoLookups<'a>) -> LogoLookups<'a> {
        LogoLookups {
            epg_icon: base.epg_icon,
            manager_by_name: HashMap::new(),
            tv_logos: base.tv_logos.clone(),
            default_logo: base.default_logo,
            country: base.country,
            threshold: base.threshold,
        }
    }
}
