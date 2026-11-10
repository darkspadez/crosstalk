//! Lineup files: load one by name, with Lineuparr's country and grouping semantics.
//!
//! The 29 Lineuparr lineup files ship embedded in the binary
//! (`crosstalk_matching::data::merged::MERGED_LINEUPS`), copied byte-for-byte from
//! `reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/*_lineup.json` — never read from the
//! operator's disk. A file is named by its short name (`US_Combined`) or its full filename
//! (`US_Combined_lineup.json`); anything else — a path, a traversal, an unknown name — is
//! refused. There is no filesystem access here at all, so a hostile `lineup_file` value
//! cannot escape anywhere.
//!
//! The pipeline ports Lineuparr's `Plugin._load_lineup` (`plugin.py:790`) statement for
//! statement, in the plugin's own order:
//!
//!   1. filename country (`_parse_lineup_filename`, `plugin.py:925`):
//!      `{CC}_{Provider}_lineup.json` -> `CC`.
//!   2. `CC_` entry-name prefix rewrite (`_rewrite_country_prefixes`, `plugin.py:825`):
//!      `UK_CNN` becomes name `CNN` with country `UK`, via the unified
//!      [`crosstalk_matching::country::lineuparr_name_country_prefix`].
//!   3. `category_detail` folding (`_apply_category_detail`, `plugin.py:878`) with the
//!      plugin's own [`SIMPLIFIED_CATEGORIES`] / [`REFINED_CATEGORIES`] maps and its
//!      [`SINGLE_GROUP_CATEGORY`] sentinel (`PluginConfig`, `plugin.py:165,220`).
//!   4. per-category country (`_resolve_category_country`, `plugin.py:935`) via the
//!      unified [`crosstalk_matching::country::lineuparr_category_country`], falling back
//!      to the filename country.
//!   5. per-entry country (`_resolve_entry_country`, `plugin.py:858`): the entry's own
//!      prefix country wins, then the category's, then the filename's.
//!   6. group names (`_make_group_name` / `_get_group_prefix`, `plugin.py:993,1123`).
//!   7. channel numbers (`_parse_channel_number`, `plugin.py:1009`): an int, or a range
//!      string (`923-946`) read as its first number.
//!
//! Citations are the plugin lines above; behaviour beyond them (the category-order
//! scanner, the `Lineup` struct) is crosstalk-only and said so.

use anyhow::Result;
use crosstalk_matching::data::merged;

/// The `category_detail` values Lineuparr offers (`plugin.py`, `category_detail` field).
pub const CATEGORY_DETAILS: [&str; 4] = ["none", "refined", "simple", "normal"];

/// The `channel_numbering` values Lineuparr offers (`NUMBERING_LABELS`, `plugin.py:2071`).
pub const NUMBERING_MODES: [&str; 4] = ["lineup", "auto_next", "auto_highest", "specific"];

/// The sentinel category `_apply_category_detail` merges everything under for
/// `category_detail = "none"` (`PluginConfig.SINGLE_GROUP_CATEGORY`, `plugin.py:165`).
pub const SINGLE_GROUP_CATEGORY: &str = "All";

/// `category_detail = "simple"` folding (`PluginConfig.SIMPLIFIED_CATEGORIES`,
/// `plugin.py:220`): simplified name -> original categories merged into it.
pub const SIMPLIFIED_CATEGORIES: [(&str, &[&str]); 7] = [
    ("News & Info", &["News", "Discovery", "Crime"]),
    (
        "Sports & Outdoors",
        &["Sports", "Regional Sports", "Outdoors"],
    ),
    (
        "Entertainment & Lifestyle",
        &[
            "Entertainment",
            "Comedy",
            "Reality & Lifestyle",
            "Food & Travel",
        ],
    ),
    ("Movies", &["Movies"]),
    ("Kids & Family", &["Kids", "Faith"]),
    ("Foreign Language", &["Spanish", "French"]),
    ("Music & Shopping", &["Music", "Shopping"]),
];

/// `category_detail = "refined"` folding (`PluginConfig.REFINED_CATEGORIES`,
/// `plugin.py:230`): refined name -> original categories merged into it.
pub const REFINED_CATEGORIES: [(&str, &[&str]); 6] = [
    ("News", &["News"]),
    ("Sports", &["Sports", "Regional Sports"]),
    ("Movies", &["Movies"]),
    ("Family", &["Kids", "Faith"]),
    ("Foreign", &["Spanish", "French"]),
    (
        "Entertainment",
        &[
            "Entertainment",
            "Reality & Lifestyle",
            "Comedy",
            "Discovery",
            "Crime",
            "Music",
            "Shopping",
            "Outdoors",
            "Food & Travel",
        ],
    ),
];

/// One channel of a loaded lineup: the entry Lineuparr's sync, stream match and EPG match
/// all read from — name, number, category, group, country and aliases together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineupEntry {
    /// The verbatim lineup name, after the `CC_` prefix rewrite.
    pub name: String,
    /// The lineup number (`_parse_channel_number`: an int, or a range's first number).
    pub number: Option<i64>,
    /// The category after `category_detail` folding.
    pub category: String,
    /// The Dispatcharr group this category lands in (`_make_group_name`).
    pub group: String,
    /// The enforced country: entry prefix, then category, then filename.
    pub country: Option<String>,
    /// Accepted alternate names (`aliases` on the entry, when present).
    pub aliases: Vec<String>,
}

/// A loaded lineup file: every entry in file order, with the file's own metadata.
#[derive(Debug, Clone)]
pub struct Lineup {
    /// The full filename as shipped (`US_Combined_lineup.json`).
    pub file_name: String,
    /// The display name from the file (`package`).
    pub package: String,
    /// The owning country from the filename (`_parse_lineup_filename`).
    pub country: Option<String>,
    /// The group prefix in effect (`_get_group_prefix`).
    pub prefix: String,
    /// The `category_detail` this load folded with.
    pub detail: String,
    /// Folded category names, in file order.
    pub categories: Vec<String>,
    /// Every entry, in file order.
    pub entries: Vec<LineupEntry>,
}

/// Short names of every embedded lineup file (`US_Combined`, `UK_Freeview`, ...).
pub fn available_lineups() -> Vec<&'static str> {
    merged::merged_lineup_names()
}

/// Whether `name` resolves to an embedded lineup file (short or full filename).
pub fn lineup_exists(name: &str) -> bool {
    normalize_lineup_name(name).is_some()
}

/// Resolve a `lineup_file` setting to the embedded short name, or `None`.
///
/// Accepts the short name (`US_Combined`) or the full filename
/// (`US_Combined_lineup.json`); anything else — a path, a traversal, an unknown name — is
/// `None`. Lookup is exact against the embedded table, so there is no filesystem and no
/// traversal to guard beyond the table itself.
pub fn normalize_lineup_name(name: &str) -> Option<&'static str> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    for candidate in merged::merged_lineup_names() {
        if trimmed == candidate || trimmed == format!("{candidate}_lineup.json") {
            return Some(candidate);
        }
    }
    None
}

/// The full filename for a short name (`US_Combined` -> `US_Combined_lineup.json`).
pub fn full_filename(short: &str) -> String {
    format!("{short}_lineup.json")
}

/// The country code from a lineup filename (`_parse_lineup_filename`, `plugin.py:925`):
/// `^([A-Z]{2})_(.+)_lineup\.json$` -> `CC`, anything else -> `None`.
pub fn filename_country(filename: &str) -> Option<String> {
    let stem = filename.strip_suffix("_lineup.json")?;
    let (code, provider) = stem.split_once('_')?;
    if code.len() == 2 && code.chars().all(|c| c.is_ascii_uppercase()) && !provider.is_empty() {
        return Some(code.to_string());
    }
    None
}

/// The group prefix in effect (`_get_group_prefix`, `plugin.py:993`): the setting with
/// leading whitespace stripped; `none` (any case, surrounding spaces ignored) means no
/// prefix; blank means the first word of the package name.
pub fn group_prefix_for(setting: &str, package: &str) -> String {
    let prefix = setting.trim_start();
    if prefix.trim().eq_ignore_ascii_case("none") {
        return String::new();
    }
    if !prefix.trim().is_empty() {
        return prefix.to_string();
    }
    package
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

/// The Dispatcharr group name for a category (`_make_group_name`, `plugin.py:1123`).
///
/// A prefix ending in a separator character appends the category directly (`DTV-` +
/// `News` = `DTV-News`); otherwise prefix and category join with `": "`. The
/// [`SINGLE_GROUP_CATEGORY`] sentinel (`category_detail = "none"`) is the exception: the
/// group is the prefix ALONE (`"<prefix>: All"` would reintroduce the categories `none`
/// promises to merge away), or the sentinel itself when there is no prefix.
pub fn make_group_name(prefix: &str, category: &str) -> String {
    if category == SINGLE_GROUP_CATEGORY {
        // `prefix.rstrip(" :_-/")` in the plugin (trailing spaces included).
        let stripped = prefix
            .trim_end_matches([' ', ':', '_', '-', '/'])
            .to_string();
        if stripped.is_empty() {
            return SINGLE_GROUP_CATEGORY.to_string();
        }
        return stripped;
    }
    if prefix.is_empty() {
        return category.to_string();
    }
    if matches!(prefix.chars().last(), Some(':' | '_' | '-' | '/' | ' ')) {
        return format!("{prefix}{category}");
    }
    format!("{prefix}: {category}")
}

/// A lineup channel number (`_parse_channel_number`, `plugin.py:1009`): an int as-is; a
/// range string (`923-946`) read as its first number; anything else -> `None`.
///
/// The plugin returns the raw value through for unparseable formats and lets Django
/// reject it; no shipped file carries one (all 29 files hold ints and dash ranges), so
/// `None` is the faithful load-time answer here.
pub fn parse_channel_number(raw: &serde_json::Value) -> Option<i64> {
    match raw {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return None;
            }
            if let Some((first, _)) = trimmed.split_once('-') {
                let first = first.trim();
                if !first.is_empty() && first.chars().all(|c| c.is_ascii_digit()) {
                    return first.parse::<i64>().ok();
                }
                return None;
            }
            if trimmed.chars().all(|c| c.is_ascii_digit()) {
                return trimmed.parse::<i64>().ok();
            }
            None
        }
        _ => None,
    }
}

/// The largest lineup document [`parse_lineup`] reads (16 MiB). The biggest shipped file is
/// well under 1 MiB; the cap is for a lineup that does not come from the embedded table.
pub const MAX_LINEUP_BYTES: usize = 16 * 1024 * 1024;

/// The most categories one lineup may hold.
pub const MAX_CATEGORIES: usize = 10_000;

/// The most entries one lineup may hold, across every category (the shipped 29 files hold
/// 7,485 together).
pub const MAX_ENTRIES: usize = 200_000;

/// Load a lineup by name, folding categories and resolving every entry.
///
/// `file` is a short name or full filename (see [`normalize_lineup_name`]); `detail` is
/// a [`CATEGORY_DETAILS`] value — an unknown value folds as `normal`, exactly as the
/// plugin's `// Unknown detail value - treat as normal` branch does; `group_prefix` is
/// the raw `group_prefix` setting (see [`group_prefix_for`]).
pub fn load_lineup(file: &str, detail: &str, group_prefix: &str) -> Result<Lineup> {
    let short = normalize_lineup_name(file).ok_or_else(|| {
        let mut known = available_lineups();
        known.sort_unstable();
        anyhow::anyhow!("unknown lineup file {file:?}; known: {}", known.join(", "))
    })?;
    let raw = merged::merged_lineup_json(short)
        .ok_or_else(|| anyhow::anyhow!("the embedded lineup {short} is missing"))?;
    parse_lineup(raw, &full_filename(short), detail, group_prefix)
}

/// Parse one lineup document: the whole `_load_lineup` pipeline over `raw`.
///
/// `filename` is the full filename (`US_Combined_lineup.json`), which carries the
/// filename country and, without a `package`, the display name. The document is
/// held to [`MAX_LINEUP_BYTES`], [`MAX_CATEGORIES`] and [`MAX_ENTRIES`], so a lineup
/// from outside the embedded table cannot make a load quadratic or unbounded.
pub fn parse_lineup(raw: &str, filename: &str, detail: &str, group_prefix: &str) -> Result<Lineup> {
    if raw.len() > MAX_LINEUP_BYTES {
        anyhow::bail!(
            "invalid lineup file: {} bytes is over the {MAX_LINEUP_BYTES} byte cap ({filename})",
            raw.len()
        );
    }
    let parsed: LineupJson = serde_json::from_str(raw)?;
    let package = parsed.package.as_ref().and_then(|value| value.as_str());
    let categories = parsed.categories.ok_or_else(|| {
        anyhow::anyhow!("invalid lineup file: missing 'categories' key ({filename})")
    })?;
    if categories.0.len() > MAX_CATEGORIES {
        anyhow::bail!(
            "invalid lineup file: {} categories is over the {MAX_CATEGORIES} cap ({filename})",
            categories.0.len()
        );
    }
    let entry_count: usize = categories
        .0
        .iter()
        .filter_map(|(_, value)| value.as_array().map(Vec::len))
        .sum();
    if entry_count > MAX_ENTRIES {
        anyhow::bail!(
            "invalid lineup file: {entry_count} entries is over the {MAX_ENTRIES} cap ({filename})"
        );
    }
    let short = filename.strip_suffix("_lineup.json").unwrap_or(filename);

    // 1. The filename country, from the FULL filename the setting may have named.
    let filename = filename.to_string();
    let lineup_country = filename_country(&filename);

    // 2. The `CC_` entry-name prefix rewrite, before anything else sees the lineup.
    let mut rewritten: Vec<(String, Vec<RawEntry>)> = Vec::new();
    for (category, entries_value) in categories.0 {
        let Some(entries) = entries_value.as_array() else {
            continue;
        };
        let mut out = Vec::with_capacity(entries.len());
        for entry_value in entries {
            let name = entry_value
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let (prefix_country, stripped) =
                crosstalk_matching::country::lineuparr_name_country_prefix(&name);
            let name = stripped.to_string();
            let aliases = entry_value
                .get("aliases")
                .and_then(|value| value.as_array())
                .map(|list| {
                    list.iter()
                        .filter_map(|alias| alias.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            out.push(RawEntry {
                name,
                number: entry_value.get("number").cloned(),
                entry_country: prefix_country.map(str::to_string),
                aliases,
            });
        }
        rewritten.push((category, out));
    }

    // 3. The `category_detail` fold, over the rewritten categories.
    let detail = if CATEGORY_DETAILS.contains(&detail) {
        detail
    } else {
        "normal"
    };
    let folded = fold_categories(&rewritten, detail);

    // 4-6. Prefix, per-category country, per-entry country, group names.
    // `lineup_data.get("package", "")`: a file without one auto-prefixes nothing.
    let prefix = group_prefix_for(group_prefix, package.unwrap_or_default());
    let mut categories = Vec::with_capacity(folded.len());
    let mut entries = Vec::new();
    for (category, raws) in &folded {
        categories.push(category.clone());
        let category_country =
            crosstalk_matching::country::lineuparr_category_country(category).map(str::to_string);
        let enforced = category_country.or_else(|| lineup_country.clone());
        let group = make_group_name(&prefix, category);
        for raw in raws {
            let country = raw.entry_country.clone().or_else(|| enforced.clone());
            entries.push(LineupEntry {
                name: raw.name.clone(),
                number: raw.number.as_ref().and_then(parse_channel_number),
                category: category.clone(),
                group: group.clone(),
                country,
                aliases: raw.aliases.clone(),
            });
        }
    }

    Ok(Lineup {
        file_name: filename,
        package: package.unwrap_or(short).to_string(),
        country: lineup_country,
        prefix,
        detail: detail.to_string(),
        categories,
        entries,
    })
}

/// One raw entry between the prefix rewrite and the detail fold.
struct RawEntry {
    name: String,
    number: Option<serde_json::Value>,
    entry_country: Option<String>,
    aliases: Vec<String>,
}

/// Fold rewritten categories per `category_detail` (`_apply_category_detail`).
fn fold_categories(
    rewritten: &[(String, Vec<RawEntry>)],
    detail: &str,
) -> Vec<(String, Vec<RawEntry>)> {
    match detail {
        "none" => {
            let mut all = Vec::new();
            for (_, entries) in rewritten {
                all.extend(entries.iter().map(clone_raw));
            }
            vec![(SINGLE_GROUP_CATEGORY.to_string(), all)]
        }
        "simple" | "refined" => {
            let map: &[(&str, &[&str])] = if detail == "refined" {
                &REFINED_CATEGORIES
            } else {
                &SIMPLIFIED_CATEGORIES
            };
            let mut merged: Vec<(String, Vec<RawEntry>)> = Vec::new();
            let mut mapped: Vec<&str> = Vec::new();
            for (group_name, originals) in map {
                let mut entries = Vec::new();
                for original in *originals {
                    if let Some((_, raws)) = rewritten.iter().find(|(name, _)| name == original) {
                        entries.extend(raws.iter().map(clone_raw));
                        mapped.push(original);
                    }
                }
                if !entries.is_empty() {
                    merged.push(((*group_name).to_string(), entries));
                }
            }
            // A category the map does not cover folds into the broad Entertainment
            // catch-all, so Refined/Simple yield only their documented fixed set of
            // groups. The catch-all differs per mode, as in the plugin.
            let catchall = if detail == "refined" {
                "Entertainment"
            } else {
                "Entertainment & Lifestyle"
            };
            for (name, raws) in rewritten {
                if !mapped.iter().any(|seen| *seen == name) && !raws.is_empty() {
                    match merged
                        .iter_mut()
                        .find(|(group, _)| group.as_str() == catchall)
                    {
                        Some((_, entries)) => entries.extend(raws.iter().map(clone_raw)),
                        None => {
                            merged
                                .push((catchall.to_string(), raws.iter().map(clone_raw).collect()));
                        }
                    }
                }
            }
            merged
        }
        _ => rewritten
            .iter()
            .map(|(name, raws)| (name.clone(), raws.iter().map(clone_raw).collect()))
            .collect(),
    }
}

fn clone_raw(raw: &RawEntry) -> RawEntry {
    RawEntry {
        name: raw.name.clone(),
        number: raw.number.clone(),
        entry_country: raw.entry_country.clone(),
        aliases: raw.aliases.clone(),
    }
}

/// The parts of a lineup file the loader reads.
#[derive(serde::Deserialize)]
struct LineupJson {
    /// Any JSON value: the plugin reads it with `.get`, so a non-string is "no package".
    #[serde(default)]
    package: Option<serde_json::Value>,
    #[serde(default)]
    categories: Option<OrderedObject>,
}

/// A JSON object's entries in FILE order.
///
/// `serde_json::Map` sorts its keys unless the crate-wide `preserve_order` feature is on,
/// and turning that on would reorder every map the workspace serializes. `_load_lineup`
/// iterates the file's order (a Python dict), which decides group creation order and the
/// merged `none` listing, so the order is kept by deserializing the entries into a list.
/// A repeated key keeps its FIRST position and its LAST value, as `json.load` does.
/// Crosstalk-only scaffolding around the faithful port, not plugin behaviour.
struct OrderedObject(Vec<(String, serde_json::Value)>);

impl<'de> serde::Deserialize<'de> for OrderedObject {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = OrderedObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("'categories' as an object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                // An index beside the list: a repeated key is found in O(1), so a document
                // with many keys stays linear.
                let mut entries: Vec<(String, serde_json::Value)> = Vec::new();
                let mut index: std::collections::HashMap<String, usize> =
                    std::collections::HashMap::new();
                while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                    match index.get(&key) {
                        Some(&at) => entries[at].1 = value,
                        None => {
                            if entries.len() >= MAX_CATEGORIES {
                                return Err(serde::de::Error::custom(format!(
                                    "more than {MAX_CATEGORIES} categories"
                                )));
                            }
                            index.insert(key.clone(), entries.len());
                            entries.push((key, value));
                        }
                    }
                }
                Ok(OrderedObject(entries))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shipped_file_resolves_by_either_spelling_and_nothing_else_does() {
        for short in available_lineups() {
            assert!(lineup_exists(short), "{short} resolves");
            assert_eq!(normalize_lineup_name(short), Some(short));
            let full = full_filename(short);
            assert!(full.ends_with("_lineup.json"), "{full}");
            assert_eq!(normalize_lineup_name(&full), Some(short));
        }
        assert_eq!(available_lineups().len(), 29);
        for bad in [
            "",
            "   ",
            "US_Combined_lineup.json.bak",
            "../data/merged/lineups/US_Combined_lineup.json",
            "data/merged/lineups/US_Combined_lineup.json",
            "..",
            "US_Combined_lineup.json ",
            "us_combined",
            "US_Combined.json",
            "US_Combined_lineup.JSON",
        ] {
            // The trailing-space full filename trims to the real one and resolves; the
            // rest are unknown. Paths never resolve: there is no filesystem here.
            if bad.trim() == "US_Combined_lineup.json" {
                assert!(lineup_exists(bad), "{bad:?} is the real file with spaces");
            } else {
                assert!(!lineup_exists(bad), "{bad:?} must not resolve");
            }
        }
    }

    #[test]
    fn filename_countries_cover_every_shipped_file() {
        // Nine filename countries across the 29 files (plus QA's beIN-MENA).
        let mut countries: Vec<String> = available_lineups()
            .iter()
            .map(|short| {
                let country = filename_country(&full_filename(short));
                assert!(
                    country.is_some(),
                    "{short} matches {{CC}}_{{Provider}}_lineup.json"
                );
                country.expect("checked")
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        countries.sort();
        assert_eq!(
            countries,
            vec!["AU", "CA", "DO", "ES", "FR", "NL", "PE", "QA", "TZ", "UK", "US"],
            "the shipped set"
        );
        assert_eq!(
            filename_country("US_Combined_lineup.json"),
            Some("US".to_string())
        );
        for bad in [
            "nope.json",
            "USA_TooLong_lineup.json",
            "US_lineup.json",
            "US__lineup.json",
            "us_Combined_lineup.json",
            "",
        ] {
            assert_eq!(filename_country(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn numbers_parse_like_the_plugin() {
        // `_parse_channel_number`: ints as-is, ranges to their first number.
        let int = |n: i64| serde_json::json!(n);
        let text = |s: &str| serde_json::json!(s);
        assert_eq!(parse_channel_number(&int(206)), Some(206));
        assert_eq!(parse_channel_number(&text("923-946")), Some(923));
        assert_eq!(parse_channel_number(&text("104")), Some(104));
        assert_eq!(parse_channel_number(&text("  206  ")), Some(206));
        assert_eq!(parse_channel_number(&text("")), None);
        assert_eq!(parse_channel_number(&text("  ")), None);
        assert_eq!(parse_channel_number(&serde_json::json!(null)), None);
        assert_eq!(parse_channel_number(&serde_json::json!(206.5)), None);
    }

    #[test]
    fn group_names_follow_the_plugin() {
        // `_make_group_name` (+ the `none`-detail sentinel): the separator rule and the
        // prefix-alone rule.
        assert_eq!(make_group_name("DIRECTV", "News"), "DIRECTV: News");
        assert_eq!(make_group_name("DTV-", "News"), "DTV-News");
        assert_eq!(make_group_name("US ", "News"), "US News");
        assert_eq!(make_group_name("", "News"), "News");
        assert_eq!(make_group_name("DIRECTV", "All"), "DIRECTV");
        assert_eq!(make_group_name("DTV-", "All"), "DTV");
        assert_eq!(make_group_name("", "All"), "All");
        assert_eq!(make_group_name("none", "All"), "none");
        // A REAL category named "All" under a detail that keeps it collapses the same
        // way — the accepted, harmless edge case the plugin documents.
        assert_eq!(make_group_name("US", "All"), "US");
    }

    #[test]
    fn group_prefixes_resolve_like_the_plugin() {
        // `_get_group_prefix`: `none` (any case) means no prefix, blank means the first
        // package word, anything else is literal (leading whitespace stripped, trailing
        // separators kept).
        assert_eq!(group_prefix_for("", "DIRECTV Premier"), "DIRECTV");
        assert_eq!(group_prefix_for("   ", "DIRECTV Premier"), "DIRECTV");
        assert_eq!(group_prefix_for("none", "DIRECTV Premier"), "");
        assert_eq!(group_prefix_for(" None ", "DIRECTV Premier"), "");
        assert_eq!(group_prefix_for("NONE", "DIRECTV Premier"), "");
        assert_eq!(group_prefix_for("US ", "DIRECTV Premier"), "US ");
        assert_eq!(group_prefix_for("  DTV-", "x"), "DTV-");
        assert_eq!(group_prefix_for("", ""), "");
    }

    #[test]
    fn detail_folding_matches_the_plugin_maps() {
        // The maps are the plugin's own (`PluginConfig`, `plugin.py:220,230`).
        assert_eq!(SIMPLIFIED_CATEGORIES.len(), 7);
        assert_eq!(REFINED_CATEGORIES.len(), 6);
        // Every US_Combined category is covered by both maps or explicitly caught.
        let lineup = load_lineup("US_Combined", "normal", "").expect("loads");
        let covered_simple: Vec<&str> = SIMPLIFIED_CATEGORIES
            .iter()
            .flat_map(|(_, originals)| originals.iter().copied())
            .collect();
        let covered_refined: Vec<&str> = REFINED_CATEGORIES
            .iter()
            .flat_map(|(_, originals)| originals.iter().copied())
            .collect();
        for category in &lineup.categories {
            assert!(
                covered_simple.contains(&category.as_str())
                    || covered_refined.contains(&category.as_str()),
                "{category} is covered by neither map"
            );
        }
    }

    #[test]
    fn unknown_detail_folds_as_normal() {
        // The plugin's `// Unknown detail value - treat as normal` branch.
        let normal = load_lineup("US_Combined", "normal", "").expect("loads");
        let unknown = load_lineup("US_Combined", "ultra", "").expect("loads");
        assert_eq!(unknown.detail, "normal");
        assert_eq!(unknown.categories, normal.categories);
        assert_eq!(unknown.entries, normal.entries);
    }

    #[test]
    fn all_29_files_load_in_all_4_modes_with_stable_totals() {
        // 7,485 channels across the 29 files (see `data/merged/lineups/README.md`); a
        // detail fold only regroups, it never adds or drops a channel.
        let mut total = 0usize;
        for short in available_lineups() {
            let normal = load_lineup(short, "normal", "").expect("loads");
            total += normal.entries.len();
            for detail in ["none", "refined", "simple"] {
                let folded = load_lineup(short, detail, "").expect("loads");
                assert_eq!(
                    folded.entries.len(),
                    normal.entries.len(),
                    "{short} x {detail}: folds regroup, never drop"
                );
                assert_eq!(folded.detail, detail);
            }
            let none = load_lineup(short, "none", "").expect("loads");
            assert_eq!(none.categories, vec![SINGLE_GROUP_CATEGORY.to_string()]);
        }
        assert_eq!(total, 7_485, "the shipped channel count");
    }

    #[test]
    fn category_order_is_the_file_order() {
        // Python dict order: first position, last value for a repeated key. The shipped
        // files' order is pinned by the parity script against the plugin itself.
        let parsed: LineupJson = serde_json::from_str(
            r#"{"package": 5, "categories": {"Zeta": [1], "Alpha": [], "Zeta": [2], "M\u00fc": []}}"#,
        )
        .expect("parses");
        assert!(parsed.package.is_some_and(|value| !value.is_string()));
        let names: Vec<(String, serde_json::Value)> = parsed.categories.expect("present").0;
        assert_eq!(
            names,
            vec![
                ("Zeta".to_string(), serde_json::json!([2])),
                ("Alpha".to_string(), serde_json::json!([])),
                ("M\u{fc}".to_string(), serde_json::json!([])),
            ]
        );
        assert!(serde_json::from_str::<LineupJson>(r#"{"categories": []}"#).is_err());
        let missing: LineupJson = serde_json::from_str("{}").expect("parses");
        assert!(missing.categories.is_none());
        // Every shipped file keeps its categories, in a stable order.
        for short in available_lineups() {
            let lineup = load_lineup(short, "normal", "").expect("loads");
            assert!(!lineup.categories.is_empty(), "{short}");
        }
    }

    #[test]
    fn countries_resolve_entry_then_category_then_filename() {
        // Most entries carry the filename country; every entry resolves to something.
        let lineup = load_lineup("US_Combined", "normal", "").expect("loads");
        assert_eq!(lineup.country.as_deref(), Some("US"));
        assert!(
            lineup.entries.iter().all(|entry| entry.country.is_some()),
            "no entry is left country-less"
        );
        assert!(
            lineup
                .entries
                .iter()
                .all(|entry| entry.country.as_deref() == Some("US")),
            "single-country lineup, plain theme categories"
        );
        // Entries with a `CC_` name prefix keep their own country (port of the rewrite).
        let mut prefixed = 0usize;
        for short in available_lineups() {
            let lineup = load_lineup(short, "normal", "").expect("loads");
            for entry in &lineup.entries {
                if entry.country.as_deref() != lineup.country.as_deref() {
                    prefixed += 1;
                    // The prefix is stripped from the name and never leaks into it.
                    assert!(
                        !entry.name.contains('_')
                            || !entry.name.chars().nth(2).is_some_and(|c| c == '_'),
                        "{short}: {:?} keeps no prefix",
                        entry.name
                    );
                }
            }
        }
        assert!(prefixed > 0, "the shipped files carry prefixed entries");
    }

    #[test]
    fn aliases_survive_the_load() {
        // TZ files carry `aliases`; they must reach the entries that carry them.
        let lineup = load_lineup("TZ_Combined", "normal", "").expect("loads");
        let aliased: Vec<&LineupEntry> = lineup
            .entries
            .iter()
            .filter(|entry| !entry.aliases.is_empty())
            .collect();
        assert!(!aliased.is_empty(), "TZ entries carry aliases");
    }

    #[test]
    fn prefix_settings_reach_the_group_names() {
        let lineup = load_lineup("US_Combined", "normal", "").expect("loads");
        assert!(
            lineup
                .entries
                .iter()
                .all(|entry| entry.group.starts_with("US: ")),
            "auto prefix from the package name"
        );
        let lineup = load_lineup("US_Combined", "normal", "none").expect("loads");
        assert!(
            lineup
                .entries
                .iter()
                .all(|entry| !entry.group.contains(':')),
            "none means category names only"
        );
        let lineup = load_lineup("US_Combined", "normal", "DTV-").expect("loads");
        assert!(
            lineup
                .entries
                .iter()
                .all(|entry| entry.group.starts_with("DTV-")),
            "a trailing separator appends directly"
        );
        let lineup = load_lineup("US_Combined", "simple", "DTV-").expect("loads");
        assert!(
            lineup
                .entries
                .iter()
                .any(|entry| entry.group == "DTV-Entertainment & Lifestyle"),
            "unmapped categories fold into the simple catch-all, not their own groups"
        );
        let refined = load_lineup("US_Combined", "refined", "").expect("loads");
        let groups: std::collections::BTreeSet<&str> = refined
            .entries
            .iter()
            .map(|entry| entry.group.as_str())
            .collect();
        assert_eq!(groups.len(), 6, "{groups:?}");
    }
}

#[cfg(test)]
mod proptests {
    //! Target 2 of docs/audit/D-inputs.md: `parse_lineup` over generated documents.
    use super::*;
    use crosstalk_matching::country::{lineuparr_category_country, lineuparr_name_country_prefix};
    use proptest::prelude::*;
    use serde_json::{json, Value};

    fn config() -> ProptestConfig {
        // `PROPTEST_CASES` overrides this (10_000 for a local soak).
        ProptestConfig {
            cases: std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|cases| cases.parse().ok())
                .unwrap_or(256),
            ..ProptestConfig::default()
        }
    }

    const CATEGORY_NAMES: &[&str] = &[
        "News",
        "Sports",
        "Regional Sports",
        "Movies",
        "Kids",
        "Faith",
        "Spanish",
        "Music",
        "Entertainment",
        "Foo",
        "All",
        "",
        "UK",
        "Mexico",
        "Ñandú",
        "News & Info",
    ];
    const ENTRY_NAMES: &[&str] = &[
        "CNN",
        "UK_BBC One",
        "US_ESPN",
        "CA_",
        "MX_Canal 5",
        "X_Y",
        "",
        "ESPN HD",
        "Ñ_Ñ",
        "uk_lower",
        "日本",
    ];
    const FILENAMES: &[&str] = &["US_Combined_lineup.json", "UK_X_lineup.json", "weird.json"];
    const DETAILS: &[&str] = &["none", "refined", "simple", "normal", "junk"];

    fn number() -> impl Strategy<Value = Option<Value>> {
        prop_oneof![
            any::<i64>().prop_map(|n| Some(json!(n))),
            Just(Some(json!("923-946"))),
            Just(Some(json!("-5"))),
            Just(Some(json!(" 104 "))),
            Just(Some(json!("99999999999999999999999"))),
            Just(Some(json!(2.5))),
            Just(Some(Value::Null)),
            Just(Some(json!("junk"))),
            Just(None),
        ]
    }

    fn entry() -> impl Strategy<Value = Value> {
        (
            prop_oneof![
                4 => proptest::sample::select(ENTRY_NAMES).prop_map(|name| json!(name)),
                1 => any::<String>().prop_map(|name| json!(name)),
                1 => Just(json!(7)),
                1 => Just(Value::Null),
            ],
            number(),
            proptest::option::of(proptest::collection::vec(
                prop_oneof![
                    Just(json!("Alias")),
                    Just(json!(1)),
                    Just(Value::Null),
                    any::<String>().prop_map(|alias| json!(alias)),
                ],
                0..3,
            )),
        )
            .prop_map(|(name, number, aliases)| {
                let mut object = serde_json::Map::new();
                object.insert("name".to_string(), name);
                if let Some(number) = number {
                    object.insert("number".to_string(), number);
                }
                if let Some(aliases) = aliases {
                    object.insert("aliases".to_string(), Value::Array(aliases));
                }
                Value::Object(object)
            })
    }

    /// `(key, value)` pairs in document order; keys repeat often, values are sometimes
    /// not arrays.
    fn categories() -> impl Strategy<Value = Vec<(String, Value)>> {
        proptest::collection::vec(
            (
                prop_oneof![
                    6 => proptest::sample::select(CATEGORY_NAMES).prop_map(str::to_string),
                    1 => any::<String>(),
                ],
                prop_oneof![
                    8 => proptest::collection::vec(entry(), 0..6).prop_map(Value::Array),
                    1 => Just(json!(5)),
                    1 => Just(json!({"name": "x"})),
                ],
            ),
            0..10,
        )
    }

    /// The document as raw text, so a repeated key reaches the parser as written.
    fn document(package: &Option<Value>, pairs: &[(String, Value)]) -> String {
        let body: Vec<String> = pairs
            .iter()
            .map(|(key, value)| format!("{}: {}", json!(key), value))
            .collect();
        match package {
            Some(package) => format!(
                "{{\"package\": {package}, \"categories\": {{{}}}}}",
                body.join(", ")
            ),
            None => format!("{{\"categories\": {{{}}}}}", body.join(", ")),
        }
    }

    /// First position, last value: what `json.load` makes of repeated keys.
    fn deduped(pairs: &[(String, Value)]) -> Vec<(String, Value)> {
        let mut out: Vec<(String, Value)> = Vec::new();
        for (key, value) in pairs {
            match out.iter_mut().find(|(seen, _)| seen == key) {
                Some((_, slot)) => *slot = value.clone(),
                None => out.push((key.clone(), value.clone())),
            }
        }
        out
    }

    proptest! {
        #![proptest_config(config())]

        #[test]
        fn a_lineup_parses_to_its_documented_invariants(
            package in proptest::option::of(prop_oneof![
                Just(json!("DIRECTV Premier")),
                Just(json!(5)),
                Just(Value::Null),
                any::<String>().prop_map(|text| json!(text)),
            ]),
            pairs in categories(),
            filename in proptest::sample::select(FILENAMES),
            prefix in prop_oneof![Just(""), Just("none"), Just("DTV-"), Just("US "), Just(" X")],
        ) {
            let raw = document(&package, &pairs);
            let kept = deduped(&pairs);
            let total: usize = kept
                .iter()
                .filter_map(|(_, value)| value.as_array().map(Vec::len))
                .sum();
            for detail in DETAILS {
                let lineup = parse_lineup(&raw, filename, detail, prefix).expect("parses");
                prop_assert_eq!(lineup.entries.len(), total, "{} conserves entries", detail);
                for entry in &lineup.entries {
                    prop_assert!(lineup.categories.contains(&entry.category));
                    prop_assert_eq!(&entry.group, &make_group_name(&lineup.prefix, &entry.category));
                }
                let mut seen = std::collections::HashSet::new();
                for category in &lineup.categories {
                    prop_assert!(seen.insert(category), "{} repeats {:?}", detail, category);
                }
                if *detail == "none" {
                    prop_assert_eq!(&lineup.categories, &vec![SINGLE_GROUP_CATEGORY.to_string()]);
                }
            }

            // `normal`: the deduped array-valued categories, in first-position order,
            // entries flattened in file order, countries entry > category > filename.
            let lineup = parse_lineup(&raw, filename, "normal", prefix).expect("parses");
            let arrays: Vec<&(String, Value)> =
                kept.iter().filter(|(_, value)| value.is_array()).collect();
            let names: Vec<String> = arrays.iter().map(|(key, _)| key.clone()).collect();
            prop_assert_eq!(&lineup.categories, &names);
            let mut index = 0;
            for (category, value) in &arrays {
                for raw_entry in value.as_array().expect("array") {
                    let entry = &lineup.entries[index];
                    index += 1;
                    let raw_name = raw_entry.get("name").and_then(Value::as_str).unwrap_or_default();
                    let (prefix_country, stripped) = lineuparr_name_country_prefix(raw_name);
                    prop_assert_eq!(&entry.name, stripped);
                    prop_assert_eq!(&entry.category, category);
                    let expected = prefix_country
                        .map(str::to_string)
                        .or_else(|| lineuparr_category_country(category).map(str::to_string))
                        .or_else(|| filename_country(filename));
                    prop_assert_eq!(&entry.country, &expected);
                    prop_assert_eq!(
                        entry.number,
                        raw_entry.get("number").and_then(parse_channel_number)
                    );
                }
            }
        }

        #[test]
        fn arbitrary_documents_never_panic(raw in any::<String>(), detail in proptest::sample::select(DETAILS)) {
            let _ = parse_lineup(&raw, "US_Combined_lineup.json", detail, "");
        }

        #[test]
        fn arbitrary_json_values_never_panic(
            categories in proptest::collection::vec((any::<String>(), any::<i64>()), 0..5),
            detail in proptest::sample::select(DETAILS),
        ) {
            let object: serde_json::Map<String, Value> = categories
                .into_iter()
                .map(|(key, number)| (key, json!([number, {"name": number}, [number]])))
                .collect();
            let raw = json!({"categories": object}).to_string();
            let _ = parse_lineup(&raw, "US_Combined_lineup.json", detail, "");
        }

        #[test]
        fn a_channel_number_from_a_string_is_never_negative(text in any::<String>()) {
            if let Some(number) = parse_channel_number(&json!(text)) {
                prop_assert!(number >= 0, "{:?} -> {}", text, number);
                prop_assert!(text.trim().starts_with(|c: char| c.is_ascii_digit()), "{:?}", text);
            }
        }

        #[test]
        fn a_channel_number_is_some_only_for_an_int_or_a_digit_led_string(value in prop_oneof![
            any::<i64>().prop_map(|n| json!(n)),
            any::<u64>().prop_map(|n| json!(n)),
            any::<f64>().prop_map(|n| json!(n)),
            "[ ]{0,2}-?[0-9]{0,5}(-[0-9]{0,3})?[ a]{0,2}".prop_map(|text| json!(text)),
            any::<bool>().prop_map(|flag| json!(flag)),
            Just(Value::Null),
        ]) {
            let parsed = parse_channel_number(&value);
            match &value {
                Value::Number(number) => prop_assert_eq!(parsed, number.as_i64()),
                Value::String(text) => {
                    if let Some(number) = parsed {
                        prop_assert!(number >= 0);
                        prop_assert!(text.trim().starts_with(|c: char| c.is_ascii_digit()));
                    }
                }
                _ => prop_assert_eq!(parsed, None),
            }
        }

        #[test]
        fn only_a_shipped_name_resolves(name in any::<String>()) {
            if let Some(short) = normalize_lineup_name(&name) {
                prop_assert!(available_lineups().contains(&short));
                let trimmed = name.trim();
                prop_assert!(trimmed == short || trimmed == full_filename(short));
            }
            if name.contains('/') || name.contains('\\') || name.contains("..") {
                prop_assert_eq!(normalize_lineup_name(&name), None);
            }
        }
    }

    #[test]
    fn ten_thousand_categories_of_ten_parse_quickly() {
        let mut categories = serde_json::Map::new();
        for n in 0..MAX_CATEGORIES {
            let entries: Vec<Value> = (0..10)
                .map(|e| json!({"name": format!("Channel {n} {e}"), "number": e}))
                .collect();
            categories.insert(format!("Category {n}"), Value::Array(entries));
        }
        let raw = json!({"package": "Big Lineup", "categories": categories}).to_string();
        let started = std::time::Instant::now();
        let lineup = parse_lineup(&raw, "US_Big_lineup.json", "normal", "").expect("parses");
        let elapsed = started.elapsed();
        assert_eq!(lineup.entries.len(), MAX_CATEGORIES * 10);
        // One second is the release budget; an unoptimised test build gets ten.
        let budget = if cfg!(debug_assertions) {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_secs(1)
        };
        assert!(elapsed < budget, "{elapsed:?}");

        // One past the cap is refused, not parsed.
        categories.insert("one too many".to_string(), json!([]));
        let raw = json!({"categories": categories}).to_string();
        let error = parse_lineup(&raw, "US_Big_lineup.json", "normal", "").expect_err("capped");
        assert!(format!("{error:#}").contains("categories"), "{error:#}");
    }

    #[test]
    fn parse_lineup_is_what_load_lineup_runs() {
        for short in ["US_Combined", "TZ_Combined"] {
            let raw = merged::merged_lineup_json(short).expect("embedded");
            for detail in DETAILS {
                let loaded = load_lineup(short, detail, "").expect("loads");
                let parsed = parse_lineup(raw, &full_filename(short), detail, "").expect("parses");
                assert_eq!(parsed.entries, loaded.entries);
                assert_eq!(parsed.categories, loaded.categories);
                assert_eq!(parsed.package, loaded.package);
            }
        }
    }
}
