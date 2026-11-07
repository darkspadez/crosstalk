//! One rename/template engine and one collision resolver.
//!
//! Phase 3 step 7 (goal reset, "Also unified"). Three plugins each render names
//! their own way:
//!
//! * triage renders `{name}` formats (`streams.triage::render_format`);
//! * Channel-Mapparr renders OTA names from `{NETWORK}`/`{CITY}`/`{STATE}`/
//!   `{CALLSIGN}` (`_format_ota_name`,
//!   `reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/plugin.py:1772`);
//! * Name Cleaner resolves collisions globally (suffix / numeric / skip,
//!   `reference/dispatcharr-channel-name-cleaner/channel_name_cleaner/plugin.py:233`).
//!
//! [`render_channel`] is the per-channel template (`{name}`, `{group}`,
//! `{number}`, `{epg}`, `{id}`): what `channel_ops`' `RenameKind::Template`
//! calls, with byte-identical output to the renderer it replaces (unknown
//! `{…}` tokens pass through for other tools, empty values render empty, a
//! blank result is refused by the caller). [`render_ota`] is CM's strict OTA
//! template: any placeholder whose value is empty — or any unknown placeholder
//! — yields `None`, exactly like `_format_ota_name` returning `None` when a
//! required field is missing. The two share a tokenizer; they differ on
//! purpose, and the difference is the point: a channel template fills what it
//! knows, an OTA template refuses to invent a station name from half a record.
//!
//! [`resolve_collisions`] scopes collisions GLOBALLY, like Name Cleaner (any
//! two equal names anywhere, case-insensitive). Per-group scoping was
//! considered and rejected: Dispatcharr's outputs list channels FLAT — the
//! HDHomeRun lineup (`lineup.json`, what Plex/Emby/Jellyfin tune from) has no
//! group at all, XMLTV `display-name`s are a flat list, and most players show
//! the M3U `group-title` only as a filter — so two `ESPN`s in different groups
//! are two indistinguishable `ESPN`s in the guide. The resolver never renames
//! a channel the plan does not already rename, so global scope only changes
//! which name a PLANNED rename lands on. The strategies are NC's (`suffix`
//! re-uses a removed quality tag, `numeric` appends `(2)`, `skip` reports the
//! collision), deterministic in channel-id order.

use crosstalk_matching::tags::has_trailing;
use std::collections::{HashMap, HashSet};

/// The placeholders [`render_channel`] understands (channel-ops' set).
pub const CHANNEL_PLACEHOLDERS: [&str; 5] = ["{name}", "{group}", "{number}", "{epg}", "{id}"];

/// The placeholders [`render_ota`] understands (CM's `_format_ota_name` set,
///
/// plus `{format}` for lineup's quality-qualified names).
pub const OTA_PLACEHOLDERS: [&str; 5] =
    ["{NETWORK}", "{CITY}", "{STATE}", "{CALLSIGN}", "{format}"];

/// Values a channel template renders from.
#[derive(Debug, Clone, Default)]
pub struct ChannelValues {
    pub name: String,
    pub group: String,
    pub number: String,
    pub epg: String,
    pub id: String,
}

/// Broadcast-station fields an OTA template renders from, ALREADY cased.
///
/// Build with [`ota_fields`]: CM's case rules are NETWORK upper, CITY title,
/// STATE upper, CALLSIGN through `normalize_callsign` (the `-TV`/`-CD`/`-LP`/
/// `-DT`/`-LD` suffix stripped, case kept as given).
#[derive(Debug, Clone, Default)]
pub struct OtaFields {
    pub network: String,
    pub city: String,
    pub state: String,
    pub callsign: String,
    pub format: String,
}

/// Apply CM's case rules to raw station data (`_format_ota_name`:1785-1796).
///
/// `network_override` (the stream's own stated network) wins over the FCC
/// station's parsed primary affiliation, which can disagree on subchannels.
/// NETWORK reads upper (CM's override comes from `_extract_stream_network`,
/// which already uppercases), CITY title, STATE upper, CALLSIGN through the
/// shared `normalize_callsign` (suffix stripped, case kept). Fields are
/// trimmed first, which CM does not do for city/state (a stray space in an FCC
/// record is not part of the name).
pub fn ota_fields(
    network_raw: &str,
    network_override: Option<&str>,
    city_raw: &str,
    state_raw: &str,
    callsign_raw: &str,
) -> OtaFields {
    OtaFields {
        network: network_override
            .filter(|net| !net.trim().is_empty())
            .map(|net| net.trim().to_string())
            .or_else(|| {
                let parsed = parse_network_affiliation(network_raw);
                if parsed.trim().is_empty() {
                    None
                } else {
                    Some(parsed.trim().to_string())
                }
            })
            .unwrap_or_default()
            .to_uppercase(),
        city: title_case(city_raw.trim()),
        state: state_raw.trim().to_uppercase(),
        callsign: crosstalk_matching::callsign::normalize_callsign(callsign_raw.trim()),
        format: String::new(),
    }
}

/// Title case one city (`birmingham` → `Birmingham`), CM's `.title()`.
///
/// Python's `str.title()` uppercases after EVERY non-letter (so `o'brien` →
/// `O'Brien` and `st. louis` → `St. Louis`); this mirrors that rather than only
/// capitalizing the first letter.
fn title_case(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut word_start = true;
    for c in raw.chars() {
        if c.is_alphabetic() {
            if word_start {
                out.extend(c.to_uppercase());
            } else {
                out.extend(c.to_lowercase());
            }
            word_start = false;
        } else {
            out.push(c);
            word_start = true;
        }
    }
    out
}

/// CM's `_parse_network_affiliation` (plugin.py:1694): the first real network
/// token of a messy FCC affiliation string.
///
/// Legacy `D1-` subchannel prefixes, callsign-prefixed `D1 -` runs and `Ch 3.1`
/// position markers are dropped first; the rest is split on `[;,/&.:]` and
/// whitespace (`:` is a fix over CM's `[;,/&.]`: `NBC: ION` read as `NBC:`, and
/// the rendered `NBC: - CA Eureka (KIEM)` re-renamed on the next run once the
/// stream-network override read `NBC`); LEADING callsign-shaped tokens (`KALB`, `WXYZ-TV`) are dropped
/// because the station is not the network; a trailing `Television Network` /
/// `Network` is stripped; the survivor reads upper. `KALB/NBC` → `NBC`,
/// `CBS Ch 3.1, CW/MTN Ch 3.2` → `CBS`.
pub fn parse_network_affiliation(raw: &str) -> String {
    use std::sync::OnceLock;
    fn re(cell: &'static OnceLock<regex::Regex>, pattern: &str) -> &'static regex::Regex {
        cell.get_or_init(|| regex::Regex::new(pattern).expect("static regex"))
    }
    static D_PREFIX: OnceLock<regex::Regex> = OnceLock::new();
    static CALL_D_PREFIX: OnceLock<regex::Regex> = OnceLock::new();
    static CHANNEL_MARKER: OnceLock<regex::Regex> = OnceLock::new();
    static PARENS: OnceLock<regex::Regex> = OnceLock::new();
    static SPLIT: OnceLock<regex::Regex> = OnceLock::new();
    static CALL: OnceLock<regex::Regex> = OnceLock::new();
    static TRAIL: OnceLock<regex::Regex> = OnceLock::new();
    let mut text = raw.trim().to_string();
    if text.is_empty() {
        return String::new();
    }
    // Legacy subchannel prefixes: `D1-CBS`, `WXYZ-TV D1 - CBS`.
    text = re(&D_PREFIX, r"^D\d+-").replace(&text, "").to_string();
    text = re(
        &CALL_D_PREFIX,
        r"^[KW][A-Z]{3,4}(?:-(?:TV|CD|LP|DT|LD))?\s+D\d+\s*-\s*",
    )
    .replace(&text, "")
    .to_string();
    text = re(&CHANNEL_MARKER, r"(?i)\b(?:CH|CHANNEL)\s*\d+(?:\.\d+)?")
        .replace_all(&text, " ")
        .to_string();
    text = re(&PARENS, r"\([^)]*\)")
        .replace_all(&text, " ")
        .to_string();
    let call = re(&CALL, r"^[KW][A-Z]{2,3}(?:-(?:TV|CD|LP|DT|LD)\d*)?$");
    let mut tokens: Vec<&str> = re(&SPLIT, r"[;,/&.:]|\s+")
        .split(&text)
        .filter(|t| !t.trim().is_empty())
        .collect();
    // Drop leading callsign-shaped tokens: station, not network.
    while tokens
        .first()
        .is_some_and(|t| call.is_match(&t.to_uppercase()))
    {
        tokens.remove(0);
    }
    let Some(first) = tokens.first() else {
        return String::new();
    };
    re(&TRAIL, r"(?i)\s+(?:Television\s+)?Network\s*$")
        .replace(first.trim(), "")
        .trim()
        .to_uppercase()
}

/// The `{word}` placeholders of an OTA template: CM's `re.findall(r'\{(\w+)\}')`.
///
/// Only `\w+` between braces is a placeholder (`{ }`, `{}` and `{a-b}` are
/// literal text, as in CM); the scan restarts after each `{` so `{{CITY}` still
/// finds `{CITY}`.
fn tokens(template: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let word_len = after
            .char_indices()
            .find(|(_, c)| !(c.is_alphanumeric() || *c == '_'))
            .map(|(at, _)| at)
            .unwrap_or(after.len());
        if word_len > 0 && after[word_len..].starts_with('}') {
            out.push(&rest[open..open + 1 + word_len + 1]);
            rest = &after[word_len + 1..];
        } else {
            rest = after;
        }
    }
    out
}

/// The bracket labels a rename format carries: the plugin's `_extract_format_tags`
/// (3393), one parser for every app that reads rename/template suffix rules.
///
/// `{name} [DEAD]` yields `["DEAD"]`, and `[X] {name} [DEAD]` yields `["X", "DEAD"]`.
/// An unclosed bracket yields nothing, which is what the plugin's `findall` does too.
/// Step 9 moved this beside `naming` from streams' triage so lineup's suffix rules read
/// the same parser; streams' `triage::format_tags` delegates here.
pub fn format_tags(format: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut rest = format;
    while let Some(open) = rest.find('[') {
        let Some(close) = rest[open + 1..].find(']') else {
            break;
        };
        let tag = &rest[open + 1..open + 1 + close];
        if !tag.contains('[') {
            tags.push(tag.to_string());
        }
        rest = &rest[open + 1 + close + 1..];
    }
    tags
}

/// Render a per-channel template (channel-ops' `RenameKind::Template`).
///
/// One pass, so a channel literally named `{group}` is not substituted twice.
/// Unknown `{…}` tokens pass through for other tools. Empty values render
/// empty (a channel with no number renders `{number}` as nothing); the caller
/// refuses a blank RESULT, not a blank field — unlike [`render_ota`], which
/// refuses blank FIELDS.
pub fn render_channel(template: &str, values: &ChannelValues) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let Some(close) = rest[open..].find('}') else {
            out.push_str(&rest[open..]);
            return out;
        };
        let token = &rest[open..open + close + 1];
        match token {
            "{name}" => out.push_str(&values.name),
            "{group}" => out.push_str(&values.group),
            "{number}" => out.push_str(&values.number),
            "{epg}" => out.push_str(&values.epg),
            "{id}" => out.push_str(&values.id),
            other => out.push_str(other),
        }
        rest = &rest[open + close + 1..];
    }
    out.push_str(rest);
    out
}

/// Render an OTA station template, CM-strict.
///
/// Every `{word}` placeholder in the template must be a KNOWN field with a
/// NON-EMPTY value, else `None` — `_format_ota_name` returns `None` when any
/// required field is missing (and an unknown field is never in `replacements`,
/// so it refuses too). A template with NO placeholders renders as-is (still
/// `None` when blank).
pub fn render_ota(template: &str, ota: &OtaFields) -> Option<String> {
    for token in tokens(template) {
        let value = match token {
            "{NETWORK}" => &ota.network,
            "{CITY}" => &ota.city,
            "{STATE}" => &ota.state,
            "{CALLSIGN}" => &ota.callsign,
            "{format}" => &ota.format,
            _ => return None,
        };
        if value.trim().is_empty() {
            return None;
        }
    }
    let mut result = template.to_string();
    for (token, value) in [
        ("{NETWORK}", &ota.network),
        ("{CITY}", &ota.city),
        ("{STATE}", &ota.state),
        ("{CALLSIGN}", &ota.callsign),
        ("{format}", &ota.format),
    ] {
        result = result.replace(token, value);
    }
    if result.trim().is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Append a suffix unless the name already ends with it.
///
/// Returns the (possibly unchanged) name and whether it was already there —
/// CM's `[Unk]` fix and the guide's `[BadEPG]` rule share this: a rename never
/// double-appends. The check is [`has_trailing`](crosstalk_matching::tags::has_trailing):
/// bracket-aware and case-insensitive.
pub fn apply_suffix(name: &str, suffix: &str) -> (String, bool) {
    if has_trailing(name, suffix) {
        (name.to_string(), true)
    } else {
        (format!("{name}{suffix}"), false)
    }
}

/// Prepend a prefix unless the name already starts with it (case-insensitive).
pub fn apply_prefix(name: &str, prefix: &str) -> (String, bool) {
    if !prefix.is_empty()
        && name.len() >= prefix.len()
        && name[..prefix.len()].eq_ignore_ascii_case(prefix)
    {
        (name.to_string(), true)
    } else {
        (format!("{prefix}{name}"), false)
    }
}

/// How a planned rename/create resolves a name collision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollisionStrategy {
    /// Re-use the first removed quality tag (`ESPN` + `HD`), else numeric.
    #[default]
    Suffix,
    /// Append `(2)`, `(3)`, … — always.
    Numeric,
    /// Leave the name alone and report the row as a collision.
    Skip,
}

/// One name a plan wants to write.
#[derive(Debug, Clone)]
pub struct PlannedName {
    /// Channel id (plan order ties break on this: deterministic).
    pub id: i64,
    /// The name the channel has now (frees its slot for a sibling).
    pub old_name: String,
    /// The name the plan computed.
    pub new_name: String,
    /// Quality tags the cleaning removed, uppercased (`["HD"]`): what `suffix`
    /// re-appends first.
    pub removed_tags: Vec<String>,
}

/// What one planned name resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedName {
    pub id: i64,
    /// The name to write; for a `skipped` row, the planned name it was refused.
    pub final_name: String,
    /// Only set for `Skip`: the channel keeps its OLD name and is reported.
    pub skipped: bool,
}

fn fold(name: &str) -> String {
    name.to_lowercase()
}

/// Resolve a set of planned names against every existing channel name.
///
/// NC's `_resolve_collisions` (plugin.py:233), global like NC (see the module
/// docs), with two fixes over it:
///
/// * slots are COUNTED: NC subtracts each renamed channel's old name from a
///   SET of existing names, so renaming one of two `ESPN`s away frees `ESPN`
///   while the other still holds it, and a third channel can land on it. Here
///   a rename frees one occupant, not the name;
/// * a `skip` loser keeps its old name, so that name is NOT vacated after all;
///   any single claimant that took it in the first pass collides too and is
///   skipped in turn (repeated to a fixpoint). NC lets the two coincide.
///
/// Otherwise NC's: single-claimant free names keep (pass 1); the rest resolve
/// per strategy in channel-id order (pass 2). A `suffix` hit is
/// `base + sep + REMOVED[0]` (trimmed) when free, else numeric; `numeric`
/// counts from 2 past every taken `base (N)`. Comparison is case-insensitive.
pub fn resolve_collisions(
    planned: &[PlannedName],
    existing: &[String],
    strategy: CollisionStrategy,
    sep: &str,
) -> Vec<ResolvedName> {
    let mut rows: Vec<&PlannedName> = planned.iter().collect();
    rows.sort_by_key(|row| row.id);
    // Occupants per folded name: every existing channel, minus one per rename.
    let mut occupied: HashMap<String, usize> = HashMap::new();
    for name in existing {
        *occupied.entry(fold(name)).or_insert(0) += 1;
    }
    for row in &rows {
        if let Some(count) = occupied.get_mut(&fold(&row.old_name)) {
            *count = count.saturating_sub(1);
        }
    }
    let mut base_counts: HashMap<String, usize> = HashMap::new();
    for row in &rows {
        *base_counts.entry(fold(&row.new_name)).or_insert(0) += 1;
    }
    let is_occupied =
        |occupied: &HashMap<String, usize>, key: &str| occupied.get(key).is_some_and(|n| *n > 0);

    // Pass 1: clearly-unique singles (one claimant, slot free).
    let mut deferred: Vec<&PlannedName> = Vec::new();
    let mut unique: Vec<&PlannedName> = Vec::new();
    for row in &rows {
        let key = fold(&row.new_name);
        if base_counts[&key] == 1 && !is_occupied(&occupied, &key) {
            unique.push(row);
        } else {
            deferred.push(row);
        }
    }

    let mut skipped: HashSet<i64> = HashSet::new();
    if strategy == CollisionStrategy::Skip {
        // Skipped rows keep their old names: re-occupy them, and push any
        // pass-1 claimant of such a name into the skipped set too, until stable.
        let mut pending: Vec<&PlannedName> = deferred.clone();
        while !pending.is_empty() {
            for row in &pending {
                skipped.insert(row.id);
                *occupied.entry(fold(&row.old_name)).or_insert(0) += 1;
            }
            let (bumped, kept): (Vec<&PlannedName>, Vec<&PlannedName>) = unique
                .iter()
                .partition(|row| is_occupied(&occupied, &fold(&row.new_name)));
            unique = kept;
            pending = bumped;
        }
    }
    for row in &unique {
        *occupied.entry(fold(&row.new_name)).or_insert(0) += 1;
    }

    let mut resolved: HashMap<i64, ResolvedName> = HashMap::new();
    for row in &unique {
        resolved.insert(
            row.id,
            ResolvedName {
                id: row.id,
                final_name: row.new_name.clone(),
                skipped: false,
            },
        );
    }
    // Pass 2: collisions, in channel-id order.
    for row in deferred.iter().chain(rows.iter()) {
        if resolved.contains_key(&row.id) {
            continue;
        }
        if skipped.contains(&row.id) {
            resolved.insert(
                row.id,
                ResolvedName {
                    id: row.id,
                    final_name: row.new_name.clone(),
                    skipped: true,
                },
            );
            continue;
        }
        let mut candidate: Option<String> = None;
        if strategy == CollisionStrategy::Suffix {
            if let Some(first) = row.removed_tags.first() {
                let candi = format!("{}{}{}", row.new_name, sep, first.to_uppercase())
                    .trim()
                    .to_string();
                if !is_occupied(&occupied, &fold(&candi)) {
                    candidate = Some(candi);
                }
            }
        }
        let final_name = match candidate {
            Some(candi) => candi,
            None => {
                let mut i = 2;
                loop {
                    let candi = format!("{} ({i})", row.new_name);
                    if !is_occupied(&occupied, &fold(&candi)) {
                        break candi;
                    }
                    i += 1;
                }
            }
        };
        *occupied.entry(fold(&final_name)).or_insert(0) += 1;
        resolved.insert(
            row.id,
            ResolvedName {
                id: row.id,
                final_name,
                skipped: false,
            },
        );
    }
    let mut out: Vec<ResolvedName> = resolved.into_values().collect();
    out.sort_by_key(|row| row.id);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(name: &str) -> ChannelValues {
        ChannelValues {
            name: name.to_string(),
            group: "News".to_string(),
            number: "12".to_string(),
            epg: "CNN".to_string(),
            id: "7".to_string(),
        }
    }

    #[test]
    fn channel_templates_render_all_five_placeholders() {
        let values = channel("ESPN");
        assert_eq!(
            render_channel("{name} ({group}) #{number} {epg} {id}", &values),
            "ESPN (News) #12 CNN 7"
        );
        // Unknown tokens pass through for other tools; unclosed braces stay.
        assert_eq!(
            render_channel("{name} {NETWORK} {other} {unclosed", &values),
            "ESPN {NETWORK} {other} {unclosed"
        );
        // One pass: a value that looks like a placeholder is not re-expanded.
        let tricky = ChannelValues {
            name: "{group}".to_string(),
            ..channel("ignored")
        };
        assert_eq!(render_channel("{name}", &tricky), "{group}");
        // Empty values render empty (the caller refuses a blank result).
        let empty = ChannelValues::default();
        assert_eq!(render_channel("[{number}]", &empty), "[]");
    }

    #[test]
    fn ota_templates_are_strict_and_cased() {
        let ota = ota_fields("cbs & fox", None, "birmingham", "al", "WIAT");
        assert_eq!(ota.network, "CBS");
        assert_eq!(ota.city, "Birmingham");
        assert_eq!(ota.state, "AL");
        let template = "{NETWORK} - {STATE} {CITY} ({CALLSIGN})";
        assert_eq!(
            render_ota(template, &ota),
            Some("CBS - AL Birmingham (WIAT)".to_string())
        );
        // The stream's own network wins over the FCC affiliation.
        let over = ota_fields("CBS", Some("fox"), "miami", "fl", "WFOR-TV");
        assert_eq!(
            render_ota(template, &over),
            Some("FOX - FL Miami (WFOR)".to_string())
        );
        // Any empty required field refuses (CM returns None the same way).
        let mut missing = ota.clone();
        missing.city.clear();
        assert_eq!(render_ota(template, &missing), None);
        // Unknown placeholders refuse too.
        assert_eq!(render_ota("{name} ({CALLSIGN})", &ota), None);
        // No placeholders renders as-is; blank refuses.
        assert_eq!(render_ota("CNN", &ota), Some("CNN".to_string()));
        assert_eq!(render_ota("   ", &ota), None);
    }

    #[test]
    fn affiliation_parsing_takes_the_first_real_network() {
        assert_eq!(
            parse_network_affiliation("CBS Ch 3.1, CW/MTN Ch 3.2"),
            "CBS"
        );
        assert_eq!(parse_network_affiliation("KALB/NBC"), "NBC");
        assert_eq!(
            parse_network_affiliation("ABC (main) CBS (multicast)"),
            "ABC"
        );
        assert_eq!(parse_network_affiliation("FOX Television Network"), "FOX");
        assert_eq!(parse_network_affiliation("WXYZ-TV ABC"), "ABC");
        assert_eq!(parse_network_affiliation("D1-CBS"), "CBS");
        assert_eq!(parse_network_affiliation("NBC: ION"), "NBC");
        assert_eq!(parse_network_affiliation(""), "");
    }

    #[test]
    fn suffix_and_prefix_never_double_up() {
        assert_eq!(
            apply_suffix("ESPN", " [HD]"),
            ("ESPN [HD]".to_string(), false)
        );
        assert_eq!(
            apply_suffix("ESPN [HD]", " [HD]"),
            ("ESPN [HD]".to_string(), true)
        );
        assert_eq!(
            apply_suffix("ESPN [hd]", "[HD]"),
            ("ESPN [hd]".to_string(), true)
        );
        assert_eq!(
            apply_prefix("ESPN", "US: "),
            ("US: ESPN".to_string(), false)
        );
        assert_eq!(
            apply_prefix("US: ESPN", "us: "),
            ("US: ESPN".to_string(), true)
        );
    }

    fn mk(id: i64, old: &str, new: &str, removed: &[&str]) -> PlannedName {
        PlannedName {
            id,
            old_name: old.to_string(),
            new_name: new.to_string(),
            removed_tags: removed.iter().map(|t| t.to_string()).collect(),
        }
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn collisions_resolve_globally_deterministically() {
        let planned = vec![mk(2, "A", "ESPN", &["HD"]), mk(1, "B", "ESPN", &[])];
        let existing = names(&["CNN", "A", "B"]);
        // Both claim ESPN, so both defer the unique pass (NC's rule); in id order
        // id 1 has no removed tag and goes numeric, id 2 re-appends its quality.
        let out = resolve_collisions(&planned, &existing, CollisionStrategy::Suffix, " ");
        assert_eq!(
            out,
            vec![
                ResolvedName {
                    id: 1,
                    final_name: "ESPN (2)".to_string(),
                    skipped: false
                },
                ResolvedName {
                    id: 2,
                    final_name: "ESPN HD".to_string(),
                    skipped: false
                },
            ]
        );
        // Global: an existing ESPN anywhere (no groups at all) blocks the name.
        let planned = vec![mk(1, "ESPN HD", "ESPN", &["HD"])];
        let out = resolve_collisions(
            &planned,
            &names(&["ESPN HD", "ESPN"]),
            CollisionStrategy::Suffix,
            " ",
        );
        // `ESPN HD` is the renamed channel's own vacated name, so suffix lands back on it.
        assert_eq!(out[0].final_name, "ESPN HD");
        // Skip reports instead of renaming.
        let out = resolve_collisions(
            &planned,
            &names(&["ESPN HD", "ESPN"]),
            CollisionStrategy::Skip,
            " ",
        );
        assert!(out[0].skipped);
        // A swap frees both slots: two channels trading names both keep.
        let planned = vec![mk(1, "A", "B", &[]), mk(2, "B", "A", &[])];
        let out = resolve_collisions(
            &planned,
            &names(&["A", "B"]),
            CollisionStrategy::Numeric,
            " ",
        );
        assert_eq!(out[0].final_name, "B");
        assert_eq!(out[1].final_name, "A");
        // Numeric counts past taken `base (N)` names.
        let planned = vec![mk(1, "A", "ESPN", &[]), mk(2, "B", "ESPN", &[])];
        let out = resolve_collisions(
            &planned,
            &names(&["A", "B", "ESPN (2)"]),
            CollisionStrategy::Numeric,
            " ",
        );
        // id order: id 1 skips the taken `ESPN (2)` for `(3)`, id 2 takes `(4)`.
        assert_eq!(out[0].final_name, "ESPN (3)");
        assert_eq!(out[1].final_name, "ESPN (4)");
    }

    #[test]
    fn a_rename_frees_one_occupant_not_the_name() {
        // Two channels are named ESPN; one is renamed away. NC's set arithmetic
        // freed `ESPN` entirely and let a third rename land on it, leaving two.
        let planned = vec![mk(1, "ESPN", "ESPN 2", &[]), mk(3, "ESPN HD", "ESPN", &[])];
        let out = resolve_collisions(
            &planned,
            &names(&["ESPN", "ESPN", "ESPN HD"]),
            CollisionStrategy::Numeric,
            " ",
        );
        assert_eq!(out[0].final_name, "ESPN 2");
        assert_eq!(out[1].final_name, "ESPN (2)");
    }

    #[test]
    fn a_skipped_rename_keeps_its_old_name_occupied() {
        // id 1 (X → Y) collides with an existing Y and is skipped, so it stays X;
        // id 2 (Z → X) was a clean single claimant but X is still held: skipped too.
        let planned = vec![mk(1, "X", "Y", &[]), mk(2, "Z", "X", &[])];
        let out = resolve_collisions(
            &planned,
            &names(&["X", "Y", "Z"]),
            CollisionStrategy::Skip,
            " ",
        );
        assert!(out.iter().all(|row| row.skipped), "{out:?}");
        // Without the collision, both go through.
        let out = resolve_collisions(&planned, &names(&["X", "Z"]), CollisionStrategy::Skip, " ");
        assert!(out.iter().all(|row| !row.skipped), "{out:?}");
    }

    #[test]
    fn ota_placeholders_are_word_tokens_like_cm() {
        let ota = ota_fields("CBS", None, "miami", "fl", "WFOR");
        // `{ }` / `{a-b}` are not placeholders in CM (`\{(\w+)\}`): literal text.
        assert_eq!(
            render_ota("{NETWORK} {a-b} { }", &ota),
            Some("CBS {a-b} { }".to_string())
        );
        // A word placeholder CM does not know refuses.
        assert_eq!(render_ota("{NETWORK} {zone}", &ota), None);
        assert_eq!(render_ota("{{CALLSIGN}}", &ota), Some("{WFOR}".to_string()));
    }
}
