//! The unified tag vocabulary: every token the writers classify, in one table.
//!
//! Phase 3 step 7 (goal reset, "Also unified"). The table lives in
//! `data/merged/tags.json` (built by `scripts/data/build_merged.py` from the
//! four plugins plus Name Cleaner and IPTV Checker, `--check` covers it) and is
//! embedded here; [`TAG_TABLE`] parses it lazily. Each entry carries the
//! plugins that use it (`sources`), so an operator can see why a token counts.
//!
//! # Kinds
//!
//! | kind | what | writers |
//! |------|------|---------|
//! | quality | picture/format tokens: `HD`/`FHD`/`UHD`/`4K`/`8K`/`SD`/`HEVC`/`RAW`, resolutions (`1080p`, `720p`, `\d{3,4}p`), superscript marks (`ᴴᴰ` …), IC's `Unknown` | IC format suffixes, triage, LU tiers |
//! | junk | packaging tokens Name Cleaner strips as junk-only segments: `VIP`, `MULTI`, `BACKUP`, `GEO`, `LOW`, `DUAL`, `HQ`, `LQ` | clean |
//! | status | state tags writers append: `[DEAD]`/`[Slow]`/`[Blank]` (IC/CM), `[Unk]` (CM), `[BadEPG]` (guide) | triage, restore, guide cleanup |
//! | regional | feed-region words: `East`/`West`/`Pacific`/`Central`/`Mountain`/`Atlantic` | SM extract, CM re-attachment |
//! | timeshift | `+1`/`+2` markers | zone/shift guards, clean |
//! | country_prefix | anchored `CC:`/`[CC]`/`(CC)`/`CC_`/`USA …` marker codes (the SM+LU union) | country detector, clean |
//!
//! # Relation to the normalizer (read before changing either)
//!
//! The unified engine's normalizer tables (decided rows N01–N14) stay EXACTLY as
//! decided: they strip for COMPARISON, and step 6's corpus gate pins them. This
//! vocabulary is the SUPERSET used when WRITING names — cleaning, tagging,
//! templates, logos — and it is deliberately wider than any one normalizer
//! subset (Name Cleaner lumps `VIP`/`MULTI`/… with picture tokens; the
//! vocabulary splits them into junk). Never consult this table from the match
//! path: a token's writability says nothing about its comparability.
//!
//! # Matching rules
//!
//! [`classify_token`] matches one token case-insensitively, with or without its
//! brackets (`HD`, `[HD]` and `(HD)` are the same token; `Unknown` is quality
//! while `Unk` is status — the table is exact, not prefix-based). Patterns the
//! table cannot enumerate are recognized structurally: `\d{3,4}p` resolutions,
//! `FHD\d+`, `H.265`/`H.264` spellings, `+\d` shifts and bare country codes.
//!
//! [`split_tags`] strips the TRAILING tag run off a name (`ESPN (East) [HD]`
//! → `ESPN` + `[East, HD]`), stopping at the first group that is not a known
//! tag (`CNN (Moldova)` keeps its parens). A bare trailing region word counts
//! (`HBO West` → `HBO` + `[West]`), mirroring CM's `build_final_channel_name`
//! which re-attaches the region WITHOUT parens — except `Central`/`Atlantic`,
//! which end real names (`Comedy Central`, `Sky Atlantic`) and so only count
//! bracketed, and a region word after `Middle`/`Far`/`Wild` (a place name). Leading markers (`US: ESPN`)
//! are NOT tags: they are the country detector's business and stay in the core.

use std::borrow::Cow;
use std::sync::OnceLock;

/// What a token IS, for a writer deciding what to strip, keep or append.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TagKind {
    Quality,
    Junk,
    Status,
    Regional,
    Timeshift,
    CountryPrefix,
}

impl TagKind {
    /// The `tags.json` spelling.
    pub fn label(self) -> &'static str {
        match self {
            TagKind::Quality => "quality",
            TagKind::Junk => "junk",
            TagKind::Status => "status",
            TagKind::Regional => "regional",
            TagKind::Timeshift => "timeshift",
            TagKind::CountryPrefix => "country_prefix",
        }
    }

    /// Parse a `tags.json` kind label.
    pub fn parse(label: &str) -> Option<Self> {
        match label {
            "quality" => Some(TagKind::Quality),
            "junk" => Some(TagKind::Junk),
            "status" => Some(TagKind::Status),
            "regional" => Some(TagKind::Regional),
            "timeshift" => Some(TagKind::Timeshift),
            "country_prefix" => Some(TagKind::CountryPrefix),
            _ => None,
        }
    }
}

/// One vocabulary row: the token, its kind, and the plugins that use it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TagEntry {
    pub token: &'static str,
    pub kind: TagKind,
    pub sources: &'static [&'static str],
}

/// A classified token: its kind, canonical spelling and sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub kind: TagKind,
    /// Canonical spelling: uppercased for word tokens (`hd` → `HD`), `+N` for
    /// shifts, the dotted form for codecs (`h265` → `H.265`).
    pub canonical: String,
    pub sources: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
struct OwnedEntry {
    token: &'static str,
    kind: TagKind,
    sources: &'static [&'static str],
}

fn table() -> &'static [OwnedEntry] {
    static TABLE: OnceLock<Vec<OwnedEntry>> = OnceLock::new();
    TABLE.get_or_init(|| {
        // tags.json is written sorted by (kind, casefolded token); the parse
        // keeps file order so diagnostics read deterministically. Strings are
        // leaked once here: the table lives for the process lifetime anyway.
        let raw = crate::data::merged::MERGED_TAGS_JSON;
        let value: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
        let mut out = Vec::new();
        if let Some(tags) = value.get("tags").and_then(|v| v.as_array()) {
            for entry in tags {
                let token: &'static str = entry
                    .get("token")
                    .and_then(|t| t.as_str())
                    .map(|t| Box::leak(t.to_string().into_boxed_str()) as &'static str)
                    .unwrap_or("");
                let kind = entry
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .and_then(TagKind::parse)
                    .unwrap_or(TagKind::Junk);
                let sources: &'static [&'static str] = entry
                    .get("sources")
                    .and_then(|s| s.as_array())
                    .map(|list| {
                        list.iter()
                            .filter_map(|s| s.as_str())
                            .map(|s| Box::leak(s.to_string().into_boxed_str()) as &'static str)
                            .collect::<Vec<_>>()
                            .into_boxed_slice()
                    })
                    .map(|slice| Box::leak(slice) as &'static mut [&'static str])
                    .map(|leaked| leaked as &'static [&'static str])
                    .unwrap_or(&[]);
                out.push(OwnedEntry {
                    token,
                    kind,
                    sources,
                });
            }
        }
        out
    })
}

/// The whole vocabulary, in `tags.json` order.
pub fn tag_table() -> Vec<TagEntry> {
    table()
        .iter()
        .map(|entry| TagEntry {
            token: entry.token,
            kind: entry.kind,
            sources: entry.sources,
        })
        .collect()
}

/// The vocabulary size per kind, for reports.
pub fn vocabulary_counts() -> Vec<(TagKind, usize)> {
    let mut counts = [0usize; 6];
    for entry in table() {
        counts[entry.kind as usize] += 1;
    }
    [
        TagKind::Quality,
        TagKind::Junk,
        TagKind::Status,
        TagKind::Regional,
        TagKind::Timeshift,
        TagKind::CountryPrefix,
    ]
    .into_iter()
    .map(|kind| (kind, counts[kind as usize]))
    .collect()
}

/// The vocabulary tokens of one kind that one plugin uses, in `tags.json` order.
///
/// The single source for a writer that needs "the tags plugin X appends":
/// streams' restore reads IC's status tags here (`DEAD`/`Slow`/`Blank`)
/// rather than keeping its own list.
pub fn tokens_from(kind: TagKind, source: &str) -> Vec<&'static str> {
    table()
        .iter()
        .filter(|entry| entry.kind == kind && entry.sources.contains(&source))
        .map(|entry| entry.token)
        .collect()
}

fn lookup(bare_upper: &str) -> Option<(TagKind, &'static [&'static str])> {
    // Uppercased token → first row, built once: classify runs per token per
    // channel, and a linear scan with a fresh uppercase per row would dominate.
    static INDEX: OnceLock<std::collections::HashMap<String, (TagKind, &'static [&'static str])>> =
        OnceLock::new();
    INDEX
        .get_or_init(|| {
            let mut index = std::collections::HashMap::new();
            for entry in table() {
                index
                    .entry(entry.token.to_uppercase())
                    .or_insert((entry.kind, entry.sources));
            }
            index
        })
        .get(bare_upper)
        .copied()
}

/// Normalize a codec spelling: `H265`/`H 265` → `H.265`.
fn codec_canon(stripped: &str) -> Option<String> {
    let compact: String = stripped
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    match compact.as_str() {
        "H265" => Some("H.265".to_string()),
        "H264" => Some("H.264".to_string()),
        _ => None,
    }
}

/// Whether the token is a `+\d` time-shift marker (SM's `plus_shift_re` shape,
///
/// trimmed to one token: `+1`, `+ 1`, `+12`).
fn shift_canon(token: &str) -> Option<String> {
    let mut chars = token.chars();
    if chars.next() != Some('+') {
        return None;
    }
    let digits: String = chars.filter(|c| !c.is_whitespace()).collect();
    if !digits.is_empty() && digits.len() <= 2 && digits.chars().all(|c| c.is_ascii_digit()) {
        Some(format!("+{digits}"))
    } else {
        None
    }
}

/// Whether a pipe segment is junk Name Cleaner would drop: a single quality/junk
/// token (NC's `QUALITY_SEGMENT`, plugin.py:46: `^\s*(?:QUALITY)\s*$`).
///
/// The token lists come from this vocabulary, not from a re-declared regex:
/// `classify_token` covers the tabled tokens (`HD`, `VIP`, `MULTI-AUDIO`, …)
/// plus the structural shapes NC's pattern enumerates (`\d{3,4}p`, `FHD\d+`,
/// `H.265`/`H.264` spellings). Anything else — prose, callsigns, bare numbers —
/// is not junk (`RTE`, `Moldova`). Case-insensitive, like NC's `re.IGNORECASE`.
/// A `+N` shift is timeshift, not quality/junk, so it never counts here. Only tokens
/// NC itself uses (`sources` contains `"NC"`) count: the vocabulary is wider than
/// any one plugin (IC's `Unknown` is quality here but NC keeps it; LU's bare
/// `1080` is quality here but NC's `\d{3,4}p` needs the `p`).
pub fn is_pipe_junk_segment(segment: &str) -> bool {
    classify_token(segment).is_some_and(|tag| {
        matches!(tag.kind, TagKind::Quality | TagKind::Junk) && tag.sources.contains(&"NC")
    })
}

/// Whether a bracket's inner text is junk Name Cleaner would strip (NC's
/// `BRACKET_JUNK_INNER`, plugin.py:49:
/// `^(?:\s*(?:\d{3,4}p|QUALITY)\s*[/,&+]?\s*)+$`, `re.IGNORECASE`).
///
/// The structural shape — junk tokens joined by `/`, `,`, `&` or `+` — is not a
/// token and so cannot live in `tags.json`; this is the one structural pattern the
/// vocabulary adds for NC (step 14 review). Each `/,&+`-separated run must split
/// on whitespace into tokens that are all NC quality/junk per [`classify_token`]
/// (which carries NC's exact sets: the tabled tokens plus `\d{3,4}p`, `FHD\d+`
/// and the codec spellings; only `sources` containing `"NC"` count, as above).
/// `(1080p)`, `[VIP]` and `(720p / HEVC)` strip; `(Moldova)`, `(4K + 1)` and
/// `(East)` do not (a region is not junk).
pub fn is_bracket_junk_inner(inner: &str) -> bool {
    if inner.trim().is_empty() {
        return false;
    }
    // NC matches one bracket pair at a time (`\(([^()]*)\)`, `\[([^\[\]]*)\]`):
    // a nested bracket of either shape never matches the outer pair first, and the
    // junk pattern itself cannot match a bracket char, so any bracket in here keeps it.
    if inner.contains(['[', ']', '(', ')']) {
        return false;
    }
    let mut any_word = false;
    for run in inner.split(['/', ',', '&', '+']) {
        let run = run.trim();
        if run.is_empty() {
            // A leading/trailing/doubled separator: NC's trailing `[/,&+]?` allows it.
            continue;
        }
        for word in run.split_whitespace() {
            any_word = true;
            let junk = classify_token(word).is_some_and(|tag| {
                matches!(tag.kind, TagKind::Quality | TagKind::Junk) && tag.sources.contains(&"NC")
            });
            if !junk {
                return false;
            }
        }
    }
    any_word
}

/// The core's delimited country-prefix codes (`CORE_PROVIDER_PREFIX_PATTERNS` rows
/// 1 and 6 in `normalize.rs`): the codes [`collapse_pipe_segments`] keeps a
/// delimiter beside, compared ASCII-case-insensitively like the core's tables.
const PIPE_COUNTRY_CODES: [&str; 13] = [
    "US", "USA", "UK", "CA", "AU", "FR", "DE", "ES", "IT", "NL", "BR", "MX", "IN",
];

/// Whether a `|` segment is a bare country code the engine keeps delimited (N15):
/// the ONE definition lineup's `clean` shares, so a cleaned name never strands the
/// code the engine would have read as a marker.
pub fn is_pipe_country_code(token: &str) -> bool {
    token.is_ascii() && PIPE_COUNTRY_CODES.contains(&token.to_ascii_uppercase().as_str())
}

/// Collapse `|`-separated segments for the engine's key/zone/country reads
/// (decided N15, step-14 review).
///
/// Providers delimit packaging junk with pipes (`Cinemax East | HD |`,
/// `UK | Sky Sports | HD`): before keying and before the zone/country reads the
/// engine treats `|` as a segment separator, drops segments that are empty or pure
/// junk per [`is_pipe_junk_segment`] (the ONE shared definition — Name Cleaner's
/// exact set, step 14), and joins the survivors with a single space. A name with a
/// meaningful pipe segment keeps it (`Sky Sports | Premier League` -> `Sky Sports
/// Premier League`). Without a `|` the name borrows unchanged; when every segment
/// drops (a name that is only junk and pipes) the collapse is empty.
///
/// A country code keeps a delimiter the core's tables already strip, so the pipe
/// never strands a bare country word: a segment ending in a code joins with `: `
/// (`UK | Sky Sports | HD` -> `UK: Sky Sports`, read and stripped like `UK: Sky
/// Sports`; `RK: US | CNN` -> `RK: US: CNN`, whose `US:` the geographic table takes
/// after the `RK:` prefix goes), and a final code-only segment keeps its bar
/// (`BBC One | UK` stays, the core's trailing `| UK` strip). A lone code is no
/// marker: `USA | HD |` (USA Network) collapses to `USA`, country-less like the
/// cleaned name.
///
/// `+N` shifts are timeshift, never pipe junk, so they survive (`ESPN West +1 |
/// HD |` -> `ESPN West +1`). The zone strip ([`strip_zone_ignorable`]) runs after
/// this, on the collapsed form.
///
/// Byte parity with `scripts/corpus/ablate.py::collapse_pipe_segments` (which reads
/// the same `data/merged/tags.json` vocabulary) is pinned by `engine_parity.py`
/// (`pipe` kind: every corpus name bare and pipe-decorated) plus the battery in
/// `ablate.py --self-test`. `str::trim`/`split_whitespace` are Unicode White_Space,
/// which is exactly the `_RUST_WS` set the Python mirror uses (Python's
/// `str.isspace` also counts U+001C..U+001F).
pub fn collapse_pipe_segments(name: &str) -> Cow<'_, str> {
    if !name.contains('|') {
        return Cow::Borrowed(name);
    }
    let mut kept: Vec<&str> = Vec::new();
    for part in name.split('|') {
        let piece = part.trim();
        if piece.is_empty() {
            continue;
        }
        if is_pipe_junk_segment(piece) {
            continue;
        }
        kept.push(piece);
    }
    let mut out = String::with_capacity(name.len());
    for (index, piece) in kept.iter().enumerate() {
        if index > 0 {
            let previous_ends_in_code = kept[index - 1]
                .split_whitespace()
                .next_back()
                .is_some_and(is_pipe_country_code);
            if previous_ends_in_code {
                out.push_str(": ");
            } else if index == kept.len() - 1 && is_pipe_country_code(piece) {
                out.push_str(" | ");
            } else {
                out.push(' ');
            }
        }
        out.push_str(piece);
    }
    Cow::Owned(out)
}

/// Whether the token is a resolution (`1080p`, `720P`, `4K`-style bare is in
/// the table; this is the `\d{3,4}p` pattern NC's `BRACKET_JUNK_INNER` matches).
/// Char-based: byte slicing would split multi-byte marks (`ᴴᴰ`).
fn resolution_canon(upper: &str) -> Option<String> {
    let mut chars: Vec<char> = upper.chars().collect();
    if chars.pop() != Some('P') {
        return None;
    }
    if (3..=4).contains(&chars.len()) && chars.iter().all(|c| c.is_ascii_digit()) {
        return Some(upper.to_string());
    }
    None
}

/// Classify ONE token, with or without its brackets.
///
/// Returns `None` for prose (`Moldova`), callsigns (`WABC`), numbers that are
/// not resolutions and anything empty. Country codes resolve through the table
/// (the SM+LU union is enumerated there); the generic alpha check is only a
/// backstop and never fires for tabled codes.
pub fn classify_token(token: &str) -> Option<Tag> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(canon) = shift_canon(trimmed) {
        let sources = lookup("+1").map(|(_, s)| s).unwrap_or(&[]);
        return Some(Tag {
            kind: TagKind::Timeshift,
            canonical: canon,
            sources,
        });
    }
    // One bracket layer: `[HD]`/`(HD)` → `HD`. Nested or unbalanced stays literal.
    let bare = {
        let bytes = trimmed.as_bytes();
        if bytes.len() >= 2
            && ((bytes[0] == b'[' && bytes[bytes.len() - 1] == b']')
                || (bytes[0] == b'(' && bytes[bytes.len() - 1] == b')'))
        {
            &trimmed[1..trimmed.len() - 1]
        } else {
            trimmed
        }
    };
    let bare = bare.trim();
    if bare.is_empty() {
        return None;
    }
    let upper = bare.to_uppercase();
    if let Some(canon) = resolution_canon(&upper) {
        let sources = lookup("1080P").map(|(_, s)| s).unwrap_or(&[]);
        return Some(Tag {
            kind: TagKind::Quality,
            canonical: canon,
            sources,
        });
    }
    // `FHD1080` (NC's `FHD\d*` quality segment).
    if upper.starts_with("FHD") && upper.len() > 3 && upper[3..].chars().all(|c| c.is_ascii_digit())
    {
        let sources = lookup("FHD1080").map(|(_, s)| s).unwrap_or(&[]);
        return Some(Tag {
            kind: TagKind::Quality,
            canonical: upper,
            sources,
        });
    }
    if let Some(canon) = codec_canon(&upper) {
        let sources = lookup(&canon).map(|(_, s)| s).unwrap_or(&[]);
        return Some(Tag {
            kind: TagKind::Quality,
            canonical: canon,
            sources,
        });
    }
    if let Some((kind, sources)) = lookup(&upper) {
        return Some(Tag {
            kind,
            canonical: if kind == TagKind::Timeshift {
                upper
            } else {
                canonical_spelling(kind, &upper)
            },
            sources,
        });
    }
    None
}

/// Canonical spelling for a tabled token: uppercase, except regions which read
/// title-cased (`east` → `East`, matching SM's `extract_tags` output).
fn canonical_spelling(kind: TagKind, upper: &str) -> String {
    if kind == TagKind::Regional {
        let mut chars = upper.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => first.to_string() + &chars.as_str().to_lowercase(),
        }
    } else {
        upper.to_string()
    }
}

/// Whether `name` already ends with `tag`, bracket-aware and case-insensitive.
///
/// `tag` may carry its own brackets or not: `has_trailing("ESPN [HD]", "HD")`,
/// `has_trailing("ESPN [HD]", "[HD]")` and `has_trailing("ESPN [HD]", "hd")`
/// are all true. A suffix/prefix applier asks this before appending, so a tag
/// is never doubled. A bare regional word (`HBO West` vs `West`) does NOT
/// count: only a bracketed group or the exact string does.
pub fn has_trailing(name: &str, tag: &str) -> bool {
    let trimmed = name.trim_end();
    let tag = tag.trim();
    if tag.is_empty() {
        return false;
    }
    // A bare word token (`HD`, `West`) never matches a bare ending: only a
    // bracketed group or a literal affix (`[HD]`, ` [BadEPG]`, `+1`, `ᴴᴰ`)
    // counts. Without this, `HBO West` would "end with" the region `West`.
    let is_word = tag.chars().all(|c| c.is_ascii_alphanumeric());
    // Compared as BYTES: slicing the str at `len - tag.len()` could land inside
    // a multi-byte char (`Café` vs `!`) and panic. Equal trailing bytes of two
    // valid UTF-8 strings start at a char boundary, so a byte match is a str match.
    if !is_word
        && trimmed.len() >= tag.len()
        && trimmed.as_bytes()[trimmed.len() - tag.len()..].eq_ignore_ascii_case(tag.as_bytes())
    {
        return true;
    }
    let inner = {
        let bytes = tag.as_bytes();
        if bytes.len() >= 2
            && ((bytes[0] == b'[' && bytes[bytes.len() - 1] == b']')
                || (bytes[0] == b'(' && bytes[bytes.len() - 1] == b')'))
        {
            tag[1..tag.len() - 1].trim()
        } else {
            tag
        }
    };
    if inner.is_empty() {
        return false;
    }
    let Some(without_close) = trimmed.strip_suffix(']') else {
        return trimmed
            .strip_suffix(')')
            .and_then(|without| without.rfind('(').map(|open| without[open + 1..].trim()))
            .is_some_and(|got| got.eq_ignore_ascii_case(inner));
    };
    without_close
        .rfind('[')
        .map(|open| without_close[open + 1..].trim())
        .is_some_and(|got| got.eq_ignore_ascii_case(inner))
}

fn trailing_bracket(name: &str) -> Option<(usize, &str, u8)> {
    let trimmed = name.trim_end();
    if let Some(without) = trimmed.strip_suffix(']') {
        return without
            .rfind('[')
            .map(|open| (open, without[open + 1..].trim(), b']'));
    }
    if let Some(without) = trimmed.strip_suffix(')') {
        return without
            .rfind('(')
            .map(|open| (open, without[open + 1..].trim(), b')'));
    }
    None
}

/// Strip the trailing tags writers append — quality, junk and status kinds — plus
/// time-shift markers, for feed-zone detection (decided Z08).
///
/// A zone word followed by a trailing tag is still a zone word: `HBO West (Backup)`,
/// `HBO West [4K]` and `HBO (West) [HD]` read West; `ITV1 (Central West) HD` reads
/// West; `ESPN West +1` reads West (and stays timeshift: the shift is stripped for the
/// zone read only, [`crate::engine::is_time_shift`] still reads the original name).
/// Regional words are never stripped here — a bare `West` stops the run, so the zone
/// patterns see it — and neither are unknown groups (`CNN (Moldova)` is untouched) or
/// country markers. The run never eats the whole name (`West` alone stays `West`).
/// Only the trailing RUN strips, exactly like [`split_tags`]: the first group that is
/// not a strippable tag stops the peel.
pub fn strip_zone_ignorable(name: &str) -> &str {
    strip_trailing_writer_tags(name, true)
}

/// Strip the trailing writer tags for the callsign read (decided C09): the same peel
/// as [`strip_zone_ignorable`] WITHOUT the time-shift marker, which stays in place so
/// a `+N` feed keeps the shared core's low-confidence read (`Local CBS KMOV +1` is not
/// anchored onto `Local CBS KMOV`; the shift is identity).
pub fn strip_writer_tags(name: &str) -> &str {
    strip_trailing_writer_tags(name, false)
}

fn strip_trailing_writer_tags(name: &str, shifts: bool) -> &str {
    let mut rest = name.trim_end();
    loop {
        let shift = if shifts { trailing_shift(rest) } else { None };
        let next = if let Some(start) = shift {
            Some(rest[..start].trim_end())
        } else if let Some((open, inner, _)) = trailing_bracket(rest) {
            if inner.contains('[')
                || inner.contains(']')
                || inner.contains('(')
                || inner.contains(')')
            {
                None
            } else {
                match classify_token(inner).map(|tag| tag.kind) {
                    Some(
                        TagKind::Quality | TagKind::Junk | TagKind::Status | TagKind::Timeshift,
                    ) => Some(rest[..open].trim_end()),
                    _ => None,
                }
            }
        } else {
            match trailing_bare_word(rest) {
                Some((start, word)) => match classify_token(word).map(|tag| tag.kind) {
                    Some(TagKind::Quality | TagKind::Junk | TagKind::Status) => {
                        Some(rest[..start].trim_end())
                    }
                    _ => None,
                },
                None => None,
            }
        };
        match next {
            Some(peeled) if !peeled.is_empty() => rest = peeled,
            _ => return rest,
        }
    }
}

/// The last whitespace-separated token and its byte start, when it is a plain word.
///
/// A token glued to punctuation (`West-Ham`, `HD!`) is prose, not a tag; the bracket
/// and shift shapes have their own peelers above.
fn trailing_bare_word(name: &str) -> Option<(usize, &str)> {
    let trimmed = name.trim_end();
    let word = trimmed.split_whitespace().last()?;
    if !word.chars().all(|c| c.is_alphanumeric()) {
        return None;
    }
    // Never eat the whole name: a lone tag-looking word is a name, not a tag.
    if word.len() == trimmed.len() {
        return None;
    }
    Some((trimmed.len() - word.len(), word))
}

fn trailing_shift(name: &str) -> Option<usize> {
    let bytes = name.trim_end();
    let mut end = bytes.len();
    let raw = bytes.as_bytes();
    while end > 0 && raw[end - 1].is_ascii_digit() {
        end -= 1;
    }
    if end == bytes.len() || bytes.len() - end > 2 {
        return None;
    }
    let mut start = end;
    while start > 0 && raw[start - 1] == b' ' {
        start -= 1;
    }
    if start > 0 && raw[start - 1] == b'+' {
        // A `+N` glued to a word (`E+1`?) is not a shift marker; require a
        // boundary or string start before the `+`.
        if start - 1 == 0
            || raw[start - 2] == b' '
            || raw[start - 2] == b']'
            || raw[start - 2] == b')'
        {
            return Some(start - 1);
        }
    }
    None
}

/// Region words that count BARE (no brackets) at the end of a name.
///
/// `Central` and `Atlantic` are left out on purpose: they end real channel
/// names (`Comedy Central`, `Sky Atlantic`), so only `(Central)`/`[Atlantic]`
/// read as a region there. SM's `extract_tags` word pattern takes them bare and
/// turns `Comedy Central` into `Comedy` + region `Central`; the vocabulary
/// does not.
const BARE_REGION_WORDS: [&str; 4] = ["EAST", "WEST", "PACIFIC", "MOUNTAIN"];

/// Words that make a following bare region word part of a place name
/// (`Middle East`, `Far East`, `Wild West`), not a feed region.
const REGION_NAME_LEADS: [&str; 3] = ["MIDDLE", "FAR", "WILD"];

fn trailing_region_word(name: &str) -> Option<(usize, &str)> {
    let trimmed = name.trim_end();
    let mut words = trimmed.split_whitespace().rev();
    let last = words.next()?;
    if !BARE_REGION_WORDS.contains(&last.to_uppercase().as_str()) {
        return None;
    }
    if words
        .next()
        .is_some_and(|lead| REGION_NAME_LEADS.contains(&lead.to_uppercase().as_str()))
    {
        return None;
    }
    // Never eat the whole name: `West` alone is a name, not a tag.
    if last.len() == trimmed.len() {
        return None;
    }
    // A region word glued to punctuation (`West-Ham`) is prose, not a tag.
    if !last.chars().all(|c| c.is_alphanumeric()) {
        return None;
    }
    let start = trimmed.len() - last.len();
    match classify_token(last) {
        Some(tag) if tag.kind == TagKind::Regional => Some((start, last)),
        _ => None,
    }
}

/// Split a name into its core and its trailing tags.
///
/// Strips the trailing run — bracket groups, `+N` shifts and bare region words
/// — classifying each; the first group that is NOT a known tag stops the run
/// (`CNN (Moldova)` is untouched). Tags come back in NAME order. Leading
/// markers (`US: ESPN`) stay in the core: they are the country detector's, not
/// tags'.
pub fn split_tags(name: &str) -> (String, Vec<Tag>) {
    let mut rest = name.trim().to_string();
    let mut tags: Vec<Tag> = Vec::new();
    loop {
        if let Some(start) = trailing_shift(&rest) {
            let token = rest[start..].trim().to_string();
            if let Some(tag) = classify_token(&token) {
                rest.truncate(start);
                rest = rest.trim_end().to_string();
                tags.push(tag);
                continue;
            }
            break;
        }
        if let Some((open, inner, _close)) = trailing_bracket(&rest) {
            if inner.contains('[')
                || inner.contains(']')
                || inner.contains('(')
                || inner.contains(')')
            {
                break;
            }
            if let Some(tag) = classify_token(inner) {
                rest.truncate(open);
                rest = rest.trim_end().to_string();
                tags.push(tag);
                continue;
            }
            break;
        }
        if let Some((start, word)) = trailing_region_word(&rest) {
            if let Some(tag) = classify_token(word) {
                rest.truncate(start);
                rest = rest.trim_end().to_string();
                tags.push(tag);
                continue;
            }
            break;
        }
        break;
    }
    tags.reverse();
    (rest, tags)
}

/// Strip the trailing tags of the given kinds plus operator-configured labels.
///
/// The vocabulary knows the plugin-standard tags; an operator's own suffixes
/// (`video_format_suffixes`) and custom rename-format labels are not in it, so they
/// arrive as `extra` and strip as trailing bracket groups (case-insensitive),
/// interleaved with the kind run: with kinds `[Quality]` and `extra = ["MyTag"]`,
/// `ESPN [HD] [MyTag]` strips to `ESPN`. Output changes vs one regex per tag set:
/// parens strip, and stacked mixed runs strip in one pass.
///
/// With no `kinds` only `extra` strips: an app whose strip set is its own (triage's
/// format labels, restore's status + format labels) names it all in `extra` and the
/// vocabulary decides nothing else.
pub fn strip_with(name: &str, kinds: &[TagKind], extra: &[String]) -> String {
    let strip = |name: &str| -> String {
        if kinds.is_empty() {
            name.trim().to_string()
        } else {
            strip(name, kinds)
        }
    };
    let mut rest = strip(name);
    loop {
        let trimmed = rest.trim_end().to_string();
        let mut peeled = trimmed.clone();
        if let Some((open, inner, _)) = trailing_bracket(&trimmed) {
            let inner = inner.trim();
            if !inner.contains('[')
                && !inner.contains(']')
                && !inner.contains('(')
                && !inner.contains(')')
                && extra
                    .iter()
                    .any(|label| label.trim().eq_ignore_ascii_case(inner))
            {
                peeled.truncate(open);
                peeled = peeled.trim_end().to_string();
            }
        }
        if peeled == trimmed {
            return trimmed;
        }
        rest = strip(&peeled);
    }
}

/// Strip the trailing tags of the given kinds off a name.
///
/// Only the trailing RUN strips, and only while its kind is wanted: with
/// `&[TagKind::Quality]`, `ESPN (East) [HD]` becomes `ESPN (East)` (the region
/// stops the run), while `ESPN [HD] [FHD]` becomes `ESPN`. An empty kind list
/// strips nothing.
pub fn strip(name: &str, kinds: &[TagKind]) -> String {
    let (core, tags) = split_tags(name);
    if tags.is_empty() {
        return core;
    }
    // Walk back from the end while kinds match; rebuild from the core plus the
    // kept prefix of the run.
    let mut keep = tags.len();
    while keep > 0 && kinds.contains(&tags[keep - 1].kind) {
        keep -= 1;
    }
    if keep == tags.len() {
        return name.trim().to_string();
    }
    if keep == 0 {
        return core;
    }
    // The split tags lost their bracket shapes, so peel the removable tail off
    // the ORIGINAL string, one trailing group per stripped tag.
    let mut rest = name.trim().to_string();
    for _ in keep..tags.len() {
        if let Some(start) = trailing_shift(&rest) {
            rest.truncate(start);
        } else if let Some((open, _, _)) = trailing_bracket(&rest) {
            rest.truncate(open);
        } else if let Some((start, _)) = trailing_region_word(&rest) {
            rest.truncate(start);
        } else {
            break;
        }
        rest = rest.trim_end().to_string();
    }
    rest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_parses_with_sources() {
        let counts = vocabulary_counts();
        let total: usize = counts.iter().map(|(_, n)| n).sum();
        assert!(total >= 90, "{counts:?}");
        let quality = counts
            .iter()
            .find(|(kind, _)| *kind == TagKind::Quality)
            .map(|(_, n)| *n)
            .unwrap_or(0);
        assert!(quality >= 20, "{counts:?}");
        // Spot-check provenance: NC junk, CM status, guide status.
        let table = tag_table();
        let find = |token: &str| {
            table
                .iter()
                .find(|entry| entry.token.eq_ignore_ascii_case(token))
                .unwrap_or_else(|| panic!("{token} in vocabulary"))
        };
        assert_eq!(find("VIP").kind, TagKind::Junk);
        assert!(find("VIP").sources.contains(&"NC"));
        assert_eq!(find("Unk").kind, TagKind::Status);
        assert_eq!(find("BadEPG").kind, TagKind::Status);
        assert!(find("UK").sources.contains(&"LU"));
    }

    #[test]
    fn tokens_classify_with_or_without_brackets() {
        for (token, kind) in [
            ("HD", TagKind::Quality),
            ("[HD]", TagKind::Quality),
            ("(hd)", TagKind::Quality),
            ("1080p", TagKind::Quality),
            ("[1080P]", TagKind::Quality),
            ("720p", TagKind::Quality),
            ("FHD1080", TagKind::Quality),
            ("h.265", TagKind::Quality),
            ("H265", TagKind::Quality),
            ("ᴴᴰ", TagKind::Quality),
            ("Unknown", TagKind::Quality),
            ("VIP", TagKind::Junk),
            ("[MULTI]", TagKind::Junk),
            ("BACKUP", TagKind::Junk),
            ("DEAD", TagKind::Status),
            ("[dead]", TagKind::Status),
            ("(Slow)", TagKind::Status),
            ("Unk", TagKind::Status),
            ("[Unk]", TagKind::Status),
            ("BadEPG", TagKind::Status),
            ("East", TagKind::Regional),
            ("[West]", TagKind::Regional),
            ("+1", TagKind::Timeshift),
            ("+ 2", TagKind::Timeshift),
            ("US", TagKind::CountryPrefix),
            ("[UK]", TagKind::CountryPrefix),
        ] {
            let tag = classify_token(token).unwrap_or_else(|| panic!("{token} classifies"));
            assert_eq!(tag.kind, kind, "{token}");
        }
        // Unknown ≠ Unk; prose, callsigns and bare numbers are not tags.
        assert_eq!(classify_token("Unknown").unwrap().kind, TagKind::Quality);
        assert_eq!(classify_token("Unk").unwrap().kind, TagKind::Status);
        for token in ["", "Moldova", "WABC", "12", "(KGO)", "News"] {
            assert!(classify_token(token).is_none(), "{token}");
        }
    }

    #[test]
    fn trailing_checks_are_bracket_aware_and_case_insensitive() {
        assert!(has_trailing("ESPN [HD]", "HD"));
        assert!(has_trailing("ESPN [HD]", "[HD]"));
        assert!(has_trailing("ESPN [HD]", "hd"));
        assert!(has_trailing("ESPN (Slow)", "[Slow]"));
        assert!(has_trailing("Télé-Québec [BadEPG]", " [BadEPG]"));
        assert!(!has_trailing("[HD] ESPN", "HD"));
        assert!(!has_trailing("ESPN [HD] News", "HD"));
        assert!(!has_trailing("HBO West", "West"));
        assert!(!has_trailing("ESPN", "HD"));
    }

    #[test]
    fn split_strips_the_trailing_run_only() {
        let (core, tags) = split_tags("ESPN (East) [HD]");
        assert_eq!(core, "ESPN");
        assert_eq!(
            tags.iter().map(|t| t.kind).collect::<Vec<_>>(),
            [TagKind::Regional, TagKind::Quality]
        );
        let (core, tags) = split_tags("HBO West");
        assert_eq!(core, "HBO");
        assert_eq!(tags.len(), 1);
        let (core, tags) = split_tags("ITV2 +1");
        assert_eq!(core, "ITV2");
        assert_eq!(tags[0].kind, TagKind::Timeshift);
        // Unknown groups stop the run; leading markers stay.
        let (core, tags) = split_tags("CNN (Moldova)");
        assert_eq!((core.as_str(), tags.len()), ("CNN (Moldova)", 0));
        let (core, _) = split_tags("US: ESPN HD");
        assert_eq!(core, "US: ESPN HD");
        let (core, _) = split_tags("West");
        assert_eq!(core, "West");
        // Region words that end real names stay; bracketed they are regions.
        for name in ["Comedy Central", "Sky Atlantic", "Al Jazeera Middle East"] {
            assert_eq!(split_tags(name), (name.to_string(), Vec::new()), "{name}");
        }
        let (core, tags) = split_tags("Fox Sports (Central)");
        assert_eq!(
            (core.as_str(), tags[0].kind),
            ("Fox Sports", TagKind::Regional)
        );
        // `1+1` is a channel name, not a shift; a glued `+1` is not split either.
        assert_eq!(split_tags("1+1 Спорт").1.len(), 0);
        assert_eq!(split_tags("1+1").0, "1+1");
        // Words containing a tag are not tags (`HD` inside `HDTV`, `SD` vs `S.D.`).
        assert_eq!(split_tags("ESPN HDTV").1.len(), 0);
        assert!(classify_token("S.D.").is_none());
        assert!(classify_token("TV5").is_none());
        assert!(classify_token("E!").is_none());
    }

    #[test]
    fn pipe_segments_and_bracket_inners_read_the_vocabulary() {
        // NC's `QUALITY_SEGMENT` (plugin.py:46): one quality/junk token, nothing else.
        for segment in [
            "HD",
            "hd",
            "FHD",
            "FHD1080",
            "UHD",
            "4K",
            "8K",
            "HEVC",
            "H.265",
            "H265",
            "H 265",
            "RAW",
            "MULTI",
            "MULTI-AUDIO",
            "MULTIAUDIO",
            "VIP",
            "HQ",
            "LQ",
            "LOW",
            "DUAL",
            "GEO",
            "BACKUP",
            "1080p",
            "720P",
            "ᴴᴰ",
        ] {
            assert!(is_pipe_junk_segment(segment), "{segment}");
        }
        for segment in [
            "", "Moldova", "RTE", "WABC", "12", "1080", "East", "+1", "Unknown",
        ] {
            // `Unknown` is IC's quality label, not NC's: NC keeps it. LU's bare `1080`
            // is quality here but NC's `\d{3,4}p` needs the `p`. A region and a shift
            // never count as pipe junk.
            assert!(!is_pipe_junk_segment(segment), "{segment}");
        }
        // NC's `BRACKET_JUNK_INNER` (plugin.py:49): junk tokens joined by `/,&+`.
        for inner in [
            "1080p",
            "VIP",
            "720p / HEVC",
            "1080p, HEVC",
            "HD & FHD",
            "H.265+RAW",
            "MULTI-AUDIO",
            "FHD1080",
            "HD /",
        ] {
            assert!(is_bracket_junk_inner(inner), "{inner}");
        }
        for inner in [
            "",
            "   ",
            "Moldova",
            "East",
            "4K + 1",
            "1080p Moldova",
            "BBC",
            "1080p (HD)",
        ] {
            assert!(!is_bracket_junk_inner(inner), "{inner}");
        }
    }

    #[test]
    fn pipe_collapse_keeps_prose_drops_junk() {
        // Decided N15 (step-14 review): `|` is a segment separator before the
        // engine's key/zone/country reads; pure-junk segments drop (the one
        // shared `is_pipe_junk_segment` definition), the rest join with a space.
        assert_eq!(
            collapse_pipe_segments("Cinemax East | HD |"),
            "Cinemax East"
        );
        // A country code keeps a delimiter the core's tables strip.
        assert_eq!(
            collapse_pipe_segments("UK | Sky Sports | HD"),
            "UK: Sky Sports"
        );
        assert_eq!(collapse_pipe_segments("HD | UK | Sky One"), "UK: Sky One");
        assert_eq!(collapse_pipe_segments("RK: US | CNN"), "RK: US: CNN");
        assert_eq!(collapse_pipe_segments("BBC One | UK | HD"), "BBC One | UK");
        assert_eq!(collapse_pipe_segments("us | cnn"), "us: cnn");
        assert_eq!(collapse_pipe_segments("USA | HD |"), "USA");
        // A meaningful pipe segment survives the join.
        assert_eq!(
            collapse_pipe_segments("Sky Sports | Premier League"),
            "Sky Sports Premier League"
        );
        // Shifts are timeshift, never pipe junk.
        assert_eq!(
            collapse_pipe_segments("ESPN West +1 | HD |"),
            "ESPN West +1"
        );
        // No pipe: borrowed unchanged. All-junk collapses to empty.
        assert_eq!(collapse_pipe_segments("ESPN"), "ESPN");
        // Bracket-wrapped junk is junk too (`[HD]` classifies through its
        // brackets), so an all-junk name collapses to empty.
        assert_eq!(collapse_pipe_segments("| HD | [HD] |"), "");
    }

    #[test]
    fn trailing_checks_never_split_a_multibyte_char() {
        // A one-byte affix against a name ending in a two-byte char used to
        // slice mid-char and panic.
        assert!(!has_trailing("Café", "!"));
        assert!(!has_trailing("Télé", "+"));
        assert!(has_trailing("Télé +1", "+1"));
    }

    #[test]
    fn name_cleaners_superscripts_are_exact() {
        // plugin.py:46 UNICODE_QUALITY, codepoint for codepoint.
        for mark in ["ᵁᴴᴰ", "ᴴᴰ", "ᴿᴬᵂ", "ᴴᴱᵛᶜ", "⁴ᴷ", "ᔕᗪ"] {
            assert_eq!(
                classify_token(mark).map(|t| t.kind),
                Some(TagKind::Quality),
                "{mark}"
            );
        }
        assert_eq!(
            tokens_from(TagKind::Status, "IC"),
            ["Blank", "DEAD", "Slow"]
        );
    }

    #[test]
    fn strip_honors_kinds() {
        assert_eq!(strip("ESPN [HD] [FHD]", &[TagKind::Quality]), "ESPN");
        // A region stops a quality-only run.
        assert_eq!(
            strip("ESPN (East) [HD]", &[TagKind::Quality]),
            "ESPN (East)"
        );
        assert_eq!(
            strip("ESPN (East) [HD]", &[TagKind::Quality, TagKind::Regional]),
            "ESPN"
        );
        assert_eq!(strip("ESPN [HD]", &[]), "ESPN [HD]");
        assert_eq!(strip("ESPN [VIP]", &[TagKind::Quality]), "ESPN [VIP]");
        assert_eq!(strip("ESPN [VIP]", &[TagKind::Junk]), "ESPN");
    }
}
