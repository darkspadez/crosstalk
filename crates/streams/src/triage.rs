//! `triage_preview` / `triage_apply`: IPTV Checker's seven post-actions, as one run.
//!
//! The plugin has eight buttons that read the last check and change channels: rename dead
//! (`rename_channels_action`, 3495), move dead (`move_dead_channels_action`, 3526), rename
//! blank (`rename_black_screen_channels_action`, 3824), move blank (3855), rename slow
//! (`rename_low_framerate_channels_action`, 3626), move slow (3660), add the format suffix
//! (`add_video_format_suffix_action`, 3686) and delete dead (out of scope). They share one
//! input — the last check's per-channel rows — and one output — a `bulk_update` of a channel
//! name or group. So crosstalk has ONE action with seven switches, and every write goes
//! through [`channel_ops`], which is what records the `before` values
//! [`crate::restore`] reverses.
//!
//! # What the last probe decided
//!
//! The input is the newest SUCCEEDED `streams.probe` run's rows — plus the finished runs back to
//! the previous succeeded one, when a windowed pass split its verdicts across nights (see
//! [`crate::probe::LastProbe`]) — read by [`crate::probe::recorded_verdicts`], never the CSV
//! and never a live re-check: the plugin's
//! own docstring says every action "reads the last check. None of them re-probe anything".
//! The verdicts are already the plugin's three predicates, disjoint by construction
//! ([`crate::probe::verdict_for`]):
//!
//! | verdict | IPTV Checker | triage |
//! |---|---|---|
//! | `dead` | `_is_dead_nonblack` (3319): EVERY stream Dead, none a black screen | rename dead, move dead |
//! | `blank` | `_is_black_screen` (3323): EVERY stream Dead as a black screen | rename blank, move blank |
//! | `low_frame_rate` | every ALIVE stream below 24 fps (3626) | rename slow, move slow |
//! | `alive` | at least one Alive stream, not all slow | format suffix only |
//! | `skipped` / `inconclusive` | not proven bad | nothing |
//!
//! `dead` and `blank` are disjoint on purpose, and the plugin says why in its own user guide:
//! a channel is never tagged both `[DEAD]` and `[Blank]`. A channel that still plays on a
//! backup stream is never dead either — [`crate::probe::verdict_for`] requires EVERY stream to
//! have failed — so a working channel cannot be renamed or exiled over one bad URL.
//!
//! # The settings, and why everything is off
//!
//! The seven switches are the plugin's `scheduler_*` toggles verbatim
//! (`plugin.json:368-410`), and like the plugin's they all default OFF. In the plugin they
//! only gated the SCHEDULED check, because each manual button was its own confirmation; here
//! one action covers both, so the switches are the action's whole instruction set. A bare
//! `triage_apply` therefore refuses: [`plan`] errors with the list of switches rather than
//! running a no-op that reads like a success. The scheduled shape is a workflow — step 12's
//! `[workflows]` chains `streams.probe` then `streams.triage_apply`, which is the plugin's
//! "post-actions after a scheduled check" with the same order and the same switches.
//!
//! # The rename
//!
//! `rename_format.replace('{name}', name)` (3508, 3861, 3645): the format must contain
//! `{name}` (the plugin refuses otherwise, 3503), it is stripped, and the name it is applied
//! to is THE NAME THE CHECK SAW — the results row's `channel_name`, not a fresh read of the
//! channel (the plugin never re-reads a name for a rename). crosstalk does the same: the base
//! name is [`crate::probe::RecordedVerdict::channel_name`].
//!
//! One deliberate divergence, and it is the one the plugin's users complain about: the plugin
//! re-applies the format to whatever name it last recorded, so a dead channel renamed on
//! Monday is `ESPN [DEAD] [DEAD]` by Wednesday, and only `restore` collapses the stack. A
//! triage run here does nothing when the base name already ENDS with one of the format's own
//! bracket labels ([`format_tags`], the plugin's `_extract_format_tags`, 3393) — the tag is
//! already saying what this run would say. The parity harness counts this shape separately
//! and asserts that this run's name is the plugin's with the repeated tag removed.
//!
//! # The move
//!
//! `_get_or_create_group` (2666) then one `bulk_update(channel_group_id=...)`: the group named
//! in the class's setting, created when it does not exist. A preview must not create anything,
//! so it plans the move against a placeholder group id that carries the name (see
//! [`GroupTargets::preview`]), and an apply resolves the real id first. The plugin records the
//! ORIGINAL group in `channel_state.json` before moving (3593, `_capture_original_state`);
//! crosstalk's record is the run row `channel_ops` writes, whose `before_value` is the group
//! id the channel came from. `_capture_original_state` also refuses to record a managed
//! destination group (Graveyard/Slow/Black Screens) as an original; the run rows do not need
//! that rule because [`crate::restore`] follows the chain of changes rather than trusting the
//! newest one.
//!
//! # The format suffix
//!
//! `add_video_format_suffix_action` (3686) in full:
//!
//!   * the configured list is lowercased, split on commas and stripped
//!     (`video_format_suffixes`); a channel whose format is not in it is left alone;
//!   * the format appended is `" [{format.upper()}]"`, from the LAST Alive row of the channel
//!     ([`crate::probe::format_for`]);
//!   * every already-applied format tag is stripped first — `[UHD]`, `[FHD]`, `[HD]`, `[SD]`,
//!     `[Unknown]` and the configured ones, case-insensitively, as a trailing run — so a
//!     channel that changes quality REPLACES its tag instead of stacking (`issue #18`);
//!   * one that would come out unchanged is skipped;
//!   * and it runs LAST, after the renames (scheduled step 6, `plugin.py:2085`), against the
//!     name the renames just wrote. A slow channel with the suffix on therefore ends up
//!     `ESPN [Slow] [HD]`, exactly as the plugin leaves it.
//!
//! # Order of operations in one run
//!
//! The plugin's scheduled order is restore, rename dead, rename slow, rename blank, suffix,
//! move dead, move slow, move blank (`plugin.py:2060-2107`). Restore is [`crate::restore`],
//! and a workflow that wants the plugin's healing loop runs it first. Within triage the
//! renames compose before the suffix per channel, and all moves ride along; the ORDER does
//! not matter beyond that, because every write is a full value rather than an append and
//! `channel_ops` folds two operations on one field into one row.
//!
//! # Scale
//!
//! One plan over the whole result set: an id index over the world's channels, one pass over
//! the verdicts, two `Op::Set`s (names, groups) and two batched writes. Nothing here is
//! per-channel requests — 20k channels is one `edit/bulk` payload per field group.

use crate::probe::{self, ChannelVerdict, RecordedVerdict};
use crate::settings::StreamsSettings;
use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{self, Field, Op, ReportSpec, Selection, SetValue};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, DispatcharrClient, World, WorldKind, WorldKinds,
};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use regex::Regex;
use std::collections::{BTreeMap, HashMap, HashSet};

/// The report's title, without the preview/applied suffix.
const REPORT_TITLE: &str = "Triage";

/// The three failure classes triage acts on, in the plugin's own order.
///
/// `Alive` is not here: a channel that plays is only ever given the format suffix, which has
/// no class of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TriageClass {
    Dead,
    Blank,
    LowFrameRate,
}

impl TriageClass {
    /// The classes, in the plugin's order (rename dead, rename slow, rename blank — the
    /// scheduled steps are 4, 5 and 5b).
    pub const ALL: [Self; 3] = [Self::Dead, Self::LowFrameRate, Self::Blank];

    /// The verdict this class acts on.
    pub fn verdict(self) -> ChannelVerdict {
        match self {
            Self::Dead => ChannelVerdict::Dead,
            Self::Blank => ChannelVerdict::Blank,
            Self::LowFrameRate => ChannelVerdict::LowFrameRate,
        }
    }

    /// The stored/CSV form, which is the verdict's own.
    pub fn as_str(self) -> &'static str {
        self.verdict().as_str()
    }

    /// How the report names the class.
    pub fn label(self) -> &'static str {
        match self {
            Self::Dead => "dead",
            Self::Blank => "blank",
            Self::LowFrameRate => "low framerate",
        }
    }

    /// The rename switch, the plugin's `scheduler_rename_*`.
    pub fn rename_enabled(self, settings: &StreamsSettings) -> bool {
        match self {
            Self::Dead => settings.rename_dead_channels,
            Self::Blank => settings.rename_blank_channels,
            Self::LowFrameRate => settings.rename_low_framerate_channels,
        }
    }

    /// The move switch, the plugin's `scheduler_move_*`.
    pub fn move_enabled(self, settings: &StreamsSettings) -> bool {
        match self {
            Self::Dead => settings.move_dead_channels,
            Self::Blank => settings.move_blank_channels,
            Self::LowFrameRate => settings.move_low_framerate_channels,
        }
    }

    /// The rename format, the plugin's `*_rename_format`.
    pub fn rename_format(self, settings: &StreamsSettings) -> &str {
        match self {
            Self::Dead => &settings.dead_rename_format,
            Self::Blank => &settings.black_screen_rename_format,
            Self::LowFrameRate => &settings.low_framerate_rename_format,
        }
    }

    /// The destination group NAME, the plugin's `move_*group*`.
    pub fn move_group(self, settings: &StreamsSettings) -> &str {
        match self {
            Self::Dead => &settings.move_to_group_name,
            Self::Blank => &settings.move_black_screen_group,
            Self::LowFrameRate => &settings.move_low_framerate_group,
        }
    }

    /// The format setting this class renames with, for a refusal's wording.
    fn rename_setting(self) -> &'static str {
        match self {
            Self::Dead => "dead_rename_format",
            Self::Blank => "black_screen_rename_format",
            Self::LowFrameRate => "low_framerate_rename_format",
        }
    }

    /// The group setting this class moves with, for a refusal's wording.
    fn move_setting(self) -> &'static str {
        match self {
            Self::Dead => "move_to_group_name",
            Self::Blank => "move_black_screen_group",
            Self::LowFrameRate => "move_low_framerate_group",
        }
    }

    /// The switch names a refusal lists, so the operator knows what to turn on.
    fn rename_switch(self) -> &'static str {
        match self {
            Self::Dead => "rename_dead_channels",
            Self::Blank => "rename_blank_channels",
            Self::LowFrameRate => "rename_low_framerate_channels",
        }
    }

    fn move_switch(self) -> &'static str {
        match self {
            Self::Dead => "move_dead_channels",
            Self::Blank => "move_blank_channels",
            Self::LowFrameRate => "move_low_framerate_channels",
        }
    }
}

/// One channel's planned rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    pub channel_id: i64,
    pub class: TriageClass,
    /// The name the check saw, which the format is applied to.
    pub from: String,
    pub to: String,
}

/// One channel's planned format suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suffix {
    pub channel_id: i64,
    /// `UHD`, `HD`, `SD`, … — the channel's last alive stream's tier.
    pub format: String,
    /// The name the suffix is applied to (the renamed name when this run renames it too).
    pub from: String,
    pub to: String,
}

/// One channel's planned move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Move {
    pub channel_id: i64,
    pub class: TriageClass,
    /// The destination group's NAME; the id is resolved (or created) by the run.
    pub group: String,
}

/// A channel triage looked at and left alone, with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
    pub channel_id: i64,
    pub class: Option<TriageClass>,
    pub reason: String,
}

/// What one triage run would write. Nothing has been written.
#[derive(Debug, Clone, Default)]
pub struct TriageOutcome {
    /// Channels the last probe judged.
    pub judged: usize,
    /// How many channels carried each verdict, keyed by the verdict's own name.
    pub verdicts: BTreeMap<String, usize>,
    pub renames: Vec<Rename>,
    pub suffixes: Vec<Suffix>,
    pub moves: Vec<Move>,
    /// Channels no switch acted on, or whose write would have changed nothing.
    pub skipped: Vec<Skip>,
    /// The post-actions this run was given, in words, for the report and the log.
    pub switches: Vec<String>,
}

impl TriageOutcome {
    /// The channels this run touches, whatever the field: what a selection and a summary count.
    pub fn channels(&self) -> usize {
        self.target_ids().len()
    }

    /// The ids this run would write, sorted and de-duplicated — the selection's id list.
    pub fn target_ids(&self) -> Vec<i64> {
        let mut ids: HashSet<i64> = HashSet::new();
        ids.extend(self.renames.iter().map(|rename| rename.channel_id));
        ids.extend(self.suffixes.iter().map(|suffix| suffix.channel_id));
        ids.extend(self.moves.iter().map(|movement| movement.channel_id));
        let mut ids: Vec<i64> = ids.into_iter().collect();
        ids.sort_unstable();
        ids
    }

    /// Whether any switch would write anything.
    pub fn is_empty(&self) -> bool {
        self.renames.is_empty() && self.suffixes.is_empty() && self.moves.is_empty()
    }

    /// Drop every change for a channel outside the scope this run may touch, and report each
    /// as a skip.
    ///
    /// The probe may have run with a wider `[scope]` than the triage run has now, and a plan is
    /// always held to the scope it runs under. Doing it here rather than leaving it to
    /// [`channel_ops`] keeps a destination group from being CREATED for a channel that could
    /// never be moved into it.
    pub fn retain_in_scope(&mut self, channels: &HashSet<i64>) {
        let mut out: Vec<Skip> = Vec::new();
        self.renames.retain(|rename| {
            if channels.contains(&rename.channel_id) {
                return true;
            }
            out.push(Skip {
                channel_id: rename.channel_id,
                class: Some(rename.class),
                reason: OUT_OF_SCOPE.to_string(),
            });
            false
        });
        self.suffixes.retain(|suffix| {
            if channels.contains(&suffix.channel_id) {
                return true;
            }
            out.push(Skip {
                channel_id: suffix.channel_id,
                class: None,
                reason: OUT_OF_SCOPE.to_string(),
            });
            false
        });
        self.moves.retain(|movement| {
            if channels.contains(&movement.channel_id) {
                return true;
            }
            out.push(Skip {
                channel_id: movement.channel_id,
                class: Some(movement.class),
                reason: OUT_OF_SCOPE.to_string(),
            });
            false
        });
        self.skipped.extend(out);
        self.skipped.sort_by_key(|skip| skip.channel_id);
    }

    /// The skips in words: `3 already ends with [DEAD], 1 outside the scope`, for the report.
    pub fn skip_summary(&self) -> Option<String> {
        if self.skipped.is_empty() {
            return None;
        }
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for skip in &self.skipped {
            *counts.entry(skip.reason.as_str()).or_default() += 1;
        }
        let mut parts: Vec<String> = counts
            .into_iter()
            .map(|(reason, count)| format!("{count} {reason}"))
            .collect();
        parts.sort();
        Some(parts.join("; "))
    }
}

/// Why a channel the probe judged is not written: the run is held to `[scope]`.
const OUT_OF_SCOPE: &str = "outside the scope this run may touch, so it is left exactly as it is";

/// The bracket labels a rename format carries — the plugin's `_extract_format_tags` (3393).
///
/// `{name} [DEAD]` yields `["DEAD"]`, and `[X] {name} [DEAD]` yields `["X", "DEAD"]`. An
/// unclosed bracket yields nothing, which is what the plugin's `findall` does too.
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

/// Whether `name` already ends with the tag `label`, bracketed and case-insensitively — the
/// plugin's trailing-tag regex for one label.
///
/// `ESPN [DEAD]` ends with `DEAD`; `ESPN [dead]` does too (the plugin compiles the tag regex
/// with `re.IGNORECASE`); `[DEAD] ESPN` does not, because the plugin only ever strips and
/// compares TRAILING tags.
pub fn ends_with_tag(name: &str, label: &str) -> bool {
    let trimmed = name.trim_end();
    let Some(without_close) = trimmed.strip_suffix(']') else {
        return false;
    };
    let Some(open) = without_close.rfind('[') else {
        return false;
    };
    without_close[open + 1..]
        .trim()
        .eq_ignore_ascii_case(label.trim())
}

/// The configured suffixes, lowercased, split on commas and stripped — the plugin's
/// `suffixes_to_add_str.split(',')` over a lowercased value (3700).
pub fn configured_suffixes(settings: &StreamsSettings) -> Vec<String> {
    settings
        .video_format_suffixes
        .to_lowercase()
        .split(',')
        .map(|suffix| suffix.trim().to_string())
        .filter(|suffix| !suffix.is_empty())
        .collect()
}

/// The tags a format suffix REPLACES: the configured ones plus the standard five, always —
/// the plugin's `issue #18` rule (3705: narrowing `video_format_suffixes` must not stack a
/// second tag onto a channel that carries one from before the narrowing).
pub fn known_format_tags(suffixes: &[String]) -> Vec<String> {
    let mut tags: Vec<String> = suffixes.to_vec();
    for standard in ["uhd", "fhd", "hd", "sd", "unknown"] {
        if !tags.iter().any(|tag| tag == standard) {
            tags.push(standard.to_string());
        }
    }
    tags
}

/// The trailing-tag matcher, compiled once per run.
///
/// The plugin compiles one `trailing_tag_re` and reuses it across every channel of the run
/// (3705); at 20k channels a per-channel `re.compile` (or `Regex::new`) is the difference
/// between one pass and 20 000 regex compilations. The tags are escaped, so the pattern cannot
/// fail to compile; a hypothetical failure leaves the name alone rather than losing it.
#[derive(Debug, Clone)]
pub struct FormatTags {
    tags: Vec<String>,
    pattern: Option<Regex>,
}

impl FormatTags {
    /// The tags of this settings file: the configured suffixes plus the standard five.
    pub fn of(settings: &StreamsSettings) -> Self {
        Self::from_tags(known_format_tags(&configured_suffixes(settings)))
    }

    /// Explicit tags, lowercased or not: matching is case-insensitive either way.
    pub fn from_tags(tags: Vec<String>) -> Self {
        // Longest first, so `[unknown]` is preferred over a shorter tag that prefixes it — the
        // plugin sorts the same way (3405). The bracketed shape makes the order harmless, but a
        // stable order keeps two runs' reports identical.
        let mut sorted: Vec<String> = tags;
        sorted.sort_by(|left, right| right.len().cmp(&left.len()).then(left.cmp(right)));
        sorted.dedup();
        let alternation = sorted
            .iter()
            .map(|tag| regex::escape(tag))
            .collect::<Vec<_>>()
            .join("|");
        let pattern = if alternation.is_empty() {
            None
        } else {
            Regex::new(&format!(r"(?i)(?:\s*\[(?:{alternation})\])+\s*$")).ok()
        };
        Self {
            tags: sorted,
            pattern,
        }
    }

    /// The tags themselves, longest first.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Strip a trailing run of format tags, then the space before it — `trailing_tag_re.sub`
    /// plus `rstrip` (3712-3714).
    pub fn strip(&self, name: &str) -> String {
        match &self.pattern {
            Some(pattern) => pattern.replace(name, "").trim_end().to_string(),
            None => name.trim_end().to_string(),
        }
    }
}

/// [`FormatTags::strip`] for a one-off call (a test, or a harness).
pub fn strip_format_tags(name: &str, tags: &[String]) -> String {
    FormatTags::from_tags(tags.to_vec()).strip(name)
}

/// The new name a rename format produces, or `None` when it would be empty.
///
/// `format.replace('{name}', name)` (3508): every `{name}` becomes the base name, and any
/// other brace token is left exactly as written — the plugin only ever replaces this one.
pub fn render_format(format: &str, name: &str) -> Option<String> {
    let rendered = format.trim().replace("{name}", name);
    if rendered.trim().is_empty() {
        None
    } else {
        Some(rendered)
    }
}

/// The switches this settings file has on, in words. Empty means triage would do nothing.
fn enabled_switches(settings: &StreamsSettings) -> Vec<String> {
    let mut switches = Vec::new();
    for class in TriageClass::ALL {
        if class.rename_enabled(settings) {
            switches.push(class.rename_switch().to_string());
        }
        if class.move_enabled(settings) {
            switches.push(class.move_switch().to_string());
        }
    }
    if settings.add_video_format_suffix {
        switches.push("add_video_format_suffix".to_string());
    }
    switches
}

/// Plan a triage run over the last probe's verdicts. Pure: nothing is written.
///
/// `verdicts` is [`crate::probe::recorded_verdicts`]' answer for the newest succeeded probe
/// run, `world` is the live channel list the plan re-reads (so a channel with no verdict row
/// is simply not judged, and one that has disappeared is skipped with a reason), and
/// `settings` carries the seven switches and the formats and groups they use.
///
/// Refuses when no switch is on — the plugin's own shape, where an action with nothing to do
/// is an operator mistake rather than a quiet success — and when an enabled class's setting
/// is unusable (a format that would erase a name, a blank group name), with the plugin's own
/// reasons (3499-3503, 3528-3530, 3628-3632, 3662-3664, 3826-3830, 3857-3859, 3698-3700).
pub fn plan(
    world: &World,
    settings: &StreamsSettings,
    verdicts: &[RecordedVerdict],
) -> Result<TriageOutcome> {
    let switches = enabled_switches(settings);
    if switches.is_empty() {
        bail!(
            "no triage post-action is switched on, so this run would change nothing: enable the \
             classes you want in streams.toml — rename_dead_channels, \
             rename_blank_channels, rename_low_framerate_channels, add_video_format_suffix, \
             move_dead_channels, move_blank_channels, move_low_framerate_channels. Every one \
             ships off, as IPTV Checker's own post-action switches do, so that a run only \
             makes the changes an operator asked for"
        );
    }

    let suffixes = configured_suffixes(settings);
    if settings.add_video_format_suffix && suffixes.is_empty() {
        bail!(
            "add_video_format_suffix is on but video_format_suffixes is empty, so there is no \
             format to append: list the formats you want tagged, e.g. \"UHD, FHD, HD, SD, \
             Unknown\" (IPTV Checker refuses this run too, 3698-3700)"
        );
    }
    for class in TriageClass::ALL {
        if class.rename_enabled(settings) {
            let format = class.rename_format(settings);
            if format.trim().is_empty() {
                bail!(
                    "{setting} is empty and {switch} is on: an empty format would erase every \
                     {label} channel's name",
                    setting = class.rename_setting(),
                    switch = class.rename_switch(),
                    label = class.label(),
                );
            }
            if !format.contains("{name}") {
                bail!(
                    "{setting} has no {{name}} placeholder and {switch} is on, so every {label} \
                     channel would get the same name (IPTV Checker refuses this run for the \
                     same reason, 3503)",
                    setting = class.rename_setting(),
                    switch = class.rename_switch(),
                    label = class.label(),
                );
            }
        }
        if class.move_enabled(settings) && class.move_group(settings).trim().is_empty() {
            bail!(
                "{setting} is empty and {switch} is on, so there is nowhere to move a {label} \
                 channel (IPTV Checker refuses this run too, 3528-3530)",
                setting = class.move_setting(),
                switch = class.move_switch(),
                label = class.label(),
            );
        }
    }

    let channels: HashMap<i64, &Channel> = world
        .channels
        .iter()
        .map(|channel| (channel.id, channel))
        .collect();
    let tags = FormatTags::of(settings);

    let mut outcome = TriageOutcome {
        judged: verdicts.len(),
        switches,
        ..TriageOutcome::default()
    };
    for verdict in verdicts {
        *outcome
            .verdicts
            .entry(verdict.verdict.as_str().to_string())
            .or_insert(0) += 1;
        plan_channel(
            &mut outcome,
            settings,
            &suffixes,
            &tags,
            channels.get(&verdict.channel_id).copied(),
            verdict,
        );
    }

    // One entry per channel whatever the order the verdicts arrived in, so a report and the
    // parity harness compare two runs by content rather than by row order.
    outcome.renames.sort_by_key(|rename| rename.channel_id);
    outcome.suffixes.sort_by_key(|suffix| suffix.channel_id);
    outcome.moves.sort_by_key(|movement| movement.channel_id);
    outcome.skipped.sort_by_key(|skip| skip.channel_id);
    Ok(outcome)
}

/// Plan one channel's rename, suffix and move.
fn plan_channel(
    outcome: &mut TriageOutcome,
    settings: &StreamsSettings,
    suffixes: &[String],
    tags: &FormatTags,
    channel: Option<&Channel>,
    verdict: &RecordedVerdict,
) {
    let Some(channel) = channel else {
        outcome.skipped.push(Skip {
            channel_id: verdict.channel_id,
            class: class_of(verdict.verdict),
            reason: "the channel no longer exists in Dispatcharr".to_string(),
        });
        return;
    };

    // 1. The class rename, from the name the check saw (IPTV Checker's `channel_name` from
    //    the results file, never a fresh read).
    let mut current = channel.name.clone();
    if let Some(class) = class_of(verdict.verdict) {
        if class.rename_enabled(settings) {
            let format = class.rename_format(settings);
            let label_tags = format_tags(format);
            let base = verdict.channel_name.clone();
            if let Some(label) = label_tags.iter().find(|label| ends_with_tag(&base, label)) {
                // The one deliberate divergence: the plugin would append the tag again.
                outcome.skipped.push(Skip {
                    channel_id: verdict.channel_id,
                    class: Some(class),
                    reason: format!(
                        "the name already ends with [{label}], so this run does not tag it again \
                         (IPTV Checker would append a second one)"
                    ),
                });
            } else if let Some(to) = render_format(format, &base) {
                if to == base {
                    outcome.skipped.push(Skip {
                        channel_id: verdict.channel_id,
                        class: Some(class),
                        reason: "the rename format produces the name the channel already has"
                            .to_string(),
                    });
                } else {
                    outcome.renames.push(Rename {
                        channel_id: verdict.channel_id,
                        class,
                        from: base,
                        to: to.clone(),
                    });
                    current = to;
                }
            }
        }
        if class.move_enabled(settings) {
            let group = class.move_group(settings).trim().to_string();
            outcome.moves.push(Move {
                channel_id: verdict.channel_id,
                class,
                group,
            });
        }
    }

    // 2. The format suffix, last and only for a channel that produced an ALIVE stream — the
    //    plugin's `channel_formats` (3718-3722), which is keyed on Alive rows, so a slow
    //    channel (Alive, below 24 fps) gets one too.
    if settings.add_video_format_suffix && alive_rows(verdict.verdict) {
        let format = verdict
            .format
            .clone()
            .unwrap_or_else(|| "Unknown".to_string());
        if !suffixes
            .iter()
            .any(|suffix| *suffix == format.to_lowercase())
        {
            outcome.skipped.push(Skip {
                channel_id: verdict.channel_id,
                class: None,
                reason: format!(
                    "format {format:?} is not in video_format_suffixes, so it gets no suffix"
                ),
            });
            return;
        }
        // The plugin strips the applied format tags off the CURRENT name (it re-reads every
        // channel for this action), which in the same run is the name the rename above wrote.
        let base = tags.strip(&current);
        let to = format!("{base} [{}]", format.to_uppercase());
        if to == current {
            outcome.skipped.push(Skip {
                channel_id: verdict.channel_id,
                class: None,
                reason: format!(
                    "the name already carries the [{0}] suffix",
                    format.to_uppercase()
                ),
            });
            return;
        }
        outcome.suffixes.push(Suffix {
            channel_id: verdict.channel_id,
            format,
            from: current,
            to,
        });
    }
}

/// The class a verdict belongs to, or `None` for one triage never acts on.
fn class_of(verdict: ChannelVerdict) -> Option<TriageClass> {
    TriageClass::ALL
        .into_iter()
        .find(|class| class.verdict() == verdict)
}

/// Whether the channel produced an Alive stream, which is what the suffix step keys on.
///
/// `LowFrameRate` is the plugin's Alive-with-a-slow-picture, so it counts: its rows are
/// `status == 'Alive'` and `channel_formats` picks them up.
fn alive_rows(verdict: ChannelVerdict) -> bool {
    matches!(
        verdict,
        ChannelVerdict::Alive | ChannelVerdict::LowFrameRate
    )
}

/// The groups a run's moves target, resolved to ids.
///
/// A preview must not create anything, so an apply and a preview differ only here: an apply
/// calls `POST /channels/groups/` (upstream `get_or_create`, 2666) for a missing group, while
/// a preview plans against a NEGATIVE placeholder id that carries the real name, so the CSV
/// says "Graveyard" rather than "No Group" and nothing is written. A placeholder id can never
/// collide with a real one, and no write is ever attempted with one.
#[derive(Debug, Clone, Default)]
pub struct GroupTargets {
    /// Group name -> id (a real id on an apply, a placeholder on a preview).
    pub ids: HashMap<String, i64>,
    /// Groups that did not exist when the run started.
    pub missing: Vec<String>,
    /// Groups this run created (apply only).
    pub created: Vec<String>,
}

impl GroupTargets {
    /// Resolve (and, unless `dry_run`, create) every group the moves name.
    pub async fn resolve(
        client: Option<&DispatcharrClient>,
        world: &World,
        moves: &[Move],
        dry_run: bool,
    ) -> Result<Self> {
        let mut targets = GroupTargets::default();
        let mut wanted: Vec<String> = Vec::new();
        for movement in moves {
            if !wanted.contains(&movement.group) {
                wanted.push(movement.group.clone());
            }
        }
        // 0, then downwards: a placeholder must not name a real group, and an id the world
        // does not know prints as "No Group", which would misreport the move.
        let mut placeholder = 0i64;
        for group in wanted {
            if let Some(found) = world
                .groups
                .iter()
                .find(|candidate| candidate.name == group)
            {
                targets.ids.insert(group, found.id);
                continue;
            }
            targets.missing.push(group.clone());
            if dry_run {
                loop {
                    placeholder -= 1;
                    if !world.groups.iter().any(|group| group.id == placeholder) {
                        break;
                    }
                }
                targets.ids.insert(group, placeholder);
                continue;
            }
            let Some(client) = client else {
                bail!(
                    "this run has no Dispatcharr session, so it cannot create the channel group \
                     {group:?} the moves need"
                );
            };
            let created = client.create_group(&group).await?;
            targets.ids.insert(group.clone(), created.id);
            targets.created.push(group);
        }
        Ok(targets)
    }

    /// The world to PLAN against: the loaded one, plus a named entry for every group the run
    /// would create, so `channel_ops` prints the group's name instead of "No Group".
    pub fn plan_world(&self, world: &World) -> World {
        let mut plan_world = world.clone();
        for group in &self.missing {
            let Some(id) = self.ids.get(group) else {
                continue;
            };
            if plan_world
                .groups
                .iter()
                .any(|candidate| candidate.id == *id)
            {
                continue;
            }
            plan_world.groups.push(ChannelGroup {
                id: *id,
                name: group.clone(),
            });
        }
        plan_world
    }
}

/// The action: preview (a read and a CSV) or apply (the writes).
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let action_id = format!(
        "streams.triage_{}",
        if dry_run { "preview" } else { "apply" }
    );
    let scope = context.config().scope.clone();

    // 1. The result set, before anything else: a mistyped action with no probe run costs no
    //    request, exactly as the plugin refuses without a results file (3505-3507).
    let probe = probe::last_verdicts(context)?;
    probe::announce(context, &action_id, &probe);

    // 2. Channels (names, groups) and the groups slice: the plan re-reads every channel it
    //    writes, so a probe from any age is safe. Profiles only when the scope names them.
    let mut kinds = WorldKinds::only(WorldKind::Channels) | WorldKind::Groups;
    if !scope.profiles.trim().is_empty() {
        kinds = kinds | WorldKind::Profiles;
    }
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.triage",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = client.load_world_with(kinds).await?;

    // The scope is resolved once, up front: a scope that names both a group filter and an
    // ignore filter is refused here rather than after a destination group has been created.
    let scoped = crosstalk_core::scope::scope_channels(&world, &scope)?;
    let mut outcome = plan(&world, settings, &probe.verdicts)?;
    // A probe from before a `[scope]` change may judge channels this run may not touch.
    let in_scope: HashSet<i64> = scoped.channels.iter().map(|channel| channel.id).collect();
    outcome.retain_in_scope(&in_scope);

    // 3. The destination groups: created here on an apply, named by a placeholder on a
    //    preview, so a preview writes nothing at all.
    let targets = GroupTargets::resolve(Some(client), &world, &outcome.moves, dry_run).await?;
    let plan_world = targets.plan_world(&world);

    // 4. One per-channel value map per field, through `channel_ops`: the same selection, scope
    //    intersection, no-op dropping, CSV and restore rows every other write gets.
    let mut names: HashMap<i64, SetValue> = HashMap::new();
    for rename in &outcome.renames {
        names.insert(rename.channel_id, SetValue::Name(rename.to.clone()));
    }
    for suffix in &outcome.suffixes {
        names.insert(suffix.channel_id, SetValue::Name(suffix.to.clone()));
    }
    let mut groups: HashMap<i64, SetValue> = HashMap::new();
    for movement in &outcome.moves {
        let Some(id) = targets.ids.get(&movement.group) else {
            continue;
        };
        groups.insert(movement.channel_id, SetValue::Group(Some(*id)));
    }

    let mut operations = Vec::new();
    if !names.is_empty() {
        operations.push(Op::Set {
            field: Field::Name,
            values: names,
        });
    }
    if !groups.is_empty() {
        operations.push(Op::Set {
            field: Field::Group,
            values: groups,
        });
    }
    // A plan needs at least one operation even when nothing is to be written (every judged
    // channel was skipped): an empty per-channel map plans no change and reports the
    // selection, which is exactly what a report of "nothing to do" needs.
    if operations.is_empty() {
        operations.push(Op::Set {
            field: Field::Name,
            values: HashMap::new(),
        });
    }

    let selection = Selection {
        channel_ids: Some(outcome.target_ids()),
        ..Selection::in_scope(&scope)
    };
    let op_plan = channel_ops::plan(&plan_world, &selection, &operations)?;

    let applied = if dry_run || op_plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &op_plan, context, &action_id).await)
    };

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!("triage_{}", if dry_run { "preview" } else { "applied" });
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if dry_run { "Preview" } else { "Applied" }
    );
    let mut spec = ReportSpec::new(app, &schema, &values, &stem, &title)
        .note("Result set", probe.describe())
        .note("Post-actions", outcome.switches.join(", "))
        .note(
            "Verdicts",
            outcome
                .verdicts
                .iter()
                .map(|(verdict, count)| format!("{count} {verdict}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
    if !targets.missing.is_empty() {
        spec = spec.note(
            "Groups",
            format!(
                "{} group(s) named by the moves: {}{}",
                targets.missing.len(),
                targets.missing.join(", "),
                if dry_run {
                    ", created by an apply"
                } else {
                    ", created now"
                }
            ),
        );
    }
    if let Some(skips) = outcome.skip_summary() {
        spec = spec.note("Left alone", skips);
    }
    let report = channel_ops::write_ops_report(context, &spec, &op_plan, applied.as_ref())?;
    context.record_rows(&channel_ops::run_rows(&op_plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut builder = OutcomeBuilder::new(if dry_run {
        format!(
            "Dry run complete. {} channel(s) would change: {} renamed, {} suffixed, {} moved.",
            outcome.channels(),
            outcome.renames.len(),
            outcome.suffixes.len(),
            outcome.moves.len()
        )
    } else {
        format!(
            "Triage applied: {} renamed, {} suffixed, {} moved.",
            outcome.renames.len(),
            outcome.suffixes.len(),
            outcome.moves.len()
        )
    })
    .count("judged", outcome.judged as i64)
    .count("renamed", outcome.renames.len() as i64)
    .count("suffixed", outcome.suffixes.len() as i64)
    .count("moved", outcome.moves.len() as i64)
    .count("skipped", outcome.skipped.len() as i64)
    .count("changes", op_plan.changes.len() as i64)
    .count("groups_created", targets.created.len() as i64);
    for (verdict, count) in &outcome.verdicts {
        builder = builder.count(format!("verdict_{verdict}"), *count as i64);
    }
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    // The cancel rule (see crosstalk_core::jobs): whatever landed stays recorded, and the run
    // says it was cancelled rather than claiming the tags are all in place.
    if let Some(note) = channel_ops::cancelled_note(context, &op_plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ServerFixture;
    use crate::{StreamsApp, ACTION_TRIAGE_APPLY, ACTION_TRIAGE_PREVIEW};
    use crosstalk_core::dispatcharr::Channel;
    use crosstalk_core::store::{RunRow, RunStatus, Store, Subject};

    /// A world of four channels, one per verdict triage can act on.
    fn world() -> World {
        let channel = |id: i64, name: &str, group: i64| Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(id)),
            channel_group_id: Some(group),
            ..Channel::default()
        };
        World {
            channels: vec![
                channel(1, "ESPN", 100),
                channel(2, "TNT", 100),
                channel(3, "AMC", 100),
                channel(4, "CNN", 100),
            ],
            groups: vec![ChannelGroup {
                id: 100,
                name: "US: Cable".to_string(),
            }],
            ..World::default()
        }
    }

    fn verdict(
        channel_id: i64,
        name: &str,
        verdict: ChannelVerdict,
        format: Option<&str>,
    ) -> RecordedVerdict {
        RecordedVerdict {
            channel_id,
            channel_name: name.to_string(),
            channel_group: "US: Cable".to_string(),
            verdict,
            format: format.map(str::to_string),
        }
    }

    /// The four shapes: dead, blank, slow and playing.
    fn verdicts() -> Vec<RecordedVerdict> {
        vec![
            verdict(1, "ESPN", ChannelVerdict::Dead, None),
            verdict(2, "TNT", ChannelVerdict::Blank, None),
            verdict(3, "AMC", ChannelVerdict::LowFrameRate, Some("SD")),
            verdict(4, "CNN", ChannelVerdict::Alive, Some("HD")),
        ]
    }

    fn all_on() -> StreamsSettings {
        StreamsSettings {
            rename_dead_channels: true,
            rename_blank_channels: true,
            rename_low_framerate_channels: true,
            add_video_format_suffix: true,
            move_dead_channels: true,
            move_blank_channels: true,
            move_low_framerate_channels: true,
            ..StreamsSettings::default()
        }
    }

    #[test]
    fn every_class_renames_and_moves_and_the_channel_that_plays_gets_a_suffix() {
        let outcome = plan(&world(), &all_on(), &verdicts()).expect("plan");
        let renames: Vec<(i64, &str)> = outcome
            .renames
            .iter()
            .map(|rename| (rename.channel_id, rename.to.as_str()))
            .collect();
        assert_eq!(
            renames,
            vec![(1, "ESPN [DEAD]"), (2, "TNT [Blank]"), (3, "AMC [Slow]"),]
        );
        // A slow channel is Alive in the plugin's terms, so it gets the format suffix too —
        // appended after the rename, which is the plugin's scheduled order.
        let suffixes: Vec<(i64, &str)> = outcome
            .suffixes
            .iter()
            .map(|suffix| (suffix.channel_id, suffix.to.as_str()))
            .collect();
        assert_eq!(suffixes, vec![(3, "AMC [Slow] [SD]"), (4, "CNN [HD]")]);
        let moves: Vec<(i64, &str)> = outcome
            .moves
            .iter()
            .map(|movement| (movement.channel_id, movement.group.as_str()))
            .collect();
        assert_eq!(
            moves,
            vec![(1, "Graveyard"), (2, "Black Screens"), (3, "Slow"),]
        );
        assert_eq!(outcome.judged, 4);
        assert_eq!(outcome.verdicts.get("dead"), Some(&1));
        assert_eq!(outcome.channels(), 4);
    }

    #[test]
    fn a_repeated_tag_is_not_appended_again() {
        // The documented divergence: IPTV Checker renames from the recorded name, so a channel
        // it tagged last night becomes `ESPN [DEAD] [DEAD]` tonight.
        let world = world();
        let verdicts = vec![verdict(1, "ESPN [DEAD]", ChannelVerdict::Dead, None)];
        let outcome = plan(&world, &all_on(), &verdicts).expect("plan");
        assert!(outcome.renames.is_empty(), "{:?}", outcome.renames);
        assert!(
            outcome
                .skipped
                .iter()
                .any(|skip| skip.reason.contains("already ends with [DEAD]")),
            "{:?}",
            outcome.skipped
        );
        // It is still moved: the tag is a rename question, not a move one.
        assert_eq!(outcome.moves.len(), 1);
    }

    #[test]
    fn the_suffix_replaces_an_existing_format_tag() {
        let mut world = world();
        world.channels[3].name = "CNN [SD]".to_string();
        let verdicts = vec![verdict(4, "CNN [SD]", ChannelVerdict::Alive, Some("HD"))];
        let outcome = plan(&world, &all_on(), &verdicts).expect("plan");
        // The suffix step reads the LIVE name and strips the old tag (issue #18).
        assert_eq!(outcome.suffixes[0].from, "CNN [SD]");
        assert_eq!(outcome.suffixes[0].to, "CNN [HD]");
    }

    #[test]
    fn a_format_outside_the_configured_list_gets_no_suffix() {
        let world = world();
        let settings = StreamsSettings {
            add_video_format_suffix: true,
            video_format_suffixes: "UHD, FHD".to_string(),
            ..StreamsSettings::default()
        };
        let verdicts = vec![verdict(4, "CNN", ChannelVerdict::Alive, Some("HD"))];
        let outcome = plan(&world, &settings, &verdicts).expect("plan");
        assert!(outcome.suffixes.is_empty());
        assert!(outcome
            .skipped
            .iter()
            .any(|skip| skip.reason.contains("not in video_format_suffixes")));
    }

    #[test]
    fn a_channel_the_check_proved_nothing_about_is_left_alone() {
        let world = world();
        for verdict_value in [ChannelVerdict::Skipped, ChannelVerdict::Inconclusive] {
            let verdicts = vec![verdict(4, "CNN", verdict_value, None)];
            let outcome = plan(&world, &all_on(), &verdicts).expect("plan");
            assert!(outcome.is_empty(), "{verdict_value:?}");
        }
    }

    #[test]
    fn every_switch_off_is_refused_with_the_list_of_switches() {
        let error = plan(&world(), &StreamsSettings::default(), &verdicts()).expect_err("refused");
        let message = format!("{error}");
        assert!(message.contains("no triage post-action"), "{message}");
        for switch in [
            "rename_dead_channels",
            "add_video_format_suffix",
            "move_low_framerate_channels",
        ] {
            assert!(message.contains(switch), "{switch} not named: {message}");
        }
    }

    #[test]
    fn a_rename_without_the_name_placeholder_is_refused() {
        let settings = StreamsSettings {
            rename_dead_channels: true,
            dead_rename_format: "[DEAD]".to_string(),
            ..StreamsSettings::default()
        };
        let error = plan(&world(), &settings, &verdicts()).expect_err("refused");
        assert!(format!("{error}").contains("{name}"), "{error}");
        assert!(
            format!("{error}").contains("rename_dead_channels"),
            "{error}"
        );
    }

    #[test]
    fn a_move_with_a_blank_group_name_is_refused() {
        let settings = StreamsSettings {
            move_dead_channels: true,
            move_to_group_name: "  ".to_string(),
            ..StreamsSettings::default()
        };
        let error = plan(&world(), &settings, &verdicts()).expect_err("refused");
        assert!(format!("{error}").contains("move_to_group_name"), "{error}");
        assert!(format!("{error}").contains("move_dead_channels"), "{error}");
    }

    #[test]
    fn the_suffix_switch_with_no_formats_is_refused() {
        let settings = StreamsSettings {
            add_video_format_suffix: true,
            video_format_suffixes: " , ".to_string(),
            ..StreamsSettings::default()
        };
        let error = plan(&world(), &settings, &verdicts()).expect_err("refused");
        assert!(
            format!("{error}").contains("video_format_suffixes"),
            "{error}"
        );
    }

    #[test]
    fn a_channel_that_is_gone_is_skipped_not_written() {
        let mut world = world();
        world.channels.retain(|channel| channel.id != 1);
        let outcome = plan(&world, &all_on(), &verdicts()).expect("plan");
        assert!(outcome
            .skipped
            .iter()
            .any(|skip| skip.channel_id == 1 && skip.reason.contains("no longer exists")));
        assert!(!outcome.renames.iter().any(|rename| rename.channel_id == 1));
    }

    #[test]
    fn the_format_tags_come_from_the_brackets_the_plugin_reads() {
        assert_eq!(format_tags("{name} [DEAD]"), vec!["DEAD".to_string()]);
        assert_eq!(
            format_tags("[X] {name} [DEAD]"),
            vec!["X".to_string(), "DEAD".to_string()]
        );
        assert!(format_tags("{name}").is_empty());
        assert!(
            format_tags("{name} [DEAD").is_empty(),
            "an unclosed bracket"
        );
        assert!(ends_with_tag("ESPN [DEAD]", "DEAD"));
        assert!(ends_with_tag("ESPN [dead]", "DEAD"), "case-insensitive");
        assert!(ends_with_tag("ESPN [DEAD] ", "DEAD"));
        assert!(!ends_with_tag("[DEAD] ESPN", "DEAD"));
        assert!(!ends_with_tag("ESPN [DEAD] News", "DEAD"));
    }

    #[test]
    fn the_format_suffix_strips_every_known_tag_before_appending() {
        let tags = known_format_tags(&configured_suffixes(&StreamsSettings::default()));
        assert_eq!(strip_format_tags("ESPN [HD] [SD]", &tags), "ESPN");
        assert_eq!(strip_format_tags("ESPN [unknown]", &tags), "ESPN");
        assert_eq!(strip_format_tags("ESPN", &tags), "ESPN");
        // A status tag is not a FORMAT tag: the suffix step does not touch it.
        assert_eq!(strip_format_tags("ESPN [DEAD]", &tags), "ESPN [DEAD]");
        // A tag in the middle is not trailing, so it stays (the plugin's regex is `$`-anchored).
        assert_eq!(strip_format_tags("[HD] ESPN", &tags), "[HD] ESPN");
    }

    #[test]
    fn a_formats_rendering_leaves_other_braces_alone() {
        assert_eq!(
            render_format("{name} [DEAD] {group}", "ESPN").as_deref(),
            Some("ESPN [DEAD] {group}")
        );
        assert_eq!(render_format("   ", "ESPN"), None);
        assert_eq!(render_format("{name}", "ESPN").as_deref(), Some("ESPN"));
    }

    // ---------------------------------------------------------------------------------
    // Group resolution
    // ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn an_apply_creates_a_missing_group_and_a_preview_does_not() {
        let world = world();
        let moves = vec![Move {
            channel_id: 1,
            class: TriageClass::Dead,
            group: "Parked".to_string(),
        }];

        let preview = GroupTargets::resolve(None, &world, &moves, true)
            .await
            .expect("a preview resolves without a client");
        assert_eq!(preview.missing, vec!["Parked".to_string()]);
        assert!(preview.created.is_empty());
        let placeholder = preview.ids["Parked"];
        assert!(placeholder < 0, "a placeholder id must not be a real id");
        // The plan prints the group's NAME for a placeholder, not "No Group".
        let plan_world = preview.plan_world(&world);
        assert_eq!(plan_world.group_name(Some(placeholder)), "Parked");

        let server = ServerFixture::start(&world).await;
        let client = server.client();
        let applied = GroupTargets::resolve(Some(&client), &world, &moves, false)
            .await
            .expect("an apply creates it");
        assert_eq!(applied.created, vec!["Parked".to_string()]);
        assert_eq!(applied.ids["Parked"], 700);
        let bodies = server.bodies("POST", "/api/channels/groups/").await;
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0]["name"], serde_json::json!("Parked"));
    }

    #[tokio::test]
    async fn an_existing_group_is_reused_not_created() {
        let world = world();
        let moves = vec![Move {
            channel_id: 1,
            class: TriageClass::Dead,
            group: "US: Cable".to_string(),
        }];
        let server = ServerFixture::start(&world).await;
        let client = server.client();
        let targets = GroupTargets::resolve(Some(&client), &world, &moves, false)
            .await
            .expect("resolve");
        assert_eq!(targets.ids["US: Cable"], 100);
        assert!(targets.missing.is_empty());
        assert_eq!(
            server.bodies("POST", "/api/channels/groups/").await.len(),
            0,
            "an existing group is not re-created"
        );
    }

    // ---------------------------------------------------------------------------------
    // Scale
    // ---------------------------------------------------------------------------------

    /// A synthetic world of `count` channels, four in ten dead, one blank, one slow, the rest
    /// playing, with one verdict row each.
    fn synthetic_world(count: i64) -> (World, Vec<RecordedVerdict>) {
        let mut channels = Vec::with_capacity(count as usize);
        let mut verdicts = Vec::with_capacity(count as usize);
        for id in 1..=count {
            let (verdict_value, format) = match id % 10 {
                0..=3 => (ChannelVerdict::Dead, None),
                4 => (ChannelVerdict::Blank, None),
                5 => (ChannelVerdict::LowFrameRate, Some("SD")),
                _ => (ChannelVerdict::Alive, Some("HD")),
            };
            channels.push(Channel {
                id,
                name: format!("Channel {id}"),
                channel_number: Some(serde_json::json!(id)),
                channel_group_id: Some(100),
                ..Channel::default()
            });
            verdicts.push(verdict(id, &format!("Channel {id}"), verdict_value, format));
        }
        (
            World {
                channels,
                groups: vec![ChannelGroup {
                    id: 100,
                    name: "US: Cable".to_string(),
                }],
                ..World::default()
            },
            verdicts,
        )
    }

    #[test]
    fn triaging_twenty_thousand_channels_is_one_pass() {
        // The plan's scale target: one pass over the verdicts, one id index over the channels
        // and one compiled tag matcher — no per-channel regex compile and no per-channel
        // channel lookup. Generous bound: this runs in a DEBUG build.
        let (world, verdicts) = synthetic_world(20_000);
        let started = std::time::Instant::now();
        let outcome = plan(&world, &all_on(), &verdicts).expect("plan");
        let elapsed = started.elapsed();
        assert_eq!(outcome.judged, 20_000);
        assert_eq!(outcome.renames.len(), 12_000, "dead, blank and slow");
        assert_eq!(outcome.moves.len(), 12_000);
        // Every channel that plays gets a suffix, and so does every slow one.
        assert_eq!(outcome.suffixes.len(), 8_000 + 2_000);
        let bound = std::time::Duration::from_secs(10);
        assert!(
            elapsed < bound,
            "planning 20000 channels took {elapsed:?}, over the {bound:?} bound"
        );
        eprintln!("triage bench (guard): 20000 channels in {elapsed:?}");
    }

    // ---------------------------------------------------------------------------------
    // End to end
    // ---------------------------------------------------------------------------------

    /// Record a succeeded probe run with these verdicts, which is what triage reads.
    fn record_probe(store: &Store, verdicts: &[RecordedVerdict]) -> i64 {
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_PROBE, false)
            .expect("start probe");
        let mut rows: Vec<RunRow> = Vec::new();
        for verdict in verdicts {
            rows.push(RunRow {
                ordinal: rows.len() as i64,
                subject: Subject::Channel,
                channel_id: verdict.channel_id,
                channel_name: verdict.channel_name.clone(),
                channel_group: verdict.channel_group.clone(),
                status: verdict.verdict.as_str().to_string(),
                field: probe::VERDICT_FIELD.to_string(),
                after_value: Some(verdict.verdict.as_str().to_string()),
                ..RunRow::default()
            });
            // The format tier the suffix step reads, recorded for a channel that played.
            if let Some(format) = &verdict.format {
                rows.push(RunRow {
                    ordinal: rows.len() as i64,
                    subject: Subject::Channel,
                    channel_id: verdict.channel_id,
                    channel_name: verdict.channel_name.clone(),
                    channel_group: verdict.channel_group.clone(),
                    status: "format".to_string(),
                    reason: format!("last alive stream reports {format}"),
                    field: probe::FORMAT_FIELD.to_string(),
                    after_value: Some(format.clone()),
                    ..RunRow::default()
                });
            }
        }
        store.append_rows(run_id, &rows).expect("append");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("probe"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        run_id
    }

    /// Close a run so the store's one-live-run-per-app index allows the next one.
    fn finish(store: &Store, context: &JobContext) {
        store
            .finish_run(
                context.run_id().expect("run id"),
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
    }

    fn settings_file(server: &ServerFixture, settings: &StreamsSettings) {
        let path = server.config.config_dir.join("streams.toml");
        std::fs::create_dir_all(server.config.config_dir.as_path()).expect("config dir");
        crate::settings::save(&path, settings).expect("save settings");
    }

    #[tokio::test]
    async fn a_triage_run_with_no_probe_refuses_before_any_request() {
        let world = world();
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let context = server.context_for_run(ACTION_TRIAGE_APPLY, &store, false);
        let error = StreamsApp::new()
            .dispatch(ACTION_TRIAGE_APPLY, &context, false)
            .await
            .expect_err("refused");
        assert!(
            format!("{error}").contains("no finished `probe` run"),
            "{error:#}"
        );
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0
        );
    }

    #[tokio::test]
    async fn a_dry_run_probe_is_a_result_set_too() {
        // A --dry-run probe records the same verdict rows and only skips the Dispatcharr
        // write, so triage plans from it exactly like an apply's run.
        let world = world();
        let server = ServerFixture::start(&world).await;
        settings_file(&server, &all_on());
        let store = server.store();
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_PROBE, true)
            .expect("start probe");
        let mut rows: Vec<RunRow> = Vec::new();
        for verdict in verdicts() {
            rows.push(RunRow {
                ordinal: rows.len() as i64,
                subject: Subject::Channel,
                channel_id: verdict.channel_id,
                channel_name: verdict.channel_name.clone(),
                channel_group: verdict.channel_group.clone(),
                status: verdict.verdict.as_str().to_string(),
                field: probe::VERDICT_FIELD.to_string(),
                after_value: Some(verdict.verdict.as_str().to_string()),
                ..RunRow::default()
            });
        }
        store.append_rows(run_id, &rows).expect("append");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("dry run"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        let context = server.context_for_run(ACTION_TRIAGE_PREVIEW, &store, true);
        let preview = StreamsApp::new()
            .dispatch(ACTION_TRIAGE_PREVIEW, &context, true)
            .await
            .expect("a dry-run probe is a result set");
        assert_eq!(preview.counts.get("renamed"), Some(&3));
        finish(&store, &context);
    }

    #[tokio::test]
    async fn preview_writes_nothing_and_apply_writes_names_and_groups() {
        let world = world();
        let server = ServerFixture::start(&world).await;
        settings_file(&server, &all_on());
        let store = server.store();
        record_probe(&store, &verdicts());
        let app = StreamsApp::new();

        let context = server.context_for_run(ACTION_TRIAGE_PREVIEW, &store, true);
        let preview = app
            .dispatch(ACTION_TRIAGE_PREVIEW, &context, true)
            .await
            .expect("preview");
        assert_eq!(preview.counts.get("renamed"), Some(&3));
        assert_eq!(preview.counts.get("suffixed"), Some(&2));
        assert_eq!(preview.counts.get("moved"), Some(&3));
        assert_eq!(
            preview.counts.get("groups_created"),
            Some(&0),
            "a preview creates nothing"
        );
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0,
            "a preview writes nothing"
        );
        assert_eq!(
            server.bodies("POST", "/api/channels/groups/").await.len(),
            0,
            "a preview creates no group"
        );

        finish(&store, &context);
        let context = server.context_for_run(ACTION_TRIAGE_APPLY, &store, false);
        let applied = app
            .dispatch(ACTION_TRIAGE_APPLY, &context, false)
            .await
            .expect("apply");
        // Four names (three marks and one suffix) and three group moves.
        assert_eq!(
            applied.counts.get("written"),
            Some(&7),
            "{:?}",
            applied.counts
        );
        assert_eq!(applied.counts.get("groups_created"), Some(&3));

        // The groups the moves named, then one edit/bulk payload carrying every rename and move.
        let created: Vec<String> = server
            .bodies("POST", "/api/channels/groups/")
            .await
            .iter()
            .map(|body| body["name"].as_str().unwrap_or_default().to_string())
            .collect();
        // In the order the moves name them (channel id), which is the order the groups are
        // created in.
        assert_eq!(created, vec!["Graveyard", "Black Screens", "Slow"]);

        let bodies = server
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1, "one batched write");
        let rows = bodies[0].as_array().expect("the bulk payload is an array");
        let by_id: HashMap<i64, &serde_json::Value> = rows
            .iter()
            .map(|row| (row["id"].as_i64().expect("id"), row))
            .collect();
        assert_eq!(by_id[&1]["name"], serde_json::json!("ESPN [DEAD]"));
        assert_eq!(by_id[&2]["name"], serde_json::json!("TNT [Blank]"));
        assert_eq!(by_id[&3]["name"], serde_json::json!("AMC [Slow] [SD]"));
        assert_eq!(by_id[&4]["name"], serde_json::json!("CNN [HD]"));
        // Every created group answered id 700, so every move targets it (the mock's answer).
        assert_eq!(by_id[&1]["channel_group_id"], serde_json::json!(700));

        // The run rows carry the before values a restore reads.
        let run_id = context.run_id().expect("run id");
        let rows = store.rows(run_id).expect("rows");
        let name_rows: Vec<&RunRow> = rows
            .iter()
            .filter(|row| row.field == "name" && row.status == "ok")
            .collect();
        assert_eq!(name_rows.len(), 4, "{rows:?}");
        assert!(name_rows
            .iter()
            .any(|row| row.channel_id == 1 && row.before_value.as_deref() == Some("ESPN")));
    }

    #[tokio::test]
    async fn a_probe_that_did_not_finish_is_named_as_the_reason() {
        // A windowed probe pauses at its deadline, and its rows are partial: the refusal says
        // which run it found rather than "no probe", which would send the operator looking for
        // a run that is right there.
        let server = ServerFixture::start(&world()).await;
        settings_file(&server, &all_on());
        let store = server.store();
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_PROBE, false)
            .expect("start");
        store
            .finish_run(
                run_id,
                RunStatus::Paused,
                Some("window closed"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let context = server.context_for_run(ACTION_TRIAGE_APPLY, &store, false);
        let error = StreamsApp::new()
            .dispatch(ACTION_TRIAGE_APPLY, &context, false)
            .await
            .expect_err("refused");
        let message = format!("{error}");
        assert!(message.contains("ended paused"), "{message}");
        assert!(message.contains(&format!("#{run_id}")), "{message}");
    }

    #[tokio::test]
    async fn an_apply_with_every_switch_off_refuses_and_writes_nothing() {
        let world = world();
        let server = ServerFixture::start(&world).await;
        // The shipped defaults: every post-action off.
        settings_file(&server, &StreamsSettings::default());
        let store = server.store();
        record_probe(&store, &verdicts());
        let context = server.context_for_run(ACTION_TRIAGE_APPLY, &store, false);
        let error = StreamsApp::new()
            .dispatch(ACTION_TRIAGE_APPLY, &context, false)
            .await
            .expect_err("refused");
        assert!(
            format!("{error}").contains("no triage post-action"),
            "{error:#}"
        );
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0
        );
    }

    #[tokio::test]
    async fn a_channel_outside_the_scope_is_not_written() {
        // A probe deliberately run with a wider scope: the triage run must still respect
        // `[scope] groups`, so the channel in the other group is reported, not written.
        let mut world = world();
        world.channels.push(Channel {
            id: 5,
            name: "Outside".to_string(),
            channel_number: Some(serde_json::json!(5)),
            channel_group_id: Some(101),
            ..Channel::default()
        });
        world.groups.push(ChannelGroup {
            id: 101,
            name: "Outside Group".to_string(),
        });
        let mut server = ServerFixture::start(&world).await;
        server.config.scope.groups = "US: Cable".to_string();
        settings_file(&server, &all_on());
        let store = server.store();
        let mut recorded = verdicts();
        recorded.push(verdict(5, "Outside", ChannelVerdict::Dead, None));
        record_probe(&store, &recorded);

        let context = server.context_for_run(ACTION_TRIAGE_APPLY, &store, false);
        let outcome = StreamsApp::new()
            .dispatch(ACTION_TRIAGE_APPLY, &context, false)
            .await
            .expect("apply");
        let run_id = context.run_id().expect("run id");
        let rows = store.rows(run_id).expect("rows");
        // The out-of-scope channel gets no row at all: it is left out of the plan (and counted
        // as a skip in the summary and the report's "Left alone" note), and the group it would
        // have moved to is not created on its behalf.
        assert!(
            !rows.iter().any(|row| row.channel_id == 5),
            "an out-of-scope channel must not be written: {rows:?}"
        );
        assert!(
            outcome
                .counts
                .get("skipped")
                .is_some_and(|count| *count >= 1),
            "{:?}",
            outcome.counts
        );
        let written: Vec<i64> = rows
            .iter()
            .filter(|row| row.status == "ok")
            .map(|row| row.channel_id)
            .collect();
        assert!(!written.contains(&5), "{written:?}");
        assert_eq!(
            server
                .bodies("PATCH", "/api/channels/channels/edit/bulk/")
                .await
                .len(),
            1,
            "the in-scope channels are still written"
        );
    }
}
