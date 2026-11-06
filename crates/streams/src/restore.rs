//! `restore_preview` / `restore_apply`: undo what triage did to a channel that came back.
//!
//! IPTV Checker's `restore_channels_action` (3878) heals a channel that was marked and exiled
//! and is Alive again: it strips the plugin's name tags and moves the channel back to the
//! exact group it came from. The plugin has two sources for "where it came from":
//!
//!   * `channel_state.json`, written by `_capture_original_state` (3593) just before a move —
//!     `{channel_id: {original_group_id, original_group_name, moved_at}}`, never overwritten,
//!     never recording a channel that has no group or that already sits in a managed group;
//!   * the NAME itself, via [`strip_tags`]: every known tag is a trailing `[TAG]` run, and
//!     `_compute_restore_plan` (3432) is eligible if the name carries a trailing STATUS tag
//!     (`[DEAD]`, `[Slow]`, `[Blank]` or a label from the configured formats) even with no
//!     state entry — the "renamed but never moved" shape.
//!
//! crosstalk has no state file: [`crate::triage`] writes every change through
//! [`channel_ops`], and the run row of a rename or a move carries the `before` value. So the
//! first source is the run store, and the second is the name, exactly as the plugin's second
//! is. That is the one structural divergence, and it buys three things:
//!
//!   * the record cannot drift from the channel: it is the write's own `before` value,
//!     written in the same transaction the run recorded;
//!   * no file to lose, and no lost update between two processes;
//!   * restore survives the state file being pruned, because the fallback is the plugin's.
//!
//! # Which change is still in effect
//!
//! The plugin keeps ONE state entry per channel and clears it after restoring, so restoring
//! twice is a no-op. Here the history is the newest finished `triage_apply` runs' channel rows
//! ([`crosstalk_core::store::Store::field_history`]), oldest first, and a change is still in
//! effect when the channel's CURRENT value equals the change's `after` value. The run follows
//! that chain backwards while it holds, and writes where it ends:
//!
//! ```text
//! current = channel's value now
//! for change in newest..oldest:
//!     if current == change.after { current = change.before; target = change.before }
//!     else break
//! ```
//!
//! A trailing run of format tags (` [HD]`) on top of the recorded mark does not break the
//! chain: triage's own suffix step (or a hand-added quality tag) appends it to the marked
//! name, so the chain is followed from the name with that run stripped and the restore drops
//! it with the mark — which is what the plugin's tag strip does with the same name.
//!
//! One change is the normal case, and the answer is its `before` — the plugin's state entry.
//! Two marks on one channel (dead, restored, dead again) both unwind, to the value from before
//! the first: the plugin's state entry is never overwritten either, so it restores the same
//! place. Nothing needs to be written to mark the reversal: a restore that wrote the `before`
//! leaves the channel holding it, which breaks the chain on the next run. That is stronger
//! than a marker row, because a marker can be pruned while the value it marked is still there.
//!
//! # The name fallback, ported
//!
//! [`strip_tags`] is `_derive_strippable_tags` (3419): the standard status tags
//! (`DEAD`, `Slow`, `Blank`), the standard quality tags (`UHD`, `FHD`, `HD`, `SD`, `Unknown`),
//! the bracket labels of the three rename formats, and the configured `video_format_suffixes`
//! — every one of them stripped as a TRAILING run, case-insensitively. [`has_status_tag`] is
//! `_derive_status_tags` (3411) and decides eligibility: a healthy channel that merely carries
//! `[HD]` is never touched (the plugin's own user guide says so), while `ESPN [DEAD]` is.
//!
//! # What it does NOT undo
//!
//! A recorded FORMAT SUFFIX (`ESPN` → `ESPN [HD]`) is left in place. The plugin's own settings
//! section says so — "It does NOT strip the video format suffix, which is applied to channels
//! that play" — and undoing it nightly would flap against the next triage run. Only a MARK
//! (`[DEAD]`, `[Slow]`, `[Blank]`, a custom label) is reversed. The name fallback below is the
//! plugin's own, so an IC-tagged channel still loses its quality tag with its status tag, which
//! is IC's `_derive_strippable_tags`. The parity harness counts that shape.
//!
//! # Where it runs
//!
//! IPTV Checker's scheduled order puts Restore FIRST, before re-marking anything
//! (`plugin.py:2058-2066`): heal, then judge. A workflow says the same thing —
//! `steps = ["streams.probe", "streams.restore_apply", "streams.triage_apply"]` — and a
//! schedule can run `streams.restore_apply` on its own, on the same rule the plugin's
//! `scheduler_restore_channels` switch uses.
//!
//! # Group "No Group"
//!
//! A channel triage moved OUT of "No Group" recorded `none` as its `before` group, and
//! Dispatcharr's channel serializer rejects a null `channel_group_id` — so the move is planned
//! and reported and refused per row by [`channel_ops::apply`] with that reason. The plugin
//! never records such a channel at all (`_compute_capture_state` skips a channel with no
//! group), so this shape only exists because crosstalk records every change. `README.md` lists
//! it among the differences; the bridge closes it later.
//!
//! # Divergences, each deliberate
//!
//!   * the restore target is what crosstalk WROTE, not what the name looks like: a `[HD]` the
//!     operator added by hand survives a restore, where the plugin's tag strip removes it. A
//!     channel with no recorded change at all still gets the plugin's strip, which is how a
//!     channel tagged by the plugin before crosstalk took over is healed;
//!   * a recorded group that no longer exists keeps its name restored and its group untouched,
//!     with the reason in the CSV — the plugin's `missing_group_ids` warning (3906-3910), as a
//!     per-row skip;
//!   * `LowFrameRate` counts as recovered, because the plugin's `alive_ids` is built from rows
//!     whose `status` is `Alive` and a slow channel's rows are Alive. It is re-tagged by the
//!     next triage run, which is the plugin's own flap and not a new one.

use crate::probe::{self, ChannelVerdict, RecordedVerdict};
use crate::settings::StreamsSettings;
use crate::triage::configured_suffixes;
use anyhow::Result;
use crosstalk_core::channel_ops::{self, Field, Op, ReportSpec, Selection, SetValue};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::store::{RunRow, Subject};
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use std::collections::{HashMap, HashSet};

/// The report's title, without the preview/applied suffix.
const REPORT_TITLE: &str = "Restore Recovered Channels";

/// How many `triage_apply` runs back the chain is read.
///
/// The newest run usually settles every channel a restore cares about (a triage run records a
/// row for every channel it selected), so this is a safety bound rather than a working limit:
/// it keeps a busy install from reading a month of rows to find one name. `[jobs]
/// run_history_days` prunes the table underneath, and a change older than this cap simply
/// falls back to what the name itself says.
pub const HISTORY_RUNS: usize = 50;

/// The tags this plugin can append to a name, whatever the settings say — the plugin's
/// `STANDARD_STATUS_TAGS` (3324), read from the unified vocabulary (the `status` rows whose
/// source is IC). The order differs from the plugin's tuple; the strip pattern sorts its
/// alternation by length, so the order never reaches the output.
pub fn standard_status_tags() -> Vec<&'static str> {
    crosstalk_matching::tags::tokens_from(crosstalk_matching::tags::TagKind::Status, "IC")
}

/// The quality tags this plugin can append, whatever the settings say — the plugin's
/// `STANDARD_QUALITY_TAGS` (3325), which is the shared quality model's label set.
pub const STANDARD_QUALITY_TAGS: [&str; 5] = crosstalk_core::quality::IC_FORMAT_LABELS;

/// Where a restored value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A `triage_apply` run row: the `before` value of a change that still holds.
    Recorded,
    /// No recorded change: the plugin's tag strip on the name itself.
    Tags,
}

impl Source {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::Tags => "stripped",
        }
    }
}

/// One channel's planned name restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    pub channel_id: i64,
    /// The channel's name now.
    pub from: String,
    pub to: String,
    pub source: Source,
}

/// One channel's planned move back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveBack {
    pub channel_id: i64,
    /// The group the channel is in now; `None` is "No Group".
    pub from: Option<i64>,
    /// The group it came from; `None` is "No Group", which a REST write cannot express (see
    /// the module docs).
    pub to: Option<i64>,
}

/// A channel restore looked at and left alone, with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
    pub channel_id: i64,
    pub reason: String,
}

/// What one restore run would write. Nothing has been written.
#[derive(Debug, Clone, Default)]
pub struct RestoreOutcome {
    /// Channels the last probe found alive (or playing slowly, which the plugin counts too).
    pub recovered: usize,
    /// Rows read from the triage history.
    pub history_rows: usize,
    pub renames: Vec<Rename>,
    pub moves: Vec<MoveBack>,
    pub skipped: Vec<Skip>,
    /// Recorded groups that no longer exist, so the move was dropped (the plugin's
    /// `missing_group_ids`).
    pub missing_groups: Vec<i64>,
}

impl RestoreOutcome {
    /// The channels this run touches.
    pub fn channels(&self) -> usize {
        self.target_ids().len()
    }

    /// The ids this run would write, sorted and de-duplicated.
    pub fn target_ids(&self) -> Vec<i64> {
        let mut ids: HashSet<i64> = HashSet::new();
        ids.extend(self.renames.iter().map(|rename| rename.channel_id));
        ids.extend(self.moves.iter().map(|movement| movement.channel_id));
        let mut ids: Vec<i64> = ids.into_iter().collect();
        ids.sort_unstable();
        ids
    }

    /// Whether this run would write nothing.
    pub fn is_empty(&self) -> bool {
        self.renames.is_empty() && self.moves.is_empty()
    }

    /// The skips in words, for the report: `2 the name has changed since the recorded
    /// rename`, one clause per reason.
    pub fn skip_summary(&self) -> Option<String> {
        if self.skipped.is_empty() {
            return None;
        }
        let mut counts: std::collections::BTreeMap<&str, usize> = Default::default();
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

    /// How many channels were healed from a recorded change, and how many from the name.
    pub fn by_source(&self) -> (usize, usize) {
        let recorded = self
            .renames
            .iter()
            .filter(|rename| rename.source == Source::Recorded)
            .count();
        let tags = self.renames.len() - recorded;
        (recorded, tags)
    }
}

/// The tag sets one restore run strips with: what makes a channel eligible for the tag
/// strip and what the strip removes.
///
/// Step 9: the plugin's compiled `strip_re`/`status_re` (3888-3889) become the shared
/// trailing-tag parser ([`tags::strip_with`] / [`tags::has_trailing`]), which compiles
/// nothing — so the one-matcher-per-run shape costs nothing per channel either.
///
/// Strip-set decision: restore strips exactly what triage can have written — the
/// IC-source `status` rows (`[DEAD]`/`[Slow]`/`[Blank]`), the five IC format labels, the
/// rename-format labels and the configured suffixes ([`strippable_tags`], the plugin's
/// `_derive_strippable_tags`) — never the rest of the vocabulary. CM `Unk` is lineup's
/// mark and guide `BadEPG` is guide's, and the vocabulary's other quality rows
/// (`[1080p]`, `[HEVC]`, `[RAW]`, ...) are provider tags triage never appends: stripping
/// any of them would undo another app's (or the provider's) name. The parser change
/// alone is the output change: parens strip and stacked mixed runs strip in one pass.
#[derive(Debug, Clone)]
pub struct Tagger {
    status: Vec<String>,
    strippable: Vec<String>,
}

impl Tagger {
    /// The tag sets of this settings file.
    pub fn of(settings: &StreamsSettings) -> Self {
        Self {
            status: status_tags(settings),
            strippable: strippable_tags(settings),
        }
    }

    /// Whether the name carries a trailing STATUS tag — the plugin's `status_re.search` (3442).
    ///
    /// Step 9: the shared trailing-tag check (parens and literal affixes count now).
    pub fn is_eligible(&self, name: &str) -> bool {
        use crosstalk_matching::tags::has_trailing;
        self.status.iter().any(|label| has_trailing(name, label))
    }

    /// The name with every known tag stripped, or `None` when nothing would change or the
    /// result would be empty (the plugin's `if base and base != name`, 3448-3451).
    ///
    /// Step 9 output changes: parens strip and stacked mixed runs strip in one pass.
    pub fn strip(&self, name: &str) -> Option<String> {
        let base = crosstalk_matching::tags::strip_with(name, &[], &self.strippable);
        if base.is_empty() || base == name {
            None
        } else {
            Some(base)
        }
    }

    /// The format-tag strip, for [`is_format_suffix`]: the standard five plus the
    /// configured suffixes.
    pub fn strip_format(&self, name: &str, settings: &StreamsSettings) -> String {
        crate::triage::strip_format_tags(name, settings)
    }
}

/// The bracket labels of a rename format — the plugin's `_extract_format_tags` (3393).
///
/// Step 9: one parser beside `naming`; this delegates there.
pub fn format_tags(format: &str) -> Vec<String> {
    crosstalk_core::naming::format_tags(format)
}

/// Every tag this settings file can strip: the standard status and quality tags, the labels of
/// the three rename formats, and `video_format_suffixes` — `_derive_strippable_tags` (3419).
pub fn strippable_tags(settings: &StreamsSettings) -> Vec<String> {
    let mut tags: Vec<String> = standard_status_tags()
        .iter()
        .chain(STANDARD_QUALITY_TAGS.iter())
        .map(|tag| tag.to_string())
        .collect();
    for format in [
        &settings.dead_rename_format,
        &settings.low_framerate_rename_format,
        &settings.black_screen_rename_format,
    ] {
        tags.extend(format_tags(format));
    }
    tags.extend(configured_suffixes(settings));
    tags
}

/// The tags that make a channel ELIGIBLE for the fallback: the status tags plus the labels of
/// the three rename formats — `_derive_status_tags` (3411). A quality tag is not here, which is
/// why a healthy channel carrying `[HD]` is never touched.
pub fn status_tags(settings: &StreamsSettings) -> Vec<String> {
    let mut tags: Vec<String> = standard_status_tags()
        .iter()
        .map(|tag| tag.to_string())
        .collect();
    for format in [
        &settings.dead_rename_format,
        &settings.low_framerate_rename_format,
        &settings.black_screen_rename_format,
    ] {
        tags.extend(format_tags(format));
    }
    tags
}

/// Whether the name ends with one of the STATUS tags — the plugin's eligibility test.
///
/// Step 9: the shared trailing-tag check over the unified vocabulary.
pub fn has_status_tag(name: &str, settings: &StreamsSettings) -> bool {
    use crosstalk_matching::tags::has_trailing;
    status_tags(settings)
        .iter()
        .any(|label| has_trailing(name, label))
}

/// Strip every known tag from the name — `strip_re.sub('', name).rstrip()` (3448).
///
/// `None` when nothing would change, or when the result is empty: a name that is ONLY a tag
/// keeps it, which is the plugin's `if base and base != name`. Step 9: the shared parser
/// (parens strip; stacked mixed runs strip in one pass) over the plugin's own set.
pub fn strip_tags(name: &str, settings: &StreamsSettings) -> Option<String> {
    Tagger::of(settings).strip(name)
}

/// One recorded change to one field of one channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub before: String,
    pub after: String,
}

/// Whether a recorded name change is a FORMAT SUFFIX rather than a mark.
///
/// The suffix step appends ` [HD]` (and replaces whatever format tag was there), which leaves
/// the name as the old name plus one trailing format tag. Restore deliberately does not undo
/// that: the plugin's own settings section says so ("It does NOT strip the video format
/// suffix, which is applied to channels that play rather than to ones that failed"), and
/// undoing it nightly would flap against the next triage run. A MARK ([DEAD], [Slow], [Blank],
/// or a custom label) leaves `after` minus a format tag unequal to `before`, so it stays.
fn is_format_suffix(before: &str, after: &str, settings: &StreamsSettings) -> bool {
    after != before
        && crate::triage::strip_format_tags(after, settings).trim_end() == before.trim_end()
}

/// The recorded changes per (channel, field), oldest first.
///
/// Only rows that WROTE something count: a triage row with `status == "ok"` whose field is one
/// this restore reverses. A skip row (a no-op, an already-tagged channel) is not a change, and
/// neither is a `planned` row from a preview or a `failed` one — and a recorded format suffix
/// is left out of the name chain on purpose (see [`is_format_suffix`]).
fn chains(history: &[RunRow], settings: &StreamsSettings) -> HashMap<(i64, Field), Vec<Change>> {
    let mut chains: HashMap<(i64, Field), Vec<Change>> = HashMap::new();
    for row in history {
        if row.subject != Subject::Channel || row.status != "ok" {
            continue;
        }
        let Some(field) = Field::from_label(&row.field) else {
            continue;
        };
        if !matches!(field, Field::Name | Field::Group) {
            continue;
        }
        let (Some(before), Some(after)) = (row.before_value.clone(), row.after_value.clone())
        else {
            continue;
        };
        if field == Field::Name && is_format_suffix(&before, &after, settings) {
            continue;
        }
        chains
            .entry((row.channel_id, field))
            .or_default()
            .push(Change { before, after });
    }
    chains
}

/// Follow the chain of changes that still hold, newest first, and return where it ends.
///
/// `current` is the channel's value now. A change whose `after` is not the current value never
/// took effect (or was already undone), so the walk stops: the newest change that holds is the
/// one to undo.
fn unwind<'a>(current: &str, changes: &'a [Change]) -> Option<&'a str> {
    let mut value = current.to_string();
    let mut target: Option<&'a str> = None;
    for change in changes.iter().rev() {
        if value != change.after {
            break;
        }
        value = change.before.clone();
        target = Some(change.before.as_str());
    }
    target
}

/// The group wire value of a channel's id, which is how [`crosstalk_core::channel_ops`] records
/// a group (`none` for "No Group").
fn group_wire(group: Option<i64>) -> String {
    match group {
        Some(id) => id.to_string(),
        None => "none".to_string(),
    }
}

/// Parse a recorded group wire value back into an id.
fn group_of(wire: &str) -> Option<i64> {
    let wire = wire.trim();
    if wire.is_empty() || wire.eq_ignore_ascii_case("none") {
        return None;
    }
    wire.parse().ok()
}

/// Plan a restore run over the last probe's verdicts and the recorded triage changes.
///
/// Pure: nothing is written. `history` is [`crosstalk_core::store::Store::field_history`]'s
/// answer — the channel rows of `name` and `channel_group_id` from the newest finished
/// `triage_apply` runs, OLDEST FIRST — and `world` is the live channel list, so a channel that
/// was deleted or renamed by hand since is handled by what is there now, not by what was
/// recorded.
pub fn plan(
    world: &World,
    settings: &StreamsSettings,
    verdicts: &[RecordedVerdict],
    history: &[RunRow],
) -> RestoreOutcome {
    let tagger = Tagger::of(settings);
    let chains = chains(history, settings);
    // One index over the channel list: a `find` per recovered channel would be quadratic at
    // 20k channels, which is exactly the shape this app exists to avoid.
    let by_id: HashMap<i64, &crosstalk_core::dispatcharr::Channel> = world
        .channels
        .iter()
        .map(|channel| (channel.id, channel))
        .collect();
    let groups: HashSet<i64> = world.groups.iter().map(|group| group.id).collect();
    let mut outcome = RestoreOutcome {
        history_rows: history.len(),
        ..RestoreOutcome::default()
    };

    for verdict in verdicts {
        // The plugin's `alive_ids` (3884): rows whose status is Alive, which is what a slow
        // channel's rows are too.
        if !matches!(
            verdict.verdict,
            ChannelVerdict::Alive | ChannelVerdict::LowFrameRate
        ) {
            continue;
        }
        outcome.recovered += 1;
        let Some(channel) = by_id.get(&verdict.channel_id).copied() else {
            outcome.skipped.push(Skip {
                channel_id: verdict.channel_id,
                reason: "the channel no longer exists in Dispatcharr".to_string(),
            });
            continue;
        };

        // 1. The name: the chain of recorded renames, or — for a channel crosstalk never
        //    renamed — the plugin's tag strip.
        let name_changes = chains
            .get(&(channel.id, Field::Name))
            .map(Vec::as_slice)
            .unwrap_or_default();
        // A format suffix stacked on top of the recorded mark: triage's own suffix step (or
        // a hand-added quality tag) appends ` [HD]` to the marked name, so the current value
        // no longer equals the recorded `after` even though the mark is still in effect. The
        // plugin's tag strip removes that trailing run along with the mark, so the chain is
        // followed from the name with the format run stripped — and the restore drops it too,
        // exactly as the plugin's does.
        let stripped = tagger.strip_format(&channel.name, settings);
        let effective = unwind(&channel.name, name_changes).or_else(|| {
            if stripped == channel.name {
                return None;
            }
            unwind(&stripped, name_changes)
        });
        match effective {
            Some(target) => {
                if target != channel.name {
                    outcome.renames.push(Rename {
                        channel_id: channel.id,
                        from: channel.name.clone(),
                        to: target.to_string(),
                        source: Source::Recorded,
                    });
                }
            }
            None if name_changes.is_empty() && tagger.is_eligible(&channel.name) => {
                match tagger.strip(&channel.name) {
                    Some(base) => outcome.renames.push(Rename {
                        channel_id: channel.id,
                        from: channel.name.clone(),
                        to: base,
                        source: Source::Tags,
                    }),
                    None => outcome.skipped.push(Skip {
                        channel_id: channel.id,
                        reason: "the name is only a tag, so stripping it would leave nothing"
                            .to_string(),
                    }),
                }
            }
            None if !name_changes.is_empty() => {
                // The newest recorded rename no longer holds: somebody renamed the channel
                // since. Restoring a recorded value would undo their change too.
                outcome.skipped.push(Skip {
                    channel_id: channel.id,
                    reason: "the name has changed since the recorded rename, so it is left \
                             alone"
                        .to_string(),
                });
            }
            None => {}
        }

        // 2. The group: the chain of recorded moves, and only a group that still exists.
        let group_changes = chains
            .get(&(channel.id, Field::Group))
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Some(target) = unwind(&group_wire(channel.channel_group_id), group_changes) {
            let target = group_of(target);
            match target {
                Some(id) if !groups.contains(&id) => {
                    // The plugin warns and leaves the channel where it is (3906-3910).
                    outcome.missing_groups.push(id);
                    outcome.skipped.push(Skip {
                        channel_id: channel.id,
                        reason: format!(
                            "the group it came from (id {id}) no longer exists, so its group is \
                             left alone"
                        ),
                    });
                }
                _ => outcome.moves.push(MoveBack {
                    channel_id: channel.id,
                    from: channel.channel_group_id,
                    to: target,
                }),
            }
        }
    }

    outcome.renames.sort_by_key(|rename| rename.channel_id);
    outcome.moves.sort_by_key(|movement| movement.channel_id);
    outcome.skipped.sort_by_key(|skip| skip.channel_id);
    outcome
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
        "streams.restore_{}",
        if dry_run { "preview" } else { "apply" }
    );

    // 1. Which channels recovered, and what crosstalk did to them.
    let probe = probe::last_verdicts(context)?;
    probe::announce(context, &action_id, &probe);
    let actions = [crate::ACTION_TRIAGE_APPLY];
    let fields = [Field::Name.label(), Field::Group.label()];
    let history = match context.store() {
        Some(store) => store.field_history(
            crate::APP_ID,
            &actions,
            &fields.iter().map(String::as_str).collect::<Vec<&str>>(),
            HISTORY_RUNS,
        )?,
        // `last_verdicts` already refused a run with no store, so this is unreachable; a
        // restore with no history is the tag-strip fallback rather than an error.
        None => Vec::new(),
    };

    // 2. The live channels, which is what every value below is compared against.
    // Profiles when the scope names them, or when a step override is present (a named
    // scope resolving to a table may name them).
    let mut kinds = WorldKinds::only(WorldKind::Channels) | WorldKind::Groups;
    if context.may_need_profiles() {
        kinds = kinds | WorldKind::Profiles;
    }
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.restore",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = client.load_world_with(kinds).await?;
    // The scope is resolved by `channel_ops::plan` below, which is where every channel
    // operation is held to it; a restore writes nothing before that.
    let outcome = plan(&world, settings, &probe.verdicts, &history);

    // 3. One per-channel value map per field, through channel_ops.
    let mut names: HashMap<i64, SetValue> = HashMap::new();
    for rename in &outcome.renames {
        names.insert(rename.channel_id, SetValue::Name(rename.to.clone()));
    }
    let mut groups: HashMap<i64, SetValue> = HashMap::new();
    for movement in &outcome.moves {
        groups.insert(movement.channel_id, SetValue::Group(movement.to));
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
    if operations.is_empty() {
        operations.push(Op::Set {
            field: Field::Name,
            values: HashMap::new(),
        });
    }

    // Every recovered channel is the selection, so a channel with nothing to restore is
    // reported by `channel_ops` as the no-op it is rather than silently dropped. A step
    // override resolving to ids bounds the recovered set to exactly those channels.
    let mut recovered_ids: Vec<i64> = probe
        .verdicts
        .iter()
        .filter(|verdict| {
            matches!(
                verdict.verdict,
                ChannelVerdict::Alive | ChannelVerdict::LowFrameRate
            )
        })
        .map(|verdict| verdict.channel_id)
        .collect();
    let base = context.base_selection(&world)?;
    if let Some(limit) = base.channel_ids.as_deref() {
        recovered_ids.retain(|id| limit.contains(id));
    }
    let selection = Selection {
        channel_ids: Some(recovered_ids),
        ..base
    };
    let op_plan = channel_ops::plan(&world, &selection, &operations)?;

    let applied = if dry_run || op_plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &op_plan, context, &action_id).await)
    };

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!("restore_{}", if dry_run { "preview" } else { "applied" });
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if dry_run { "Preview" } else { "Applied" }
    );
    let (recorded, stripped) = outcome.by_source();
    let mut spec = ReportSpec::new(app, &schema, &values, &stem, &title)
        .note("Result set", probe.describe())
        .note(
            "Recorded changes",
            format!(
                "{} row(s) from the newest {HISTORY_RUNS} triage run(s): {recorded} channel(s) \
                 restored from a recorded change, {stripped} from a name tag",
                outcome.history_rows
            ),
        );
    if !outcome.missing_groups.is_empty() {
        spec = spec.note(
            "Groups gone",
            format!(
                "{} recorded group id(s) no longer exist: {}",
                outcome.missing_groups.len(),
                outcome
                    .missing_groups
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
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
            "Dry run complete. {} of {} recovered channel(s) would be restored: {} renamed, {} \
             moved back.",
            outcome.channels(),
            outcome.recovered,
            outcome.renames.len(),
            outcome.moves.len()
        )
    } else {
        format!(
            "Restored {} recovered channel(s): {} renamed, {} moved back to their original \
             group.",
            outcome.channels(),
            outcome.renames.len(),
            outcome.moves.len()
        )
    })
    .count("recovered", outcome.recovered as i64)
    .count("renamed", outcome.renames.len() as i64)
    .count("moved", outcome.moves.len() as i64)
    .count("from_recorded", recorded as i64)
    .count("from_tags", stripped as i64)
    .count("history_rows", outcome.history_rows as i64)
    .count("skipped", outcome.skipped.len() as i64)
    .count("changes", op_plan.changes.len() as i64);
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if !outcome.missing_groups.is_empty() {
        builder = builder.note(format!(
            "{} recorded group(s) no longer exist, so those channels keep their group",
            outcome.missing_groups.len()
        ));
    }
    if let Some(note) = channel_ops::cancelled_note(context, &op_plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ServerFixture;
    use crate::{StreamsApp, ACTION_RESTORE_APPLY, ACTION_RESTORE_PREVIEW, ACTION_TRIAGE_APPLY};
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup};
    use crosstalk_core::store::{RunRow, RunStatus, Store, Subject};

    fn world() -> World {
        let channel = |id: i64, name: &str, group: Option<i64>| Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(id)),
            channel_group_id: group,
            ..Channel::default()
        };
        World {
            channels: vec![
                channel(1, "ESPN [DEAD]", Some(102)),
                channel(2, "Sky", Some(100)),
                channel(3, "AMC [Blank]", Some(102)),
                channel(4, "CNN", Some(100)),
            ],
            groups: vec![
                ChannelGroup {
                    id: 100,
                    name: "US: Cable".to_string(),
                },
                ChannelGroup {
                    id: 102,
                    name: "Graveyard".to_string(),
                },
            ],
            ..World::default()
        }
    }

    fn alive(channel_id: i64, name: &str) -> RecordedVerdict {
        RecordedVerdict {
            channel_id,
            channel_name: name.to_string(),
            channel_group: String::new(),
            verdict: ChannelVerdict::Alive,
            format: Some("HD".to_string()),
        }
    }

    fn dead(channel_id: i64, name: &str) -> RecordedVerdict {
        RecordedVerdict {
            channel_id,
            channel_name: name.to_string(),
            channel_group: String::new(),
            verdict: ChannelVerdict::Dead,
            format: None,
        }
    }

    /// One `triage_apply` row, which is what a restore reads.
    fn row(channel_id: i64, field: &str, before: &str, after: &str) -> RunRow {
        RunRow {
            subject: Subject::Channel,
            channel_id,
            field: field.to_string(),
            before_value: Some(before.to_string()),
            after_value: Some(after.to_string()),
            status: "ok".to_string(),
            ..RunRow::default()
        }
    }

    #[test]
    fn a_recorded_rename_and_move_are_undone() {
        let mut world = world();
        world.channels[0].name = "ESPN [DEAD]".to_string();
        let history = vec![
            row(1, "name", "ESPN", "ESPN [DEAD]"),
            row(1, "channel_group_id", "100", "102"),
        ];
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &history,
        );
        assert_eq!(outcome.renames.len(), 1);
        assert_eq!(outcome.renames[0].to, "ESPN");
        assert_eq!(outcome.renames[0].source, Source::Recorded);
        assert_eq!(outcome.moves.len(), 1);
        assert_eq!(outcome.moves[0].from, Some(102));
        assert_eq!(outcome.moves[0].to, Some(100));
        assert_eq!(outcome.recovered, 1);
        assert_eq!(outcome.by_source(), (1, 0));
    }

    #[test]
    fn a_channel_that_is_still_dead_is_left_alone() {
        let history = vec![
            row(1, "name", "ESPN", "ESPN [DEAD]"),
            row(1, "channel_group_id", "100", "102"),
        ];
        let outcome = plan(
            &world(),
            &StreamsSettings::default(),
            &[dead(1, "ESPN")],
            &history,
        );
        assert_eq!(outcome.recovered, 0);
        assert!(outcome.is_empty());
    }

    #[test]
    fn a_slow_channel_counts_as_recovered() {
        // The plugin's `alive_ids` is built from rows whose status is Alive, and a slow
        // channel's rows are Alive.
        let mut verdict = alive(1, "ESPN");
        verdict.verdict = ChannelVerdict::LowFrameRate;
        let outcome = plan(
            &world(),
            &StreamsSettings::default(),
            &[verdict],
            &[row(1, "name", "ESPN", "ESPN [DEAD]")],
        );
        assert_eq!(outcome.recovered, 1);
        assert_eq!(outcome.renames.len(), 1);
    }

    #[test]
    fn an_already_restored_channel_is_a_no_op() {
        // The recorded change's `after` is not the current value, so it no longer holds: the
        // restored value IS the marker, which no pruning can lose.
        let mut world = world();
        world.channels[0].name = "ESPN".to_string();
        world.channels[0].channel_group_id = Some(100);
        let history = vec![
            row(1, "name", "ESPN", "ESPN [DEAD]"),
            row(1, "channel_group_id", "100", "102"),
        ];
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &history,
        );
        assert!(outcome.is_empty(), "{outcome:?}");
    }

    #[test]
    fn the_chain_unwinds_two_stacked_marks() {
        let mut world = world();
        world.channels[0].name = "ESPN [DEAD] [DEAD]".to_string();
        let history = vec![
            row(1, "name", "ESPN", "ESPN [DEAD]"),
            row(1, "name", "ESPN [DEAD]", "ESPN [DEAD] [DEAD]"),
        ];
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &history,
        );
        assert_eq!(outcome.renames[0].to, "ESPN", "the oldest pending value");
    }

    #[test]
    fn a_recorded_format_suffix_is_not_undone() {
        // IPTV Checker's own settings section: restore does not strip the video format
        // suffix, because it was applied to a channel that plays.
        let mut world = world();
        world.channels[3].name = "CNN [HD]".to_string();
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(4, "CNN")],
            &[row(4, "name", "CNN", "CNN [HD]")],
        );
        assert!(outcome.renames.is_empty(), "{:?}", outcome.renames);
    }

    #[test]
    fn a_format_suffix_stacked_on_the_mark_does_not_block_the_restore() {
        // Triage marked the channel dead, then — before any restore ran — triage ran again on
        // the recovered channel and its suffix step appended ` [HD]` to the marked name (a
        // hand-added quality tag lands in the same shape). The plugin's tag strip removes the
        // whole trailing run; the chain follows from the name with the format run stripped
        // and drops it with the mark.
        let mut world = world();
        world.channels[0].name = "ESPN [DEAD] [HD]".to_string();
        let history = vec![
            row(1, "name", "ESPN", "ESPN [DEAD]"),
            // The suffix step's own write: excluded from the chain (a suffix is not a mark),
            // but present in the history all the same.
            row(1, "name", "ESPN [DEAD]", "ESPN [DEAD] [HD]"),
        ];
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &history,
        );
        assert_eq!(outcome.renames.len(), 1, "{outcome:?}");
        assert_eq!(outcome.renames[0].to, "ESPN");
        assert_eq!(outcome.renames[0].source, Source::Recorded);
    }

    #[test]
    fn a_chain_that_no_longer_holds_is_left_alone() {
        // Somebody renamed the channel after triage: restoring would undo their change too.
        let mut world = world();
        world.channels[0].name = "ESPN News".to_string();
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &[row(1, "name", "ESPN", "ESPN [DEAD]")],
        );
        assert!(outcome.renames.is_empty());
        assert!(outcome
            .skipped
            .iter()
            .any(|skip| skip.reason.contains("changed since the recorded rename")));
    }

    #[test]
    fn the_tag_strip_heals_a_channel_crosstalk_never_touched() {
        // A channel IPTV Checker tagged before crosstalk took over: no history, but a trailing
        // status tag, which is the plugin's own eligibility rule.
        let outcome = plan(
            &world(),
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &[],
        );
        assert_eq!(outcome.renames.len(), 1);
        assert_eq!(outcome.renames[0].to, "ESPN");
        assert_eq!(outcome.renames[0].source, Source::Tags);
        assert_eq!(outcome.by_source(), (0, 1));
    }

    #[test]
    fn the_tag_strip_never_removes_another_apps_mark_or_a_provider_tag() {
        // Restore strips only what triage can have written: guide's `[BadEPG]`, CM's
        // `[Unk]` and a provider's `[1080p]` stay; the status tag (bracketed or in
        // parens, the shared parser) goes.
        let settings = StreamsSettings::default();
        assert_eq!(
            strip_tags("ESPN [BadEPG] [DEAD]", &settings).as_deref(),
            Some("ESPN [BadEPG]")
        );
        assert_eq!(
            strip_tags("ESPN [1080p] [DEAD]", &settings).as_deref(),
            Some("ESPN [1080p]")
        );
        assert_eq!(
            strip_tags("ESPN [Unk] (DEAD)", &settings).as_deref(),
            Some("ESPN [Unk]")
        );
        assert_eq!(strip_tags("ESPN [BadEPG]", &settings), None);
        assert!(!has_status_tag("ESPN [BadEPG]", &settings));
        assert_eq!(
            strip_tags("ESPN [HD] [DEAD]", &settings).as_deref(),
            Some("ESPN")
        );
    }

    #[test]
    fn a_quality_tag_alone_is_never_stripped() {
        let mut world = world();
        world.channels[3].name = "CNN [HD]".to_string();
        let outcome = plan(&world, &StreamsSettings::default(), &[alive(4, "CNN")], &[]);
        assert!(outcome.renames.is_empty(), "{:?}", outcome.renames);
    }

    #[test]
    fn a_name_that_is_only_a_tag_keeps_it() {
        let mut world = world();
        world.channels[0].name = "[DEAD]".to_string();
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &[],
        );
        assert!(outcome.renames.is_empty());
        assert!(outcome
            .skipped
            .iter()
            .any(|skip| skip.reason.contains("only a tag")));
    }

    #[test]
    fn stacked_tags_collapse_to_the_clean_base() {
        // `_derive_strippable_tags` (3419): every known tag, as a trailing run.
        let settings = StreamsSettings::default();
        assert_eq!(
            strip_tags("ESPN [HD] [Blank]", &settings).as_deref(),
            Some("ESPN")
        );
        assert_eq!(
            strip_tags("ESPN [dead]", &settings).as_deref(),
            Some("ESPN")
        );
        assert_eq!(
            strip_tags("[DEAD] ESPN Sports", &settings),
            None,
            "not trailing"
        );
        assert_eq!(
            strip_tags("ESPN [Unknown]", &settings).as_deref(),
            Some("ESPN")
        );
        // A custom label from an edited format is honoured.
        let custom = StreamsSettings {
            dead_rename_format: "{name} [GONE]".to_string(),
            ..StreamsSettings::default()
        };
        assert_eq!(strip_tags("ESPN [GONE]", &custom).as_deref(), Some("ESPN"));
        assert!(has_status_tag("ESPN [Dead]", &settings));
        assert!(has_status_tag("ESPN [GONE]", &custom));
        assert!(
            !has_status_tag("ESPN [HD]", &settings),
            "a quality tag is not status"
        );
    }

    #[test]
    fn a_recorded_group_that_is_gone_keeps_the_group() {
        let history = vec![row(1, "channel_group_id", "999", "102")];
        let outcome = plan(
            &world(),
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &history,
        );
        assert!(outcome.moves.is_empty(), "{:?}", outcome.moves);
        assert_eq!(outcome.missing_groups, vec![999]);
    }

    #[test]
    fn a_move_out_of_no_group_is_planned_with_no_target() {
        // The channel triage moved OUT of "No Group": the recorded before is `none`, which a
        // REST write cannot express — planned here, refused per row by channel_ops::apply.
        let mut world = world();
        world.channels[0].channel_group_id = Some(102);
        let history = vec![row(1, "channel_group_id", "none", "102")];
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &history,
        );
        assert_eq!(outcome.moves.len(), 1);
        assert_eq!(outcome.moves[0].to, None);
    }

    #[test]
    fn a_failed_or_planned_row_is_not_a_change() {
        for status in ["failed", "planned", "skipped"] {
            let mut history = vec![row(1, "name", "ESPN", "ESPN [DEAD]")];
            history[0].status = status.to_string();
            let outcome = plan(
                &world(),
                &StreamsSettings::default(),
                &[alive(1, "ESPN")],
                &history,
            );
            assert!(
                outcome
                    .renames
                    .iter()
                    .all(|rename| rename.source == Source::Tags),
                "{status} must not count as a recorded change: {outcome:?}"
            );
        }
    }

    #[test]
    fn a_channel_that_is_gone_is_skipped() {
        let mut world = world();
        world.channels.retain(|channel| channel.id != 1);
        let outcome = plan(
            &world,
            &StreamsSettings::default(),
            &[alive(1, "ESPN")],
            &[row(1, "name", "ESPN", "ESPN [DEAD]")],
        );
        assert!(outcome
            .skipped
            .iter()
            .any(|skip| skip.channel_id == 1 && skip.reason.contains("no longer exists")));
    }

    // ---------------------------------------------------------------------------------
    // Scale
    // ---------------------------------------------------------------------------------

    #[test]
    fn restoring_twenty_thousand_channels_is_one_pass() {
        // The chain is read once, the tag matchers are compiled once, and the world's channels
        // are indexed once: a per-channel regex compile or channel scan would be 20 000 of
        // each. Generous bound: this runs in a DEBUG build.
        let count = 20_000i64;
        let channel = |id: i64| Channel {
            id,
            name: format!("Channel {id}"),
            channel_number: Some(serde_json::json!(id)),
            channel_group_id: Some(100),
            ..Channel::default()
        };
        let world = World {
            channels: (1..=count).map(channel).collect(),
            groups: vec![ChannelGroup {
                id: 100,
                name: "US: Cable".to_string(),
            }],
            ..World::default()
        };
        let mut verdicts = Vec::with_capacity(count as usize);
        let mut history = Vec::with_capacity(count as usize * 2);
        for id in 1..=count {
            // Every other channel is tagged and exiled; the rest were never touched.
            let tagged = id % 2 == 0;
            verdicts.push(RecordedVerdict {
                channel_id: id,
                channel_name: format!("Channel {id}"),
                channel_group: String::new(),
                verdict: ChannelVerdict::Alive,
                format: Some("HD".to_string()),
            });
            if tagged {
                history.push(row(
                    id,
                    "name",
                    &format!("Channel {id}"),
                    &format!("Channel {id} [DEAD]"),
                ));
                history.push(row(id, "channel_group_id", "100", "102"));
            }
        }
        let mut world = world;
        for channel in &mut world.channels {
            if channel.id % 2 == 0 {
                channel.name = format!("Channel {} [DEAD]", channel.id);
                channel.channel_group_id = Some(102);
            }
        }

        let started = std::time::Instant::now();
        let outcome = plan(&world, &StreamsSettings::default(), &verdicts, &history);
        let elapsed = started.elapsed();
        assert_eq!(outcome.recovered, 20_000);
        assert_eq!(outcome.renames.len(), 10_000);
        assert_eq!(outcome.moves.len(), 10_000);
        let bound = std::time::Duration::from_secs(10);
        assert!(
            elapsed < bound,
            "planning 20000 channels took {elapsed:?}, over the {bound:?} bound"
        );
        eprintln!("restore bench (guard): 20000 channels in {elapsed:?}");
    }

    // ---------------------------------------------------------------------------------
    // End to end
    // ---------------------------------------------------------------------------------

    fn settings_file(server: &ServerFixture, settings: &StreamsSettings) {
        let path = server.config.config_dir.join("streams.toml");
        std::fs::create_dir_all(server.config.config_dir.as_path()).expect("config dir");
        crate::settings::save(&path, settings).expect("save settings");
    }

    /// A succeeded probe run, and a succeeded triage run whose rows are the changes to undo.
    fn record(store: &Store, verdicts: &[RecordedVerdict], triage_rows: &[RunRow]) -> i64 {
        let probe = store
            .start_run(crate::APP_ID, crate::ACTION_PROBE, false)
            .expect("start probe");
        let rows: Vec<RunRow> = verdicts
            .iter()
            .enumerate()
            .map(|(ordinal, verdict)| RunRow {
                ordinal: ordinal as i64,
                subject: Subject::Channel,
                channel_id: verdict.channel_id,
                channel_name: verdict.channel_name.clone(),
                status: verdict.verdict.as_str().to_string(),
                field: crate::probe::VERDICT_FIELD.to_string(),
                after_value: Some(verdict.verdict.as_str().to_string()),
                ..RunRow::default()
            })
            .collect();
        store.append_rows(probe, &rows).expect("append");
        store
            .finish_run(
                probe,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish probe");

        let triage = store
            .start_run(crate::APP_ID, ACTION_TRIAGE_APPLY, false)
            .expect("start triage");
        let mut rows = Vec::new();
        for (ordinal, row) in triage_rows.iter().enumerate() {
            let mut row = row.clone();
            row.ordinal = ordinal as i64;
            rows.push(row);
        }
        store.append_rows(triage, &rows).expect("append triage");
        store
            .finish_run(
                triage,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish triage");
        triage
    }

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

    #[tokio::test]
    async fn a_restore_run_with_no_probe_refuses_before_any_request() {
        let server = ServerFixture::start(&world()).await;
        settings_file(&server, &StreamsSettings::default());
        let store = server.store();
        let context = server.context_for_run(ACTION_RESTORE_APPLY, &store, false);
        let error = StreamsApp::new()
            .dispatch(ACTION_RESTORE_APPLY, &context, false)
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
    async fn preview_writes_nothing_and_apply_puts_the_values_back() {
        let server = ServerFixture::start(&world()).await;
        settings_file(&server, &StreamsSettings::default());
        let store = server.store();
        record(
            &store,
            &[
                alive(1, "ESPN"),
                alive(2, "Sky"),
                alive(3, "AMC"),
                alive(4, "CNN"),
            ],
            &[
                row(1, "name", "ESPN", "ESPN [DEAD]"),
                row(1, "channel_group_id", "100", "102"),
                row(3, "name", "AMC", "AMC [Blank]"),
                row(3, "channel_group_id", "100", "102"),
            ],
        );
        let app = StreamsApp::new();

        let context = server.context_for_run(ACTION_RESTORE_PREVIEW, &store, true);
        let preview = app
            .dispatch(ACTION_RESTORE_PREVIEW, &context, true)
            .await
            .expect("preview");
        assert_eq!(preview.counts.get("recovered"), Some(&4));
        assert_eq!(preview.counts.get("renamed"), Some(&2));
        assert_eq!(preview.counts.get("moved"), Some(&2));
        assert_eq!(preview.counts.get("from_recorded"), Some(&2));
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0,
            "a preview writes nothing"
        );

        finish(&store, &context);
        let context = server.context_for_run(ACTION_RESTORE_APPLY, &store, false);
        let applied = app
            .dispatch(ACTION_RESTORE_APPLY, &context, false)
            .await
            .expect("apply");
        assert_eq!(
            applied.counts.get("written"),
            Some(&4),
            "{:?}",
            applied.counts
        );
        let bodies = server
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1);
        let rows = bodies[0].as_array().expect("an array");
        let by_id: HashMap<i64, &serde_json::Value> = rows
            .iter()
            .map(|row| (row["id"].as_i64().expect("id"), row))
            .collect();
        assert_eq!(by_id[&1]["name"], serde_json::json!("ESPN"));
        assert_eq!(by_id[&1]["channel_group_id"], serde_json::json!(100));
        assert_eq!(by_id[&3]["name"], serde_json::json!("AMC"));
        // The channels with nothing to restore are not in the payload at all.
        assert!(!by_id.contains_key(&2), "{by_id:?}");

        // The restore's own rows are recorded, so the next restore is a no-op.
        let run_id = context.run_id().expect("run id");
        let rows = store.rows(run_id).expect("rows");
        assert!(rows.iter().any(|row| row.channel_id == 1
            && row.field == "name"
            && row.before_value.as_deref() == Some("ESPN [DEAD]")
            && row.after_value.as_deref() == Some("ESPN")));
    }

    #[tokio::test]
    async fn a_recovered_channel_outside_the_scope_is_not_written() {
        // The probe ran before the scope narrowed: the restore still only writes what
        // `[scope] groups` allows, and reports the rest as a skip.
        let mut world = world();
        world.channels.push(Channel {
            id: 5,
            name: "Outside [DEAD]".to_string(),
            channel_number: Some(serde_json::json!(5)),
            channel_group_id: Some(101),
            ..Channel::default()
        });
        world.groups.push(ChannelGroup {
            id: 101,
            name: "Outside Group".to_string(),
        });
        let mut server = ServerFixture::start(&world).await;
        server.config.scope.groups = "US: Cable, Graveyard".to_string();
        settings_file(&server, &StreamsSettings::default());
        let store = server.store();
        record(
            &store,
            &[alive(1, "ESPN"), alive(5, "Outside")],
            &[
                row(1, "name", "ESPN", "ESPN [DEAD]"),
                row(5, "name", "Outside", "Outside [DEAD]"),
            ],
        );
        let context = server.context_for_run(ACTION_RESTORE_APPLY, &store, false);
        StreamsApp::new()
            .dispatch(ACTION_RESTORE_APPLY, &context, false)
            .await
            .expect("apply");
        let bodies = server
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1);
        let rows = bodies[0].as_array().expect("an array");
        let ids: Vec<i64> = rows
            .iter()
            .map(|row| row["id"].as_i64().expect("id"))
            .collect();
        assert_eq!(ids, vec![1], "only the in-scope channel is written");
        let run_id = context.run_id().expect("run id");
        let rows = store.rows(run_id).expect("rows");
        assert!(
            rows.iter()
                .any(|row| row.channel_id == 5 && row.status == "skipped"),
            "an out-of-scope channel is reported as a skip: {rows:?}"
        );
    }

    #[tokio::test]
    async fn a_move_back_to_no_group_is_planned_and_refused_per_row() {
        let mut world = world();
        world.channels[0].channel_group_id = Some(102);
        let server = ServerFixture::start(&world).await;
        settings_file(&server, &StreamsSettings::default());
        let store = server.store();
        record(
            &store,
            &[alive(1, "ESPN")],
            &[row(1, "channel_group_id", "none", "102")],
        );
        let context = server.context_for_run(ACTION_RESTORE_APPLY, &store, false);
        let outcome = StreamsApp::new()
            .dispatch(ACTION_RESTORE_APPLY, &context, false)
            .await
            .expect("apply");
        assert_eq!(
            outcome.counts.get("failed"),
            Some(&1),
            "{:?}",
            outcome.counts
        );
        let run_id = context.run_id().expect("run id");
        let rows = store.rows(run_id).expect("rows");
        let failed: Vec<&RunRow> = rows.iter().filter(|row| row.status == "failed").collect();
        assert_eq!(failed.len(), 1, "{rows:?}");
        assert!(
            failed[0].reason.contains("cannot clear a channel group"),
            "{}",
            failed[0].reason
        );
    }
}
