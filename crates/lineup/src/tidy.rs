//! `tidy_*`: Lineuparr's post-match cleanup, as hide instead of delete.
//!
//! Port of the cleanup at the end of Lineuparr's stream match
//! (`reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/plugin.py`, ~3659-3680:
//! channels in lineup groups with zero streams are DELETED). Two documented
//! fixes:
//!
//! - **Hide instead of delete** (plan decision): `hidden_from_output = true`
//!   (edit/bulk, like every other visibility write) rather than a row delete.
//!   Deletes are out of scope for every create stage, and a hidden channel
//!   keeps its streams, guide, profiles and number, so re-matching restores it
//!   exactly. Recorded and undoable through the host undo.
//! - **Owned channels only**: the plugin deleted every stream-less channel in
//!   groups named like the lineup's, whether it made them or not (no ownership
//!   marker, and the single-channel filter ignored). Tidy only ever touches
//!   channels this sync owns for the configured lineup file
//!   (`lineup:<file>:`) — a foreign stream-less channel in a lineup group is
//!   reported and skipped.
//!
//! Unhiding is the mirror with a guard: owned hidden channels that now have
//! streams are unhidden ONLY when a past `tidy_apply` hid them (read from the
//! run rows via
//! [`field_history`](crosstalk_core::store::Store::field_history) — never
//! unhide a channel the operator hid by hand).
//!
//! # Scope
//!
//! Tidy always acts on the owned lineup channels only, never on the host scope
//! or a step override: the host `[scope]` bounds what matching reads, but tidy
//! is the garbage collector for what sync owns, so narrowing it would strand
//! owned empties outside the scope. (Whether the step says `scope = "lineup"`
//! or nothing, the set is the same: the channels owned under the configured
//! file's prefix.)
//!
//! `tidy_preview` (Read) / `tidy_apply` (Destructive, confirm) plan from LIVE
//! state through [`Op::Set`] `HiddenFromOutput` per channel: hide owned
//! stream-less channels, unhide owned hidden channels that regained streams
//! after tidy hid them. No-op dropping keeps already-correct channels out of
//! the plan; the CSV lists every owned channel either way.

use anyhow::Result;
use crosstalk_core::channel_ops::{self, Field, Op, PlanOptions, Selection, SetValue};
use crosstalk_core::config::ScopeConfig;
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::{World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use std::collections::{HashMap, HashSet};

/// The tidy CSV columns: the owned channel, its streams, and what the run did.
pub const COLUMNS_TIDY: [&str; 8] = [
    "id",
    "channel_name",
    "current_group",
    "channel_number",
    "stream_count",
    "hidden",
    "status",
    "reason",
];

/// What one owned channel becomes.
#[derive(Debug, Clone, PartialEq)]
pub enum TidyAction {
    /// Owned, no streams, visible: hide it.
    Hide,
    /// Owned, hidden, streams again, and tidy hid it: unhide it.
    Unhide,
    /// No write: the reason says why.
    Keep,
}

/// One owned channel's tidy decision.
#[derive(Debug, Clone)]
pub struct TidyRow {
    pub id: i64,
    pub name: String,
    pub group: String,
    pub number: Option<f64>,
    pub streams: usize,
    pub hidden: bool,
    pub action: TidyAction,
    pub reason: String,
}

/// Decide every owned live channel: hide the stream-less, unhide the
/// regained-stream ones tidy hid.
///
/// Pure: nothing is written. `owned_ids` are the live channel ids owned under
/// the configured file's prefix; `hid_by_tidy` are the ids a past
/// `tidy_apply` hid (still hidden or not — the live state decides the action).
pub fn decide_tidy(
    world: &World,
    owned_ids: &HashSet<i64>,
    hid_by_tidy: &HashSet<i64>,
) -> Vec<TidyRow> {
    let mut rows: Vec<TidyRow> = Vec::new();
    let mut ids: Vec<i64> = owned_ids.iter().copied().collect();
    ids.sort_unstable();
    // One lookup per owned id instead of a walk over the world per id (E-S5);
    // reversed so a duplicated id keeps the first entry, as `find` did.
    let channels: HashMap<i64, &crosstalk_core::dispatcharr::Channel> = world
        .channels
        .iter()
        .rev()
        .map(|channel| (channel.id, channel))
        .collect();
    let groups: HashMap<i64, &str> = world
        .groups
        .iter()
        .rev()
        .map(|group| (group.id, group.name.as_str()))
        .collect();
    for id in ids {
        let Some(channel) = channels.get(&id).copied() else {
            continue;
        };
        let base = TidyRow {
            id,
            name: channel.name.clone(),
            group: channel
                .channel_group_id
                .and_then(|group| groups.get(&group))
                .map_or_else(|| "No Group".to_string(), |name| (*name).to_string()),
            number: channel.display_number(),
            streams: channel.streams.len(),
            hidden: channel.hidden_from_output,
            action: TidyAction::Keep,
            reason: String::new(),
        };
        if channel.streams.is_empty() && !channel.hidden_from_output {
            rows.push(TidyRow {
                action: TidyAction::Hide,
                reason: "owned by this lineup and has no streams: hides it (delete in Lineuparr) \
                     — re-matching restores it exactly"
                    .to_string(),
                ..base
            });
        } else if channel.streams.is_empty() {
            rows.push(TidyRow {
                reason: "already hidden".to_string(),
                ..base
            });
        } else if channel.hidden_from_output && hid_by_tidy.contains(&id) {
            rows.push(TidyRow {
                action: TidyAction::Unhide,
                reason: format!(
                    "has {} stream(s) again after tidy hid it: unhides it",
                    channel.streams.len(),
                ),
                ..base
            });
        } else if channel.hidden_from_output {
            rows.push(TidyRow {
                reason: "hidden and has streams, but tidy never hid it: the operator hid \
                         it by hand, so it stays hidden"
                    .to_string(),
                ..base
            });
        } else {
            rows.push(TidyRow {
                reason: format!("has {} stream(s)", channel.streams.len()),
                ..base
            });
        }
    }
    rows
}

/// The ids a past `tidy_apply` hid: `hidden_from_output` rows it wrote `true`
/// to, from finished non-dry runs (undo's hand-edit rule in reverse: only what
/// tidy wrote may tidy unwrite).
///
/// The LATEST tidy write per channel decides: a channel tidy hid, then
/// unhid, then the operator hid again was last written `false` by tidy, so
/// the current hide is the operator's and stays.
///
/// Host undo runs of this app count too, in order with the tidy runs: an undo
/// that unhid a channel took it out of tidy's hands (whatever it undid), and an
/// undo that re-hid one restores tidy's hide only when the run it undid was a
/// tidy (or an undo of one) — an undo of `create` hides channels tidy never did.
pub fn hid_by_tidy_runs(store: &crosstalk_core::store::Store) -> Result<HashSet<i64>> {
    use crosstalk_core::undo::ACTION_UNDO;

    let runs = store.recent_runs_for(crate::APP_ID, TIDY_HISTORY_RUNS)?;
    let action_of: HashMap<i64, (&str, Option<i64>)> = runs
        .iter()
        .map(|run| (run.id, (run.action.as_str(), run.parent_run_id)))
        .collect();
    // Whether `run_id` is a tidy apply, or an undo (of an undo ...) of one.
    let tidy_chain = |mut run_id: i64| -> bool {
        for _ in 0..TIDY_HISTORY_RUNS {
            match action_of.get(&run_id) {
                Some((action, _)) if *action == crate::ACTION_TIDY_APPLY => return true,
                Some((action, Some(parent))) if *action == ACTION_UNDO => run_id = *parent,
                _ => return false,
            }
        }
        false
    };
    // Oldest run first, rows in order: a later write replaces an earlier one.
    let mut writers: Vec<&crosstalk_core::store::RunRecord> = runs
        .iter()
        .filter(|run| !run.dry_run && !run.status.is_running())
        .filter(|run| run.action == crate::ACTION_TIDY_APPLY || run.action == ACTION_UNDO)
        .collect();
    writers.sort_by(|a, b| (&a.started_at, a.id).cmp(&(&b.started_at, b.id)));

    let mut hid: HashSet<i64> = HashSet::new();
    for run in writers {
        let restores_tidy = run.action == crate::ACTION_TIDY_APPLY || tidy_chain(run.id);
        // Only the visibility rows, filtered in SQL (E-S5/E-S8: the run_rows_field index).
        for row in store.rows_of_fields(run.id, &["hidden_from_output"])? {
            if row.status != "ok"
                || row.subject != crosstalk_core::store::Subject::Channel
                || row.field != "hidden_from_output"
            {
                continue;
            }
            if row.after_value.as_deref() == Some("true") {
                if restores_tidy {
                    hid.insert(row.channel_id);
                }
            } else {
                hid.remove(&row.channel_id);
            }
        }
    }
    Ok(hid)
}

/// How many of the app's newest runs [`hid_by_tidy_runs`] reads.
const TIDY_HISTORY_RUNS: usize = 200;

/// Run `tidy_preview` / `tidy_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply
/// hides the owned stream-less channels and unhides the ones that regained
/// streams after tidy hid them, recorded as run rows for the host undo.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let action_id = if dry_run {
        crate::ACTION_TIDY_PREVIEW
    } else {
        crate::ACTION_TIDY_APPLY
    };
    let client = context.dispatcharr()?;
    let world: World = client
        .load_world_with(WorldKinds::only(WorldKind::Channels))
        .await?;

    let prefix = crate::owner_key_prefix(&settings.lineup_file)?;
    let mut owned_ids: HashSet<i64> = HashSet::new();
    if let Some(store) = context.store() {
        for (kind, id, key) in store.owned_keys(crate::APP_ID)? {
            if kind == "channel" && key.starts_with(&prefix) {
                owned_ids.insert(id);
            }
        }
    }
    // Deleted upstream is gone, not an error: only live ids decide. A set, not a walk
    // over the channels per owned id (E-S5).
    let live: HashSet<i64> = world.channels.iter().map(|channel| channel.id).collect();
    owned_ids.retain(|id| live.contains(id));
    let hid_by_tidy: HashSet<i64> = match context.store() {
        Some(store) => hid_by_tidy_runs(store)?,
        None => HashSet::new(),
    };
    let rows = decide_tidy(&world, &owned_ids, &hid_by_tidy);

    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for row in &rows {
        match row.action {
            TidyAction::Hide => {
                values.insert(row.id, SetValue::Flag(true));
            }
            TidyAction::Unhide => {
                values.insert(row.id, SetValue::Flag(false));
            }
            TidyAction::Keep => {}
        }
    }
    // The id bound is the rail, like sync's: the host scope never narrows tidy,
    // because a skipped owned channel would strand it hidden (or stream-less)
    // outside the scope.
    let mut selection = Selection::in_scope(&ScopeConfig::default());
    selection.channel_ids = Some(values.keys().copied().collect());
    let plan = if values.is_empty() {
        channel_ops::Plan::default()
    } else {
        channel_ops::plan_with_options(
            &world,
            &selection,
            &[Op::Set {
                field: Field::HiddenFromOutput,
                values,
            }],
            &PlanOptions::default(),
        )?
    };
    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply_recorded(client, &plan, context, action_id).await?)
    };

    // After an apply the statuses live in the report (the plan's copies stay `planned`).
    let changed: HashMap<i64, &channel_ops::Change> = applied
        .as_ref()
        .map_or(&plan.changes, |report| &report.changes)
        .iter()
        .map(|change| (change.channel_id, change))
        .collect();
    let skipped: HashMap<i64, &str> = plan
        .skipped
        .iter()
        .map(|skip| (skip.channel_id, skip.reason.as_str()))
        .collect();
    let mut outputs: Vec<TidyOutput> = Vec::with_capacity(rows.len());
    for row in &rows {
        let (status, reason) = match row.action {
            TidyAction::Keep => ("skipped".to_string(), row.reason.clone()),
            TidyAction::Hide | TidyAction::Unhide => {
                if dry_run {
                    ("planned".to_string(), row.reason.clone())
                } else {
                    match (changed.get(&row.id), skipped.get(&row.id)) {
                        (Some(change), _) => (
                            match change.status {
                                channel_ops::RowStatus::Ok => {
                                    if row.action == TidyAction::Hide {
                                        "hide"
                                    } else {
                                        "unhide"
                                    }
                                }
                                .to_string(),
                                channel_ops::RowStatus::Failed => "failed".to_string(),
                                channel_ops::RowStatus::Planned => "planned".to_string(),
                            },
                            if change.reason.is_empty() {
                                row.reason.clone()
                            } else {
                                change.reason.clone()
                            },
                        ),
                        (None, Some(reason)) => ("skipped".to_string(), (*reason).to_string()),
                        (None, None) => ("planned".to_string(), row.reason.clone()),
                    }
                }
            }
        };
        outputs.push(TidyOutput {
            id: row.id.to_string(),
            name: row.name.clone(),
            group: row.group.clone(),
            number: row
                .number
                .map(|number| format!("{number}"))
                .unwrap_or_default(),
            streams: row.streams.to_string(),
            hidden: if row.hidden {
                "Yes".to_string()
            } else {
                String::new()
            },
            status,
            reason,
        });
    }

    let hidden = outputs
        .iter()
        .filter(|output| output.status == "hide")
        .count();
    let unhidden = outputs
        .iter()
        .filter(|output| output.status == "unhide")
        .count();
    let planned = outputs
        .iter()
        .filter(|output| output.status == "planned")
        .count();
    let failed = outputs
        .iter()
        .filter(|output| output.status == "failed")
        .count();
    let kept = outputs.len() - hidden - unhidden - planned - failed;

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run {
        "tidy_preview"
    } else {
        "tidy_applied"
    };
    let title = if dry_run {
        "Tidy Preview Report"
    } else {
        "Tidy Applied Report"
    };
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(rows.len())
        .result(
            "Scope",
            format!(
                "owned channels of {} (host scope ignored: tidy is the garbage collector \
                 for what sync owns)",
                settings.lineup_file,
            ),
        )
        .result(
            "Channels written",
            if dry_run {
                "none, this was a preview".to_string()
            } else {
                format!("{hidden} hidden, {unhidden} unhidden")
            },
        )
        .write(
            context.config(),
            &schema,
            &values,
            &COLUMNS_TIDY,
            |writer| {
                for output in &outputs {
                    writer.write_record([
                        output.id.clone(),
                        csv_report::safe_cell(&output.name),
                        csv_report::safe_cell(&output.group),
                        output.number.clone(),
                        output.streams.clone(),
                        output.hidden.clone(),
                        output.status.clone(),
                        csv_report::safe_cell(&output.reason),
                    ])?;
                }
                Ok(())
            },
        )?;

    context.record_rows(&channel_ops::run_rows(&plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut summary = if dry_run {
        format!("Tidy preview: {planned} owned channel(s) to hide/unhide, {kept} already correct",)
    } else {
        let (written, apply_failed) = applied
            .as_ref()
            .map(|report| (report.ok, report.failed))
            .unwrap_or((0, 0));
        format!(
            "Tidy apply: hid {hidden}, unhidden {unhidden} ({written} written, \
             {apply_failed} failed), {kept} already correct",
        )
    };
    summary.push_str(&format!(" — lineup {}", settings.lineup_file));
    let mut built = OutcomeBuilder::new(summary)
        .count("channels", rows.len() as i64)
        .count("hidden", hidden as i64)
        .count("unhidden", unhidden as i64)
        .count("planned", planned as i64)
        .count("skipped", kept as i64)
        .count(
            "written",
            applied.as_ref().map(|report| report.ok as i64).unwrap_or(0),
        )
        .count(
            "failed",
            applied
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(failed as i64),
        )
        .report(Some(report));
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

struct TidyOutput {
    id: String,
    name: String,
    group: String,
    number: String,
    streams: String,
    hidden: String,
    status: String,
    reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup};
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::store::{RunRow, RunStatus, Store, Subject};
    use crosstalk_core::subapp::Registry;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn owned(ids: &[i64]) -> HashSet<i64> {
        ids.iter().copied().collect()
    }

    fn channel(
        id: i64,
        name: &str,
        streams: Vec<i64>,
        hidden: bool,
        group: Option<i64>,
    ) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_group_id: group,
            hidden_from_output: hidden,
            streams,
            ..Channel::default()
        }
    }

    fn tidy_world() -> World {
        World {
            channels: vec![
                channel(1, "Empty", Vec::new(), false, Some(10)),
                channel(2, "Already Hidden", Vec::new(), true, Some(10)),
                channel(3, "Regained", vec![101], true, Some(10)),
                channel(4, "Operator Hid", vec![102], true, Some(10)),
                channel(5, "Healthy", vec![103], false, Some(10)),
                channel(6, "Foreign Empty", Vec::new(), false, Some(10)),
            ],
            groups: vec![ChannelGroup {
                id: 10,
                name: "DIRECTV: News".to_string(),
            }],
            ..World::default()
        }
    }

    #[test]
    fn hide_unhide_and_never_unhide_the_operator() {
        let world = tidy_world();
        let rows = decide_tidy(&world, &owned(&[1, 2, 3, 4, 5]), &[3].into_iter().collect());
        let action = |id: i64| {
            rows.iter()
                .find(|row| row.id == id)
                .expect("row")
                .action
                .clone()
        };
        assert_eq!(action(1), TidyAction::Hide);
        assert_eq!(action(2), TidyAction::Keep, "already hidden, no streams");
        assert_eq!(
            action(3),
            TidyAction::Unhide,
            "streams again after tidy hid it"
        );
        assert_eq!(
            action(4),
            TidyAction::Keep,
            "streams but tidy never hid it: the operator did"
        );
        assert_eq!(
            action(5),
            TidyAction::Keep,
            "healthy channels are listed, not written"
        );
        // Channel 6 is foreign (not owned): no row at all, let alone a write.
        assert!(rows.iter().all(|row| row.id != 6), "{rows:?}");
        // A channel deleted upstream is gone, not an error.
        let rows = decide_tidy(&world, &owned(&[1, 999]), &HashSet::new());
        assert!(rows.iter().all(|row| row.id != 999));
    }

    #[test]
    fn hid_by_tidy_reads_only_finished_tidy_applies() {
        let store = Store::in_memory().expect("store");
        // A finished tidy_apply that hid 3 (and a skipped row for 4).
        let run = store
            .start_run(crate::APP_ID, crate::ACTION_TIDY_APPLY, false)
            .expect("run");
        store
            .append_rows(
                run,
                &[
                    RunRow {
                        subject: Subject::Channel,
                        channel_id: 3,
                        channel_name: "Regained".to_string(),
                        field: "hidden_from_output".to_string(),
                        before_value: Some("false".to_string()),
                        after_value: Some("true".to_string()),
                        status: "ok".to_string(),
                        ..RunRow::default()
                    },
                    RunRow {
                        subject: Subject::Channel,
                        ordinal: 1,
                        channel_id: 4,
                        channel_name: "Operator Hid".to_string(),
                        field: "hidden_from_output".to_string(),
                        before_value: Some("false".to_string()),
                        after_value: Some("true".to_string()),
                        status: "skipped".to_string(),
                        ..RunRow::default()
                    },
                    RunRow {
                        subject: Subject::Channel,
                        // Rows key by (run, ordinal): each needs its own.
                        ordinal: 2,
                        channel_id: 8,
                        channel_name: "Later Unhid".to_string(),
                        field: "hidden_from_output".to_string(),
                        before_value: Some("false".to_string()),
                        after_value: Some("true".to_string()),
                        status: "ok".to_string(),
                        ..RunRow::default()
                    },
                ],
            )
            .expect("rows");
        store
            .finish_run(
                run,
                RunStatus::Succeeded,
                Some("hid"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // A later tidy unhid 8: if 8 is hidden now, the operator did it.
        let later = store
            .start_run(crate::APP_ID, crate::ACTION_TIDY_APPLY, false)
            .expect("run");
        store
            .append_rows(
                later,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 8,
                    channel_name: "Later Unhid".to_string(),
                    field: "hidden_from_output".to_string(),
                    before_value: Some("true".to_string()),
                    after_value: Some("false".to_string()),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        store
            .finish_run(
                later,
                RunStatus::Succeeded,
                Some("unhid"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // A preview's rows are plans, not writes; a running run is not history.
        let preview = store
            .start_run(crate::APP_ID, crate::ACTION_TIDY_PREVIEW, true)
            .expect("preview");
        store
            .append_rows(
                preview,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 4,
                    channel_name: "Operator Hid".to_string(),
                    field: "hidden_from_output".to_string(),
                    before_value: Some("false".to_string()),
                    after_value: Some("true".to_string()),
                    status: "planned".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        // One running run per app: the preview finishes before the next starts.
        store
            .finish_run(
                preview,
                RunStatus::Succeeded,
                Some("previewed"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let running = store
            .start_run(crate::APP_ID, crate::ACTION_TIDY_APPLY, false)
            .expect("running");
        store
            .append_rows(
                running,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 5,
                    channel_name: "Healthy".to_string(),
                    field: "hidden_from_output".to_string(),
                    before_value: Some("false".to_string()),
                    after_value: Some("true".to_string()),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        let hid = hid_by_tidy_runs(&store).expect("reads");
        assert_eq!(hid, [3].into_iter().collect(), "{hid:?}");
    }

    #[test]
    fn an_undo_of_tidy_takes_the_channel_out_of_tidys_hands() {
        // N7: tidy hid 3; an undo of that run unhid it, so 3 is no longer tidy's hide. An
        // undo of a create run hides 7, which tidy never hid.
        let store = Store::in_memory().expect("store");
        let hide = |channel_id: i64, after: &str| RunRow {
            subject: Subject::Channel,
            channel_id,
            channel_name: format!("Channel {channel_id}"),
            field: "hidden_from_output".to_string(),
            before_value: Some(if after == "true" { "false" } else { "true" }.to_string()),
            after_value: Some(after.to_string()),
            status: "ok".to_string(),
            ..RunRow::default()
        };
        let finish = |run: i64| {
            store
                .finish_run(
                    run,
                    RunStatus::Succeeded,
                    None,
                    &serde_json::json!({}),
                    None,
                )
                .expect("finish");
        };
        let tidy = store
            .start_run(crate::APP_ID, crate::ACTION_TIDY_APPLY, false)
            .expect("tidy");
        store.append_rows(tidy, &[hide(3, "true")]).expect("rows");
        finish(tidy);
        let create = store
            .start_run(crate::APP_ID, crate::ACTION_CREATE_APPLY, false)
            .expect("create");
        finish(create);

        let undo_tidy = store
            .start_child_run(
                crate::APP_ID,
                crosstalk_core::undo::ACTION_UNDO,
                false,
                tidy,
            )
            .expect("undo tidy");
        store
            .append_rows(undo_tidy, &[hide(3, "false")])
            .expect("rows");
        finish(undo_tidy);
        let undo_create = store
            .start_child_run(
                crate::APP_ID,
                crosstalk_core::undo::ACTION_UNDO,
                false,
                create,
            )
            .expect("undo create");
        store
            .append_rows(undo_create, &[hide(7, "true")])
            .expect("rows");
        finish(undo_create);

        let hid = hid_by_tidy_runs(&store).expect("reads");
        assert!(hid.is_empty(), "{hid:?}");
    }

    async fn serve(world: &World) -> (MockServer, CrosstalkConfig, tempfile::TempDir) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::to_value(&world.channels).expect("channels")),
            )
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        (server, config, dir)
    }

    fn context_for(
        config: &CrosstalkConfig,
        store: &Store,
        action: &str,
        dry_run: bool,
    ) -> (JobContext, i64) {
        let run_id = store
            .start_run(crate::APP_ID, action, dry_run)
            .expect("start run");
        let context = JobContext::new(Arc::new(config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(
                crosstalk_core::dispatcharr::DispatcharrClient::new(&config.dispatcharr)
                    .expect("client"),
            )
            .for_run(
                crate::APP_ID,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        (context, run_id)
    }

    fn seed_ownership(store: &Store, file: &str, ids: &[i64]) {
        let run = store
            .start_run(crate::APP_ID, crate::ACTION_SYNC_APPLY, false)
            .expect("run");
        for id in ids {
            store
                .record_ownership(
                    "channel",
                    *id,
                    crate::APP_ID,
                    &crate::sync::channel_owner_key(file, &format!("channel-{id}")),
                    run,
                )
                .expect("own");
        }
        store
            .finish_run(
                run,
                RunStatus::Succeeded,
                Some("seeded"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
    }

    #[tokio::test]
    async fn preview_writes_nothing_but_the_csv() {
        let world = tidy_world();
        // No PATCH route mounted: a preview that writes anything fails loudly.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::to_value(&world.channels).expect("channels")),
            )
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        let store = Store::in_memory().expect("store");
        // Owned under the CONFIGURED file (the default settings' lineup).
        seed_ownership(
            &store,
            &crate::settings::LineupSettings::default().lineup_file,
            &[1, 2, 3, 4, 5],
        );
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, crate::ACTION_TIDY_PREVIEW, true);
        let outcome = run(
            &app,
            &context,
            &crate::settings::LineupSettings::default(),
            true,
        )
        .await
        .expect("previews");
        assert!(
            outcome.summary.contains("Tidy preview"),
            "{}",
            outcome.summary
        );
        let writes: Vec<_> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() != "GET")
            .collect();
        assert!(writes.is_empty(), "a preview writes nothing");
        let path = outcome.report_path.expect("a report");
        let text = std::fs::read_to_string(&path).expect("readable");
        let mut lines = text.lines().filter(|line| !line.starts_with('#'));
        let header = lines.next().expect("header");
        assert_eq!(
            header,
            "id,channel_name,current_group,channel_number,stream_count,hidden,status,reason"
        );
        // Every owned live channel is a row; the foreign one is not.
        assert_eq!(lines.count(), 5);
    }

    #[tokio::test]
    async fn apply_hides_and_unhides_owned_only() {
        let world = tidy_world();
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        // Owned under the CONFIGURED file (the default settings' lineup).
        seed_ownership(
            &store,
            &crate::settings::LineupSettings::default().lineup_file,
            &[1, 2, 3, 4, 5],
        );
        // A past tidy hid channel 3 (which regained streams since).
        let hid_run = store
            .start_run(crate::APP_ID, crate::ACTION_TIDY_APPLY, false)
            .expect("run");
        store
            .append_rows(
                hid_run,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 3,
                    channel_name: "Regained".to_string(),
                    field: "hidden_from_output".to_string(),
                    before_value: Some("false".to_string()),
                    after_value: Some("true".to_string()),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        store
            .finish_run(
                hid_run,
                RunStatus::Succeeded,
                Some("hid"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, crate::ACTION_TIDY_APPLY, false);
        let outcome = run(
            &app,
            &context,
            &crate::settings::LineupSettings::default(),
            false,
        )
        .await
        .expect("applies");
        assert!(
            outcome.summary.contains("Tidy apply"),
            "{}",
            outcome.summary
        );
        assert!(outcome.summary.contains("hid 1"), "{}", outcome.summary);
        // Undo reverses both writes; a channel the operator hid stays hidden
        // (it was never written, so there is nothing to reverse).
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let mut undone_world = world.clone();
        for channel in undone_world.channels.iter_mut() {
            match channel.id {
                1 => channel.hidden_from_output = true,
                3 => channel.hidden_from_output = false,
                _ => {}
            }
        }
        let registry = Registry::new(vec![Arc::new(crate::LineupApp::new())]);
        let undo = crosstalk_core::undo::plan_undo(&store, &undone_world, run_id, &registry)
            .expect("undo plans");
        let reversed: Vec<i64> = undo
            .changes
            .iter()
            .map(|change| change.channel_id)
            .collect();
        assert!(reversed.contains(&1), "the hide reverses: {undo:?}");
        assert!(reversed.contains(&3), "the unhide reverses: {undo:?}");
        assert!(
            !reversed.contains(&4),
            "the operator-hidden channel was never written: {undo:?}"
        );
    }
}
