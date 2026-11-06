//! Host undo: reverse a finished apply run's rows that are still in effect.
//!
//! Name Cleaner's `undo_last` restored the last apply where the name was unchanged; here that
//! rule is the host's, for every app. An undo reads the target run's `ok` rows, keeps the
//! ones still in effect — the channel's CURRENT value equals the row's recorded `after`
//! value (the same chain rule [`crate::streams::restore`] follows: a value somebody changed
//! since is left alone) — and writes the row's `before` value back through
//! [`crate::channel_ops`] as [`Op::Set`] per field.
//!
//! Two shapes are not edits. A `created` channel row is reversed by hiding the channel
//! (`hidden_from_output`), because deletes are out of scope and a hidden channel keeps its
//! ownership; created groups and logos are left in place and listed. A move back to "No
//! Group" is planned and refused per row, because Dispatcharr's REST serializer rejects a
//! null `channel_group_id` — the same API gap [`crate::channel_ops::apply`] reports for a
//! forward move.
//!
//! Memberships compare through the world's profile listing, which names only the ENABLED
//! channels, so a channel with no membership row reads as disabled. Dispatcharr's
//! `bulk-update` creates the rows it is missing, so a run that enabled a rowless channel
//! made its row; undoing that writes `enabled = false` and leaves a disabled row where
//! there was none. That reads the same as before (disabled) to every listing and to this
//! module, and undo never deletes, so it is accepted rather than worked around.
//!
//! An undo is itself a recorded, undoable run: the app is the target run's app, the action
//! is [`ACTION_UNDO`], and `parent_run_id` is the undone run. Previewing writes the CSV and
//! no rows; applying goes through the app's run guard, is cancellable, and records landed
//! rows when cancelled. Undoing an undo restores the applied state, which is the redo.

use crate::channel_ops::{
    self, parse_logo_wire, parse_number_wire, parse_stream_ids, Op, ReportSpec, Selection, SetValue,
};
use crate::dispatcharr::{World, WorldKind, WorldKinds};
use crate::jobs::JobContext;
use crate::store::{RunRecord, RunRow, RunStatus, Store, Subject};
use crate::subapp::{ActionKind, JobOutcome, OutcomeBuilder, Registry};
use anyhow::{Context, Result};
use std::collections::HashMap;

/// The action every undo run is recorded under.
///
/// A reserved host action, not one any sub-app declares: the runner records it directly, so
/// every app gets undo without declaring it. It never appears in schedules, workflows or
/// hook targets, which resolve through the registry and would call it unknown.
pub const ACTION_UNDO: &str = "undo";

/// How a clear is written in a run row's wire form.
const NONE_WIRE: &str = "none";

/// One field value an undo would write back.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedUndo {
    pub channel_id: i64,
    pub channel_name: String,
    pub field: crate::channel_ops::Field,
    pub reverse: SetValue,
}

/// A created group or logo the undo leaves in place, listed rather than reversed.
#[derive(Debug, Clone, PartialEq)]
pub struct LeftObject {
    /// `group` or `logo`, the [`crate::channel_ops::CreatedKind`] label.
    pub kind: String,
    pub id: i64,
    pub name: String,
}

/// One row the undo looked at and left alone, with the reason.
#[derive(Debug, Clone, PartialEq)]
pub struct UndoSkip {
    pub channel_id: i64,
    pub channel_name: String,
    pub field: Option<crate::channel_ops::Field>,
    pub reason: String,
}

/// What one undo run would write. Nothing has been written.
#[derive(Debug, Clone, Default)]
pub struct UndoPlan {
    /// The run being undone.
    pub target_run_id: i64,
    pub app: String,
    pub action: String,
    /// One entry per (channel, field) still in effect, in row order.
    pub changes: Vec<PlannedUndo>,
    /// Created groups and logos, left in place and listed.
    pub left: Vec<LeftObject>,
    /// Every row not still in effect, with the reason.
    pub skipped: Vec<UndoSkip>,
}

impl UndoPlan {
    /// Whether the undo would write nothing at all.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// The wire value the channel holds now for `field`, in the shape a run row records.
///
/// `None` is "the world does not say" (a membership whose profile is gone), which the
/// caller skips rather than comparing.
fn current_wire(
    channel: &crate::dispatcharr::Channel,
    field: crate::channel_ops::Field,
    world: &World,
) -> Option<String> {
    match field {
        crate::channel_ops::Field::Name => Some(channel.name.clone()),
        crate::channel_ops::Field::Group => Some(match channel.channel_group_id {
            Some(id) => id.to_string(),
            None => NONE_WIRE.to_string(),
        }),
        crate::channel_ops::Field::Epg => Some(match channel.current_epg_id() {
            Some(id) => id.to_string(),
            None => NONE_WIRE.to_string(),
        }),
        crate::channel_ops::Field::HiddenFromOutput => Some(channel.hidden_from_output.to_string()),
        crate::channel_ops::Field::Membership(profile_id) => world
            .profiles
            .iter()
            .find(|profile| profile.id == profile_id)
            .map(|profile| profile.channels.contains(&channel.id).to_string()),
        crate::channel_ops::Field::Streams => Some(if channel.streams.is_empty() {
            NONE_WIRE.to_string()
        } else {
            channel
                .streams
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        }),
        crate::channel_ops::Field::Number => Some(match channel.display_number() {
            Some(number) => format!("{number}"),
            None => NONE_WIRE.to_string(),
        }),
        crate::channel_ops::Field::Logo => Some(match channel.logo_id {
            Some(id) => id.to_string(),
            None => NONE_WIRE.to_string(),
        }),
    }
}

/// Parse a row's `before` wire value back into the value one channel gets.
fn reverse_of(field: crate::channel_ops::Field, before: &str) -> Option<SetValue> {
    let wire = before.trim();
    match field {
        crate::channel_ops::Field::Name => Some(SetValue::Name(before.to_string())),
        crate::channel_ops::Field::Group => {
            if wire.is_empty() || wire.eq_ignore_ascii_case(NONE_WIRE) {
                Some(SetValue::Group(None))
            } else {
                wire.parse::<i64>().ok().map(|id| SetValue::Group(Some(id)))
            }
        }
        crate::channel_ops::Field::Epg => {
            if wire.is_empty() || wire.eq_ignore_ascii_case(NONE_WIRE) {
                Some(SetValue::Epg(None))
            } else {
                wire.parse::<i64>().ok().map(|id| SetValue::Epg(Some(id)))
            }
        }
        crate::channel_ops::Field::HiddenFromOutput | crate::channel_ops::Field::Membership(_) => {
            wire.parse::<bool>().ok().map(SetValue::Flag)
        }
        crate::channel_ops::Field::Streams => parse_stream_ids(before).map(SetValue::Streams),
        crate::channel_ops::Field::Number => parse_number_wire(before).map(SetValue::Number),
        crate::channel_ops::Field::Logo => parse_logo_wire(before).map(SetValue::Logo),
    }
}

/// Load the target run and check it may be undone at all, or say why not.
///
/// Finished, non-dry-run applies only: a preview's rows are plans, not writes; a running or
/// paused run may still be writing; a read or file-local action wrote no channel rows; a
/// run whose app no longer exists names fields nothing can interpret; and a run whose rows
/// hold no channel write (a probe's stream rows, a watchdog's EPG-source rows, a match
/// that only measured) recorded no earlier value to put back. A workflow run is refused
/// with its step runs listed: the writes are the steps' own runs, each undone on its own,
/// newest first. An undo run is itself undoable (the redo); failed and cancelled runs are
/// undoable too, row by row, for the rows that landed.
///
/// The runner calls this BEFORE it reserves the app's slot, so a refused undo records no
/// run; [`plan_undo`] calls it again against the store it plans from.
pub fn check_target(store: &Store, registry: &Registry, run_id: i64) -> Result<RunRecord> {
    let run = store.run(run_id)?.with_context(|| {
        format!(
            "there is no run #{run_id}: it never existed, or run-history retention \
             (`jobs.run_history_days`) has pruned it and its rows, so there is nothing left \
             to undo it from"
        )
    })?;
    if run.app == crate::workflow::APP_ID {
        let mut steps: Vec<String> = store
            .child_runs(run.id)?
            .into_iter()
            .filter(|step| step.action != ACTION_UNDO)
            .map(|step| format!("#{} {}.{}", step.id, step.app, step.action))
            .collect();
        steps.reverse();
        anyhow::bail!(
            "run #{} is a workflow run: its writes are its steps' own runs, so undo those \
             one at a time, newest first{}",
            run.id,
            if steps.is_empty() {
                " (this one recorded no steps)".to_string()
            } else {
                format!(": {}", steps.join(", "))
            }
        );
    }
    if let Some(reason) = refuse_target(&run, registry) {
        anyhow::bail!("{reason}");
    }
    let rows = store.rows(run.id)?;
    let reversible = rows.iter().any(|row| {
        row.status == "ok"
            && row.subject == Subject::Channel
            && (row.field == "created"
                || crate::channel_ops::Field::from_label(&row.field).is_some())
    });
    if !reversible {
        let foreign = rows
            .iter()
            .any(|row| matches!(row.subject, Subject::Stream | Subject::EpgSource));
        anyhow::bail!(
            "run #{} recorded no channel write that landed{}: there is nothing to undo",
            run.id,
            if foreign {
                " (its rows are about streams or EPG sources — a probe's stats or a \
                 watchdog's checks — which record no earlier value to put back)"
            } else {
                ""
            }
        );
    }
    Ok(run)
}

fn refuse_target(run: &RunRecord, registry: &Registry) -> Option<String> {
    if registry.get(&run.app).is_none() {
        return Some(format!(
            "app {:?} no longer exists: its rows name fields nothing can interpret",
            run.app
        ));
    }
    if run.dry_run || run.action.ends_with("_preview") {
        return Some(format!(
            "run #{} is a preview: previews record plans, not writes, so there is nothing \
             to undo",
            run.id
        ));
    }
    if run.status == RunStatus::Running {
        return Some(format!(
            "run #{} is still running: wait for it to finish before undoing it",
            run.id
        ));
    }
    if run.status == RunStatus::Paused {
        return Some(format!(
            "run #{} is paused at its window's end: it will resume on the next firing, so \
             undoing it now would race the run itself",
            run.id
        ));
    }
    if let Some((_, spec)) = registry.find_action(&run.app, &run.action) {
        if matches!(spec.kind, ActionKind::Read | ActionKind::LocalWrite) {
            return Some(format!(
                "run #{} is {} {}, which writes no channel data: there is nothing to undo",
                run.id, run.app, run.action
            ));
        }
    }
    None
}

/// Plan the undo of one run: which of its rows are still in effect, and what each reverses
/// to.
///
/// Pure: nothing is written and the world is not modified. Only `ok` rows count — a failed
/// or skipped row never landed, and a cancelled run's confirmed rows are what make a
/// mid-run cancel undoable row by row.
pub fn plan_undo(
    store: &Store,
    world: &World,
    run_id: i64,
    registry: &Registry,
) -> Result<UndoPlan> {
    let run = check_target(store, registry, run_id)?;
    let rows = store.rows(run_id)?;

    let by_id: HashMap<i64, &crate::dispatcharr::Channel> = world
        .channels
        .iter()
        .map(|channel| (channel.id, channel))
        .collect();
    let mut plan = UndoPlan {
        target_run_id: run.id,
        app: run.app.clone(),
        action: run.action.clone(),
        ..UndoPlan::default()
    };
    // One row per (channel, field): a run folds a second write to the same field into one
    // row, so a duplicate here is a caller error rather than a second change.
    let mut seen: std::collections::HashSet<(i64, String)> = std::collections::HashSet::new();

    for row in rows.iter().filter(|row| row.status == "ok") {
        if row.subject == Subject::Channel && row.field == "created" {
            plan_created_channel(store, &by_id, row, &run, &mut plan, &mut seen);
            continue;
        }
        if matches!(row.subject, Subject::Group | Subject::Logo) && row.field == "created" {
            plan_left_object(row, &mut plan);
            continue;
        }
        // Create rows use the `created` field; anything else on a group/logo row, and any
        // stream, EPG-source or matching row, is not a channel write.
        let Some(field) = (row.subject == Subject::Channel)
            .then(|| crate::channel_ops::Field::from_label(&row.field))
            .flatten()
        else {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: row.channel_name.clone(),
                field: crate::channel_ops::Field::from_label(&row.field),
                reason: if row.field.is_empty() {
                    "this row matched or measured rather than writing a channel field, so \
                     there is nothing to reverse"
                        .to_string()
                } else {
                    format!(
                        "this row is not a channel write ({}), so there is nothing to reverse",
                        row.subject.label()
                    )
                },
            });
            continue;
        };
        let Some(channel) = by_id.get(&row.channel_id).copied() else {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: row.channel_name.clone(),
                field: Some(field),
                reason: "the channel no longer exists in Dispatcharr".to_string(),
            });
            continue;
        };
        let Some(after) = row.after_value.as_deref() else {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: "the row carries no recorded value, so there is nothing to compare"
                    .to_string(),
            });
            continue;
        };
        let Some(current) = current_wire(channel, field, world) else {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: "the profile this membership belongs to no longer exists".to_string(),
            });
            continue;
        };
        if current != after {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: "the channel has changed since: its value is no longer what this run \
                         wrote, so undoing it would undo that change too"
                    .to_string(),
            });
            continue;
        }
        let Some(before) = row.before_value.as_deref() else {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: "the row carries no earlier value to put back".to_string(),
            });
            continue;
        };
        let Some(reverse) = reverse_of(field, before) else {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: format!(
                    "the recorded earlier value {before:?} does not parse as {}",
                    field.label()
                ),
            });
            continue;
        };
        if field == crate::channel_ops::Field::Group && matches!(reverse, SetValue::Group(None)) {
            // Dispatcharr's REST serializer rejects a null `channel_group_id`, and one bad
            // row rejects the whole `edit/bulk` chunk — so the refusal is per row here,
            // before the plan is built. The channel keeps the group the run gave it.
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: "the channel came from \"No Group\", which cannot be written back \
                         over REST: it keeps the group this run gave it"
                    .to_string(),
            });
            continue;
        }
        if !seen.insert((row.channel_id, field.label())) {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: Some(field),
                reason: "a second row for the same channel field: the first reversal covers it"
                    .to_string(),
            });
            continue;
        }
        plan.changes.push(PlannedUndo {
            channel_id: row.channel_id,
            channel_name: channel.name.clone(),
            field,
            reverse,
        });
    }

    plan.changes
        .sort_by_key(|change| (change.channel_id, change.field.label()));
    plan.skipped.sort_by_key(|skip| {
        (
            skip.channel_id,
            skip.field.map(|field| field.label()).unwrap_or_default(),
        )
    });
    plan.left.sort_by_key(|left| (left.kind.clone(), left.id));
    Ok(plan)
}

/// Reverse one `created` channel row: hide the channel it made.
///
/// A created channel hidden by an undo keeps its ownership, so a later create plan still
/// finds the object it made instead of creating a duplicate. A channel somebody deleted
/// since is gone; one another app owns, or nobody owns any more, is not this run's to
/// hide — only a channel the run's app still owns is.
fn plan_created_channel(
    store: &Store,
    by_id: &HashMap<i64, &crate::dispatcharr::Channel>,
    row: &RunRow,
    run: &RunRecord,
    plan: &mut UndoPlan,
    seen: &mut std::collections::HashSet<(i64, String)>,
) {
    let field = crate::channel_ops::Field::HiddenFromOutput;
    let Some(channel) = by_id.get(&row.channel_id).copied() else {
        plan.skipped.push(UndoSkip {
            channel_id: row.channel_id,
            channel_name: row.channel_name.clone(),
            field: None,
            reason: "the created channel no longer exists in Dispatcharr".to_string(),
        });
        return;
    };
    match store.owner_of("channel", row.channel_id) {
        Ok(Some((owner, _, _))) if owner != run.app => {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: None,
                reason: format!(
                    "the created channel is owned by {owner:?}, not {:?}: it is left alone",
                    run.app
                ),
            });
            return;
        }
        Ok(None) => {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: None,
                reason: format!(
                    "the created channel is no longer owned by {:?}: it is left alone",
                    run.app
                ),
            });
            return;
        }
        Err(error) => {
            plan.skipped.push(UndoSkip {
                channel_id: row.channel_id,
                channel_name: channel.name.clone(),
                field: None,
                reason: format!("the channel's ownership could not be read: {error:#}"),
            });
            return;
        }
        _ => {}
    }
    if channel.hidden_from_output {
        plan.skipped.push(UndoSkip {
            channel_id: row.channel_id,
            channel_name: channel.name.clone(),
            field: Some(field),
            reason: "the created channel is already hidden".to_string(),
        });
        return;
    }
    if !seen.insert((row.channel_id, field.label())) {
        return;
    }
    plan.changes.push(PlannedUndo {
        channel_id: row.channel_id,
        channel_name: channel.name.clone(),
        field,
        reverse: SetValue::Flag(true),
    });
}

/// List one created group or logo row: undo never deletes, so these stay and are reported.
fn plan_left_object(row: &RunRow, plan: &mut UndoPlan) {
    let kind = match row.subject {
        Subject::Group => "group",
        Subject::Logo => "logo",
        _ => return,
    };
    let id = row
        .after_value
        .as_deref()
        .and_then(|wire| wire.trim().parse::<i64>().ok())
        .unwrap_or(row.channel_id);
    plan.left.push(LeftObject {
        kind: kind.to_string(),
        id,
        name: row.channel_name.clone(),
    });
}

/// The action: preview (a read and a CSV) or apply (the writes).
///
/// Recorded under the target run's app as [`ACTION_UNDO`] with `parent_run_id` set to the
/// undone run by the caller (the runner links the run when it starts it), so undoing an
/// undo restores the applied state.
pub async fn run_undo(
    registry: &Registry,
    target_run_id: i64,
    context: &JobContext,
    dry_run: bool,
) -> Result<JobOutcome> {
    let store = context
        .store()
        .context("an undo needs the run store")?
        .clone();
    let target = store
        .run(target_run_id)?
        .with_context(|| format!("there is no run #{target_run_id}"))?;
    let app = registry
        .get(&target.app)
        .with_context(|| format!("app {:?} no longer exists", target.app))?;
    let client = context.dispatcharr()?;
    let action_id = format!(
        "{ACTION_UNDO}_{}",
        if dry_run { "preview" } else { "apply" }
    );

    // Every slice the plan reads or the report names: channels and groups for values and
    // names, profiles for memberships, entries for guide names, streams and logos for list
    // and logo names.
    context.progress(&crate::jobs::ProgressEvent::new(
        ACTION_UNDO,
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = client
        .load_world_with(
            WorldKinds::only(WorldKind::Channels)
                | WorldKind::Groups
                | WorldKind::Profiles
                | WorldKind::EpgEntries
                | WorldKind::Streams
                | WorldKind::Logos,
        )
        .await?;
    let undo = plan_undo(&store, &world, target_run_id, registry)?;

    // One per-channel value map per field, through channel_ops.
    let mut by_field: HashMap<crate::channel_ops::Field, HashMap<i64, SetValue>> = HashMap::new();
    for change in &undo.changes {
        by_field
            .entry(change.field)
            .or_default()
            .insert(change.channel_id, change.reverse.clone());
    }
    let mut operations: Vec<Op> = by_field
        .into_iter()
        .map(|(field, values)| Op::Set { field, values })
        .collect();
    operations.sort_by_key(|op| match op {
        Op::Set { field, .. } => field.label(),
        _ => String::new(),
    });
    if operations.is_empty() {
        // `plan` needs at least one operation; an empty undo plans nothing to write.
        operations.push(Op::Set {
            field: crate::channel_ops::Field::Name,
            values: HashMap::new(),
        });
    }
    // Undo reverses exactly the rows it read: no scope filter, so a channel that has since
    // left the app's scope is still put back.
    let ids: Vec<i64> = {
        let mut ids: Vec<i64> = undo
            .changes
            .iter()
            .map(|change| change.channel_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    let selection = Selection {
        channel_ids: Some(ids),
        ..Selection::default()
    };
    let mut op_plan = channel_ops::plan(&world, &selection, &operations)?;
    // The rows the chain rule left alone, and the created objects left in place, ride along
    // as skips so the report answers both what happened and what was left alone.
    for skip in &undo.skipped {
        op_plan.skipped.push(channel_ops::Skip {
            channel_id: skip.channel_id,
            channel_name: skip.channel_name.clone(),
            channel_number: by_id_number(&world, skip.channel_id),
            channel_group: group_of(&world, skip.channel_id),
            op: skip.field.map(|_| channel_ops::OpKind::Set),
            field: skip.field,
            reason: skip.reason.clone(),
        });
    }

    let applied = if dry_run || op_plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &op_plan, context, &action_id).await)
    };

    let schema = app.schema().clone();
    let settings_path = app.settings_path(&context.config().config_dir);
    let values = app.load_settings(&settings_path).unwrap_or_default();
    let stem = format!(
        "{ACTION_UNDO}_{}",
        if dry_run { "preview" } else { "applied" }
    );
    let title = format!(
        "Undo Run #{} {} Report",
        target_run_id,
        if dry_run { "Preview" } else { "Applied" }
    );
    let left_groups = undo.left.iter().filter(|left| left.kind == "group").count();
    let left_logos = undo.left.len() - left_groups;
    let mut spec = ReportSpec::new(app.as_ref(), &schema, &values, &stem, &title)
        .note(
            "Undone run",
            format!(
                "#{} {} {} ({})",
                target.id, target.app, target.action, target.status
            ),
        )
        .note(
            "Result set",
            format!(
                "{} change(s) still in effect, {} row(s) left alone",
                undo.changes.len(),
                undo.skipped.len()
            ),
        );
    if !undo.left.is_empty() {
        let names: Vec<String> = undo
            .left
            .iter()
            .map(|left| format!("{} #{} {}", left.kind, left.id, left.name))
            .collect();
        spec = spec.note(
            "Left in place",
            format!(
                "{} created {} left, never deleted: {}",
                undo.left.len(),
                if undo.left.len() == 1 {
                    "object"
                } else {
                    "objects"
                },
                names.join("; ")
            ),
        );
    }
    let report = channel_ops::write_ops_report(context, &spec, &op_plan, applied.as_ref())?;
    context.record_rows(&channel_ops::run_rows(&op_plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    crate::csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut builder = OutcomeBuilder::new(if dry_run {
        format!(
            "Dry run complete. Undoing run #{} would reverse {} change(s) and leave {} row(s) \
             alone.",
            target_run_id,
            undo.changes.len(),
            undo.skipped.len()
        )
    } else {
        format!(
            "Undid run #{}: reversed {} change(s), {} row(s) left alone.",
            target_run_id,
            undo.changes.len(),
            undo.skipped.len()
        )
    })
    .count("undone_run", target_run_id)
    .count("changes", undo.changes.len() as i64)
    .count("skipped", undo.skipped.len() as i64)
    .count("left_groups", left_groups as i64)
    .count("left_logos", left_logos as i64)
    .count("planned", op_plan.changes.len() as i64);
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if let Some(note) = channel_ops::cancelled_note(context, &op_plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
}

fn by_id_number(world: &World, channel_id: i64) -> Option<f64> {
    world
        .channels
        .iter()
        .find(|channel| channel.id == channel_id)
        .and_then(|channel| channel.display_number())
}

fn group_of(world: &World, channel_id: i64) -> String {
    world
        .channels
        .iter()
        .find(|channel| channel.id == channel_id)
        .map(|channel| world.group_name(channel.channel_group_id))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel_ops::{Field, Value};
    use crate::dispatcharr::{Channel, ChannelGroup, ChannelProfile};
    use crate::store::Subject;
    use crate::subapp::SubApp;
    use crate::test_app::StubApp;
    use std::sync::Arc;

    fn registry() -> Registry {
        Registry::new(vec![Arc::new(StubApp::new())])
    }

    fn world() -> World {
        let channel = |id: i64, name: &str, group: Option<i64>| Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(id as f64)),
            channel_group_id: group,
            hidden_from_output: false,
            ..Channel::default()
        };
        World {
            channels: vec![
                channel(1, "ESPN", Some(100)),
                channel(2, "CNN", Some(100)),
                channel(3, "HBO", None),
            ],
            groups: vec![
                ChannelGroup {
                    id: 100,
                    name: "Sports".to_string(),
                },
                ChannelGroup {
                    id: 102,
                    name: "News".to_string(),
                },
            ],
            profiles: vec![ChannelProfile {
                id: 7,
                name: "Kids".to_string(),
                channels: vec![1],
            }],
            ..World::default()
        }
    }

    fn target(store: &Store, action: &str, dry_run: bool) -> i64 {
        let id = store.start_run("stub", action, dry_run).expect("start");
        store
            .finish_run(
                id,
                RunStatus::Succeeded,
                Some("did things"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        id
    }

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

    /// Rows keyed by `(run_id, ordinal)`, so each row needs its own ordinal.
    fn ordinaled(rows: &[RunRow]) -> Vec<RunRow> {
        rows.iter()
            .enumerate()
            .map(|(ordinal, row)| {
                let mut row = row.clone();
                row.ordinal = ordinal as i64;
                row
            })
            .collect()
    }

    fn plan_with(rows: &[RunRow], mutate: impl FnOnce(&mut World)) -> (Store, World, i64) {
        let store = Store::in_memory().expect("store");
        let id = target(&store, "apply", false);
        store.append_rows(id, &ordinaled(rows)).expect("rows");
        let mut world = world();
        mutate(&mut world);
        (store, world, id)
    }

    #[test]
    fn a_name_group_and_flag_round_trip() {
        // The world still holds what the run wrote: every row reverses to its before.
        let (store, world, id) = plan_with(
            &[
                row(1, "name", "ESPN", "ESPN HD"),
                row(1, "channel_group_id", "100", "102"),
                row(2, "hidden_from_output", "false", "true"),
            ],
            |world| {
                world.channels[0].name = "ESPN HD".to_string();
                world.channels[0].channel_group_id = Some(102);
                world.channels[1].hidden_from_output = true;
            },
        );
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 3, "{plan:?}");
        let by_field: HashMap<String, &PlannedUndo> = plan
            .changes
            .iter()
            .map(|change| (change.field.label(), change))
            .collect();
        assert_eq!(by_field["name"].reverse, SetValue::Name("ESPN".to_string()));
        assert_eq!(
            by_field["channel_group_id"].reverse,
            SetValue::Group(Some(100))
        );
        assert_eq!(
            by_field["hidden_from_output"].reverse,
            SetValue::Flag(false)
        );
        assert!(plan.skipped.is_empty(), "{:?}", plan.skipped);
    }

    #[test]
    fn every_field_type_reverses() {
        let (store, world, id) = plan_with(
            &[
                row(1, "epg_data_id", "none", "44"),
                row(1, "streams", "none", "7,8"),
                row(1, "channel_number", "1", "201"),
                row(1, "logo_id", "none", "9"),
                row(1, "profile_membership:7", "true", "false"),
            ],
            |world| {
                world.channels[0].epg_data_id = Some(44);
                world.channels[0].streams = vec![7, 8];
                world.channels[0].channel_number = Some(serde_json::json!(201.0));
                world.channels[0].logo_id = Some(9);
                world.profiles[0].channels = vec![];
            },
        );
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 5, "{plan:?}");
        let by_field: HashMap<String, &PlannedUndo> = plan
            .changes
            .iter()
            .map(|change| (change.field.label(), change))
            .collect();
        assert_eq!(by_field["epg_data_id"].reverse, SetValue::Epg(None));
        assert_eq!(by_field["streams"].reverse, SetValue::Streams(Vec::new()));
        assert_eq!(
            by_field["channel_number"].reverse,
            SetValue::Number(Some(1.0))
        );
        assert_eq!(by_field["logo_id"].reverse, SetValue::Logo(None));
        assert_eq!(
            by_field["profile_membership:7"].reverse,
            SetValue::Flag(true)
        );
    }

    #[test]
    fn a_later_hand_edit_survives_the_undo() {
        // Somebody renamed the channel after the run: the recorded rename no longer holds,
        // so it is skipped — while the group move on the same channel still reverses.
        let (store, world, id) = plan_with(
            &[
                row(1, "name", "ESPN", "ESPN HD"),
                row(1, "channel_group_id", "100", "102"),
            ],
            |world| {
                world.channels[0].name = "ESPN by hand".to_string();
                world.channels[0].channel_group_id = Some(102);
            },
        );
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(plan.changes[0].field, Field::Group);
        assert_eq!(plan.skipped.len(), 1);
        assert!(plan.skipped[0].reason.contains("changed since"), "{plan:?}");
    }

    #[test]
    fn undoing_the_undo_restores_the_applied_state() {
        // The redo: an undo run's own rows reverse its reversals. The world holds the undone
        // state (names back to before), and the undo run's rows say after = before.
        let store = Store::in_memory().expect("store");
        let first = target(&store, "apply", false);
        store
            .append_rows(first, &[row(1, "name", "ESPN", "ESPN HD")])
            .expect("rows");
        let undo_run = target(&store, ACTION_UNDO, false);
        store
            .append_rows(undo_run, &[row(1, "name", "ESPN HD", "ESPN")])
            .expect("rows");
        let mut world = world();
        world.channels[0].name = "ESPN".to_string();
        let plan = plan_undo(&store, &world, undo_run, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(
            plan.changes[0].reverse,
            SetValue::Name("ESPN HD".to_string())
        );
    }

    #[test]
    fn created_channels_hide_and_groups_logos_are_listed() {
        let store = Store::in_memory().expect("store");
        let id = target(&store, "apply", false);
        store
            .append_rows(
                id,
                &ordinaled(&[
                    RunRow {
                        subject: Subject::Channel,
                        channel_id: 50,
                        channel_name: "New Channel".to_string(),
                        field: "created".to_string(),
                        after_value: Some("50".to_string()),
                        status: "ok".to_string(),
                        owner_key: Some("stub:key".to_string()),
                        ..RunRow::default()
                    },
                    RunRow {
                        subject: Subject::Group,
                        channel_id: 60,
                        channel_name: "New Group".to_string(),
                        field: "created".to_string(),
                        after_value: Some("60".to_string()),
                        status: "ok".to_string(),
                        owner_key: Some("stub:g".to_string()),
                        ..RunRow::default()
                    },
                    RunRow {
                        subject: Subject::Logo,
                        channel_id: 70,
                        channel_name: "New Logo".to_string(),
                        field: "created".to_string(),
                        after_value: Some("70".to_string()),
                        status: "ok".to_string(),
                        owner_key: Some("stub:l".to_string()),
                        ..RunRow::default()
                    },
                ]),
            )
            .expect("rows");
        store
            .record_ownership("channel", 50, "stub", "stub:key", id)
            .expect("ownership");
        let mut world = world();
        world.channels.push(Channel {
            id: 50,
            name: "New Channel".to_string(),
            channel_group_id: Some(100),
            hidden_from_output: false,
            ..Channel::default()
        });
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(plan.changes[0].field, Field::HiddenFromOutput);
        assert_eq!(plan.changes[0].reverse, SetValue::Flag(true));
        assert_eq!(plan.left.len(), 2, "{plan:?}");
        assert!(plan
            .left
            .iter()
            .any(|left| left.kind == "group" && left.id == 60));
        assert!(plan
            .left
            .iter()
            .any(|left| left.kind == "logo" && left.id == 70));
    }

    #[test]
    fn a_created_channel_owned_elsewhere_is_left_alone() {
        let store = Store::in_memory().expect("store");
        let id = target(&store, "apply", false);
        store
            .append_rows(
                id,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 50,
                    channel_name: "New Channel".to_string(),
                    field: "created".to_string(),
                    after_value: Some("50".to_string()),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        store
            .record_ownership("channel", 50, "other", "other:key", id)
            .expect("ownership");
        let mut world = world();
        world.channels.push(Channel {
            id: 50,
            name: "New Channel".to_string(),
            ..Channel::default()
        });
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert!(plan.changes.is_empty(), "{plan:?}");
        assert!(plan.skipped[0].reason.contains("owned by"), "{plan:?}");
    }

    #[test]
    fn a_move_back_to_no_group_is_refused_per_row() {
        // REST cannot clear a group: the row is planned and refused, not sent.
        let (store, world, id) = plan_with(&[row(1, "channel_group_id", "none", "102")], |world| {
            world.channels[0].channel_group_id = Some(102);
        });
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert!(plan.changes.is_empty(), "{plan:?}");
        assert_eq!(plan.skipped.len(), 1);
        assert!(plan.skipped[0].reason.contains("No Group"), "{plan:?}");
    }

    #[test]
    fn only_ok_rows_count() {
        // A `planned` preview row, a `failed` write and a no-op `skipped` row never landed,
        // so only the one `ok` row reverses.
        let store = Store::in_memory().expect("store");
        let id = target(&store, "apply", false);
        let mut rows = vec![row(1, "name", "ESPN", "ESPN HD")];
        for status in ["planned", "failed", "skipped"] {
            let mut row = row(2, "name", "CNN", "CNN HD");
            row.status = status.to_string();
            rows.push(row);
        }
        store.append_rows(id, &ordinaled(&rows)).expect("rows");
        let mut world = world();
        world.channels[0].name = "ESPN HD".to_string();
        world.channels[1].name = "CNN HD".to_string();
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(plan.changes[0].channel_id, 1);
    }

    #[test]
    fn matching_rows_are_not_channel_writes() {
        let store = Store::in_memory().expect("store");
        let id = target(&store, "match_apply", false);
        store
            .append_rows(
                id,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 1,
                    channel_name: "ESPN".to_string(),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        // A run of nothing but matching rows wrote no field: refused, not an empty undo.
        let error = plan_undo(&store, &world(), id, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("no channel write"), "{error}");

        // Beside a real write, the matching row rides along as a skip.
        store
            .append_rows(
                id,
                &[RunRow {
                    ordinal: 1,
                    ..row(1, "name", "ESPN", "ESPN HD")
                }],
            )
            .expect("rows");
        let mut world = world();
        world.channels[0].name = "ESPN HD".to_string();
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert!(
            plan.skipped[0].reason.contains("rather than writing"),
            "{plan:?}"
        );
    }

    #[test]
    fn a_probe_run_is_refused_clearly() {
        // A probe's rows are about streams, and its stats writes record no earlier value.
        let store = Store::in_memory().expect("store");
        let id = target(&store, "apply", false);
        store
            .append_rows(
                id,
                &[RunRow {
                    subject: Subject::Stream,
                    channel_id: 900,
                    field: "stream_stats".to_string(),
                    after_value: Some("{}".to_string()),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        let error = plan_undo(&store, &world(), id, &registry()).expect_err("refused");
        let text = format!("{error}");
        assert!(text.contains("streams or EPG sources"), "{text}");
    }

    #[test]
    fn a_workflow_run_is_refused_with_its_steps_listed() {
        let store = Store::in_memory().expect("store");
        let workflow = store
            .insert_run(crate::workflow::APP_ID, "nightly", false, false, None)
            .expect("workflow");
        for action in ["apply", "other_apply"] {
            let step = store
                .insert_run("stub", action, false, true, Some(workflow))
                .expect("step");
            store
                .finish_run(
                    step,
                    RunStatus::Succeeded,
                    None,
                    &serde_json::json!({}),
                    None,
                )
                .expect("finish");
        }
        store
            .finish_run(
                workflow,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let error = plan_undo(&store, &world(), workflow, &registry()).expect_err("refused");
        let text = format!("{error}");
        assert!(text.contains("workflow run"), "{text}");
        let other = text.find("stub.other_apply").expect("second step listed");
        let first = text.find("stub.apply").expect("first step listed");
        assert!(other < first, "newest step first: {text}");
    }

    #[test]
    fn a_pruned_run_names_retention() {
        let store = Store::in_memory().expect("store");
        let error = check_target(&store, &registry(), 4242).expect_err("refused");
        assert!(format!("{error}").contains("run_history_days"), "{error}");
    }

    #[test]
    fn a_second_undo_of_the_same_run_finds_nothing_in_effect() {
        // After the first undo applied, the world holds the before values again.
        let (store, world, id) = plan_with(
            &[
                row(1, "name", "ESPN", "ESPN HD"),
                row(1, "channel_number", "1", "201"),
            ],
            |_| {},
        );
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert!(plan.is_empty(), "{plan:?}");
        assert_eq!(plan.skipped.len(), 2);
        assert!(plan
            .skipped
            .iter()
            .all(|skip| skip.reason.contains("changed since")));
    }

    #[test]
    fn a_created_channel_nobody_owns_is_left_alone() {
        let store = Store::in_memory().expect("store");
        let id = target(&store, "apply", false);
        store
            .append_rows(
                id,
                &[RunRow {
                    subject: Subject::Channel,
                    channel_id: 50,
                    channel_name: "New Channel".to_string(),
                    field: "created".to_string(),
                    after_value: Some("50".to_string()),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");
        let mut world = world();
        world.channels.push(Channel {
            id: 50,
            name: "New Channel".to_string(),
            ..Channel::default()
        });
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert!(plan.changes.is_empty(), "{plan:?}");
        assert!(
            plan.skipped[0].reason.contains("no longer owned"),
            "{plan:?}"
        );
    }

    #[test]
    fn refusals_name_the_reason() {
        let store = Store::in_memory().expect("store");
        // A preview run: dry_run refuses before any row is read.
        let preview = target(&store, "apply", true);
        let error = plan_undo(&store, &world(), preview, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("preview"), "{error}");
        // A preview action refuses even when recorded as an apply.
        let named = target(&store, "match_preview", false);
        let error = plan_undo(&store, &world(), named, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("preview"), "{error}");
        // A read action refuses.
        let read = target(&store, "preview", false);
        let error = plan_undo(&store, &world(), read, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("no channel data"), "{error}");
        // A running run refuses.
        let running = store.start_run("stub", "apply", false).expect("start");
        let error = plan_undo(&store, &world(), running, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("still running"), "{error}");
        // A paused run refuses.
        store
            .finish_run(
                running,
                RunStatus::Paused,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let error = plan_undo(&store, &world(), running, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("paused"), "{error}");
        // An unknown run refuses.
        let error = plan_undo(&store, &world(), 9999, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("no run"), "{error}");
        // An app that no longer exists refuses.
        let gone = store.start_run("gone", "apply", false).expect("start");
        store
            .finish_run(
                gone,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let error = plan_undo(&store, &world(), gone, &registry()).expect_err("refused");
        assert!(format!("{error}").contains("no longer exists"), "{error}");
    }

    #[test]
    fn an_undo_plan_applies_back_to_the_before_state() {
        // apply → undo → state equals before, through the same plan path `run_undo` uses
        // (minus the network): the undo's reversals re-planned as `Op::Set` land exactly on
        // the recorded before values.
        let (store, world, id) = plan_with(
            &[
                row(1, "name", "ESPN", "ESPN HD"),
                row(1, "channel_group_id", "100", "102"),
                row(2, "hidden_from_output", "false", "true"),
            ],
            |world| {
                world.channels[0].name = "ESPN HD".to_string();
                world.channels[0].channel_group_id = Some(102);
                world.channels[1].hidden_from_output = true;
            },
        );
        let undo = plan_undo(&store, &world, id, &registry()).expect("plans");
        let mut by_field: HashMap<Field, HashMap<i64, SetValue>> = HashMap::new();
        for change in &undo.changes {
            by_field
                .entry(change.field)
                .or_default()
                .insert(change.channel_id, change.reverse.clone());
        }
        let mut operations: Vec<Op> = by_field
            .into_iter()
            .map(|(field, values)| Op::Set { field, values })
            .collect();
        operations.sort_by_key(|op| match op {
            Op::Set { field, .. } => field.label(),
            _ => String::new(),
        });
        let selection = Selection {
            channel_ids: Some(vec![1, 2]),
            ..Selection::default()
        };
        let op_plan = channel_ops::plan(&world, &selection, &operations).expect("plans");
        assert_eq!(op_plan.changes.len(), 3, "{op_plan:?}");
        for change in &op_plan.changes {
            match change.field {
                Field::Name => assert_eq!(change.after, Value::Name("ESPN".to_string())),
                Field::Group => assert_eq!(
                    change.after,
                    Value::Group(Some((100, "Sports".to_string())))
                ),
                Field::HiddenFromOutput => assert_eq!(change.after, Value::Flag(false)),
                field => panic!("unexpected field {field:?}"),
            }
        }
    }

    /// One app under another name: undo is a host path, so the app id is only a label.
    #[derive(Debug)]
    struct NamedApp {
        app_id: &'static str,
    }

    impl SubApp for NamedApp {
        fn id(&self) -> &'static str {
            self.app_id
        }
        fn name(&self) -> &'static str {
            self.app_id
        }
        fn description(&self) -> &'static str {
            "test app"
        }
        fn actions(&self) -> Vec<crate::subapp::ActionSpec> {
            vec![
                crate::subapp::ActionSpec::new("scan", "Scan", "reads", ActionKind::Read),
                crate::subapp::ActionSpec::new("match_apply", "Match", "writes", ActionKind::Write),
                crate::subapp::ActionSpec::new(
                    "triage_apply",
                    "Triage",
                    "writes",
                    ActionKind::Write,
                ),
            ]
        }
        fn schema(&self) -> &crate::subapp::SettingsSchema {
            static SCHEMA: std::sync::OnceLock<crate::subapp::SettingsSchema> =
                std::sync::OnceLock::new();
            SCHEMA.get_or_init(crate::subapp::SettingsSchema::default)
        }
        fn load_settings(
            &self,
            _path: &std::path::Path,
        ) -> anyhow::Result<crate::subapp::SettingsValues> {
            Ok(crate::subapp::SettingsValues::new())
        }
        fn save_settings(
            &self,
            _path: &std::path::Path,
            _values: &crate::subapp::SettingsValues,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        fn run<'a>(
            &'a self,
            _action: &'a str,
            _context: &'a JobContext,
            _dry_run: bool,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<JobOutcome>> + Send + 'a>,
        > {
            Box::pin(async { Ok(JobOutcome::new("ok")) })
        }
    }

    fn cross_registry() -> Registry {
        Registry::new(vec![
            Arc::new(NamedApp { app_id: "guide" }),
            Arc::new(NamedApp { app_id: "streams" }),
        ])
    }

    fn finished(store: &Store, app: &str, action: &str, rows: &[RunRow]) -> i64 {
        let id = store.start_run(app, action, false).expect("start");
        let rows: Vec<RunRow> = rows
            .iter()
            .enumerate()
            .map(|(ordinal, row)| {
                let mut row = row.clone();
                row.ordinal = ordinal as i64;
                row
            })
            .collect();
        store.append_rows(id, &rows).expect("rows");
        store
            .finish_run(id, RunStatus::Succeeded, None, &serde_json::json!({}), None)
            .expect("finish");
        id
    }

    #[test]
    fn cross_app_runs_undo_through_the_same_path() {
        // A guide match run (guide assignment), a streams match run (stream list) and a
        // triage run (name + group): three apps, one host rule.
        let store = Store::in_memory().expect("store");
        let registry = cross_registry();
        let guide = finished(
            &store,
            "guide",
            "match_apply",
            &[row(1, "epg_data_id", "none", "44")],
        );
        let streams = finished(
            &store,
            "streams",
            "match_apply",
            &[row(1, "streams", "none", "7,8")],
        );
        let triage = finished(
            &store,
            "streams",
            "triage_apply",
            &[
                row(2, "name", "CNN", "CNN [Slow]"),
                row(2, "channel_group_id", "100", "102"),
            ],
        );
        let mut world = world();
        world.channels[0].epg_data_id = Some(44);
        world.channels[0].streams = vec![7, 8];
        world.channels[1].name = "CNN [Slow]".to_string();
        world.channels[1].channel_group_id = Some(102);

        let plan = plan_undo(&store, &world, guide, &registry).expect("guide plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(plan.changes[0].reverse, SetValue::Epg(None));

        let plan = plan_undo(&store, &world, streams, &registry).expect("streams plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(plan.changes[0].reverse, SetValue::Streams(Vec::new()));

        let plan = plan_undo(&store, &world, triage, &registry).expect("triage plans");
        assert_eq!(plan.changes.len(), 2, "{plan:?}");
    }

    #[tokio::test]
    async fn cancel_mid_undo_records_landed_rows() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // The undo below reverses two renames. The mock answers the bulk edit, but the
        // context is already cancelled: nothing is sent, every row fails as cancelled, and
        // the recorded rows say so — the same rows a mid-run cancel leaves behind.
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": [{"id": 1}]})),
            )
            .mount(&server)
            .await;
        let client =
            crate::dispatcharr::DispatcharrClient::new(&crate::config::DispatcharrConfig {
                base_url: server.uri(),
                api_key: "test-key".to_string(),
                timeout_secs: 5,
                concurrency: 4,
                ..crate::config::DispatcharrConfig::default()
            })
            .expect("client");

        let (store, world, id) = plan_with(
            &[
                row(1, "name", "ESPN", "ESPN HD"),
                row(2, "name", "CNN", "CNN HD"),
            ],
            |world| {
                world.channels[0].name = "ESPN HD".to_string();
                world.channels[1].name = "CNN HD".to_string();
            },
        );
        let undo = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(undo.changes.len(), 2);
        let mut values: HashMap<i64, SetValue> = HashMap::new();
        for change in &undo.changes {
            values.insert(change.channel_id, change.reverse.clone());
        }
        let selection = Selection {
            channel_ids: Some(vec![1, 2]),
            ..Selection::default()
        };
        let op_plan = channel_ops::plan(
            &world,
            &selection,
            &[Op::Set {
                field: Field::Name,
                values,
            }],
        )
        .expect("plans");

        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        context.cancel();
        let applied = channel_ops::apply(&client, &op_plan, &context, "undo_apply").await;
        assert_eq!(applied.ok, 0);
        assert_eq!(applied.failed, 2);
        assert!(
            applied
                .changes
                .iter()
                .all(|change| change.reason.contains("cancelled")),
            "{:#?}",
            applied.changes
        );
        // The landed rows are recorded, not lost: a retry or a later undo reads them.
        let rows = channel_ops::run_rows(&op_plan, Some(&applied));
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.status == "failed"));
        assert!(rows.iter().all(|row| row.field == "name"));
    }

    #[test]
    fn a_failed_run_undoes_the_rows_that_landed() {
        // A cancel mid-apply records the confirmed rows: the undo covers exactly those.
        let store = Store::in_memory().expect("store");
        let id = store.start_run("stub", "apply", false).expect("start");
        store
            .append_rows(
                id,
                &ordinaled(&[
                    row(1, "name", "ESPN", "ESPN HD"),
                    RunRow {
                        status: "failed".to_string(),
                        ..row(2, "name", "CNN", "CNN HD")
                    },
                ]),
            )
            .expect("rows");
        store
            .finish_run(
                id,
                RunStatus::Cancelled,
                Some("cancelled after 1 of 2"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let mut world = world();
        world.channels[0].name = "ESPN HD".to_string();
        world.channels[1].name = "CNN".to_string();
        let plan = plan_undo(&store, &world, id, &registry()).expect("plans");
        assert_eq!(plan.changes.len(), 1, "{plan:?}");
        assert_eq!(plan.changes[0].channel_id, 1);
    }
}
