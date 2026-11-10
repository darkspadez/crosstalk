//! JSON harness for `scripts/channel_ops_parity.py`.
//!
//! Reads one JSON document — a fixture world and a list of channel-op cases — and writes one
//! JSON answer per line, so the Python harness can compare `channel_ops::plan` against the
//! plugin's own selection and payload logic for the five cleanup actions.
//!
//! Input:
//!
//! ```json
//! {
//!   "world": {"channels": [...], "groups": [...], "profiles": [...], "entries": [...],
//!             "streams": [...]},
//!   "cases": [{"name": "clear_groups", "selection": {...}, "ops": [{"op": "clear_epg"}]}]
//! }
//! ```
//!
//! A per-channel operation is `{"op": "set", "field": "<field>", "values": {"<channel id>":
//! <value>}}`, and its values take the field's own shape: `{"name": "…"}`, `{"group": 20}`,
//! `{"group": null}`, `{"epg": 31}`, `{"flag": true}`, `{"streams": [30, 20]}`. A move to
//! "No Group" is `{"op": "move_group", "group_id": null}`.
//!
//! Output, one line per case, in input order:
//!
//! ```json
//! {"name": "clear_groups", "selected": [1, 2],
//!  "changes": [{"channel_id": 1, "field": "epg_data_id", "before": "ESPN HD",
//!               "after": "No EPG", "before_wire": "31", "after_wire": "none"}],
//!  "skipped": [{"channel_id": 3, "field": "epg_data_id", "reason": "..."}]}
//! ```
//!
//! A case `plan` refuses answers `{"name": ..., "error": "..."}` instead, which is how the
//! harness checks that a Python-only pattern is rejected rather than matched differently.

use crosstalk_core::channel_ops::{self, Op, Plan, PlanOptions, Selection};
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, ChannelProfile, EpgEntry, Stream, World};
use serde::Deserialize;
use std::io::{Read, Write};

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Fixture {
    channels: Vec<Channel>,
    groups: Vec<ChannelGroup>,
    profiles: Vec<ChannelProfile>,
    entries: Vec<EpgEntry>,
    /// Only for the names a stream list prints; the lists themselves come with the channels.
    streams: Vec<Stream>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Document {
    world: Fixture,
    cases: Vec<Case>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Case {
    name: String,
    selection: Selection,
    ops: Vec<Op>,
    options: PlanOptions,
}

fn main() {
    let input = match std::env::args().nth(1) {
        Some(path) => match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) => fail(&format!("reading {path}: {error}")),
        },
        None => {
            let mut text = String::new();
            match std::io::stdin().read_to_string(&mut text) {
                Ok(_) => text,
                Err(error) => fail(&format!("reading stdin: {error}")),
            }
        }
    };

    let document: Document = match serde_json::from_str(&input) {
        Ok(document) => document,
        Err(error) => fail(&format!("invalid fixture: {error}")),
    };

    let world = World {
        channels: document.world.channels,
        groups: document.world.groups,
        profiles: document.world.profiles,
        entries: document.world.entries,
        streams: document.world.streams,
        ..World::default()
    };

    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    for case in &document.cases {
        let answer =
            match channel_ops::plan_with_options(&world, &case.selection, &case.ops, &case.options)
            {
                Ok(plan) => answer(&case.name, &plan),
                Err(error) => serde_json::json!({
                    "name": case.name,
                    "error": format!("{error:#}"),
                }),
            };
        let _ = writeln!(
            out,
            "{}",
            serde_json::to_string(&answer).unwrap_or_default()
        );
    }
    let _ = out.flush();
}

fn fail(message: &str) -> ! {
    eprintln!("channel_ops_io: {message}");
    std::process::exit(2);
}

fn answer(name: &str, plan: &Plan) -> serde_json::Value {
    let changes: Vec<serde_json::Value> = plan
        .changes
        .iter()
        .map(|change| {
            serde_json::json!({
                "channel_id": change.channel_id,
                "field": change.field.label(),
                "before": change.before.display(),
                "after": change.after.display(),
                "before_wire": change.before.wire(),
                "after_wire": change.after.wire(),
            })
        })
        .collect();
    let skipped: Vec<serde_json::Value> = plan
        .skipped
        .iter()
        .map(|skip| {
            serde_json::json!({
                "channel_id": skip.channel_id,
                "field": skip.field.map(|field| field.label()),
                "reason": skip.reason,
            })
        })
        .collect();
    serde_json::json!({
        "name": name,
        "description": plan.description,
        "selected": plan.selected.iter().map(|row| row.id).collect::<Vec<i64>>(),
        "changes": changes,
        "skipped": skipped,
    })
}
