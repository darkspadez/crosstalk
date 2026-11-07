//! Dump the tidy pipeline's decisions for `scripts/oracle_report_lineup_sync.py`.
//!
//! Reads one JSON document from stdin:
//!
//! ```json
//! {
//!   "channels": [{"id": 1, "name": "CNN", "group": "DIRECTV: News", "streams": 0, "hidden": false}],
//!   "owned": [1],
//!   "hid": [3]
//! }
//! ```
//!
//! `owned` lists the live channel ids owned under the configured file's prefix;
//! `hid` lists the ids a past `tidy_apply` hid (the run reads these from the
//! run rows via `field_history`). Prints one JSON object with one decision row
//! per owned channel (the same
//! [`decide_tidy`](crosstalk_lineup::tidy::decide_tidy) the action plans from) —
//! the oracle compares these against Lineuparr's cleanup rule (delete every
//! stream-less channel in lineup groups, owned or not) over the same world.

use std::collections::{HashMap, HashSet};
use std::io::Read;

#[derive(Debug, serde::Deserialize)]
struct InputChannel {
    id: i64,
    name: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    number: Option<f64>,
    #[serde(default)]
    streams: usize,
    #[serde(default)]
    hidden: bool,
}

#[derive(Debug, serde::Deserialize)]
struct Input {
    #[serde(default)]
    channels: Vec<InputChannel>,
    #[serde(default)]
    owned: Vec<i64>,
    #[serde(default)]
    hid: Vec<i64>,
}

fn main() -> anyhow::Result<()> {
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text)?;
    let input: Input = serde_json::from_str(&text)?;

    let mut group_ids: HashMap<String, i64> = HashMap::new();
    let mut groups: Vec<crosstalk_core::dispatcharr::ChannelGroup> = Vec::new();
    for channel in &input.channels {
        if !channel.group.is_empty() && !group_ids.contains_key(&channel.group) {
            let id = 1_000_000 + group_ids.len() as i64;
            group_ids.insert(channel.group.clone(), id);
            groups.push(crosstalk_core::dispatcharr::ChannelGroup {
                id,
                name: channel.group.clone(),
            });
        }
    }
    // Stream counts are what tidy reads; the ids are placeholders.
    let world = crosstalk_core::dispatcharr::World {
        channels: input
            .channels
            .iter()
            .map(|channel| crosstalk_core::dispatcharr::Channel {
                id: channel.id,
                name: channel.name.clone(),
                channel_number: channel.number.map(|number| serde_json::json!(number)),
                channel_group_id: group_ids.get(&channel.group).copied(),
                hidden_from_output: channel.hidden,
                streams: (0..channel.streams as i64).collect(),
                ..crosstalk_core::dispatcharr::Channel::default()
            })
            .collect(),
        groups,
        ..crosstalk_core::dispatcharr::World::default()
    };

    let owned: HashSet<i64> = input.owned.iter().copied().collect();
    let hid: HashSet<i64> = input.hid.iter().copied().collect();
    let rows = crosstalk_lineup::tidy::decide_tidy(&world, &owned, &hid);
    let rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "id": row.id,
                "name": row.name,
                "group": row.group,
                "number": row.number,
                "streams": row.streams,
                "hidden": row.hidden,
                "action": format!("{:?}", row.action),
                "reason": row.reason,
            })
        })
        .collect();
    println!("{}", serde_json::json!({"rows": rows}));
    Ok(())
}
