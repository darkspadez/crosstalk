//! Run the streams read-only tools over a synthetic world, for `scripts/tools_parity.py`.
//!
//! One JSON object per line, one line back per case. The case says which tool to exercise and
//! carries the world that tool reads: streams (name, group, guide id), the channel groups their
//! names come from, and the settings the discovery tools resolve (the placeholder block for
//! `scan_placeholders`, the regex rules for `test_rules`).
//!
//! The harness drives Stream-Mapparr's own code over the same case — its
//! `check_stream_countries_action`, its `_resolve_stream_regex_rules` +
//! `_apply_regex_rules_to_streams`, and `placeholder_scan.py` imported directly, since that
//! module is plain Python — so this binary is the only Rust in the comparison.
//!
//! ```text
//! echo '{"tool":"scan_placeholders","streams":[{"name":"MAX 100"}]}' \
//!   | cargo run -p crosstalk-streams --example tools_io
//! ```

use crosstalk_core::dispatcharr::{ChannelGroup, Stream, World};
use crosstalk_streams::placeholder_scan::{self, ScanStats, ScanStream};
use crosstalk_streams::settings::StreamsSettings;
use crosstalk_streams::tools;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
struct StreamInput {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    group: Option<i64>,
    #[serde(default)]
    tvg_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GroupInput {
    id: i64,
    #[serde(default)]
    name: String,
}

/// The settings a tool resolves, in `streams.toml`'s own names.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct SettingsInput {
    epg_placeholder_matching_enabled: bool,
    epg_placeholder_name_patterns: String,
    epg_title_cleanup_rules: String,
    epg_skip_titles: String,
    epg_channel_schedule_cleanup_rules: String,
    epg_watch_source_streams: String,
    stream_name_regex_rules: String,
}

impl Default for SettingsInput {
    fn default() -> Self {
        let defaults = StreamsSettings::default();
        Self {
            epg_placeholder_matching_enabled: defaults.epg_placeholder_matching_enabled,
            epg_placeholder_name_patterns: defaults.epg_placeholder_name_patterns,
            epg_title_cleanup_rules: defaults.epg_title_cleanup_rules,
            epg_skip_titles: defaults.epg_skip_titles,
            epg_channel_schedule_cleanup_rules: defaults.epg_channel_schedule_cleanup_rules,
            epg_watch_source_streams: defaults.epg_watch_source_streams,
            stream_name_regex_rules: defaults.stream_name_regex_rules,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct Case {
    tool: String,
    streams: Vec<StreamInput>,
    groups: Vec<GroupInput>,
    settings: SettingsInput,
    /// `scan_placeholders`' name-length cap. The plugin's own default is 500; `0` means none.
    max_name_len: usize,
}

impl Case {
    fn world(&self) -> World {
        World {
            groups: self
                .groups
                .iter()
                .map(|group| ChannelGroup {
                    id: group.id,
                    name: group.name.clone(),
                })
                .collect(),
            streams: self
                .streams
                .iter()
                .map(|stream| Stream {
                    id: stream.id,
                    name: stream.name.clone(),
                    m3u_account: Some(1),
                    channel_group: stream.group,
                    tvg_id: stream.tvg_id.clone(),
                    ..Stream::default()
                })
                .collect(),
            ..World::default()
        }
    }

    fn settings(&self) -> StreamsSettings {
        StreamsSettings {
            epg_placeholder_matching_enabled: self.settings.epg_placeholder_matching_enabled,
            epg_placeholder_name_patterns: self.settings.epg_placeholder_name_patterns.clone(),
            epg_title_cleanup_rules: self.settings.epg_title_cleanup_rules.clone(),
            epg_skip_titles: self.settings.epg_skip_titles.clone(),
            epg_channel_schedule_cleanup_rules: self
                .settings
                .epg_channel_schedule_cleanup_rules
                .clone(),
            epg_watch_source_streams: self.settings.epg_watch_source_streams.clone(),
            stream_name_regex_rules: self.settings.stream_name_regex_rules.clone(),
            ..StreamsSettings::default()
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();
    while std::io::BufRead::read_line(&mut reader, &mut line)? > 0 {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let case: Case = serde_json::from_str(trimmed)?;
        println!("{}", serde_json::to_string(&run_case(&case)?)?);
        line.clear();
    }
    Ok(())
}

fn run_case(case: &Case) -> anyhow::Result<Value> {
    let world = case.world();
    let settings = case.settings();
    match case.tool.as_str() {
        "check_countries" => {
            let check = tools::check_countries(&world);
            let rows: Vec<Value> = check
                .rows
                .iter()
                .map(|row| {
                    json!({
                        "id": row.stream_id,
                        "name": row.stream_name,
                        "group": row.group,
                        "group_code": row.group_code,
                        "epg_code": row.epg_code,
                        "verdict": row.verdict.as_str(),
                    })
                })
                .collect();
            Ok(json!({
                "tool": "check_countries",
                "streams": check.streams,
                "both": check.both,
                "agree": check.agree,
                "disagree": check.disagree,
                "no_group_country": check.no_group_country,
                "no_epg_country": check.no_epg_country,
                "message": check.message(),
                "rows": rows,
            }))
        }
        "scan_placeholders" => {
            let scan = tools::scan_placeholders(&world, &settings);
            let families: Vec<Value> = scan
                .families
                .iter()
                .map(|family| {
                    json!({
                        "template": family.template,
                        "count": family.count,
                        "distinct_numbers": family.distinct_numbers,
                        "example": family.example,
                        "with_epg_id": family.with_epg_id,
                        "covered": family.covered,
                        "uncovered": family.uncovered,
                        "suggested": family.suggested,
                        "mostly_carries_epg": family.mostly_carries_epg(),
                    })
                })
                .collect();
            let stats: ScanStats = scan.stats;
            Ok(json!({
                "tool": "scan_placeholders",
                "streams": scan.streams,
                "patterns": scan.patterns,
                "enabled": scan.feature_enabled,
                "stats": {"scanned": stats.scanned, "skipped_long": stats.skipped_long},
                "families": families,
                "headline": scan.headline(),
            }))
        }
        "test_rules" => {
            let test = tools::test_rules(&world, &settings);
            let report: Vec<Value> = test
                .report
                .iter()
                .map(|entry| {
                    json!({
                        "index": entry.index,
                        "status": entry.status,
                        "detail": entry.detail,
                        "pattern": entry.pattern,
                    })
                })
                .collect();
            let changed: Vec<Value> = test
                .changed
                .iter()
                .map(|name| {
                    json!({
                        "id": name.stream_id,
                        "before": name.before,
                        "after": name.after,
                    })
                })
                .collect();
            Ok(json!({
                "tool": "test_rules",
                "streams": test.streams,
                "rules": test.rules,
                "report": report,
                "message": test.message(),
                "changed": changed,
                "counters": {
                    "changed": test.counters.changed,
                    "skipped_long": test.counters.skipped_long,
                    "growth_reverted": test.counters.growth_reverted,
                    "emptied": test.counters.emptied,
                    "budget_tripped": test.counters.budget_tripped,
                },
                "samples": test.samples(),
            }))
        }
        "template_of" => {
            // The scan's own template splitter, case by case: the harness compares it with
            // `placeholder_scan.template_of` directly.
            let templates: HashMap<String, Value> = case
                .streams
                .iter()
                .map(|stream| {
                    let (template, numbers) = placeholder_scan::template_of(&stream.name);
                    (
                        stream.name.clone(),
                        json!({
                            "template": template,
                            "numbers": numbers,
                            "suggested": placeholder_scan::suggested_pattern(&template),
                        }),
                    )
                })
                .collect();
            Ok(json!({"tool": "template_of", "templates": templates}))
        }
        "scan_with_cap" => {
            let (patterns, _report) = crosstalk_streams::epg::resolve_placeholder_patterns(
                &settings.epg_placeholder_name_patterns,
            );
            let streams: Vec<ScanStream<'_>> = case
                .streams
                .iter()
                .map(|stream| ScanStream {
                    name: stream.name.as_str(),
                    has_epg_id: !stream.tvg_id.trim().is_empty(),
                })
                .collect();
            let cap = (case.max_name_len > 0).then_some(case.max_name_len);
            let (families, stats) = placeholder_scan::scan_families(streams, &patterns, cap);
            let families: Vec<Value> = families
                .iter()
                .map(|family| {
                    json!({
                        "template": family.template,
                        "count": family.count,
                        "distinct_numbers": family.distinct_numbers,
                        "example": family.example,
                        "with_epg_id": family.with_epg_id,
                        "covered": family.covered,
                        "uncovered": family.uncovered,
                        "suggested": family.suggested,
                    })
                })
                .collect();
            Ok(json!({
                "tool": "scan_with_cap",
                "families": families,
                "stats": {"scanned": stats.scanned, "skipped_long": stats.skipped_long},
            }))
        }
        other => anyhow::bail!("unknown tool {other:?}"),
    }
}
