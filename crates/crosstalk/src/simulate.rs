//! `crosstalk simulate-streams`: the offline stream-to-channel match simulator.
//!
//! Port of `tools/match_sim.py` from the Python plugin. Its purpose is the parity oracle:
//! given the same channels CSV and provider M3U, the per-stream table must be identical
//! to the Python tool's. It is also useful on its own for tuning thresholds without
//! touching a live Dispatcharr.

use anyhow::{bail, Context, Result};
use crosstalk_matching::alias::AliasMap;
use crosstalk_matching::callsign::KnownCallsigns;
use crosstalk_matching::data::known_callsigns_for;
use crosstalk_matching::normalize::NormalizeFlags;
use crosstalk_matching::pipeline::{CandidateIndex, MatchOptions, Matcher};
use fancy_regex::Regex as FancyRegex;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One parsed `#EXTINF` line.
#[derive(Debug, Clone, Default)]
pub struct StreamEntry {
    pub display_name: String,
    pub tvg_name: String,
    pub group_title: String,
}

impl StreamEntry {
    /// The name used as the query: the display name, falling back to `tvg-name`.
    fn query_name(&self) -> &str {
        if self.display_name.is_empty() {
            &self.tvg_name
        } else {
            &self.display_name
        }
    }
}

/// Parse the `#EXTINF` lines of an M3U.
///
/// Only the two attributes the simulator uses (`tvg-name`, `group-title`) are read; the
/// plugin's parser keeps `tvg-id` as well, which nothing here needs.
pub fn parse_m3u(text: &str) -> Vec<StreamEntry> {
    let text = text.trim_start_matches('\u{feff}');
    let name_re = FancyRegex::new(r"^#EXTINF:[^,]*,([\s\S]*)$").expect("static regex");
    let attr_re = FancyRegex::new(r#"([\w-]+)="([^"]*)""#).expect("static regex");

    let mut entries = Vec::new();
    for line in crosstalk_matching::pyre::py_splitlines(text) {
        let line = crosstalk_matching::pyre::py_trim(line);
        if !line.starts_with("#EXTINF:") {
            continue;
        }
        let Some(display) = name_re
            .captures(line)
            .ok()
            .flatten()
            .and_then(|caps| caps.get(1))
            .map(|m| crosstalk_matching::pyre::py_trim(m.as_str()).to_string())
        else {
            continue;
        };

        let mut tvg_name = String::new();
        let mut group_title = String::new();
        for caps in attr_re.captures_iter(line).flatten() {
            let (Some(key), Some(value)) = (caps.get(1), caps.get(2)) else {
                continue;
            };
            match key.as_str() {
                "tvg-name" => tvg_name = value.as_str().to_string(),
                "group-title" => group_title = value.as_str().to_string(),
                _ => {}
            }
        }
        entries.push(StreamEntry {
            display_name: display,
            tvg_name,
            group_title,
        });
    }
    entries
}

const CHANNEL_COL_CANDIDATES: [&str; 3] = ["channel_name", "channel name", "name"];

/// Read channel names out of an export CSV, skipping the `#` comment preamble and
/// de-duplicating.
pub fn parse_channels_csv(text: &str, channel_col: Option<&str>) -> Result<Vec<String>> {
    let text = text.trim_start_matches('\u{feff}');
    // `splitlines()` semantics, then re-joined with `\n`, because the plugin feeds a
    // rebuilt string to `csv.DictReader`.
    let lines: Vec<&str> = crosstalk_matching::pyre::py_splitlines(text)
        .into_iter()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect();
    if lines.first().is_some_and(|line| line.trim().is_empty()) {
        // Python hands the blank line to `DictReader` as the header, which leaves no
        // columns, so the column autodetect fails and the tool exits 2.
        bail!("CSV has no non-comment rows");
    }
    let data: String = lines.join("\n");
    if data.trim().is_empty() {
        bail!("CSV has no non-comment rows");
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(data.as_bytes());
    let headers = reader.headers()?.clone();

    let column = match channel_col {
        Some(requested) => {
            if !headers.iter().any(|header| header == requested) {
                bail!(
                    "--channel-col {requested:?} not in CSV headers: {:?}",
                    headers.iter().collect::<Vec<_>>()
                );
            }
            requested.to_string()
        }
        None => headers
            .iter()
            .find(|header| CHANNEL_COL_CANDIDATES.contains(&header.trim().to_lowercase().as_str()))
            .map(str::to_string)
            .with_context(|| {
                format!(
                    "could not auto-detect a channel-name column in {:?}; pass --channel-col",
                    headers.iter().collect::<Vec<_>>()
                )
            })?,
    };
    // Python's DictReader keeps the LAST column when a header repeats, because it zips
    // the names into a dict.
    let index = headers
        .iter()
        .enumerate()
        .filter(|(_, header)| *header == column)
        .map(|(position, _)| position)
        .last()
        .expect("column exists");

    let mut seen = std::collections::HashSet::new();
    let mut names = Vec::new();
    for record in reader.records() {
        let record = record?;
        let name = crosstalk_matching::pyre::py_trim(record.get(index).unwrap_or("")).to_string();
        if name.is_empty() || !seen.insert(name.clone()) {
            continue;
        }
        names.push(name);
    }
    Ok(names)
}

/// Per-stream results: `stream -> [(channel, score, method), ...]`, best first.
pub type PerStream = HashMap<String, Vec<(String, u8, String)>>;

/// Run the simulation.
///
/// Channels are the query side, because the alias table is keyed by canonical channel
/// names; results are inverted into a per-stream view. The candidate side is the stream
/// list alone, which is exactly the `candidate_names` the Python tool passes; its
/// `precompute_normalizations(channels + streams)` call only warms a name-keyed cache, and
/// the channel half of that cache is never read.
pub fn simulate(
    channel_names: &[String],
    stream_names: &[String],
    matcher: &Matcher,
    user_ignored_tags: &[&str],
) -> PerStream {
    let index = CandidateIndex::build(stream_names, matcher.normalizer(), matcher.known());

    // Score every channel in parallel (P4): matching is pure, so each channel's ranked
    // list is independent. The merge below stays serial in channel-iteration order, which
    // is what keeps the output byte-identical to the sequential run (and to the Python
    // tool): ties keep insertion order, which is how the Python tool's stable sort
    // resolves them, so a HashMap here would shuffle tied rows between runs.
    // `par_iter` preserves order on collect, so `per_channel[i]` is channel `i`'s list.
    let per_channel: Vec<Vec<crosstalk_matching::pipeline::Ranked>> = {
        use rayon::prelude::*;
        channel_names
            .par_iter()
            .map(|channel| matcher.match_all_streams_all(channel, &index, user_ignored_tags))
            .collect()
    };

    // Per stream, the channels that matched it, in channel-iteration order.
    let mut accumulated: Vec<Vec<(String, u8, String)>> = vec![Vec::new(); stream_names.len()];
    // stream position -> channel -> position in that stream's row list
    let mut lookup: Vec<HashMap<&str, usize>> = vec![HashMap::new(); stream_names.len()];

    for (channel, entries) in channel_names.iter().zip(per_channel.iter()) {
        for entry in entries {
            if entry.index >= accumulated.len() {
                continue;
            }
            let rows = &mut accumulated[entry.index];
            let seen = &mut lookup[entry.index];
            match seen.get(channel.as_str()) {
                Some(position) => {
                    if entry.score > rows[*position].1 {
                        rows[*position].1 = entry.score;
                        rows[*position].2 = entry.method.label();
                    }
                }
                None => {
                    seen.insert(channel.as_str(), rows.len());
                    rows.push((channel.clone(), entry.score, entry.method.label()));
                }
            }
        }
    }

    let mut per_stream = HashMap::with_capacity(stream_names.len());
    for (position, stream_name) in stream_names.iter().enumerate() {
        let mut rows = std::mem::take(&mut accumulated[position]);
        // Stable, like the Python `rows.sort(key=score, reverse=True)`.
        rows.sort_by_key(|row| std::cmp::Reverse(row.1));
        per_stream.insert(stream_name.clone(), rows);
    }
    per_stream
}

/// Options for one simulation run.
pub struct SimulateOptions {
    pub channels: PathBuf,
    pub streams: PathBuf,
    pub threshold: u8,
    pub top_n: usize,
    pub margin: u8,
    pub channel_col: Option<String>,
    pub group_filter: Option<String>,
    pub custom_aliases: Option<String>,
    pub countries: Option<String>,
    pub out: PathBuf,
    /// Which `user_ignored_tags` the run passes, in the plugin's order:
    /// `[quality, regional, geographic, misc]`.
    pub tags: [bool; 4],
}

/// Read inputs, run the simulation and write the five report files.
pub fn run(options: &SimulateOptions) -> Result<PathBuf> {
    let channels_text = std::fs::read_to_string(&options.channels)
        .with_context(|| format!("reading {}", options.channels.display()))?;
    let streams_text = std::fs::read_to_string(&options.streams)
        .with_context(|| format!("reading {}", options.streams.display()))?;

    let channel_names = parse_channels_csv(&channels_text, options.channel_col.as_deref())?;
    let mut entries = parse_m3u(&streams_text);

    if let Some(pattern) = options.group_filter.as_deref() {
        let regex = FancyRegex::new(&crosstalk_matching::pyre::pythonize(pattern))
            .with_context(|| format!("invalid --group-filter regex {pattern:?}"))?;
        entries.retain(|entry| regex.is_match(&entry.group_title).unwrap_or(false));
    }

    let mut stream_names: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for entry in &entries {
        let name = crosstalk_matching::pyre::py_trim(entry.query_name()).to_string();
        if name.is_empty() || !seen.insert(name.clone()) {
            continue;
        }
        stream_names.push(name);
    }

    if channel_names.is_empty() {
        bail!("no channel names parsed");
    }
    if stream_names.is_empty() {
        bail!("no stream names after parsing/filter");
    }

    // Custom aliases MERGE over the built-in table, replacing per key; they do not
    // replace it. A file with one entry must still keep the other 200-odd entries.
    let alias_map = match options.custom_aliases.as_deref() {
        Some(path) if !path.trim().is_empty() => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading custom aliases {path}"))?;
            let text = text.trim_start_matches('\u{feff}');
            match AliasMap::overrides_from_json(text) {
                Some(overrides) => AliasMap::builtin().with_overrides(overrides),
                None => AliasMap::builtin(),
            }
        }
        _ => AliasMap::builtin(),
    };

    let known = match options.countries.as_deref() {
        Some(list) if !list.trim().is_empty() => {
            let codes: Vec<&str> = list
                .split(',')
                .map(str::trim)
                .filter(|code| !code.is_empty())
                .collect();
            known_callsigns_for(&codes)
        }
        // No channel databases named: the shipped FCC allowlist alone, as in the tool.
        _ => KnownCallsigns::default(),
    };
    let mut merged = KnownCallsigns::shipped().cloned().unwrap_or_default();
    for callsign in known_iter(&known) {
        merged.insert(&callsign);
    }

    // The plugin's `FuzzyMatcher` keeps its `ignore_*` attributes at their defaults; the
    // `--no-ignore-*` flags only change the `user_ignored_tags` list handed to
    // `match_all_streams`. Passing them as NormalizeFlags would change normalisation
    // itself and diverge from the tool on every non-default run.
    let matcher = Matcher::new(
        NormalizeFlags::default(),
        alias_map.clone(),
        Arc::new(merged),
        MatchOptions {
            match_threshold: options.threshold,
        },
    );

    let tags = tag_list(&options.tags);
    let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
    let per_stream = simulate(&channel_names, &stream_names, &matcher, &tag_refs);

    write_reports(
        options,
        &per_stream,
        &channel_names,
        &stream_names,
        &alias_map,
        &tags,
    )
}

/// The plugin orders its ignored tags `quality, regional, geographic, misc`.
fn tag_list(enabled: &[bool; 4]) -> Vec<String> {
    let names = ["quality", "regional", "geographic", "misc"];
    let mut tags = Vec::new();
    for (index, name) in names.iter().enumerate() {
        if enabled[index] {
            tags.push((*name).to_string());
        }
    }
    tags
}

/// Iterate a `KnownCallsigns` without exposing its internals.
fn known_iter(known: &KnownCallsigns) -> Vec<String> {
    known.to_vec()
}

fn write_reports(
    options: &SimulateOptions,
    per_stream: &PerStream,
    channel_names: &[String],
    stream_names: &[String],
    alias_map: &AliasMap,
    tags: &[String],
) -> Result<PathBuf> {
    let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S").to_string();
    let out_dir = options.out.join(stamp);
    std::fs::create_dir_all(&out_dir).with_context(|| format!("creating {}", out_dir.display()))?;

    // Python's `csv` module terminates rows with CRLF by default, so the reports match it
    // byte for byte; the parity check compares files with `cmp`.
    let writer = |name: &str| -> Result<csv::Writer<std::fs::File>> {
        Ok(csv::WriterBuilder::new()
            .terminator(csv::Terminator::CRLF)
            .from_path(out_dir.join(name))?)
    };
    let mut matches = writer("per_stream_matches.csv")?;
    let mut unmatched = writer("unmatched.csv")?;
    let mut low = writer("low_confidence.csv")?;
    let mut top = writer("top_n.csv")?;

    matches.write_record([
        "stream_name",
        "best_channel",
        "score",
        "match_type",
        "threshold",
        "passed",
    ])?;
    unmatched.write_record(["stream_name"])?;
    low.write_record(["stream_name", "best_channel", "score", "match_type"])?;
    top.write_record(["stream_name", "rank", "candidate", "score", "match_type"])?;

    let mut passed_count = 0usize;
    let mut low_count = 0usize;
    let mut unmatched_count = 0usize;
    let mut type_counts: std::collections::BTreeMap<String, usize> = Default::default();
    let mut histogram: std::collections::BTreeMap<u8, usize> = Default::default();

    for stream_name in stream_names {
        let rows = per_stream.get(stream_name).cloned().unwrap_or_default();
        let Some(best) = rows.first() else {
            unmatched_count += 1;
            matches.write_record([
                stream_name.as_str(),
                "",
                "",
                "",
                &options.threshold.to_string(),
                "False",
            ])?;
            unmatched.write_record([stream_name.as_str()])?;
            continue;
        };

        let (channel, score, method) = best;
        let passed = *score >= options.threshold;
        matches.write_record([
            stream_name.as_str(),
            channel.as_str(),
            &score.to_string(),
            method.as_str(),
            &options.threshold.to_string(),
            if passed { "True" } else { "False" },
        ])?;
        *type_counts.entry(method.clone()).or_insert(0) += 1;
        *histogram.entry((*score / 10).min(10) * 10).or_insert(0) += 1;

        if passed {
            passed_count += 1;
        } else if *score >= options.threshold.saturating_sub(options.margin) {
            low_count += 1;
            low.write_record([
                stream_name.as_str(),
                channel.as_str(),
                &score.to_string(),
                method.as_str(),
            ])?;
        }

        for (rank, (candidate, candidate_score, candidate_method)) in
            rows.iter().take(options.top_n).enumerate()
        {
            top.write_record([
                stream_name.as_str(),
                &(rank + 1).to_string(),
                candidate.as_str(),
                &candidate_score.to_string(),
                candidate_method.as_str(),
            ])?;
        }
    }
    matches.flush()?;
    unmatched.flush()?;
    low.flush()?;
    top.flush()?;

    let total = per_stream.len();
    let matched = total.saturating_sub(unmatched_count);
    let rate = if total == 0 {
        0.0
    } else {
        matched as f64 / total as f64 * 100.0
    };

    let by_type = type_counts
        .iter()
        .map(|(method, count)| format!("{method}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let hist = histogram
        .iter()
        .map(|(bucket, count)| format!("{bucket}-{}:{count}", bucket + 9))
        .collect::<Vec<_>>()
        .join(", ");

    let summary = format!(
        "Total streams: {total}\n\
         Matched (>=1 candidate): {matched} ({rate:.1}%)\n\
         Passed threshold ({}): {passed_count}\n\
         Low confidence [{},{}): {low_count}\n\
         Unmatched: {unmatched_count}\n\
         \n\
         By match_type: {by_type}\n\
         Score histogram (10-pt buckets): {hist}\n\
         \n\
         Run config: alias_entries={}, channels={}, ignored_tags={}, margin={}, streams={}, threshold={}, top_n={}\n",
        options.threshold,
        i32::from(options.threshold) - i32::from(options.margin),
        options.threshold,
        alias_map.len(),
        channel_names.len(),
        if tags.is_empty() {
            "none".to_string()
        } else {
            tags.join("|")
        },
        options.margin,
        stream_names.len(),
        options.threshold,
        options.top_n,
    );
    let mut summary_file = std::fs::File::create(out_dir.join("summary.txt"))?;
    summary_file.write_all(summary.as_bytes())?;

    Ok(out_dir)
}

/// Resolve a path relative to the current directory, for error messages.
pub fn display(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_matching::normalize::NormalizeFlags;

    #[test]
    fn m3u_parsing_reads_display_name_and_attributes() {
        let text = "\u{feff}#EXTM3U\n\
                    #EXTINF:-1 tvg-id=\"x\" tvg-name=\"ESPN HD\" group-title=\"Sports\",ESPN\n\
                    http://example.invalid/1\n\
                    #EXTINF:-1,Plain Name\n\
                    http://example.invalid/2\n\
                    not an entry\n";
        let entries = parse_m3u(text);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].display_name, "ESPN");
        assert_eq!(entries[0].tvg_name, "ESPN HD");
        assert_eq!(entries[0].group_title, "Sports");
        // An empty display name falls back to `tvg-name`, and then to an empty query.
        assert_eq!(entries[1].display_name, "Plain Name");
        assert_eq!(entries[1].query_name(), "Plain Name");
    }

    #[test]
    fn m3u_query_name_falls_back_to_tvg_name() {
        let entry = StreamEntry {
            display_name: String::new(),
            tvg_name: "Fallback".to_string(),
            group_title: String::new(),
        };
        assert_eq!(entry.query_name(), "Fallback");
    }

    #[test]
    fn channels_csv_skips_comments_and_detects_the_column() {
        let text = "# a preamble line\n\
                    channel_id,channel_name,score\n\
                    1,ESPN,0\n\
                    2,CNN,0\n\
                    3,CNN,0\n\
                    4,   ,0\n";
        let names = parse_channels_csv(text, None).expect("parses");
        assert_eq!(names, vec!["ESPN".to_string(), "CNN".to_string()]);
    }

    #[test]
    fn channels_csv_honours_an_explicit_column() {
        let text = "custom,other\nA,B\n";
        assert_eq!(
            parse_channels_csv(text, Some("other")).expect("parses"),
            vec!["B".to_string()]
        );
        assert!(parse_channels_csv(text, Some("missing")).is_err());
        assert!(parse_channels_csv("no headers\n", None).is_err());
    }

    #[test]
    fn simulate_inverts_and_orders_results() {
        let matcher = Matcher::new(
            NormalizeFlags::default(),
            AliasMap::builtin(),
            std::sync::Arc::new(crosstalk_matching::callsign::KnownCallsigns::default()),
            MatchOptions::default(),
        );
        let channels = vec!["ESPN".to_string(), "CNN".to_string()];
        let streams = vec!["ESPN HD".to_string(), "Unrelated".to_string()];
        let per_stream = simulate(&channels, &streams, &matcher, &[]);

        let espn = per_stream.get("ESPN HD").expect("stream present");
        assert_eq!(espn[0].0, "ESPN");
        assert_eq!(espn[0].1, 100);
        assert!(per_stream
            .get("Unrelated")
            .expect("stream present")
            .is_empty());
    }

    #[test]
    fn tag_list_follows_the_flag_order_the_plugin_uses() {
        assert_eq!(
            tag_list(&[true, true, true, true]),
            vec!["quality", "regional", "geographic", "misc"]
        );
        assert_eq!(
            tag_list(&[false, true, false, true]),
            vec!["regional", "misc"]
        );
        assert!(tag_list(&[false, false, false, false]).is_empty());
    }

    #[test]
    fn splitlines_follows_python() {
        let lines = crosstalk_matching::pyre::py_splitlines("a\rb\nc\r\nd\u{2028}e");
        assert_eq!(lines, vec!["a", "b", "c", "d", "e"]);
    }
}
