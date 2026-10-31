//! CSV reports: the commented preamble, the writer, and the retention rule.
//!
//! Port of the plugin's export path. Three properties are deliberate and were learned the hard
//! way there:
//!
//!   * every preamble line starts with `#`, and the whole preamble is ASCII, because a
//!     spreadsheet opening the file under another code page turns anything else into mojibake;
//!   * the preamble names the settings by the labels the form shows, so a reader can see what
//!     the run was told to do without opening the config;
//!   * retention only ever considers files this app wrote, because `/data/exports` is shared
//!     with other plugins, and it never deletes the file a run has just written.

use crate::config::CrosstalkConfig;
use crate::subapp::{FieldValue, SettingsSchema, SettingsValues};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Seconds in a day, for the retention arithmetic.
const SECONDS_PER_DAY: u64 = 86_400;

/// The file-name prefix this app owns inside a shared exports directory.
pub const EXPORT_PREFIX: &str = "epg_janitor_";

/// Build the commented preamble that opens every report.
///
/// `results` are `(label, value)` pairs saying what the run DID, printed above the settings
/// because that is what a reader wants first.
pub fn preamble(
    host: &CrosstalkConfig,
    schema: &SettingsSchema,
    values: &SettingsValues,
    report_title: &str,
    channels_processed: usize,
    results: &[(&str, String)],
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "# EPG Janitor v{} - {report_title}",
        env!("CARGO_PKG_VERSION")
    ));
    lines.push(
        "# This file was written by crosstalk. Every line starting with a # describes the run \
         rather than data: tell your spreadsheet to skip comment lines, and read the column \
         headings from the first line that does not start with a #."
            .to_string(),
    );
    lines.push(format!(
        "# Generated: {}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S %Z")
    ));
    lines.push("#".to_string());

    lines.push("# What this run did:".to_string());
    lines.push(format!("#   Channels processed: {channels_processed}"));
    for (label, value) in results {
        lines.push(format!("#   {label}: {value}"));
    }
    lines.push("#".to_string());

    lines.push("# Settings used:".to_string());
    lines.push(format!("#   Channel Databases: {}", render_databases(host)));
    for (label, value) in render_settings(schema, values) {
        lines.push(format!("#   {label}: {value}"));
    }
    lines.push("#".to_string());
    lines
}

/// The settings, rendered under the labels the form shows.
fn render_settings(schema: &SettingsSchema, values: &SettingsValues) -> Vec<(String, String)> {
    let mut rendered = Vec::new();
    for field in schema.editable() {
        let value = values
            .get(field.id)
            .cloned()
            .unwrap_or_else(|| field.default.clone());
        rendered.push((
            field.label.to_string(),
            render_setting(field.id, &field.kind, &value),
        ));
    }
    rendered
}

/// One setting, read the way a person reading the report needs it.
fn render_setting(id: &str, kind: &crate::subapp::FieldKind, value: &FieldValue) -> String {
    match (id, kind) {
        ("automatch_confidence_threshold", _) | ("heal_confidence_threshold", _) => {
            format!("{} out of 100, higher is stricter", value.render())
        }
        ("check_hours", _) => format!("{} hours ahead of now", value.render()),
        ("csv_retention_days", _) => {
            let days: i64 = match value {
                FieldValue::Number(number) => *number as i64,
                _ => 0,
            };
            if days <= 0 {
                "0, so every export is kept".to_string()
            } else {
                format!("{days} days, older exports of this app are deleted after each run")
            }
        }
        ("ai.accept_threshold", _) | ("ai.reject_threshold", _) | ("ai.min_confidence", _) => {
            format!("{} (0 to 1)", value.render())
        }
        (_, crate::subapp::FieldKind::JsonObject) => value.render(),
        _ => {
            let text = value.render();
            // A multi-line textarea would emit continuation lines carrying no `#`, which a
            // spreadsheet reads as data and a reader reads as a broken header.
            if text.trim().is_empty() {
                "(not set)".to_string()
            } else {
                text.split_whitespace().collect::<Vec<_>>().join(" ")
            }
        }
    }
}

fn render_databases(host: &CrosstalkConfig) -> String {
    let databases = host.matching.databases();
    if databases.is_empty() {
        "none".to_string()
    } else {
        databases.join(", ")
    }
}

/// Write a report: preamble, header row, then the rows.
///
/// Rows are written through a callback so a caller can stream them rather than build the whole
/// table first.
pub fn write_report<F>(
    path: &Path,
    preamble_lines: &[String],
    header: &[&str],
    rows: F,
) -> Result<()>
where
    F: FnOnce(&mut csv::Writer<std::fs::File>) -> Result<()>,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // The preamble is written straight to the file, then the csv writer takes it over: a csv
    // writer would quote a comment line containing a comma, and a comment line must stay a
    // comment line.
    {
        use std::io::Write;
        let mut file = std::fs::File::create(path)
            .with_context(|| format!("creating the report {}", path.display()))?;
        let mut buffer = String::new();
        for line in preamble_lines {
            buffer.push_str(line);
            buffer.push_str("\r\n");
        }
        file.write_all(buffer.as_bytes())
            .with_context(|| format!("writing the preamble to {}", path.display()))?;
    }

    let file = std::fs::File::options()
        .append(true)
        .open(path)
        .with_context(|| format!("reopening the report {}", path.display()))?;
    let mut writer = csv::WriterBuilder::new()
        // The plugin's `csv` module wrote CRLF, and every consumer of these files was written
        // against that; keeping it makes the reports byte-comparable with the old ones.
        .terminator(csv::Terminator::CRLF)
        .from_writer(file);
    writer.write_record(header)?;
    rows(&mut writer)?;
    writer.flush()?;
    Ok(())
}

/// A file in the exports directory, as the retention rule sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportFile {
    pub name: String,
    pub modified: SystemTime,
}

/// Whether a file was written by this app.
///
/// One definition, used by both the age rule and any "clear exports" action: when the two
/// decided this separately they disagreed in between.
pub fn is_our_export(name: &str) -> bool {
    name.starts_with(EXPORT_PREFIX) && name.ends_with(".csv")
}

/// Which of this app's exports are old enough to delete.
///
/// Pure, so the tests can drive it without a filesystem. Nothing is deleted when the retention
/// is 0, the file just written is never deleted, and at least one of this app's files always
/// survives so a small number cannot empty the directory.
pub fn exports_to_delete(
    entries: &[ExportFile],
    retention_days: u32,
    now: SystemTime,
    protect: Option<&str>,
) -> Vec<String> {
    if retention_days == 0 {
        return Vec::new();
    }

    let mut mine: Vec<(&str, SystemTime)> = entries
        .iter()
        .filter(|entry| is_our_export(&entry.name))
        .map(|entry| (entry.name.as_str(), entry.modified))
        .collect();
    if mine.is_empty() {
        return Vec::new();
    }

    let survivor = match protect {
        Some(protected) if mine.iter().any(|(name, _)| *name == protected) => Some(protected),
        _ => mine
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)))
            .map(|(name, _)| *name),
    };

    let cutoff = now
        .checked_sub(Duration::from_secs(
            u64::from(retention_days) * SECONDS_PER_DAY,
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);

    mine.sort_by(|a, b| a.0.cmp(b.0));
    mine.into_iter()
        .filter(|(name, modified)| *modified < cutoff && Some(*name) != survivor)
        .map(|(name, _)| name.to_string())
        .collect()
}

/// Delete this app's exports older than `retention_days`, keeping `protect`.
///
/// Never raises for a filesystem problem after a successful export: a failure to tidy up must
/// not turn a completed run into a reported error. Returns how many were removed.
pub fn prune_exports(directory: &Path, retention_days: u32, protect: Option<&str>) -> usize {
    if retention_days == 0 {
        return 0;
    }
    let Ok(entries) = std::fs::read_dir(directory) else {
        return 0;
    };

    let files: Vec<ExportFile> = entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some(ExportFile { name, modified })
        })
        .collect();

    let doomed = exports_to_delete(&files, retention_days, SystemTime::now(), protect);
    let mut removed = 0;
    for name in doomed {
        match std::fs::remove_file(directory.join(&name)) {
            Ok(()) => {
                removed += 1;
                tracing::info!("deleted CSV export older than {retention_days} days: {name}");
            }
            Err(error) => {
                tracing::warn!("could not delete the old CSV export {name}: {error}");
            }
        }
    }
    removed
}

/// The report path for a run: a timestamped file inside the exports directory.
pub fn report_path(exports_dir: &Path, stem: &str) -> PathBuf {
    // Milliseconds as well as seconds: two runs started from the UI in the same second
    // must not overwrite each other's report.
    let stamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f");
    exports_dir.join(format!("{EXPORT_PREFIX}{stem}_{stamp}.csv"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subapp::{FieldKind, FieldSpec};

    fn at(seconds_ago: u64) -> SystemTime {
        SystemTime::now() - Duration::from_secs(seconds_ago)
    }

    #[test]
    fn only_our_own_files_are_considered() {
        assert!(is_our_export(
            "epg_janitor_automatch_preview_20260101_000000.csv"
        ));
        assert!(!is_our_export("other_plugin_export.csv"));
        assert!(!is_our_export("epg_janitor_notes.txt"));
        assert!(!is_our_export("epg_janitor_backup.csv.bak"));
    }

    #[test]
    fn retention_off_keeps_everything() {
        let entries = vec![
            ExportFile {
                name: "epg_janitor_a.csv".to_string(),
                modified: at(90 * 86_400),
            },
            ExportFile {
                name: "epg_janitor_b.csv".to_string(),
                modified: at(1),
            },
        ];
        assert!(exports_to_delete(&entries, 0, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn older_files_are_deleted_and_the_newest_survives() {
        let entries = vec![
            ExportFile {
                name: "epg_janitor_old.csv".to_string(),
                modified: at(30 * 86_400),
            },
            ExportFile {
                name: "epg_janitor_new.csv".to_string(),
                modified: at(1),
            },
        ];
        let doomed = exports_to_delete(&entries, 7, SystemTime::now(), None);
        assert_eq!(doomed, vec!["epg_janitor_old.csv".to_string()]);
    }

    #[test]
    fn the_file_just_written_is_never_deleted() {
        // Both are old; the run's own file is protected even though the age rule would remove
        // it, which is the case that matters after a clock change.
        let entries = vec![
            ExportFile {
                name: "epg_janitor_a.csv".to_string(),
                modified: at(30 * 86_400),
            },
            ExportFile {
                name: "epg_janitor_b.csv".to_string(),
                modified: at(20 * 86_400),
            },
        ];
        let doomed = exports_to_delete(&entries, 7, SystemTime::now(), Some("epg_janitor_b.csv"));
        assert_eq!(doomed, vec!["epg_janitor_a.csv".to_string()]);
    }

    #[test]
    fn at_least_one_file_always_survives() {
        let entries = vec![ExportFile {
            name: "epg_janitor_only.csv".to_string(),
            modified: at(365 * 86_400),
        }];
        assert!(exports_to_delete(&entries, 1, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn other_plugins_files_are_never_touched() {
        let entries = vec![
            ExportFile {
                name: "lineuparr_export.csv".to_string(),
                modified: at(365 * 86_400),
            },
            ExportFile {
                name: "epg_janitor_mine.csv".to_string(),
                modified: at(1),
            },
        ];
        assert!(exports_to_delete(&entries, 7, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn an_empty_directory_deletes_nothing() {
        assert!(exports_to_delete(&[], 7, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn a_future_timestamp_is_not_deleted() {
        let entries = vec![
            ExportFile {
                name: "epg_janitor_future.csv".to_string(),
                modified: SystemTime::now() + Duration::from_secs(86_400),
            },
            ExportFile {
                name: "epg_janitor_newest.csv".to_string(),
                modified: at(1),
            },
        ];
        // The future-dated file is the newest, so it is the survivor, not a deletion.
        let doomed = exports_to_delete(&entries, 7, SystemTime::now(), None);
        assert!(!doomed.contains(&"epg_janitor_future.csv".to_string()));
    }

    #[test]
    fn the_preamble_is_all_comments_and_ascii() {
        let host = CrosstalkConfig::default();
        let schema = SettingsSchema::new(vec![
            FieldSpec::new(
                "check_hours",
                "_s",
                "Hours to Check Ahead",
                FieldKind::Number,
                FieldValue::Number(12.0),
            ),
            FieldSpec::new(
                "automatch_confidence_threshold",
                "_s",
                "Auto-Match Confidence Threshold",
                FieldKind::Number,
                FieldValue::Number(95.0),
            ),
            FieldSpec::new(
                "allow_epg_without_programs",
                "_s",
                "Allow EPG Without Program Data",
                FieldKind::Boolean,
                FieldValue::Boolean(false),
            ),
            FieldSpec::new(
                "selected_groups",
                "_s",
                "Channel Groups",
                FieldKind::Text,
                FieldValue::Text("A\nB".to_string()),
            ),
        ]);
        let mut values = SettingsValues::new();
        values.insert("allow_epg_without_programs", FieldValue::Boolean(true));
        values.insert("selected_groups", FieldValue::Text("A\nB".to_string()));

        let lines = preamble(
            &host,
            &schema,
            &values,
            "Auto-Match Preview Report",
            1200,
            &[("Matched", "900".to_string())],
        );
        for line in &lines {
            assert!(line.starts_with('#'), "not a comment: {line:?}");
            assert!(line.is_ascii(), "not ASCII: {line:?}");
        }
        let joined = lines.join("\n");
        assert!(joined.contains("Channels processed: 1200"));
        assert!(joined.contains("Matched: 900"));
        assert!(joined.contains("Channel Databases: US"));
        assert!(joined.contains("Hours to Check Ahead: 12 hours ahead of now"));
        assert!(
            joined.contains("Auto-Match Confidence Threshold: 95 out of 100, higher is stricter")
        );
        assert!(joined.contains("Allow EPG Without Program Data: Yes"));
        // A multi-line value is flattened so it cannot emit a line with no `#`.
        assert!(joined.contains("Channel Groups: A B"));
    }

    #[test]
    fn the_report_keeps_the_preamble_unquoted_and_the_rows_quoted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("report.csv");
        let preamble = vec![
            "# EPG Janitor - test".to_string(),
            "# A line, with a comma".to_string(),
        ];
        write_report(&path, &preamble, &["channel_name", "score"], |writer| {
            writer.write_record(["ESPN, HD", "100"])?;
            Ok(())
        })
        .expect("write");

        let text = std::fs::read_to_string(&path).expect("read");
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("# EPG Janitor - test"));
        // The comma in a comment must not cause quoting.
        assert_eq!(lines.next(), Some("# A line, with a comma"));
        assert_eq!(lines.next(), Some("channel_name,score"));
        assert_eq!(lines.next(), Some("\"ESPN, HD\",100"));
    }

    #[test]
    fn pruning_removes_old_files_and_keeps_the_protected_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let old = dir.path().join("epg_janitor_old.csv");
        let fresh = dir.path().join("epg_janitor_fresh.csv");
        let foreign = dir.path().join("other_plugin.csv");
        for path in [&old, &fresh, &foreign] {
            std::fs::write(path, "x").expect("write");
        }
        // Backdate the old file and the foreign one past any retention.
        let old_time = SystemTime::now() - Duration::from_secs(90 * 86_400);
        set_modified(&old, old_time);
        set_modified(&foreign, old_time);

        let removed = prune_exports(dir.path(), 7, Some("epg_janitor_fresh.csv"));
        assert_eq!(removed, 1);
        assert!(!old.exists());
        assert!(fresh.exists());
        assert!(
            foreign.exists(),
            "another plugin's file is not ours to delete"
        );
    }

    #[test]
    fn pruning_does_nothing_when_retention_is_off() {
        let dir = tempfile::tempdir().expect("tempdir");
        let old = dir.path().join("epg_janitor_old.csv");
        std::fs::write(&old, "x").expect("write");
        set_modified(&old, SystemTime::now() - Duration::from_secs(90 * 86_400));
        assert_eq!(prune_exports(dir.path(), 0, None), 0);
        assert!(old.exists());
    }

    #[test]
    fn report_paths_carry_the_prefix_and_a_timestamp() {
        let path = report_path(Path::new("/exports"), "automatch_preview");
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .to_string();
        assert!(is_our_export(&name), "{name}");
        assert!(name.contains("automatch_preview"), "{name}");
        assert_eq!(path.parent(), Some(Path::new("/exports")));
    }

    /// Set a file's modification time, for the retention tests.
    fn set_modified(path: &Path, when: SystemTime) {
        let file = std::fs::File::options()
            .write(true)
            .open(path)
            .expect("open for time");
        file.set_modified(when).expect("set mtime");
    }
}
