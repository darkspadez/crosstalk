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
use crate::subapp::{FieldValue, SettingsSchema, SettingsValues, SubApp};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Seconds in a day, for the retention arithmetic.
const SECONDS_PER_DAY: u64 = 86_400;

/// Build the commented preamble that opens every report.
///
/// `title` is the whole first line, comment marker included, built by
/// [`SubApp::report_title`] so the report names the app that wrote it rather than a constant a
/// caller might forget to change. `results` are
/// `(label, value)` pairs saying what the run DID, printed above the settings because that is
/// what a reader wants first.
pub fn preamble(
    host: &CrosstalkConfig,
    title: &str,
    schema: &SettingsSchema,
    values: &SettingsValues,
    channels_processed: usize,
    results: &[(&str, String)],
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(title.to_string());
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
    // The settings every app shares live in the host file, so they come from it rather
    // than from the app's own values: a report that omitted them would say what this app
    // was told but not what it was pointed at.
    for (label, value) in render_settings(
        &crate::config::shared_schema(),
        &crate::config::shared_values(host),
    ) {
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
///
/// The rule is picked by the setting's own name, not its full path: the same option is
/// `retention_days` under `[exports]` and `check_hours` in an app's file, and one wording
/// per meaning is what a reader needs.
fn render_setting(id: &str, kind: &crate::subapp::FieldKind, value: &FieldValue) -> String {
    let name = id.rsplit('.').next().unwrap_or(id);
    match (name, kind) {
        ("match_confidence_threshold", _) | ("heal_confidence_threshold", _) => {
            format!("{} out of 100, higher is stricter", value.render())
        }
        ("check_hours", _) => format!("{} hours ahead of now", value.render()),
        ("retention_days", _) => {
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
        ("accept_threshold", _) | ("reject_threshold", _) | ("min_confidence", _) => {
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

/// One CSV export: the path, the preamble and the header every app repeats.
///
/// An action writes its report in a few lines: build this, say how many channels it looked at
/// and what it did, then hand over the columns and the cells. Everything shared with the other
/// sub-apps lives here — the app's own prefix, the timestamped path, the commented preamble
/// naming the settings, the CRLF writer and the retention tidy-up — so only the columns and the
/// cell contents are the app's own.
pub struct Report<'a> {
    exports_dir: &'a Path,
    app: &'a dyn SubApp,
    stem: &'a str,
    title: &'a str,
    channels_processed: usize,
    results: Vec<(String, String)>,
}

impl<'a> Report<'a> {
    /// A report the app writes to `{exports_dir}/{app.export_prefix()}{stem}_{timestamp}.csv`.
    ///
    /// The app supplies both halves of its identity: the prefix its files carry (so retention
    /// and the host's "clear exports" only ever touch its own files) and the name in the
    /// preamble.
    pub fn new(exports_dir: &'a Path, app: &'a dyn SubApp, stem: &'a str, title: &'a str) -> Self {
        Self {
            exports_dir,
            app,
            stem,
            title,
            channels_processed: 0,
            results: Vec::new(),
        }
    }

    /// How many channels the run looked at.
    pub fn channels_processed(mut self, count: usize) -> Self {
        self.channels_processed = count;
        self
    }

    /// One `Label: value` line saying what the run DID, printed above the settings.
    pub fn result(mut self, label: &str, value: impl std::fmt::Display) -> Self {
        self.results.push((label.to_string(), value.to_string()));
        self
    }

    /// Write the report and return its path.
    ///
    /// Rows go through a callback so a caller can stream them rather than build the whole
    /// table first.
    pub fn write<F>(
        &self,
        host: &CrosstalkConfig,
        schema: &SettingsSchema,
        values: &SettingsValues,
        header: &[&str],
        rows: F,
    ) -> Result<PathBuf>
    where
        F: FnOnce(&mut csv::Writer<std::fs::File>) -> Result<()>,
    {
        let path = report_path(self.exports_dir, &self.app.export_prefix(), self.stem);
        let results: Vec<(&str, String)> = self
            .results
            .iter()
            .map(|(label, value)| (label.as_str(), value.clone()))
            .collect();
        let preamble = preamble(
            host,
            &self.app.report_title(self.title),
            schema,
            values,
            self.channels_processed,
            &results,
        );
        write_report(&path, &preamble, header, rows)?;
        Ok(path)
    }

    /// Delete this app's older exports, keeping the one just written.
    ///
    /// Called after a successful run only: deleting the history before an apply that then
    /// fails would throw it away while producing nothing to replace it.
    pub fn prune(&self, retention_days: u32, written: &Path) -> usize {
        let name = written
            .file_name()
            .map(|name| name.to_string_lossy().to_string());
        prune_exports(
            self.exports_dir,
            &self.app.export_prefix(),
            retention_days,
            name.as_deref(),
        )
    }
}

/// Defuse a spreadsheet formula before it reaches the CSV.
///
/// Channel and guide names come from Dispatcharr, which an operator does not control:
/// a name starting with `=`, `+`, `-` or `@` (or a tab/CR, which some spreadsheets
/// also honour) would execute as a formula when the export is opened. Prefixing such
/// a cell with a single quote keeps the text visible but inert. Note the quote BECOMES
/// part of the parsed CSV value: a spreadsheet hides it as a display instruction, but
/// any other CSV reader sees the quote as the cell's first character. Ordinary cells
/// pass through unchanged.
pub fn safe_cell(text: &str) -> String {
    match text.chars().next() {
        Some('=') | Some('+') | Some('-') | Some('@') | Some('\t') | Some('\r') => {
            format!("'{text}")
        }
        _ => text.to_string(),
    }
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
/// decided this separately they disagreed in between. `prefix` is the app's own, because
/// `/data/exports` is shared with other plugins and, later, with the other sub-apps.
///
/// A single prefix is ambiguous when app ids nest: `guide_extra_probe_….csv` starts with
/// `guide_`, so it matches both apps. Deciding for one app against the full registry needs
/// [`owning_prefix`], which awards the file to the longest match; the registry test pins
/// that no two registered prefixes nest, so the single-prefix age rule stays exact.
pub fn is_our_export(name: &str, prefix: &str) -> bool {
    name.starts_with(prefix) && name.ends_with(".csv")
}

/// Which registered app owns `name`: the longest of `prefixes` that names it, if any.
///
/// App ids nest (`guide` today, a hypothetical `guide_extra` tomorrow), and so do their
/// `id_` file prefixes. A per-app decision — the CLI's `--app` filter, and conceptually
/// each app's retention pass — must use this rather than [`is_our_export`] alone, or `guide`
/// would delete `guide_extra`'s reports. Clearing over the whole registry needs no
/// ownership: the union is deleted whoever owns what.
pub fn owning_prefix<'a>(name: &str, prefixes: &'a [String]) -> Option<&'a str> {
    prefixes
        .iter()
        .map(String::as_str)
        .filter(|prefix| is_our_export(name, prefix))
        .max_by_key(|prefix| prefix.len())
}

/// Which of this app's exports are old enough to delete.
///
/// Pure, so the tests can drive it without a filesystem. Nothing is deleted when the retention
/// is 0, the file just written is never deleted, and at least one of this app's files always
/// survives so a small number cannot empty the directory.
pub fn exports_to_delete(
    entries: &[ExportFile],
    prefix: &str,
    retention_days: u32,
    now: SystemTime,
    protect: Option<&str>,
) -> Vec<String> {
    if retention_days == 0 {
        return Vec::new();
    }

    let mut mine: Vec<(&str, SystemTime)> = entries
        .iter()
        .filter(|entry| is_our_export(&entry.name, prefix))
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
///
/// `prefix` is matched on its own, so it relies on the registry's non-nesting rule (see
/// [`SubApp::export_prefix`]): with no nested id in the registry no other app's file can
/// match it.
pub fn prune_exports(
    directory: &Path,
    prefix: &str,
    retention_days: u32,
    protect: Option<&str>,
) -> usize {
    if retention_days == 0 {
        return 0;
    }
    let files = read_exports(directory);

    let doomed = exports_to_delete(&files, prefix, retention_days, SystemTime::now(), protect);
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

/// Every file in the exports directory, as the retention rule and a "clear" see it.
///
/// Unreadable entries are dropped rather than raised: one file whose metadata cannot be
/// stat'ed must not stop the others from being considered. Directories are skipped: a
/// directory named `guide_old.csv` is not a report, and attempting to delete one would only
/// turn up as a failure in the summary. Names come from `read_dir`, so each is a single
/// path component directly in the directory — there is no `..` to escape with — and deletion
/// uses `remove_file`, which unlinks a symlink itself rather than following it to its target.
fn read_exports(directory: &Path) -> Vec<ExportFile> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            // `file_type` does not follow symlinks, so a symlink to a directory is still
            // a symlink here rather than a directory: only genuine directories are skipped,
            // and a symlink is unlinked, never followed, when its turn comes to be deleted.
            if entry.file_type().ok()?.is_dir() {
                return None;
            }
            // `metadata` follows a symlink for the timestamp; a dangling link has no target
            // to ask, so fall back to the link itself rather than dropping it silently.
            let modified = entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok())
                .or_else(|| {
                    std::fs::symlink_metadata(entry.path())
                        .ok()?
                        .modified()
                        .ok()
                })?;
            Some(ExportFile { name, modified })
        })
        .collect()
}

/// How long a report is protected from a "clear exports" after it was last written.
///
/// The web UI refuses to clear while a run is active, which is exact for the daemon's own
/// runs. A one-shot `crosstalk exports clear` cannot ask the daemon either: the run store must
/// not be opened from a second process, because opening it retires every `running` row. So both
/// paths also keep the reports a run may still be holding — a report is written just before the
/// apply, and the apply can outlast the seconds it takes the run record to say the run is over.
/// This window is a safeguard, not a proof: the run check is what actually guarantees it.
pub const CLEAR_GRACE: Duration = Duration::from_secs(120);

/// The exports one of `prefixes` names, in name order, whatever their age.
///
/// The exports directory is shared with the plugins and, later, with the other sub-apps, so a
/// file is only ever one of ours when a registered app's own prefix names it.
pub fn app_exports(directory: &Path, prefixes: &[String]) -> Vec<ExportFile> {
    let mut files: Vec<ExportFile> = read_exports(directory)
        .into_iter()
        .filter(|file| {
            prefixes
                .iter()
                .any(|prefix| is_our_export(&file.name, prefix))
        })
        .collect();
    files.sort_by(|a, b| a.name.cmp(&b.name));
    files
}

/// Which exports a "clear" deletes: the ones `prefixes` names, except those written within
/// `grace` of `now`.
///
/// Pure, so the tests can drive it without a filesystem. A file dated in the future is kept for
/// the same reason retention keeps it: a clock change is not a licence to delete more.
/// `prefixes` must be the whole registry here: with a single app's prefix a nested id (see
/// [`owning_prefix`]) would over-match, so per-app callers filter by ownership first and use
/// [`grace_doomed`] directly.
pub fn exports_to_clear(
    entries: &[ExportFile],
    prefixes: &[String],
    now: SystemTime,
    grace: Duration,
) -> Vec<String> {
    let mine: Vec<&ExportFile> = entries
        .iter()
        .filter(|entry| {
            prefixes
                .iter()
                .any(|prefix| is_our_export(&entry.name, prefix))
        })
        .collect();
    grace_doomed(&mine, now, grace)
}

/// The names in `entries` last written before `now - grace`, in name order.
///
/// Pure, so per-app callers that already filtered by [`owning_prefix`] share the exact grace
/// rule — and its future-timestamp behaviour — with [`exports_to_clear`] instead of
/// reimplementing it.
pub fn grace_doomed(entries: &[&ExportFile], now: SystemTime, grace: Duration) -> Vec<String> {
    let cutoff = now.checked_sub(grace).unwrap_or(SystemTime::UNIX_EPOCH);
    let mut doomed: Vec<&str> = entries
        .iter()
        .filter(|entry| entry.modified < cutoff)
        .map(|entry| entry.name.as_str())
        .collect();
    doomed.sort_unstable();
    doomed.into_iter().map(str::to_string).collect()
}

/// What a "clear exports" did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClearSummary {
    /// Files removed.
    pub deleted: usize,
    /// The apps' files left alone because a run may still be holding them.
    pub kept_recent: usize,
    /// Files that matched but could not be removed.
    pub failed: usize,
}

/// Delete the registered apps' CSV exports, and nothing else in the directory.
///
/// The host's feature: the plugins each had their own `clear_csv_exports`, and one host action
/// over the registered prefixes covers all of them without a second copy per app. Reports
/// written within [`CLEAR_GRACE`] survive, so a run in progress keeps the file it is about to
/// link. Returns what it did rather than raising: a file removed by someone else in the
/// meantime is not an error the operator needs to see, and the count is what the UI reports.
///
/// `prefixes` must be every registered app's prefix. A run starting concurrently is safe
/// without locking: its report is created after this listing, so it is never among the names
/// deleted below; the grace window additionally keeps what a just-finished run just wrote.
pub fn clear_exports(directory: &Path, prefixes: &[String]) -> ClearSummary {
    let files = app_exports(directory, prefixes);
    let doomed = exports_to_clear(&files, prefixes, SystemTime::now(), CLEAR_GRACE);
    delete_names(directory, files.len(), doomed)
}

/// Delete one app's CSV exports, leaving the other registered apps' files alone.
///
/// The CLI's `--app` filter. Ownership goes to the longest matching prefix (see
/// [`owning_prefix`]), so clearing `guide` never deletes a nested `guide_extra_*` report even
/// though it starts with `guide_`.
pub fn clear_app_exports(directory: &Path, all_prefixes: &[String], prefix: &str) -> ClearSummary {
    let files: Vec<ExportFile> = app_exports(directory, all_prefixes)
        .into_iter()
        .filter(|file| owning_prefix(&file.name, all_prefixes) == Some(prefix))
        .collect();
    let owned: Vec<&ExportFile> = files.iter().collect();
    let doomed = grace_doomed(&owned, SystemTime::now(), CLEAR_GRACE);
    delete_names(directory, files.len(), doomed)
}

/// Unlink `doomed` from `directory` and summarize. `listed` is how many of the apps' files
/// were considered, so the survivors count as kept rather than vanishing from the report.
fn delete_names(directory: &Path, listed: usize, doomed: Vec<String>) -> ClearSummary {
    let mut summary = ClearSummary {
        deleted: 0,
        kept_recent: listed - doomed.len(),
        failed: 0,
    };
    for name in doomed {
        match std::fs::remove_file(directory.join(&name)) {
            Ok(()) => {
                summary.deleted += 1;
                tracing::info!("deleted CSV export {name} on request");
            }
            Err(error) => {
                summary.failed += 1;
                tracing::warn!("could not delete the CSV export {name}: {error}");
            }
        }
    }
    summary
}

/// The report path for a run: a timestamped file inside the exports directory.
pub fn report_path(exports_dir: &Path, prefix: &str, stem: &str) -> PathBuf {
    // Milliseconds as well as seconds: two runs started from the UI in the same second
    // must not overwrite each other's report.
    let stamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f");
    exports_dir.join(format!("{prefix}{stem}_{stamp}.csv"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subapp::{FieldKind, FieldSpec};
    use crate::test_app::StubApp;

    fn at(seconds_ago: u64) -> SystemTime {
        SystemTime::now() - Duration::from_secs(seconds_ago)
    }

    #[test]
    fn formula_cells_are_quoted_and_plain_ones_pass_through() {
        for hostile in ["=cmd", "+1", "-2", "@mention", "\tindented", "\rreturn"] {
            assert_eq!(safe_cell(hostile), format!("'{hostile}"), "{hostile:?}");
        }
        for plain in ["ESPN HD", "007", "", "(not set)", "Yes", "N/A", "'already"] {
            assert_eq!(safe_cell(plain), plain, "{plain:?}");
        }
    }

    #[test]
    fn only_our_own_files_are_considered() {
        assert!(is_our_export(
            "guide_match_preview_20260101_000000.csv",
            "guide_"
        ));
        assert!(!is_our_export("other_plugin_export.csv", "guide_"));
        assert!(!is_our_export("guide_notes.txt", "guide_"));
        assert!(!is_our_export("guide_backup.csv.bak", "guide_"));
    }

    #[test]
    fn retention_off_keeps_everything() {
        let entries = vec![
            ExportFile {
                name: "guide_a.csv".to_string(),
                modified: at(90 * 86_400),
            },
            ExportFile {
                name: "guide_b.csv".to_string(),
                modified: at(1),
            },
        ];
        assert!(exports_to_delete(&entries, "guide_", 0, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn older_files_are_deleted_and_the_newest_survives() {
        let entries = vec![
            ExportFile {
                name: "guide_old.csv".to_string(),
                modified: at(30 * 86_400),
            },
            ExportFile {
                name: "guide_new.csv".to_string(),
                modified: at(1),
            },
        ];
        let doomed = exports_to_delete(&entries, "guide_", 7, SystemTime::now(), None);
        assert_eq!(doomed, vec!["guide_old.csv".to_string()]);
    }

    #[test]
    fn the_file_just_written_is_never_deleted() {
        // Both are old; the run's own file is protected even though the age rule would remove
        // it, which is the case that matters after a clock change.
        let entries = vec![
            ExportFile {
                name: "guide_a.csv".to_string(),
                modified: at(30 * 86_400),
            },
            ExportFile {
                name: "guide_b.csv".to_string(),
                modified: at(20 * 86_400),
            },
        ];
        let doomed = exports_to_delete(
            &entries,
            "guide_",
            7,
            SystemTime::now(),
            Some("guide_b.csv"),
        );
        assert_eq!(doomed, vec!["guide_a.csv".to_string()]);
    }

    #[test]
    fn at_least_one_file_always_survives() {
        let entries = vec![ExportFile {
            name: "guide_only.csv".to_string(),
            modified: at(365 * 86_400),
        }];
        assert!(exports_to_delete(&entries, "guide_", 1, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn other_plugins_files_are_never_touched() {
        let entries = vec![
            ExportFile {
                name: "lineuparr_export.csv".to_string(),
                modified: at(365 * 86_400),
            },
            ExportFile {
                name: "guide_mine.csv".to_string(),
                modified: at(1),
            },
        ];
        assert!(exports_to_delete(&entries, "guide_", 7, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn an_empty_directory_deletes_nothing() {
        assert!(exports_to_delete(&[], "guide_", 7, SystemTime::now(), None).is_empty());
    }

    #[test]
    fn a_future_timestamp_is_not_deleted() {
        let entries = vec![
            ExportFile {
                name: "guide_future.csv".to_string(),
                modified: SystemTime::now() + Duration::from_secs(86_400),
            },
            ExportFile {
                name: "guide_newest.csv".to_string(),
                modified: at(1),
            },
        ];
        // The future-dated file is the newest, so it is the survivor, not a deletion.
        let doomed = exports_to_delete(&entries, "guide_", 7, SystemTime::now(), None);
        assert!(!doomed.contains(&"guide_future.csv".to_string()));
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
                "match_confidence_threshold",
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
            &StubApp::new().report_title("Auto-Match Preview Report"),
            &schema,
            &values,
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
    fn the_preamble_reports_the_settings_that_live_in_the_host_file() {
        // Scope, retention and the AI policy moved to crosstalk.toml, but a reader of the
        // report still needs to see what the run was told to do.
        let mut host = CrosstalkConfig::default();
        host.scope.profiles = "All".to_string();
        host.scope.groups = "Sports".to_string();
        host.exports.retention_days = 30;
        host.ai.verify.enabled = true;
        host.ai.verify.accept_threshold = 0.85;

        let lines = preamble(
            &host,
            &StubApp::new().report_title("Auto-Match Preview Report"),
            &SettingsSchema::new(vec![]),
            &SettingsValues::new(),
            10,
            &[],
        );
        let joined = lines.join("\n");
        assert!(joined.contains("Channel Profile Names: All"), "{joined}");
        assert!(joined.contains("Channel Groups: Sports"), "{joined}");
        assert!(
            joined.contains("Delete CSV Exports Older Than (Days): 30 days"),
            "{joined}"
        );
        assert!(joined.contains("Enable AI Verification: Yes"), "{joined}");
        assert!(
            joined.contains("AI Accept Threshold: 0.85 (0 to 1)"),
            "{joined}"
        );
        for line in &lines {
            assert!(line.starts_with('#'), "not a comment: {line:?}");
        }
    }

    #[test]
    fn the_report_keeps_the_preamble_unquoted_and_the_rows_quoted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("report.csv");
        let preamble = vec![
            "# crosstalk guide - test".to_string(),
            "# A line, with a comma".to_string(),
        ];
        write_report(&path, &preamble, &["channel_name", "score"], |writer| {
            writer.write_record(["ESPN, HD", "100"])?;
            Ok(())
        })
        .expect("write");

        let text = std::fs::read_to_string(&path).expect("read");
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("# crosstalk guide - test"));
        // The comma in a comment must not cause quoting.
        assert_eq!(lines.next(), Some("# A line, with a comma"));
        assert_eq!(lines.next(), Some("channel_name,score"));
        assert_eq!(lines.next(), Some("\"ESPN, HD\",100"));
    }

    #[test]
    fn the_builder_writes_the_path_the_preamble_and_the_rows() {
        // What an app has to do for a report: name it, say what happened, hand over the
        // cells. Everything else is the builder's.
        let dir = tempfile::tempdir().expect("tempdir");
        let app = StubApp::new();
        let report = Report::new(dir.path(), &app, "probe_preview", "Probe Preview Report")
            .channels_processed(4)
            .result("Streams found", 3);
        let path = report
            .write(
                &CrosstalkConfig::default(),
                &SettingsSchema::new(vec![]),
                &SettingsValues::new(),
                &["channel_name"],
                |writer| {
                    writer.write_record([safe_cell("=ESPN")])?;
                    Ok(())
                },
            )
            .expect("write");

        assert!(path.exists(), "{}", path.display());
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .to_string();
        // Both halves of the app's identity come from the app itself: the file prefix and the
        // name in the first preamble line.
        assert!(name.starts_with("stub_probe_preview_"), "{name}");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(
            text.starts_with(&format!(
                "# crosstalk Stub v{} - Probe Preview Report",
                crate::HOST_VERSION
            )),
            "{text}"
        );
        assert!(text.contains("Channels processed: 4"), "{text}");
        assert!(text.contains("Streams found: 3"), "{text}");
        assert!(text.contains("'=ESPN"), "{text}");
    }

    #[test]
    fn pruning_removes_old_files_and_keeps_the_protected_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let old = dir.path().join("guide_old.csv");
        let fresh = dir.path().join("guide_fresh.csv");
        let foreign = dir.path().join("other_plugin.csv");
        for path in [&old, &fresh, &foreign] {
            std::fs::write(path, "x").expect("write");
        }
        // Backdate the old file and the foreign one past any retention.
        let old_time = SystemTime::now() - Duration::from_secs(90 * 86_400);
        set_modified(&old, old_time);
        set_modified(&foreign, old_time);

        let removed = prune_exports(dir.path(), "guide_", 7, Some("guide_fresh.csv"));
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
        let old = dir.path().join("guide_old.csv");
        std::fs::write(&old, "x").expect("write");
        set_modified(&old, SystemTime::now() - Duration::from_secs(90 * 86_400));
        assert_eq!(prune_exports(dir.path(), "guide_", 0, None), 0);
        assert!(old.exists());
    }

    fn prefixes() -> Vec<String> {
        vec!["guide_".to_string(), "streams_".to_string()]
    }

    #[test]
    fn clearing_only_names_the_registered_apps_files() {
        // The directory is shared with the plugins, so an unregistered prefix and a non-CSV
        // file are not ours to delete however old they are.
        let entries = vec![
            ExportFile {
                name: "guide_match_preview_20260101.csv".to_string(),
                modified: at(90 * 86_400),
            },
            ExportFile {
                name: "streams_probe_20260101.csv".to_string(),
                modified: at(90 * 86_400),
            },
            ExportFile {
                name: "lineuparr_export.csv".to_string(),
                modified: at(90 * 86_400),
            },
            ExportFile {
                name: "guide_notes.txt".to_string(),
                modified: at(90 * 86_400),
            },
            ExportFile {
                name: ".keep".to_string(),
                modified: at(90 * 86_400),
            },
        ];
        let doomed = exports_to_clear(&entries, &prefixes(), SystemTime::now(), CLEAR_GRACE);
        assert_eq!(
            doomed,
            vec![
                "guide_match_preview_20260101.csv".to_string(),
                "streams_probe_20260101.csv".to_string(),
            ]
        );
    }

    #[test]
    fn a_report_written_moments_ago_survives_a_clear() {
        // A run writes its report before it applies, so the newest file may still be in use.
        let entries = vec![
            ExportFile {
                name: "guide_just_written.csv".to_string(),
                modified: at(1),
            },
            ExportFile {
                name: "guide_older.csv".to_string(),
                modified: at(3_600),
            },
            // A future timestamp is kept, the way retention keeps it: a clock change is not a
            // licence to delete more.
            ExportFile {
                name: "guide_future.csv".to_string(),
                modified: SystemTime::now() + Duration::from_secs(86_400),
            },
        ];
        let doomed = exports_to_clear(&entries, &prefixes(), SystemTime::now(), CLEAR_GRACE);
        assert_eq!(doomed, vec!["guide_older.csv".to_string()]);
    }

    #[test]
    fn no_registered_prefix_deletes_nothing() {
        let entries = vec![ExportFile {
            name: "guide_old.csv".to_string(),
            modified: at(90 * 86_400),
        }];
        assert!(exports_to_clear(&entries, &[], SystemTime::now(), CLEAR_GRACE).is_empty());
    }

    #[test]
    fn clear_exports_removes_only_the_apps_old_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mine = dir.path().join("guide_old.csv");
        let recent = dir.path().join("streams_recent.csv");
        let foreign = dir.path().join("lineuparr_export.csv");
        let not_csv = dir.path().join("guide_notes.txt");
        for path in [&mine, &recent, &foreign, &not_csv] {
            std::fs::write(path, "x").expect("write");
        }
        let old = SystemTime::now() - Duration::from_secs(90 * 86_400);
        set_modified(&mine, old);
        set_modified(&foreign, old);
        set_modified(&not_csv, old);

        let summary = clear_exports(dir.path(), &prefixes());
        assert_eq!(summary.deleted, 1);
        assert_eq!(summary.kept_recent, 1);
        assert_eq!(summary.failed, 0);
        assert!(!mine.exists());
        assert!(recent.exists(), "a report a run may still hold survives");
        assert!(foreign.exists(), "another program's file is untouched");
        assert!(not_csv.exists(), "only .csv files are exports");
    }

    #[test]
    fn ownership_goes_to_the_longest_matching_prefix() {
        // `guide_extra_probe_….csv` starts with `guide_` too: a lone prefix match cannot tell
        // the two apps apart, so per-app decisions ask who owns the file instead.
        let prefixes = vec!["guide_".to_string(), "guide_extra_".to_string()];
        assert_eq!(
            owning_prefix("guide_extra_probe_20260101.csv", &prefixes),
            Some("guide_extra_")
        );
        assert_eq!(
            owning_prefix("guide_match_preview_20260101.csv", &prefixes),
            Some("guide_")
        );
        assert_eq!(owning_prefix("lineuparr_export.csv", &prefixes), None);
        assert_eq!(owning_prefix("guide_notes.txt", &prefixes), None);
    }

    #[test]
    fn clearing_one_app_leaves_a_nested_app_alone() {
        // The `--app guide` filter with a `guide_extra` app registered: both files match
        // `guide_` as strings, but only one belongs to it.
        let dir = tempfile::tempdir().expect("tempdir");
        let all = vec!["guide_".to_string(), "guide_extra_".to_string()];
        let mine = dir.path().join("guide_old.csv");
        let nested = dir.path().join("guide_extra_old.csv");
        for path in [&mine, &nested] {
            std::fs::write(path, "x").expect("write");
            set_modified(path, SystemTime::now() - Duration::from_secs(90 * 86_400));
        }

        let summary = clear_app_exports(dir.path(), &all, "guide_");
        assert_eq!(summary.deleted, 1);
        assert!(!mine.exists(), "the app's own report is deleted");
        assert!(
            nested.exists(),
            "the nested app's report survives a single-app clear"
        );
    }

    #[test]
    fn a_directory_named_like_a_report_is_not_an_export() {
        // Only regular files directly in the directory are exports: a directory that happens
        // to end in `.csv` is skipped rather than attempted and counted as a failure.
        let dir = tempfile::tempdir().expect("tempdir");
        let impostor = dir.path().join("guide_old.csv");
        std::fs::create_dir(&impostor).expect("mkdir");

        assert!(app_exports(dir.path(), &prefixes()).is_empty());
        assert_eq!(
            clear_exports(dir.path(), &prefixes()),
            ClearSummary::default()
        );
        assert!(impostor.is_dir(), "the directory is left alone");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_is_unlinked_never_followed() {
        // A symlink named `guide_*.csv` that points outside the exports directory: clearing
        // must at most unlink the link itself, never delete the target's contents.
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = dir.path().join("precious.csv");
        std::fs::write(&outside, "do not delete").expect("write");
        let link = dir.path().join("exports").join("guide_link.csv");
        std::fs::create_dir_all(link.parent().expect("parent")).expect("mkdir");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");
        let old = link.parent().expect("parent").join("guide_old.csv");
        std::fs::write(&old, "x").expect("write");
        set_modified(&old, SystemTime::now() - Duration::from_secs(90 * 86_400));

        let summary = clear_exports(link.parent().expect("parent"), &prefixes());
        assert_eq!(summary.deleted, 1);
        assert!(!old.exists());
        assert_eq!(
            std::fs::read_to_string(&outside).expect("target readable"),
            "do not delete",
            "the symlink's target outside the exports directory is untouched"
        );
    }

    #[test]
    fn clearing_an_absent_exports_directory_is_harmless() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("never-created");
        assert_eq!(
            clear_exports(&missing, &prefixes()),
            ClearSummary::default()
        );
        assert!(app_exports(&missing, &prefixes()).is_empty());
    }

    #[test]
    fn report_paths_carry_the_prefix_and_a_timestamp() {
        let path = report_path(Path::new("/exports"), "guide_", "match_preview");
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .to_string();
        assert!(is_our_export(&name, "guide_"), "{name}");
        assert!(name.contains("match_preview"), "{name}");
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
