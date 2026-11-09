//! Owner-only files: the run store, the CSV reports and the directories holding them.
//!
//! The run store keeps every run's rows, AI audit trail and stream health, and a report is
//! what an operator hands to someone else on purpose; neither should be readable by every
//! other account on the host because the umask happened to be `022`. So the store file (and
//! the `-wal`/`-shm` files SQLite creates beside it with the same mode), the reports and
//! their directories lose their group and world bits. Best effort: a filesystem that cannot
//! hold Unix modes (a Windows share, some FUSE mounts) is warned about, never fatal.
//!
//! Unix only; elsewhere every function here is a no-op.

use std::path::Path;

/// A file's permission bits (`0o644`), when the platform has them and the file exists.
pub fn mode(path: &Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .ok()
            .map(|metadata| metadata.permissions().mode() & 0o7777)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Whether anyone but the owner can read `path` (a group or world read bit is set).
pub fn readable_by_others(path: &Path) -> bool {
    mode(path).is_some_and(|mode| mode & 0o044 != 0)
}

/// Drop the group and world bits of `path`, if it exists. Logs when it cannot.
pub fn restrict(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Some(current) = mode(path) else {
            return;
        };
        let wanted = current & !0o077;
        if wanted == current {
            return;
        }
        if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(wanted))
        {
            tracing::warn!(
                "could not make {} owner-only (mode {current:o}): {error}",
                path.display()
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// The run store at `database`: the file, SQLite's `-wal` and `-shm` beside it, and the
/// directory holding them.
pub fn restrict_store(database: &Path) {
    if let Some(parent) = database
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        restrict(parent);
    }
    restrict(database);
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = database.as_os_str().to_owned();
        sidecar.push(suffix);
        restrict(Path::new(&sidecar));
    }
}

/// `create_dir_all`, with every directory it creates made `0700` and the leaf tightened if
/// it already existed.
pub fn create_private_dir_all(directory: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(directory)?;
    restrict(directory);
    Ok(())
}

/// Create (or truncate) `path` for writing, readable by its owner only (`0600`).
pub fn create_private_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    // `mode` only applies to a file the call creates: an older report of the same name
    // keeps its bits unless they are dropped here.
    restrict(path);
    Ok(file)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    #[test]
    fn a_private_file_and_directory_are_owner_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("exports").join("deeper");
        create_private_dir_all(&nested).expect("mkdir");
        assert_eq!(mode(&nested), Some(0o700));
        assert_eq!(mode(&dir.path().join("exports")), Some(0o700));

        let file = nested.join("report.csv");
        std::fs::write(&file, "old").expect("seed");
        set_mode(&file, 0o644);
        drop(create_private_file(&file).expect("create"));
        assert_eq!(
            mode(&file),
            Some(0o600),
            "an existing file is tightened too"
        );
        assert!(!readable_by_others(&file));
    }

    #[test]
    fn the_store_and_its_sidecars_lose_their_group_and_world_bits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("data");
        std::fs::create_dir(&data).expect("mkdir");
        set_mode(&data, 0o755);
        let database = data.join("crosstalk.db");
        for path in [
            database.clone(),
            data.join("crosstalk.db-wal"),
            data.join("crosstalk.db-shm"),
        ] {
            std::fs::write(&path, "x").expect("seed");
            set_mode(&path, 0o664);
        }
        restrict_store(&database);
        assert_eq!(mode(&data), Some(0o700));
        assert_eq!(mode(&database), Some(0o600));
        assert_eq!(mode(&data.join("crosstalk.db-wal")), Some(0o600));
        assert_eq!(mode(&data.join("crosstalk.db-shm")), Some(0o600));
        // Absent sidecars are not an error.
        restrict_store(&dir.path().join("missing").join("x.db"));
    }

    #[test]
    fn the_run_store_is_owner_only_after_open_and_after_a_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("data");
        std::fs::create_dir(&data).expect("mkdir");
        set_mode(&data, 0o755);
        let database = data.join("crosstalk.db");
        let store = crate::store::Store::open(&database).expect("open");
        store
            .start_run("guide", "match_apply", false)
            .expect("write");
        assert_eq!(mode(&data), Some(0o700));
        assert_eq!(mode(&database), Some(0o600));
        for sidecar in ["crosstalk.db-wal", "crosstalk.db-shm"] {
            let path = data.join(sidecar);
            if path.exists() {
                assert_eq!(mode(&path), Some(0o600), "{sidecar}");
            }
        }
        // A second process opening it shared tightens a loosened file again.
        set_mode(&database, 0o644);
        drop(crate::store::Store::open_shared(&database).expect("shared"));
        assert_eq!(mode(&database), Some(0o600));
    }

    #[test]
    fn a_report_and_its_directory_are_owner_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let report = dir.path().join("exports").join("guide_scan.csv");
        crate::csv_report::write_report(&report, &["made".to_string()], &["a"], |writer| {
            writer.write_record(["1"])?;
            Ok(())
        })
        .expect("write");
        assert_eq!(mode(&report), Some(0o600));
        assert_eq!(mode(&dir.path().join("exports")), Some(0o700));
    }
}
