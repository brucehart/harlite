use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rusqlite::{Connection, DatabaseName, OpenFlags};

use crate::error::{HarliteError, Result};

/// Controls whether database-supplied blob paths may be accessed.
///
/// Paths are disabled by default. When enabled, both relative and absolute
/// paths must resolve beneath a canonical root directory.
#[derive(Clone, Debug)]
pub struct ExternalPathPolicy {
    root: Option<PathBuf>,
}

impl ExternalPathPolicy {
    pub fn new(
        database: &Path,
        allow_external_paths: bool,
        external_path_root: Option<&Path>,
    ) -> Result<Self> {
        if !allow_external_paths {
            return Ok(Self { root: None });
        }

        let root = external_path_root
            .map(Path::to_path_buf)
            .or_else(|| {
                database.parent().and_then(|parent| {
                    (!parent.as_os_str().is_empty()).then(|| parent.to_path_buf())
                })
            })
            .unwrap_or_else(|| PathBuf::from("."));
        let root = fs::canonicalize(&root).map_err(|err| {
            HarliteError::InvalidArgs(format!(
                "External path root could not be resolved ({}): {err}",
                root.display()
            ))
        })?;
        if !root.is_dir() {
            return Err(HarliteError::InvalidArgs(format!(
                "External path root is not a directory: {}",
                root.display()
            )));
        }

        Ok(Self { root: Some(root) })
    }

    pub fn resolve_file(&self, raw_path: &str) -> Option<PathBuf> {
        let root = self.root.as_ref()?;
        let candidate = PathBuf::from(raw_path);
        let candidate = if candidate.is_absolute() {
            candidate
        } else {
            root.join(candidate)
        };
        let resolved = fs::canonicalize(candidate).ok()?;
        if resolved.starts_with(root) && resolved.is_file() {
            Some(resolved)
        } else {
            None
        }
    }
}

/// Copy a live SQLite database, including committed WAL content, via SQLite's
/// online backup API rather than a raw filesystem copy.
pub fn copy_database_consistent(source: &Path, destination: &Path) -> Result<()> {
    let source_conn = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source_conn.backup(DatabaseName::Main, destination, None)?;
    Ok(())
}

static NEXT_STAGED_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

/// A consistent database copy staged beside its final destination.
///
/// The staged database and its sidecars are removed on error or early return.
/// Publishing uses a same-directory rename so callers never expose an
/// unredacted source copy at the requested output path.
pub struct StagedDatabase {
    path: PathBuf,
    destination: PathBuf,
    overwrite: bool,
    published: bool,
}

impl StagedDatabase {
    pub fn copy_from(source: &Path, destination: &Path, overwrite: bool) -> Result<Self> {
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let file_name = destination
            .file_name()
            .ok_or_else(|| HarliteError::InvalidArgs("Output path must be a file".to_string()))?
            .to_string_lossy();

        let path = loop {
            let id = NEXT_STAGED_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
            let candidate = parent.join(format!(
                ".{file_name}.harlite-stage-{}-{id}",
                std::process::id()
            ));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&candidate)
            {
                Ok(file) => {
                    drop(file);
                    break candidate;
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        };

        let staged = Self {
            path,
            destination: destination.to_path_buf(),
            overwrite,
            published: false,
        };
        copy_database_consistent(source, &staged.path)?;
        Ok(staged)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn publish(mut self) -> Result<PathBuf> {
        if self.destination.exists() && !self.overwrite {
            return Err(HarliteError::InvalidArgs(format!(
                "Output database already exists: {} (use --force to overwrite)",
                self.destination.display()
            )));
        }

        remove_database_sidecars(&self.path)?;
        remove_database_sidecars(&self.destination)?;
        if let Err(err) = fs::rename(&self.path, &self.destination) {
            // Windows does not replace an existing file with rename. Preserve
            // atomic replacement where the platform supports it, then fall
            // back to remove-and-rename only for an explicit overwrite.
            if self.overwrite && self.destination.exists() {
                fs::remove_file(&self.destination)?;
                fs::rename(&self.path, &self.destination)?;
            } else {
                return Err(err.into());
            }
        }
        self.published = true;
        Ok(self.destination.clone())
    }
}

impl Drop for StagedDatabase {
    fn drop(&mut self) {
        if !self.published {
            let _ = remove_database_with_sidecars(&self.path);
        }
    }
}

pub fn remove_database_with_sidecars(database: &Path) -> Result<()> {
    if database.exists() {
        fs::remove_file(database)?;
    }
    remove_database_sidecars(database)
}

fn remove_database_sidecars(database: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = database.as_os_str().to_os_string();
        sidecar.push(suffix);
        let sidecar = PathBuf::from(sidecar);
        if sidecar.exists() {
            fs::remove_file(sidecar)?;
        }
    }
    Ok(())
}

/// Remove blobs and FTS rows no longer referenced by any entry.
pub fn delete_orphaned_blobs(conn: &Connection) -> Result<usize> {
    let has_fts: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='response_body_fts')",
        [],
        |row| row.get(0),
    )?;
    if has_fts {
        conn.execute(
            "DELETE FROM response_body_fts WHERE NOT EXISTS (SELECT 1 FROM entries WHERE response_body_hash = response_body_fts.hash)",
            [],
        )?;
    }
    Ok(conn.execute(
        "DELETE FROM blobs WHERE NOT EXISTS (SELECT 1 FROM entries WHERE request_body_hash = blobs.hash OR response_body_hash = blobs.hash OR response_body_hash_raw = blobs.hash)",
        [],
    )?)
}

/// Configure deletion overwrites before sensitive writes and compact the final
/// database so superseded values are not retained in free pages or WAL files.
pub fn prepare_sensitive_write(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA secure_delete=ON;")?;
    Ok(())
}

pub fn finalize_sensitive_write(conn: &Connection) -> Result<()> {
    delete_orphaned_blobs(conn)?;
    conn.execute_batch(
        "PRAGMA wal_checkpoint(TRUNCATE); VACUUM; PRAGMA wal_checkpoint(TRUNCATE);",
    )?;
    Ok(())
}

pub fn canonicalize_path_for_compare(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(fs::canonicalize(path)?);
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent_canon = fs::canonicalize(parent)?;
    let name = path.file_name().ok_or_else(|| {
        HarliteError::InvalidArgs("Output path must be a file".to_string())
    })?;
    Ok(parent_canon.join(name))
}

pub fn resolve_database(database: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(db) = database {
        return Ok(db);
    }

    resolve_database_in_dir(Path::new("."))
}

/// Parse a timestamp from various formats into a `DateTime<Utc>`.
///
/// Supports:
/// - RFC3339 format (e.g., "2024-01-15T10:30:00Z")
/// - Date format (e.g., "2024-01-15")
/// - Unix timestamp in seconds (e.g., 1705315800)
/// - Unix timestamp in milliseconds (e.g., 1705315800000)
///
/// Returns `None` if the input cannot be parsed.
pub fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(parsed.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }
    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(num) = trimmed.parse::<i64>() {
            return parse_timestamp_number(num);
        }
    }
    None
}

/// Parse a Unix timestamp (seconds or milliseconds) into a `DateTime<Utc>`.
pub fn parse_timestamp_number(value: i64) -> Option<DateTime<Utc>> {
    let dt = if value >= 1_000_000_000_000 {
        Utc.timestamp_millis_opt(value).single()?
    } else {
        Utc.timestamp_opt(value, 0).single()?
    };
    Some(dt)
}

pub fn parse_cert_expiry(value: &str) -> Option<DateTime<Utc>> {
    parse_timestamp(value)
}

fn resolve_database_in_dir(dir: &Path) -> Result<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("db") {
            continue;
        }
        candidates.push(path);
    }

    match candidates.len() {
        1 => Ok(candidates.remove(0)),
        0 => Err(HarliteError::InvalidArgs(
            "No database specified and no .db files found in the current directory".to_string(),
        )),
        n => Err(HarliteError::InvalidArgs(format!(
            "No database specified and found {} .db files in the current directory; please pass a database path",
            n
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_database_in_dir, ExternalPathPolicy};
    use crate::error::HarliteError;
    use tempfile::TempDir;

    #[test]
    fn resolve_database_in_dir_returns_single_match() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("only.db");
        std::fs::write(&db_path, b"").unwrap();

        let resolved = resolve_database_in_dir(tmp.path()).unwrap();
        assert_eq!(resolved, db_path);
    }

    #[test]
    fn resolve_database_in_dir_errors_when_missing() {
        let tmp = TempDir::new().unwrap();
        let err = resolve_database_in_dir(tmp.path()).unwrap_err();
        match err {
            HarliteError::InvalidArgs(msg) => {
                assert!(msg.contains("no .db files"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn resolve_database_in_dir_errors_when_multiple() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("one.db"), b"").unwrap();
        std::fs::write(tmp.path().join("two.db"), b"").unwrap();

        let err = resolve_database_in_dir(tmp.path()).unwrap_err();
        match err {
            HarliteError::InvalidArgs(msg) => {
                assert!(msg.contains("found 2 .db files"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn external_paths_are_disabled_by_default() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("blob");
        std::fs::write(&file, b"secret").unwrap();
        let policy = ExternalPathPolicy::new(&tmp.path().join("test.db"), false, None).unwrap();
        assert!(policy.resolve_file(file.to_str().unwrap()).is_none());
    }

    #[test]
    fn external_path_default_root_handles_relative_database() {
        let policy = ExternalPathPolicy::new(std::path::Path::new("test.db"), true, None);
        assert!(policy.is_ok());
    }

    #[test]
    fn external_paths_must_stay_inside_root() {
        let root = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let inside_file = root.path().join("blob");
        let outside_file = outside.path().join("secret");
        std::fs::write(&inside_file, b"inside").unwrap();
        std::fs::write(&outside_file, b"outside").unwrap();

        let policy = ExternalPathPolicy::new(
            &root.path().join("test.db"),
            true,
            Some(root.path()),
        )
        .unwrap();
        assert_eq!(policy.resolve_file("blob"), Some(inside_file.canonicalize().unwrap()));
        assert!(policy
            .resolve_file(outside_file.to_str().unwrap())
            .is_none());
    }
}
