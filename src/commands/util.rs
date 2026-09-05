use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rusqlite::{Connection, DatabaseName, OpenFlags};

use crate::error::{HarliteError, Result};

/// Return whether a path points to a SQLite database by inspecting its magic
/// header. Standard input (`-`) is never treated as SQLite.
pub fn is_sqlite_file(path: &Path) -> bool {
    if path == Path::new("-") {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut header = [0u8; 16];
    file.read_exact(&mut header).is_ok() && header == *b"SQLite format 3\0"
}

/// Build a sibling output name such as `capture-redacted.har`.
pub fn derived_output_path(input: &Path, suffix: &str, extension: &str) -> Result<PathBuf> {
    if input == Path::new("-") {
        return Err(HarliteError::InvalidArgs(
            "Reading from standard input requires --output".to_string(),
        ));
    }
    let parent = input
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stem = input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| HarliteError::InvalidArgs("Input path must have a filename".to_string()))?;
    Ok(parent.join(format!("{stem}{suffix}.{extension}")))
}

/// Serialize JSON through a same-directory temporary file and publish it with
/// a rename, so failures never leave a partial output at the destination.
pub fn write_json_atomic<T: serde::Serialize>(
    destination: &Path,
    value: &T,
    pretty: bool,
    overwrite: bool,
) -> Result<()> {
    if destination == Path::new("-") {
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        if pretty {
            serde_json::to_writer_pretty(&mut output, value)?;
        } else {
            serde_json::to_writer(&mut output, value)?;
        }
        output.write_all(b"\n")?;
        return Ok(());
    }
    write_file_atomic(destination, overwrite, |output| {
        if pretty {
            serde_json::to_writer_pretty(&mut *output, value)?;
        } else {
            serde_json::to_writer(&mut *output, value)?;
        }
        output.write_all(b"\n")?;
        Ok(())
    })
}

/// Write already-rendered bytes through a same-directory temporary file and
/// atomically publish the completed output.
pub fn write_bytes_atomic(destination: &Path, bytes: &[u8], overwrite: bool) -> Result<()> {
    if destination == Path::new("-") {
        std::io::stdout().lock().write_all(bytes)?;
        return Ok(());
    }
    write_file_atomic(destination, overwrite, |output| {
        output.write_all(bytes)?;
        Ok(())
    })
}

/// Reject aliases of an input, including symlinks and hard links, before output opens.
pub(crate) fn ensure_output_not_input(input: &Path, output: &Path) -> Result<()> {
    if input != Path::new("-")
        && output != Path::new("-")
        && output.exists()
        && same_file::is_same_file(input, output)?
    {
        return Err(HarliteError::InvalidArgs(
            "Output must be different from the input file".into(),
        ));
    }
    Ok(())
}

/// Preserve existing export overwrite behavior, but only publish complete bytes.
pub(crate) fn write_output_atomic(
    destination: &Path,
    write: impl FnOnce(&mut dyn Write) -> Result<()>,
) -> Result<()> {
    if destination == Path::new("-") {
        let mut output = std::io::stdout().lock();
        write(&mut output)?;
        output.flush()?;
        return Ok(());
    }
    write_file_atomic(destination, true, |output| write(output))
}

/// Restrict secret-bearing files at creation, before any bytes are copied.
/// Windows uses the containing directory's inherited ACL.
fn private_file_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.read(true).write(true).create_new(true);
    options
}

pub(crate) fn write_file_atomic(
    destination: &Path,
    overwrite: bool,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
) -> Result<()> {
    if destination.exists() && !destination.is_file() {
        return Err(HarliteError::InvalidArgs(format!(
            "Output path must be a file: {}",
            destination.display()
        )));
    }
    if destination.exists() && !overwrite {
        return Err(HarliteError::InvalidArgs(format!(
            "Output file already exists: {} (use --force to overwrite)",
            destination.display()
        )));
    }

    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = destination
        .file_name()
        .ok_or_else(|| HarliteError::InvalidArgs("Output path must be a file".to_string()))?
        .to_string_lossy();
    let staged_path = unique_sibling(parent, &file_name, "stage")?;

    let write_result = (|| -> Result<()> {
        let file = private_file_options().open(&staged_path)?;
        let mut output = BufWriter::new(file);
        write(&mut output)?;
        output.flush()?;
        output.get_ref().sync_all()?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&staged_path);
        return Err(error);
    }

    let backup_path = if destination.exists() {
        let backup = unique_sibling(parent, &file_name, "backup")?;
        fs::rename(destination, &backup)?;
        Some(backup)
    } else {
        None
    };
    if let Err(error) = fs::rename(&staged_path, destination) {
        if let Some(backup) = &backup_path {
            let _ = fs::rename(backup, destination);
        }
        let _ = fs::remove_file(&staged_path);
        return Err(error.into());
    }
    if let Some(backup) = backup_path {
        fs::remove_file(backup)?;
    }
    Ok(())
}

fn unique_sibling(parent: &Path, file_name: &str, kind: &str) -> Result<PathBuf> {
    loop {
        let id = NEXT_STAGED_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{file_name}.harlite-{kind}-{}-{id}",
            std::process::id()
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
}

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

struct DestinationBackup {
    destination: PathBuf,
    directory: PathBuf,
    moved_main: bool,
    moved_sidecars: Vec<&'static str>,
    active: bool,
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
            match private_file_options().open(&candidate) {
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
        if self.destination.exists() && !self.destination.is_file() {
            return Err(HarliteError::InvalidArgs(format!(
                "Output path must be a file: {}",
                self.destination.display()
            )));
        }
        if self.destination.exists() && !self.overwrite {
            return Err(HarliteError::InvalidArgs(format!(
                "Output database already exists: {} (use --force to overwrite)",
                self.destination.display()
            )));
        }

        remove_database_sidecars(&self.path)?;
        let has_destination_files = self.destination.exists()
            || database_sidecar_suffixes()
                .iter()
                .any(|suffix| database_sidecar_path(&self.destination, suffix).exists());
        let backup = if has_destination_files {
            let mut backup = DestinationBackup::new(&self.destination)?;
            backup.capture()?;
            Some(backup)
        } else {
            None
        };

        fs::rename(&self.path, &self.destination)?;
        if let Some(backup) = backup {
            backup.discard();
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

impl DestinationBackup {
    fn new(destination: &Path) -> Result<Self> {
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let file_name = destination
            .file_name()
            .ok_or_else(|| HarliteError::InvalidArgs("Output path must be a file".to_string()))?
            .to_string_lossy();
        let directory = loop {
            let id = NEXT_STAGED_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
            let candidate = parent.join(format!(
                ".{file_name}.harlite-backup-{}-{id}",
                std::process::id()
            ));
            match fs::create_dir(&candidate) {
                Ok(()) => break candidate,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        };

        Ok(Self {
            destination: destination.to_path_buf(),
            directory,
            moved_main: false,
            moved_sidecars: Vec::new(),
            active: true,
        })
    }

    fn capture(&mut self) -> Result<()> {
        if self.destination.exists() {
            fs::rename(&self.destination, self.directory.join("database"))?;
            self.moved_main = true;
        }
        for suffix in database_sidecar_suffixes() {
            let source = database_sidecar_path(&self.destination, suffix);
            if source.exists() {
                fs::rename(&source, self.directory.join(format!("database{suffix}")))?;
                self.moved_sidecars.push(suffix);
            }
        }
        Ok(())
    }

    fn restore(&mut self) -> Result<()> {
        while let Some(suffix) = self.moved_sidecars.pop() {
            fs::rename(
                self.directory.join(format!("database{suffix}")),
                database_sidecar_path(&self.destination, suffix),
            )?;
        }
        if self.moved_main {
            fs::rename(self.directory.join("database"), &self.destination)?;
            self.moved_main = false;
        }
        fs::remove_dir(&self.directory)?;
        self.active = false;
        Ok(())
    }

    fn discard(mut self) {
        self.active = false;
        let _ = fs::remove_dir_all(&self.directory);
    }
}

impl Drop for DestinationBackup {
    fn drop(&mut self) {
        if self.active {
            let _ = self.restore();
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
    for suffix in database_sidecar_suffixes() {
        let sidecar = database_sidecar_path(database, suffix);
        if sidecar.exists() {
            fs::remove_file(sidecar)?;
        }
    }
    Ok(())
}

fn database_sidecar_suffixes() -> [&'static str; 3] {
    ["-journal", "-wal", "-shm"]
}

fn database_sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = database.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
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
    conn.execute_batch(
        "PRAGMA wal_checkpoint(TRUNCATE); VACUUM; PRAGMA wal_checkpoint(TRUNCATE);",
    )?;
    Ok(())
}

pub fn canonicalize_path_for_compare(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(fs::canonicalize(path)?);
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_canon = fs::canonicalize(parent)?;
    let name = path
        .file_name()
        .ok_or_else(|| HarliteError::InvalidArgs("Output path must be a file".to_string()))?;
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
    use super::{
        remove_database_with_sidecars, resolve_database_in_dir, DestinationBackup,
        ExternalPathPolicy, StagedDatabase,
    };
    use crate::error::HarliteError;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    fn sensitive_staging_and_published_outputs_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("private.db");
        let destination = tmp.path().join("redacted.db");
        let conn = rusqlite::Connection::open(&source).unwrap();
        conn.execute_batch(
            "CREATE TABLE secrets (value TEXT); INSERT INTO secrets VALUES ('private');",
        )
        .unwrap();
        drop(conn);
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
        let staged = StagedDatabase::copy_from(&source, &destination, false).unwrap();
        assert_eq!(
            std::fs::metadata(staged.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        staged.publish().unwrap();
        assert_eq!(
            std::fs::metadata(&destination)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let output = tmp.path().join("redacted.har");
        super::write_file_atomic(&output, false, |writer| {
            assert_eq!(
                writer.get_ref().metadata()?.permissions().mode() & 0o777,
                0o600
            );
            std::io::Write::write_all(writer, b"sensitive")?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            std::fs::metadata(output).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn failed_output_write_keeps_destination_and_removes_stage() {
        let tmp = TempDir::new().unwrap();
        let destination = tmp.path().join("result.json");
        std::fs::write(&destination, b"previous output").unwrap();
        let result = super::write_output_atomic(&destination, |out| {
            out.write_all(b"partial replacement")?;
            Err(std::io::Error::other("simulated serialization failure").into())
        });
        assert!(result.is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), b"previous output");
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 1);
    }

    #[test]
    fn resolve_database_in_dir_returns_single_match() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("only.db");
        std::fs::write(&db_path, b"").unwrap();

        let resolved = resolve_database_in_dir(tmp.path()).unwrap();
        assert_eq!(resolved, db_path);
    }

    #[test]
    fn remove_database_cleans_sqlite_sidecars() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("staged.db");
        std::fs::write(&db_path, b"database").unwrap();
        for suffix in ["-journal", "-wal", "-shm"] {
            std::fs::write(tmp.path().join(format!("staged.db{suffix}")), b"sidecar").unwrap();
        }

        remove_database_with_sidecars(&db_path).unwrap();

        assert!(!db_path.exists());
        for suffix in ["-journal", "-wal", "-shm"] {
            assert!(!tmp.path().join(format!("staged.db{suffix}")).exists());
        }
    }

    #[test]
    fn destination_backup_restores_database_and_sidecars_on_drop() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("existing.db");
        std::fs::write(&db_path, b"database").unwrap();
        for suffix in ["-journal", "-wal", "-shm"] {
            std::fs::write(tmp.path().join(format!("existing.db{suffix}")), suffix).unwrap();
        }

        {
            let mut backup = DestinationBackup::new(&db_path).unwrap();
            backup.capture().unwrap();
            assert!(!db_path.exists());
            assert!(!tmp.path().join("existing.db-wal").exists());
        }

        assert_eq!(std::fs::read(&db_path).unwrap(), b"database");
        for suffix in ["-journal", "-wal", "-shm"] {
            assert_eq!(
                std::fs::read(tmp.path().join(format!("existing.db{suffix}"))).unwrap(),
                suffix.as_bytes()
            );
        }
    }

    #[test]
    fn staged_database_replaces_existing_database_and_sidecars() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("source.db");
        let destination = tmp.path().join("destination.db");
        let conn = rusqlite::Connection::open(&source).unwrap();
        conn.execute_batch("CREATE TABLE value (number INTEGER); INSERT INTO value VALUES (42);")
            .unwrap();
        drop(conn);
        std::fs::write(&destination, b"previous database").unwrap();
        for suffix in ["-journal", "-wal", "-shm"] {
            std::fs::write(
                tmp.path().join(format!("destination.db{suffix}")),
                b"previous sidecar",
            )
            .unwrap();
        }

        let staged = StagedDatabase::copy_from(&source, &destination, true).unwrap();
        staged.publish().unwrap();

        let conn = rusqlite::Connection::open(&destination).unwrap();
        let value: i64 = conn
            .query_row("SELECT number FROM value", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, 42);
        for suffix in ["-journal", "-wal", "-shm"] {
            assert!(!tmp.path().join(format!("destination.db{suffix}")).exists());
        }
        assert!(!std::fs::read_dir(tmp.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("harlite-backup")));
    }

    #[test]
    fn staged_database_restores_existing_files_when_install_fails() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("source.db");
        let destination = tmp.path().join("destination.db");
        let conn = rusqlite::Connection::open(&source).unwrap();
        conn.execute_batch("CREATE TABLE value (number INTEGER);")
            .unwrap();
        drop(conn);
        std::fs::write(&destination, b"previous database").unwrap();
        for suffix in ["-journal", "-wal", "-shm"] {
            std::fs::write(
                tmp.path().join(format!("destination.db{suffix}")),
                suffix.as_bytes(),
            )
            .unwrap();
        }

        let staged = StagedDatabase::copy_from(&source, &destination, true).unwrap();
        std::fs::remove_file(staged.path()).unwrap();
        assert!(staged.publish().is_err());

        assert_eq!(std::fs::read(&destination).unwrap(), b"previous database");
        for suffix in ["-journal", "-wal", "-shm"] {
            assert_eq!(
                std::fs::read(tmp.path().join(format!("destination.db{suffix}"))).unwrap(),
                suffix.as_bytes()
            );
        }
        assert!(!std::fs::read_dir(tmp.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("harlite-backup")));
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

        let policy =
            ExternalPathPolicy::new(&root.path().join("test.db"), true, Some(root.path())).unwrap();
        assert_eq!(
            policy.resolve_file("blob"),
            Some(inside_file.canonicalize().unwrap())
        );
        assert!(policy
            .resolve_file(outside_file.to_str().unwrap())
            .is_none());
    }
}
