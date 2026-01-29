use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Utc};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use rusqlite::Connection;

use crate::commands::{run_import, run_info, run_stats, ImportOptions, StatsOptions};
use crate::db::create_schema;
use crate::error::{HarliteError, Result};

#[derive(Clone)]
pub struct WatchOptions {
    pub output: Option<PathBuf>,
    pub recursive: bool,
    pub debounce_ms: u64,
    pub stable_ms: u64,
    pub import_existing: bool,
    pub post_info: bool,
    pub post_stats: bool,
    pub post_stats_json: bool,
    pub import_options: ImportOptions,
}

#[derive(Clone, Copy)]
struct FileFingerprint {
    size: u64,
    modified: SystemTime,
}

struct PendingFile {
    last_event: Instant,
    last_size: u64,
    last_mtime: SystemTime,
    last_change: Instant,
}

pub fn run_watch(directory: PathBuf, options: &WatchOptions) -> Result<()> {
    if !directory.exists() {
        return Err(HarliteError::InvalidArgs(format!(
            "Watch directory does not exist: {}",
            directory.display()
        )));
    }
    if !directory.is_dir() {
        return Err(HarliteError::InvalidArgs(format!(
            "Watch path is not a directory: {}",
            directory.display()
        )));
    }

    let output_db = resolve_watch_output(&directory, options.output.as_ref())?;
    let mut import_options = options.import_options.clone();
    import_options.output = Some(output_db.clone());
    import_options.jobs = 1;

    let mut imported_history = load_import_history(&output_db)?;
    let mut imported_files: HashMap<String, FileFingerprint> = HashMap::new();
    let mut pending: HashMap<PathBuf, PendingFile> = HashMap::new();

    if options.import_existing {
        import_existing_files(
            &directory,
            options.recursive,
            &import_options,
            &imported_history,
            &mut imported_files,
            options,
            &output_db,
        )?;
    }

    let (tx, rx) = mpsc::channel();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    })
    .map_err(|err| HarliteError::InvalidArgs(format!("Failed to init watcher: {err}")))?;

    let recursive_mode = if options.recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    watcher
        .watch(&directory, recursive_mode)
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to watch directory: {err}")))?;

    let stop = Arc::new(AtomicBool::new(false));
    let stop_handler = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        stop_handler.store(true, Ordering::SeqCst);
    })
    .map_err(|err| HarliteError::InvalidArgs(format!("Failed to install Ctrl+C handler: {err}")))?;

    println!(
        "Watching {}{} (Ctrl+C to stop)...",
        directory.display(),
        if options.recursive { " recursively" } else { "" }
    );

    let debounce = Duration::from_millis(options.debounce_ms.max(50));
    let stable = Duration::from_millis(options.stable_ms.max(50));
    let tick = Duration::from_millis(200);

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        match rx.recv_timeout(tick) {
            Ok(Ok(event)) => {
                for path in event.paths {
                    if !is_har_file(&path) {
                        continue;
                    }
                    pending
                        .entry(path)
                        .and_modify(|state| state.last_event = Instant::now())
                        .or_insert_with(|| PendingFile {
                            last_event: Instant::now(),
                            last_size: 0,
                            last_mtime: SystemTime::UNIX_EPOCH,
                            last_change: Instant::now(),
                        });
                }
            }
            Ok(Err(err)) => {
                eprintln!("Watch error: {err}");
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        let mut ready = Vec::new();
        let now = Instant::now();
        pending.retain(|path, state| {
            if now.duration_since(state.last_event) < debounce {
                return true;
            }

            let metadata = match fs::metadata(path) {
                Ok(meta) => meta,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return false;
                }
                Err(err) => {
                    eprintln!("Failed to stat {}: {}", path.display(), err);
                    return true;
                }
            };

            if !metadata.is_file() {
                return false;
            }

            let size = metadata.len();
            let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            if size != state.last_size || mtime != state.last_mtime {
                state.last_size = size;
                state.last_mtime = mtime;
                state.last_change = now;
                return true;
            }

            if now.duration_since(state.last_change) >= stable {
                ready.push(path.to_path_buf());
                return false;
            }

            true
        });

        for path in ready {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let canonical = path
                .canonicalize()
                .unwrap_or_else(|_| path.to_path_buf());
            let key = canonical.to_string_lossy().to_string();
            let fingerprint = match file_fingerprint(&canonical) {
                Ok(fp) => fp,
                Err(err) => {
                    eprintln!("Failed to read {}: {}", canonical.display(), err);
                    continue;
                }
            };

            if let Some(existing) = imported_files.get(&key) {
                if fingerprints_equal(existing, &fingerprint) {
                    continue;
                }
            }
            if let Some(imported_at) = imported_history.get(&key) {
                if fingerprint.modified <= *imported_at {
                    continue;
                }
            }

            if let Err(err) = run_import(&[canonical.clone()], &import_options) {
                eprintln!("Import failed for {}: {}", canonical.display(), err);
                continue;
            }

            imported_files.insert(key.clone(), fingerprint);
            imported_history.insert(key, SystemTime::now());

            if options.post_info {
                if let Err(err) = run_info(output_db.clone()) {
                    eprintln!("Post-import info failed: {}", err);
                }
            }
            if options.post_stats {
                let stats_options = StatsOptions {
                    json: options.post_stats_json,
                };
                if let Err(err) = run_stats(output_db.clone(), &stats_options) {
                    eprintln!("Post-import stats failed: {}", err);
                }
            }
        }
    }

    println!("Watch stopped.");
    Ok(())
}

fn resolve_watch_output(directory: &Path, output: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(path) = output {
        return Ok(path.clone());
    }
    let name = directory
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("watch");
    Ok(PathBuf::from(format!("{name}.db")))
}

fn file_fingerprint(path: &Path) -> Result<FileFingerprint> {
    let metadata = fs::metadata(path)?;
    Ok(FileFingerprint {
        size: metadata.len(),
        modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
    })
}

fn fingerprints_equal(a: &FileFingerprint, b: &FileFingerprint) -> bool {
    a.size == b.size && a.modified == b.modified
}

fn is_har_file(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("har"))
}

fn load_import_history(db_path: &Path) -> Result<HashMap<String, SystemTime>> {
    let conn = Connection::open(db_path)?;
    create_schema(&conn)?;
    let mut stmt = conn.prepare(
        "SELECT source_file, imported_at, status FROM imports WHERE source_file IS NOT NULL",
    )?;
    let mut rows = stmt.query([])?;
    let mut history = HashMap::new();
    while let Some(row) = rows.next()? {
        let source: String = row.get(0)?;
        let imported_at: String = row.get(1)?;
        let status: Option<String> = row.get(2)?;
        if status.as_deref() != Some("complete") {
            continue;
        }
        if let Some(ts) = parse_imported_at(&imported_at) {
            history.insert(source, ts);
        }
    }
    Ok(history)
}

fn parse_imported_at(value: &str) -> Option<SystemTime> {
    let dt = DateTime::parse_from_rfc3339(value).ok()?;
    let utc = dt.with_timezone(&Utc);
    let secs = utc.timestamp();
    if secs < 0 {
        return None;
    }
    let nanos = utc.timestamp_subsec_nanos() as u64;
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64) + Duration::from_nanos(nanos))
}

fn import_existing_files(
    directory: &Path,
    recursive: bool,
    import_options: &ImportOptions,
    imported_history: &HashMap<String, SystemTime>,
    imported_files: &mut HashMap<String, FileFingerprint>,
    options: &WatchOptions,
    output_db: &Path,
) -> Result<()> {
    let mut stack = vec![directory.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = match entry.metadata() {
                Ok(meta) => meta,
                Err(_) => continue,
            };
            if metadata.is_dir() {
                if recursive {
                    stack.push(path);
                }
                continue;
            }
            if !is_har_file(&path) {
                continue;
            }
            let canonical = path
                .canonicalize()
                .unwrap_or_else(|_| path.to_path_buf());
            let key = canonical.to_string_lossy().to_string();
            let fingerprint = match file_fingerprint(&canonical) {
                Ok(fp) => fp,
                Err(_) => continue,
            };
            if let Some(imported_at) = imported_history.get(&key) {
                if fingerprint.modified <= *imported_at {
                    continue;
                }
            }
            run_import(&[canonical.clone()], import_options)?;
            imported_files.insert(key, fingerprint);
            if options.post_info {
                if let Err(err) = run_info(output_db.to_path_buf()) {
                    eprintln!("Post-import info failed: {}", err);
                }
            }
            if options.post_stats {
                let stats_options = StatsOptions {
                    json: options.post_stats_json,
                };
                if let Err(err) = run_stats(output_db.to_path_buf(), &stats_options) {
                    eprintln!("Post-import stats failed: {}", err);
                }
            }
        }
    }
    Ok(())
}
