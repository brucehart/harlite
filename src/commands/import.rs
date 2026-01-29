use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, path, thread};

use chrono::{DateTime, NaiveDate, Utc};
use indicatif::ProgressBar;
use regex::Regex;
use rusqlite::{Connection, TransactionBehavior};
use url::Url;

use crate::db::{
    create_import_with_status, create_schema, entry_content_hash, entry_hash_from_fields,
    insert_entry_with_hash, insert_page, update_import_metadata, EntryHashFields,
    ExtractBodiesKind, ImportStats, InsertEntryOptions,
};
use crate::error::{HarliteError, Result};
use crate::har::{parse_har_file, parse_har_file_async, Entry};

/// Options for importing HAR files.
#[derive(Clone)]
pub struct ImportOptions {
    pub output: Option<PathBuf>,
    pub store_bodies: bool,
    pub max_body_size: Option<usize>,
    pub text_only: bool,
    pub show_stats: bool,
    pub incremental: bool,
    pub resume: bool,
    pub jobs: usize,
    pub async_read: bool,
    pub decompress_bodies: bool,
    pub keep_compressed: bool,
    pub extract_bodies_dir: Option<PathBuf>,
    pub extract_bodies_kind: ExtractBodiesKind,
    pub extract_bodies_shard_depth: u8,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub url_regex: Vec<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            output: None,
            store_bodies: false,
            max_body_size: Some(100 * 1024),
            text_only: false,
            show_stats: false,
            incremental: false,
            resume: false,
            jobs: 0,
            async_read: false,
            decompress_bodies: false,
            keep_compressed: false,
            extract_bodies_dir: None,
            extract_bodies_kind: ExtractBodiesKind::Both,
            extract_bodies_shard_depth: 0,
            host: Vec::new(),
            method: Vec::new(),
            status: Vec::new(),
            url_regex: Vec::new(),
            from: None,
            to: None,
        }
    }
}

#[derive(Clone)]
struct ImportFilters {
    hosts: Vec<String>,
    methods: Vec<String>,
    statuses: Vec<i32>,
    url_regexes: Vec<Regex>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    to_is_exclusive: bool,
}

const DEFAULT_BATCH_SIZE: usize = 500;
const BACKFILL_BATCH_SIZE: usize = 1000;

#[derive(Clone, Copy)]
struct ImportRunConfig {
    incremental: bool,
    resume: bool,
    async_read: bool,
    parallel: bool,
}

impl ImportRunConfig {
    fn use_progress(&self, total_entries: usize) -> bool {
        !self.parallel && total_entries > 0
    }
}

struct ResumeImport {
    import_id: i64,
    entries_imported: usize,
    entries_skipped: usize,
}

/// Import one or more HAR files into a SQLite database.
pub fn run_import(files: &[PathBuf], options: &ImportOptions) -> Result<ImportStats> {
    if files.is_empty() {
        return Err(HarliteError::InvalidHar(
            "No input files specified".to_string(),
        ));
    }
    if options.keep_compressed && !options.decompress_bodies {
        return Err(HarliteError::InvalidArgs(
            "--keep-compressed requires --decompress-bodies".to_string(),
        ));
    }

    let output_path = match &options.output {
        Some(p) => p.clone(),
        None => {
            let first_file = &files[0];
            let stem = first_file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("output");
            PathBuf::from(format!("{}.db", stem))
        }
    };

    let mut conn = Connection::open(&output_path)?;
    setup_connection(&conn)?;

    let extract_dir = if let Some(dir) = &options.extract_bodies_dir {
        fs::create_dir_all(dir)?;
        Some(path::Path::new(dir).canonicalize()?)
    } else {
        None
    };

    let entry_options = InsertEntryOptions {
        store_bodies: options.store_bodies || extract_dir.is_some(),
        max_body_size: options.max_body_size,
        text_only: options.text_only,
        decompress_bodies: options.decompress_bodies,
        keep_compressed: options.keep_compressed,
        extract_bodies_dir: extract_dir,
        extract_bodies_kind: options.extract_bodies_kind,
        extract_bodies_shard_depth: options.extract_bodies_shard_depth,
    };
    let filters = build_import_filters(options)?;

    let jobs = resolve_jobs(files.len(), options.jobs);
    let run_config = ImportRunConfig {
        incremental: options.incremental || options.resume,
        resume: options.resume,
        async_read: options.async_read,
        parallel: jobs > 1,
    };
    if run_config.incremental {
        let updated = backfill_entry_hashes(&mut conn)?;
        if updated > 0 {
            println!("Backfilled entry hashes for {} existing entries.", updated);
        }
    }
    let total_stats = if jobs == 1 {
        let mut stats = ImportStats::default();
        for file_path in files {
            let file_stats =
                import_single_file(&mut conn, file_path, &entry_options, &filters, &run_config)?;
            stats.add_assign(file_stats);
        }
        stats
    } else {
        drop(conn);
        import_parallel(
            files,
            &output_path,
            &entry_options,
            &filters,
            &run_config,
            jobs,
        )?
    };

    if options.show_stats {
        print_stats(&total_stats);
    }

    if total_stats.entries_skipped > 0 {
        println!(
            "Imported {} entries to {} (skipped {} duplicates)",
            total_stats.entries_imported,
            output_path.display(),
            total_stats.entries_skipped
        );
    } else {
        println!(
            "Imported {} entries to {}",
            total_stats.entries_imported,
            output_path.display()
        );
    }

    Ok(total_stats)
}

fn import_single_file(
    conn: &mut Connection,
    path: &Path,
    options: &InsertEntryOptions,
    filters: &ImportFilters,
    run_config: &ImportRunConfig,
) -> Result<ImportStats> {
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    let source_key = source_key(path);

    println!("Importing {}...", file_name);

    let har = if run_config.async_read {
        parse_har_file_async(path)?
    } else {
        parse_har_file(path)?
    };
    let total_entries = har.log.entries.len();
    let mut base_imported = 0usize;
    let mut base_skipped = 0usize;
    let mut resumed = None;
    let import_id = if run_config.resume {
        if let Some(resume) = find_resume_import(conn, &source_key)? {
            let import_id = resume.import_id;
            base_imported = resume.entries_imported;
            base_skipped = resume.entries_skipped;
            update_import_metadata(
                conn,
                import_id,
                None,
                Some(total_entries),
                Some(base_skipped),
                Some("in_progress"),
            )?;
            resumed = Some(resume);
            import_id
        } else {
            create_import_with_status(
                conn,
                &source_key,
                Some(&har.log.extensions),
                "in_progress",
                Some(total_entries),
                Some(0),
            )?
        }
    } else {
        create_import_with_status(
            conn,
            &source_key,
            Some(&har.log.extensions),
            "in_progress",
            Some(total_entries),
            Some(0),
        )?
    };

    if let Some(resume) = resumed {
        println!(
            "Resuming import {} (id {}, {} entries imported, {} skipped)",
            file_name, resume.import_id, base_imported, base_skipped
        );
    }

    if let Some(pages) = &har.log.pages {
        for page in pages {
            insert_page(conn, import_id, page)?;
        }
    }

    let entries = &har.log.entries;
    let pb = if run_config.use_progress(total_entries) {
        ProgressBar::new(total_entries as u64)
    } else {
        ProgressBar::hidden()
    };

    let mut stats = ImportStats::default();
    let mut tx = begin_import_tx(conn, run_config)?;
    let mut batch_count = 0usize;

    for entry in entries {
        if !entry_matches_filters(entry, filters)? {
            pb.inc(1);
            continue;
        }
        let entry_hash = entry_content_hash(entry);
        if run_config.incremental && entry_hash_exists(&tx, &entry_hash)? {
            stats.entries_skipped += 1;
            pb.inc(1);
            continue;
        }

        let entry_result =
            insert_entry_with_hash(&tx, import_id, entry, options, Some(&entry_hash), false)?;
        if entry_result.inserted {
            stats.entries_imported += 1;
            stats.request.add_assign(entry_result.blob_stats.request);
            stats.response.add_assign(entry_result.blob_stats.response);
        }

        batch_count += 1;
        if batch_count >= DEFAULT_BATCH_SIZE {
            tx.commit()?;
            update_import_metadata(
                conn,
                import_id,
                Some(base_imported + stats.entries_imported),
                Some(total_entries),
                Some(base_skipped + stats.entries_skipped),
                Some("in_progress"),
            )?;
            tx = begin_import_tx(conn, run_config)?;
            batch_count = 0;
        }

        pb.inc(1);
    }

    tx.commit()?;
    pb.finish_and_clear();

    update_import_metadata(
        conn,
        import_id,
        Some(base_imported + stats.entries_imported),
        Some(total_entries),
        Some(base_skipped + stats.entries_skipped),
        Some("complete"),
    )?;

    Ok(stats)
}

fn setup_connection(conn: &Connection) -> Result<()> {
    conn.busy_timeout(Duration::from_secs(30))?;
    create_schema(conn)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    Ok(())
}

fn source_key(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string()
}

fn begin_import_tx<'a>(
    conn: &'a mut Connection,
    run_config: &ImportRunConfig,
) -> Result<rusqlite::Transaction<'a>> {
    if run_config.incremental {
        Ok(conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
    } else {
        Ok(conn.unchecked_transaction()?)
    }
}

fn resolve_jobs(file_count: usize, requested: usize) -> usize {
    if file_count <= 1 {
        return 1;
    }
    let jobs = if requested > 0 {
        requested
    } else {
        let cpus = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        cpus.min(4).max(1)
    };
    jobs.min(file_count).max(1)
}

fn import_parallel(
    files: &[PathBuf],
    output_path: &Path,
    entry_options: &InsertEntryOptions,
    filters: &ImportFilters,
    run_config: &ImportRunConfig,
    jobs: usize,
) -> Result<ImportStats> {
    let queue = Arc::new(Mutex::new(files.iter().cloned().collect::<VecDeque<_>>()));
    let mut handles = Vec::new();

    for _ in 0..jobs {
        let queue = Arc::clone(&queue);
        let output_path = output_path.to_path_buf();
        let entry_options = entry_options.clone();
        let filters = filters.clone();
        let run_config = *run_config;
        handles.push(thread::spawn(move || -> Result<ImportStats> {
            let mut conn = Connection::open(&output_path)?;
            setup_connection(&conn)?;
            let mut stats = ImportStats::default();
            loop {
                let next = {
                    let mut guard = queue.lock().expect("import queue");
                    guard.pop_front()
                };
                let Some(path) = next else {
                    break;
                };
                let file_stats =
                    import_single_file(&mut conn, &path, &entry_options, &filters, &run_config)?;
                stats.add_assign(file_stats);
            }
            Ok(stats)
        }));
    }

    let mut total = ImportStats::default();
    for handle in handles {
        let stats = handle
            .join()
            .map_err(|_| HarliteError::InvalidHar("Import worker panicked".to_string()))??;
        total.add_assign(stats);
    }

    Ok(total)
}

fn backfill_entry_hashes(conn: &mut Connection) -> Result<usize> {
    let missing: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entries WHERE entry_hash IS NULL",
        [],
        |row| row.get(0),
    )?;
    if missing == 0 {
        return Ok(0);
    }

    let mut updated = 0usize;
    loop {
        let mut stmt = conn.prepare(
            "SELECT id, page_id, started_at, time_ms, method, url, host, path, query_string, http_version,\n\
                    request_headers, request_cookies, request_body_size, status, status_text,\n\
                    response_headers, response_cookies, response_mime_type, is_redirect,\n\
                    server_ip, connection_id, entry_extensions, request_extensions, response_extensions,\n\
                    content_extensions, timings_extensions, post_data_extensions\n\
             FROM entries WHERE entry_hash IS NULL LIMIT ?1",
        )?;
        let rows: Vec<_> = stmt
            .query_map([BACKFILL_BATCH_SIZE as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<f64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, Option<i64>>(13)?,
                    row.get::<_, Option<String>>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<String>>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, Option<i64>>(18)?,
                    row.get::<_, Option<String>>(19)?,
                    row.get::<_, Option<String>>(20)?,
                    row.get::<_, Option<String>>(21)?,
                    row.get::<_, Option<String>>(22)?,
                    row.get::<_, Option<String>>(23)?,
                    row.get::<_, Option<String>>(24)?,
                    row.get::<_, Option<String>>(25)?,
                    row.get::<_, Option<String>>(26)?,
                ))
            })?
            .filter_map(|row| row.ok())
            .collect();

        if rows.is_empty() {
            break;
        }

        let tx = conn.unchecked_transaction()?;
        for row in rows {
            let (
                id,
                page_id,
                started_at,
                time_ms,
                method,
                url,
                host,
                path,
                query_string,
                http_version,
                request_headers,
                request_cookies,
                request_body_size,
                status,
                status_text,
                response_headers,
                response_cookies,
                response_mime_type,
                is_redirect,
                server_ip,
                connection_id,
                entry_extensions,
                request_extensions,
                response_extensions,
                content_extensions,
                timings_extensions,
                post_data_extensions,
            ) = row;
            let fields = EntryHashFields {
                page_id: page_id.as_deref(),
                started_at: started_at.as_deref(),
                time_ms,
                method: method.as_deref(),
                url: url.as_deref(),
                host: host.as_deref(),
                path: path.as_deref(),
                query_string: query_string.as_deref(),
                http_version: http_version.as_deref(),
                request_headers: request_headers.as_deref(),
                request_cookies: request_cookies.as_deref(),
                request_body_size,
                status,
                status_text: status_text.as_deref(),
                response_headers: response_headers.as_deref(),
                response_cookies: response_cookies.as_deref(),
                response_mime_type: response_mime_type.as_deref(),
                is_redirect,
                server_ip: server_ip.as_deref(),
                connection_id: connection_id.as_deref(),
                entry_extensions: entry_extensions.as_deref(),
                request_extensions: request_extensions.as_deref(),
                response_extensions: response_extensions.as_deref(),
                content_extensions: content_extensions.as_deref(),
                timings_extensions: timings_extensions.as_deref(),
                post_data_extensions: post_data_extensions.as_deref(),
            };
            let hash = entry_hash_from_fields(&fields);
            tx.execute(
                "UPDATE entries SET entry_hash = ?1 WHERE id = ?2",
                rusqlite::params![hash, id],
            )?;
            updated += 1;
        }
        tx.commit()?;
    }

    Ok(updated)
}

fn entry_hash_exists(tx: &rusqlite::Transaction<'_>, hash: &str) -> Result<bool> {
    match tx.query_row(
        "SELECT 1 FROM entries WHERE entry_hash = ?1 LIMIT 1",
        [hash],
        |_| Ok(()),
    ) {
        Ok(_) => Ok(true),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

fn find_resume_import(conn: &Connection, source_key: &str) -> Result<Option<ResumeImport>> {
    let mut stmt = conn.prepare(
        "SELECT id, COALESCE(entries_skipped, 0)\n\
         FROM imports\n\
         WHERE source_file = ?1 AND (status IS NULL OR status != 'complete')\n\
         ORDER BY id DESC\n\
         LIMIT 1",
    )?;
    let row = stmt.query_row([source_key], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
    });
    let (import_id, entries_skipped) = match row {
        Ok(row) => row,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let entries_imported: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entries WHERE import_id = ?1",
        [import_id],
        |r| r.get(0),
    )?;
    Ok(Some(ResumeImport {
        import_id,
        entries_imported: entries_imported as usize,
        entries_skipped: entries_skipped as usize,
    }))
}

fn build_import_filters(options: &ImportOptions) -> Result<ImportFilters> {
    let url_regexes = options
        .url_regex
        .iter()
        .map(|value| {
            Regex::new(value).map_err(|err| {
                HarliteError::InvalidArgs(format!("Invalid URL regex '{value}': {err}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let hosts = options.host.iter().map(|h| h.to_lowercase()).collect();
    let methods = options.method.iter().map(|m| m.to_lowercase()).collect();
    let from = match options.from.as_deref() {
        Some(value) => Some(parse_started_at_bound(value, false)?.0),
        None => None,
    };
    let (to, to_is_exclusive) = match options.to.as_deref() {
        Some(value) => {
            let (dt, exclusive) = parse_started_at_bound(value, true)?;
            (Some(dt), exclusive)
        }
        None => (None, false),
    };

    Ok(ImportFilters {
        hosts,
        methods,
        statuses: options.status.clone(),
        url_regexes,
        from,
        to,
        to_is_exclusive,
    })
}

fn entry_matches_filters(entry: &Entry, filters: &ImportFilters) -> Result<bool> {
    if !filters.hosts.is_empty() {
        let host = Url::parse(&entry.request.url)
            .ok()
            .and_then(|url| url.host_str().map(|h| h.to_lowercase()));
        let matches = host
            .as_deref()
            .is_some_and(|value| filters.hosts.iter().any(|h| h == value));
        if !matches {
            return Ok(false);
        }
    }

    if !filters.methods.is_empty()
        && !filters
            .methods
            .iter()
            .any(|method| method.eq_ignore_ascii_case(&entry.request.method))
    {
        return Ok(false);
    }

    if !filters.statuses.is_empty() && !filters.statuses.contains(&entry.response.status) {
        return Ok(false);
    }

    if !filters.url_regexes.is_empty()
        && !filters
            .url_regexes
            .iter()
            .any(|re| re.is_match(&entry.request.url))
    {
        return Ok(false);
    }

    if filters.from.is_some() || filters.to.is_some() {
        let entry_dt = DateTime::parse_from_rfc3339(&entry.started_date_time)
            .map_err(|err| HarliteError::InvalidHar(format!("Invalid entry time: {err}")))?
            .with_timezone(&Utc);
        if filters.from.is_some_and(|from| entry_dt < from) {
            return Ok(false);
        }
        if let Some(to) = filters.to {
            if filters.to_is_exclusive {
                if entry_dt >= to {
                    return Ok(false);
                }
            } else if entry_dt > to {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

fn parse_started_at_bound(s: &str, is_end: bool) -> Result<(DateTime<Utc>, bool)> {
    let s = s.trim();
    if s.is_empty() {
        return Err(HarliteError::InvalidHar(
            "Empty timestamp bound".to_string(),
        ));
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok((dt.with_timezone(&Utc), false));
    }

    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
    let (dt, exclusive) = if is_end {
        let next_day = date
            .succ_opt()
            .ok_or_else(|| HarliteError::InvalidHar("Invalid end date".to_string()))?;
        let dt = next_day
            .and_hms_opt(0, 0, 0)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid end date".to_string()))?;
        (dt, true)
    } else {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid start date".to_string()))?;
        (dt, false)
    };

    Ok((dt, exclusive))
}

fn print_stats(stats: &ImportStats) {
    let total_created = stats.request.created + stats.response.created;
    let total_deduplicated = stats.request.deduplicated + stats.response.deduplicated;
    let total_bytes_stored = stats.request.bytes_stored + stats.response.bytes_stored;
    let total_bytes_deduplicated =
        stats.request.bytes_deduplicated + stats.response.bytes_deduplicated;

    println!("\nImport Statistics:");
    println!("  Entries imported: {}", stats.entries_imported);
    if stats.entries_skipped > 0 {
        println!("  Entries skipped (dedup): {}", stats.entries_skipped);
    }
    if total_created > 0 || total_deduplicated > 0 {
        println!("  Unique blobs stored: {}", total_created);
        println!("  Duplicate blobs skipped: {}", total_deduplicated);
        println!("  Bytes stored: {} KB", total_bytes_stored / 1024);
        println!(
            "  Bytes saved by deduplication: {} KB",
            total_bytes_deduplicated / 1024
        );
    }
}
