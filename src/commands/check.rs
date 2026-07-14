use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::DateTime;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use url::Url;

use crate::error::{HarliteError, Result};
use crate::har::{parse_har_file, Entry, Har};

use super::util::ExternalPathPolicy;

#[derive(Clone, Debug, Default)]
pub struct CheckOptions {
    pub json: bool,
    pub strict: bool,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum InputKind {
    Har,
    Database,
}

#[derive(Debug, Default, Serialize)]
struct CheckStats {
    entries: Option<u64>,
    pages: Option<u64>,
    imports: Option<u64>,
    blobs: Option<u64>,
    external_blobs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct CheckReport {
    input: String,
    input_kind: InputKind,
    valid: bool,
    errors: Vec<String>,
    warnings: Vec<String>,
    stats: CheckStats,
}

impl CheckReport {
    fn new(input: &Path, input_kind: InputKind) -> Self {
        Self {
            input: if input == Path::new("-") {
                "stdin".to_string()
            } else {
                input.display().to_string()
            },
            input_kind,
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            stats: CheckStats::default(),
        }
    }

    fn error(&mut self, message: impl Into<String>) {
        self.errors.push(message.into());
    }

    fn warn(&mut self, message: impl Into<String>) {
        self.warnings.push(message.into());
    }

    fn finish(&mut self, strict: bool) {
        self.errors.sort();
        self.errors.dedup();
        self.warnings.sort();
        self.warnings.dedup();
        self.valid = self.errors.is_empty() && (!strict || self.warnings.is_empty());
    }
}

pub fn run_check(input: PathBuf, options: &CheckOptions) -> Result<()> {
    let kind = detect_input_kind(&input);
    let mut report = match kind {
        InputKind::Har => check_har(&input),
        InputKind::Database => check_database(&input, options),
    };
    report.finish(options.strict);
    write_report(&report, options.json)?;

    let failures = report.errors.len()
        + if options.strict {
            report.warnings.len()
        } else {
            0
        };
    if failures > 0 {
        return Err(HarliteError::ValidationFailed(failures));
    }
    Ok(())
}

fn detect_input_kind(path: &Path) -> InputKind {
    if path == Path::new("-") {
        return InputKind::Har;
    }
    if sqlite_magic(path) {
        return InputKind::Database;
    }
    match path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("db" | "db3" | "sqlite" | "sqlite3") => InputKind::Database,
        _ => InputKind::Har,
    }
}

fn sqlite_magic(path: &Path) -> bool {
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut header = [0u8; 16];
    file.read_exact(&mut header).is_ok() && header == *b"SQLite format 3\0"
}

fn check_har(path: &Path) -> CheckReport {
    let mut report = CheckReport::new(path, InputKind::Har);
    let har = match parse_har_file(path) {
        Ok(har) => har,
        Err(error) => {
            report.error(error.to_string());
            return report;
        }
    };

    validate_har(&har, &mut report);
    report
}

fn validate_har(har: &Har, report: &mut CheckReport) {
    report.stats.entries = Some(har.log.entries.len() as u64);
    report.stats.pages = Some(har.log.pages.as_ref().map_or(0, Vec::len) as u64);

    match har.log.version.as_deref() {
        Some("1.2") => {}
        Some(version) => report.warn(format!(
            "HAR version is {version}; 1.2 is the interoperable baseline"
        )),
        None => report.warn("HAR log.version is missing"),
    }
    if har.log.creator.is_none() {
        report.warn("HAR log.creator is missing");
    }
    if har.log.entries.is_empty() {
        report.warn("HAR contains no entries");
    }

    let mut page_ids = HashSet::new();
    if let Some(pages) = &har.log.pages {
        for (index, page) in pages.iter().enumerate() {
            let context = format!("page {}", index + 1);
            if page.id.trim().is_empty() {
                report.error(format!("{context}: id is empty"));
            } else if !page_ids.insert(page.id.clone()) {
                report.error(format!("{context}: duplicate id '{}'", page.id));
            }
            validate_timestamp(
                &page.started_date_time,
                &format!("{context}.startedDateTime"),
                report,
            );
        }
    }

    for (index, entry) in har.log.entries.iter().enumerate() {
        validate_entry(index + 1, entry, &page_ids, report);
    }
}

fn validate_entry(
    index: usize,
    entry: &Entry,
    page_ids: &HashSet<String>,
    report: &mut CheckReport,
) {
    let context = format!("entry {index}");
    validate_timestamp(
        &entry.started_date_time,
        &format!("{context}.startedDateTime"),
        report,
    );

    if !entry.time.is_finite() || entry.time < 0.0 {
        report.error(format!(
            "{context}: time must be a non-negative finite number"
        ));
    }
    if entry.request.method.trim().is_empty() {
        report.error(format!("{context}: request.method is empty"));
    }
    if entry.request.http_version.trim().is_empty() {
        report.warn(format!("{context}: request.httpVersion is empty"));
    }
    if let Err(error) = Url::parse(&entry.request.url) {
        report.error(format!("{context}: invalid request URL: {error}"));
    }
    if !(0..=999).contains(&entry.response.status) {
        report.error(format!("{context}: response.status is outside 0..999"));
    }
    if entry.response.status == 0 {
        report.warn(format!(
            "{context}: response.status is 0 (request may be incomplete)"
        ));
    }
    if entry.response.http_version.trim().is_empty() {
        report.warn(format!("{context}: response.httpVersion is empty"));
    }

    validate_size(
        entry.request.headers_size,
        &format!("{context}.request.headersSize"),
        report,
    );
    validate_size(
        entry.request.body_size,
        &format!("{context}.request.bodySize"),
        report,
    );
    validate_size(
        entry.response.headers_size,
        &format!("{context}.response.headersSize"),
        report,
    );
    validate_size(
        entry.response.body_size,
        &format!("{context}.response.bodySize"),
        report,
    );
    validate_size(
        Some(entry.response.content.size),
        &format!("{context}.response.content.size"),
        report,
    );

    for header in entry.request.headers.iter().chain(&entry.response.headers) {
        if header.name.trim().is_empty() {
            report.error(format!("{context}: header name is empty"));
        }
    }

    if let Some(page_ref) = entry.pageref.as_deref() {
        if !page_ids.contains(page_ref) {
            report.warn(format!(
                "{context}: pageref '{page_ref}' has no matching page"
            ));
        }
    }

    if let Some(timings) = &entry.timings {
        for (name, value) in [
            ("blocked", timings.blocked),
            ("dns", timings.dns),
            ("connect", timings.connect),
            ("send", Some(timings.send)),
            ("wait", Some(timings.wait)),
            ("receive", Some(timings.receive)),
            ("ssl", timings.ssl),
        ] {
            if let Some(value) = value {
                if !value.is_finite() || (value < 0.0 && value != -1.0) {
                    report.error(format!(
                        "{context}: timing {name} must be -1 or non-negative"
                    ));
                }
            }
        }
    }

    if let Some(encoding) = entry.response.content.encoding.as_deref() {
        match encoding.to_ascii_lowercase().as_str() {
            "base64" => {
                if let Some(text) = entry.response.content.text.as_deref() {
                    if STANDARD.decode(text).is_err() {
                        report.error(format!("{context}: response content is not valid base64"));
                    }
                } else {
                    report.warn(format!(
                        "{context}: base64 encoding is set but content.text is missing"
                    ));
                }
            }
            other => report.warn(format!(
                "{context}: unrecognized response content encoding '{other}'"
            )),
        }
    }
}

fn validate_timestamp(value: &str, field: &str, report: &mut CheckReport) {
    if DateTime::parse_from_rfc3339(value).is_err() {
        report.error(format!("{field} is not RFC3339"));
    }
}

fn validate_size(value: Option<i64>, field: &str, report: &mut CheckReport) {
    if value.is_some_and(|value| value < -1) {
        report.error(format!("{field} must be -1 or non-negative"));
    }
}

fn check_database(path: &Path, options: &CheckOptions) -> CheckReport {
    let mut report = CheckReport::new(path, InputKind::Database);
    let connection = match Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(connection) => connection,
        Err(error) => {
            report.error(error.to_string());
            return report;
        }
    };
    if let Err(error) = connection.execute_batch("PRAGMA query_only=ON") {
        report.error(error.to_string());
        return report;
    }

    if let Err(error) = check_database_inner(&connection, path, options, &mut report) {
        report.error(error.to_string());
    }
    report
}

fn check_database_inner(
    connection: &Connection,
    path: &Path,
    options: &CheckOptions,
    report: &mut CheckReport,
) -> Result<()> {
    let tables = database_tables(connection)?;
    for required in ["entries", "blobs", "pages", "imports"] {
        if !tables.contains(required) {
            report.error(format!("missing required table '{required}'"));
        }
    }
    if !report.errors.is_empty() {
        return Ok(());
    }

    // A database-wide quick_check invokes FTS5's integrity hook, which attempts
    // an internal write and therefore fails on a deliberately read-only check.
    // Check each ordinary table instead and validate FTS references below.
    for table in ["entries", "blobs", "pages", "imports", "graphql_fields"] {
        if !tables.contains(table) {
            continue;
        }
        let mut quick_check = connection.prepare(&format!("PRAGMA quick_check('{table}')"))?;
        let rows = quick_check.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let message = row?;
            if message != "ok" {
                report.error(format!("SQLite quick_check({table}): {message}"));
            }
        }
    }

    let entry_columns = table_columns(connection, "entries")?;
    for required in [
        "id",
        "import_id",
        "started_at",
        "method",
        "url",
        "status",
        "request_body_hash",
        "response_body_hash",
        "entry_hash",
    ] {
        if !entry_columns.contains(required) {
            report.error(format!(
                "entries table is missing current column '{required}'"
            ));
        }
    }

    report.stats.entries = Some(query_count(connection, "entries")?);
    report.stats.pages = Some(query_count(connection, "pages")?);
    report.stats.imports = Some(query_count(connection, "imports")?);
    report.stats.blobs = Some(query_count(connection, "blobs")?);

    let foreign_key_failures: u64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_failures > 0 {
        report.error(format!("{foreign_key_failures} foreign-key violation(s)"));
    }

    for (column, label) in [
        ("request_body_hash", "request body"),
        ("response_body_hash", "response body"),
        ("response_body_hash_raw", "raw response body"),
    ] {
        if !entry_columns.contains(column) {
            continue;
        }
        let sql = format!(
            "SELECT COUNT(*) FROM entries e LEFT JOIN blobs b ON e.{column}=b.hash WHERE e.{column} IS NOT NULL AND b.hash IS NULL"
        );
        let count: u64 = connection.query_row(&sql, [], |row| row.get(0))?;
        if count > 0 {
            report.error(format!(
                "{count} {label} reference(s) point to missing blobs"
            ));
        }
    }

    if tables.contains("response_body_fts") {
        let missing: u64 = connection.query_row(
            "SELECT COUNT(*) FROM response_body_fts f LEFT JOIN blobs b ON f.hash=b.hash WHERE b.hash IS NULL",
            [],
            |row| row.get(0),
        )?;
        if missing > 0 {
            report.error(format!("FTS contains {missing} hash(es) with no blob"));
        }
    } else {
        report.warn("response_body_fts table is missing; run harlite fts-rebuild");
    }

    let incomplete: u64 = connection.query_row(
        "SELECT COUNT(*) FROM imports WHERE COALESCE(status, '') != 'complete'",
        [],
        |row| row.get(0),
    )?;
    if incomplete > 0 {
        report.warn(format!("{incomplete} import(s) are not marked complete"));
    }

    check_blobs(connection, path, options, report)?;
    if tables.contains("response_body_fts") {
        check_fts_contents(connection, path, options, report)?;
    }
    Ok(())
}

fn check_fts_contents(
    connection: &Connection,
    database: &Path,
    options: &CheckOptions,
    report: &mut CheckReport,
) -> Result<()> {
    let duplicates: u64 = connection.query_row(
        "SELECT COUNT(*) FROM (SELECT hash FROM response_body_fts GROUP BY hash HAVING COUNT(*) > 1)",
        [],
        |row| row.get(0),
    )?;
    if duplicates > 0 {
        report.error(format!(
            "FTS contains {duplicates} duplicate blob hash(es); run harlite fts-rebuild"
        ));
    }

    let policy = ExternalPathPolicy::new(
        database,
        options.allow_external_paths,
        options.external_path_root.as_deref(),
    )?;
    let mut statement = connection.prepare(
        "SELECT f.hash, f.body, b.content, b.size, b.external_path
         FROM response_body_fts f JOIN blobs b ON f.hash=b.hash
         ORDER BY f.hash",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;

    for row in rows {
        let (hash, indexed_body, mut content, size, external_path) = row?;
        let Some(indexed_body) = indexed_body else {
            report.error(format!("FTS body for blob {hash} is NULL"));
            continue;
        };
        if content.is_empty() && size > 0 {
            if !options.allow_external_paths {
                continue;
            }
            let Some(external_path) = external_path else {
                continue;
            };
            let Some(path) = policy.resolve_file(&external_path) else {
                continue;
            };
            let Ok(bytes) = std::fs::read(path) else {
                continue;
            };
            content = bytes;
        }
        match std::str::from_utf8(&content) {
            Ok(text) if text == indexed_body => {}
            Ok(_) => report.error(format!(
                "FTS body for blob {hash} does not match blob content; run harlite fts-rebuild"
            )),
            Err(_) => report.error(format!(
                "FTS body for blob {hash} references non-UTF-8 content; run harlite fts-rebuild"
            )),
        }
    }
    Ok(())
}

fn database_tables(connection: &Connection) -> Result<HashSet<String>> {
    let mut statement =
        connection.prepare("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<HashSet<_>, _>>()?)
}

fn table_columns(connection: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    Ok(rows.collect::<std::result::Result<HashSet<_>, _>>()?)
}

fn query_count(connection: &Connection, table: &str) -> Result<u64> {
    Ok(
        connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

fn check_blobs(
    connection: &Connection,
    database: &Path,
    options: &CheckOptions,
    report: &mut CheckReport,
) -> Result<()> {
    let policy = ExternalPathPolicy::new(
        database,
        options.allow_external_paths,
        options.external_path_root.as_deref(),
    )?;
    let mut statement =
        connection.prepare("SELECT hash, size, content, external_path FROM blobs ORDER BY hash")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;

    let mut external_count = 0u64;
    for row in rows {
        let (hash, size, content, external_path) = row?;
        if size < 0 {
            report.error(format!("blob {hash}: size is negative"));
        }
        if !content.is_empty() {
            validate_blob_bytes(&hash, size, &content, report);
            continue;
        }
        if size == 0 {
            validate_blob_bytes(&hash, size, &content, report);
            if external_path.is_none() {
                continue;
            }
        }
        let Some(external_path) = external_path else {
            report.error(format!(
                "blob {hash}: content is empty and external_path is missing"
            ));
            continue;
        };
        external_count += 1;
        if options.allow_external_paths {
            match policy.resolve_file(&external_path) {
                Some(path) => match std::fs::read(path) {
                    Ok(bytes) => validate_blob_bytes(&hash, size, &bytes, report),
                    Err(error) => report.error(format!(
                        "blob {hash}: external content cannot be read: {error}"
                    )),
                },
                None => report.error(format!(
                    "blob {hash}: external_path is missing or outside the trusted root"
                )),
            }
        }
    }
    report.stats.external_blobs = Some(external_count);
    if external_count > 0 && !options.allow_external_paths {
        report.warn(format!(
            "{external_count} external blob(s) were not verified; use --allow-external-paths with --external-path-root"
        ));
    }
    Ok(())
}

fn validate_blob_bytes(hash: &str, size: i64, bytes: &[u8], report: &mut CheckReport) {
    if size != bytes.len() as i64 {
        report.error(format!(
            "blob {hash}: recorded size {size} differs from {} bytes",
            bytes.len()
        ));
    }
    let actual_hash = blake3::hash(bytes).to_hex().to_string();
    if actual_hash != hash {
        report.error(format!("blob {hash}: content hash is {actual_hash}"));
    }
}

fn write_report(report: &CheckReport, json: bool) -> Result<()> {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    if json {
        serde_json::to_writer_pretty(&mut output, report)?;
        writeln!(output)?;
        return Ok(());
    }

    writeln!(output, "Input: {}", report.input)?;
    writeln!(
        output,
        "Type: {}",
        match report.input_kind {
            InputKind::Har => "HAR",
            InputKind::Database => "SQLite",
        }
    )?;
    writeln!(output, "Valid: {}", if report.valid { "yes" } else { "no" })?;
    if let Some(entries) = report.stats.entries {
        writeln!(output, "Entries: {entries}")?;
    }
    if let Some(blobs) = report.stats.blobs {
        writeln!(output, "Blobs: {blobs}")?;
    }
    for error in &report.errors {
        writeln!(output, "ERROR: {error}")?;
    }
    for warning in &report.warnings {
        writeln!(output, "WARNING: {warning}")?;
    }
    Ok(())
}
