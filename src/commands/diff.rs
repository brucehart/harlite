use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use regex::Regex;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::db::{load_entries, EntryQuery, EntryRow};
use crate::error::{HarliteError, Result};
use crate::har::{parse_har_file, Entry as HarEntry, Header};

use super::{csv::write_csv_field, OutputFormat};

const FLOAT_TOLERANCE: f64 = 1e-6;

pub struct DiffOptions {
    pub format: OutputFormat,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub url_regex: Vec<String>,
    pub fail_on: Vec<DiffFailOn>,
    pub max_total_regression_ms: Option<f64>,
    pub max_ttfb_regression_ms: Option<f64>,
    pub max_response_size_increase: Option<i64>,
    pub max_new_errors: Option<usize>,
    pub ignore_query_params: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[clap(rename_all = "kebab-case")]
pub enum DiffFailOn {
    Any,
    Added,
    Changed,
    Removed,
    NewErrors,
    Regression,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EntryKey {
    method: String,
    url: String,
}

#[derive(Clone, Debug)]
struct EntrySnapshot {
    method: String,
    url: String,
    host: Option<String>,
    status: Option<i32>,
    total_ms: Option<f64>,
    ttfb_ms: Option<f64>,
    request_headers: HashMap<String, String>,
    response_headers: HashMap<String, String>,
    request_body_size: Option<i64>,
    response_body_size: Option<i64>,
}

#[derive(Serialize)]
struct DiffRow {
    change: String,
    method: String,
    url: String,
    status_left: Option<i32>,
    status_right: Option<i32>,
    total_ms_left: Option<f64>,
    total_ms_right: Option<f64>,
    delta_total_ms: Option<f64>,
    ttfb_ms_left: Option<f64>,
    ttfb_ms_right: Option<f64>,
    delta_ttfb_ms: Option<f64>,
    request_header_changes: Option<usize>,
    response_header_changes: Option<usize>,
    request_body_size_left: Option<i64>,
    request_body_size_right: Option<i64>,
    response_body_size_left: Option<i64>,
    response_body_size_right: Option<i64>,
    delta_request_body_size: Option<i64>,
    delta_response_body_size: Option<i64>,
}

#[derive(Clone, Debug)]
struct Filters {
    host_set: HashSet<String>,
    method_set: HashSet<String>,
    status_set: HashSet<i32>,
    url_regexes: Vec<Regex>,
}

pub fn run_diff(left: PathBuf, right: PathBuf, options: &DiffOptions) -> Result<()> {
    if left == Path::new("-") && right == Path::new("-") {
        return Err(HarliteError::InvalidArgs(
            "Only one diff input may read from standard input".to_string(),
        ));
    }
    validate_gate_options(options)?;
    let left_is_db = is_db_path(&left);
    let right_is_db = is_db_path(&right);
    if left_is_db != right_is_db {
        return Err(HarliteError::InvalidArgs(
            "diff expects two HAR files or two SQLite databases".to_string(),
        ));
    }

    let filters = build_filters(options)?;

    let left_entries = if left_is_db {
        load_entries_from_db(&left, options, &filters)?
    } else {
        load_entries_from_har(&left, &filters)?
    };

    let right_entries = if right_is_db {
        load_entries_from_db(&right, options, &filters)?
    } else {
        load_entries_from_har(&right, &filters)?
    };

    let rows = diff_entries(left_entries, right_entries, &options.ignore_query_params);

    match options.format {
        OutputFormat::Json => write_json(&rows),
        OutputFormat::Csv => write_csv(&rows),
        OutputFormat::Table => write_table(&rows),
    }?;

    enforce_diff_gates(&rows, options)
}

fn validate_gate_options(options: &DiffOptions) -> Result<()> {
    if options
        .max_total_regression_ms
        .is_some_and(|value| !value.is_finite() || value < 0.0)
        || options
            .max_ttfb_regression_ms
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        || options
            .max_response_size_increase
            .is_some_and(|value| value < 0)
    {
        return Err(HarliteError::InvalidArgs(
            "Diff regression thresholds must be finite and non-negative".to_string(),
        ));
    }
    Ok(())
}

fn is_db_path(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
        return is_sqlite_file(path);
    };

    let ext = ext.to_ascii_lowercase();
    if matches!(ext.as_str(), "db" | "db3" | "sqlite" | "sqlite3") {
        return true;
    }

    is_sqlite_file(path)
}

fn is_sqlite_file(path: &Path) -> bool {
    let Ok(mut file) = File::open(path) else {
        return false;
    };

    let mut header = [0u8; 16];
    let Ok(read_len) = file.read(&mut header) else {
        return false;
    };
    if read_len < 16 {
        return false;
    }

    header == *b"SQLite format 3\0"
}

fn build_filters(options: &DiffOptions) -> Result<Filters> {
    let host_set = options
        .host
        .iter()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    let method_set = options
        .method
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    let status_set = options.status.iter().copied().collect();
    let url_regexes = options
        .url_regex
        .iter()
        .map(|s| Regex::new(s))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(Filters {
        host_set,
        method_set,
        status_set,
        url_regexes,
    })
}

fn load_entries_from_db(
    path: &Path,
    options: &DiffOptions,
    filters: &Filters,
) -> Result<Vec<EntrySnapshot>> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    let query = EntryQuery {
        statuses: options.status.clone(),
        ..EntryQuery::default()
    };

    let entries = load_entries(&conn, &query)?;
    Ok(entries
        .into_iter()
        .map(EntrySnapshot::from_db)
        .filter(|e| entry_matches_filters(e, filters))
        .collect())
}

fn load_entries_from_har(path: &Path, filters: &Filters) -> Result<Vec<EntrySnapshot>> {
    let har = parse_har_file(path)?;
    Ok(har
        .log
        .entries
        .iter()
        .map(EntrySnapshot::from_har)
        .filter(|e| entry_matches_filters(e, filters))
        .collect())
}

fn entry_matches_filters(entry: &EntrySnapshot, filters: &Filters) -> bool {
    if !filters.host_set.is_empty() {
        let host = entry.host.as_deref().map(|h| h.to_ascii_lowercase());
        if host
            .as_deref()
            .is_none_or(|h| !filters.host_set.contains(h))
        {
            return false;
        }
    }

    if !filters.method_set.is_empty() && !filters.method_set.contains(&entry.method) {
        return false;
    }

    if !filters.status_set.is_empty()
        && entry
            .status
            .is_none_or(|s| !filters.status_set.contains(&s))
    {
        return false;
    }

    if !filters.url_regexes.is_empty()
        && (entry.url.is_empty() || !filters.url_regexes.iter().any(|re| re.is_match(&entry.url)))
    {
        return false;
    }

    true
}

impl EntrySnapshot {
    fn from_db(row: EntryRow) -> Self {
        let method = row.method.unwrap_or_default().to_ascii_uppercase();
        let url = row.url.unwrap_or_default();
        let host = row
            .host
            .or_else(|| host_from_url(&url))
            .map(|h| h.to_ascii_lowercase());

        EntrySnapshot {
            method,
            url,
            host,
            status: row.status,
            total_ms: row.time_ms,
            ttfb_ms: normalize_ms(row.wait_ms),
            request_headers: headers_from_json(row.request_headers.as_deref()),
            response_headers: headers_from_json(row.response_headers.as_deref()),
            request_body_size: normalize_i64(row.request_body_size),
            response_body_size: normalize_i64(row.response_body_size),
        }
    }

    fn from_har(entry: &HarEntry) -> Self {
        let method = entry.request.method.to_ascii_uppercase();
        let url = entry.request.url.clone();
        let host = host_from_url(&url).map(|h| h.to_ascii_lowercase());

        EntrySnapshot {
            method,
            url,
            host,
            status: Some(entry.response.status),
            total_ms: Some(entry.time),
            ttfb_ms: entry.timings.as_ref().map(|t| t.wait).filter(|v| *v >= 0.0),
            request_headers: headers_from_list(&entry.request.headers),
            response_headers: headers_from_list(&entry.response.headers),
            request_body_size: har_request_body_size(entry),
            response_body_size: har_response_body_size(entry),
        }
    }
}

fn host_from_url(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

fn normalize_i64(value: Option<i64>) -> Option<i64> {
    match value {
        Some(v) if v >= 0 => Some(v),
        _ => None,
    }
}

fn normalize_ms(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v >= 0.0 => Some(v),
        _ => None,
    }
}

fn har_request_body_size(entry: &HarEntry) -> Option<i64> {
    if let Some(size) = entry.request.body_size {
        return normalize_i64(Some(size));
    }
    entry
        .request
        .post_data
        .as_ref()
        .and_then(|p| p.text.as_ref())
        .map(|t| t.len() as i64)
}

fn har_response_body_size(entry: &HarEntry) -> Option<i64> {
    if entry.response.content.size >= 0 {
        return Some(entry.response.content.size);
    }
    normalize_i64(entry.response.body_size)
}

fn headers_from_list(headers: &[Header]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for header in headers {
        map.insert(
            header.name.trim().to_ascii_lowercase(),
            header.value.clone(),
        );
    }
    map
}

fn headers_from_json(json: Option<&str>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Some(json) = json else {
        return map;
    };

    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return map;
    };

    let serde_json::Value::Object(obj) = value else {
        return map;
    };

    for (k, v) in obj {
        let value_str = match v {
            serde_json::Value::String(s) => s,
            _ => v.to_string(),
        };
        map.insert(k.trim().to_ascii_lowercase(), value_str);
    }

    map
}

fn diff_entries(
    left: Vec<EntrySnapshot>,
    right: Vec<EntrySnapshot>,
    ignore_query_params: &[String],
) -> Vec<DiffRow> {
    let mut left_map: HashMap<EntryKey, Vec<EntrySnapshot>> = HashMap::new();
    let mut right_map: HashMap<EntryKey, Vec<EntrySnapshot>> = HashMap::new();

    for entry in left {
        let key = EntryKey {
            method: entry.method.clone(),
            url: comparison_url(&entry.url, ignore_query_params),
        };
        left_map.entry(key).or_default().push(entry);
    }

    for entry in right {
        let key = EntryKey {
            method: entry.method.clone(),
            url: comparison_url(&entry.url, ignore_query_params),
        };
        right_map.entry(key).or_default().push(entry);
    }

    let mut keys: Vec<EntryKey> = left_map.keys().chain(right_map.keys()).cloned().collect();
    keys.sort_by(|a, b| a.method.cmp(&b.method).then(a.url.cmp(&b.url)));
    keys.dedup();

    let mut rows: Vec<DiffRow> = Vec::new();

    for key in keys {
        let left_entries = left_map.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let right_entries = right_map.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let max_len = left_entries.len().max(right_entries.len());

        for idx in 0..max_len {
            let left_entry = left_entries.get(idx);
            let right_entry = right_entries.get(idx);

            if let Some(row) = diff_entry(left_entry, right_entry) {
                rows.push(row);
            }
        }
    }

    rows
}

fn comparison_url(raw: &str, ignored_params: &[String]) -> String {
    if ignored_params.is_empty() {
        return raw.to_string();
    }
    let ignored: HashSet<String> = ignored_params
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let (without_fragment, fragment) = raw
        .split_once('#')
        .map(|(url, fragment)| (url, Some(fragment)))
        .unwrap_or((raw, None));
    let Some((prefix, query)) = without_fragment.split_once('?') else {
        return raw.to_string();
    };
    let mut removed = false;
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| {
            let raw_name = pair.split_once('=').map_or(*pair, |(name, _)| name);
            let decoded_name = url::form_urlencoded::parse(raw_name.as_bytes())
                .next()
                .map(|(name, _)| name.into_owned())
                .unwrap_or_else(|| raw_name.to_string());
            let should_remove = ignored.contains(&decoded_name.to_ascii_lowercase());
            removed |= should_remove;
            !should_remove
        })
        .collect();
    if !removed {
        return raw.to_string();
    }

    let mut result = prefix.to_string();
    if !kept.is_empty() {
        result.push('?');
        result.push_str(&kept.join("&"));
    }
    if let Some(fragment) = fragment {
        result.push('#');
        result.push_str(fragment);
    }
    result
}

fn enforce_diff_gates(rows: &[DiffRow], options: &DiffOptions) -> Result<()> {
    let added = rows.iter().filter(|row| row.change == "added").count();
    let changed = rows.iter().filter(|row| row.change == "changed").count();
    let removed = rows.iter().filter(|row| row.change == "removed").count();
    let new_errors = rows
        .iter()
        .filter(|row| {
            row.status_right.is_some_and(|status| status >= 400)
                && row.status_left.is_none_or(|status| status < 400)
        })
        .count();
    let regressions = rows
        .iter()
        .filter(|row| {
            row.delta_total_ms
                .is_some_and(|delta| regression_exceeds(delta, 0.0))
                || row
                    .delta_ttfb_ms
                    .is_some_and(|delta| regression_exceeds(delta, 0.0))
                || row.delta_response_body_size.is_some_and(|delta| delta > 0)
        })
        .count();

    let mut violations = Vec::new();
    for condition in &options.fail_on {
        let (count, label) = match condition {
            DiffFailOn::Any => (rows.len(), "difference(s)"),
            DiffFailOn::Added => (added, "added request(s)"),
            DiffFailOn::Changed => (changed, "changed request(s)"),
            DiffFailOn::Removed => (removed, "removed request(s)"),
            DiffFailOn::NewErrors => (new_errors, "new HTTP error(s)"),
            DiffFailOn::Regression => (regressions, "regression(s)"),
        };
        if count > 0 {
            violations.push(format!("{count} {label}"));
        }
    }

    if let Some(limit) = options.max_new_errors {
        if new_errors > limit {
            violations.push(format!("{new_errors} new HTTP error(s) exceed {limit}"));
        }
    }
    if let Some(limit) = options.max_total_regression_ms {
        let count = rows
            .iter()
            .filter(|row| {
                row.delta_total_ms
                    .is_some_and(|delta| regression_exceeds(delta, limit))
            })
            .count();
        if count > 0 {
            violations.push(format!(
                "{count} total-time regression(s) exceed {limit:.2}ms"
            ));
        }
    }
    if let Some(limit) = options.max_ttfb_regression_ms {
        let count = rows
            .iter()
            .filter(|row| {
                row.delta_ttfb_ms
                    .is_some_and(|delta| regression_exceeds(delta, limit))
            })
            .count();
        if count > 0 {
            violations.push(format!("{count} TTFB regression(s) exceed {limit:.2}ms"));
        }
    }
    if let Some(limit) = options.max_response_size_increase {
        let count = rows
            .iter()
            .filter(|row| {
                row.delta_response_body_size
                    .is_some_and(|delta| delta > limit)
            })
            .count();
        if count > 0 {
            violations.push(format!(
                "{count} response-size increase(s) exceed {limit} bytes"
            ));
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(HarliteError::ThresholdExceeded(violations.join("; ")))
    }
}

fn diff_entry(left: Option<&EntrySnapshot>, right: Option<&EntrySnapshot>) -> Option<DiffRow> {
    match (left, right) {
        (None, None) => None,
        (Some(l), None) => Some(DiffRow {
            change: "removed".to_string(),
            method: l.method.clone(),
            url: l.url.clone(),
            status_left: l.status,
            status_right: None,
            total_ms_left: l.total_ms,
            total_ms_right: None,
            delta_total_ms: None,
            ttfb_ms_left: l.ttfb_ms,
            ttfb_ms_right: None,
            delta_ttfb_ms: None,
            request_header_changes: None,
            response_header_changes: None,
            request_body_size_left: l.request_body_size,
            request_body_size_right: None,
            response_body_size_left: l.response_body_size,
            response_body_size_right: None,
            delta_request_body_size: None,
            delta_response_body_size: None,
        }),
        (None, Some(r)) => Some(DiffRow {
            change: "added".to_string(),
            method: r.method.clone(),
            url: r.url.clone(),
            status_left: None,
            status_right: r.status,
            total_ms_left: None,
            total_ms_right: r.total_ms,
            delta_total_ms: None,
            ttfb_ms_left: None,
            ttfb_ms_right: r.ttfb_ms,
            delta_ttfb_ms: None,
            request_header_changes: None,
            response_header_changes: None,
            request_body_size_left: None,
            request_body_size_right: r.request_body_size,
            response_body_size_left: None,
            response_body_size_right: r.response_body_size,
            delta_request_body_size: None,
            delta_response_body_size: None,
        }),
        (Some(l), Some(r)) => {
            let req_header_changes = count_header_changes(&l.request_headers, &r.request_headers);
            let resp_header_changes =
                count_header_changes(&l.response_headers, &r.response_headers);

            let status_changed = l.status != r.status;
            let total_changed = f64_changed(l.total_ms, r.total_ms);
            let ttfb_changed = f64_changed(l.ttfb_ms, r.ttfb_ms);
            let request_size_changed = l.request_body_size != r.request_body_size;
            let response_size_changed = l.response_body_size != r.response_body_size;

            let any_changed = status_changed
                || total_changed
                || ttfb_changed
                || request_size_changed
                || response_size_changed
                || req_header_changes > 0
                || resp_header_changes > 0;

            if !any_changed {
                return None;
            }

            Some(DiffRow {
                change: "changed".to_string(),
                method: l.method.clone(),
                url: l.url.clone(),
                status_left: l.status,
                status_right: r.status,
                total_ms_left: l.total_ms,
                total_ms_right: r.total_ms,
                delta_total_ms: diff_f64(l.total_ms, r.total_ms),
                ttfb_ms_left: l.ttfb_ms,
                ttfb_ms_right: r.ttfb_ms,
                delta_ttfb_ms: diff_f64(l.ttfb_ms, r.ttfb_ms),
                request_header_changes: Some(req_header_changes),
                response_header_changes: Some(resp_header_changes),
                request_body_size_left: l.request_body_size,
                request_body_size_right: r.request_body_size,
                response_body_size_left: l.response_body_size,
                response_body_size_right: r.response_body_size,
                delta_request_body_size: diff_i64(l.request_body_size, r.request_body_size),
                delta_response_body_size: diff_i64(l.response_body_size, r.response_body_size),
            })
        }
    }
}

fn count_header_changes(left: &HashMap<String, String>, right: &HashMap<String, String>) -> usize {
    let mut keys: HashSet<&String> = HashSet::new();
    keys.extend(left.keys());
    keys.extend(right.keys());

    keys.into_iter()
        .filter(|k| left.get(*k) != right.get(*k))
        .count()
}

fn f64_changed(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (None, None) => false,
        (Some(a), Some(b)) => (a - b).abs() > FLOAT_TOLERANCE,
        _ => true,
    }
}

fn regression_exceeds(delta: f64, limit: f64) -> bool {
    delta - limit > FLOAT_TOLERANCE
}

fn diff_f64(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(b - a),
        _ => None,
    }
}

fn diff_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(b - a),
        _ => None,
    }
}

fn write_json(rows: &[DiffRow]) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, &rows)?;
    handle.write_all(b"\n")?;
    Ok(())
}

fn write_csv(rows: &[DiffRow]) -> Result<()> {
    let columns = diff_columns();
    let mut out = io::stdout().lock();
    write_csv_row(&mut out, columns.iter().copied())?;
    for row in rows {
        let fields = diff_row_values(row);
        write_csv_row(&mut out, fields.iter().map(|s| s.as_str()))?;
    }
    Ok(())
}

fn write_table(rows: &[DiffRow]) -> Result<()> {
    let columns = diff_columns();
    let mut data: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for row in rows {
        data.push(diff_row_values(row));
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &data {
        for (i, value) in row.iter().enumerate() {
            widths[i] = widths[i].max(value.chars().count());
        }
    }

    for width in &mut widths {
        *width = (*width).clamp(8, 80);
    }

    let mut out = io::stdout().lock();
    write_table_row(&mut out, columns.iter().copied(), &widths)?;
    write_table_sep(&mut out, &widths)?;
    for row in data {
        write_table_row(&mut out, row.iter().map(|s| s.as_str()), &widths)?;
    }
    Ok(())
}

fn diff_columns() -> Vec<&'static str> {
    vec![
        "change",
        "method",
        "url",
        "status_left",
        "status_right",
        "delta_total_ms",
        "delta_ttfb_ms",
        "req_hdr_changes",
        "resp_hdr_changes",
        "delta_req_body",
        "delta_resp_body",
    ]
}

fn diff_row_values(row: &DiffRow) -> Vec<String> {
    vec![
        row.change.clone(),
        row.method.clone(),
        row.url.clone(),
        opt_i32(row.status_left),
        opt_i32(row.status_right),
        opt_f64(row.delta_total_ms),
        opt_f64(row.delta_ttfb_ms),
        opt_usize(row.request_header_changes),
        opt_usize(row.response_header_changes),
        opt_i64_signed(row.delta_request_body_size),
        opt_i64_signed(row.delta_response_body_size),
    ]
}

fn opt_i32(value: Option<i32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_usize(value: Option<usize>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_i64_signed(value: Option<i64>) -> String {
    value.map(|v| format!("{v:+}")).unwrap_or_default()
}

fn opt_f64(value: Option<f64>) -> String {
    value.map(|v| format!("{v:.2}")).unwrap_or_default()
}

fn write_csv_row<'a, I>(out: &mut impl Write, fields: I) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut first = true;
    for field in fields {
        if !first {
            out.write_all(b",")?;
        }
        first = false;
        write_csv_field(out, field)?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_table_row<'a, I>(out: &mut impl Write, fields: I, widths: &[usize]) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    for (i, field) in fields.into_iter().enumerate() {
        if i > 0 {
            out.write_all(b" | ")?;
        }
        let width = widths.get(i).copied().unwrap_or(0);
        let field = truncate(field, width);
        out.write_all(field.as_bytes())?;
        let field_len = field.chars().count();
        if field_len < width {
            out.write_all(" ".repeat(width - field_len).as_bytes())?;
        }
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_table_sep(out: &mut impl Write, widths: &[usize]) -> Result<()> {
    for (i, w) in widths.iter().copied().enumerate() {
        if i > 0 {
            out.write_all(b"-+-")?;
        }
        out.write_all("-".repeat(w).as_bytes())?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    if max <= 3 {
        return "...".to_string();
    }
    let mut end = 0usize;
    for (i, ch) in s.char_indices() {
        if i >= max - 3 {
            break;
        }
        end = i + ch.len_utf8();
    }
    let mut out = s[..end].to_string();
    out.push_str("...");
    out
}
