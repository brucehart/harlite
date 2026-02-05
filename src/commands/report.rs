use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use regex::Regex;
use rusqlite::Connection;
use serde::Serialize;
use url::Url;

use super::entry_filter::{load_entries_with_filters, EntryFilterOptions};
use super::waterfall::WaterfallGroupBy;
use crate::commands::util::parse_timestamp;
use crate::db::{ensure_schema_upgrades, load_pages_for_imports, EntryRow, PageRow};
use crate::error::{HarliteError, Result};
use crate::har::{parse_har_file, Entry as HarEntry, Page as HarPage};
use crate::size;

pub struct ReportOptions {
    pub output: Option<PathBuf>,
    pub title: Option<String>,
    pub top: usize,
    pub slow_total_ms: f64,
    pub slow_ttfb_ms: f64,
    pub waterfall_limit: usize,
    pub group_by: WaterfallGroupBy,
    pub page: Vec<String>,
    pub filters: EntryFilterOptions,
}

#[derive(Debug, Clone)]
struct ReportEntry {
    started_at: String,
    started_at_dt: DateTime<Utc>,
    start_ms: f64,
    total_ms: f64,
    timings: TimingsMs,
    method: String,
    url: String,
    host: String,
    path: Option<String>,
    status: Option<i32>,
    mime: Option<String>,
    request_body_size: Option<i64>,
    response_body_size: Option<i64>,
    page_id: Option<String>,
    page_title: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct TimingsMs {
    blocked: Option<f64>,
    dns: Option<f64>,
    connect: Option<f64>,
    ssl: Option<f64>,
    send: Option<f64>,
    wait: Option<f64>,
    receive: Option<f64>,
}

#[derive(Debug, Clone)]
struct ReportPage {
    id: String,
    title: Option<String>,
    started_at: Option<String>,
    on_content_load_ms: Option<f64>,
    on_load_ms: Option<f64>,
}

#[derive(Debug, Clone)]
struct WaterfallGroup {
    name: String,
    start_ms: f64,
    entries: Vec<ReportEntry>,
}

#[derive(Serialize)]
struct JsonReport<'a> {
    title: &'a str,
    generated_at: String,
    input: String,
    total_entries: usize,
    rendered_entries: usize,
    time_range: Option<TimeRange>,
    status_counts: StatusCounts,
    slow: SlowSummary,
    top_slowest_total: Vec<SlowRow>,
    top_slowest_ttfb: Vec<SlowRow>,
    top_error_endpoints: Vec<ErrorEndpointRow>,
    pages: Vec<PageSummary>,
    waterfall: WaterfallSummary<'a>,
}

#[derive(Serialize)]
struct TimeRange {
    start: String,
    end: String,
    total_ms: f64,
}

#[derive(Serialize, Default)]
struct StatusCounts {
    s2xx: usize,
    s3xx: usize,
    s4xx: usize,
    s5xx: usize,
    none: usize,
}

#[derive(Serialize)]
struct SlowSummary {
    slow_total_threshold_ms: f64,
    slow_ttfb_threshold_ms: f64,
    slow_total_count: usize,
    slow_ttfb_count: usize,
}

#[derive(Serialize)]
struct SlowRow {
    started_at: String,
    method: String,
    status: Option<i32>,
    host: String,
    url: String,
    total_ms: f64,
    ttfb_ms: Option<f64>,
}

#[derive(Serialize)]
struct ErrorEndpointRow {
    endpoint: String,
    host: String,
    path: String,
    count: usize,
    sample_url: String,
}

#[derive(Serialize)]
struct PageSummary {
    id: String,
    title: Option<String>,
    started_at: Option<String>,
    on_content_load_ms: Option<f64>,
    on_load_ms: Option<f64>,
}

#[derive(Serialize)]
struct WaterfallSummary<'a> {
    group_by: &'a str,
    total_ms: f64,
    groups: Vec<WaterfallGroupJson>,
}

#[derive(Serialize)]
struct WaterfallGroupJson {
    name: String,
    entries: Vec<WaterfallEntryJson>,
}

#[derive(Serialize)]
struct WaterfallEntryJson {
    started_at: String,
    start_ms: f64,
    total_ms: f64,
    method: String,
    status: Option<i32>,
    host: String,
    url: String,
    mime: Option<String>,
    phases: PhaseJson,
}

#[derive(Serialize, Default)]
struct PhaseJson {
    blocked: f64,
    dns: f64,
    connect: f64,
    ssl: f64,
    send: f64,
    wait: f64,
    receive: f64,
}

fn normalize_ms(v: Option<f64>) -> Option<f64> {
    match v {
        Some(x) if x >= 0.0 => Some(x),
        _ => None,
    }
}

fn normalize_ms0(v: Option<f64>) -> f64 {
    normalize_ms(v).unwrap_or(0.0)
}

fn normalize_i64(v: Option<i64>) -> Option<i64> {
    match v {
        Some(x) if x >= 0 => Some(x),
        _ => None,
    }
}

fn open_output(path: &Path) -> Result<Box<dyn Write>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout().lock()));
    }
    Ok(Box::new(BufWriter::new(File::create(path)?)))
}

fn default_output_for_input(input: &Path) -> PathBuf {
    // If the input already has an extension, swap it for .html; otherwise append .html.
    let mut out = input.to_path_buf();
    match input.extension().and_then(|s| s.to_str()) {
        Some(_) => {
            out.set_extension("html");
            out
        }
        None => PathBuf::from(format!("{}.html", input.display())),
    }
}

enum InputKind {
    Db,
    Har,
}

fn detect_input_kind(input: &Path) -> Result<InputKind> {
    let lower = input
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if lower == "db" || input
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .ends_with(".db")
    {
        return Ok(InputKind::Db);
    }
    if lower == "har" || lower == "json" || input
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .contains(".har")
    {
        return Ok(InputKind::Har);
    }

    // Heuristic fallback: try SQLite, then HAR.
    if let Ok(conn) = Connection::open(input) {
        let has_entries: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='entries'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if has_entries > 0 {
            return Ok(InputKind::Db);
        }
    }

    // Try parsing as HAR to decide.
    parse_har_file(input).map_err(|e| {
        HarliteError::InvalidArgs(format!(
            "Unable to detect input type for {} (not a harlite DB; HAR parse failed: {e})",
            input.display()
        ))
    })?;
    Ok(InputKind::Har)
}

fn escape_html(s: &str) -> Cow<'_, str> {
    if !s.contains(['&', '<', '>', '"', '\'']) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len() + 16);
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    Cow::Owned(out)
}

fn escape_script_json(json: &str) -> Cow<'_, str> {
    // Make JSON safe for embedding in a <script type="application/json"> tag.
    //
    // - Escape '<'/'>' to avoid forming HTML tags like </script>.
    // - Escape U+2028/U+2029 since they are treated as line terminators in JS contexts.
    if !json
        .chars()
        .any(|ch| matches!(ch, '<' | '>' | '\u{2028}' | '\u{2029}'))
    {
        return Cow::Borrowed(json);
    }

    let mut out = String::with_capacity(json.len() + 16);
    for ch in json.chars() {
        match ch {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ => out.push(ch),
        }
    }
    Cow::Owned(out)
}

fn url_extension(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let path = parsed.path();
    let file = path.rsplit('/').next().unwrap_or("");
    let ext = file.rsplit('.').next()?;
    if ext == file {
        return None;
    }
    Some(ext.to_ascii_lowercase())
}

fn normalize_lower(s: &str) -> String {
    s.trim().to_ascii_lowercase()
}

fn page_matches_filter(page_id: Option<&str>, page_title: Option<&str>, filters: &[String]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let id = page_id.map(normalize_lower);
    let title = page_title.map(normalize_lower);
    filters.iter().any(|f| {
        let needle = normalize_lower(f);
        id.as_ref().is_some_and(|v| v.contains(&needle))
            || title.as_ref().is_some_and(|v| v.contains(&needle))
    })
}

fn is_navigation_entry(entry: &ReportEntry) -> bool {
    if entry.method.to_ascii_uppercase() != "GET" {
        return false;
    }
    let Some(mime) = entry.mime.as_deref() else {
        return false;
    };
    let mime = mime.to_ascii_lowercase();
    mime.contains("text/html") || mime.contains("application/xhtml+xml")
}

fn group_by_label(group_by: WaterfallGroupBy) -> &'static str {
    match group_by {
        WaterfallGroupBy::Page => "page",
        WaterfallGroupBy::Navigation => "navigation",
        WaterfallGroupBy::None => "none",
    }
}

fn build_groups(
    mut entries: Vec<ReportEntry>,
    group_by: WaterfallGroupBy,
    page_filters: &[String],
) -> Vec<WaterfallGroup> {
    if entries.is_empty() {
        return Vec::new();
    }

    entries.sort_by(|a, b| a.started_at_dt.cmp(&b.started_at_dt).then_with(|| a.url.cmp(&b.url)));

    let mut groups: Vec<WaterfallGroup> = Vec::new();
    let mut group_map: HashMap<String, usize> = HashMap::new();
    let mut nav_index = 0usize;
    let mut current_nav: Option<String> = None;

    for entry in entries {
        if !page_matches_filter(entry.page_id.as_deref(), entry.page_title.as_deref(), page_filters)
        {
            continue;
        }

        let key = match group_by {
            WaterfallGroupBy::None => "all".to_string(),
            WaterfallGroupBy::Page => {
                if let Some(pid) = entry.page_id.as_deref() {
                    format!("page:{pid}")
                } else {
                    "page:none".to_string()
                }
            }
            WaterfallGroupBy::Navigation => {
                if let Some(pid) = entry.page_id.as_deref() {
                    format!("page:{pid}")
                } else {
                    let is_nav = is_navigation_entry(&entry);
                    if current_nav.is_none() || is_nav {
                        nav_index += 1;
                        current_nav = Some(format!("nav-{nav_index}"));
                    }
                    current_nav.clone().unwrap_or_else(|| "nav-1".to_string())
                }
            }
        };

        let idx = if let Some(idx) = group_map.get(&key).copied() {
            idx
        } else {
            let name = match group_by {
                WaterfallGroupBy::None => "All Requests".to_string(),
                WaterfallGroupBy::Page | WaterfallGroupBy::Navigation => {
                    if let Some(pid) = entry.page_id.as_deref() {
                        entry.page_title.clone().unwrap_or_else(|| pid.to_string())
                    } else if key.starts_with("nav-") {
                        if entry.url.is_empty() {
                            format!("Navigation {nav_index}")
                        } else {
                            format!("Navigation {nav_index}: {}", entry.url)
                        }
                    } else {
                        "No Page".to_string()
                    }
                }
            };
            groups.push(WaterfallGroup {
                name,
                start_ms: entry.start_ms,
                entries: Vec::new(),
            });
            let idx = groups.len() - 1;
            group_map.insert(key.clone(), idx);
            idx
        };

        let group = &mut groups[idx];
        if entry.start_ms < group.start_ms {
            group.start_ms = entry.start_ms;
        }
        group.entries.push(entry);
    }

    for group in &mut groups {
        group.entries.sort_by(|a, b| {
            a.start_ms
                .partial_cmp(&b.start_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.url.cmp(&b.url))
        });
    }
    groups.sort_by(|a, b| {
        a.start_ms
            .partial_cmp(&b.start_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    groups
}

fn entry_to_report_entry_db(
    row: EntryRow,
    page_map: &HashMap<(i64, String), PageRow>,
) -> Option<ReportEntry> {
    let started_at = row.started_at?;
    let dt = DateTime::parse_from_rfc3339(&started_at).ok()?.with_timezone(&Utc);

    let url = row.url.unwrap_or_default();
    let host = row.host.unwrap_or_else(|| {
        Url::parse(&url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
            .unwrap_or_default()
    });

    let (page_id, page_title) = row
        .page_id
        .as_ref()
        .and_then(|pid| {
            page_map
                .get(&(row.import_id, pid.clone()))
                .map(|p| (Some(p.id.clone()), p.title.clone()))
        })
        .unwrap_or((row.page_id.clone(), None));

    Some(ReportEntry {
        started_at: started_at.clone(),
        started_at_dt: dt,
        start_ms: 0.0,
        total_ms: normalize_ms0(row.time_ms),
        timings: TimingsMs {
            blocked: normalize_ms(row.blocked_ms),
            dns: normalize_ms(row.dns_ms),
            connect: normalize_ms(row.connect_ms),
            ssl: normalize_ms(row.ssl_ms),
            send: normalize_ms(row.send_ms),
            wait: normalize_ms(row.wait_ms),
            receive: normalize_ms(row.receive_ms),
        },
        method: row.method.unwrap_or_else(|| "GET".to_string()),
        url,
        host,
        path: row.path,
        status: row.status,
        mime: row.response_mime_type,
        request_body_size: normalize_i64(row.request_body_size),
        response_body_size: normalize_i64(row.response_body_size),
        page_id,
        page_title,
    })
}

fn entry_to_report_entry_har(entry: HarEntry, page_title_map: &HashMap<String, Option<String>>) -> Option<ReportEntry> {
    let dt = DateTime::parse_from_rfc3339(&entry.started_date_time)
        .ok()?
        .with_timezone(&Utc);

    let url = entry.request.url.clone();
    let host = Url::parse(&url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let path = Url::parse(&url).ok().map(|u| u.path().to_string());

    let page_id = entry.pageref.clone();
    let page_title = page_id
        .as_deref()
        .and_then(|pid| page_title_map.get(pid).cloned())
        .flatten();

    let t = entry.timings;
    let timings = t.as_ref().map(|t| TimingsMs {
        blocked: normalize_ms(t.blocked),
        dns: normalize_ms(t.dns),
        connect: normalize_ms(t.connect),
        ssl: normalize_ms(t.ssl),
        send: Some(t.send).filter(|v| *v >= 0.0),
        wait: Some(t.wait).filter(|v| *v >= 0.0),
        receive: Some(t.receive).filter(|v| *v >= 0.0),
    }).unwrap_or_default();

    Some(ReportEntry {
        started_at: entry.started_date_time.clone(),
        started_at_dt: dt,
        start_ms: 0.0,
        total_ms: if entry.time >= 0.0 { entry.time } else { 0.0 },
        timings,
        method: entry.request.method.clone(),
        url,
        host,
        path,
        status: Some(entry.response.status),
        mime: entry.response.content.mime_type.clone(),
        request_body_size: normalize_i64(entry.request.body_size),
        response_body_size: normalize_i64(entry.response.body_size).or(Some(entry.response.content.size).filter(|v| *v >= 0)),
        page_id,
        page_title,
    })
}

fn apply_start_offsets(entries: &mut [ReportEntry]) -> Option<(DateTime<Utc>, DateTime<Utc>, f64)> {
    if entries.is_empty() {
        return None;
    }
    entries.sort_by(|a, b| a.started_at_dt.cmp(&b.started_at_dt).then_with(|| a.url.cmp(&b.url)));
    let base = entries.first().map(|e| e.started_at_dt)?;
    let mut max_end = 0.0;
    for e in entries.iter_mut() {
        let offset_ms = (e.started_at_dt - base).num_milliseconds() as f64;
        e.start_ms = offset_ms;
        let end = offset_ms + e.total_ms.max(0.0);
        if end > max_end {
            max_end = end;
        }
    }
    let start = base;
    let end = entries.last().map(|e| e.started_at_dt).unwrap_or(base);
    Some((start, end, max_end))
}

fn status_bucket(status: Option<i32>) -> &'static str {
    match status {
        Some(s) if (200..=299).contains(&s) => "2xx",
        Some(s) if (300..=399).contains(&s) => "3xx",
        Some(s) if (400..=499).contains(&s) => "4xx",
        Some(s) if (500..=599).contains(&s) => "5xx",
        _ => "none",
    }
}

fn summarize_status_counts(entries: &[ReportEntry]) -> StatusCounts {
    let mut c = StatusCounts::default();
    for e in entries {
        match status_bucket(e.status) {
            "2xx" => c.s2xx += 1,
            "3xx" => c.s3xx += 1,
            "4xx" => c.s4xx += 1,
            "5xx" => c.s5xx += 1,
            _ => c.none += 1,
        }
    }
    c
}

fn ttfb_ms(entry: &ReportEntry) -> Option<f64> {
    normalize_ms(entry.timings.wait)
}

fn top_slowest(entries: &[ReportEntry], top: usize) -> (Vec<SlowRow>, Vec<SlowRow>) {
    let mut by_total: Vec<&ReportEntry> = entries.iter().collect();
    by_total.sort_by(|a, b| b.total_ms.partial_cmp(&a.total_ms).unwrap_or(std::cmp::Ordering::Equal));

    let mut by_ttfb: Vec<&ReportEntry> = entries.iter().collect();
    by_ttfb.sort_by(|a, b| {
        let av = ttfb_ms(a).unwrap_or(-1.0);
        let bv = ttfb_ms(b).unwrap_or(-1.0);
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });

    let to_row = |e: &ReportEntry| SlowRow {
        started_at: e.started_at.clone(),
        method: e.method.clone(),
        status: e.status,
        host: e.host.clone(),
        url: e.url.clone(),
        total_ms: e.total_ms,
        ttfb_ms: ttfb_ms(e),
    };

    let mut out_total = by_total.into_iter().take(top).map(to_row).collect::<Vec<_>>();
    out_total.retain(|r| r.total_ms > 0.0);

    let mut out_ttfb = by_ttfb.into_iter().take(top).map(to_row).collect::<Vec<_>>();
    out_ttfb.retain(|r| r.ttfb_ms.unwrap_or(-1.0) >= 0.0);

    (out_total, out_ttfb)
}

fn count_over_threshold(entries: &[ReportEntry], threshold: f64, fetch: fn(&ReportEntry) -> Option<f64>) -> usize {
    entries
        .iter()
        .filter_map(|e| fetch(e))
        .filter(|v| *v >= threshold)
        .count()
}

fn top_error_endpoints(entries: &[ReportEntry], top: usize) -> Vec<ErrorEndpointRow> {
    let mut map: HashMap<(String, String), (usize, String)> = HashMap::new();
    for e in entries {
        let status = e.status.unwrap_or(0);
        if status < 400 {
            continue;
        }
        let host = e.host.clone();
        let path = e
            .path
            .clone()
            .or_else(|| Url::parse(&e.url).ok().map(|u| u.path().to_string()))
            .unwrap_or_else(|| "/".to_string());
        let key = (host, path);
        let entry = map.entry(key).or_insert((0, e.url.clone()));
        entry.0 += 1;
    }

    let mut rows = map
        .into_iter()
        .map(|((host, path), (count, sample_url))| ErrorEndpointRow {
            endpoint: format!("{host}{path}"),
            host,
            path,
            count,
            sample_url,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.endpoint.cmp(&b.endpoint)));
    if rows.len() > top {
        rows.truncate(top);
    }
    rows
}

fn build_pages_summary(pages: Vec<ReportPage>) -> Vec<PageSummary> {
    pages
        .into_iter()
        .map(|p| PageSummary {
            id: p.id,
            title: p.title,
            started_at: p.started_at,
            on_content_load_ms: p.on_content_load_ms,
            on_load_ms: p.on_load_ms,
        })
        .collect()
}

fn entry_matches_filters_har(
    e: &ReportEntry,
    options: &ReportOptions,
    url_regexes: &[Regex],
    exts: &std::collections::HashSet<String>,
    from_dt: Option<DateTime<Utc>>,
    to_dt: Option<DateTime<Utc>>,
    source_name: &str,
) -> bool {
    let f = &options.filters;

    if !f.url.is_empty() && !f.url.iter().any(|u| u == &e.url) {
        return false;
    }
    if !f.url_contains.is_empty()
        && !f.url_contains.iter().any(|needle| e.url.contains(needle))
    {
        return false;
    }
    if !f.host.is_empty() && !f.host.iter().any(|h| h == &e.host) {
        return false;
    }
    if !f.method.is_empty()
        && !f
            .method
            .iter()
            .any(|m| m.eq_ignore_ascii_case(&e.method))
    {
        return false;
    }
    if !f.status.is_empty() && !f.status.iter().any(|s| e.status == Some(*s)) {
        return false;
    }
    if !f.mime_contains.is_empty() {
        let mime = e.mime.as_deref().unwrap_or("");
        if !f
            .mime_contains
            .iter()
            .any(|m| mime.to_ascii_lowercase().contains(&m.to_ascii_lowercase()))
        {
            return false;
        }
    }
    if !url_regexes.is_empty() && !url_regexes.iter().any(|re| re.is_match(&e.url)) {
        return false;
    }
    if !exts.is_empty() {
        let Some(ext) = url_extension(&e.url) else { return false };
        if !exts.contains(&ext) {
            return false;
        }
    }
    if let Some(from) = from_dt {
        if e.started_at_dt < from {
            return false;
        }
    }
    if let Some(to) = to_dt {
        if e.started_at_dt > to {
            return false;
        }
    }

    if !f.source.is_empty() {
        // Mirror DB behavior loosely: match exact or path suffix.
        let ok = f.source.iter().any(|s| {
            source_name == s
                || source_name.ends_with(&format!("/{s}"))
                || source_name.ends_with(&format!("\\{s}"))
        });
        if !ok {
            return false;
        }
    }
    if !f.source_contains.is_empty()
        && !f.source_contains.iter().any(|s| source_name.contains(s))
    {
        return false;
    }

    // Size filters
    let req = normalize_i64(e.request_body_size).unwrap_or(0);
    let resp = normalize_i64(e.response_body_size).unwrap_or(0);
    let min_req = f
        .min_request_size
        .as_deref()
        .and_then(|v| size::parse_size_bytes_i64(v).ok().flatten());
    let max_req = f
        .max_request_size
        .as_deref()
        .and_then(|v| size::parse_size_bytes_i64(v).ok().flatten());
    let min_resp = f
        .min_response_size
        .as_deref()
        .and_then(|v| size::parse_size_bytes_i64(v).ok().flatten());
    let max_resp = f
        .max_response_size
        .as_deref()
        .and_then(|v| size::parse_size_bytes_i64(v).ok().flatten());

    if let Some(min) = min_req {
        if req < min {
            return false;
        }
    }
    if let Some(max) = max_req {
        if req > max {
            return false;
        }
    }
    if let Some(min) = min_resp {
        if resp < min {
            return false;
        }
    }
    if let Some(max) = max_resp {
        if resp > max {
            return false;
        }
    }

    true
}

fn timings_to_phase_json(t: TimingsMs) -> PhaseJson {
    PhaseJson {
        blocked: normalize_ms0(t.blocked),
        dns: normalize_ms0(t.dns),
        connect: normalize_ms0(t.connect),
        ssl: normalize_ms0(t.ssl),
        send: normalize_ms0(t.send),
        wait: normalize_ms0(t.wait),
        receive: normalize_ms0(t.receive),
    }
}

fn render_html(report: &JsonReport<'_>) -> Result<String> {
    let title = escape_html(report.title);
    let json = serde_json::to_string(report)?;
    let json = escape_script_json(&json);

    let css = r#"
:root{
  --bg:#0b1020;
  --panel:#111a33;
  --panel2:#0f1730;
  --text:#e7ecff;
  --muted:#aab3d4;
  --line:#26325d;
  --good:#39d98a;
  --warn:#ffcc66;
  --bad:#ff5c7a;
  --accent:#5aa6ff;
  --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}
*{box-sizing:border-box}
body{
  margin:0;
  background: radial-gradient(1200px 600px at 10% 0%, #172457 0%, var(--bg) 55%) fixed;
  color:var(--text);
  font: 14px/1.4 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
}
header{
  padding:20px 24px;
  border-bottom:1px solid var(--line);
  background: linear-gradient(180deg, rgba(17,26,51,.9), rgba(17,26,51,.35));
  position: sticky;
  top: 0;
  backdrop-filter: blur(8px);
}
h1{margin:0 0 6px 0; font-size: 18px; letter-spacing:.2px}
.meta{color:var(--muted); font-family:var(--mono); font-size:12px}
main{padding:18px 24px 40px 24px; max-width: 1200px; margin: 0 auto}
.grid{display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:10px; margin: 12px 0 18px 0}
.card{background: rgba(17,26,51,.72); border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px}
.card .k{color:var(--muted); font-size:12px; font-family:var(--mono)}
.card .v{font-size:18px; margin-top: 6px}
.section{margin-top: 18px}
.section h2{font-size:14px; margin:0 0 10px 0; color: #dbe3ff}
.panel{background: rgba(15,23,48,.70); border: 1px solid var(--line); border-radius: 12px; overflow:hidden}
.panel .pad{padding:12px}
.tabs{display:flex; gap:8px; padding: 10px 12px; border-bottom:1px solid var(--line); background: rgba(17,26,51,.55)}
.tabs button{
  appearance:none; border:1px solid var(--line); background: transparent; color: var(--text);
  padding: 6px 10px; border-radius: 999px; cursor:pointer; font-family:var(--mono); font-size:12px;
}
.tabs button.active{border-color: var(--accent); box-shadow: 0 0 0 2px rgba(90,166,255,.15) inset}
table{width:100%; border-collapse:collapse}
th,td{padding: 8px 10px; border-bottom:1px solid rgba(38,50,93,.65); vertical-align:top}
th{font-family:var(--mono); font-size:12px; color: var(--muted); text-align:left; user-select:none; cursor:pointer}
tr:hover td{background: rgba(90,166,255,.06)}
.pill{display:inline-block; padding:2px 8px; border-radius:999px; font-family:var(--mono); font-size:12px; border:1px solid var(--line)}
.s2xx{color:var(--good); border-color: rgba(57,217,138,.35)}
.s3xx{color:var(--accent); border-color: rgba(90,166,255,.35)}
.s4xx{color:var(--warn); border-color: rgba(255,204,102,.35)}
.s5xx{color:var(--bad); border-color: rgba(255,92,122,.35)}
.muted{color:var(--muted)}
.mono{font-family:var(--mono)}
.url{word-break: break-all}
details{border-top: 1px solid rgba(38,50,93,.65)}
details > summary{
  padding: 10px 12px;
  cursor: pointer;
  user-select: none;
  list-style:none;
  font-family: var(--mono);
  font-size: 12px;
  color: var(--muted);
}
details > summary::-webkit-details-marker{display:none}
.wf-row{
  display:grid;
  /* Make the timeline column responsive so the URL column stays readable. */
  grid-template-columns: 110px 54px minmax(0, 3fr) minmax(0, 2fr);
  gap: 10px;
  align-items:center;
  padding: 6px 12px;
  border-top: 1px solid rgba(38,50,93,.45);
}
.wf-row:hover{background: rgba(90,166,255,.05)}
.wf-method{font-family:var(--mono)}
.wf-status{font-family:var(--mono); text-align:right}
.wf-bar{width: 100%; max-width: 900px; height: 18px; justify-self:end}
.wf-small{font-size:12px; color: var(--muted)}
@media (max-width: 1050px){
  .grid{grid-template-columns: repeat(2, minmax(0,1fr))}
  .wf-row{grid-template-columns: 110px 54px 1fr}
  .wf-bar{display:none}
}
"#;

    let js = r#"
function $(id){ return document.getElementById(id); }
function parseData(){
  const el = $("harlite-data");
  return JSON.parse(el.textContent);
}
function fmtMs(v){
  if (v == null) return "-";
  const n = Number(v);
  if (!isFinite(n)) return "-";
  if (n >= 1000) return (n/1000).toFixed(2) + "s";
  return n.toFixed(1) + "ms";
}
function setText(id, txt){ const el = $(id); if (el) el.textContent = txt; }

function renderOverview(d){
  setText("ov-total", String(d.total_entries));
  setText("ov-rendered", String(d.rendered_entries));
  setText("ov-range", d.time_range ? (d.time_range.start + " .. " + d.time_range.end) : "-");
  setText("ov-totalms", d.time_range ? fmtMs(d.time_range.total_ms) : "-");

  setText("sc-2xx", String(d.status_counts.s2xx));
  setText("sc-3xx", String(d.status_counts.s3xx));
  setText("sc-4xx", String(d.status_counts.s4xx));
  setText("sc-5xx", String(d.status_counts.s5xx));
  setText("sc-none", String(d.status_counts.none));

  setText("slow-total-count", String(d.slow.slow_total_count));
  setText("slow-ttfb-count", String(d.slow.slow_ttfb_count));
  setText("slow-total-th", fmtMs(d.slow.slow_total_threshold_ms));
  setText("slow-ttfb-th", fmtMs(d.slow.slow_ttfb_threshold_ms));
}

function clearChildren(el){
  // replaceChildren() isn't supported in some older browsers; this is tiny and safe.
  while (el.firstChild) el.removeChild(el.firstChild);
}
function appendTd(tr, className, txt){
  const td = document.createElement("td");
  if (className) td.className = className;
  td.textContent = txt;
  tr.appendChild(td);
}

function renderTable(tableId, rows){
  const tbody = $(tableId).querySelector("tbody");
  clearChildren(tbody);
  for (const r of rows){
    const tr = document.createElement("tr");
    const status = r.status == null ? "-" : String(r.status);
    appendTd(tr, "mono", String(r.started_at));
    appendTd(tr, "mono", String(r.method));
    appendTd(tr, "mono", status);
    appendTd(tr, "mono", String(r.host));
    appendTd(tr, "url", String(r.url));
    appendTd(tr, "mono", fmtMs(r.total_ms));
    appendTd(tr, "mono", fmtMs(r.ttfb_ms));
    tbody.appendChild(tr);
  }
}

function makeSortable(table){
  const ths = table.querySelectorAll("th");
  ths.forEach((th, idx) => {
    th.addEventListener("click", () => {
      const tbody = table.querySelector("tbody");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      const asc = th.dataset.asc !== "1";
      th.dataset.asc = asc ? "1" : "0";
      rows.sort((a,b) => {
        const av = a.children[idx].textContent.trim();
        const bv = b.children[idx].textContent.trim();
        const an = Number(av.replace("ms","").replace("s","")) ;
        const bn = Number(bv.replace("ms","").replace("s","")) ;
        const bothNum = isFinite(an) && isFinite(bn);
        if (bothNum) return asc ? (an - bn) : (bn - an);
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      });
      clearChildren(tbody);
      for (const r of rows) tbody.appendChild(r);
    });
  });
}

function boot(){
  const d = parseData();
  renderOverview(d);
  renderTable("tbl-slow-total", d.top_slowest_total);
  renderTable("tbl-slow-ttfb", d.top_slowest_ttfb);

  // error endpoints
  {
    const tbody = $("tbl-errors").querySelector("tbody");
    clearChildren(tbody);
    for (const r of d.top_error_endpoints){
      const tr = document.createElement("tr");
      appendTd(tr, "mono", String(r.count));
      appendTd(tr, "mono", String(r.host));
      appendTd(tr, "mono", String(r.path));
      appendTd(tr, "url", String(r.sample_url));
      tbody.appendChild(tr);
    }
  }

  // pages
  {
    const tbody = $("tbl-pages").querySelector("tbody");
    clearChildren(tbody);
    for (const p of d.pages){
      const tr = document.createElement("tr");
      appendTd(tr, "mono", String(p.id));
      appendTd(tr, "", String(p.title ?? ""));
      appendTd(tr, "mono", String(p.started_at ?? ""));
      appendTd(tr, "mono", fmtMs(p.on_content_load_ms));
      appendTd(tr, "mono", fmtMs(p.on_load_ms));
      tbody.appendChild(tr);
    }
  }

  document.querySelectorAll("table").forEach(makeSortable);
}
window.addEventListener("DOMContentLoaded", boot);
"#;

    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{css}</style>
</head>
<body>
<header>
  <h1>{title}</h1>
  <div class="meta">input=<span class="mono">{input}</span> | generated_at=<span class="mono">{generated}</span> | entries=<span class="mono">{total}</span> | waterfall_rendered=<span class="mono">{rendered}</span></div>
</header>
<main>
  <div class="grid">
    <div class="card"><div class="k">Entries</div><div class="v" id="ov-total"></div></div>
    <div class="card"><div class="k">Waterfall Rendered</div><div class="v" id="ov-rendered"></div></div>
    <div class="card"><div class="k">Time Range</div><div class="v" style="font-size:12px" id="ov-range"></div></div>
    <div class="card"><div class="k">Total Span</div><div class="v" id="ov-totalms"></div></div>
  </div>

  <div class="section">
    <h2>Error Summary</h2>
    <div class="panel">
      <div class="pad">
        <span class="pill s2xx">2xx <span id="sc-2xx"></span></span>
        <span class="pill s3xx">3xx <span id="sc-3xx"></span></span>
        <span class="pill s4xx">4xx <span id="sc-4xx"></span></span>
        <span class="pill s5xx">5xx <span id="sc-5xx"></span></span>
        <span class="pill muted">none <span id="sc-none"></span></span>
      </div>
      <table id="tbl-errors">
        <thead><tr><th>Count</th><th>Host</th><th>Path</th><th>Sample URL</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Slow Requests</h2>
    <div class="panel">
      <div class="pad">
        <div class="wf-small">slow_total: <span class="mono" id="slow-total-count"></span> over <span class="mono" id="slow-total-th"></span> | slow_ttfb: <span class="mono" id="slow-ttfb-count"></span> over <span class="mono" id="slow-ttfb-th"></span></div>
      </div>
      <div class="tabs">
        <button class="active" type="button" onclick="document.getElementById('slow-total').style.display='block';document.getElementById('slow-ttfb').style.display='none';this.classList.add('active');this.nextElementSibling.classList.remove('active')">Top Total</button>
        <button type="button" onclick="document.getElementById('slow-total').style.display='none';document.getElementById('slow-ttfb').style.display='block';this.classList.add('active');this.previousElementSibling.classList.remove('active')">Top TTFB</button>
      </div>
      <div id="slow-total" style="display:block">
        <table id="tbl-slow-total">
          <thead><tr><th>Started</th><th>Method</th><th>Status</th><th>Host</th><th>URL</th><th>Total</th><th>TTFB</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div id="slow-ttfb" style="display:none">
        <table id="tbl-slow-ttfb">
          <thead><tr><th>Started</th><th>Method</th><th>Status</th><th>Host</th><th>URL</th><th>Total</th><th>TTFB</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="section">
    <h2>Pages</h2>
    <div class="panel">
      <table id="tbl-pages">
        <thead><tr><th>ID</th><th>Title</th><th>Started</th><th>onContentLoad</th><th>onLoad</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Waterfall ({group_by})</h2>
    <div class="panel">
      <div class="pad wf-small">Timeline total: <span class="mono">{wf_total}</span> | group_by=<span class="mono">{group_by}</span></div>
      {waterfall_html}
    </div>
  </div>
</main>
<script type="application/json" id="harlite-data">{json}</script>
<script>{js}</script>
</body>
</html>"#,
        title = title,
        css = css,
        input = escape_html(&report.input),
        generated = escape_html(&report.generated_at),
        total = report.total_entries,
        rendered = report.rendered_entries,
        group_by = escape_html(report.waterfall.group_by),
        wf_total = escape_html(&format!("{:.1}ms", report.waterfall.total_ms)),
        waterfall_html = waterfall_html(report),
        json = json,
        js = js
    );
    Ok(html)
}

fn waterfall_html(report: &JsonReport<'_>) -> String {
    const W: f64 = 900.0;
    let total_ms = if report.waterfall.total_ms > 0.0 {
        report.waterfall.total_ms
    } else {
        1.0
    };

    fn color_for_phase(phase: &str) -> &'static str {
        match phase {
            "blocked" => "#3c4a7a",
            "dns" => "#5aa6ff",
            "connect" => "#7c5cff",
            "ssl" => "#ffcc66",
            "send" => "#39d98a",
            "wait" => "#ff7aa6",
            "receive" => "#9aa8ff",
            _ => "#5aa6ff",
        }
    }

    let mut out = String::new();
    for g in &report.waterfall.groups {
        out.push_str(&format!(
            r#"<details open><summary>{}</summary>"#,
            escape_html(&g.name)
        ));
        for e in &g.entries {
            let status = e.status.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
            let x0 = (e.start_ms / total_ms) * W;
            let mut x = x0;
            let mut rects = String::new();

            let phases = [
                ("blocked", e.phases.blocked),
                ("dns", e.phases.dns),
                ("connect", e.phases.connect),
                ("ssl", e.phases.ssl),
                ("send", e.phases.send),
                ("wait", e.phases.wait),
                ("receive", e.phases.receive),
            ];

            let sum_phases: f64 = phases.iter().map(|(_, v)| *v).sum();
            if sum_phases > 0.0 {
                for (name, v) in phases {
                    if v <= 0.0 {
                        continue;
                    }
                    let w = (v / total_ms) * W;
                    rects.push_str(&format!(
                        r#"<rect x="{:.2}" y="2" width="{:.2}" height="14" rx="2" fill="{}"><title>{}: {:.1}ms</title></rect>"#,
                        x,
                        w.max(0.75),
                        color_for_phase(name),
                        name,
                        v
                    ));
                    x += w;
                }
            } else {
                let w = (e.total_ms / total_ms) * W;
                rects.push_str(&format!(
                    r#"<rect x="{:.2}" y="2" width="{:.2}" height="14" rx="2" fill="{}"><title>total: {:.1}ms</title></rect>"#,
                    x0,
                    w.max(0.75),
                    "#5aa6ff",
                    e.total_ms
                ));
            }

            out.push_str(&format!(
                r#"<div class="wf-row">
  <div class="wf-method">{}</div>
  <div class="wf-status">{}</div>
  <div class="url">{}</div>
  <svg class="wf-bar" viewBox="0 0 900 18" preserveAspectRatio="none">{}</svg>
</div>"#,
                escape_html(&e.method),
                escape_html(&status),
                escape_html(&e.url),
                rects
            ));
        }
        out.push_str("</details>");
    }
    out
}

pub fn run_report(input: PathBuf, options: &ReportOptions) -> Result<()> {
    let kind = detect_input_kind(&input)?;

    let title = options
        .title
        .clone()
        .unwrap_or_else(|| "harlite report".to_string());

    let output_path = options
        .output
        .clone()
        .unwrap_or_else(|| default_output_for_input(&input));

    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    let (mut entries, pages) = match kind {
        InputKind::Db => {
            let conn = Connection::open(&input)?;
            ensure_schema_upgrades(&conn)?;

            let rows = load_entries_with_filters(&conn, &options.filters)?;

            let import_ids: Vec<i64> = {
                let mut ids = rows.iter().map(|r| r.import_id).collect::<Vec<_>>();
                ids.sort();
                ids.dedup();
                ids
            };
            let page_rows = load_pages_for_imports(&conn, &import_ids)?;
            let mut page_map: HashMap<(i64, String), PageRow> = HashMap::new();
            for p in page_rows.iter() {
                page_map.insert((p.import_id, p.id.clone()), p.clone());
            }

            let report_pages = page_rows
                .into_iter()
                .map(|p| ReportPage {
                    id: p.id,
                    title: p.title,
                    started_at: p.started_at,
                    on_content_load_ms: p.on_content_load_ms,
                    on_load_ms: p.on_load_ms,
                })
                .collect::<Vec<_>>();

            let entries = rows
                .into_iter()
                .filter_map(|r| entry_to_report_entry_db(r, &page_map))
                .collect::<Vec<_>>();
            (entries, report_pages)
        }
        InputKind::Har => {
            let har = parse_har_file(&input)?;
            let source_name = input.to_string_lossy().to_string();

            let mut page_title_map: HashMap<String, Option<String>> = HashMap::new();
            let mut report_pages: Vec<ReportPage> = Vec::new();
            if let Some(pages) = har.log.pages {
                for p in pages {
                    page_title_map.insert(p.id.clone(), p.title.clone());
                    report_pages.push(report_page_from_har_page(p));
                }
            }

            let from_dt = options
                .filters
                .from
                .as_deref()
                .and_then(parse_timestamp);
            let to_dt = options.filters.to.as_deref().and_then(parse_timestamp).map(|dt| {
                // If user passed a date-only string, parse_timestamp returned midnight.
                // Accept that behavior (consistent with existing util); users can pass RFC3339 for precision.
                dt
            });

            let url_regexes: Vec<Regex> = options
                .filters
                .url_regex
                .iter()
                .map(|s| Regex::new(s))
                .collect::<std::result::Result<Vec<_>, _>>()?;

            let exts: std::collections::HashSet<String> = options
                .filters
                .ext
                .iter()
                .flat_map(|s| s.split(','))
                .map(|s| s.trim().trim_start_matches('.').to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect();

            let mut entries = Vec::new();
            for e in har.log.entries {
                let Some(re) = entry_to_report_entry_har(e, &page_title_map) else { continue };
                if !entry_matches_filters_har(
                    &re,
                    options,
                    &url_regexes,
                    &exts,
                    from_dt,
                    to_dt,
                    &source_name,
                ) {
                    continue;
                }
                entries.push(re);
            }
            (entries, report_pages)
        }
    };

    let total_entries = entries.len();
    let status_counts = summarize_status_counts(&entries);

    let time_range = apply_start_offsets(&mut entries).map(|(start, end, total_ms)| TimeRange {
        start: start.to_rfc3339_opts(SecondsFormat::Millis, true),
        end: end.to_rfc3339_opts(SecondsFormat::Millis, true),
        total_ms,
    });

    let slow_total_count = count_over_threshold(&entries, options.slow_total_ms, |e| Some(e.total_ms));
    let slow_ttfb_count = count_over_threshold(&entries, options.slow_ttfb_ms, ttfb_ms);
    let slow = SlowSummary {
        slow_total_threshold_ms: options.slow_total_ms,
        slow_ttfb_threshold_ms: options.slow_ttfb_ms,
        slow_total_count,
        slow_ttfb_count,
    };

    let (top_slowest_total, top_slowest_ttfb) = top_slowest(&entries, options.top);
    let top_error_endpoints = top_error_endpoints(&entries, options.top);

    // Waterfall rendering can be expensive for huge traces. Render the first N entries by start time.
    entries.sort_by(|a, b| {
        a.started_at_dt
            .cmp(&b.started_at_dt)
            .then_with(|| a.url.cmp(&b.url))
    });
    if entries.len() > options.waterfall_limit {
        entries.truncate(options.waterfall_limit);
    }
    let rendered_entries = entries.len();

    let groups = build_groups(entries, options.group_by, &options.page);

    let wf_total_ms = time_range.as_ref().map(|r| r.total_ms).unwrap_or(0.0);
    let waterfall_groups = groups
        .into_iter()
        .map(|g| WaterfallGroupJson {
            name: g.name,
            entries: g
                .entries
                .into_iter()
                .map(|e| WaterfallEntryJson {
                    started_at: e.started_at,
                    start_ms: e.start_ms,
                    total_ms: e.total_ms,
                    method: e.method,
                    status: e.status,
                    host: e.host,
                    url: e.url,
                    mime: e.mime,
                    phases: timings_to_phase_json(e.timings),
                })
                .collect(),
        })
        .collect::<Vec<_>>();

    let report = JsonReport {
        title: &title,
        generated_at: now,
        input: input.to_string_lossy().to_string(),
        total_entries,
        rendered_entries,
        time_range,
        status_counts,
        slow,
        top_slowest_total,
        top_slowest_ttfb,
        top_error_endpoints,
        pages: build_pages_summary(pages),
        waterfall: WaterfallSummary {
            group_by: group_by_label(options.group_by),
            total_ms: wf_total_ms,
            groups: waterfall_groups,
        },
    };

    let html = render_html(&report)?;
    let mut writer = open_output(&output_path)?;
    writer.write_all(html.as_bytes())?;
    writer.flush()?;
    Ok(())
}

fn report_page_from_har_page(p: HarPage) -> ReportPage {
    ReportPage {
        id: p.id,
        title: p.title,
        started_at: Some(p.started_date_time),
        on_content_load_ms: p.page_timings.as_ref().and_then(|t| t.on_content_load),
        on_load_ms: p.page_timings.as_ref().and_then(|t| t.on_load),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::{DateTime, Utc};
    use regex::Regex;

    use super::{
        build_groups, entry_matches_filters_har, escape_script_json, render_html, JsonReport,
        ReportEntry, ReportOptions, SlowSummary, StatusCounts, TimingsMs, WaterfallSummary,
    };
    use crate::commands::entry_filter::EntryFilterOptions;
    use crate::commands::waterfall::WaterfallGroupBy;
    use crate::error::Result;
    use crate::size;

    #[test]
    fn escapes_script_termination_in_embedded_json() {
        let input = r#"{"x":"</script><script>alert(1)</script>"}"#;
        let escaped = escape_script_json(input);
        assert!(!escaped.contains("</script>"));
        assert!(escaped.contains("\\u003c/script\\u003e"));
    }

    #[test]
    fn escapes_js_line_separators_in_embedded_json() {
        let input = format!("{{\"x\":\"a{}\u{2029}b\"}}", '\u{2028}');
        let escaped = escape_script_json(&input);
        assert!(escaped.contains("\\u2028"));
        assert!(escaped.contains("\\u2029"));
    }

    #[test]
    fn render_html_includes_data_tag() {
        let report = JsonReport {
            title: "t",
            generated_at: "2024-01-15T00:00:00.000Z".to_string(),
            input: "x".to_string(),
            total_entries: 0,
            rendered_entries: 0,
            time_range: None,
            status_counts: StatusCounts::default(),
            slow: SlowSummary {
                slow_total_threshold_ms: 1000.0,
                slow_ttfb_threshold_ms: 500.0,
                slow_total_count: 0,
                slow_ttfb_count: 0,
            },
            top_slowest_total: Vec::new(),
            top_slowest_ttfb: Vec::new(),
            top_error_endpoints: Vec::new(),
            pages: Vec::new(),
            waterfall: WaterfallSummary {
                group_by: "none",
                total_ms: 0.0,
                groups: Vec::new(),
            },
        };
        let html = render_html(&report).expect("html");
        assert!(html.contains("id=\"harlite-data\""));
        assert!(html.contains("<title>"));
        // Guard against obvious XSS sinks in the rendering script.
        assert!(!html.contains("tr.innerHTML"));
    }

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn entry(started_at: &str, start_ms: f64, url: &str) -> ReportEntry {
        ReportEntry {
            started_at: started_at.to_string(),
            started_at_dt: dt(started_at),
            start_ms,
            total_ms: 10.0,
            timings: TimingsMs::default(),
            method: "GET".to_string(),
            url: url.to_string(),
            host: "example.com".to_string(),
            path: None,
            status: Some(200),
            mime: Some("text/html".to_string()),
            request_body_size: Some(0),
            response_body_size: Some(0),
            page_id: None,
            page_title: None,
        }
    }

    #[test]
    fn build_groups_none_puts_everything_in_one_group_sorted_by_start() {
        let e1 = entry("2024-01-15T00:00:01.000Z", 100.0, "https://example.com/a");
        let e2 = entry("2024-01-15T00:00:00.000Z", 0.0, "https://example.com/b");
        let groups = build_groups(vec![e1, e2], WaterfallGroupBy::None, &[]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "All Requests");
        assert_eq!(groups[0].entries.len(), 2);
        assert_eq!(groups[0].entries[0].url, "https://example.com/b");
    }

    #[test]
    fn build_groups_page_filters_match_title_and_id() {
        let mut e1 = entry("2024-01-15T00:00:00.000Z", 0.0, "https://example.com/a");
        e1.page_id = Some("p1".to_string());
        e1.page_title = Some("Home".to_string());
        let mut e2 = entry("2024-01-15T00:00:01.000Z", 10.0, "https://example.com/b");
        e2.page_id = Some("p2".to_string());
        e2.page_title = Some("Other".to_string());

        let groups = build_groups(
            vec![e1, e2],
            WaterfallGroupBy::Page,
            &["home".to_string()],
        );
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].entries.len(), 1);
        assert_eq!(groups[0].entries[0].page_id.as_deref(), Some("p1"));
    }

    #[test]
    fn entry_matches_filters_har_ext_mime_and_size_filters() -> Result<()> {
        let mut e = entry("2024-01-15T00:00:00.000Z", 0.0, "https://example.com/app.js");
        e.mime = Some("application/json; charset=utf-8".to_string());
        e.request_body_size = Some(100);
        e.response_body_size = Some(200);

        let mut filters = EntryFilterOptions::default();
        filters.ext = vec!["js".to_string()];
        filters.mime_contains = vec!["JSON".to_string()];
        filters.min_request_size = Some("50".to_string());
        filters.max_request_size = Some("150".to_string());
        filters.min_response_size = Some("100".to_string());
        filters.max_response_size = Some("250".to_string());

        // Sanity check size parsing; these should all be valid.
        let _ = size::parse_size_bytes_i64(filters.min_request_size.as_deref().unwrap())?;

        let options = ReportOptions {
            output: None,
            title: None,
            top: 10,
            slow_total_ms: 1000.0,
            slow_ttfb_ms: 500.0,
            waterfall_limit: 500,
            group_by: WaterfallGroupBy::None,
            page: Vec::new(),
            filters,
        };

        let url_regexes: Vec<Regex> = Vec::new();
        let exts: HashSet<String> = ["js".to_string()].into_iter().collect();

        assert!(entry_matches_filters_har(
            &e,
            &options,
            &url_regexes,
            &exts,
            None,
            None,
            "trace.har"
        ));

        e.url = "https://example.com/app.css".to_string();
        assert!(!entry_matches_filters_har(
            &e,
            &options,
            &url_regexes,
            &exts,
            None,
            None,
            "trace.har"
        ));
        Ok(())
    }
}
