use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::SecondsFormat;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::Connection;
use serde::Serialize;

use crate::db::{ensure_schema_upgrades, load_entries, load_pages_for_imports, EntryQuery, PageRow};
use crate::error::{HarliteError, Result};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum WaterfallFormat {
    Text,
    Trace,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum WaterfallGroupBy {
    Page,
    Navigation,
    None,
}

pub struct WaterfallOptions {
    pub output: Option<PathBuf>,
    pub format: WaterfallFormat,
    pub group_by: WaterfallGroupBy,
    pub host: Vec<String>,
    pub page: Vec<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub width: Option<usize>,
}

fn parse_started_at_bound(s: &str, is_end: bool) -> Result<String> {
    let s = s.trim();
    if s.is_empty() {
        return Err(HarliteError::InvalidHar(
            "Empty timestamp bound".to_string(),
        ));
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Millis, true));
    }

    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
    let dt = if is_end {
        date.and_hms_milli_opt(23, 59, 59, 999)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid end date".to_string()))?
    } else {
        date.and_hms_opt(0, 0, 0)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid start date".to_string()))?
    };
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn open_output(path: &Path) -> Result<Box<dyn Write>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout().lock()));
    }
    Ok(Box::new(BufWriter::new(File::create(path)?)))
}

#[derive(Debug, Clone)]
struct WaterfallEntry {
    import_id: i64,
    page_id: Option<String>,
    page_title: Option<String>,
    started_at: String,
    start_ms: f64,
    duration_ms: f64,
    method: String,
    url: String,
    host: String,
    status: Option<i32>,
    mime: Option<String>,
}

#[derive(Debug, Clone)]
struct GroupInfo {
    name: String,
    start_ms: f64,
    entries: Vec<WaterfallEntry>,
}

fn normalize_lower(s: &str) -> String {
    s.trim().to_ascii_lowercase()
}

fn page_matches_filter(
    page_id: Option<&str>,
    page_title: Option<&str>,
    filters: &[String],
) -> bool {
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

fn is_navigation_entry(entry: &WaterfallEntry) -> bool {
    if entry.method.to_ascii_uppercase() != "GET" {
        return false;
    }
    let Some(mime) = entry.mime.as_deref() else {
        return false;
    };
    let mime = mime.to_ascii_lowercase();
    mime.contains("text/html") || mime.contains("application/xhtml+xml")
}

fn build_bar(offset_ms: f64, duration_ms: f64, total_ms: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let total = if total_ms > 0.0 { total_ms } else { 1.0 };
    let start_col = ((offset_ms / total) * width as f64).floor() as isize;
    let mut dur_cols = ((duration_ms / total) * width as f64).ceil() as isize;
    if duration_ms > 0.0 {
        dur_cols = dur_cols.max(1);
    } else {
        dur_cols = 0;
    }
    let mut buf = vec![' '; width];
    let start = start_col.clamp(0, (width - 1) as isize) as usize;
    buf[start] = '|';
    let end = (start as isize + 1 + dur_cols).clamp(0, width as isize) as usize;
    for idx in (start + 1)..end {
        buf[idx] = '=';
    }
    buf.into_iter().collect()
}

fn render_text(groups: &[GroupInfo], total_ms: f64, width: usize, writer: &mut dyn Write) -> Result<()> {
    writeln!(
        writer,
        "Range: 0.0ms..{:.1}ms | width={} | groups={}",
        total_ms,
        width,
        groups.len()
    )?;

    for group in groups {
        writeln!(writer)?;
        writeln!(writer, "Group: {} (entries={})", group.name, group.entries.len())?;
        for entry in &group.entries {
            let bar = build_bar(entry.start_ms, entry.duration_ms, total_ms, width);
            let status = entry
                .status
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            writeln!(
                writer,
                "{:>8.1} {:>8.1} {} {} {} {}",
                entry.start_ms,
                entry.duration_ms,
                bar,
                entry.method,
                status,
                entry.url
            )?;
        }
    }

    Ok(())
}

#[derive(Serialize)]
struct Trace {
    #[serde(rename = "traceEvents")]
    trace_events: Vec<TraceEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "displayTimeUnit")]
    display_time_unit: Option<String>,
}

#[derive(Serialize)]
struct TraceEvent {
    name: String,
    ph: String,
    pid: i32,
    tid: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    cat: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dur: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<serde_json::Value>,
}

fn render_trace(groups: &[GroupInfo], writer: &mut dyn Write) -> Result<()> {
    let mut events: Vec<TraceEvent> = Vec::new();
    for (idx, group) in groups.iter().enumerate() {
        let pid = idx as i32 + 1;
        events.push(TraceEvent {
            name: "process_name".to_string(),
            ph: "M".to_string(),
            pid,
            tid: 0,
            cat: None,
            ts: Some(0),
            dur: None,
            args: Some(serde_json::json!({ "name": group.name })),
        });

        for (order, entry) in group.entries.iter().enumerate() {
            let ts = (entry.start_ms * 1000.0).round() as i64;
            let dur = (entry.duration_ms.max(0.0) * 1000.0).round() as i64;
            let name = format!("{} {}", entry.method, entry.url);
            events.push(TraceEvent {
                name,
                ph: "X".to_string(),
                pid,
                tid: 0,
                cat: Some("net".to_string()),
                ts: Some(ts),
                dur: Some(dur),
                args: Some(serde_json::json!({
                    "url": entry.url.clone(),
                    "method": entry.method.clone(),
                    "status": entry.status,
                    "host": entry.host.clone(),
                    "started_at": entry.started_at.clone(),
                    "time_ms": entry.duration_ms,
                    "order": order as i64,
                    "page_id": entry.page_id.clone(),
                    "page_title": entry.page_title.clone(),
                })),
            });
        }
    }

    let trace = Trace {
        trace_events: events,
        display_time_unit: Some("ms".to_string()),
    };
    serde_json::to_writer_pretty(writer, &trace)?;
    Ok(())
}

pub fn run_waterfall(database: PathBuf, options: &WaterfallOptions) -> Result<()> {
    let conn = Connection::open(&database)?;
    ensure_schema_upgrades(&conn)?;

    let from_started_at = match options.from.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, false)?),
        None => None,
    };
    let to_started_at = match options.to.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, true)?),
        None => None,
    };

    let mut query = EntryQuery::default();
    query.from_started_at = from_started_at;
    query.to_started_at = to_started_at;
    query.hosts = options.host.clone();

    let mut rows = load_entries(&conn, &query)?;
    if rows.is_empty() {
        return Ok(());
    }

    let import_ids: Vec<i64> = {
        let mut ids: Vec<i64> = rows.iter().map(|r| r.import_id).collect();
        ids.sort();
        ids.dedup();
        ids
    };
    let pages = load_pages_for_imports(&conn, &import_ids)?;
    let mut page_map: HashMap<(i64, String), PageRow> = HashMap::new();
    for page in pages {
        page_map.insert((page.import_id, page.id.clone()), page);
    }

    let multi_import = import_ids.len() > 1;
    let mut parsed: Vec<(WaterfallEntry, DateTime<Utc>)> = Vec::new();
    let mut skipped = 0usize;

    for row in rows.drain(..) {
        let Some(started_at) = row.started_at.as_deref() else {
            skipped += 1;
            continue;
        };
        let Ok(dt) = DateTime::parse_from_rfc3339(started_at) else {
            skipped += 1;
            continue;
        };
        let dt = dt.with_timezone(&Utc);
        let duration_ms = row.time_ms.unwrap_or(0.0);

        let (page_id, page_title) = row
            .page_id
            .as_ref()
            .and_then(|pid| page_map.get(&(row.import_id, pid.clone())))
            .map(|p| (Some(p.id.clone()), p.title.clone()))
            .unwrap_or((row.page_id.clone(), None));

        if !page_matches_filter(page_id.as_deref(), page_title.as_deref(), &options.page) {
            continue;
        }

        let entry = WaterfallEntry {
            import_id: row.import_id,
            page_id,
            page_title,
            started_at: started_at.to_string(),
            start_ms: 0.0,
            duration_ms,
            method: row.method.unwrap_or_else(|| "GET".to_string()),
            url: row.url.unwrap_or_else(|| "<unknown>".to_string()),
            host: row.host.unwrap_or_else(|| "".to_string()),
            status: row.status,
            mime: row.response_mime_type,
        };
        parsed.push((entry, dt));
    }

    if parsed.is_empty() {
        return Ok(());
    }

    parsed.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.url.cmp(&b.0.url)));
    let base_time = parsed.first().map(|(_, dt)| *dt).unwrap();

    let mut entries: Vec<WaterfallEntry> = Vec::with_capacity(parsed.len());
    let mut max_end = 0.0;
    for (mut entry, dt) in parsed {
        let offset_ms = (dt - base_time).num_milliseconds() as f64;
        entry.start_ms = offset_ms;
        let end_ms = offset_ms + entry.duration_ms.max(0.0);
        if end_ms > max_end {
            max_end = end_ms;
        }
        entries.push(entry);
    }

    let mut groups: Vec<GroupInfo> = Vec::new();
    let mut group_map: HashMap<String, usize> = HashMap::new();
    let mut nav_index = 0usize;
    let mut current_nav: Option<String> = None;

    for entry in entries {
        let key = match options.group_by {
            WaterfallGroupBy::None => "all".to_string(),
            WaterfallGroupBy::Page => {
                if let Some(pid) = entry.page_id.as_deref() {
                    format!("page:{}:{}", entry.import_id, pid)
                } else {
                    "page:none".to_string()
                }
            }
            WaterfallGroupBy::Navigation => {
                if let Some(pid) = entry.page_id.as_deref() {
                    format!("page:{}:{}", entry.import_id, pid)
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
            let name = match options.group_by {
                WaterfallGroupBy::None => "All Requests".to_string(),
                WaterfallGroupBy::Page | WaterfallGroupBy::Navigation => {
                    if let Some(pid) = entry.page_id.as_deref() {
                        let title = entry.page_title.clone().unwrap_or_else(|| pid.to_string());
                        if multi_import {
                            format!("{}:{}", entry.import_id, title)
                        } else {
                            title
                        }
                    } else if key.starts_with("nav-") {
                        let label = format!("Navigation {}", nav_index);
                        if entry.url == "<unknown>" {
                            label
                        } else {
                            format!("{label}: {}", entry.url)
                        }
                    } else {
                        "No Page".to_string()
                    }
                }
            };
            let group = GroupInfo {
                name,
                start_ms: entry.start_ms,
                entries: Vec::new(),
            };
            groups.push(group);
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

    if skipped > 0 {
        eprintln!("Skipped {skipped} entries without valid started_at values.");
    }

    let output_path = match &options.output {
        Some(p) => p.clone(),
        None => PathBuf::from("-"),
    };
    let mut writer = open_output(&output_path)?;

    match options.format {
        WaterfallFormat::Text => {
            let width = options.width.unwrap_or(60);
            render_text(&groups, max_end, width, writer.as_mut())?;
        }
        WaterfallFormat::Trace => {
            render_trace(&groups, writer.as_mut())?;
        }
    }

    Ok(())
}
