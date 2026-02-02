use std::collections::HashSet;

use chrono::SecondsFormat;
use chrono::{DateTime, NaiveDate, Utc};
use regex::Regex;
use rusqlite::Connection;
use url::Url;

use crate::db::{load_entries, EntryQuery, EntryRow};
use crate::error::{HarliteError, Result};
use crate::size;

#[derive(Debug, Default, Clone)]
pub struct EntryFilterOptions {
    pub url: Vec<String>,
    pub url_contains: Vec<String>,
    pub url_regex: Vec<String>,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub mime_contains: Vec<String>,
    pub ext: Vec<String>,
    pub source: Vec<String>,
    pub source_contains: Vec<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub min_request_size: Option<String>,
    pub max_request_size: Option<String>,
    pub min_response_size: Option<String>,
    pub max_response_size: Option<String>,
}

pub fn load_entries_with_filters(
    conn: &Connection,
    options: &EntryFilterOptions,
) -> Result<Vec<EntryRow>> {
    let from_started_at = match options.from.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, false)?),
        None => None,
    };
    let to_started_at = match options.to.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, true)?),
        None => None,
    };

    let mut query = EntryQuery::default();
    let import_ids = load_import_ids_by_source(conn, &options.source, &options.source_contains)?;
    if !options.source.is_empty() || !options.source_contains.is_empty() {
        if import_ids.is_empty() {
            return Ok(Vec::new());
        }
    }
    if !import_ids.is_empty() {
        query.import_ids = import_ids;
    }
    query.from_started_at = from_started_at;
    query.to_started_at = to_started_at;
    query.url_exact = options.url.clone();
    query.url_contains = options.url_contains.clone();
    query.hosts = options.host.clone();
    query.methods = options.method.clone();
    query.statuses = options.status.clone();
    query.mime_contains = options.mime_contains.clone();
    query.min_request_size = match options.min_request_size.as_deref() {
        Some(value) => size::parse_size_bytes_i64(value)?,
        None => None,
    };
    query.max_request_size = match options.max_request_size.as_deref() {
        Some(value) => size::parse_size_bytes_i64(value)?,
        None => None,
    };
    query.min_response_size = match options.min_response_size.as_deref() {
        Some(value) => size::parse_size_bytes_i64(value)?,
        None => None,
    };
    query.max_response_size = match options.max_response_size.as_deref() {
        Some(value) => size::parse_size_bytes_i64(value)?,
        None => None,
    };

    let mut entries = load_entries(conn, &query)?;

    let url_regexes: Vec<Regex> = options
        .url_regex
        .iter()
        .map(|s| Regex::new(s))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let exts: HashSet<String> = options
        .ext
        .iter()
        .map(|s| s.trim().trim_start_matches('.').to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    if !url_regexes.is_empty() || !exts.is_empty() {
        entries.retain(|e| {
            let Some(url) = e.url.as_deref() else {
                return false;
            };

            if !url_regexes.is_empty() && !url_regexes.iter().any(|re| re.is_match(url)) {
                return false;
            }

            if !exts.is_empty() {
                let Some(ext) = url_extension(url) else {
                    return false;
                };
                if !exts.contains(&ext) {
                    return false;
                }
            }

            true
        });
    }

    Ok(entries)
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

fn load_import_ids_by_source(
    conn: &Connection,
    source: &[String],
    source_contains: &[String],
) -> Result<Vec<i64>> {
    if source.is_empty() && source_contains.is_empty() {
        return Ok(Vec::new());
    }

    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();

    if !source.is_empty() {
        let mut source_clauses = Vec::new();
        for s in source {
            source_clauses.push(
                "(source_file = ? OR source_file LIKE '%/' || ? OR source_file LIKE '%\\\\' || ?)"
                    .to_string(),
            );
            params.push(rusqlite::types::Value::Text(s.clone()));
            params.push(rusqlite::types::Value::Text(s.clone()));
            params.push(rusqlite::types::Value::Text(s.clone()));
        }
        clauses.push(format!("({})", source_clauses.join(" OR ")));
    }

    if !source_contains.is_empty() {
        let joined = std::iter::repeat("source_file LIKE '%' || ? || '%'")
            .take(source_contains.len())
            .collect::<Vec<_>>()
            .join(" OR ");
        clauses.push(format!("({joined})"));
        for s in source_contains {
            params.push(rusqlite::types::Value::Text(s.clone()));
        }
    }

    let mut sql = "SELECT id FROM imports".to_string();
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }

    let mut stmt = conn.prepare(&sql)?;
    let ids = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect::<Vec<i64>>();
    Ok(ids)
}

fn url_extension(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let path = parsed.path();
    let file = path.rsplit('/').next().unwrap_or("");
    let ext = file.rsplit('.').next()?;
    if ext == file {
        return None;
    }
    Some(ext.to_lowercase())
}
