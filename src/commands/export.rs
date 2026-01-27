use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::SecondsFormat;
use chrono::{DateTime, NaiveDate, Utc};
use regex::Regex;
use rusqlite::Connection;
use url::Url;

use crate::db::{load_blobs_by_hashes, load_entries, load_pages_for_imports, BlobRow, EntryQuery};
use crate::error::{HarliteError, Result};
use crate::har::{
    Content, Cookie, Creator, Entry, Har, Header, Log, Page, PageTimings, PostData, QueryParam,
    Request, Response, Timings,
};

/// Options for exporting a harlite database back to a HAR file.
pub struct ExportOptions {
    pub output: Option<PathBuf>,
    pub pretty: bool,
    pub include_bodies: bool,

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

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            output: None,
            pretty: true,
            include_bodies: false,
            url: Vec::new(),
            url_contains: Vec::new(),
            url_regex: Vec::new(),
            host: Vec::new(),
            method: Vec::new(),
            status: Vec::new(),
            mime_contains: Vec::new(),
            ext: Vec::new(),
            source: Vec::new(),
            source_contains: Vec::new(),
            from: None,
            to: None,
            min_request_size: None,
            max_request_size: None,
            min_response_size: None,
            max_response_size: None,
        }
    }
}

fn parse_size_bytes(s: &str) -> Option<i64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return None;
    }
    if s == "unlimited" {
        return None;
    }

    let (num, mult) = if s.ends_with("kb") {
        (s.trim_end_matches("kb").trim(), 1024i64)
    } else if s.ends_with("mb") {
        (s.trim_end_matches("mb").trim(), 1024i64 * 1024)
    } else if s.ends_with("gb") {
        (s.trim_end_matches("gb").trim(), 1024i64 * 1024 * 1024)
    } else {
        (s.as_str(), 1i64)
    };

    num.parse::<i64>().ok().map(|n| n.saturating_mul(mult))
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
        date.and_hms_opt(23, 59, 59)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid end date".to_string()))?
    } else {
        date.and_hms_opt(0, 0, 0)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid start date".to_string()))?
    };
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn headers_from_json(json: Option<&str>) -> Vec<Header> {
    let Some(json) = json else {
        return Vec::new();
    };

    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    let Some(obj) = value.as_object() else {
        return Vec::new();
    };

    let mut out: Vec<Header> = obj
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k, s)))
        .map(|(k, v)| Header {
            name: k.to_string(),
            value: v.to_string(),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn cookies_from_json(json: Option<&str>) -> Vec<Cookie> {
    let Some(json) = json else {
        return Vec::new();
    };
    serde_json::from_str::<Vec<Cookie>>(json).unwrap_or_default()
}

fn query_string_from_url(url: &str) -> Option<Vec<QueryParam>> {
    let parsed = Url::parse(url).ok()?;
    let mut out: Vec<QueryParam> = Vec::new();
    for (name, value) in parsed.query_pairs() {
        out.push(QueryParam {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
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

fn request_mime_type(headers: &[Header]) -> Option<String> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-type"))
        .map(|h| {
            h.value
                .split(';')
                .next()
                .unwrap_or(&h.value)
                .trim()
                .to_string()
        })
        .filter(|s| !s.is_empty())
}

fn body_text_and_encoding(content: &[u8]) -> (Option<String>, Option<String>) {
    if content.is_empty() {
        return (None, None);
    }
    match std::str::from_utf8(content) {
        Ok(s) => (Some(s.to_string()), None),
        Err(_) => {
            use base64::{engine::general_purpose::STANDARD, Engine as _};
            (Some(STANDARD.encode(content)), Some("base64".to_string()))
        }
    }
}

fn page_export_id(import_id: i64, page_id: &str, multi_import: bool) -> String {
    if multi_import {
        format!("{import_id}:{page_id}")
    } else {
        page_id.to_string()
    }
}

fn open_output(path: &Path) -> Result<Box<dyn Write>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout().lock()));
    }
    Ok(Box::new(BufWriter::new(File::create(path)?)))
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
        let placeholders = std::iter::repeat("?")
            .take(source.len())
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("source_file IN ({placeholders})"));
        for s in source {
            params.push(rusqlite::types::Value::Text(s.clone()));
        }
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

/// Export a harlite SQLite database back to a HAR file.
pub fn run_export(database: PathBuf, options: &ExportOptions) -> Result<()> {
    let conn = Connection::open(&database)?;

    let output_path = match &options.output {
        Some(p) => p.clone(),
        None => {
            let stem = database
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("export");
            PathBuf::from(format!("{stem}.har"))
        }
    };

    let from_started_at = match options.from.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, false)?),
        None => None,
    };
    let to_started_at = match options.to.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, true)?),
        None => None,
    };

    let mut query = EntryQuery::default();

    let import_ids = load_import_ids_by_source(&conn, &options.source, &options.source_contains)?;
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
    query.min_request_size = options
        .min_request_size
        .as_deref()
        .and_then(parse_size_bytes);
    query.max_request_size = options
        .max_request_size
        .as_deref()
        .and_then(parse_size_bytes);
    query.min_response_size = options
        .min_response_size
        .as_deref()
        .and_then(parse_size_bytes);
    query.max_response_size = options
        .max_response_size
        .as_deref()
        .and_then(parse_size_bytes);

    let mut entries = load_entries(&conn, &query)?;

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

    let import_ids: Vec<i64> = {
        let mut uniq: Vec<i64> = entries.iter().map(|e| e.import_id).collect();
        uniq.sort_unstable();
        uniq.dedup();
        uniq
    };
    let multi_import = import_ids.len() > 1;

    let pages = load_pages_for_imports(&conn, &import_ids)?;
    let page_by_key: HashMap<(i64, String), crate::db::PageRow> = pages
        .into_iter()
        .map(|p| ((p.import_id, p.id.clone()), p))
        .collect();

    let mut needed_pages: HashSet<(i64, String)> = HashSet::new();
    for e in &entries {
        if let Some(pid) = &e.page_id {
            needed_pages.insert((e.import_id, pid.clone()));
        }
    }

    let mut har_pages: Vec<Page> = Vec::new();
    for (import_id, pid) in needed_pages.iter() {
        if let Some(p) = page_by_key.get(&(*import_id, pid.clone())) {
            har_pages.push(Page {
                started_date_time: p
                    .started_at
                    .clone()
                    .unwrap_or_else(|| Utc::now().to_rfc3339()),
                id: page_export_id(*import_id, &p.id, multi_import),
                title: p.title.clone(),
                page_timings: Some(PageTimings {
                    on_content_load: p.on_content_load_ms,
                    on_load: p.on_load_ms,
                }),
            });
        }
    }
    har_pages.sort_by(|a, b| a.started_date_time.cmp(&b.started_date_time));

    let mut blob_map: HashMap<String, BlobRow> = HashMap::new();
    if options.include_bodies {
        let mut hashes: Vec<String> = entries
            .iter()
            .flat_map(|e| {
                [e.request_body_hash.as_ref(), e.response_body_hash.as_ref()]
                    .into_iter()
                    .flatten()
            })
            .cloned()
            .collect();
        hashes.sort();
        hashes.dedup();
        let blobs = load_blobs_by_hashes(&conn, &hashes)?;
        blob_map = blobs.into_iter().map(|b| (b.hash.clone(), b)).collect();
    }

    let mut har_entries: Vec<Entry> = Vec::with_capacity(entries.len());
    for row in entries {
        let started = row
            .started_at
            .clone()
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let time_ms = row.time_ms.unwrap_or(0.0);
        let url = row.url.clone().unwrap_or_default();

        let request_headers = headers_from_json(row.request_headers.as_deref());
        let response_headers = headers_from_json(row.response_headers.as_deref());
        let request_cookies = cookies_from_json(row.request_cookies.as_deref());
        let response_cookies = cookies_from_json(row.response_cookies.as_deref());

        let mut request_body_text: Option<String> = None;
        let mut request_body_len: Option<i64> = None;
        if options.include_bodies {
            if let Some(hash) = &row.request_body_hash {
                if let Some(blob) = blob_map.get(hash) {
                    (request_body_text, _) = body_text_and_encoding(&blob.content);
                    request_body_len = Some(blob.content.len() as i64);
                }
            }
        }

        let mut response_body_text: Option<String> = None;
        let mut response_body_encoding: Option<String> = None;
        let mut response_body_size: i64 = row.response_body_size.unwrap_or(0);
        let mut response_mime = row.response_mime_type.clone();
        if options.include_bodies {
            if let Some(hash) = &row.response_body_hash {
                if let Some(blob) = blob_map.get(hash) {
                    let (text, enc) = body_text_and_encoding(&blob.content);
                    response_body_text = text;
                    response_body_encoding = enc;
                    response_body_size = blob.content.len() as i64;
                    if response_mime.is_none() {
                        response_mime = blob.mime_type.clone();
                    }
                }
            }
        }

        let request_body_size = row.request_body_size.or(request_body_len);
        let response_body_size_field = if options.include_bodies && response_body_text.is_some() {
            Some(response_body_size)
        } else {
            row.response_body_size
        };

        let timings = Some(Timings {
            blocked: None,
            dns: None,
            connect: None,
            send: 0.0,
            wait: time_ms,
            receive: 0.0,
            ssl: None,
        });

        let post_data = if request_body_text.is_some() {
            Some(PostData {
                mime_type: request_mime_type(&request_headers),
                text: request_body_text,
                params: None,
            })
        } else {
            None
        };

        har_entries.push(Entry {
            pageref: row
                .page_id
                .as_deref()
                .map(|pid| page_export_id(row.import_id, pid, multi_import)),
            started_date_time: started,
            time: time_ms,
            request: Request {
                method: row.method.clone().unwrap_or_default(),
                url: url.clone(),
                http_version: row.http_version.clone().unwrap_or_default(),
                cookies: Some(request_cookies),
                headers: request_headers,
                query_string: query_string_from_url(&url),
                post_data,
                headers_size: None,
                body_size: request_body_size,
            },
            response: Response {
                status: row.status.unwrap_or(0),
                status_text: row.status_text.clone().unwrap_or_default(),
                http_version: row.http_version.clone().unwrap_or_default(),
                cookies: Some(response_cookies),
                headers: response_headers,
                content: Content {
                    size: response_body_size,
                    compression: None,
                    mime_type: response_mime,
                    text: response_body_text,
                    encoding: response_body_encoding,
                },
                redirect_url: None,
                headers_size: None,
                body_size: response_body_size_field,
            },
            cache: None,
            timings,
            server_ip_address: row.server_ip.clone(),
            connection: row.connection_id.clone(),
        });
    }

    let har = Har {
        log: Log {
            version: Some("1.2".to_string()),
            creator: Some(Creator {
                name: "harlite".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            }),
            browser: None,
            pages: if har_pages.is_empty() {
                None
            } else {
                Some(har_pages)
            },
            entries: har_entries,
        },
    };

    let mut writer = open_output(&output_path)?;
    if options.pretty {
        serde_json::to_writer_pretty(&mut writer, &har)?;
    } else {
        serde_json::to_writer(&mut writer, &har)?;
    }
    writer.write_all(b"\n")?;

    if output_path != PathBuf::from("-") {
        println!(
            "Exported {} entries to {}",
            har.log.entries.len(),
            output_path.display()
        );
    }

    Ok(())
}
