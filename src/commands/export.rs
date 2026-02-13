use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use regex::Regex;
use rusqlite::Connection;
use url::Url;

use super::entry_filter::{load_entries_with_filters, EntryFilterOptions};
use crate::db::{ensure_schema_upgrades, load_blobs_by_hashes, load_pages_for_imports, BlobRow};
use crate::error::{HarliteError, Result};
use crate::har::{
    Content, Cookie, Creator, Entry, Extensions, Har, Header, Log, Page, PageTimings, PostData,
    QueryParam, Request, Response, Timings,
};
use crate::plugins::{PluginContext, PluginSet};

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum ExportInputFormat {
    Db,
    Har,
}

/// Options for exporting a harlite database back to a HAR file.
pub struct ExportOptions {
    pub output: Option<PathBuf>,
    pub format: Option<ExportInputFormat>,
    pub pretty: bool,
    pub include_bodies: bool,
    pub include_raw_response_bodies: bool,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,

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
    pub plugins: PluginSet,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            output: None,
            format: None,
            pretty: true,
            include_bodies: false,
            include_raw_response_bodies: false,
            allow_external_paths: false,
            external_path_root: None,
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
            plugins: PluginSet::default(),
        }
    }
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

fn normalize_ms(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v >= 0.0 => Some(v),
        _ => None,
    }
}

fn cookies_from_json(json: Option<&str>) -> Vec<Cookie> {
    let Some(json) = json else {
        return Vec::new();
    };
    serde_json::from_str::<Vec<Cookie>>(json).unwrap_or_default()
}

fn extensions_from_json(json: Option<&str>) -> Extensions {
    let Some(json) = json else {
        return Extensions::new();
    };
    serde_json::from_str::<Extensions>(json).unwrap_or_default()
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

fn load_external_blob_content(mut blob: BlobRow, external_root: Option<&Path>) -> Result<BlobRow> {
    if !blob.content.is_empty() || blob.size <= 0 {
        return Ok(blob);
    }
    let Some(path) = &blob.external_path else {
        return Ok(blob);
    };
    let Some(root) = external_root else {
        return Ok(blob);
    };

    let candidate = PathBuf::from(path);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    };
    let resolved = match candidate.canonicalize() {
        Ok(p) => p,
        Err(_) => return Ok(blob),
    };
    if !resolved.starts_with(root) {
        return Ok(blob);
    }
    blob.content = std::fs::read(resolved)?;
    Ok(blob)
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

/// Export a harlite database or HAR back to a HAR file.
pub fn run_export(database: PathBuf, options: &ExportOptions) -> Result<()> {
    let input_format = resolve_export_format(&database, options.format)?;
    let output_path = options
        .output
        .clone()
        .unwrap_or_else(|| default_export_output_path(&database, input_format));

    match input_format {
        ExportInputFormat::Db => run_export_from_db(&database, &output_path, options),
        ExportInputFormat::Har => run_export_from_har(&database, &output_path, options),
    }
}

fn default_export_output_path(database: &Path, format: ExportInputFormat) -> PathBuf {
    match format {
        ExportInputFormat::Db => {
            let stem = database
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("export");
            PathBuf::from(format!("{stem}.har"))
        }
        ExportInputFormat::Har => {
            let file_name = database
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("export");
            let base = file_name
                .trim_end_matches(".har.br")
                .trim_end_matches(".har.gz")
                .trim_end_matches(".har")
                .trim_end_matches(".json");
            let default = format!("{base}.filtered.har");
            database.with_file_name(default)
        }
    }
}

fn run_export_from_db(database: &Path, output_path: &Path, options: &ExportOptions) -> Result<()> {
    let conn = Connection::open(database)?;
    ensure_schema_upgrades(&conn)?;
    let external_root = if options.allow_external_paths {
        let root = options
            .external_path_root
            .clone()
            .or_else(|| database.parent().map(|p| p.to_path_buf()))
            .ok_or_else(|| {
                HarliteError::InvalidArgs(
                    "Cannot resolve external path root; pass --external-path-root".to_string(),
                )
            })?;
        Some(root.canonicalize()?)
    } else {
        None
    };

    let output_str = output_path.to_string_lossy();
    let database_str = database.to_string_lossy();
    let mut context = PluginContext {
        command: "export",
        source: None,
        database: Some(database_str.as_ref()),
        output: Some(output_str.as_ref()),
    };

    let filters = EntryFilterOptions {
        url: options.url.clone(),
        url_contains: options.url_contains.clone(),
        url_regex: options.url_regex.clone(),
        host: options.host.clone(),
        method: options.method.clone(),
        status: options.status.clone(),
        mime_contains: options.mime_contains.clone(),
        ext: options.ext.clone(),
        source: options.source.clone(),
        source_contains: options.source_contains.clone(),
        from: options.from.clone(),
        to: options.to.clone(),
        min_request_size: options.min_request_size.clone(),
        max_request_size: options.max_request_size.clone(),
        min_response_size: options.min_response_size.clone(),
        max_response_size: options.max_response_size.clone(),
    };
    let entries = load_entries_with_filters(&conn, &filters)?;

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
                    extensions: extensions_from_json(p.page_timings_extensions.as_deref()),
                }),
                extensions: extensions_from_json(p.page_extensions.as_deref()),
            });
        }
    }
    har_pages.sort_by(|a, b| a.started_date_time.cmp(&b.started_date_time));

    let mut blob_map: HashMap<String, BlobRow> = HashMap::new();
    if options.include_bodies {
        let mut hashes: Vec<String> = entries
            .iter()
            .flat_map(|e| {
                [
                    e.request_body_hash.as_ref(),
                    e.response_body_hash.as_ref(),
                    e.response_body_hash_raw.as_ref(),
                ]
                .into_iter()
                .flatten()
            })
            .cloned()
            .collect();
        hashes.sort();
        hashes.dedup();
        let blobs = load_blobs_by_hashes(&conn, &hashes)?;
        let hydrated: Vec<BlobRow> = blobs
            .into_iter()
            .map(|b| load_external_blob_content(b, external_root.as_deref()))
            .collect::<Result<Vec<_>>>()?;
        blob_map = hydrated.into_iter().map(|b| (b.hash.clone(), b)).collect();
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
        let mut response_body_size_raw: Option<i64> = row.response_body_size_raw;
        let mut response_mime = row.response_mime_type.clone();
        let mut response_compression: Option<i64> = None;
        let mut response_body_is_raw = false;

        if options.include_bodies {
            let mut resolved = false;
            if options.include_raw_response_bodies {
                if let Some(hash) = &row.response_body_hash_raw {
                    if let Some(blob) = blob_map.get(hash) {
                        if !blob.content.is_empty() || blob.size <= 0 {
                            let (text, enc) = body_text_and_encoding(&blob.content);
                            response_body_text = text;
                            response_body_encoding = enc;
                            response_body_size_raw = Some(blob.content.len() as i64);
                            response_body_is_raw = true;
                            resolved = true;
                            if response_mime.is_none() {
                                response_mime = blob.mime_type.clone();
                            }
                        }
                    }
                }
            }

            if !resolved {
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
        }

        if response_body_is_raw {
            let raw_len = response_body_size_raw.unwrap_or(0);
            let uncompressed_len = if response_body_size > 0 {
                response_body_size
            } else {
                raw_len
            };
            response_body_size = uncompressed_len;
            if uncompressed_len > raw_len && raw_len > 0 {
                response_compression = Some(uncompressed_len - raw_len);
            }
        }

        let request_body_size = row.request_body_size.or(request_body_len);
        let response_body_size_field = if options.include_bodies && response_body_text.is_some() {
            if response_body_is_raw {
                response_body_size_raw
            } else {
                Some(response_body_size)
            }
        } else {
            row.response_body_size
        };

        let has_timing_parts = row.blocked_ms.is_some()
            || row.dns_ms.is_some()
            || row.connect_ms.is_some()
            || row.ssl_ms.is_some()
            || row.send_ms.is_some()
            || row.wait_ms.is_some()
            || row.receive_ms.is_some();
        let wait_ms =
            normalize_ms(row.wait_ms)
                .unwrap_or_else(|| if has_timing_parts { 0.0 } else { time_ms });
        let timings = Some(Timings {
            blocked: normalize_ms(row.blocked_ms),
            dns: normalize_ms(row.dns_ms),
            connect: normalize_ms(row.connect_ms),
            send: normalize_ms(row.send_ms).unwrap_or(0.0),
            wait: wait_ms,
            receive: normalize_ms(row.receive_ms).unwrap_or(0.0),
            ssl: normalize_ms(row.ssl_ms),
            extensions: Extensions::new(),
        });

        let post_data = if request_body_text.is_some() {
            Some(PostData {
                mime_type: request_mime_type(&request_headers),
                text: request_body_text,
                params: None,
                extensions: extensions_from_json(row.post_data_extensions.as_deref()),
            })
        } else if row
            .post_data_extensions
            .as_deref()
            .is_some_and(|json| !json.trim().is_empty())
        {
            Some(PostData {
                mime_type: None,
                text: None,
                params: None,
                extensions: extensions_from_json(row.post_data_extensions.as_deref()),
            })
        } else {
            None
        };

        let entry = Entry {
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
                extensions: extensions_from_json(row.request_extensions.as_deref()),
            },
            response: Response {
                status: row.status.unwrap_or(0),
                status_text: row.status_text.clone().unwrap_or_default(),
                http_version: row.http_version.clone().unwrap_or_default(),
                cookies: Some(response_cookies),
                headers: response_headers,
                content: Content {
                    size: response_body_size,
                    compression: response_compression,
                    mime_type: response_mime,
                    text: response_body_text,
                    encoding: response_body_encoding,
                    extensions: extensions_from_json(row.content_extensions.as_deref()),
                },
                redirect_url: None,
                headers_size: None,
                body_size: response_body_size_field,
                extensions: extensions_from_json(row.response_extensions.as_deref()),
            },
            cache: None,
            timings: timings.map(|t| Timings {
                extensions: extensions_from_json(row.timings_extensions.as_deref()),
                ..t
            }),
            server_ip_address: row.server_ip.clone(),
            connection: row.connection_id.clone(),
            extensions: extensions_from_json(row.entry_extensions.as_deref()),
        };

        if let Some(entry) = options.plugins.apply_export_entry(entry, &context)? {
            har_entries.push(entry);
        }
    }

    let log_extensions = match (multi_import, import_ids.len()) {
        (false, 1) => query_db_log_extension(&conn, import_ids[0])?,
        _ => Extensions::new(),
    };

    let mut har = Har {
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
            extensions: log_extensions,
        },
    };

    write_exported_har(
        &mut har,
        output_path,
        options.pretty,
        &options.plugins,
        &mut context,
    )
}

fn run_export_from_har(database: &Path, output_path: &Path, options: &ExportOptions) -> Result<()> {
    let mut har = crate::har::parse_har_file(database)?;
    let output_str = output_path.to_string_lossy();
    let source = database.to_string_lossy();
    let mut context = PluginContext {
        command: "export",
        source: Some(source.as_ref()),
        database: None,
        output: Some(output_str.as_ref()),
    };

    let from = options
        .from
        .as_deref()
        .map(|value| parse_started_at_bound(value, false))
        .transpose()?;
    let to = options
        .to
        .as_deref()
        .map(|value| parse_started_at_bound(value, true))
        .transpose()?;
    let min_request_size = options
        .min_request_size
        .as_deref()
        .map(crate::size::parse_size_bytes_i64)
        .transpose()?
        .flatten();
    let max_request_size = options
        .max_request_size
        .as_deref()
        .map(crate::size::parse_size_bytes_i64)
        .transpose()?
        .flatten();
    let min_response_size = options
        .min_response_size
        .as_deref()
        .map(crate::size::parse_size_bytes_i64)
        .transpose()?
        .flatten();
    let max_response_size = options
        .max_response_size
        .as_deref()
        .map(crate::size::parse_size_bytes_i64)
        .transpose()?
        .flatten();
    let url_regexes: Vec<Regex> = options
        .url_regex
        .iter()
        .map(|s| Regex::new(s))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let ext_filters: HashSet<String> = options
        .ext
        .iter()
        .map(|value| value.trim().trim_start_matches('.').to_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let mut needed_pages: HashSet<String> = HashSet::new();

    let mut filtered_entries: Vec<Entry> = Vec::with_capacity(har.log.entries.len());
    let source_path = database.to_string_lossy().to_string();
    if !matches_har_source_filter(&source_path, &options.source, &options.source_contains) {
        har.log.entries.clear();
        har.log.pages = None;
        return write_exported_har(
            &mut har,
            output_path,
            options.pretty,
            &options.plugins,
            &mut context,
        );
    }

    for entry in har.log.entries.drain(..) {
        if !matches_har_filters(
            &entry,
            &options.url,
            &options.url_contains,
            &url_regexes,
            &options.host,
            &options.method,
            &options.status,
            &options.mime_contains,
            &ext_filters,
            from,
            to,
            min_request_size,
            max_request_size,
            min_response_size,
            max_response_size,
        ) {
            continue;
        }

        let entry = apply_har_body_inclusion(entry, options.include_bodies);
        if let Some(page_id) = entry.pageref.clone() {
            needed_pages.insert(page_id);
        }
        if let Some(entry) = options.plugins.apply_export_entry(entry, &context)? {
            filtered_entries.push(entry);
        }
    }

    let filtered_pages = har
        .log
        .pages
        .take()
        .unwrap_or_default()
        .into_iter()
        .filter(|page| needed_pages.contains(&page.id))
        .collect::<Vec<_>>();
    har.log.entries = filtered_entries;
    har.log.pages = if filtered_pages.is_empty() {
        None
    } else {
        Some(filtered_pages)
    };

    write_exported_har(
        &mut har,
        output_path,
        options.pretty,
        &options.plugins,
        &mut context,
    )
}

fn matches_har_filters(
    entry: &Entry,
    url: &[String],
    url_contains: &[String],
    url_regexes: &[Regex],
    host: &[String],
    method: &[String],
    status: &[i32],
    mime_contains: &[String],
    exts: &HashSet<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    min_request_size: Option<i64>,
    max_request_size: Option<i64>,
    min_response_size: Option<i64>,
    max_response_size: Option<i64>,
) -> bool {
    let entry_url = entry.request.url.to_ascii_lowercase();
    if !url.is_empty() && !url.iter().any(|value| value == &entry.request.url) {
        return false;
    }
    if !url_contains.is_empty()
        && !url_contains
            .iter()
            .any(|value| entry_url.contains(&value.to_ascii_lowercase()))
    {
        return false;
    }
    if !url_regexes.is_empty() && !url_regexes.iter().any(|re| re.is_match(&entry.request.url)) {
        return false;
    }
    if !host.is_empty() {
        let entry_host = Url::parse(&entry.request.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned));
        if entry_host.is_none()
            || !host
                .iter()
                .any(|h| entry_host.as_deref() == Some(h.as_str()))
        {
            return false;
        }
    }
    if !method.is_empty() && !method.iter().any(|value| value == &entry.request.method) {
        return false;
    }
    if !status.is_empty() && !status.iter().any(|value| value == &entry.response.status) {
        return false;
    }
    if !mime_contains.is_empty() {
        let response_mime = entry.response.content.mime_type.as_deref().unwrap_or("");
        if !mime_contains.iter().any(|value| {
            response_mime
                .to_ascii_lowercase()
                .contains(&value.to_ascii_lowercase())
        }) {
            return false;
        }
    }
    if !exts.is_empty() {
        if let Some(ext) = entry_url_extension(&entry.request.url) {
            if !exts.contains(&ext) {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(started_at) = parse_started_at(&entry.started_date_time) {
        if let Some(from) = from {
            if started_at < from {
                return false;
            }
        }
        if let Some(to) = to {
            if started_at > to {
                return false;
            }
        }
    }

    let request_size = entry.request.body_size.unwrap_or(0);
    let request_size = if request_size < 0 { 0 } else { request_size };
    if let Some(min) = min_request_size {
        if request_size < min {
            return false;
        }
    }
    if let Some(max) = max_request_size {
        if request_size > max {
            return false;
        }
    }
    let response_size = entry.response.content.size;
    let response_size = if response_size < 0 { 0 } else { response_size };
    if let Some(min) = min_response_size {
        if response_size < min {
            return false;
        }
    }
    if let Some(max) = max_response_size {
        if response_size > max {
            return false;
        }
    }

    true
}

fn matches_har_source_filter(path: &str, source: &[String], source_contains: &[String]) -> bool {
    if source.is_empty() && source_contains.is_empty() {
        return true;
    }
    if !source.is_empty() {
        let path_filename = std::path::Path::new(path)
            .file_name()
            .and_then(|value| value.to_str());
        let has_exact_match = source
            .iter()
            .any(|value| value == path || path_filename == Some(value.as_str()));
        if !has_exact_match {
            return false;
        }
    }
    if !source_contains.is_empty() {
        let has_contains = source_contains.iter().any(|value| path.contains(value));
        if !has_contains {
            return false;
        }
    }
    true
}

fn apply_har_body_inclusion(mut entry: Entry, include_bodies: bool) -> Entry {
    if include_bodies {
        return entry;
    }

    if let Some(post_data) = entry.request.post_data.as_mut() {
        post_data.text = None;
        post_data.params = None;
    }
    entry.response.content.text = None;
    entry.response.content.encoding = None;
    entry
}

fn entry_url_extension(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let file = parsed.path().rsplit('/').next().unwrap_or("");
    let ext = file.rsplit('.').next()?;
    if ext == file {
        return None;
    }
    Some(ext.to_lowercase())
}

fn write_exported_har(
    har: &mut Har,
    output_path: &Path,
    pretty: bool,
    plugins: &PluginSet,
    context: &mut PluginContext<'_>,
) -> Result<()> {
    let export_outcome = plugins.run_exporters(har, context)?;
    if export_outcome.skip_default {
        if export_outcome.ran {
            println!("Export handled by plugin(s); skipping default HAR output.");
        }
        return Ok(());
    }

    let mut writer = open_output(output_path)?;
    if pretty {
        serde_json::to_writer_pretty(&mut writer, &har)?;
    } else {
        serde_json::to_writer(&mut writer, &har)?;
    }
    writer.write_all(b"\n")?;

    if output_path != Path::new("-") {
        println!(
            "Exported {} entries to {}",
            har.log.entries.len(),
            output_path.display()
        );
    }

    Ok(())
}

fn parse_started_at_bound(value: &str, is_end: bool) -> Result<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(HarliteError::InvalidHar(
            "Empty timestamp bound".to_string(),
        ));
    }

    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Ok(parsed.with_timezone(&Utc));
    }
    let date = chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")?;
    let dt = if is_end {
        date.and_hms_milli_opt(23, 59, 59, 999)
            .and_then(|d| d.and_local_timezone(chrono::Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid end date".to_string()))?
    } else {
        date.and_hms_opt(0, 0, 0)
            .and_then(|d| d.and_local_timezone(chrono::Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid start date".to_string()))?
    };
    Ok(dt)
}

fn parse_started_at(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn resolve_export_format(
    path: &Path,
    format: Option<ExportInputFormat>,
) -> Result<ExportInputFormat> {
    match format {
        Some(format) => Ok(format),
        None => detect_export_format(path),
    }
}

fn detect_export_format(path: &Path) -> Result<ExportInputFormat> {
    if is_db_path(path) {
        return Ok(ExportInputFormat::Db);
    }
    if is_har_path(path) {
        return Ok(ExportInputFormat::Har);
    }
    Err(HarliteError::InvalidArgs(format!(
        "Could not infer export input format for {}. Use --format to specify har|db",
        path.display()
    )))
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

fn is_har_path(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
        return false;
    };
    let ext = ext.to_ascii_lowercase();
    if matches!(ext.as_str(), "har" | "json") {
        return true;
    }
    if ext == "br" || ext == "gz" {
        let name = path.to_string_lossy().to_ascii_lowercase();
        return name.ends_with(".har.br") || name.ends_with(".har.gz");
    }
    false
}

fn query_db_log_extension(conn: &Connection, import_id: i64) -> Result<Extensions> {
    match conn.query_row(
        "SELECT log_extensions FROM imports WHERE id = ?1",
        [import_id],
        |row| row.get::<_, Option<String>>(0),
    ) {
        Ok(Some(s)) => Ok(extensions_from_json(Some(s.as_str()))),
        Ok(None) => Ok(Extensions::new()),
        Err(_) => Ok(Extensions::new()),
    }
}
