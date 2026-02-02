use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use rusqlite::Connection;
use url::Url;

use crate::db::{ensure_schema_upgrades, load_blobs_by_hashes, load_pages_for_imports, BlobRow};
use crate::error::{HarliteError, Result};
use crate::har::{
    Content, Cookie, Creator, Entry, Extensions, Har, Header, Log, Page, PageTimings, PostData,
    QueryParam, Request, Response, Timings,
};
use crate::plugins::{PluginContext, PluginSet};
use super::entry_filter::{load_entries_with_filters, EntryFilterOptions};

/// Options for exporting a harlite database back to a HAR file.
pub struct ExportOptions {
    pub output: Option<PathBuf>,
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

/// Export a harlite SQLite database back to a HAR file.
pub fn run_export(database: PathBuf, options: &ExportOptions) -> Result<()> {
    let conn = Connection::open(&database)?;
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
    let output_str = output_path.to_string_lossy();
    let database_str = database.to_string_lossy();
    let context = PluginContext {
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
        let wait_ms = normalize_ms(row.wait_ms).unwrap_or_else(|| if has_timing_parts { 0.0 } else { time_ms });
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

    let log_extensions = if !multi_import && import_ids.len() == 1 {
        conn.query_row(
            "SELECT log_extensions FROM imports WHERE id = ?1",
            [import_ids[0]],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
        .map(|s| extensions_from_json(Some(s.as_str())))
        .unwrap_or_default()
    } else {
        Extensions::new()
    };

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
            extensions: log_extensions,
        },
    };

    let export_outcome = options.plugins.run_exporters(&har, &context)?;
    if export_outcome.skip_default {
        if export_outcome.ran {
            println!("Export handled by plugin(s); skipping default HAR output.");
        }
        return Ok(());
    }

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
