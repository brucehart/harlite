use rusqlite::{params, Connection};
use url::Url;

use crate::error::Result;
use crate::har::{Cookie, Entry, Header, Page};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Summary statistics for an import operation.
pub struct ImportStats {
    pub entries_imported: usize,
    pub blobs_created: usize,
    pub blobs_deduplicated: usize,
    pub bytes_stored: usize,
    pub bytes_deduplicated: usize,
}

/// Create an import record and return its row id.
pub fn create_import(conn: &Connection, source_file: &str) -> Result<i64> {
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO imports (source_file, imported_at, entry_count) VALUES (?1, ?2, 0)",
        params![source_file, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Update the entry count for an import record.
pub fn update_import_count(conn: &Connection, import_id: i64, count: usize) -> Result<()> {
    conn.execute(
        "UPDATE imports SET entry_count = ?1 WHERE id = ?2",
        params![count as i64, import_id],
    )?;
    Ok(())
}

/// Insert a page record.
pub fn insert_page(conn: &Connection, import_id: i64, page: &Page) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO pages (id, import_id, started_at, title, on_content_load_ms, on_load_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            page.id,
            import_id,
            page.started_date_time,
            page.title,
            page.page_timings.as_ref().and_then(|t| t.on_content_load),
            page.page_timings.as_ref().and_then(|t| t.on_load),
        ],
    )?;
    Ok(())
}

/// Store content as a deduplicated blob, returning the hash and whether it was newly inserted.
pub fn store_blob(
    conn: &Connection,
    content: &[u8],
    mime_type: Option<&str>,
    external_path: Option<&str>,
    store_inline: bool,
) -> Result<(String, bool)> {
    let hash = blake3::hash(content).to_hex().to_string();

    let content_to_store: &[u8] = if store_inline { content } else { &[] };
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO blobs (hash, content, size, mime_type, external_path) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            hash,
            content_to_store,
            content.len() as i64,
            mime_type,
            external_path
        ],
    )?;

    if inserted == 0 && external_path.is_some() {
        conn.execute(
            "UPDATE blobs SET external_path = COALESCE(external_path, ?2) WHERE hash = ?1",
            params![hash, external_path],
        )?;
    }

    Ok((hash, inserted > 0))
}

fn headers_to_json(headers: &[Header]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .map(|h| {
            (
                h.name.to_lowercase(),
                serde_json::Value::String(h.value.clone()),
            )
        })
        .collect();
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

fn cookies_to_json(cookies: &Option<Vec<Cookie>>) -> String {
    match cookies {
        Some(c) => serde_json::to_string(c).unwrap_or_else(|_| "[]".to_string()),
        None => "[]".to_string(),
    }
}

fn parse_url_parts(url_str: &str) -> (Option<String>, Option<String>, Option<String>) {
    match Url::parse(url_str) {
        Ok(url) => {
            let host = url.host_str().map(|s| s.to_string());
            let path = Some(url.path().to_string());
            let query = url.query().map(|s| s.to_string());
            (host, path, query)
        }
        Err(_) => (None, None, None),
    }
}

/// Options controlling how entry bodies are stored.
pub struct InsertEntryOptions {
    pub store_bodies: bool,
    pub max_body_size: Option<usize>,
    pub text_only: bool,
    pub decompress_bodies: bool,
    pub keep_compressed: bool,
    pub extract_bodies_dir: Option<PathBuf>,
    pub extract_bodies_kind: ExtractBodiesKind,
    pub extract_bodies_shard_depth: u8,
}

impl Default for InsertEntryOptions {
    fn default() -> Self {
        Self {
            store_bodies: false,
            max_body_size: Some(100 * 1024),
            text_only: false,
            decompress_bodies: false,
            keep_compressed: false,
            extract_bodies_dir: None,
            extract_bodies_kind: ExtractBodiesKind::Both,
            extract_bodies_shard_depth: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum ExtractBodiesKind {
    Request,
    Response,
    Both,
}

fn extract_for_request(kind: ExtractBodiesKind) -> bool {
    matches!(kind, ExtractBodiesKind::Request | ExtractBodiesKind::Both)
}

fn extract_for_response(kind: ExtractBodiesKind) -> bool {
    matches!(kind, ExtractBodiesKind::Response | ExtractBodiesKind::Both)
}

fn is_text_mime_type(mime: Option<&str>) -> bool {
    match mime {
        None => false,
        Some(m) => {
            let m = m.to_lowercase();
            m.contains("text/")
                || m.contains("json")
                || m.contains("xml")
                || m.contains("javascript")
                || m.contains("css")
                || m.contains("html")
                || m.contains("x-www-form-urlencoded")
        }
    }
}

fn header_value(headers: &[Header], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn decode_body(content: &crate::har::Content) -> Option<Vec<u8>> {
    let text = content.text.as_ref()?;

    match content.encoding.as_deref() {
        Some("base64") => {
            use base64::{engine::general_purpose::STANDARD, Engine};
            STANDARD.decode(text).ok()
        }
        _ => Some(text.as_bytes().to_vec()),
    }
}

fn synthesize_post_params(post_data: &crate::har::PostData) -> Option<(Vec<u8>, Option<String>)> {
    let params = post_data.params.as_ref()?;
    if params.is_empty() {
        return None;
    }

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for param in params {
        let value = param.value.as_deref().unwrap_or("");
        serializer.append_pair(&param.name, value);
    }
    let body = serializer.finish();
    if body.is_empty() {
        return None;
    }

    let mime = post_data
        .mime_type
        .clone()
        .or_else(|| Some("application/x-www-form-urlencoded".to_string()));
    Some((body.into_bytes(), mime))
}

fn read_to_end_limited<R: Read>(mut r: R, max: Option<usize>) -> std::io::Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 16 * 1024];

    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        if let Some(max) = max {
            if out.len().saturating_add(n) > max {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "decompressed body exceeded limit",
                ));
            }
        }
        out.extend_from_slice(&buf[..n]);
    }

    Ok(out)
}

fn decompress_body(
    body: &[u8],
    content_encoding: &str,
    max_output: Option<usize>,
) -> Option<Vec<u8>> {
    let encs: Vec<String> = content_encoding
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty() && s != "identity")
        .collect();

    if encs.is_empty() {
        return None;
    }

    // Decode in reverse order if multiple encodings are present.
    let mut current: Vec<u8> = body.to_vec();
    for enc in encs.into_iter().rev() {
        match enc.as_str() {
            "gzip" | "x-gzip" => {
                let mut decoder = flate2::read::GzDecoder::new(&current[..]);
                current = read_to_end_limited(&mut decoder, max_output).ok()?;
            }
            "br" => {
                let mut decoder = brotli::Decompressor::new(&current[..], 4096);
                current = read_to_end_limited(&mut decoder, max_output).ok()?;
            }
            _ => return None,
        }
    }

    Some(current)
}

fn blob_path(root: &Path, hash: &str, shard_depth: u8) -> PathBuf {
    let mut out = root.to_path_buf();
    let depth = shard_depth as usize;
    for i in 0..depth {
        let start = i * 2;
        let end = start + 2;
        if hash.len() >= end {
            out.push(&hash[start..end]);
        }
    }
    out.push(hash);
    out
}

fn write_blob_if_missing(path: &Path, content: &[u8]) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(content)?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn maybe_index_response_body_fts(
    conn: &Connection,
    hash: &str,
    body: &[u8],
    mime: Option<&str>,
    max_bytes: Option<usize>,
) -> Result<()> {
    if body.is_empty() {
        return Ok(());
    }

    let Some(max) = max_bytes else {
        // Still keep a safety cap to avoid unbounded indexing.
        return maybe_index_response_body_fts(conn, hash, body, mime, Some(1024 * 1024));
    };
    if body.len() > max {
        return Ok(());
    }

    let text = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };

    if mime.is_some() && !is_text_mime_type(mime) {
        return Ok(());
    }

    conn.execute(
        "DELETE FROM response_body_fts WHERE hash = ?1",
        params![hash],
    )?;
    conn.execute(
        "INSERT INTO response_body_fts (hash, body) VALUES (?1, ?2)",
        params![hash, text],
    )?;
    Ok(())
}

const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 50 * 1024 * 1024;

/// Insert an entry into the database and optionally store bodies.
pub fn insert_entry(
    conn: &Connection,
    import_id: i64,
    entry: &Entry,
    options: &InsertEntryOptions,
) -> Result<(bool, usize)> {
    let (host, path, query_string) = parse_url_parts(&entry.request.url);

    let request_headers_json = headers_to_json(&entry.request.headers);
    let response_headers_json = headers_to_json(&entry.response.headers);
    let request_cookies_json = cookies_to_json(&entry.request.cookies);
    let response_cookies_json = cookies_to_json(&entry.response.cookies);

    let is_redirect = if (300..400).contains(&entry.response.status) {
        1
    } else {
        0
    };

    let mut request_body_hash: Option<String> = None;
    let request_body_size = entry.request.body_size.filter(|&s| s >= 0);

    let mut response_body_hash: Option<String> = None;
    let mut response_body_size = if entry.response.content.size >= 0 {
        Some(entry.response.content.size)
    } else {
        None
    };
    let mut response_body_hash_raw: Option<String> = None;
    let mut response_body_size_raw: Option<i64> = None;
    let mut response_mime_owned = entry.response.content.mime_type.clone();
    if response_mime_owned.is_none() {
        response_mime_owned = header_value(&entry.response.headers, "content-type")
            .map(|v| v.split(';').next().unwrap_or(v.as_str()).trim().to_string());
    }
    let response_mime = response_mime_owned.as_deref();

    let mut blob_created = false;
    let mut bytes_accounted = 0usize;

    if options.store_bodies {
        if let Some(mut body) = decode_body(&entry.response.content) {
            let type_ok = !options.text_only || is_text_mime_type(response_mime);
            if type_ok && !body.is_empty() {
                if options.decompress_bodies {
                    if let Some(enc) = header_value(&entry.response.headers, "content-encoding") {
                        let decompress_limit =
                            options.max_body_size.unwrap_or(DEFAULT_MAX_DECOMPRESSED_BYTES);
                        if let Some(decompressed) =
                            decompress_body(&body, &enc, Some(decompress_limit))
                        {
                            if options.keep_compressed {
                                let raw_size_ok =
                                    options.max_body_size.is_none_or(|max| body.len() <= max);
                                if raw_size_ok {
                                    response_body_size_raw = Some(body.len() as i64);
                                    let (hash, is_new) =
                                        store_response_blob(conn, &body, response_mime, options)?;
                                    response_body_hash_raw = Some(hash);
                                    if is_new {
                                        blob_created = true;
                                    }
                                }
                            }
                            body = decompressed;
                        }
                    }
                }

                let size_ok = options.max_body_size.is_none_or(|max| body.len() <= max);
                if size_ok {
                    let (hash, is_new) = store_response_blob(conn, &body, response_mime, options)?;
                    response_body_hash = Some(hash.clone());
                    response_body_size = Some(body.len() as i64);
                    if is_new {
                        blob_created = true;
                    }
                    bytes_accounted = body.len();

                    // Index decompressed/stored bytes when they are text.
                    maybe_index_response_body_fts(
                        conn,
                        &hash,
                        &body,
                        response_mime,
                        options.max_body_size,
                    )?;
                }
            }
        }

        if let Some(post_data) = &entry.request.post_data {
            if let Some(text) = &post_data.text {
                let body = text.as_bytes();
                let size_ok = options.max_body_size.is_none_or(|max| body.len() <= max);
                let mime = post_data.mime_type.as_deref();
                let type_ok = !options.text_only || is_text_mime_type(mime);

                if size_ok && type_ok && !body.is_empty() {
                    let (hash, is_new) = store_request_blob(conn, body, mime, options)?;
                    request_body_hash = Some(hash);
                    if is_new {
                        blob_created = true;
                    }
                    bytes_accounted = body.len();
                }
            } else if let Some((body, mime)) = synthesize_post_params(post_data) {
                let size_ok = options.max_body_size.is_none_or(|max| body.len() <= max);
                let mime = mime.as_deref();
                let type_ok = !options.text_only || is_text_mime_type(mime);

                if size_ok && type_ok && !body.is_empty() {
                    let (hash, is_new) = store_request_blob(conn, &body, mime, options)?;
                    request_body_hash = Some(hash);
                    if is_new {
                        blob_created = true;
                    }
                    bytes_accounted = body.len();
                }
            }
        }
    }

    conn.execute(
        "INSERT INTO entries (
            import_id, page_id, started_at, time_ms,
            method, url, host, path, query_string, http_version,
            request_headers, request_cookies, request_body_hash, request_body_size,
            status, status_text, response_headers, response_cookies,
            response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw, response_mime_type,
            is_redirect, server_ip, connection_id
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26
        )",
        params![
            import_id,
            entry.pageref,
            entry.started_date_time,
            entry.time,
            entry.request.method,
            entry.request.url,
            host,
            path,
            query_string,
            entry.request.http_version,
            request_headers_json,
            request_cookies_json,
            request_body_hash,
            request_body_size,
            entry.response.status,
            entry.response.status_text,
            response_headers_json,
            response_cookies_json,
            response_body_hash,
            response_body_size,
            response_body_hash_raw,
            response_body_size_raw,
            response_mime,
            is_redirect,
            entry.server_ip_address,
            entry.connection,
        ],
    )?;

    Ok((blob_created, bytes_accounted))
}

fn store_request_blob(
    conn: &Connection,
    body: &[u8],
    mime: Option<&str>,
    options: &InsertEntryOptions,
) -> Result<(String, bool)> {
    let (external_path, store_inline) =
        match (&options.extract_bodies_dir, options.extract_bodies_kind) {
            (Some(dir), kind) if extract_for_request(kind) => {
                let hash = blake3::hash(body).to_hex().to_string();
                let path = blob_path(dir, &hash, options.extract_bodies_shard_depth);
                write_blob_if_missing(&path, body)?;
                (Some(path.to_string_lossy().to_string()), false)
            }
            _ => (None, true),
        };
    store_blob(conn, body, mime, external_path.as_deref(), store_inline)
}

fn store_response_blob(
    conn: &Connection,
    body: &[u8],
    mime: Option<&str>,
    options: &InsertEntryOptions,
) -> Result<(String, bool)> {
    let (external_path, store_inline) =
        match (&options.extract_bodies_dir, options.extract_bodies_kind) {
            (Some(dir), kind) if extract_for_response(kind) => {
                let hash = blake3::hash(body).to_hex().to_string();
                let path = blob_path(dir, &hash, options.extract_bodies_shard_depth);
                write_blob_if_missing(&path, body)?;
                (Some(path.to_string_lossy().to_string()), false)
            }
            _ => (None, true),
        };
    store_blob(conn, body, mime, external_path.as_deref(), store_inline)
}

#[cfg(test)]
mod tests {
    use super::{insert_entry, InsertEntryOptions};
    use crate::db::create_schema;
    use crate::har::Har;
    use rusqlite::{params, Connection};

    #[test]
    fn inserts_entry_and_blob() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        create_schema(&conn).expect("schema created");

        let json = r#"
        {
          "log": {
            "entries": [
              {
                "startedDateTime": "2024-01-15T10:30:00.000Z",
                "time": 123.0,
                "request": {
                  "method": "GET",
                  "url": "https://example.com/path?x=1",
                  "httpVersion": "HTTP/1.1",
                  "headers": [{"name": "Accept", "value": "text/html"}],
                  "cookies": [],
                  "headersSize": 100,
                  "bodySize": 0
                },
                "response": {
                  "status": 200,
                  "statusText": "OK",
                  "httpVersion": "HTTP/1.1",
                  "headers": [{"name": "Content-Type", "value": "text/html"}],
                  "cookies": [],
                  "content": {
                    "size": 13,
                    "mimeType": "text/html",
                    "text": "<html></html>"
                  },
                  "redirectURL": "",
                  "headersSize": 50,
                  "bodySize": 13
                },
                "cache": {},
                "timings": {"send": 1, "wait": 10, "receive": 2}
              }
            ]
          }
        }
        "#;

        let har: Har = serde_json::from_str(json).expect("parse har");
        let entry = &har.log.entries[0];

        let options = InsertEntryOptions {
            store_bodies: true,
            max_body_size: None,
            text_only: false,
            ..Default::default()
        };

        let import_id = 1i64;
        conn.execute(
            "INSERT INTO imports (id, source_file, imported_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
            params![import_id, "test.har", "2024-01-01T00:00:00Z", 0],
        )
        .expect("insert import");

        let (_created, _bytes) =
            insert_entry(&conn, import_id, entry, &options).expect("insert entry");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
            .expect("count entries");
        assert_eq!(count, 1);

        let blob_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .expect("count blobs");
        assert_eq!(blob_count, 1);

        let host: String = conn
            .query_row("SELECT host FROM entries", [], |r| r.get(0))
            .expect("host");
        assert_eq!(host, "example.com");
    }

    #[test]
    fn inserts_params_only_request_body() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        create_schema(&conn).expect("schema created");

        let json = r#"
        {
          "log": {
            "entries": [
              {
                "startedDateTime": "2024-01-15T10:30:00.000Z",
                "time": 45.0,
                "request": {
                  "method": "POST",
                  "url": "https://example.com/form",
                  "httpVersion": "HTTP/1.1",
                  "headers": [{"name": "Content-Type", "value": "application/x-www-form-urlencoded"}],
                  "postData": {
                    "params": [
                      {"name": "a", "value": "1"},
                      {"name": "b", "value": "two words"}
                    ]
                  }
                },
                "response": {
                  "status": 204,
                  "statusText": "No Content",
                  "httpVersion": "HTTP/1.1",
                  "headers": [],
                  "content": {
                    "size": 0
                  }
                }
              }
            ]
          }
        }
        "#;

        let har: Har = serde_json::from_str(json).expect("parse har");
        let entry = &har.log.entries[0];

        let options = InsertEntryOptions {
            store_bodies: true,
            max_body_size: None,
            text_only: false,
            ..Default::default()
        };

        let import_id = 1i64;
        conn.execute(
            "INSERT INTO imports (id, source_file, imported_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
            params![import_id, "test.har", "2024-01-01T00:00:00Z", 0],
        )
        .expect("insert import");

        insert_entry(&conn, import_id, entry, &options).expect("insert entry");

        let (hash, body): (String, Vec<u8>) = conn
            .query_row(
                "SELECT blobs.hash, blobs.content FROM entries JOIN blobs ON entries.request_body_hash = blobs.hash",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("request body blob");

        assert!(!hash.is_empty());
        assert_eq!(String::from_utf8(body).expect("utf8"), "a=1&b=two+words");
    }

    #[test]
    fn params_only_request_body_respects_max_body_size() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        create_schema(&conn).expect("schema created");

        let json = r#"
        {
          "log": {
            "entries": [
              {
                "startedDateTime": "2024-01-15T10:30:00.000Z",
                "time": 45.0,
                "request": {
                  "method": "POST",
                  "url": "https://example.com/form",
                  "httpVersion": "HTTP/1.1",
                  "headers": [],
                  "postData": {
                    "params": [
                      {"name": "a", "value": "1"},
                      {"name": "b", "value": "two words"}
                    ]
                  }
                },
                "response": {
                  "status": 204,
                  "statusText": "No Content",
                  "httpVersion": "HTTP/1.1",
                  "headers": [],
                  "content": {
                    "size": 0
                  }
                }
              }
            ]
          }
        }
        "#;

        let har: Har = serde_json::from_str(json).expect("parse har");
        let entry = &har.log.entries[0];

        let options = InsertEntryOptions {
            store_bodies: true,
            max_body_size: Some(5),
            text_only: false,
            ..Default::default()
        };

        let import_id = 1i64;
        conn.execute(
            "INSERT INTO imports (id, source_file, imported_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
            params![import_id, "test.har", "2024-01-01T00:00:00Z", 0],
        )
        .expect("insert import");

        insert_entry(&conn, import_id, entry, &options).expect("insert entry");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .expect("count blobs");
        assert_eq!(count, 0);

        let request_hash: Option<String> = conn
            .query_row("SELECT request_body_hash FROM entries", [], |r| r.get(0))
            .expect("request body hash");
        assert!(request_hash.is_none());
    }
}
