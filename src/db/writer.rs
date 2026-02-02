use rusqlite::{params, Connection};
use serde_json::{Map, Value};
use url::Url;

use crate::commands::util::{parse_timestamp, parse_timestamp_number};
use crate::error::Result;
use crate::graphql::extract_graphql_info;
use crate::har::{Cookie, Entry, Header, Page};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Summary statistics for an import operation.
pub struct ImportStats {
    pub entries_imported: usize,
    pub entries_skipped: usize,
    pub request: BlobStats,
    pub response: BlobStats,
}

impl Default for ImportStats {
    fn default() -> Self {
        Self {
            entries_imported: 0,
            entries_skipped: 0,
            request: BlobStats::default(),
            response: BlobStats::default(),
        }
    }
}

impl ImportStats {
    pub fn add_assign(&mut self, other: ImportStats) {
        self.entries_imported += other.entries_imported;
        self.entries_skipped += other.entries_skipped;
        self.request.add_assign(other.request);
        self.response.add_assign(other.response);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobStats {
    pub created: usize,
    pub deduplicated: usize,
    pub bytes_stored: usize,
    pub bytes_deduplicated: usize,
}

impl BlobStats {
    fn record(&mut self, is_new: bool, bytes: usize) {
        if bytes == 0 {
            return;
        }

        if is_new {
            self.created += 1;
            self.bytes_stored += bytes;
        } else {
            self.deduplicated += 1;
            self.bytes_deduplicated += bytes;
        }
    }

    pub fn add_assign(&mut self, other: BlobStats) {
        self.created += other.created;
        self.deduplicated += other.deduplicated;
        self.bytes_stored += other.bytes_stored;
        self.bytes_deduplicated += other.bytes_deduplicated;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EntryBlobStats {
    pub request: BlobStats,
    pub response: BlobStats,
}

pub struct EntryInsertResult {
    pub inserted: bool,
    pub blob_stats: EntryBlobStats,
}

#[derive(Clone, Debug, Default)]
pub struct EntryRelations {
    pub request_id: Option<String>,
    pub parent_request_id: Option<String>,
    pub initiator_type: Option<String>,
    pub initiator_url: Option<String>,
    pub initiator_line: Option<i64>,
    pub initiator_column: Option<i64>,
    pub redirect_url: Option<String>,
}

#[derive(Default, Clone, Debug)]
struct TlsDetails {
    version: Option<String>,
    cipher_suite: Option<String>,
    cert_subject: Option<String>,
    cert_issuer: Option<String>,
    cert_expiry: Option<String>,
}

#[allow(dead_code)]
/// Create an import record and return its row id.
pub fn create_import(
    conn: &Connection,
    source_file: &str,
    log_extensions: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<i64> {
    create_import_with_status(conn, source_file, log_extensions, "complete", None, None)
}

/// Create an import record with explicit status/progress metadata.
pub fn create_import_with_status(
    conn: &Connection,
    source_file: &str,
    log_extensions: Option<&serde_json::Map<String, serde_json::Value>>,
    status: &str,
    entries_total: Option<usize>,
    entries_skipped: Option<usize>,
) -> Result<i64> {
    let now = chrono::Utc::now().to_rfc3339();
    let log_extensions_json = log_extensions.and_then(extensions_to_json);
    conn.execute(
        "INSERT INTO imports (source_file, imported_at, entry_count, log_extensions, status, entries_total, entries_skipped)
         VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6)",
        params![
            source_file,
            now,
            log_extensions_json,
            status,
            entries_total.map(|v| v as i64),
            entries_skipped.map(|v| v as i64)
        ],
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

/// Update import metadata fields (status/progress).
pub fn update_import_metadata(
    conn: &Connection,
    import_id: i64,
    entry_count: Option<usize>,
    entries_total: Option<usize>,
    entries_skipped: Option<usize>,
    status: Option<&str>,
) -> Result<()> {
    if let Some(count) = entry_count {
        conn.execute(
            "UPDATE imports SET entry_count = ?1 WHERE id = ?2",
            params![count as i64, import_id],
        )?;
    }
    if let Some(total) = entries_total {
        conn.execute(
            "UPDATE imports SET entries_total = ?1 WHERE id = ?2",
            params![total as i64, import_id],
        )?;
    }
    if let Some(skipped) = entries_skipped {
        conn.execute(
            "UPDATE imports SET entries_skipped = ?1 WHERE id = ?2",
            params![skipped as i64, import_id],
        )?;
    }
    if let Some(status) = status {
        conn.execute(
            "UPDATE imports SET status = ?1 WHERE id = ?2",
            params![status, import_id],
        )?;
    }
    Ok(())
}

/// Insert a page record.
pub fn insert_page(conn: &Connection, import_id: i64, page: &Page) -> Result<()> {
    let page_extensions_json = extensions_to_json(&page.extensions);
    let timings_extensions_json = page
        .page_timings
        .as_ref()
        .and_then(|t| extensions_to_json(&t.extensions));
    conn.execute(
        "INSERT OR IGNORE INTO pages (id, import_id, started_at, title, on_content_load_ms, on_load_ms, page_extensions, page_timings_extensions)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            page.id,
            import_id,
            page.started_date_time,
            page.title,
            page.page_timings.as_ref().and_then(|t| t.on_content_load),
            page.page_timings.as_ref().and_then(|t| t.on_load),
            page_extensions_json,
            timings_extensions_json,
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

fn extensions_to_json(extensions: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    if extensions.is_empty() {
        None
    } else {
        serde_json::to_string(extensions).ok()
    }
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

fn encode_string(buf: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn encode_opt_string(buf: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(s) => {
            buf.push(1);
            encode_string(buf, s);
        }
        None => buf.push(0),
    }
}

fn encode_opt_i64(buf: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        None => buf.push(0),
    }
}

fn encode_opt_f64(buf: &mut Vec<u8>, value: Option<f64>) {
    match value {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        None => buf.push(0),
    }
}

/// Stable content hash for an entry (v1).
pub fn entry_content_hash(entry: &Entry) -> String {
    let (host, path, query_string) = parse_url_parts(&entry.request.url);
    let request_headers_json = headers_to_json(&entry.request.headers);
    let response_headers_json = headers_to_json(&entry.response.headers);
    let request_cookies_json = cookies_to_json(&entry.request.cookies);
    let response_cookies_json = cookies_to_json(&entry.response.cookies);
    let entry_extensions_json = extensions_to_json(&entry.extensions);
    let request_extensions_json = extensions_to_json(&entry.request.extensions);
    let response_extensions_json = extensions_to_json(&entry.response.extensions);
    let content_extensions_json = extensions_to_json(&entry.response.content.extensions);
    let timings_extensions_json = entry
        .timings
        .as_ref()
        .and_then(|timings| extensions_to_json(&timings.extensions));
    let post_data_extensions_json = entry
        .request
        .post_data
        .as_ref()
        .and_then(|post| extensions_to_json(&post.extensions));
    let request_body_size = entry.request.body_size.filter(|&s| s >= 0);
    let response_mime = entry
        .response
        .content
        .mime_type
        .clone()
        .or_else(|| header_value(&entry.response.headers, "content-type"))
        .map(|v| v.split(';').next().unwrap_or(v.as_str()).trim().to_string());

    let fields = EntryHashFields {
        page_id: entry.pageref.as_deref(),
        started_at: Some(&entry.started_date_time),
        time_ms: Some(entry.time),
        method: Some(&entry.request.method),
        url: Some(&entry.request.url),
        host: host.as_deref(),
        path: path.as_deref(),
        query_string: query_string.as_deref(),
        http_version: Some(&entry.request.http_version),
        request_headers: Some(&request_headers_json),
        request_cookies: Some(&request_cookies_json),
        request_body_size,
        status: Some(i64::from(entry.response.status)),
        status_text: Some(&entry.response.status_text),
        response_headers: Some(&response_headers_json),
        response_cookies: Some(&response_cookies_json),
        response_mime_type: response_mime.as_deref(),
        is_redirect: Some(if (300..400).contains(&entry.response.status) {
            1
        } else {
            0
        }),
        server_ip: entry.server_ip_address.as_deref(),
        connection_id: entry.connection.as_deref(),
        entry_extensions: entry_extensions_json.as_deref(),
        request_extensions: request_extensions_json.as_deref(),
        response_extensions: response_extensions_json.as_deref(),
        content_extensions: content_extensions_json.as_deref(),
        timings_extensions: timings_extensions_json.as_deref(),
        post_data_extensions: post_data_extensions_json.as_deref(),
    };

    entry_hash_from_fields(&fields)
}

pub struct EntryHashFields<'a> {
    pub page_id: Option<&'a str>,
    pub started_at: Option<&'a str>,
    pub time_ms: Option<f64>,
    pub method: Option<&'a str>,
    pub url: Option<&'a str>,
    pub host: Option<&'a str>,
    pub path: Option<&'a str>,
    pub query_string: Option<&'a str>,
    pub http_version: Option<&'a str>,
    pub request_headers: Option<&'a str>,
    pub request_cookies: Option<&'a str>,
    pub request_body_size: Option<i64>,
    pub status: Option<i64>,
    pub status_text: Option<&'a str>,
    pub response_headers: Option<&'a str>,
    pub response_cookies: Option<&'a str>,
    pub response_mime_type: Option<&'a str>,
    pub is_redirect: Option<i64>,
    pub server_ip: Option<&'a str>,
    pub connection_id: Option<&'a str>,
    pub entry_extensions: Option<&'a str>,
    pub request_extensions: Option<&'a str>,
    pub response_extensions: Option<&'a str>,
    pub content_extensions: Option<&'a str>,
    pub timings_extensions: Option<&'a str>,
    pub post_data_extensions: Option<&'a str>,
}

pub fn entry_hash_from_fields(fields: &EntryHashFields<'_>) -> String {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"harlite:entry:v1");
    encode_opt_string(&mut buf, fields.page_id);
    encode_opt_string(&mut buf, fields.started_at);
    encode_opt_f64(&mut buf, fields.time_ms);
    encode_opt_string(&mut buf, fields.method);
    encode_opt_string(&mut buf, fields.url);
    encode_opt_string(&mut buf, fields.host);
    encode_opt_string(&mut buf, fields.path);
    encode_opt_string(&mut buf, fields.query_string);
    encode_opt_string(&mut buf, fields.http_version);
    encode_opt_string(&mut buf, fields.request_headers);
    encode_opt_string(&mut buf, fields.request_cookies);
    encode_opt_i64(&mut buf, fields.request_body_size);
    encode_opt_i64(&mut buf, fields.status);
    encode_opt_string(&mut buf, fields.status_text);
    encode_opt_string(&mut buf, fields.response_headers);
    encode_opt_string(&mut buf, fields.response_cookies);
    encode_opt_string(&mut buf, fields.response_mime_type);
    encode_opt_i64(&mut buf, fields.is_redirect);
    encode_opt_string(&mut buf, fields.server_ip);
    encode_opt_string(&mut buf, fields.connection_id);
    encode_opt_string(&mut buf, fields.entry_extensions);
    encode_opt_string(&mut buf, fields.request_extensions);
    encode_opt_string(&mut buf, fields.response_extensions);
    encode_opt_string(&mut buf, fields.content_extensions);
    encode_opt_string(&mut buf, fields.timings_extensions);
    encode_opt_string(&mut buf, fields.post_data_extensions);
    blake3::hash(&buf).to_hex().to_string()
}

/// Options controlling how entry bodies are stored.
#[derive(Clone)]
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

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
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

fn tls_details_is_empty(details: &TlsDetails) -> bool {
    details.version.is_none()
        && details.cipher_suite.is_none()
        && details.cert_subject.is_none()
        && details.cert_issuer.is_none()
        && details.cert_expiry.is_none()
}

fn merge_tls_details(target: &mut TlsDetails, incoming: TlsDetails) {
    if target.version.is_none() {
        target.version = incoming.version;
    }
    if target.cipher_suite.is_none() {
        target.cipher_suite = incoming.cipher_suite;
    }
    if target.cert_subject.is_none() {
        target.cert_subject = incoming.cert_subject;
    }
    if target.cert_issuer.is_none() {
        target.cert_issuer = incoming.cert_issuer;
    }
    if target.cert_expiry.is_none() {
        target.cert_expiry = incoming.cert_expiry;
    }
}

fn get_string_value(map: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            if let Some(s) = value.as_str() {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

fn get_value(map: &Map<String, Value>, keys: &[&str]) -> Option<Value> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            return Some(value.clone());
        }
    }
    None
}

fn parse_expiry_number(value: i64) -> Option<String> {
    parse_timestamp_number(value).map(|dt| dt.to_rfc3339())
}

fn parse_expiry_str(value: &str) -> Option<String> {
    parse_timestamp(value).map(|dt| dt.to_rfc3339())
}

fn parse_expiry_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => parse_expiry_str(s),
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|v| v as i64))
            .and_then(parse_expiry_number),
        _ => None,
    }
}

fn tls_details_from_value(value: &Value) -> Option<TlsDetails> {
    match value {
        Value::Object(map) => {
            let details = tls_details_from_map(map);
            if tls_details_is_empty(&details) {
                None
            } else {
                Some(details)
            }
        }
        Value::Array(items) => {
            for item in items {
                if let Some(details) = tls_details_from_value(item) {
                    return Some(details);
                }
            }
            None
        }
        _ => None,
    }
}

fn tls_details_from_map(map: &Map<String, Value>) -> TlsDetails {
    let mut out = TlsDetails::default();
    out.version = get_string_value(map, &["protocol", "tlsVersion", "version"]);
    out.cipher_suite = get_string_value(map, &["cipher", "cipherSuite", "cipherSuiteName"]);
    out.cert_subject = get_string_value(
        map,
        &[
            "subjectName",
            "subject",
            "certificateSubject",
            "certSubject",
        ],
    );
    out.cert_issuer = get_string_value(
        map,
        &["issuer", "issuerName", "certificateIssuer", "certIssuer"],
    );
    if let Some(value) = get_value(
        map,
        &["validTo", "expiry", "expires", "notAfter", "expiresAt"],
    ) {
        out.cert_expiry = parse_expiry_value(&value);
    }

    if out.cert_subject.is_none() || out.cert_issuer.is_none() || out.cert_expiry.is_none() {
        if let Some(value) = get_value(map, &["certificate", "cert", "certificates"]) {
            if let Some(details) = tls_details_from_value(&value) {
                merge_tls_details(&mut out, details);
            }
        }
    }

    out
}

fn tls_details_from_extensions(extensions: &Map<String, Value>) -> Option<TlsDetails> {
    let mut out = tls_details_from_map(extensions);
    let candidates = [
        "securityDetails",
        "_securityDetails",
        "security",
        "_security",
        "tls",
        "_tls",
        "tlsDetails",
    ];

    for key in candidates {
        if let Some(value) = extensions.get(key) {
            if let Some(details) = tls_details_from_value(value) {
                merge_tls_details(&mut out, details);
            }
        }
    }

    if tls_details_is_empty(&out) {
        None
    } else {
        Some(out)
    }
}

fn extract_tls_details(entry: &Entry) -> TlsDetails {
    let mut out = TlsDetails::default();
    for extensions in [
        &entry.extensions,
        &entry.response.extensions,
        &entry.request.extensions,
        &entry.response.content.extensions,
    ] {
        if let Some(details) = tls_details_from_extensions(extensions) {
            merge_tls_details(&mut out, details);
        }
    }
    out
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

fn is_urlencoded_mime_type(mime: &str) -> bool {
    let media_type = mime.split(';').next().unwrap_or(mime).trim();
    media_type.eq_ignore_ascii_case("application/x-www-form-urlencoded")
}

fn synthesize_post_params(post_data: &crate::har::PostData) -> Option<(Vec<u8>, Option<String>)> {
    let params = post_data.params.as_ref()?;
    if params.is_empty() {
        return None;
    }

    if let Some(mime) = post_data.mime_type.as_deref() {
        if !is_urlencoded_mime_type(mime) {
            return None;
        }
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
    relations: &EntryRelations,
) -> Result<EntryInsertResult> {
    let entry_hash = entry_content_hash(entry);
    insert_entry_with_hash(
        conn,
        import_id,
        entry,
        options,
        relations,
        Some(&entry_hash),
        false,
    )
}

/// Insert an entry with an optional content hash (used for incremental imports).
pub fn insert_entry_with_hash(
    conn: &Connection,
    import_id: i64,
    entry: &Entry,
    options: &InsertEntryOptions,
    relations: &EntryRelations,
    entry_hash: Option<&str>,
    ignore_duplicates: bool,
) -> Result<EntryInsertResult> {
    let (host, path, query_string) = parse_url_parts(&entry.request.url);

    let request_headers_json = headers_to_json(&entry.request.headers);
    let response_headers_json = headers_to_json(&entry.response.headers);
    let request_cookies_json = cookies_to_json(&entry.request.cookies);
    let response_cookies_json = cookies_to_json(&entry.response.cookies);
    let entry_extensions_json = extensions_to_json(&entry.extensions);
    let request_extensions_json = extensions_to_json(&entry.request.extensions);
    let response_extensions_json = extensions_to_json(&entry.response.extensions);
    let content_extensions_json = extensions_to_json(&entry.response.content.extensions);
    let timings_extensions_json = entry
        .timings
        .as_ref()
        .and_then(|timings| extensions_to_json(&timings.extensions));
    let post_data_extensions_json = entry
        .request
        .post_data
        .as_ref()
        .and_then(|post| extensions_to_json(&post.extensions));
    let mut blocked_ms = None;
    let mut dns_ms = None;
    let mut connect_ms = None;
    let mut send_ms = None;
    let mut wait_ms = None;
    let mut receive_ms = None;
    let mut ssl_ms = None;
    if let Some(timings) = &entry.timings {
        let normalize = |value: f64| if value >= 0.0 { Some(value) } else { None };
        blocked_ms = timings.blocked.and_then(|v| normalize(v));
        dns_ms = timings.dns.and_then(|v| normalize(v));
        connect_ms = timings.connect.and_then(|v| normalize(v));
        send_ms = normalize(timings.send);
        wait_ms = normalize(timings.wait);
        receive_ms = normalize(timings.receive);
        ssl_ms = timings.ssl.and_then(|v| normalize(v));
    }
    let tls_details = extract_tls_details(entry);

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

    let mut entry_stats = EntryBlobStats::default();

    if options.store_bodies {
        if let Some(mut body) = decode_body(&entry.response.content) {
            let type_ok = !options.text_only || is_text_mime_type(response_mime);
            if type_ok && !body.is_empty() {
                if options.decompress_bodies {
                    if let Some(enc) = header_value(&entry.response.headers, "content-encoding") {
                        let decompress_limit = options
                            .max_body_size
                            .unwrap_or(DEFAULT_MAX_DECOMPRESSED_BYTES);
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
                                    entry_stats.response.record(is_new, body.len());
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
                    entry_stats.response.record(is_new, body.len());

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
                    entry_stats.request.record(is_new, body.len());
                }
            } else if let Some((body, mime)) = synthesize_post_params(post_data) {
                let size_ok = options.max_body_size.is_none_or(|max| body.len() <= max);
                let mime = mime.as_deref();
                let type_ok = !options.text_only || is_text_mime_type(mime);

                if size_ok && type_ok && !body.is_empty() {
                    let (hash, is_new) = store_request_blob(conn, &body, mime, options)?;
                    request_body_hash = Some(hash);
                    entry_stats.request.record(is_new, body.len());
                }
            }
        }
    }

    let graphql_info = extract_graphql_info(entry);
    let graphql_operation_type = graphql_info
        .as_ref()
        .and_then(|info| info.operation_type.clone());
    let graphql_operation_name = graphql_info
        .as_ref()
        .and_then(|info| info.operation_name.clone());
    let graphql_top_level_fields = graphql_info.as_ref().and_then(|info| {
        if info.top_level_fields.is_empty() {
            None
        } else {
            serde_json::to_string(&info.top_level_fields).ok()
        }
    });

    let insert_sql = if ignore_duplicates {
        "INSERT OR IGNORE INTO entries (
            import_id, page_id, started_at, time_ms, blocked_ms, dns_ms, connect_ms, send_ms, wait_ms, receive_ms, ssl_ms,
            method, url, host, path, query_string, http_version,
            request_headers, request_cookies, request_body_hash, request_body_size,
            status, status_text, response_headers, response_cookies,
            response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw, response_mime_type,
            is_redirect, server_ip, connection_id, request_id, parent_request_id, initiator_type, initiator_url, initiator_line, initiator_column, redirect_url,
            tls_version, tls_cipher_suite, tls_cert_subject, tls_cert_issuer, tls_cert_expiry, entry_hash,
            entry_extensions, request_extensions, response_extensions, content_extensions, timings_extensions, post_data_extensions,
            graphql_operation_type, graphql_operation_name, graphql_top_level_fields
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            ?41, ?42, ?43, ?44, ?45, ?46, ?47, ?48, ?49, ?50,
            ?51, ?52, ?53, ?54, ?55
        )"
    } else {
        "INSERT INTO entries (
            import_id, page_id, started_at, time_ms, blocked_ms, dns_ms, connect_ms, send_ms, wait_ms, receive_ms, ssl_ms,
            method, url, host, path, query_string, http_version,
            request_headers, request_cookies, request_body_hash, request_body_size,
            status, status_text, response_headers, response_cookies,
            response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw, response_mime_type,
            is_redirect, server_ip, connection_id, request_id, parent_request_id, initiator_type, initiator_url, initiator_line, initiator_column, redirect_url,
            tls_version, tls_cipher_suite, tls_cert_subject, tls_cert_issuer, tls_cert_expiry, entry_hash,
            entry_extensions, request_extensions, response_extensions, content_extensions, timings_extensions, post_data_extensions,
            graphql_operation_type, graphql_operation_name, graphql_top_level_fields
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            ?41, ?42, ?43, ?44, ?45, ?46, ?47, ?48, ?49, ?50,
            ?51, ?52, ?53, ?54, ?55
        )"
    };

    let inserted = conn.execute(
        insert_sql,
        params![
            import_id,
            entry.pageref,
            entry.started_date_time,
            entry.time,
            blocked_ms,
            dns_ms,
            connect_ms,
            send_ms,
            wait_ms,
            receive_ms,
            ssl_ms,
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
            relations.request_id.as_deref(),
            relations.parent_request_id.as_deref(),
            relations.initiator_type.as_deref(),
            relations.initiator_url.as_deref(),
            relations.initiator_line,
            relations.initiator_column,
            relations.redirect_url.as_deref(),
            tls_details.version,
            tls_details.cipher_suite,
            tls_details.cert_subject,
            tls_details.cert_issuer,
            tls_details.cert_expiry,
            entry_hash,
            entry_extensions_json,
            request_extensions_json,
            response_extensions_json,
            content_extensions_json,
            timings_extensions_json,
            post_data_extensions_json,
            graphql_operation_type,
            graphql_operation_name,
            graphql_top_level_fields,
        ],
    )?;

    if inserted > 0 {
        if let Some(info) = graphql_info {
            if !info.top_level_fields.is_empty() {
                let entry_id = conn.last_insert_rowid();
                let mut stmt = conn.prepare_cached(
                    "INSERT OR IGNORE INTO graphql_fields (entry_id, field) VALUES (?1, ?2)",
                )?;
                for field in info.top_level_fields {
                    stmt.execute(params![entry_id, field])?;
                }
            }
        }
    }

    Ok(EntryInsertResult {
        inserted: inserted > 0,
        blob_stats: entry_stats,
    })
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
    use super::{insert_entry, EntryRelations, InsertEntryOptions};
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

        let result =
            insert_entry(&conn, import_id, entry, &options, &EntryRelations::default())
                .expect("insert entry");
        assert!(result.inserted);
        assert_eq!(result.blob_stats.request.created, 0);
        assert_eq!(result.blob_stats.response.created, 1);

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
    fn inserts_graphql_metadata() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        create_schema(&conn).expect("schema created");

        let json = serde_json::json!({
            "log": {
                "entries": [
                    {
                        "startedDateTime": "2024-01-15T10:30:00.000Z",
                        "time": 12.0,
                        "request": {
                            "method": "POST",
                            "url": "https://example.com/graphql",
                            "httpVersion": "HTTP/1.1",
                            "headers": [{"name": "Content-Type", "value": "application/json"}],
                            "cookies": [],
                            "postData": {
                                "mimeType": "application/json",
                                "text": "{\"query\":\"query GetUser { viewer { login } }\",\"operationName\":\"GetUser\"}"
                            }
                        },
                        "response": {
                            "status": 200,
                            "statusText": "OK",
                            "httpVersion": "HTTP/1.1",
                            "headers": [],
                            "content": {
                                "size": 0,
                                "mimeType": "application/json"
                            }
                        }
                    }
                ]
            }
        });

        let har: Har = serde_json::from_value(json).expect("parse har");
        let entry = &har.log.entries[0];

        conn.execute(
            "INSERT INTO imports (id, source_file, imported_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
            params![1i64, "test.har", "2024-01-01T00:00:00Z", 0],
        )
        .expect("insert import");

        insert_entry(
            &conn,
            1,
            entry,
            &InsertEntryOptions::default(),
            &EntryRelations::default(),
        )
        .expect("insert entry");

        let (op_type, op_name, fields_json): (Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT graphql_operation_type, graphql_operation_name, graphql_top_level_fields FROM entries",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("load graphql fields");

        assert_eq!(op_type.as_deref(), Some("query"));
        assert_eq!(op_name.as_deref(), Some("GetUser"));
        let fields: Vec<String> = fields_json
            .and_then(|v| serde_json::from_str(&v).ok())
            .unwrap_or_default();
        assert_eq!(fields, vec!["viewer".to_string()]);

        let gql_fields: Vec<String> = conn
            .prepare("SELECT field FROM graphql_fields ORDER BY field")
            .expect("prepare")
            .query_map([], |r| r.get(0))
            .expect("query")
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(gql_fields, vec!["viewer".to_string()]);
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

        insert_entry(&conn, import_id, entry, &options, &EntryRelations::default())
            .expect("insert entry");

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
    fn params_only_request_body_skips_multipart() {
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
                  "url": "https://example.com/upload",
                  "httpVersion": "HTTP/1.1",
                  "headers": [{"name": "Content-Type", "value": "multipart/form-data"}],
                  "postData": {
                    "mimeType": "multipart/form-data",
                    "params": [
                      {"name": "file", "value": "ignored.bin"}
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

        insert_entry(&conn, import_id, entry, &options, &EntryRelations::default())
            .expect("insert entry");

        let request_body_hash: Option<String> = conn
            .query_row("SELECT request_body_hash FROM entries", [], |r| r.get(0))
            .expect("request body hash");

        assert!(request_body_hash.is_none());
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

        insert_entry(&conn, import_id, entry, &options, &EntryRelations::default())
            .expect("insert entry");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .expect("count blobs");
        assert_eq!(count, 0);

        let request_hash: Option<String> = conn
            .query_row("SELECT request_body_hash FROM entries", [], |r| r.get(0))
            .expect("request body hash");
        assert!(request_hash.is_none());
    }

    #[test]
    fn inserts_tls_metadata_from_extensions() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        create_schema(&conn).expect("schema created");

        let json = r#"
        {
          "log": {
            "entries": [
              {
                "startedDateTime": "2024-01-15T10:30:00.000Z",
                "time": 12.0,
                "_securityDetails": {
                  "protocol": "TLS 1.3",
                  "cipher": "TLS_AES_128_GCM_SHA256",
                  "subjectName": "example.com",
                  "issuer": "Example CA",
                  "validTo": 1704067200
                },
                "request": {
                  "method": "GET",
                  "url": "https://example.com/",
                  "httpVersion": "HTTP/2",
                  "headers": [],
                  "cookies": []
                },
                "response": {
                  "status": 200,
                  "statusText": "OK",
                  "httpVersion": "HTTP/2",
                  "headers": [],
                  "cookies": [],
                  "content": { "size": 0 }
                }
              }
            ]
          }
        }
        "#;

        let har: Har = serde_json::from_str(json).expect("parse har");
        let entry = &har.log.entries[0];

        let import_id = 1i64;
        conn.execute(
            "INSERT INTO imports (id, source_file, imported_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
            params![import_id, "test.har", "2024-01-01T00:00:00Z", 0],
        )
        .expect("insert import");

        insert_entry(
            &conn,
            import_id,
            entry,
            &InsertEntryOptions::default(),
            &EntryRelations::default(),
        )
            .expect("insert entry");

        let row: (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>) =
            conn.query_row(
                "SELECT tls_version, tls_cipher_suite, tls_cert_subject, tls_cert_issuer, tls_cert_expiry FROM entries",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .expect("tls row");

        assert_eq!(row.0.as_deref(), Some("TLS 1.3"));
        assert_eq!(row.1.as_deref(), Some("TLS_AES_128_GCM_SHA256"));
        assert_eq!(row.2.as_deref(), Some("example.com"));
        assert_eq!(row.3.as_deref(), Some("Example CA"));
        assert_eq!(row.4.as_deref(), Some("2024-01-01T00:00:00+00:00"));
    }
}
