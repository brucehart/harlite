use rusqlite::{params, Connection};
use url::Url;

use crate::error::Result;
use crate::har::{Cookie, Entry, Header, Page};

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
) -> Result<(String, bool)> {
    let hash = blake3::hash(content).to_hex().to_string();

    let inserted = conn.execute(
        "INSERT OR IGNORE INTO blobs (hash, content, size, mime_type) VALUES (?1, ?2, ?3, ?4)",
        params![hash, content, content.len() as i64, mime_type],
    )?;

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
}

impl Default for InsertEntryOptions {
    fn default() -> Self {
        Self {
            store_bodies: false,
            max_body_size: Some(100 * 1024),
            text_only: false,
        }
    }
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
        }
    }
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
    let response_body_size = if entry.response.content.size >= 0 {
        Some(entry.response.content.size)
    } else {
        None
    };
    let response_mime = entry.response.content.mime_type.as_deref();

    let mut blob_created = false;
    let mut bytes_accounted = 0usize;

    if options.store_bodies {
        if let Some(body) = decode_body(&entry.response.content) {
            let size_ok = options.max_body_size.is_none_or(|max| body.len() <= max);
            let type_ok = !options.text_only || is_text_mime_type(response_mime);

            if size_ok && type_ok && !body.is_empty() {
                let (hash, is_new) = store_blob(conn, &body, response_mime)?;
                response_body_hash = Some(hash);
                blob_created = is_new;
                bytes_accounted = body.len();
            }
        }

        if let Some(post_data) = &entry.request.post_data {
            if let Some(text) = &post_data.text {
                let body = text.as_bytes();
                let size_ok = options.max_body_size.is_none_or(|max| body.len() <= max);
                let mime = post_data.mime_type.as_deref();
                let type_ok = !options.text_only || is_text_mime_type(mime);

                if size_ok && type_ok && !body.is_empty() {
                    let (hash, is_new) = store_blob(conn, body, mime)?;
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
            response_body_hash, response_body_size, response_mime_type,
            is_redirect, server_ip, connection_id
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24
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
            response_mime,
            is_redirect,
            entry.server_ip_address,
            entry.connection,
        ],
    )?;

    Ok((blob_created, bytes_accounted))
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
}
