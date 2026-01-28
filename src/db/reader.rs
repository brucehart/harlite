use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection};

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct EntryRow {
    pub import_id: i64,
    pub page_id: Option<String>,
    pub started_at: Option<String>,
    pub time_ms: Option<f64>,
    pub method: Option<String>,
    pub url: Option<String>,
    pub http_version: Option<String>,
    pub request_headers: Option<String>,
    pub request_cookies: Option<String>,
    pub request_body_hash: Option<String>,
    pub request_body_size: Option<i64>,
    pub status: Option<i32>,
    pub status_text: Option<String>,
    pub response_headers: Option<String>,
    pub response_cookies: Option<String>,
    pub response_body_hash: Option<String>,
    pub response_body_size: Option<i64>,
    pub response_body_hash_raw: Option<String>,
    pub response_body_size_raw: Option<i64>,
    pub response_mime_type: Option<String>,
    pub server_ip: Option<String>,
    pub connection_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct EntryQuery {
    pub import_ids: Vec<i64>,
    pub from_started_at: Option<String>,
    pub to_started_at: Option<String>,
    pub url_exact: Vec<String>,
    pub url_contains: Vec<String>,
    pub hosts: Vec<String>,
    pub methods: Vec<String>,
    pub statuses: Vec<i32>,
    pub mime_contains: Vec<String>,
    pub min_request_size: Option<i64>,
    pub max_request_size: Option<i64>,
    pub min_response_size: Option<i64>,
    pub max_response_size: Option<i64>,
}

fn push_in_clause(
    clauses: &mut Vec<String>,
    params: &mut Vec<Value>,
    column: &str,
    values: &[String],
) {
    if values.is_empty() {
        return;
    }
    let placeholders = std::iter::repeat("?")
        .take(values.len())
        .collect::<Vec<_>>()
        .join(", ");
    clauses.push(format!("{column} IN ({placeholders})"));
    for v in values {
        params.push(Value::Text(v.clone()));
    }
}

fn push_in_clause_i32(
    clauses: &mut Vec<String>,
    params: &mut Vec<Value>,
    column: &str,
    values: &[i32],
) {
    if values.is_empty() {
        return;
    }
    let placeholders = std::iter::repeat("?")
        .take(values.len())
        .collect::<Vec<_>>()
        .join(", ");
    clauses.push(format!("{column} IN ({placeholders})"));
    for v in values {
        params.push(Value::Integer(i64::from(*v)));
    }
}

fn push_in_clause_i64(
    clauses: &mut Vec<String>,
    params: &mut Vec<Value>,
    column: &str,
    values: &[i64],
) {
    if values.is_empty() {
        return;
    }
    let placeholders = std::iter::repeat("?")
        .take(values.len())
        .collect::<Vec<_>>()
        .join(", ");
    clauses.push(format!("{column} IN ({placeholders})"));
    for v in values {
        params.push(Value::Integer(*v));
    }
}

fn push_like_any(
    clauses: &mut Vec<String>,
    params: &mut Vec<Value>,
    predicate: &str,
    needles: &[String],
) {
    if needles.is_empty() {
        return;
    }

    let joined = std::iter::repeat(predicate)
        .take(needles.len())
        .collect::<Vec<_>>()
        .join(" OR ");
    clauses.push(format!("({joined})"));
    for needle in needles {
        params.push(Value::Text(needle.clone()));
    }
}

pub fn load_entries(conn: &Connection, query: &EntryQuery) -> Result<Vec<EntryRow>> {
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();

    push_in_clause_i64(&mut clauses, &mut params, "import_id", &query.import_ids);

    if let Some(from) = &query.from_started_at {
        clauses.push("started_at >= ?".to_string());
        params.push(Value::Text(from.clone()));
    }
    if let Some(to) = &query.to_started_at {
        clauses.push("started_at <= ?".to_string());
        params.push(Value::Text(to.clone()));
    }

    push_in_clause(&mut clauses, &mut params, "url", &query.url_exact);
    push_in_clause(&mut clauses, &mut params, "host", &query.hosts);
    push_in_clause(&mut clauses, &mut params, "method", &query.methods);
    push_in_clause_i32(&mut clauses, &mut params, "status", &query.statuses);
    push_like_any(
        &mut clauses,
        &mut params,
        "url LIKE '%' || ? || '%'",
        &query.url_contains,
    );
    push_like_any(
        &mut clauses,
        &mut params,
        "LOWER(response_mime_type) LIKE '%' || LOWER(?) || '%'",
        &query.mime_contains,
    );

    if let Some(min) = query.min_request_size {
        clauses.push("COALESCE(request_body_size, 0) >= ?".to_string());
        params.push(Value::Integer(min));
    }
    if let Some(max) = query.max_request_size {
        clauses.push("COALESCE(request_body_size, 0) <= ?".to_string());
        params.push(Value::Integer(max));
    }
    if let Some(min) = query.min_response_size {
        clauses.push("COALESCE(response_body_size, 0) >= ?".to_string());
        params.push(Value::Integer(min));
    }
    if let Some(max) = query.max_response_size {
        clauses.push("COALESCE(response_body_size, 0) <= ?".to_string());
        params.push(Value::Integer(max));
    }

    let mut sql = r#"
        SELECT
            import_id,
            page_id,
            started_at,
            time_ms,
            method,
            url,
            http_version,
            request_headers,
            request_cookies,
            request_body_hash,
            request_body_size,
            status,
            status_text,
            response_headers,
            response_cookies,
            response_body_hash,
            response_body_size,
            response_body_hash_raw,
            response_body_size_raw,
            response_mime_type,
            server_ip,
            connection_id
        FROM entries
    "#
    .to_string();

    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
    sql.push_str(" ORDER BY started_at, id");

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
        Ok(EntryRow {
            import_id: row.get(0)?,
            page_id: row.get(1)?,
            started_at: row.get(2)?,
            time_ms: row.get(3)?,
            method: row.get(4)?,
            url: row.get(5)?,
            http_version: row.get(6)?,
            request_headers: row.get(7)?,
            request_cookies: row.get(8)?,
            request_body_hash: row.get(9)?,
            request_body_size: row.get(10)?,
            status: row.get(11)?,
            status_text: row.get(12)?,
            response_headers: row.get(13)?,
            response_cookies: row.get(14)?,
            response_body_hash: row.get(15)?,
            response_body_size: row.get(16)?,
            response_body_hash_raw: row.get(17)?,
            response_body_size_raw: row.get(18)?,
            response_mime_type: row.get(19)?,
            server_ip: row.get(20)?,
            connection_id: row.get(21)?,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

#[derive(Debug, Clone)]
pub struct PageRow {
    pub import_id: i64,
    pub id: String,
    pub started_at: Option<String>,
    pub title: Option<String>,
    pub on_content_load_ms: Option<f64>,
    pub on_load_ms: Option<f64>,
}

pub fn load_pages_for_imports(conn: &Connection, import_ids: &[i64]) -> Result<Vec<PageRow>> {
    if import_ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat("?")
        .take(import_ids.len())
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT import_id, id, started_at, title, on_content_load_ms, on_load_ms FROM pages WHERE import_id IN ({placeholders})"
    );

    let params: Vec<Value> = import_ids
        .iter()
        .copied()
        .map(|id| Value::Integer(id))
        .collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
        Ok(PageRow {
            import_id: row.get(0)?,
            id: row.get(1)?,
            started_at: row.get(2)?,
            title: row.get(3)?,
            on_content_load_ms: row.get(4)?,
            on_load_ms: row.get(5)?,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

#[derive(Debug, Clone)]
pub struct BlobRow {
    pub hash: String,
    pub content: Vec<u8>,
    pub size: i64,
    pub mime_type: Option<String>,
    pub external_path: Option<String>,
}

pub fn load_blobs_by_hashes(conn: &Connection, hashes: &[String]) -> Result<Vec<BlobRow>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }

    let has_external_path = {
        let mut stmt = conn.prepare("PRAGMA table_info(blobs)")?;
        let cols: Vec<String> = stmt
            .query_map([], |row| row.get(1))?
            .filter_map(|r| r.ok())
            .collect();
        cols.iter().any(|c| c == "external_path")
    };

    let mut out: Vec<BlobRow> = Vec::new();
    // SQLite defaults to 999 parameters; stay under that.
    const CHUNK: usize = 900;

    for chunk in hashes.chunks(CHUNK) {
        let placeholders = std::iter::repeat("?")
            .take(chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let params: Vec<Value> = chunk.iter().map(|h| Value::Text(h.clone())).collect();

        if has_external_path {
            let sql = format!(
                "SELECT hash, content, size, mime_type, external_path FROM blobs WHERE hash IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
                Ok(BlobRow {
                    hash: row.get(0)?,
                    content: row.get(1)?,
                    size: row.get(2)?,
                    mime_type: row.get(3)?,
                    external_path: row.get(4)?,
                })
            })?;
            out.extend(rows.filter_map(|r| r.ok()));
        } else {
            let sql = format!(
                "SELECT hash, content, size, mime_type FROM blobs WHERE hash IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
                Ok(BlobRow {
                    hash: row.get(0)?,
                    content: row.get(1)?,
                    size: row.get(2)?,
                    mime_type: row.get(3)?,
                    external_path: None,
                })
            })?;
            out.extend(rows.filter_map(|r| r.ok()));
        }
    }

    Ok(out)
}
