use std::io::{self, Write};
use std::path::PathBuf;

use rusqlite::types::Value;
use rusqlite::types::ValueRef;
use rusqlite::{params_from_iter, Connection, OpenFlags};

use crate::error::{HarliteError, Result};

use super::query::{OutputFormat, QueryOptions};
use super::util::resolve_database;

pub fn run_search(query: String, database: Option<PathBuf>, options: &QueryOptions) -> Result<()> {
    let query = query.trim();
    if query.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "Search query cannot be empty".to_string(),
        ));
    }

    let database = resolve_database(database)?;
    let conn = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.execute_batch("PRAGMA query_only=ON;")?;

    let has_fts: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='response_body_fts'",
        [],
        |r| r.get(0),
    )?;
    if has_fts == 0 {
        return Err(HarliteError::InvalidArgs(
            "FTS index not found (response_body_fts). Run `harlite fts-rebuild <db>` first."
                .to_string(),
        ));
    }

    let (sql, params) = build_search_sql(query, options.limit, options.offset);
    let mut stmt = conn.prepare(&sql)?;
    if !stmt.readonly() {
        return Err(HarliteError::InvalidArgs(
            "Only read-only queries are allowed".to_string(),
        ));
    }

    let mut rows = stmt.query(params_from_iter(params.iter()))?;

    let columns = ["rank", "started_at", "status", "url", "snippet"];
    match options.format {
        OutputFormat::Csv => write_csv(&columns, &mut rows),
        OutputFormat::Json => write_json(&columns, &mut rows),
        OutputFormat::Table => write_table(&columns, &mut rows, options.quiet),
    }
}

fn build_search_sql(query: &str, limit: Option<u64>, offset: Option<u64>) -> (String, Vec<Value>) {
    let mut sql = r#"
        SELECT
            bm25(response_body_fts) AS rank,
            e.started_at,
            e.status,
            e.url,
            snippet(response_body_fts, 1, '[', ']', '…', 12) AS snippet
        FROM response_body_fts
        JOIN entries e ON e.response_body_hash = response_body_fts.hash
        WHERE response_body_fts MATCH ?1
        ORDER BY rank, e.started_at, e.id
    "#
    .to_string();

    let mut params: Vec<Value> = vec![Value::Text(query.to_string())];
    match (limit, offset) {
        (Some(lim), Some(off)) => {
            sql.push_str(" LIMIT ?2 OFFSET ?3");
            params.push(Value::Integer(lim as i64));
            params.push(Value::Integer(off as i64));
        }
        (Some(lim), None) => {
            sql.push_str(" LIMIT ?2");
            params.push(Value::Integer(lim as i64));
        }
        (None, Some(off)) => {
            sql.push_str(" LIMIT -1 OFFSET ?2");
            params.push(Value::Integer(off as i64));
        }
        (None, None) => {}
    }

    (sql, params)
}

fn value_to_table(v: ValueRef<'_>) -> String {
    match v {
        ValueRef::Null => "".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => format!("{f:.4}"),
        ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
        ValueRef::Blob(b) => format!("<blob:{}>", b.len()),
    }
}

fn write_table_row<'a, I>(out: &mut impl Write, fields: I, widths: &[usize]) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut i = 0usize;
    for field in fields {
        if i > 0 {
            out.write_all(b"  ")?;
        }
        let width = widths.get(i).copied().unwrap_or(0);
        write!(out, "{:width$}", field, width = width)?;
        i += 1;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_table_sep(out: &mut impl Write, widths: &[usize]) -> Result<()> {
    let mut first = true;
    for w in widths {
        if !first {
            out.write_all(b"  ")?;
        }
        first = false;
        out.write_all(&vec![b'-'; *w])?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_table(columns: &[&str], rows: &mut rusqlite::Rows<'_>, quiet: bool) -> Result<()> {
    const TABLE_CELL_MAX_WIDTH: usize = 200;
    const TABLE_CELL_MIN_WIDTH: usize = 32;
    let widths: Vec<usize> = columns
        .iter()
        .map(|c| {
            let base = c.chars().count().max(TABLE_CELL_MIN_WIDTH);
            if *c == "snippet" {
                TABLE_CELL_MAX_WIDTH
            } else {
                base.min(TABLE_CELL_MAX_WIDTH)
            }
        })
        .collect();

    let mut out = io::stdout().lock();
    write_table_row(&mut out, columns.iter().copied(), &widths)?;
    if !quiet {
        write_table_sep(&mut out, &widths)?;
    }
    while let Some(row) = rows.next()? {
        let mut fields: Vec<String> = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let value = value_to_table(row.get_ref(i)?);
            fields.push(truncate(&value, widths[i]));
        }
        write_table_row(&mut out, fields.iter().map(|s| s.as_str()), &widths)?;
    }
    Ok(())
}

fn write_csv_field(out: &mut impl Write, field: &str) -> Result<()> {
    let needs_quotes = field.contains([',', '"', '\n', '\r']);
    if !needs_quotes {
        out.write_all(field.as_bytes())?;
        return Ok(());
    }

    out.write_all(b"\"")?;
    for b in field.as_bytes() {
        if *b == b'"' {
            out.write_all(b"\"\"")?;
        } else {
            out.write_all(&[*b])?;
        }
    }
    out.write_all(b"\"")?;
    Ok(())
}

fn write_csv_row<'a, I>(out: &mut impl Write, fields: I) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut first = true;
    for field in fields {
        if !first {
            out.write_all(b",")?;
        }
        first = false;
        write_csv_field(out, field)?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_csv(columns: &[&str], rows: &mut rusqlite::Rows<'_>) -> Result<()> {
    let mut out = io::stdout().lock();
    write_csv_row(&mut out, columns.iter().copied())?;

    while let Some(row) = rows.next()? {
        let values: Vec<String> = (0..columns.len())
            .map(|i| Ok::<_, rusqlite::Error>(value_to_table(row.get_ref(i)?)))
            .collect::<std::result::Result<_, _>>()?;
        let fields: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        write_csv_row(&mut out, fields)?;
    }

    Ok(())
}

fn value_to_json(v: ValueRef<'_>) -> serde_json::Value {
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::Value::Number(i.into()),
        ValueRef::Real(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        ValueRef::Text(t) => serde_json::Value::String(String::from_utf8_lossy(t).to_string()),
        ValueRef::Blob(b) => serde_json::Value::String(format!("<blob:{}>", b.len())),
    }
}

fn write_json(columns: &[&str], rows: &mut rusqlite::Rows<'_>) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(b"[")?;
    let mut first = true;
    while let Some(row) = rows.next()? {
        let mut obj = serde_json::Map::with_capacity(columns.len());
        for (i, name) in columns.iter().enumerate() {
            obj.insert((*name).to_string(), value_to_json(row.get_ref(i)?));
        }
        if !first {
            handle.write_all(b",")?;
        }
        first = false;
        serde_json::to_writer(&mut handle, &serde_json::Value::Object(obj))?;
    }
    handle.write_all(b"]\n")?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    if max <= 3 {
        return "...".to_string();
    }
    let mut end = 0usize;
    for (i, ch) in s.char_indices() {
        if i >= max - 3 {
            break;
        }
        end = i + ch.len_utf8();
    }
    let mut out = s[..end].to_string();
    out.push_str("...");
    out
}
