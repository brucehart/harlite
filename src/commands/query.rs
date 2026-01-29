use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection, OpenFlags};

use crate::error::{HarliteError, Result};

use super::util::resolve_database;

#[derive(Clone, Copy, Debug, ValueEnum, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Table,
    Csv,
    Json,
}

impl OutputFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            OutputFormat::Table => "table",
            OutputFormat::Csv => "csv",
            OutputFormat::Json => "json",
        }
    }
}

pub struct QueryOptions {
    pub format: OutputFormat,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub quiet: bool,
}

pub fn open_readonly_connection(database: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    // Defense in depth: even on a read-only handle, enforce query-only mode.
    conn.execute_batch("PRAGMA query_only=ON;")?;
    Ok(conn)
}

pub fn run_query(sql: String, database: Option<PathBuf>, options: &QueryOptions) -> Result<()> {
    let database = resolve_database(database)?;
    let conn = open_readonly_connection(&database)?;
    execute_query(&conn, &sql, options)
}

pub fn execute_query(conn: &Connection, sql: &str, options: &QueryOptions) -> Result<()> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "SQL query cannot be empty".to_string(),
        ));
    }
    let base = normalize_single_statement(trimmed)?;

    let (sql, params) = wrap_query(&base, options.limit, options.offset);
    let mut stmt = conn.prepare(&sql)?;
    if !stmt.readonly() {
        return Err(HarliteError::InvalidArgs(
            "Only read-only queries are allowed".to_string(),
        ));
    }

    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    if columns.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "Query returned no columns".to_string(),
        ));
    }

    let mut rows = stmt.query(params_from_iter(params.iter()))?;

    match options.format {
        OutputFormat::Csv => write_csv(&columns, &mut rows),
        OutputFormat::Json => write_json(&columns, &mut rows),
        OutputFormat::Table => write_table(&columns, &mut rows, options.quiet),
    }
}

fn wrap_query(sql: &str, limit: Option<u64>, offset: Option<u64>) -> (String, Vec<Value>) {
    if limit.is_none() && offset.is_none() {
        return (sql.to_string(), Vec::new());
    }

    let mut out = format!("SELECT * FROM ({})", sql);
    let mut params: Vec<Value> = Vec::new();

    match (limit, offset) {
        (Some(lim), Some(off)) => {
            out.push_str(" LIMIT ?1 OFFSET ?2");
            params.push(Value::Integer(lim as i64));
            params.push(Value::Integer(off as i64));
        }
        (Some(lim), None) => {
            out.push_str(" LIMIT ?1");
            params.push(Value::Integer(lim as i64));
        }
        (None, Some(off)) => {
            out.push_str(" LIMIT -1 OFFSET ?1");
            params.push(Value::Integer(off as i64));
        }
        (None, None) => {}
    }

    (out, params)
}

fn normalize_single_statement(sql: &str) -> Result<String> {
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut last_was_star = false;

    let mut it = sql.chars().peekable();
    while let Some(ch) = it.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            if last_was_star && ch == '/' {
                in_block_comment = false;
            }
            last_was_star = ch == '*';
            continue;
        }

        if in_single {
            if ch == '\'' {
                if it.peek() == Some(&'\'') {
                    it.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }

        if in_double {
            if ch == '"' {
                if it.peek() == Some(&'"') {
                    it.next();
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '-' if it.peek() == Some(&'-') => {
                it.next();
                in_line_comment = true;
            }
            '/' if it.peek() == Some(&'*') => {
                it.next();
                in_block_comment = true;
                last_was_star = false;
            }
            ';' => {
                return Err(HarliteError::InvalidArgs(
                    "Only a single SQL statement is allowed".to_string(),
                ));
            }
            _ => {}
        }
    }

    Ok(sql.to_string())
}

fn write_csv(columns: &[String], rows: &mut rusqlite::Rows<'_>) -> Result<()> {
    let mut out = io::stdout().lock();
    write_csv_row(&mut out, columns.iter().map(|s| s.as_str()))?;

    while let Some(row) = rows.next()? {
        let values: Vec<String> = (0..columns.len())
            .map(|i| Ok::<_, rusqlite::Error>(value_to_csv(row.get_ref(i)?)))
            .collect::<std::result::Result<_, _>>()?;
        write_csv_row(&mut out, values.iter().map(|s| s.as_str()))?;
    }

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

fn write_json(columns: &[String], rows: &mut rusqlite::Rows<'_>) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(b"[")?;
    let mut first = true;
    while let Some(row) = rows.next()? {
        if !first {
            handle.write_all(b",")?;
        }
        first = false;
        let mut obj = serde_json::Map::with_capacity(columns.len());
        for (i, name) in columns.iter().enumerate() {
            let val = value_to_json(row.get_ref(i)?);
            obj.insert(name.clone(), val);
        }
        serde_json::to_writer(&mut handle, &serde_json::Value::Object(obj))?;
    }
    handle.write_all(b"]\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{normalize_single_statement, wrap_query};
    use crate::error::HarliteError;
    use rusqlite::types::Value;

    #[test]
    fn normalize_single_statement_allows_semicolons_in_strings() {
        let sql = "SELECT ';' AS semi, 'a'';''b' AS escaped";
        assert_eq!(normalize_single_statement(sql).unwrap(), sql);
    }

    #[test]
    fn normalize_single_statement_allows_semicolons_in_comments() {
        let sql = "SELECT 1 -- trailing; comment\n";
        assert_eq!(normalize_single_statement(sql).unwrap(), sql);

        let block = "SELECT 1 /* block; comment */";
        assert_eq!(normalize_single_statement(block).unwrap(), block);
    }

    #[test]
    fn normalize_single_statement_rejects_multiple_statements() {
        let err = normalize_single_statement("SELECT 1; SELECT 2").unwrap_err();
        match err {
            HarliteError::InvalidArgs(msg) => {
                assert!(msg.contains("single SQL statement"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn wrap_query_adds_limits() {
        let (sql, params) = wrap_query("SELECT * FROM entries", Some(10), Some(5));
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM entries) LIMIT ?1 OFFSET ?2"
        );
        assert_eq!(
            params,
            vec![Value::Integer(10), Value::Integer(5)]
        );
    }

    #[test]
    fn wrap_query_handles_offset_only() {
        let (sql, params) = wrap_query("SELECT * FROM entries", None, Some(5));
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM entries) LIMIT -1 OFFSET ?1"
        );
        assert_eq!(params, vec![Value::Integer(5)]);
    }
}

const TABLE_CELL_MAX_WIDTH: usize = 80;
const TABLE_CELL_MIN_WIDTH: usize = 32;

fn column_widths(columns: &[String]) -> Vec<usize> {
    columns
        .iter()
        .map(|c| {
            c.chars()
                .count()
                .max(TABLE_CELL_MIN_WIDTH)
                .min(TABLE_CELL_MAX_WIDTH)
        })
        .collect()
}

fn write_table(columns: &[String], rows: &mut rusqlite::Rows<'_>, quiet: bool) -> Result<()> {
    let widths = column_widths(columns);

    let mut out = io::stdout().lock();

    write_table_row(&mut out, columns.iter().map(|s| s.as_str()), &widths)?;
    if !quiet {
        write_table_sep(&mut out, &widths)?;
    }
    while let Some(row) = rows.next()? {
        let mut out_fields: Vec<String> = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let value = value_to_table(row.get_ref(i)?);
            out_fields.push(truncate(&value, widths[i]));
        }
        write_table_row(&mut out, out_fields.iter().map(|s| s.as_str()), &widths)?;
    }

    Ok(())
}

fn write_table_row<'a, I>(out: &mut impl Write, fields: I, widths: &[usize]) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut i = 0usize;
    for field in fields {
        if i > 0 {
            out.write_all(b" | ")?;
        }
        let width = widths.get(i).copied().unwrap_or(0);
        let field = truncate(field, width);
        out.write_all(field.as_bytes())?;
        let field_len = field.chars().count();
        if field_len < width {
            out.write_all(" ".repeat(width - field_len).as_bytes())?;
        }
        i += 1;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_table_sep(out: &mut impl Write, widths: &[usize]) -> Result<()> {
    for (i, w) in widths.iter().copied().enumerate() {
        if i > 0 {
            out.write_all(b"-+-")?;
        }
        out.write_all("-".repeat(w).as_bytes())?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn value_to_csv(v: ValueRef<'_>) -> String {
    match v {
        ValueRef::Null => "".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => f.to_string(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).to_string(),
        ValueRef::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

fn value_to_json(v: ValueRef<'_>) -> serde_json::Value {
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::Value::Number(i.into()),
        ValueRef::Real(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ValueRef::Text(t) => serde_json::Value::String(String::from_utf8_lossy(t).to_string()),
        ValueRef::Blob(b) => {
            use base64::{engine::general_purpose::STANDARD, Engine};
            serde_json::json!({
                "type": "blob",
                "bytes": b.len(),
                "base64": STANDARD.encode(b),
            })
        }
    }
}

fn value_to_table(v: ValueRef<'_>) -> String {
    const MAX_CELL: usize = 200;

    match v {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "NULL".to_string()
            }
        }
        ValueRef::Text(t) => sanitize_table_text(&String::from_utf8_lossy(t), MAX_CELL),
        ValueRef::Blob(b) => format!("<blob {} bytes>", b.len()),
    }
}

fn sanitize_table_text(s: &str, max: usize) -> String {
    let s = s
        .replace('\r', "\\r")
        .replace('\n', "\\n")
        .replace('\t', "\\t");
    truncate(&s, max)
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
