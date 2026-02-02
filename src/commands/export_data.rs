use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use rusqlite::Connection;

use crate::db::{ensure_schema_upgrades, EntryRow};
use crate::error::{HarliteError, Result};

use super::entry_filter::{load_entries_with_filters, EntryFilterOptions};

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum DataExportFormat {
    Csv,
    Jsonl,
    Parquet,
}

impl DataExportFormat {
    fn extension(self) -> &'static str {
        match self {
            DataExportFormat::Csv => "csv",
            DataExportFormat::Jsonl => "jsonl",
            DataExportFormat::Parquet => "parquet",
        }
    }
}

pub struct ExportDataOptions {
    pub output: Option<PathBuf>,
    pub format: DataExportFormat,
    pub filters: EntryFilterOptions,
}

pub fn run_export_data(database: PathBuf, options: &ExportDataOptions) -> Result<()> {
    let conn = Connection::open(&database)?;
    ensure_schema_upgrades(&conn)?;

    let entries = load_entries_with_filters(&conn, &options.filters)?;

    let output_path = match &options.output {
        Some(p) => p.clone(),
        None => {
            let stem = database
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("export");
            PathBuf::from(format!("{}.{}", stem, options.format.extension()))
        }
    };

    if matches!(options.format, DataExportFormat::Parquet) && output_path == PathBuf::from("-") {
        return Err(HarliteError::InvalidArgs(
            "Parquet export requires a file path; '-' is not supported".to_string(),
        ));
    }

    match options.format {
        DataExportFormat::Csv => {
            let mut writer = open_output(&output_path)?;
            write_csv(&mut writer, &entries)?;
        }
        DataExportFormat::Jsonl => {
            let mut writer = open_output(&output_path)?;
            write_jsonl(&mut writer, &entries)?;
        }
        DataExportFormat::Parquet => write_parquet(&output_path, &entries)?,
    }

    if output_path != PathBuf::from("-") {
        println!(
            "Exported {} entries to {}",
            entries.len(),
            output_path.display()
        );
    }

    Ok(())
}

fn open_output(path: &Path) -> Result<Box<dyn Write>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout().lock()));
    }
    Ok(Box::new(BufWriter::new(File::create(path)?)))
}

const ENTRY_COLUMNS: &[&str] = &[
    "import_id",
    "page_id",
    "started_at",
    "time_ms",
    "blocked_ms",
    "dns_ms",
    "connect_ms",
    "send_ms",
    "wait_ms",
    "receive_ms",
    "ssl_ms",
    "method",
    "url",
    "host",
    "path",
    "query_string",
    "http_version",
    "request_headers",
    "request_cookies",
    "request_body_hash",
    "request_body_size",
    "status",
    "status_text",
    "response_headers",
    "response_cookies",
    "response_body_hash",
    "response_body_size",
    "response_body_hash_raw",
    "response_body_size_raw",
    "response_mime_type",
    "is_redirect",
    "server_ip",
    "connection_id",
    "request_id",
    "parent_request_id",
    "initiator_type",
    "initiator_url",
    "initiator_line",
    "initiator_column",
    "redirect_url",
    "tls_version",
    "tls_cipher_suite",
    "tls_cert_subject",
    "tls_cert_issuer",
    "tls_cert_expiry",
    "entry_hash",
    "entry_extensions",
    "request_extensions",
    "response_extensions",
    "content_extensions",
    "timings_extensions",
    "post_data_extensions",
];

fn write_csv(out: &mut impl Write, entries: &[EntryRow]) -> Result<()> {
    write_csv_row(out, ENTRY_COLUMNS.iter().copied())?;
    for entry in entries {
        let row = entry_to_csv_row(entry);
        write_csv_row(out, row.iter().map(|s| s.as_str()))?;
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

fn entry_to_csv_row(entry: &EntryRow) -> Vec<String> {
    vec![
        entry.import_id.to_string(),
        opt_string(&entry.page_id),
        opt_string(&entry.started_at),
        opt_f64(entry.time_ms),
        opt_f64(entry.blocked_ms),
        opt_f64(entry.dns_ms),
        opt_f64(entry.connect_ms),
        opt_f64(entry.send_ms),
        opt_f64(entry.wait_ms),
        opt_f64(entry.receive_ms),
        opt_f64(entry.ssl_ms),
        opt_string(&entry.method),
        opt_string(&entry.url),
        opt_string(&entry.host),
        opt_string(&entry.path),
        opt_string(&entry.query_string),
        opt_string(&entry.http_version),
        opt_string(&entry.request_headers),
        opt_string(&entry.request_cookies),
        opt_string(&entry.request_body_hash),
        opt_i64(entry.request_body_size),
        opt_i32(entry.status),
        opt_string(&entry.status_text),
        opt_string(&entry.response_headers),
        opt_string(&entry.response_cookies),
        opt_string(&entry.response_body_hash),
        opt_i64(entry.response_body_size),
        opt_string(&entry.response_body_hash_raw),
        opt_i64(entry.response_body_size_raw),
        opt_string(&entry.response_mime_type),
        opt_i32(entry.is_redirect),
        opt_string(&entry.server_ip),
        opt_string(&entry.connection_id),
        opt_string(&entry.request_id),
        opt_string(&entry.parent_request_id),
        opt_string(&entry.initiator_type),
        opt_string(&entry.initiator_url),
        opt_i64(entry.initiator_line),
        opt_i64(entry.initiator_column),
        opt_string(&entry.redirect_url),
        opt_string(&entry.tls_version),
        opt_string(&entry.tls_cipher_suite),
        opt_string(&entry.tls_cert_subject),
        opt_string(&entry.tls_cert_issuer),
        opt_string(&entry.tls_cert_expiry),
        opt_string(&entry.entry_hash),
        opt_string(&entry.entry_extensions),
        opt_string(&entry.request_extensions),
        opt_string(&entry.response_extensions),
        opt_string(&entry.content_extensions),
        opt_string(&entry.timings_extensions),
        opt_string(&entry.post_data_extensions),
    ]
}

fn opt_string(value: &Option<String>) -> String {
    value.clone().unwrap_or_default()
}

fn opt_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_i32(value: Option<i32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_f64(value: Option<f64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn write_jsonl(out: &mut impl Write, entries: &[EntryRow]) -> Result<()> {
    for entry in entries {
        let record = EntryExportRecord::from(entry);
        serde_json::to_writer(&mut *out, &record)?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct EntryExportRecord {
    import_id: i64,
    page_id: Option<String>,
    started_at: Option<String>,
    time_ms: Option<f64>,
    blocked_ms: Option<f64>,
    dns_ms: Option<f64>,
    connect_ms: Option<f64>,
    send_ms: Option<f64>,
    wait_ms: Option<f64>,
    receive_ms: Option<f64>,
    ssl_ms: Option<f64>,
    method: Option<String>,
    url: Option<String>,
    host: Option<String>,
    path: Option<String>,
    query_string: Option<String>,
    http_version: Option<String>,
    request_headers: Option<String>,
    request_cookies: Option<String>,
    request_body_hash: Option<String>,
    request_body_size: Option<i64>,
    status: Option<i32>,
    status_text: Option<String>,
    response_headers: Option<String>,
    response_cookies: Option<String>,
    response_body_hash: Option<String>,
    response_body_size: Option<i64>,
    response_body_hash_raw: Option<String>,
    response_body_size_raw: Option<i64>,
    response_mime_type: Option<String>,
    is_redirect: Option<i32>,
    server_ip: Option<String>,
    connection_id: Option<String>,
    request_id: Option<String>,
    parent_request_id: Option<String>,
    initiator_type: Option<String>,
    initiator_url: Option<String>,
    initiator_line: Option<i64>,
    initiator_column: Option<i64>,
    redirect_url: Option<String>,
    tls_version: Option<String>,
    tls_cipher_suite: Option<String>,
    tls_cert_subject: Option<String>,
    tls_cert_issuer: Option<String>,
    tls_cert_expiry: Option<String>,
    entry_hash: Option<String>,
    entry_extensions: Option<String>,
    request_extensions: Option<String>,
    response_extensions: Option<String>,
    content_extensions: Option<String>,
    timings_extensions: Option<String>,
    post_data_extensions: Option<String>,
}

impl From<&EntryRow> for EntryExportRecord {
    fn from(entry: &EntryRow) -> Self {
        Self {
            import_id: entry.import_id,
            page_id: entry.page_id.clone(),
            started_at: entry.started_at.clone(),
            time_ms: entry.time_ms,
            blocked_ms: entry.blocked_ms,
            dns_ms: entry.dns_ms,
            connect_ms: entry.connect_ms,
            send_ms: entry.send_ms,
            wait_ms: entry.wait_ms,
            receive_ms: entry.receive_ms,
            ssl_ms: entry.ssl_ms,
            method: entry.method.clone(),
            url: entry.url.clone(),
            host: entry.host.clone(),
            path: entry.path.clone(),
            query_string: entry.query_string.clone(),
            http_version: entry.http_version.clone(),
            request_headers: entry.request_headers.clone(),
            request_cookies: entry.request_cookies.clone(),
            request_body_hash: entry.request_body_hash.clone(),
            request_body_size: entry.request_body_size,
            status: entry.status,
            status_text: entry.status_text.clone(),
            response_headers: entry.response_headers.clone(),
            response_cookies: entry.response_cookies.clone(),
            response_body_hash: entry.response_body_hash.clone(),
            response_body_size: entry.response_body_size,
            response_body_hash_raw: entry.response_body_hash_raw.clone(),
            response_body_size_raw: entry.response_body_size_raw,
            response_mime_type: entry.response_mime_type.clone(),
            is_redirect: entry.is_redirect,
            server_ip: entry.server_ip.clone(),
            connection_id: entry.connection_id.clone(),
            request_id: entry.request_id.clone(),
            parent_request_id: entry.parent_request_id.clone(),
            initiator_type: entry.initiator_type.clone(),
            initiator_url: entry.initiator_url.clone(),
            initiator_line: entry.initiator_line,
            initiator_column: entry.initiator_column,
            redirect_url: entry.redirect_url.clone(),
            tls_version: entry.tls_version.clone(),
            tls_cipher_suite: entry.tls_cipher_suite.clone(),
            tls_cert_subject: entry.tls_cert_subject.clone(),
            tls_cert_issuer: entry.tls_cert_issuer.clone(),
            tls_cert_expiry: entry.tls_cert_expiry.clone(),
            entry_hash: entry.entry_hash.clone(),
            entry_extensions: entry.entry_extensions.clone(),
            request_extensions: entry.request_extensions.clone(),
            response_extensions: entry.response_extensions.clone(),
            content_extensions: entry.content_extensions.clone(),
            timings_extensions: entry.timings_extensions.clone(),
            post_data_extensions: entry.post_data_extensions.clone(),
        }
    }
}

#[cfg(feature = "parquet")]
fn write_parquet(path: &Path, entries: &[EntryRow]) -> Result<()> {
    use parquet::basic::{Repetition, Type as PhysicalType};
    use parquet::column::writer::ColumnWriter;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;
    use std::sync::Arc;

    let schema = Type::group_type_builder("schema")
        .with_fields(vec![
            Type::primitive_type_builder("import_id", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()?,
            Type::primitive_type_builder("page_id", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("started_at", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("time_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("blocked_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("dns_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("connect_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("send_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("wait_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("receive_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("ssl_ms", PhysicalType::DOUBLE)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("method", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("host", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("path", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("query_string", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("http_version", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("request_headers", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("request_cookies", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("request_body_hash", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("request_body_size", PhysicalType::INT64)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("status", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("status_text", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_headers", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_cookies", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_body_hash", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_body_size", PhysicalType::INT64)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_body_hash_raw", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_body_size_raw", PhysicalType::INT64)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_mime_type", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("is_redirect", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("server_ip", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("connection_id", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("request_id", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("parent_request_id", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("initiator_type", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("initiator_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("initiator_line", PhysicalType::INT64)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("initiator_column", PhysicalType::INT64)
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("redirect_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("tls_version", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("tls_cipher_suite", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("tls_cert_subject", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("tls_cert_issuer", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("tls_cert_expiry", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("entry_hash", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("entry_extensions", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("request_extensions", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("response_extensions", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("content_extensions", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("timings_extensions", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
            Type::primitive_type_builder("post_data_extensions", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()?,
        ])
        .build()?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, Arc::new(schema), Arc::new(props))?;

    let mut row_group_writer = writer.next_row_group()?;

    let mut write_string_col = |values: Vec<Option<String>>| -> Result<()> {
        let def_levels: Vec<i16> = values.iter().map(|v| if v.is_some() { 1 } else { 0 }).collect();
        let data: Vec<ByteArray> = values
            .into_iter()
            .filter_map(|v| v.map(|s| ByteArray::from(s.into_bytes())))
            .collect();
        if let Some(col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(mut w) => {
                    w.write_batch(&data, Some(&def_levels), None)?;
                }
                _ => {
                    return Err(HarliteError::InvalidArgs(
                        "Unexpected Parquet column type".to_string(),
                    ))
                }
            }
        }
        Ok(())
    };

    let mut write_i64_col = |values: Vec<Option<i64>>| -> Result<()> {
        let def_levels: Vec<i16> = values.iter().map(|v| if v.is_some() { 1 } else { 0 }).collect();
        let data: Vec<i64> = values.into_iter().filter_map(|v| v).collect();
        if let Some(col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::Int64ColumnWriter(mut w) => {
                    w.write_batch(&data, Some(&def_levels), None)?;
                }
                _ => {
                    return Err(HarliteError::InvalidArgs(
                        "Unexpected Parquet column type".to_string(),
                    ))
                }
            }
        }
        Ok(())
    };

    let mut write_i32_col = |values: Vec<Option<i32>>| -> Result<()> {
        let def_levels: Vec<i16> = values.iter().map(|v| if v.is_some() { 1 } else { 0 }).collect();
        let data: Vec<i32> = values.into_iter().filter_map(|v| v).collect();
        if let Some(col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::Int32ColumnWriter(mut w) => {
                    w.write_batch(&data, Some(&def_levels), None)?;
                }
                _ => {
                    return Err(HarliteError::InvalidArgs(
                        "Unexpected Parquet column type".to_string(),
                    ))
                }
            }
        }
        Ok(())
    };

    let mut write_f64_col = |values: Vec<Option<f64>>| -> Result<()> {
        let def_levels: Vec<i16> = values.iter().map(|v| if v.is_some() { 1 } else { 0 }).collect();
        let data: Vec<f64> = values.into_iter().filter_map(|v| v).collect();
        if let Some(col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::DoubleColumnWriter(mut w) => {
                    w.write_batch(&data, Some(&def_levels), None)?;
                }
                _ => {
                    return Err(HarliteError::InvalidArgs(
                        "Unexpected Parquet column type".to_string(),
                    ))
                }
            }
        }
        Ok(())
    };

    let import_ids = entries.iter().map(|e| e.import_id).collect::<Vec<_>>();
    let import_def = vec![1; import_ids.len()];
    if let Some(col_writer) = row_group_writer.next_column()? {
        match col_writer {
            ColumnWriter::Int64ColumnWriter(mut w) => {
                w.write_batch(&import_ids, Some(&import_def), None)?;
            }
            _ => {
                return Err(HarliteError::InvalidArgs(
                    "Unexpected Parquet column type".to_string(),
                ))
            }
        }
    }

    write_string_col(entries.iter().map(|e| e.page_id.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.started_at.clone()).collect())?;
    write_f64_col(entries.iter().map(|e| e.time_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.blocked_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.dns_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.connect_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.send_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.wait_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.receive_ms).collect())?;
    write_f64_col(entries.iter().map(|e| e.ssl_ms).collect())?;
    write_string_col(entries.iter().map(|e| e.method.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.url.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.host.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.path.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.query_string.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.http_version.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.request_headers.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.request_cookies.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.request_body_hash.clone()).collect())?;
    write_i64_col(entries.iter().map(|e| e.request_body_size).collect())?;
    write_i32_col(entries.iter().map(|e| e.status).collect())?;
    write_string_col(entries.iter().map(|e| e.status_text.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.response_headers.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.response_cookies.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.response_body_hash.clone()).collect())?;
    write_i64_col(entries.iter().map(|e| e.response_body_size).collect())?;
    write_string_col(entries.iter().map(|e| e.response_body_hash_raw.clone()).collect())?;
    write_i64_col(entries.iter().map(|e| e.response_body_size_raw).collect())?;
    write_string_col(entries.iter().map(|e| e.response_mime_type.clone()).collect())?;
    write_i32_col(entries.iter().map(|e| e.is_redirect).collect())?;
    write_string_col(entries.iter().map(|e| e.server_ip.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.connection_id.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.request_id.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.parent_request_id.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.initiator_type.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.initiator_url.clone()).collect())?;
    write_i64_col(entries.iter().map(|e| e.initiator_line).collect())?;
    write_i64_col(entries.iter().map(|e| e.initiator_column).collect())?;
    write_string_col(entries.iter().map(|e| e.redirect_url.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.tls_version.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.tls_cipher_suite.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.tls_cert_subject.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.tls_cert_issuer.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.tls_cert_expiry.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.entry_hash.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.entry_extensions.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.request_extensions.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.response_extensions.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.content_extensions.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.timings_extensions.clone()).collect())?;
    write_string_col(entries.iter().map(|e| e.post_data_extensions.clone()).collect())?;

    row_group_writer.close()?;
    writer.close()?;
    Ok(())
}

#[cfg(not(feature = "parquet"))]
fn write_parquet(_path: &Path, _entries: &[EntryRow]) -> Result<()> {
    Err(HarliteError::InvalidArgs(
        "Parquet export requires the 'parquet' feature".to_string(),
    ))
}
