use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};

use crate::db::create_schema;
use crate::error::{HarliteError, Result};

#[derive(Clone, Copy, Debug, clap::ValueEnum, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DedupStrategy {
    Hash,
    Exact,
}

pub struct MergeOptions {
    pub output: Option<PathBuf>,
    pub dry_run: bool,
    pub dedup: DedupStrategy,
}

#[derive(Default)]
struct MergeStats {
    imports_total: usize,
    imports_added: usize,
    imports_deduped: usize,
    pages_total: usize,
    pages_added: usize,
    pages_deduped: usize,
    entries_total: usize,
    entries_added: usize,
    entries_deduped: usize,
    blobs_total: usize,
    blobs_added: usize,
    blobs_deduped: usize,
    fts_total: usize,
    fts_added: usize,
    fts_deduped: usize,
}

#[derive(Clone, Debug)]
struct ImportRow {
    id: i64,
    source_file: String,
    imported_at: String,
    log_extensions: Option<String>,
}

#[derive(Clone, Debug)]
struct PageRow {
    import_id: i64,
    id: String,
    started_at: Option<String>,
    title: Option<String>,
    on_content_load_ms: Option<f64>,
    on_load_ms: Option<f64>,
    page_extensions: Option<String>,
    page_timings_extensions: Option<String>,
}

#[derive(Clone, Debug)]
struct EntryRow {
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
    status: Option<i64>,
    status_text: Option<String>,
    response_headers: Option<String>,
    response_cookies: Option<String>,
    response_body_hash: Option<String>,
    response_body_size: Option<i64>,
    response_body_hash_raw: Option<String>,
    response_body_size_raw: Option<i64>,
    response_mime_type: Option<String>,
    is_redirect: Option<i64>,
    server_ip: Option<String>,
    connection_id: Option<String>,
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
    graphql_operation_type: Option<String>,
    graphql_operation_name: Option<String>,
    graphql_top_level_fields: Option<String>,
}

#[derive(Clone, Debug)]
struct BlobRow {
    hash: String,
    content: Vec<u8>,
    size: i64,
    mime_type: Option<String>,
    external_path: Option<String>,
}

#[derive(Clone, Debug)]
struct ImportMeta {
    id: i64,
    log_extensions: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum EntryKey {
    Hash([u8; 32]),
    Exact(Vec<u8>),
}

const IMPORT_COLUMNS: &[&str] = &["id", "source_file", "imported_at", "log_extensions"];

const PAGE_COLUMNS: &[&str] = &[
    "import_id",
    "id",
    "started_at",
    "title",
    "on_content_load_ms",
    "on_load_ms",
    "page_extensions",
    "page_timings_extensions",
];

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
    "graphql_operation_type",
    "graphql_operation_name",
    "graphql_top_level_fields",
];

pub fn run_merge(databases: Vec<PathBuf>, options: &MergeOptions) -> Result<()> {
    if databases.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No database files specified".to_string(),
        ));
    }
    if databases.len() < 2 {
        return Err(HarliteError::InvalidArgs(
            "Merge requires at least two databases".to_string(),
        ));
    }

    let output_path = resolve_output_path(&databases, options.output.as_ref())?;
    ensure_output_not_in_inputs(&databases, &output_path)?;

    let output_conn = if options.dry_run {
        Connection::open_in_memory()?
    } else {
        Connection::open(&output_path)?
    };
    create_schema(&output_conn)?;

    let output_columns = table_columns(&output_conn, "entries")?;
    let mut import_map = load_existing_imports(&output_conn)?;
    let mut entry_keys: HashMap<i64, HashMap<EntryKey, i64>> = HashMap::new();
    let mut fts_hashes = load_existing_fts_hashes(&output_conn)?;

    let tx = output_conn.unchecked_transaction()?;

    let mut stats = MergeStats::default();
    let input_count = databases.len();

    for db_path in databases {
        let input_conn = Connection::open(&db_path)?;
        let input_columns = table_columns(&input_conn, "entries")?;

        merge_blobs(&input_conn, &tx, &mut stats)?;

        let imports = load_imports(&input_conn)?;
        let mut import_id_map: HashMap<i64, i64> = HashMap::new();
        for import in imports {
            stats.imports_total += 1;
            let key = (import.source_file.clone(), import.imported_at.clone());
            if let Some(meta) = import_map.get(&key) {
                import_id_map.insert(import.id, meta.id);
                stats.imports_deduped += 1;
                if meta.log_extensions.is_none() && import.log_extensions.is_some() {
                    tx.execute(
                        "UPDATE imports SET log_extensions = ?1 WHERE id = ?2",
                        params![import.log_extensions, meta.id],
                    )?;
                    import_map.insert(
                        key,
                        ImportMeta {
                            id: meta.id,
                            log_extensions: import.log_extensions,
                        },
                    );
                }
            } else {
                let new_id = insert_import(&tx, &import)?;
                import_id_map.insert(import.id, new_id);
                import_map.insert(
                    key,
                    ImportMeta {
                        id: new_id,
                        log_extensions: import.log_extensions,
                    },
                );
                stats.imports_added += 1;
            }
        }

        let pages = load_pages(&input_conn)?;
        for page in pages {
            stats.pages_total += 1;
            let Some(&mapped_import_id) = import_id_map.get(&page.import_id) else {
                continue;
            };
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO pages (id, import_id, started_at, title, on_content_load_ms, on_load_ms, page_extensions, page_timings_extensions)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    page.id,
                    mapped_import_id,
                    page.started_at,
                    page.title,
                    page.on_content_load_ms,
                    page.on_load_ms,
                    page.page_extensions,
                    page.page_timings_extensions,
                ],
            )?;
            if inserted > 0 {
                stats.pages_added += 1;
            } else {
                stats.pages_deduped += 1;
            }
        }

        let graphql_fields = load_graphql_fields(&input_conn)?;

        let mut stmt = input_conn.prepare(&entry_select_sql(&input_columns))?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                EntryRow {
                    import_id: row.get(1)?,
                    page_id: row.get(2)?,
                    started_at: row.get(3)?,
                    time_ms: row.get(4)?,
                    blocked_ms: row.get(5)?,
                    dns_ms: row.get(6)?,
                    connect_ms: row.get(7)?,
                    send_ms: row.get(8)?,
                    wait_ms: row.get(9)?,
                    receive_ms: row.get(10)?,
                    ssl_ms: row.get(11)?,
                    method: row.get(12)?,
                    url: row.get(13)?,
                    host: row.get(14)?,
                    path: row.get(15)?,
                    query_string: row.get(16)?,
                    http_version: row.get(17)?,
                    request_headers: row.get(18)?,
                    request_cookies: row.get(19)?,
                    request_body_hash: row.get(20)?,
                    request_body_size: row.get(21)?,
                    status: row.get(22)?,
                    status_text: row.get(23)?,
                    response_headers: row.get(24)?,
                    response_cookies: row.get(25)?,
                    response_body_hash: row.get(26)?,
                    response_body_size: row.get(27)?,
                    response_body_hash_raw: row.get(28)?,
                    response_body_size_raw: row.get(29)?,
                    response_mime_type: row.get(30)?,
                    is_redirect: row.get(31)?,
                    server_ip: row.get(32)?,
                    connection_id: row.get(33)?,
                    tls_version: row.get(34)?,
                    tls_cipher_suite: row.get(35)?,
                    tls_cert_subject: row.get(36)?,
                    tls_cert_issuer: row.get(37)?,
                    tls_cert_expiry: row.get(38)?,
                    entry_hash: row.get(39)?,
                    entry_extensions: row.get(40)?,
                    request_extensions: row.get(41)?,
                    response_extensions: row.get(42)?,
                    content_extensions: row.get(43)?,
                    timings_extensions: row.get(44)?,
                    post_data_extensions: row.get(45)?,
                    graphql_operation_type: row.get(46)?,
                    graphql_operation_name: row.get(47)?,
                    graphql_top_level_fields: row.get(48)?,
                },
            ))
        })?;

        for row in rows {
            let (entry_id, entry) = row?;
            stats.entries_total += 1;
            let Some(&mapped_import_id) = import_id_map.get(&entry.import_id) else {
                continue;
            };

            let keys = entry_keys.entry(mapped_import_id).or_insert_with(|| {
                load_entry_keys_for_import(&tx, &output_columns, mapped_import_id, options.dedup)
                    .unwrap_or_default()
            });

            let key = entry_key(&entry, options.dedup);
            if let Some(&existing_entry_id) = keys.get(&key) {
                // Entry already exists. Update TLS fields if they are missing in the existing entry.
                update_tls_fields(&tx, existing_entry_id, &entry)?;
                if let Some(fields) = graphql_fields.get(&entry_id) {
                    insert_graphql_fields(&tx, existing_entry_id, fields)?;
                }
                stats.entries_deduped += 1;
                continue;
            }

            let new_entry_id = insert_entry(&tx, mapped_import_id, &entry)?;
            keys.insert(key, new_entry_id);
            if let Some(fields) = graphql_fields.get(&entry_id) {
                insert_graphql_fields(&tx, new_entry_id, fields)?;
            }
            stats.entries_added += 1;
        }

        merge_fts(&input_conn, &tx, &mut fts_hashes, &mut stats)?;
    }

    tx.execute(
        "UPDATE imports SET entry_count = (SELECT COUNT(*) FROM entries WHERE entries.import_id = imports.id)",
        [],
    )?;

    tx.commit()?;

    if options.dry_run {
        println!("Dry run: no changes written.");
    }
    println!(
        "Merged {} databases into {}",
        input_count,
        output_path.display()
    );
    print_stats(&stats);

    Ok(())
}

fn resolve_output_path(databases: &[PathBuf], output: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(path) = output {
        return Ok(path.clone());
    }

    let first = databases
        .get(0)
        .ok_or_else(|| HarliteError::InvalidArgs("No input databases".to_string()))?;
    let stem = first
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("merged");
    Ok(PathBuf::from(format!("{}-merged.db", stem)))
}

fn ensure_output_not_in_inputs(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let output = output
        .canonicalize()
        .unwrap_or_else(|_| output.to_path_buf());
    for input in inputs {
        let input = input.canonicalize().unwrap_or_else(|_| input.to_path_buf());
        if input == output {
            return Err(HarliteError::InvalidArgs(
                "Output database must be different from input databases".to_string(),
            ));
        }
    }
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let cols: Vec<String> = stmt
        .query_map([], |row| row.get(1))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(cols.into_iter().collect())
}

fn select_col(columns: &HashSet<String>, name: &str) -> String {
    if columns.contains(name) {
        name.to_string()
    } else {
        format!("NULL as {}", name)
    }
}

fn load_imports(conn: &Connection) -> Result<Vec<ImportRow>> {
    let columns = table_columns(conn, "imports")?;
    let mut select_cols = Vec::new();
    for col in IMPORT_COLUMNS {
        select_cols.push(select_col(&columns, col));
    }

    let sql = format!("SELECT {} FROM imports", select_cols.join(", "));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(ImportRow {
            id: row.get(0)?,
            source_file: row.get(1)?,
            imported_at: row.get(2)?,
            log_extensions: row.get(3)?,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

fn load_pages(conn: &Connection) -> Result<Vec<PageRow>> {
    let columns = table_columns(conn, "pages")?;
    let mut select_cols = Vec::new();
    for col in PAGE_COLUMNS {
        select_cols.push(select_col(&columns, col));
    }

    let sql = format!("SELECT {} FROM pages", select_cols.join(", "));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(PageRow {
            import_id: row.get(0)?,
            id: row.get(1)?,
            started_at: row.get(2)?,
            title: row.get(3)?,
            on_content_load_ms: row.get(4)?,
            on_load_ms: row.get(5)?,
            page_extensions: row.get(6)?,
            page_timings_extensions: row.get(7)?,
        })
    })?;

    Ok(rows.filter_map(|r| r.ok()).collect())
}

fn entry_select_sql(columns: &HashSet<String>) -> String {
    let mut select_cols = Vec::new();
    for col in ENTRY_COLUMNS {
        select_cols.push(select_col(columns, col));
    }
    format!("SELECT id, {} FROM entries", select_cols.join(", "))
}

fn load_existing_imports(conn: &Connection) -> Result<HashMap<(String, String), ImportMeta>> {
    if !table_exists(conn, "imports")? {
        return Ok(HashMap::new());
    }

    let mut stmt =
        conn.prepare("SELECT source_file, imported_at, id, log_extensions FROM imports")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            ImportMeta {
                id: row.get(2)?,
                log_extensions: row.get(3)?,
            },
        ))
    })?;

    let mut out = HashMap::new();
    for row in rows {
        let (source_file, imported_at, meta) = row?;
        out.insert((source_file, imported_at), meta);
    }
    Ok(out)
}

fn insert_import(conn: &Connection, import: &ImportRow) -> Result<i64> {
    conn.execute(
        "INSERT INTO imports (source_file, imported_at, entry_count, log_extensions) VALUES (?1, ?2, 0, ?3)",
        params![
            import.source_file.as_str(),
            import.imported_at.as_str(),
            import.log_extensions.as_deref()
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

fn load_entry_keys_for_import(
    conn: &Connection,
    columns: &HashSet<String>,
    import_id: i64,
    strategy: DedupStrategy,
) -> Result<HashMap<EntryKey, i64>> {
    let sql = format!("SELECT id, {} FROM entries WHERE import_id = ?1", 
        ENTRY_COLUMNS.iter()
            .map(|col| select_col(columns, col))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![import_id], |row| {
        Ok((
            row.get::<_, i64>(0)?, // entry id
            EntryRow {
                import_id: row.get(1)?,
                page_id: row.get(2)?,
                started_at: row.get(3)?,
                time_ms: row.get(4)?,
                blocked_ms: row.get(5)?,
                dns_ms: row.get(6)?,
                connect_ms: row.get(7)?,
                send_ms: row.get(8)?,
                wait_ms: row.get(9)?,
                receive_ms: row.get(10)?,
                ssl_ms: row.get(11)?,
                method: row.get(12)?,
                url: row.get(13)?,
                host: row.get(14)?,
                path: row.get(15)?,
                query_string: row.get(16)?,
                http_version: row.get(17)?,
                request_headers: row.get(18)?,
                request_cookies: row.get(19)?,
                request_body_hash: row.get(20)?,
                request_body_size: row.get(21)?,
                status: row.get(22)?,
                status_text: row.get(23)?,
                response_headers: row.get(24)?,
                response_cookies: row.get(25)?,
                response_body_hash: row.get(26)?,
                response_body_size: row.get(27)?,
                response_body_hash_raw: row.get(28)?,
                response_body_size_raw: row.get(29)?,
                response_mime_type: row.get(30)?,
                is_redirect: row.get(31)?,
                server_ip: row.get(32)?,
                connection_id: row.get(33)?,
                tls_version: row.get(34)?,
                tls_cipher_suite: row.get(35)?,
                tls_cert_subject: row.get(36)?,
                tls_cert_issuer: row.get(37)?,
                tls_cert_expiry: row.get(38)?,
                entry_hash: row.get(39)?,
                entry_extensions: row.get(40)?,
                request_extensions: row.get(41)?,
                response_extensions: row.get(42)?,
                content_extensions: row.get(43)?,
                timings_extensions: row.get(44)?,
                post_data_extensions: row.get(45)?,
                graphql_operation_type: row.get(46)?,
                graphql_operation_name: row.get(47)?,
                graphql_top_level_fields: row.get(48)?,
            },
        ))
    })?;

    let mut keys = HashMap::new();
    for row in rows {
        let (entry_id, entry) = row?;
        keys.insert(entry_key(&entry, strategy), entry_id);
    }
    Ok(keys)
}

fn entry_key(entry: &EntryRow, strategy: DedupStrategy) -> EntryKey {
    let mut buf = Vec::new();
    encode_opt_string(&mut buf, entry.page_id.as_deref());
    encode_opt_string(&mut buf, entry.started_at.as_deref());
    encode_opt_f64(&mut buf, entry.time_ms);
    encode_opt_f64(&mut buf, entry.blocked_ms);
    encode_opt_f64(&mut buf, entry.dns_ms);
    encode_opt_f64(&mut buf, entry.connect_ms);
    encode_opt_f64(&mut buf, entry.send_ms);
    encode_opt_f64(&mut buf, entry.wait_ms);
    encode_opt_f64(&mut buf, entry.receive_ms);
    encode_opt_f64(&mut buf, entry.ssl_ms);
    encode_opt_string(&mut buf, entry.method.as_deref());
    encode_opt_string(&mut buf, entry.url.as_deref());
    encode_opt_string(&mut buf, entry.host.as_deref());
    encode_opt_string(&mut buf, entry.path.as_deref());
    encode_opt_string(&mut buf, entry.query_string.as_deref());
    encode_opt_string(&mut buf, entry.http_version.as_deref());
    encode_opt_string(&mut buf, entry.request_headers.as_deref());
    encode_opt_string(&mut buf, entry.request_cookies.as_deref());
    encode_opt_string(&mut buf, entry.request_body_hash.as_deref());
    encode_opt_i64(&mut buf, entry.request_body_size);
    encode_opt_i64(&mut buf, entry.status);
    encode_opt_string(&mut buf, entry.status_text.as_deref());
    encode_opt_string(&mut buf, entry.response_headers.as_deref());
    encode_opt_string(&mut buf, entry.response_cookies.as_deref());
    encode_opt_string(&mut buf, entry.response_body_hash.as_deref());
    encode_opt_i64(&mut buf, entry.response_body_size);
    encode_opt_string(&mut buf, entry.response_body_hash_raw.as_deref());
    encode_opt_i64(&mut buf, entry.response_body_size_raw);
    encode_opt_string(&mut buf, entry.response_mime_type.as_deref());
    encode_opt_i64(&mut buf, entry.is_redirect);
    encode_opt_string(&mut buf, entry.server_ip.as_deref());
    encode_opt_string(&mut buf, entry.connection_id.as_deref());
    // TLS fields are omitted from the merge dedup key to allow enriching existing entries with TLS metadata.
    // entry_hash is derived from the entry contents; omit from the merge dedup key.
    encode_opt_string(&mut buf, entry.entry_extensions.as_deref());
    encode_opt_string(&mut buf, entry.request_extensions.as_deref());
    encode_opt_string(&mut buf, entry.response_extensions.as_deref());
    encode_opt_string(&mut buf, entry.content_extensions.as_deref());
    encode_opt_string(&mut buf, entry.timings_extensions.as_deref());
    encode_opt_string(&mut buf, entry.post_data_extensions.as_deref());

    match strategy {
        DedupStrategy::Hash => EntryKey::Hash(*blake3::hash(&buf).as_bytes()),
        DedupStrategy::Exact => EntryKey::Exact(buf),
    }
}

fn encode_opt_string(buf: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(s) => {
            buf.push(1);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
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

fn update_tls_fields(conn: &Connection, entry_id: i64, entry: &EntryRow) -> Result<()> {
    // Update TLS fields using COALESCE to preserve existing values and only fill in missing ones
    conn.execute(
        "UPDATE entries SET
            tls_version = COALESCE(tls_version, ?1),
            tls_cipher_suite = COALESCE(tls_cipher_suite, ?2),
            tls_cert_subject = COALESCE(tls_cert_subject, ?3),
            tls_cert_issuer = COALESCE(tls_cert_issuer, ?4),
            tls_cert_expiry = COALESCE(tls_cert_expiry, ?5)
        WHERE id = ?6",
        params![
            entry.tls_version.as_deref(),
            entry.tls_cipher_suite.as_deref(),
            entry.tls_cert_subject.as_deref(),
            entry.tls_cert_issuer.as_deref(),
            entry.tls_cert_expiry.as_deref(),
            entry_id,
        ],
    )?;
    Ok(())
}

fn insert_entry(conn: &Connection, import_id: i64, entry: &EntryRow) -> Result<i64> {
    conn.execute(
        "INSERT INTO entries (
            import_id, page_id, started_at, time_ms, blocked_ms, dns_ms, connect_ms, send_ms, wait_ms, receive_ms, ssl_ms,
            method, url, host, path, query_string, http_version,
            request_headers, request_cookies, request_body_hash, request_body_size,
            status, status_text, response_headers, response_cookies,
            response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw, response_mime_type,
            is_redirect, server_ip, connection_id, tls_version, tls_cipher_suite, tls_cert_subject, tls_cert_issuer, tls_cert_expiry, entry_hash,
            entry_extensions, request_extensions, response_extensions, content_extensions, timings_extensions, post_data_extensions,
            graphql_operation_type, graphql_operation_name, graphql_top_level_fields
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40,
            ?41, ?42, ?43, ?44, ?45, ?46, ?47, ?48
        )",
        params![
            import_id,
            entry.page_id.as_deref(),
            entry.started_at.as_deref(),
            entry.time_ms,
            entry.blocked_ms,
            entry.dns_ms,
            entry.connect_ms,
            entry.send_ms,
            entry.wait_ms,
            entry.receive_ms,
            entry.ssl_ms,
            entry.method.as_deref(),
            entry.url.as_deref(),
            entry.host.as_deref(),
            entry.path.as_deref(),
            entry.query_string.as_deref(),
            entry.http_version.as_deref(),
            entry.request_headers.as_deref(),
            entry.request_cookies.as_deref(),
            entry.request_body_hash.as_deref(),
            entry.request_body_size,
            entry.status,
            entry.status_text.as_deref(),
            entry.response_headers.as_deref(),
            entry.response_cookies.as_deref(),
            entry.response_body_hash.as_deref(),
            entry.response_body_size,
            entry.response_body_hash_raw.as_deref(),
            entry.response_body_size_raw,
            entry.response_mime_type.as_deref(),
            entry.is_redirect,
            entry.server_ip.as_deref(),
            entry.connection_id.as_deref(),
            entry.tls_version.as_deref(),
            entry.tls_cipher_suite.as_deref(),
            entry.tls_cert_subject.as_deref(),
            entry.tls_cert_issuer.as_deref(),
            entry.tls_cert_expiry.as_deref(),
            entry.entry_hash.as_deref(),
            entry.entry_extensions.as_deref(),
            entry.request_extensions.as_deref(),
            entry.response_extensions.as_deref(),
            entry.content_extensions.as_deref(),
            entry.timings_extensions.as_deref(),
            entry.post_data_extensions.as_deref(),
            entry.graphql_operation_type.as_deref(),
            entry.graphql_operation_name.as_deref(),
            entry.graphql_top_level_fields.as_deref(),
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

fn update_entry_tls_metadata(conn: &Connection, rowid: i64, entry: &EntryRow) -> Result<()> {
    conn.execute(
        "UPDATE entries SET
            tls_version = COALESCE(tls_version, ?1),
            tls_cipher_suite = COALESCE(tls_cipher_suite, ?2),
            tls_cert_subject = COALESCE(tls_cert_subject, ?3),
            tls_cert_issuer = COALESCE(tls_cert_issuer, ?4),
            tls_cert_expiry = COALESCE(tls_cert_expiry, ?5)
         WHERE rowid = ?6",
        params![
            entry.tls_version.as_deref(),
            entry.tls_cipher_suite.as_deref(),
            entry.tls_cert_subject.as_deref(),
            entry.tls_cert_issuer.as_deref(),
            entry.tls_cert_expiry.as_deref(),
            rowid,
        ],
    )?;
    Ok(())
}

fn merge_blobs(conn: &Connection, output: &Connection, stats: &mut MergeStats) -> Result<()> {
    if !table_exists(conn, "blobs")? {
        return Ok(());
    }

    let columns = table_columns(conn, "blobs")?;
    let has_external_path = columns.contains("external_path");
    let sql = if has_external_path {
        "SELECT hash, content, size, mime_type, external_path FROM blobs"
    } else {
        "SELECT hash, content, size, mime_type, NULL as external_path FROM blobs"
    };

    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(BlobRow {
            hash: row.get(0)?,
            content: row.get(1)?,
            size: row.get(2)?,
            mime_type: row.get(3)?,
            external_path: row.get(4)?,
        })
    })?;

    for row in rows {
        let blob = row?;
        stats.blobs_total += 1;
        let hash = blob.hash;
        let content = blob.content;
        let size = blob.size;
        let mime_type = blob.mime_type;
        let external_path = blob.external_path;
        let inserted = output.execute(
            "INSERT OR IGNORE INTO blobs (hash, content, size, mime_type, external_path) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &hash,
                &content,
                size,
                mime_type.as_deref(),
                external_path.as_deref(),
            ],
        )?;
        if inserted > 0 {
            stats.blobs_added += 1;
        } else {
            stats.blobs_deduped += 1;
            if external_path.is_some() {
                output.execute(
                    "UPDATE blobs SET external_path = COALESCE(external_path, ?2) WHERE hash = ?1",
                    params![&hash, external_path.as_deref()],
                )?;
            }
        }
    }

    Ok(())
}

fn merge_fts(
    conn: &Connection,
    output: &Connection,
    fts_hashes: &mut HashSet<String>,
    stats: &mut MergeStats,
) -> Result<()> {
    if !table_exists(conn, "response_body_fts")? {
        return Ok(());
    }

    let mut stmt = conn.prepare("SELECT hash, body FROM response_body_fts")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (hash, body) = row?;
        stats.fts_total += 1;
        if fts_hashes.contains(&hash) {
            stats.fts_deduped += 1;
            continue;
        }
        output.execute(
            "INSERT INTO response_body_fts (hash, body) VALUES (?1, ?2)",
            params![hash, body],
        )?;
        fts_hashes.insert(hash);
        stats.fts_added += 1;
    }

    Ok(())
}

fn load_existing_fts_hashes(conn: &Connection) -> Result<HashSet<String>> {
    if !table_exists(conn, "response_body_fts")? {
        return Ok(HashSet::new());
    }

    let mut stmt = conn.prepare("SELECT hash FROM response_body_fts")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = HashSet::new();
    for row in rows {
        if let Ok(hash) = row {
            out.insert(hash);
        }
    }
    Ok(out)
}

fn load_graphql_fields(conn: &Connection) -> Result<HashMap<i64, Vec<String>>> {
    if !table_exists(conn, "graphql_fields")? {
        return Ok(HashMap::new());
    }

    let mut stmt = conn.prepare("SELECT entry_id, field FROM graphql_fields")?;
    let rows = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?;

    let mut out: HashMap<i64, Vec<String>> = HashMap::new();
    for row in rows {
        let (entry_id, field) = row?;
        out.entry(entry_id).or_default().push(field);
    }
    Ok(out)
}

fn insert_graphql_fields(conn: &Connection, entry_id: i64, fields: &[String]) -> Result<()> {
    if fields.is_empty() || !table_exists(conn, "graphql_fields")? {
        return Ok(());
    }

    let mut stmt =
        conn.prepare_cached("INSERT OR IGNORE INTO graphql_fields (entry_id, field) VALUES (?1, ?2)")?;
    for field in fields {
        stmt.execute(params![entry_id, field])?;
    }
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn print_stats(stats: &MergeStats) {
    println!(
        "Imports: {} added, {} deduped ({} total)",
        stats.imports_added, stats.imports_deduped, stats.imports_total
    );
    println!(
        "Pages:   {} added, {} deduped ({} total)",
        stats.pages_added, stats.pages_deduped, stats.pages_total
    );
    println!(
        "Entries: {} added, {} deduped ({} total)",
        stats.entries_added, stats.entries_deduped, stats.entries_total
    );
    println!(
        "Blobs:   {} added, {} deduped ({} total)",
        stats.blobs_added, stats.blobs_deduped, stats.blobs_total
    );
    if stats.fts_total > 0 {
        println!(
            "FTS:     {} added, {} deduped ({} total)",
            stats.fts_added, stats.fts_deduped, stats.fts_total
        );
    }
}
