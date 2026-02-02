use rusqlite::Connection;

use crate::error::Result;

const SCHEMA_CORE: &str = r#"
-- Content-addressable blob storage
CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY,
    content BLOB NOT NULL,
    size INTEGER NOT NULL,
    mime_type TEXT,
    external_path TEXT
);

-- Import tracking
CREATE TABLE IF NOT EXISTS imports (
    id INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    entry_count INTEGER,
    log_extensions TEXT,
    status TEXT NOT NULL DEFAULT 'complete',
    entries_total INTEGER,
    entries_skipped INTEGER
);

-- Page information
CREATE TABLE IF NOT EXISTS pages (
    id TEXT NOT NULL,
    import_id INTEGER REFERENCES imports(id),
    started_at TEXT,
    title TEXT,
    on_content_load_ms REAL,
    on_load_ms REAL,
    page_extensions TEXT,
    page_timings_extensions TEXT,
    PRIMARY KEY (id, import_id)
);

-- Main entries table
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY,
    import_id INTEGER REFERENCES imports(id),
    page_id TEXT,

    -- Timing
    started_at TEXT,
    time_ms REAL,
    blocked_ms REAL,
    dns_ms REAL,
    connect_ms REAL,
    send_ms REAL,
    wait_ms REAL,
    receive_ms REAL,
    ssl_ms REAL,

    -- Request
    method TEXT,
    url TEXT,
    host TEXT,
    path TEXT,
    query_string TEXT,
    http_version TEXT,
    request_headers TEXT,
    request_cookies TEXT,
    request_body_hash TEXT REFERENCES blobs(hash),
    request_body_size INTEGER,

    -- Response
    status INTEGER,
    status_text TEXT,
    response_headers TEXT,
    response_cookies TEXT,
    response_body_hash TEXT REFERENCES blobs(hash),
    response_body_size INTEGER,
    response_body_hash_raw TEXT REFERENCES blobs(hash),
    response_body_size_raw INTEGER,
    response_mime_type TEXT,

    -- Metadata
    is_redirect INTEGER,
    server_ip TEXT,
    connection_id TEXT,
    tls_version TEXT,
    tls_cipher_suite TEXT,
    tls_cert_subject TEXT,
    tls_cert_issuer TEXT,
    tls_cert_expiry TEXT,
    entry_hash TEXT,

    -- HAR extensions (JSON)
    entry_extensions TEXT,
    request_extensions TEXT,
    response_extensions TEXT,
    content_extensions TEXT,
    timings_extensions TEXT,
    post_data_extensions TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_entries_url ON entries(url);
CREATE INDEX IF NOT EXISTS idx_entries_host ON entries(host);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
CREATE INDEX IF NOT EXISTS idx_entries_method ON entries(method);
CREATE INDEX IF NOT EXISTS idx_entries_mime ON entries(response_mime_type);
CREATE INDEX IF NOT EXISTS idx_entries_started ON entries(started_at);
CREATE INDEX IF NOT EXISTS idx_entries_import ON entries(import_id);
CREATE INDEX IF NOT EXISTS idx_entries_entry_hash ON entries(entry_hash);
"#;

const SCHEMA_FTS: &str = r#"
-- Full-text search over response bodies (text-only, deduped by blob hash)
CREATE VIRTUAL TABLE IF NOT EXISTS response_body_fts
USING fts5(hash UNINDEXED, body, tokenize = 'unicode61');
"#;

pub const SCHEMA: &str = r#"
-- Content-addressable blob storage
CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY,
    content BLOB NOT NULL,
    size INTEGER NOT NULL,
    mime_type TEXT,
    external_path TEXT
);

-- Import tracking
CREATE TABLE IF NOT EXISTS imports (
    id INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    entry_count INTEGER,
    log_extensions TEXT,
    status TEXT NOT NULL DEFAULT 'complete',
    entries_total INTEGER,
    entries_skipped INTEGER
);

-- Page information
CREATE TABLE IF NOT EXISTS pages (
    id TEXT NOT NULL,
    import_id INTEGER REFERENCES imports(id),
    started_at TEXT,
    title TEXT,
    on_content_load_ms REAL,
    on_load_ms REAL,
    page_extensions TEXT,
    page_timings_extensions TEXT,
    PRIMARY KEY (id, import_id)
);

-- Main entries table
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY,
    import_id INTEGER REFERENCES imports(id),
    page_id TEXT,

    -- Timing
    started_at TEXT,
    time_ms REAL,
    blocked_ms REAL,
    dns_ms REAL,
    connect_ms REAL,
    send_ms REAL,
    wait_ms REAL,
    receive_ms REAL,
    ssl_ms REAL,

    -- Request
    method TEXT,
    url TEXT,
    host TEXT,
    path TEXT,
    query_string TEXT,
    http_version TEXT,
    request_headers TEXT,
    request_cookies TEXT,
    request_body_hash TEXT REFERENCES blobs(hash),
    request_body_size INTEGER,

    -- Response
    status INTEGER,
    status_text TEXT,
    response_headers TEXT,
    response_cookies TEXT,
    response_body_hash TEXT REFERENCES blobs(hash),
    response_body_size INTEGER,
    response_body_hash_raw TEXT REFERENCES blobs(hash),
    response_body_size_raw INTEGER,
    response_mime_type TEXT,

    -- Metadata
    is_redirect INTEGER,
    server_ip TEXT,
    connection_id TEXT,
    tls_version TEXT,
    tls_cipher_suite TEXT,
    tls_cert_subject TEXT,
    tls_cert_issuer TEXT,
    tls_cert_expiry TEXT,
    entry_hash TEXT,

    -- HAR extensions (JSON)
    entry_extensions TEXT,
    request_extensions TEXT,
    response_extensions TEXT,
    content_extensions TEXT,
    timings_extensions TEXT,
    post_data_extensions TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_entries_url ON entries(url);
CREATE INDEX IF NOT EXISTS idx_entries_host ON entries(host);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
CREATE INDEX IF NOT EXISTS idx_entries_method ON entries(method);
CREATE INDEX IF NOT EXISTS idx_entries_mime ON entries(response_mime_type);
CREATE INDEX IF NOT EXISTS idx_entries_started ON entries(started_at);
CREATE INDEX IF NOT EXISTS idx_entries_import ON entries(import_id);
CREATE INDEX IF NOT EXISTS idx_entries_entry_hash ON entries(entry_hash);

-- Full-text search over response bodies (text-only, deduped by blob hash)
CREATE VIRTUAL TABLE IF NOT EXISTS response_body_fts
USING fts5(hash UNINDEXED, body, tokenize = 'unicode61');
"#;

/// Create the SQLite schema for a harlite database.
pub fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA_CORE)?;
    conn.execute_batch(SCHEMA_FTS).map_err(|e| {
        if e.to_string().contains("no such module: fts5") {
            crate::error::HarliteError::InvalidArgs(
                "SQLite FTS5 support is required (this build is missing 'fts5')".to_string(),
            )
        } else {
            e.into()
        }
    })?;
    ensure_schema_upgrades(conn)?;
    Ok(())
}

/// Apply idempotent schema upgrades for existing databases.
pub fn ensure_schema_upgrades(conn: &Connection) -> Result<()> {
    if !table_has_column(conn, "blobs", "external_path")? {
        conn.execute("ALTER TABLE blobs ADD COLUMN external_path TEXT", [])?;
    }

    if !table_has_column(conn, "imports", "log_extensions")? {
        conn.execute("ALTER TABLE imports ADD COLUMN log_extensions TEXT", [])?;
    }
    if !table_has_column(conn, "imports", "status")? {
        conn.execute(
            "ALTER TABLE imports ADD COLUMN status TEXT NOT NULL DEFAULT 'complete'",
            [],
        )?;
    }
    if !table_has_column(conn, "imports", "entries_total")? {
        conn.execute("ALTER TABLE imports ADD COLUMN entries_total INTEGER", [])?;
    }
    if !table_has_column(conn, "imports", "entries_skipped")? {
        conn.execute("ALTER TABLE imports ADD COLUMN entries_skipped INTEGER", [])?;
    }

    if !table_has_column(conn, "pages", "page_extensions")? {
        conn.execute("ALTER TABLE pages ADD COLUMN page_extensions TEXT", [])?;
    }
    if !table_has_column(conn, "pages", "page_timings_extensions")? {
        conn.execute(
            "ALTER TABLE pages ADD COLUMN page_timings_extensions TEXT",
            [],
        )?;
    }

    if !table_has_column(conn, "entries", "response_body_hash_raw")? {
        conn.execute(
            "ALTER TABLE entries ADD COLUMN response_body_hash_raw TEXT",
            [],
        )?;
    }
    if !table_has_column(conn, "entries", "response_body_size_raw")? {
        conn.execute(
            "ALTER TABLE entries ADD COLUMN response_body_size_raw INTEGER",
            [],
        )?;
    }
    if !table_has_column(conn, "entries", "blocked_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN blocked_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "dns_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN dns_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "connect_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN connect_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "send_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN send_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "wait_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN wait_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "receive_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN receive_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "ssl_ms")? {
        conn.execute("ALTER TABLE entries ADD COLUMN ssl_ms REAL", [])?;
    }
    if !table_has_column(conn, "entries", "entry_extensions")? {
        conn.execute("ALTER TABLE entries ADD COLUMN entry_extensions TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "request_extensions")? {
        conn.execute("ALTER TABLE entries ADD COLUMN request_extensions TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "response_extensions")? {
        conn.execute(
            "ALTER TABLE entries ADD COLUMN response_extensions TEXT",
            [],
        )?;
    }
    if !table_has_column(conn, "entries", "content_extensions")? {
        conn.execute("ALTER TABLE entries ADD COLUMN content_extensions TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "timings_extensions")? {
        conn.execute("ALTER TABLE entries ADD COLUMN timings_extensions TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "post_data_extensions")? {
        conn.execute(
            "ALTER TABLE entries ADD COLUMN post_data_extensions TEXT",
            [],
        )?;
    }
    if !table_has_column(conn, "entries", "entry_hash")? {
        conn.execute("ALTER TABLE entries ADD COLUMN entry_hash TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "tls_version")? {
        conn.execute("ALTER TABLE entries ADD COLUMN tls_version TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "tls_cipher_suite")? {
        conn.execute("ALTER TABLE entries ADD COLUMN tls_cipher_suite TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "tls_cert_subject")? {
        conn.execute("ALTER TABLE entries ADD COLUMN tls_cert_subject TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "tls_cert_issuer")? {
        conn.execute("ALTER TABLE entries ADD COLUMN tls_cert_issuer TEXT", [])?;
    }
    if !table_has_column(conn, "entries", "tls_cert_expiry")? {
        conn.execute("ALTER TABLE entries ADD COLUMN tls_cert_expiry TEXT", [])?;
    }

    // Ensure FTS table exists for older databases created before the feature.
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='response_body_fts'",
        [],
        |r| r.get(0),
    )?;
    if exists == 0 {
        conn.execute_batch(SCHEMA_FTS).map_err(|e| {
            if e.to_string().contains("no such module: fts5") {
                crate::error::HarliteError::InvalidArgs(
                    "SQLite FTS5 support is required (this build is missing 'fts5')".to_string(),
                )
            } else {
                e.into()
            }
        })?;
    }

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entries_entry_hash ON entries(entry_hash)",
        [],
    )?;

    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(1))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(names.iter().any(|n| n == column))
}

#[cfg(test)]
mod tests {
    use super::create_schema;
    use rusqlite::Connection;
    use std::fs;

    #[test]
    fn creates_tables() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        create_schema(&conn).expect("schema created");

        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .expect("prepare query");
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .expect("query")
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"blobs".to_string()));
        assert!(tables.contains(&"imports".to_string()));
        assert!(tables.contains(&"pages".to_string()));
        assert!(tables.contains(&"entries".to_string()));
        assert!(tables.contains(&"response_body_fts".to_string()));
    }

    #[test]
    fn schema_sql_matches_runtime_schema() {
        let on_disk = fs::read_to_string("schema.sql").expect("read schema.sql");
        let normalized_on_disk = on_disk.replace("\r\n", "\n").trim().to_string();
        let normalized_runtime = super::SCHEMA.trim().to_string();

        assert_eq!(normalized_on_disk, normalized_runtime);
    }
}
