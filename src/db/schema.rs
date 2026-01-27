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
    entry_count INTEGER
);

-- Page information
CREATE TABLE IF NOT EXISTS pages (
    id TEXT NOT NULL,
    import_id INTEGER REFERENCES imports(id),
    started_at TEXT,
    title TEXT,
    on_content_load_ms REAL,
    on_load_ms REAL,
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
    connection_id TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_entries_url ON entries(url);
CREATE INDEX IF NOT EXISTS idx_entries_host ON entries(host);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
CREATE INDEX IF NOT EXISTS idx_entries_method ON entries(method);
CREATE INDEX IF NOT EXISTS idx_entries_mime ON entries(response_mime_type);
CREATE INDEX IF NOT EXISTS idx_entries_started ON entries(started_at);
CREATE INDEX IF NOT EXISTS idx_entries_import ON entries(import_id);
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
    entry_count INTEGER
);

-- Page information
CREATE TABLE IF NOT EXISTS pages (
    id TEXT NOT NULL,
    import_id INTEGER REFERENCES imports(id),
    started_at TEXT,
    title TEXT,
    on_content_load_ms REAL,
    on_load_ms REAL,
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
    connection_id TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_entries_url ON entries(url);
CREATE INDEX IF NOT EXISTS idx_entries_host ON entries(host);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
CREATE INDEX IF NOT EXISTS idx_entries_method ON entries(method);
CREATE INDEX IF NOT EXISTS idx_entries_mime ON entries(response_mime_type);
CREATE INDEX IF NOT EXISTS idx_entries_started ON entries(started_at);
CREATE INDEX IF NOT EXISTS idx_entries_import ON entries(import_id);

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
}
