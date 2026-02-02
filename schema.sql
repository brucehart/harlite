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
    post_data_extensions TEXT,

    -- GraphQL metadata
    graphql_operation_type TEXT,
    graphql_operation_name TEXT,
    graphql_top_level_fields TEXT
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
CREATE INDEX IF NOT EXISTS idx_entries_graphql_type ON entries(graphql_operation_type);
CREATE INDEX IF NOT EXISTS idx_entries_graphql_name ON entries(graphql_operation_name);

-- GraphQL top-level fields
CREATE TABLE IF NOT EXISTS graphql_fields (
    entry_id INTEGER REFERENCES entries(id),
    field TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_graphql_fields_field ON graphql_fields(field);
CREATE INDEX IF NOT EXISTS idx_graphql_fields_entry ON graphql_fields(entry_id);

-- Full-text search over response bodies (text-only, deduped by blob hash)
CREATE VIRTUAL TABLE IF NOT EXISTS response_body_fts
USING fts5(hash UNINDEXED, body, tokenize = 'unicode61');
