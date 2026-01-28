use assert_cmd::cargo::cargo_bin_cmd;
use assert_cmd::Command;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

fn harlite() -> Command {
    cargo_bin_cmd!()
}

#[test]
fn test_help() {
    harlite()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Import HAR files into SQLite"));
}

#[test]
fn test_version() {
    harlite()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("harlite"));
}

#[test]
fn test_schema_default() {
    harlite()
        .arg("schema")
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("entries"))
        .stdout(predicate::str::contains("blobs"));
}

#[test]
fn test_import_simple() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported 2 entries"));

    assert!(db_path.exists());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);

    let host: String = conn
        .query_row("SELECT host FROM entries WHERE method = 'GET'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(host, "api.example.com");
}

#[test]
fn test_import_simple_gzip() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har.gz", "-o"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported 2 entries"));

    assert!(db_path.exists());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_import_simple_brotli() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har.br", "-o"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported 2 entries"));

    assert!(db_path.exists());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_imports_list_and_prune() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["imports"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Source"))
        .stdout(predicate::str::contains("simple.har"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_id: i64 = conn
        .query_row("SELECT id FROM imports LIMIT 1", [], |r| r.get(0))
        .unwrap();

    harlite()
        .args(["prune", "--import-id", &import_id.to_string()])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let entry_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |r| r.get(0))
        .unwrap();
    let page_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM pages", [], |r| r.get(0))
        .unwrap();
    let blob_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
        .unwrap();

    assert_eq!(entry_count, 0);
    assert_eq!(import_count, 0);
    assert_eq!(page_count, 0);
    assert_eq!(blob_count, 0);
}

#[test]
fn test_import_with_pages() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/with_pages.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let page_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM pages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(page_count, 1);

    let page_id: String = conn
        .query_row(
            "SELECT page_id FROM entries WHERE page_id IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(page_id, "page_1");
}

#[test]
fn test_import_with_bodies() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let blob_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert!(blob_count > 0);

    let content: Vec<u8> = conn
        .query_row(
            "SELECT b.content FROM entries e JOIN blobs b ON e.response_body_hash = b.hash WHERE e.method = 'GET'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let content_str = String::from_utf8(content).unwrap();
    assert!(content_str.contains("Alice"));
}

#[test]
fn test_import_filters_method_status_regex() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("filtered.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "--method",
            "GET",
            "--status",
            "200",
            "--url-regex",
            "example\\.com/users$",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported 1 entries"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    let method: String = conn
        .query_row("SELECT method FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(method, "GET");
}

#[test]
fn test_import_filters_date_range() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("filtered-date.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "--from",
            "2024-01-15T10:30:01Z",
            "--to",
            "2024-01-15T10:30:01Z",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported 1 entries"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    let method: String = conn
        .query_row("SELECT method FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(method, "POST");
}

#[test]
fn test_diff_har_json() {
    harlite()
        .args([
            "diff",
            "tests/fixtures/simple.har",
            "tests/fixtures/simple_changed.har",
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"change\":\"changed\""))
        .stdout(predicate::str::contains("\"change\":\"removed\""))
        .stdout(predicate::str::contains("\"change\":\"added\""));
}

#[test]
fn test_import_stats_counts_request_and_response_bodies() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "--bodies",
            "--stats",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Unique blobs stored: 3"));
}

#[test]
fn test_import_with_gzip_decompression() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/gzip_response.har",
            "--bodies",
            "--decompress-bodies",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let content: Vec<u8> = conn
        .query_row(
            "SELECT b.content FROM entries e JOIN blobs b ON e.response_body_hash = b.hash",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let content_str = String::from_utf8(content).unwrap();
    assert_eq!(content_str, "Alice says hello (gzip).");
}

#[test]
fn test_import_with_extracted_response_bodies() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let bodies_dir = tmp.path().join("bodies");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "--bodies",
            "--extract-bodies",
        ])
        .arg(&bodies_dir)
        .args(["--extract-bodies-kind", "response", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let (external_path, content_len, size): (String, i64, i64) = conn
        .query_row(
            "SELECT b.external_path, LENGTH(b.content), b.size FROM entries e JOIN blobs b ON e.response_body_hash = b.hash WHERE e.method = 'GET' LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();

    assert!(!external_path.is_empty());
    assert_eq!(content_len, 0);
    assert!(size > 0);
    assert!(std::path::Path::new(&external_path).exists());

    let bytes = fs::read(&external_path).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("Alice"));
}

#[test]
fn test_search_command() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["search", "Alice"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("api.example.com"))
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_deduplication() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/duplicate_bodies.har",
            "--bodies",
            "--stats",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Unique blobs stored: 1"))
        .stdout(predicate::str::contains("Duplicate blobs skipped: 2"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let blob_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(blob_count, 1);

    let distinct_hashes: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT response_body_hash) FROM entries WHERE response_body_hash IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(distinct_hashes, 1);
}

#[test]
fn test_import_multiple_files() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "tests/fixtures/with_pages.har",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |r| r.get(0))
        .unwrap();
    assert_eq!(import_count, 2);

    let entry_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(entry_count, 3);
}

#[test]
fn test_info_command() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .arg("info")
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Entries: 2"))
        .stdout(predicate::str::contains("api.example.com"));
}

#[test]
fn test_stats_command_key_value() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .arg("stats")
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("imports=1"))
        .stdout(predicate::str::contains("entries=2"))
        .stdout(predicate::str::contains("date_min=2024-01-15"))
        .stdout(predicate::str::contains("date_max=2024-01-15"))
        .stdout(predicate::str::contains("unique_hosts=1"))
        .stdout(predicate::str::contains("blobs=0"))
        .stdout(predicate::str::contains("blob_bytes=0"));
}

#[test]
fn test_stats_command_json() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let output = harlite()
        .args(["stats", "--json"])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let v: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(v["imports"], 1);
    assert_eq!(v["entries"], 2);
    assert_eq!(v["date_min"], "2024-01-15");
    assert_eq!(v["date_max"], "2024-01-15");
    assert_eq!(v["unique_hosts"], 1);
    assert_eq!(v["blobs"], 0);
    assert_eq!(v["blob_bytes"], 0);
}

#[test]
fn test_stats_command_with_null_entry_count() {
    // This test verifies the fallback path where entry_count is NULL,
    // simulating databases created by other tools or older versions.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    // Create database and schema manually (including blobs table required by stats)
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS blobs (
            hash TEXT PRIMARY KEY,
            content BLOB NOT NULL,
            size INTEGER NOT NULL,
            mime_type TEXT
        );
        CREATE TABLE IF NOT EXISTS imports (
            id INTEGER PRIMARY KEY,
            source_file TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            entry_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY,
            import_id INTEGER REFERENCES imports(id),
            started_at TEXT,
            host TEXT,
            method TEXT,
            url TEXT
        );
        "#,
    )
    .unwrap();

    // Insert an import with entry_count explicitly set to NULL
    conn.execute(
        "INSERT INTO imports (id, source_file, imported_at, entry_count) VALUES (1, 'manual.har', '2024-01-15T10:00:00Z', NULL)",
        [],
    )
    .unwrap();

    // Insert entries manually
    conn.execute(
        "INSERT INTO entries (import_id, started_at, host, method, url) VALUES (1, '2024-01-15T10:00:00Z', 'example.com', 'GET', 'https://example.com/page1')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO entries (import_id, started_at, host, method, url) VALUES (1, '2024-01-15T10:01:00Z', 'example.com', 'GET', 'https://example.com/page2')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO entries (import_id, started_at, host, method, url) VALUES (1, '2024-01-15T10:02:00Z', 'other.com', 'POST', 'https://other.com/api')",
        [],
    )
    .unwrap();

    // Verify entry_count is NULL
    let entry_count_value: Option<i64> = conn
        .query_row("SELECT entry_count FROM imports WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(entry_count_value, None, "entry_count should be NULL");

    drop(conn);

    // Run stats command and verify it correctly counts entries using the fallback path
    harlite()
        .arg("stats")
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("imports=1"))
        .stdout(predicate::str::contains("entries=3"))
        .stdout(predicate::str::contains("date_min=2024-01-15"))
        .stdout(predicate::str::contains("date_max=2024-01-15"))
        .stdout(predicate::str::contains("unique_hosts=2"))
        .stdout(predicate::str::contains("blobs=0"))
        .stdout(predicate::str::contains("blob_bytes=0"));
}

#[test]
fn test_query_csv_and_json() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args([
            "query",
            "SELECT host, status FROM entries ORDER BY id LIMIT 1",
            "--format",
            "csv",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("host,status"))
        .stdout(predicate::str::contains("api.example.com,200"));

    harlite()
        .args([
            "query",
            "SELECT host, status FROM entries ORDER BY id LIMIT 1",
            "--format",
            "json",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("\"host\":\"api.example.com\""))
        .stdout(predicate::str::contains("\"status\":200"));
}

#[test]
fn test_query_default_db_detection() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .current_dir(tmp.path())
        .args([
            "query",
            "SELECT COUNT(*) AS c FROM entries",
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"c\":2"));
}

#[test]
fn test_query_rejects_writes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["query", "DELETE FROM entries", "--format", "csv"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("read-only"));
}

#[test]
fn test_redact_dry_run_does_not_modify() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    // Create a minimal HAR file inline instead of relying on an external fixture.
    let har_path = tmp.path().join("redact.har");
    let har_content = r#"{
        "log": {
            "version": "1.2",
            "creator": { "name": "harlite-test", "version": "1.0" },
            "entries": [
                {
                    "startedDateTime": "2020-01-01T00:00:00.000Z",
                    "time": 0,
                    "request": {
                        "method": "GET",
                        "url": "https://example.com/",
                        "httpVersion": "HTTP/1.1",
                        "cookies": [
                            {
                                "name": "session",
                                "value": "sess123"
                            }
                        ],
                        "headers": [
                            {
                                "name": "Authorization",
                                "value": "Bearer supersecret"
                            }
                        ],
                        "queryString": [],
                        "headersSize": -1,
                        "bodySize": -1
                    },
                    "response": {
                        "status": 200,
                        "statusText": "OK",
                        "httpVersion": "HTTP/1.1",
                        "cookies": [],
                        "headers": [],
                        "content": {
                            "size": 0,
                            "mimeType": "text/plain",
                            "text": ""
                        },
                        "redirectURL": "",
                        "headersSize": -1,
                        "bodySize": -1
                    },
                    "cache": {},
                    "timings": {
                        "send": 0,
                        "wait": 0,
                        "receive": 0
                    }
                }
            ]
        }
    }"#;
    std::fs::write(&har_path, har_content).unwrap();

    harlite()
        .arg("import")
        .arg(&har_path)
        .arg("-o")
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["redact", "--dry-run"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Dry run: would redact"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let auth: String = conn
        .query_row(
            "SELECT json_extract(request_headers, '$.authorization') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(auth, "Bearer supersecret");

    let cookie_value: String = conn
        .query_row(
            "SELECT json_extract(request_cookies, '$[0].value') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cookie_value, "sess123");
}

#[test]
fn test_redact_output_database_keeps_input_unchanged() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let out_db_path = tmp.path().join("redacted.db");

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["redact", "--output"])
        .arg(&out_db_path)
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Redacted"));

    assert!(out_db_path.exists());

    let conn_in = rusqlite::Connection::open(&db_path).unwrap();
    let conn_out = rusqlite::Connection::open(&out_db_path).unwrap();

    let in_auth: String = conn_in
        .query_row(
            "SELECT json_extract(request_headers, '$.authorization') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(in_auth, "Bearer supersecret");

    let out_auth: String = conn_out
        .query_row(
            "SELECT json_extract(request_headers, '$.authorization') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(out_auth, "REDACTED");

    let out_set_cookie: String = conn_out
        .query_row(
            "SELECT json_extract(response_headers, '$.\"set-cookie\"') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(out_set_cookie, "REDACTED");

    let out_cookie_value: String = conn_out
        .query_row(
            "SELECT json_extract(response_cookies, '$[0].value') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(out_cookie_value, "REDACTED");

    let accept: String = conn_out
        .query_row(
            "SELECT json_extract(request_headers, '$.accept') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(accept, "application/json");
}

#[test]
fn test_query_limit_offset_wraps_query() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args([
            "query",
            "SELECT id FROM entries ORDER BY id",
            "--format",
            "json",
            "--limit",
            "1",
            "--offset",
            "1",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\":2"))
        .stdout(predicate::str::contains("\"id\":1").not());
}

#[test]
fn test_query_table_null_large_and_quiet() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args([
            "query",
            "SELECT NULL AS n, replace(printf('%250s',''), ' ', 'a') AS big",
            "--format",
            "table",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("NULL"))
        .stdout(predicate::str::contains("..."));

    harlite()
        .args([
            "query",
            "SELECT host, status FROM entries ORDER BY id LIMIT 1",
            "--format",
            "table",
            "--quiet",
        ])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("-+-").not());
}

#[test]
fn test_query_invalid_sql_and_multiple_statements_fail() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["query", "SELCT 1", "--format", "csv"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("syntax"));

    harlite()
        .args(["query", "SELECT 1; SELECT 2", "--format", "csv"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Only a single SQL statement is allowed",
        ));
}

#[test]
fn test_text_only_filter() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "--bodies",
            "--text-only",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let blob_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert!(blob_count > 0);
}

#[test]
fn test_missing_file() {
    harlite()
        .args(["import", "nonexistent.har"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_schema_from_database() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .arg("schema")
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"));
}

#[test]
fn test_headers_as_json() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let accept: String = conn
        .query_row(
            "SELECT json_extract(request_headers, '$.accept') FROM entries WHERE method = 'GET'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(accept, "application/json");
}

#[test]
fn test_export_roundtrip_with_bodies() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("export.har");
    let roundtrip_db_path = tmp.path().join("roundtrip.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["--bodies", "-o"])
        .arg(&har_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 2 entries"));

    assert!(har_path.exists());

    harlite()
        .args(["import", "--bodies"])
        .arg(&har_path)
        .args(["-o"])
        .arg(&roundtrip_db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Imported 2 entries"));

    let conn = rusqlite::Connection::open(&roundtrip_db_path).unwrap();
    let entry_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(entry_count, 2);

    let blob_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
        .unwrap();
    assert!(blob_count > 0);
}

#[test]
fn test_export_with_raw_response_bodies() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("export.har");

    harlite()
        .args([
            "import",
            "tests/fixtures/gzip_response.har",
            "--bodies",
            "--decompress-bodies",
            "--keep-compressed",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export", "--bodies-raw"])
        .arg(&db_path)
        .args(["-o"])
        .arg(&har_path)
        .assert()
        .success();

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();
    let entry0 = &exported["log"]["entries"][0];
    let content = &entry0["response"]["content"];
    let text = content["text"].as_str().unwrap_or("");
    let encoding = content["encoding"].as_str();
    let decoded = match encoding {
        Some("base64") => STANDARD.decode(text).unwrap(),
        _ => text.as_bytes().to_vec(),
    };

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let raw_blob: Vec<u8> = conn
        .query_row(
            "SELECT b.content FROM entries e JOIN blobs b ON e.response_body_hash_raw = b.hash LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(decoded, raw_blob);

    let body_size = entry0["response"]["bodySize"].as_i64().unwrap_or(-1);
    assert_eq!(body_size, raw_blob.len() as i64);

    if let Some(compression) = content["compression"].as_i64() {
        let size = content["size"].as_i64().unwrap_or(body_size);
        assert_eq!(compression, size - body_size);
    }
}

#[test]
fn test_export_without_bodies_does_not_include_text() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("export.har");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["-o"])
        .arg(&har_path)
        .assert()
        .success();

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();

    let entry0 = &exported["log"]["entries"][0];
    assert!(entry0["response"]["content"]["text"].is_null());
    assert!(entry0["request"]["postData"]["text"].is_null());
}

#[test]
fn test_export_preserves_ordering_by_started_at() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("export.har");

    harlite()
        .args(["import", "tests/fixtures/out_of_order.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["-o"])
        .arg(&har_path)
        .assert()
        .success();

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();
    let entries = exported["log"]["entries"].as_array().unwrap();

    let first = entries[0]["startedDateTime"].as_str().unwrap();
    let second = entries[1]["startedDateTime"].as_str().unwrap();
    assert!(first < second);
    assert!(entries[0]["request"]["url"]
        .as_str()
        .unwrap()
        .ends_with("/first"));
}

#[test]
fn test_export_time_range_filters() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("export.har");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["--from", "2024-01-15T10:30:01.000Z", "-o"])
        .arg(&har_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();
    let entries = exported["log"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]["startedDateTime"].as_str().unwrap(),
        "2024-01-15T10:30:01.000Z"
    );

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["--to", "2024-01-15T10:30:00.000Z", "-o"])
        .arg(&har_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();
    let entries = exported["log"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]["startedDateTime"].as_str().unwrap(),
        "2024-01-15T10:30:00.000Z"
    );
}

#[test]
fn test_export_filters() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("filtered.har");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["--method", "GET", "-o"])
        .arg(&har_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();
    let entries = exported
        .get("log")
        .and_then(|l| l.get("entries"))
        .and_then(|e| e.as_array())
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]
            .get("request")
            .and_then(|r| r.get("method"))
            .and_then(|m| m.as_str())
            .unwrap(),
        "GET"
    );
}

#[test]
fn test_export_pages_namespaced_for_multi_import() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("export.har");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "tests/fixtures/with_pages.har",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["-o"])
        .arg(&har_path)
        .assert()
        .success();

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&har_path).unwrap()).unwrap();

    let pages = exported
        .get("log")
        .and_then(|l| l.get("pages"))
        .and_then(|p| p.as_array())
        .unwrap();
    assert!(!pages.is_empty());

    let page_id = pages[0].get("id").and_then(|v| v.as_str()).unwrap();
    assert!(page_id.contains(':'));

    let entries = exported
        .get("log")
        .and_then(|l| l.get("entries"))
        .and_then(|e| e.as_array())
        .unwrap();
    let pageref = entries
        .iter()
        .find_map(|e| e.get("pageref").and_then(|v| v.as_str()))
        .unwrap();
    assert_eq!(pageref, page_id);
}

#[test]
fn test_export_filter_by_source() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("src.db");
    let har_path = tmp.path().join("filtered.har");

    harlite()
        .args([
            "import",
            "tests/fixtures/simple.har",
            "tests/fixtures/with_pages.har",
            "-o",
        ])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export"])
        .arg(&db_path)
        .args(["--source", "with_pages.har", "-o"])
        .arg(&har_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));
}

#[test]
fn test_redact_no_defaults_with_regex_mode() {
    // When using regex mode without --no-defaults, no patterns should be applied
    // since defaults are wildcard patterns
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    // With regex mode and no explicit patterns, should fail because defaults aren't applied
    harlite()
        .args(["redact", "--match", "regex"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("No redaction patterns provided"));
}

#[test]
fn test_redact_no_defaults_with_exact_mode() {
    // When using exact mode without --no-defaults, no patterns should be applied
    // since defaults are wildcard patterns
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    // With exact mode and no explicit patterns, should fail because defaults aren't applied
    harlite()
        .args(["redact", "--match", "exact"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("No redaction patterns provided"));
}

#[test]
fn test_redact_defaults_with_wildcard_mode() {
    // Wildcard mode (default) should apply defaults
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    // With wildcard mode (default), defaults should be applied
    harlite()
        .args(["redact"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Redacted"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    // Verify authorization header was redacted
    let auth: String = conn
        .query_row(
            "SELECT json_extract(request_headers, '$.authorization') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(auth, "REDACTED");
}

#[test]
fn test_redact_with_explicit_regex_patterns() {
    // Regex mode with explicit patterns should work
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    // Use regex mode with explicit pattern
    harlite()
        .args(["redact", "--match", "regex", "--header", "^author.*"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Redacted"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();

    // Verify authorization header was redacted
    let auth: String = conn
        .query_row(
            "SELECT json_extract(request_headers, '$.authorization') FROM entries",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(auth, "REDACTED");
}
