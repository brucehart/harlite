use assert_cmd::cargo::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
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
