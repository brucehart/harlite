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
