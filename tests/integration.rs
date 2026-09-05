use assert_cmd::cargo::cargo_bin_cmd;
use assert_cmd::Command;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use predicates::prelude::*;
use serde_json::json;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
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
fn test_cdp_help() {
    harlite()
        .args(["cdp", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--host"))
        .stdout(predicate::str::contains("--port"))
        .stdout(predicate::str::contains("--duration"));
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
    let graphql_fields_inserted = conn
        .execute(
            "INSERT INTO graphql_fields (entry_id, field) SELECT id, 'viewer' FROM entries WHERE import_id = ?1 LIMIT 1",
            [import_id],
        )
        .unwrap();
    assert_eq!(graphql_fields_inserted, 1);
    drop(conn);

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
    let graphql_field_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM graphql_fields", [], |r| r.get(0))
        .unwrap();

    assert_eq!(entry_count, 0);
    assert_eq!(import_count, 0);
    assert_eq!(page_count, 0);
    assert_eq!(blob_count, 0);
    assert_eq!(graphql_field_count, 0);
}

#[test]
fn test_prune_does_not_delete_untrusted_external_path_by_default() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let protected_path = tmp.path().join("must-survive.txt");
    fs::write(&protected_path, b"protected").unwrap();

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_id: i64 = conn
        .query_row("SELECT id FROM imports LIMIT 1", [], |row| row.get(0))
        .unwrap();
    conn.execute(
        "UPDATE blobs SET external_path=?1 WHERE hash=(SELECT hash FROM blobs LIMIT 1)",
        [protected_path.to_string_lossy().as_ref()],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args(["prune", "--import-id", &import_id.to_string()])
        .arg(&db_path)
        .assert()
        .success();
    assert!(protected_path.exists());
}

#[test]
fn test_prune_external_file_deletion_waits_for_commit() {
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
        .args(["-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_id: i64 = conn
        .query_row("SELECT id FROM imports LIMIT 1", [], |row| row.get(0))
        .unwrap();
    let external_paths: Vec<String> = conn
        .prepare("SELECT external_path FROM blobs WHERE external_path IS NOT NULL")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert!(!external_paths.is_empty());
    conn.execute_batch(
        "CREATE TRIGGER fail_blob_delete BEFORE DELETE ON blobs BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
    )
    .unwrap();
    drop(conn);

    harlite()
        .args([
            "prune",
            "--import-id",
            &import_id.to_string(),
            "--allow-external-paths",
            "--external-path-root",
        ])
        .arg(&bodies_dir)
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("blocked"));

    assert!(external_paths
        .iter()
        .all(|path| std::path::Path::new(path).exists()));
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let entries: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(entries, 2);
}

#[test]
fn test_prune_preserves_external_file_referenced_by_surviving_blob() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let bodies_dir = tmp.path().join("bodies");
    let shared_path = bodies_dir.join("shared-body");
    fs::create_dir(&bodies_dir).unwrap();
    fs::write(&shared_path, b"shared").unwrap();

    for _ in 0..2 {
        harlite()
            .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
            .arg(&db_path)
            .assert()
            .success();
    }

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_ids: Vec<i64> = conn
        .prepare("SELECT id FROM imports ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(import_ids.len(), 2);
    let shared = shared_path.to_string_lossy();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type, external_path) VALUES('orphan', X'', 6, 'text/plain', ?1)",
        [shared.as_ref()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type, external_path) VALUES('keeper', X'', 6, 'text/plain', ?1)",
        [shared.as_ref()],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash='orphan', request_body_size=6 WHERE id=(SELECT MIN(id) FROM entries WHERE import_id=?1)",
        [import_ids[0]],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash='keeper', request_body_size=6 WHERE id=(SELECT MIN(id) FROM entries WHERE import_id=?1)",
        [import_ids[1]],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args([
            "prune",
            "--import-id",
            &import_ids[0].to_string(),
            "--allow-external-paths",
            "--external-path-root",
        ])
        .arg(&bodies_dir)
        .arg(&db_path)
        .assert()
        .success();

    assert!(shared_path.exists());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let surviving_refs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM entries WHERE request_body_hash='keeper'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(surviving_refs, 1);
}

#[test]
fn test_prune_rolls_back_on_malformed_surviving_external_path() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let bodies_dir = tmp.path().join("bodies");
    let shared_path = bodies_dir.join("shared-body");
    fs::create_dir(&bodies_dir).unwrap();
    fs::write(&shared_path, b"shared").unwrap();

    for _ in 0..2 {
        harlite()
            .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
            .arg(&db_path)
            .assert()
            .success();
    }

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_ids: Vec<i64> = conn
        .prepare("SELECT id FROM imports ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let shared = shared_path.to_string_lossy();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type, external_path) VALUES('orphan', X'', 6, 'text/plain', ?1)",
        [shared.as_ref()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type, external_path) VALUES('keeper', X'', 6, 'text/plain', CAST(?1 AS BLOB))",
        [shared.as_ref()],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash='orphan', request_body_size=6 WHERE id=(SELECT MIN(id) FROM entries WHERE import_id=?1)",
        [import_ids[0]],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash='keeper', request_body_size=6 WHERE id=(SELECT MIN(id) FROM entries WHERE import_id=?1)",
        [import_ids[1]],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args([
            "prune",
            "--import-id",
            &import_ids[0].to_string(),
            "--allow-external-paths",
            "--external-path-root",
        ])
        .arg(&bodies_dir)
        .arg(&db_path)
        .assert()
        .failure();

    assert!(shared_path.exists());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |row| row.get(0))
        .unwrap();
    let orphan_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM blobs WHERE hash='orphan'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(import_count, 2);
    assert_eq!(orphan_count, 1);
}

#[test]
fn test_prune_rolls_back_on_malformed_pruned_external_path() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let bodies_dir = tmp.path().join("bodies");
    let external_path = bodies_dir.join("external-body");
    fs::create_dir(&bodies_dir).unwrap();
    fs::write(&external_path, b"external").unwrap();

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_id: i64 = conn
        .query_row("SELECT id FROM imports LIMIT 1", [], |row| row.get(0))
        .unwrap();
    let external = external_path.to_string_lossy();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type, external_path) VALUES('malformed-path', X'', 8, 'text/plain', CAST(?1 AS BLOB))",
        [external.as_ref()],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash='malformed-path', request_body_size=8 WHERE id=(SELECT MIN(id) FROM entries)",
        [],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args([
            "prune",
            "--import-id",
            &import_id.to_string(),
            "--allow-external-paths",
            "--external-path-root",
        ])
        .arg(&bodies_dir)
        .arg(&db_path)
        .assert()
        .failure();

    assert!(external_path.exists());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |row| row.get(0))
        .unwrap();
    let blob_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM blobs WHERE hash='malformed-path'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(import_count, 1);
    assert_eq!(blob_count, 1);
}

#[test]
fn test_prune_rolls_back_on_malformed_entry_hash() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_id: i64 = conn
        .query_row("SELECT id FROM imports LIMIT 1", [], |row| row.get(0))
        .unwrap();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type) VALUES(CAST('malformed-hash' AS BLOB), X'', 0, 'text/plain')",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash=CAST('malformed-hash' AS BLOB) WHERE id=(SELECT MIN(id) FROM entries)",
        [],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args(["prune", "--import-id", &import_id.to_string()])
        .arg(&db_path)
        .assert()
        .failure();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |row| row.get(0))
        .unwrap();
    let entry_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(import_count, 1);
    assert_eq!(entry_count, 2);
}

#[cfg(unix)]
#[test]
fn test_project_config_cannot_implicitly_execute_plugin() {
    let tmp = TempDir::new().unwrap();
    let marker = tmp.path().join("plugin-ran");
    let config = format!(
        "[[plugins]]\nname='untrusted'\nkind='filter'\ncommand='/usr/bin/touch'\nargs=['{}']\nenabled=true\n",
        marker.display()
    );
    fs::write(tmp.path().join("harlite.toml"), config).unwrap();
    let har_path = fs::canonicalize("tests/fixtures/simple.har").unwrap();

    harlite()
        .current_dir(tmp.path())
        .arg("import")
        .arg(har_path)
        .args(["-o", "test.db"])
        .assert()
        .success();
    assert!(!marker.exists());
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
fn test_export_data_jsonl() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let out_path = tmp.path().join("entries.jsonl");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export-data", "--format", "jsonl", "-o"])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success();

    let mut contents = String::new();
    fs::File::open(&out_path)
        .unwrap()
        .read_to_string(&mut contents)
        .unwrap();
    let first_line = contents.lines().next().unwrap_or("");
    let parsed: serde_json::Value = serde_json::from_str(first_line).unwrap();
    assert!(parsed.get("url").is_some());
}

#[cfg(feature = "parquet")]
#[test]
fn test_export_data_parquet() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let out_path = tmp.path().join("entries.parquet");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    harlite()
        .args(["export-data", "--format", "parquet", "-o"])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success();

    let bytes = fs::read(&out_path).unwrap();
    assert!(bytes.starts_with(b"PAR1"));
    assert!(bytes.ends_with(b"PAR1"));
}

#[test]
fn test_export_data_source_filter_no_match() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let out_path = tmp.path().join("entries.jsonl");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args([
            "export-data",
            "--format",
            "jsonl",
            "--source",
            "missing.har",
            "-o",
        ])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success();

    let contents = fs::read_to_string(&out_path).unwrap_or_default();
    assert!(contents.trim().is_empty());
}

#[test]
fn test_openapi_basic() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let out_path = tmp.path().join("openapi.json");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["openapi", "-o"])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success();

    let mut contents = String::new();
    fs::File::open(&out_path)
        .unwrap()
        .read_to_string(&mut contents)
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert_eq!(
        parsed.get("openapi").and_then(|v| v.as_str()),
        Some("3.0.3")
    );
    assert!(parsed.get("paths").is_some());
}

#[test]
fn test_replay_har_with_override() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let mut read_total = 0usize;
            loop {
                match stream.read(&mut buf[read_total..]) {
                    Ok(0) => break,
                    Ok(n) => {
                        read_total += n;
                        if read_total >= 4 && buf[..read_total].windows(4).any(|w| w == b"\r\n\r\n")
                        {
                            break;
                        }
                        if read_total >= buf.len() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            let response =
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\n\r\nOK";
            let _ = stream.write_all(response);
        }
    });

    let tmp = TempDir::new().unwrap();
    let har_path = tmp.path().join("replay.har");

    let har = json!({
        "log": {
            "version": "1.2",
            "creator": { "name": "harlite", "version": "0.0" },
            "entries": [
                {
                    "startedDateTime": "2024-01-01T00:00:00.000Z",
                    "time": 1.0,
                    "request": {
                        "method": "GET",
                        "url": "http://example.com/test",
                        "httpVersion": "HTTP/1.1",
                        "headers": [],
                        "cookies": [],
                        "queryString": [],
                        "headersSize": -1,
                        "bodySize": -1
                    },
                    "response": {
                        "status": 200,
                        "statusText": "OK",
                        "httpVersion": "HTTP/1.1",
                        "headers": [],
                        "cookies": [],
                        "content": { "size": 0, "mimeType": "text/plain" },
                        "redirectURL": "",
                        "headersSize": -1,
                        "bodySize": 0
                    },
                    "cache": {},
                    "timings": { "send": 0, "wait": 1, "receive": 0 }
                }
            ]
        }
    });

    fs::write(&har_path, serde_json::to_vec(&har).unwrap()).unwrap();

    let override_host = format!(".*=127.0.0.1:{}", addr.port());
    harlite()
        .args([
            "replay",
            "--format",
            "json",
            "--method",
            "GET",
            "--concurrency",
            "1",
        ])
        .arg(&har_path)
        .args(["--override-host", &override_host])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"status_replay\":200"))
        .stdout(predicate::str::contains("\"status_changed\":false"));

    let _ = handle.join();
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
fn test_merge_dedup_identical_db() {
    let tmp = TempDir::new().unwrap();
    let db1 = tmp.path().join("first.db");
    let db2 = tmp.path().join("second.db");
    let merged = tmp.path().join("merged.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db1)
        .assert()
        .success();

    fs::copy(&db1, &db2).unwrap();

    harlite()
        .args(["merge"])
        .arg(&db1)
        .arg(&db2)
        .args(["-o"])
        .arg(&merged)
        .assert()
        .success()
        .stdout(predicate::str::contains("Merged 2 databases"));

    let conn = rusqlite::Connection::open(&merged).unwrap();
    let entry_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    let import_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM imports", [], |r| r.get(0))
        .unwrap();

    assert_eq!(entry_count, 2);
    assert_eq!(import_count, 1);
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
fn test_diff_sqlite_extension_detection() {
    let tmp = TempDir::new().unwrap();
    let db_left = tmp.path().join("left.sqlite3");
    let db_right = tmp.path().join("right.sqlite3");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_left)
        .assert()
        .success();

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_right)
        .assert()
        .success();

    harlite()
        .args([
            "diff",
            db_left.to_str().unwrap(),
            db_right.to_str().unwrap(),
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("[]"));
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
fn test_fts_rebuild_restores_missing_index() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("DROP TABLE response_body_fts;").unwrap();
    drop(conn);

    harlite()
        .args(["search", "Alice"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("FTS index not found"));

    harlite()
        .args(["fts-rebuild"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Rebuilt response body FTS index"));

    harlite()
        .args(["search", "Alice"])
        .arg(&db_path)
        .assert()
        .success()
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
fn test_analyze_command_json() {
    let tmp = TempDir::new().unwrap();
    let har_path = tmp.path().join("analyze.har");
    let db_path = tmp.path().join("analyze.db");

    let har = json!({
        "log": {
            "version": "1.2",
            "creator": { "name": "harlite", "version": "0.0" },
            "entries": [
                {
                    "startedDateTime": "2024-01-01T00:00:00.000Z",
                    "time": 1200.0,
                    "request": {
                        "method": "GET",
                        "url": "http://example.com/api",
                        "httpVersion": "HTTP/1.1",
                        "headers": [],
                        "cookies": [],
                        "queryString": [],
                        "headersSize": -1,
                        "bodySize": -1
                    },
                    "response": {
                        "status": 200,
                        "statusText": "OK",
                        "httpVersion": "HTTP/1.1",
                        "headers": [
                            { "name": "Content-Type", "value": "application/json" }
                        ],
                        "cookies": [],
                        "content": { "size": 100, "mimeType": "application/json" },
                        "redirectURL": "",
                        "headersSize": -1,
                        "bodySize": 100
                    },
                    "cache": {},
                    "connection": "conn-1",
                    "timings": { "blocked": 50, "dns": 20, "connect": 100, "send": 10, "wait": 800, "receive": 220, "ssl": 80 }
                },
                {
                    "startedDateTime": "2024-01-01T00:00:01.000Z",
                    "time": 100.0,
                    "request": {
                        "method": "GET",
                        "url": "http://example.com/api",
                        "httpVersion": "HTTP/1.1",
                        "headers": [],
                        "cookies": [],
                        "queryString": [],
                        "headersSize": -1,
                        "bodySize": -1
                    },
                    "response": {
                        "status": 200,
                        "statusText": "OK",
                        "httpVersion": "HTTP/1.1",
                        "headers": [],
                        "cookies": [],
                        "content": { "size": 50, "mimeType": "application/json" },
                        "redirectURL": "",
                        "headersSize": -1,
                        "bodySize": 50
                    },
                    "cache": {},
                    "connection": "conn-1",
                    "timings": { "send": 5, "wait": 50, "receive": 45 }
                }
            ]
        }
    });

    fs::write(&har_path, serde_json::to_vec(&har).unwrap()).unwrap();

    harlite()
        .args(["import"])
        .arg(&har_path)
        .args(["-o"])
        .arg(&db_path)
        .assert()
        .success();

    let output = harlite()
        .args(["analyze", "--json"])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let v: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(v["entries"], 2);
    assert_eq!(v["aggregates"]["total_ms"]["count"], 2);
    assert_eq!(v["slow_requests"]["total_count"], 1);
    assert_eq!(v["connection_reuse"]["requests_with_connection_id"], 2);
    assert_eq!(v["connection_reuse"]["unique_connection_ids"], 1);
    assert_eq!(v["cache_candidates"]["unique_urls"], 1);
    assert_eq!(v["cache_candidates"]["total_requests"], 2);
}

#[test]
fn test_report_from_db_generates_html() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("report.db");
    let html_path = tmp.path().join("report.html");

    harlite()
        .args(["import", "tests/fixtures/with_pages.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["report"])
        .arg(&db_path)
        .args(["-o"])
        .arg(&html_path)
        .assert()
        .success();

    let html = fs::read_to_string(&html_path).unwrap();
    assert!(html.contains("id=\"harlite-data\""));
    assert!(html.contains("Slow Requests"));
    assert!(html.contains("Error Summary"));
    assert!(html.contains("Waterfall"));
}

#[test]
fn test_report_from_har_generates_html() {
    let tmp = TempDir::new().unwrap();
    let html_path = tmp.path().join("report.html");

    harlite()
        .args(["report", "tests/fixtures/with_pages.har", "-o"])
        .arg(&html_path)
        .assert()
        .success();

    let html = fs::read_to_string(&html_path).unwrap();
    assert!(html.contains("id=\"harlite-data\""));
    assert!(html.contains("Slow Requests"));
    assert!(html.contains("Error Summary"));
    assert!(html.contains("Waterfall"));
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
fn test_redact_output_physically_removes_superseded_secrets() {
    let tmp = TempDir::new().unwrap();
    let har_path = tmp.path().join("sensitive.har");
    let db_path = tmp.path().join("source.db");
    let out_path = tmp.path().join("redacted.db");
    let header_secret = format!("Bearer {}", "UNIQUE_HEADER_SECRET_".repeat(256));
    let body_secret = "UNIQUE_BODY_SECRET_".repeat(256);
    let har = json!({
        "log": {
            "version": "1.2",
            "creator": { "name": "test", "version": "1" },
            "entries": [{
                "startedDateTime": "2024-01-01T00:00:00.000Z",
                "time": 1,
                "request": {
                    "method": "POST",
                    "url": "https://example.test/submit",
                    "httpVersion": "HTTP/1.1",
                    "headers": [{ "name": "Authorization", "value": header_secret }],
                    "cookies": [],
                    "queryString": [],
                    "postData": { "mimeType": "text/plain", "text": body_secret },
                    "headersSize": -1,
                    "bodySize": -1
                },
                "response": {
                    "status": 200,
                    "statusText": "OK",
                    "httpVersion": "HTTP/1.1",
                    "headers": [],
                    "cookies": [],
                    "content": { "size": 0, "mimeType": "text/plain", "text": "" },
                    "redirectURL": "",
                    "headersSize": -1,
                    "bodySize": 0
                },
                "cache": {},
                "timings": { "send": 0, "wait": 1, "receive": 0 }
            }]
        }
    });
    fs::write(&har_path, serde_json::to_vec(&har).unwrap()).unwrap();

    harlite()
        .args(["import", "--bodies", "-o"])
        .arg(&db_path)
        .arg(&har_path)
        .assert()
        .success();
    harlite()
        .args(["redact", "--body-regex", "UNIQUE_BODY_SECRET_", "--output"])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let old_blob_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM blobs WHERE CAST(content AS TEXT) LIKE '%UNIQUE_BODY_SECRET_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(old_blob_count, 0);
    drop(conn);

    let raw = fs::read(&out_path).unwrap();
    assert!(!raw
        .windows(b"UNIQUE_HEADER_SECRET_".len())
        .any(|window| window == b"UNIQUE_HEADER_SECRET_"));
    assert!(!raw
        .windows(b"UNIQUE_BODY_SECRET_".len())
        .any(|window| window == b"UNIQUE_BODY_SECRET_"));
}

#[test]
fn test_redact_ignores_untrusted_external_blob_paths() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");
    let out_path = tmp.path().join("redacted.db");
    let protected_path = tmp.path().join("private.txt");
    fs::write(&protected_path, b"UNTRUSTED_EXTERNAL_SECRET").unwrap();

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO blobs(hash, content, size, mime_type, external_path) VALUES('attacker', X'', 25, 'text/plain', ?1)",
        [protected_path.to_string_lossy().as_ref()],
    )
    .unwrap();
    conn.execute(
        "UPDATE entries SET request_body_hash='attacker', request_body_size=25 WHERE id=(SELECT id FROM entries LIMIT 1)",
        [],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args([
            "redact",
            "--no-defaults",
            "--body-regex",
            "UNTRUSTED_EXTERNAL_SECRET",
            "--output",
        ])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let hash: String = conn
        .query_row("SELECT request_body_hash FROM entries LIMIT 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    let content: Vec<u8> = conn
        .query_row(
            "SELECT content FROM blobs WHERE hash='attacker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(hash, "attacker");
    assert!(content.is_empty());
}

#[test]
fn test_pii_redaction_physically_removes_superseded_body() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");
    let out_path = tmp.path().join("redacted.db");
    let secret = "physical-secret@example.com";
    fs::write(&out_path, b"previous output").unwrap();

    harlite()
        .args(["import", "tests/fixtures/redact.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "UPDATE blobs SET content=?1, size=?2 WHERE hash=(SELECT response_body_hash FROM entries LIMIT 1)",
        rusqlite::params![secret.as_bytes(), secret.len() as i64],
    )
    .unwrap();
    drop(conn);

    harlite()
        .args(["pii", "--redact", "--format", "json", "--force", "--output"])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("email"));

    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let old_blob_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM blobs WHERE CAST(content AS TEXT) LIKE '%physical-secret@example.com%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(old_blob_count, 0);
    drop(conn);
    let raw = fs::read(&out_path).unwrap();
    assert!(!raw
        .windows(secret.len())
        .any(|window| window == secret.as_bytes()));
}

#[test]
fn test_pii_redaction_rolls_back_every_entry_on_failure() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let original_hashes: Vec<String> = conn
        .prepare("SELECT response_body_hash FROM entries ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(original_hashes.len(), 2);
    conn.execute(
        "UPDATE blobs SET content=?1, size=?2 WHERE hash=?3",
        rusqlite::params![
            b"first@example.com".as_slice(),
            "first@example.com".len() as i64,
            original_hashes[0]
        ],
    )
    .unwrap();
    conn.execute(
        "UPDATE blobs SET content=?1, size=?2 WHERE hash=?3",
        rusqlite::params![
            b"second@example.com".as_slice(),
            "second@example.com".len() as i64,
            original_hashes[1]
        ],
    )
    .unwrap();
    let blobs_before: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_second_pii_update BEFORE UPDATE ON entries WHEN OLD.id=2 BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
    )
    .unwrap();
    drop(conn);

    harlite()
        .args(["pii", "--redact", "--format", "json"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("blocked"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let hashes_after: Vec<String> = conn
        .prepare("SELECT response_body_hash FROM entries ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    let blobs_after: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(hashes_after, original_hashes);
    assert_eq!(blobs_after, blobs_before);
}

#[test]
fn test_pii_redaction_rolls_back_when_orphan_cleanup_fails() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let original_hash: String = conn
        .query_row(
            "SELECT response_body_hash FROM entries ORDER BY id LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE blobs SET content=?1, size=?2 WHERE hash=?3",
        rusqlite::params![
            b"cleanup@example.com".as_slice(),
            "cleanup@example.com".len() as i64,
            original_hash.as_str()
        ],
    )
    .unwrap();
    let blobs_before: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_pii_blob_cleanup BEFORE DELETE ON blobs BEGIN SELECT RAISE(ABORT, 'cleanup blocked'); END;",
    )
    .unwrap();
    drop(conn);

    harlite()
        .args(["pii", "--redact", "--format", "json"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("cleanup blocked"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let hash_after: String = conn
        .query_row(
            "SELECT response_body_hash FROM entries ORDER BY id LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let blobs_after: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(hash_after, original_hash);
    assert_eq!(blobs_after, blobs_before);
}

#[test]
fn test_redact_rolls_back_when_orphan_cleanup_fails() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");

    harlite()
        .args(["import", "tests/fixtures/redact.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let original: (String, String) = conn
        .query_row(
            "SELECT request_headers, response_body_hash FROM entries LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let blobs_before: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_redact_blob_cleanup BEFORE DELETE ON blobs BEGIN SELECT RAISE(ABORT, 'cleanup blocked'); END;",
    )
    .unwrap();
    drop(conn);

    harlite()
        .args(["redact", "--body-regex", "ok"])
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("cleanup blocked"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let after: (String, String) = conn
        .query_row(
            "SELECT request_headers, response_body_hash FROM entries LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let blobs_after: i64 = conn
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, original);
    assert_eq!(blobs_after, blobs_before);
}

#[test]
fn test_redact_failures_do_not_publish_output_database() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");
    let invalid_output = tmp.path().join("invalid-pattern.db");
    let runtime_output = tmp.path().join("runtime-failure.db");
    let previous_output = b"previous output";

    harlite()
        .args(["import", "tests/fixtures/redact.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    fs::write(&invalid_output, previous_output).unwrap();
    harlite()
        .args(["redact", "--body-regex", "[", "--force", "--output"])
        .arg(&invalid_output)
        .arg(&db_path)
        .assert()
        .failure();
    assert_eq!(fs::read(&invalid_output).unwrap(), previous_output);

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_redact_update BEFORE UPDATE ON entries BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
    )
    .unwrap();
    drop(conn);
    fs::write(&runtime_output, previous_output).unwrap();
    harlite()
        .args(["redact", "--force", "--output"])
        .arg(&runtime_output)
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("blocked"));
    assert_eq!(fs::read(&runtime_output).unwrap(), previous_output);
    assert!(!fs::read_dir(tmp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("harlite-stage")
    }));
}

#[test]
fn test_pii_failure_does_not_publish_output_database() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("source.db");
    let out_path = tmp.path().join("redacted.db");
    let previous_output = b"previous output";

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "UPDATE blobs SET content=?1, size=?2 WHERE hash=(SELECT response_body_hash FROM entries ORDER BY id LIMIT 1)",
        rusqlite::params![b"secret@example.com".as_slice(), "secret@example.com".len() as i64],
    )
    .unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_pii_update BEFORE UPDATE ON entries BEGIN SELECT RAISE(ABORT, 'blocked'); END;",
    )
    .unwrap();
    drop(conn);
    fs::write(&out_path, previous_output).unwrap();

    harlite()
        .args(["pii", "--redact", "--format", "json", "--force", "--output"])
        .arg(&out_path)
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("blocked"));
    assert_eq!(fs::read(&out_path).unwrap(), previous_output);
    assert!(!fs::read_dir(tmp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("harlite-stage")
    }));
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
fn test_export_direct_har_filter() {
    let tmp = TempDir::new().unwrap();
    let filtered_path = tmp.path().join("filtered.har");

    harlite()
        .args([
            "export",
            "tests/fixtures/simple.har",
            "--method",
            "POST",
            "--bodies",
        ])
        .args(["-o"])
        .arg(&filtered_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&filtered_path).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 1);
    let method = entries[0]["request"]["method"]
        .as_str()
        .expect("method should be set");
    assert_eq!(method, "POST");
}

#[test]
fn test_export_har_filter_defaults_to_non_overwriting_output() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("capture.har");
    let expected_output = tmp.path().join("capture.filtered.har");

    fs::copy("tests/fixtures/simple.har", &input).unwrap();
    let original = fs::read_to_string(&input).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--method",
            "GET",
        ])
        .assert()
        .success();

    assert!(expected_output.exists());
    let preserved = fs::read_to_string(&input).unwrap();
    assert_eq!(preserved, original);
}

#[test]
fn test_export_db_defaults_to_current_directory_output() {
    let tmp = TempDir::new().unwrap();
    let stem = format!(
        "issue119-export-default-{}",
        tmp.path().file_name().unwrap().to_string_lossy()
    );
    let db_path = tmp.path().join(format!("{stem}.db"));
    let expected_output = std::env::current_dir().unwrap().join(format!("{stem}.har"));
    let _ = fs::remove_file(&expected_output);
    let output_in_db_dir = tmp.path().join(format!("{stem}.har"));

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["export", &db_path.to_string_lossy()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 2 entries"));

    assert!(expected_output.exists());
    assert!(!output_in_db_dir.exists());

    let _ = fs::remove_file(&expected_output);
}

#[test]
fn test_export_har_url_contains_is_case_insensitive() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("case-insensitive.har");
    let output = tmp.path().join("case-insensitive.filtered.har");
    let har = r#"{
      "log": {
        "version": "1.2",
        "creator": {"name": "test", "version": "1.0"},
        "entries": [
          {
            "startedDateTime": "2024-01-15T10:30:00.000Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/Users",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 0,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          },
          {
            "startedDateTime": "2024-01-15T10:30:01.000Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/notes",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 0,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          }
        ]
      }
    }"#;

    fs::write(&input, har).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--url-contains",
            "users",
            "--method",
            "GET",
        ])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]["request"]["url"]
            .as_str()
            .expect("url should be set"),
        "https://api.example.com/Users"
    );
}

#[test]
fn test_export_har_omits_form_params_without_bodies() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("form-param.har");
    let output = tmp.path().join("form-param.filtered.har");
    let har = r#"{
      "log": {
        "version": "1.2",
        "creator": {"name": "test", "version": "1.0"},
        "entries": [
          {
            "startedDateTime": "2024-01-15T10:30:00.000Z",
            "time": 10,
            "request": {
              "method": "POST",
              "url": "https://api.example.com/login",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "postData": {
                "mimeType": "application/x-www-form-urlencoded",
                "text": "username=bob&password=secret",
                "params": [
                  {"name": "username", "value": "bob"},
                  {"name": "password", "value": "secret"}
                ]
              },
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 13,
                "mimeType": "text/plain",
                "text": "logged in"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          }
        ]
      }
    }"#;

    fs::write(&input, har).unwrap();

    harlite()
        .args(["export", input.to_string_lossy().as_ref()])
        .args(["--method", "POST"])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entry = exported["log"]["entries"][0]
        .as_object()
        .expect("entry should be an object");
    let request = entry["request"]
        .as_object()
        .expect("request should be an object");
    let post_data = request["postData"]
        .as_object()
        .expect("postData should be set");
    assert!(post_data.get("text").is_none() || post_data["text"].is_null());
    assert!(post_data.get("params").is_none() || post_data["params"].is_null());
}

#[test]
fn test_export_har_unknown_response_size_treated_as_zero_for_filtering() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("unknown-size.har");
    let output = tmp.path().join("unknown-size.filtered.har");
    let har = r#"{
      "log": {
        "version": "1.2",
        "creator": {"name": "test", "version": "1.0"},
        "entries": [
          {
            "startedDateTime": "2024-01-15T10:30:00.000Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/known",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": -1,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          },
          {
            "startedDateTime": "2024-01-15T10:30:01.000Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/small",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 10,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          }
        ]
      }
    }"#;

    fs::write(&input, har).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--min-response-size",
            "0",
        ])
        .args(["--method", "GET"])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 2 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 2);
}

#[test]
fn test_export_har_invalid_request_size_still_applies_size_filters() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("invalid-request-size.har");
    let output = tmp.path().join("invalid-request-size.filtered.har");
    let har = r#"{
      "log": {
        "version": "1.2",
        "creator": {"name": "test", "version": "1.0"},
        "entries": [
          {
            "startedDateTime": "2024-01-15T10:30:00.000Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/unknown",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": -1
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 5,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          }
        ]
      }
    }"#;

    fs::write(&input, har).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--method",
            "GET",
            "--min-request-size",
            "0",
        ])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 1);
}

#[test]
fn test_export_har_source_matches_only_exact_path_or_basename() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("keep.har");
    let output = tmp.path().join("keep.filtered.har");

    fs::copy("tests/fixtures/simple.har", &input).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--source",
            "keep.har",
            "--method",
            "GET",
        ])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 1);

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--source",
            "eep.har",
            "--method",
            "GET",
        ])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 0 entries"));
}

#[test]
fn test_export_har_invalid_started_date_still_applies_size_filters() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("invalid-date.har");
    let output = tmp.path().join("invalid-date.filtered.har");
    let har = r#"{
      "log": {
        "version": "1.2",
        "creator": {"name": "test", "version": "1.0"},
        "entries": [
          {
            "startedDateTime": "not-a-date",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/invalid",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 5,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          },
          {
            "startedDateTime": "2024-01-15T10:30:01.000Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://api.example.com/valid",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 25,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          }
        ]
      }
    }"#;

    fs::write(&input, har).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--method",
            "GET",
            "--min-response-size",
            "10",
        ])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));
}

#[test]
fn test_export_with_format_override_on_extensionless_input() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("session");
    let output = tmp.path().join("session_filtered.har");

    fs::copy("tests/fixtures/simple.har", &input).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--format",
            "har",
            "--method",
            "GET",
        ])
        .args(["--bodies", "-o"])
        .arg(&output)
        .assert()
        .success()
        .stdout(predicate::str::contains("Exported 1 entries"));

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 1);
    let method = entries[0]["request"]["method"]
        .as_str()
        .expect("method should be set");
    assert_eq!(method, "GET");
}

#[test]
fn test_export_time_filter_with_timezone_offsets() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("timezone.har");
    let output = tmp.path().join("timezone.filtered.har");
    let har = r#"{
      "log": {
        "version": "1.2",
        "creator": {"name": "test", "version": "1.0"},
        "entries": [
          {
            "startedDateTime": "2024-01-01T00:00:00-05:00",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://example.com/early",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 0,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          },
          {
            "startedDateTime": "2024-01-01T00:00:00Z",
            "time": 10,
            "request": {
              "method": "GET",
              "url": "https://example.com/late",
              "httpVersion": "HTTP/1.1",
              "headers": [],
              "cookies": [],
              "queryString": [],
              "headersSize": 0,
              "bodySize": 0
            },
            "response": {
              "status": 200,
              "statusText": "OK",
              "httpVersion": "HTTP/1.1",
              "cookies": [],
              "headers": [],
              "content": {
                "size": 0,
                "mimeType": "application/json",
                "text": "{}"
              },
              "redirectURL": "",
              "headersSize": 0,
              "bodySize": 0
            },
            "cache": {},
            "timings": {"send": 1, "wait": 1, "receive": 1}
          }
        ]
      }
    }"#;

    fs::write(&input, har).unwrap();

    harlite()
        .args([
            "export",
            input.to_string_lossy().as_ref(),
            "--from",
            "2024-01-01T04:00:00Z",
        ])
        .args(["--format", "har"])
        .args(["-o"])
        .arg(&output)
        .assert()
        .success();

    let exported: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&output).unwrap()).unwrap();
    let entries = exported["log"]["entries"]
        .as_array()
        .expect("entries array should exist");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]["request"]["url"].as_str().unwrap(),
        "https://example.com/early"
    );
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
fn test_waterfall_trace_output() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("waterfall.db");

    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    let output = harlite()
        .args(["waterfall", "--format", "trace", "--group-by", "none"])
        .arg(&db_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let trace: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(trace["displayTimeUnit"], "ms");
    let events = trace["traceEvents"].as_array().unwrap();
    assert!(events.len() >= 3);
    let has_url = events.iter().any(|event| {
        event
            .get("args")
            .and_then(|args| args.get("url"))
            .and_then(|url| url.as_str())
            == Some("https://api.example.com/users")
    });
    assert!(has_url);
}

#[test]
fn test_waterfall_text_group_by_page() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("waterfall_pages.db");

    harlite()
        .args(["import", "tests/fixtures/with_pages.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();

    harlite()
        .args(["waterfall", "--format", "text", "--group-by", "page"])
        .arg(&db_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Group: Example Homepage"));
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

#[test]
fn test_check_validates_har_database_and_blob_hashes() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("check.db");
    let warning_path = tmp.path().join("warning.har");

    harlite()
        .args(["check", "tests/fixtures/simple.har", "--strict"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Valid: yes"));

    let mut warning_har: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    warning_har["log"]
        .as_object_mut()
        .unwrap()
        .remove("creator");
    fs::write(&warning_path, serde_json::to_vec(&warning_har).unwrap()).unwrap();
    harlite()
        .arg("check")
        .arg(&warning_path)
        .args(["--strict", "--json"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"valid\": false"))
        .stderr(predicate::str::contains("issue(s)"));

    let mut invalid_timing_har: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    invalid_timing_har["log"]["entries"][0]["timings"]["wait"] = json!(-0.5);
    fs::write(
        &warning_path,
        serde_json::to_vec(&invalid_timing_har).unwrap(),
    )
    .unwrap();
    harlite()
        .arg("check")
        .arg(&warning_path)
        .assert()
        .failure()
        .stdout(predicate::str::contains(
            "timing wait must be -1 or non-negative",
        ));

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    harlite()
        .arg("check")
        .arg(&db_path)
        .args(["--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"valid\": true"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "UPDATE response_body_fts SET body = 'stale indexed content' WHERE rowid = (SELECT MIN(rowid) FROM response_body_fts)",
        [],
    )
    .unwrap();
    drop(conn);
    harlite()
        .arg("check")
        .arg(&db_path)
        .assert()
        .failure()
        .stdout(predicate::str::contains("does not match blob content"));
    harlite()
        .arg("fts-rebuild")
        .arg(&db_path)
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO blobs (hash, content, size) VALUES ('not-a-blake3-hash', X'', 0)",
        [],
    )
    .unwrap();
    drop(conn);
    harlite()
        .arg("check")
        .arg(&db_path)
        .assert()
        .failure()
        .stdout(predicate::str::contains("content hash is"));

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute("DELETE FROM blobs WHERE hash = 'not-a-blake3-hash'", [])
        .unwrap();
    conn.execute(
        "UPDATE blobs SET content = X'636F7272757074' WHERE length(content) > 0",
        [],
    )
    .unwrap();
    drop(conn);

    harlite()
        .arg("check")
        .arg(&db_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Validation failed"));
}

#[test]
fn test_stdin_har_support_for_import_check_export_and_report() {
    let tmp = TempDir::new().unwrap();
    let input = fs::read("tests/fixtures/simple.har").unwrap();
    let db_path = tmp.path().join("stdin.db");
    let exported_path = tmp.path().join("filtered.har");
    let report_path = tmp.path().join("report.html");

    harlite()
        .args(["import", "-", "--output"])
        .arg(&db_path)
        .write_stdin(input.clone())
        .assert()
        .success();
    harlite()
        .args(["check", "-", "--strict"])
        .write_stdin(input.clone())
        .assert()
        .success();
    #[cfg(feature = "compression")]
    {
        let mut brotli_input = Vec::new();
        {
            let mut compressor = brotli::CompressorWriter::new(&mut brotli_input, 4096, 5, 22);
            compressor.write_all(&input).unwrap();
        }
        harlite()
            .args(["check", "-", "--strict"])
            .write_stdin(brotli_input)
            .assert()
            .success();
    }
    harlite()
        .args(["export", "-", "--output"])
        .arg(&exported_path)
        .write_stdin(input.clone())
        .assert()
        .success();
    harlite()
        .args(["report", "-", "--output"])
        .arg(&report_path)
        .write_stdin(input)
        .assert()
        .success();

    assert!(db_path.exists());
    assert!(exported_path.exists());
    assert!(report_path.exists());
}

#[test]
fn test_har_native_redaction_and_pii_redaction() {
    let tmp = TempDir::new().unwrap();
    let redact_out = tmp.path().join("safe.har");
    let form_redact_out = tmp.path().join("form-safe.har");
    let pii_input = tmp.path().join("pii.har");
    let pii_out = tmp.path().join("pii-safe.har");
    let encoded_pii_input = tmp.path().join("encoded-pii.har");
    let encoded_pii_out = tmp.path().join("encoded-pii-safe.har");
    let pii_db = tmp.path().join("pii.db");
    let pii_db_out = tmp.path().join("pii-safe.db");
    let relative_input = tmp.path().join("relative.har");
    let relative_out = tmp.path().join("relative-safe.har");

    harlite()
        .args(["redact", "tests/fixtures/redact.har", "--output"])
        .arg(&redact_out)
        .assert()
        .success();
    let redacted: serde_json::Value =
        serde_json::from_slice(&fs::read(&redact_out).unwrap()).unwrap();
    assert_eq!(
        redacted["log"]["entries"][0]["request"]["headers"][1]["value"],
        "REDACTED"
    );

    let mut relative: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/redact.har").unwrap()).unwrap();
    relative["log"]["entries"][0]["request"]["url"] = json!("/api?token=relative-url-secret");
    relative["log"]["entries"][0]["request"]["queryString"] =
        json!([{ "name": "token", "value": "relative-url-secret" }]);
    fs::write(&relative_input, serde_json::to_vec(&relative).unwrap()).unwrap();
    harlite()
        .arg("redact")
        .arg(&relative_input)
        .args([
            "--no-defaults",
            "--query-param",
            "token",
            "--match",
            "exact",
            "--output",
        ])
        .arg(&relative_out)
        .assert()
        .success();
    let relative_redacted: serde_json::Value =
        serde_json::from_slice(&fs::read(&relative_out).unwrap()).unwrap();
    assert_eq!(
        relative_redacted["log"]["entries"][0]["request"]["url"],
        "/api?token=REDACTED"
    );
    assert_eq!(
        relative_redacted["log"]["entries"][0]["request"]["queryString"][0]["value"],
        "REDACTED"
    );

    let mut pii: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/redact.har").unwrap()).unwrap();
    let encoded_response = "contact=alice%40example.com";
    pii["log"]["entries"][0]["response"]["content"]["mimeType"] =
        json!("application/x-www-form-urlencoded");
    pii["log"]["entries"][0]["response"]["content"]["encoding"] = json!("base64");
    pii["log"]["entries"][0]["response"]["content"]["text"] =
        json!(STANDARD.encode(encoded_response));
    pii["log"]["entries"][0]["response"]["content"]["size"] = json!(encoded_response.len());
    pii["log"]["entries"][0]["request"]["postData"] = json!({
        "mimeType": "application/x-www-form-urlencoded",
        "text": "contact=carol%40example.com",
        "params": [{
            "name": "erin@example.com",
            "value": "bob@example.com",
            "fileName": "frank@example.com.txt"
        }]
    });
    fs::write(&pii_input, serde_json::to_vec(&pii).unwrap()).unwrap();

    harlite()
        .arg("redact")
        .arg(&pii_input)
        .args([
            "--no-defaults",
            "--body-regex",
            r"[A-Za-z]+@example\.com",
            "--output",
        ])
        .arg(&form_redact_out)
        .assert()
        .success();
    let form_redacted = fs::read_to_string(&form_redact_out).unwrap();
    assert!(!form_redacted.contains("bob@example.com"));
    assert!(!form_redacted.contains("carol@example.com"));
    assert!(!form_redacted.contains("erin@example.com"));
    assert!(!form_redacted.contains("frank@example.com"));

    harlite()
        .arg("pii")
        .arg(&pii_input)
        .args(["--redact", "--output"])
        .arg(&pii_out)
        .args(["--format", "json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("email"));
    let pii_redacted = fs::read_to_string(&pii_out).unwrap();
    assert!(!pii_redacted.contains("alice@example.com"));
    assert!(!pii_redacted.contains("bob@example.com"));
    assert!(!pii_redacted.contains("carol@example.com"));
    assert!(!pii_redacted.contains("carol%40example.com"));
    assert!(!pii_redacted.contains("erin@example.com"));
    assert!(!pii_redacted.contains("frank@example.com"));
    assert!(pii_redacted.contains("REDACTED"));
    let pii_redacted_json: serde_json::Value = serde_json::from_str(&pii_redacted).unwrap();
    let response_text = pii_redacted_json["log"]["entries"][0]["response"]["content"]["text"]
        .as_str()
        .unwrap();
    let response_text = String::from_utf8(STANDARD.decode(response_text).unwrap()).unwrap();
    assert_eq!(response_text, "contact=REDACTED");

    harlite().arg("check").arg(&pii_out).assert().success();

    let mut encoded_pii: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    encoded_pii["log"]["entries"][0]["request"]["url"] = json!(
        "https://dave%40example.com@2125551212.example.test/users/bob%40example.com?email=alice%40example.com&nested=grace%2540example.com#carol%40example.com"
    );
    encoded_pii["log"]["entries"][0]["request"]["queryString"] =
        json!([{ "name": "email", "value": "alice@example.com" }]);
    fs::write(
        &encoded_pii_input,
        serde_json::to_vec(&encoded_pii).unwrap(),
    )
    .unwrap();
    harlite()
        .arg("pii")
        .arg(&encoded_pii_input)
        .args(["--redact", "--token", "[REDACTED]", "--output"])
        .arg(&encoded_pii_out)
        .args(["--format", "json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("email"));
    let encoded_redacted = fs::read_to_string(&encoded_pii_out).unwrap();
    assert!(!encoded_redacted.contains("alice@example.com"));
    assert!(!encoded_redacted.contains("alice%40example.com"));
    assert!(!encoded_redacted.contains("bob%40example.com"));
    assert!(!encoded_redacted.contains("carol%40example.com"));
    assert!(!encoded_redacted.contains("dave%40example.com"));
    assert!(!encoded_redacted.contains("grace%2540example.com"));
    assert!(!encoded_redacted.contains("grace%40example.com"));
    assert!(!encoded_redacted.contains("2125551212"));
    assert!(encoded_redacted.contains("REDACTED"));
    let encoded_redacted_json: serde_json::Value = serde_json::from_str(&encoded_redacted).unwrap();
    assert_eq!(
        encoded_redacted_json["log"]["entries"][1]["request"]["url"],
        encoded_pii["log"]["entries"][1]["request"]["url"]
    );
    assert_eq!(
        encoded_redacted_json["log"]["entries"][1]["request"]["queryString"],
        encoded_pii["log"]["entries"][1]["request"]["queryString"]
    );

    harlite()
        .arg("import")
        .arg(&pii_input)
        .arg(&encoded_pii_input)
        .args(["--bodies", "--output"])
        .arg(&pii_db)
        .assert()
        .success();
    harlite()
        .arg("pii")
        .arg(&pii_db)
        .args(["--redact", "--output"])
        .arg(&pii_db_out)
        .args(["--format", "json"])
        .assert()
        .success();
    let conn = rusqlite::Connection::open(&pii_db_out).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT entries.url, entries.host, entries.path, entries.query_string, blobs.content
             FROM entries
             LEFT JOIN blobs ON blobs.hash = entries.request_body_hash",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<Vec<u8>>>(4)?,
            ))
        })
        .unwrap();
    let mut database_text = String::new();
    for row in rows {
        let (url, host, path, query, body) = row.unwrap();
        database_text.push_str(&url.unwrap_or_default());
        database_text.push_str(&host.unwrap_or_default());
        database_text.push_str(&path.unwrap_or_default());
        database_text.push_str(&query.unwrap_or_default());
        database_text.push_str(&String::from_utf8(body.unwrap_or_default()).unwrap());
    }
    for secret in [
        "alice@example.com",
        "alice%40example.com",
        "bob%40example.com",
        "carol%40example.com",
        "dave%40example.com",
        "grace%2540example.com",
        "grace%40example.com",
        "2125551212",
    ] {
        assert!(
            !database_text.contains(secret),
            "database retained {secret}"
        );
    }
}

#[test]
fn test_request_export_formats_and_sensitive_header_policy() {
    let tmp = TempDir::new().unwrap();
    let form_path = tmp.path().join("form.har");
    let db_path = tmp.path().join("legacy.db");
    let cookie_db_path = tmp.path().join("cookies.db");

    harlite()
        .args(["request", "tests/fixtures/redact.har", "--format", "curl"])
        .assert()
        .success()
        .stdout(predicate::str::contains("curl"))
        .stdout(predicate::str::contains("supersecret").not());

    harlite()
        .args([
            "request",
            "tests/fixtures/redact.har",
            "--format",
            "powershell",
            "--include-sensitive",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Invoke-WebRequest"))
        .stdout(predicate::str::contains("supersecret"))
        .stdout(predicate::str::contains("session_id=sess123"));

    harlite()
        .args([
            "request",
            "tests/fixtures/simple.har",
            "--format",
            "node-fetch",
            "--limit",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "import fetch from \"node-fetch\";",
        ));

    let output = harlite()
        .args([
            "request",
            "tests/fixtures/simple.har",
            "--format",
            "node-fetch",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.matches("import fetch from").count(), 1);

    let mut form: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    form["log"]["entries"][0]["request"]["method"] = json!("POST");
    form["log"]["entries"][0]["request"]["postData"] = json!({
        "mimeType": "application/x-www-form-urlencoded; charset=UTF-8",
        "params": [{ "name": "message", "value": "hello world" }]
    });
    form["log"]["entries"][0]["request"]["headers"]
        .as_array_mut()
        .unwrap()
        .extend([
            json!({ "name": "Private-Token", "value": "gitlab-secret" }),
            json!({ "name": "X-Amz-Security-Token", "value": "aws-secret" }),
            json!({ "name": "X-Authorization", "value": "variant-secret" }),
            json!({ "name": "X-AuthToken", "value": "concatenated-auth-secret" }),
            json!({ "name": "X-AccessToken", "value": "concatenated-access-secret" }),
            json!({ "name": "Transfer-Encoding", "value": "chunked" }),
            json!({ "name": "X-Dupe", "value": "first" }),
            json!({ "name": "x-dupe", "value": "second" }),
        ]);
    fs::write(&form_path, serde_json::to_vec(&form).unwrap()).unwrap();
    harlite()
        .arg("request")
        .arg(&form_path)
        .args(["--format", "curl", "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("message=hello+world"))
        .stdout(predicate::str::contains(
            "Content-Type: application/x-www-form-urlencoded; charset=UTF-8",
        ))
        .stdout(predicate::str::contains("gitlab-secret").not())
        .stdout(predicate::str::contains("aws-secret").not())
        .stdout(predicate::str::contains("variant-secret").not())
        .stdout(predicate::str::contains("concatenated-auth-secret").not())
        .stdout(predicate::str::contains("concatenated-access-secret").not())
        .stdout(predicate::str::contains("Transfer-Encoding").not());

    let output = harlite()
        .arg("request")
        .arg(&form_path)
        .args(["--format", "powershell", "--limit", "1"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let powershell = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        powershell.to_ascii_lowercase().matches("'x-dupe'").count(),
        1
    );
    assert!(powershell.contains("first, second"));

    form["log"]["entries"][0]["request"]["postData"] = json!({
        "mimeType": "multipart/form-data",
        "params": [
            { "name": "message", "value": "hello multipart" },
            {
                "name": "upload",
                "value": "captured file contents",
                "fileName": "example.txt",
                "contentType": "text/plain"
            }
        ]
    });
    form["log"]["entries"][0]["request"]["headers"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "Content-Type",
            "value": "multipart/form-data; boundary=stale-captured-boundary"
        }));
    fs::write(&form_path, serde_json::to_vec(&form).unwrap()).unwrap();
    let output = harlite()
        .arg("request")
        .arg(&form_path)
        .args(["--format", "fetch", "--limit", "1"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let fetch = String::from_utf8(output.stdout).unwrap();
    assert!(fetch.contains("multipart/form-data; boundary=harlite-"));
    assert!(!fetch.contains("stale-captured-boundary"));
    assert!(fetch.contains("name=\\\"message\\\""));
    assert!(fetch.contains("hello multipart"));
    assert!(fetch.contains("filename=\\\"example.txt\\\""));
    assert!(fetch.contains("captured file contents"));

    form["log"]["entries"][0]["request"]["postData"] = json!({
        "mimeType": "text/plain",
        "text": "@captured-body"
    });
    fs::write(&form_path, serde_json::to_vec(&form).unwrap()).unwrap();
    harlite()
        .arg("request")
        .arg(&form_path)
        .args(["--format", "curl", "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("printf %s '@captured-body'"))
        .stdout(predicate::str::contains("--data-binary @-"));

    harlite()
        .args(["import", "tests/fixtures/redact.har", "-o"])
        .arg(&cookie_db_path)
        .assert()
        .success();
    harlite()
        .arg("request")
        .arg(&cookie_db_path)
        .args(["--include-sensitive", "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("session_id=sess123"));

    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    harlite()
        .arg("request")
        .arg(&db_path)
        .args([
            "--method",
            "get",
            "--host",
            "API.EXAMPLE.COM",
            "--limit",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("curl"));
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_graphql_fields_entry_field;
         DROP INDEX IF EXISTS idx_graphql_fields_field;
         DROP INDEX IF EXISTS idx_graphql_fields_entry;
         DROP TABLE graphql_fields;",
    )
    .unwrap();
    drop(conn);
    harlite()
        .arg("request")
        .arg(&db_path)
        .args(["--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("curl"));
}

#[test]
fn test_analyze_and_diff_ci_gates_return_failure() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("gate.db");
    let changed_db_path = tmp.path().join("gate-changed.db");
    let noise_left_path = tmp.path().join("noise-left.har");
    let noise_right_path = tmp.path().join("noise-right.har");
    harlite()
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&db_path)
        .assert()
        .success();
    harlite()
        .args(["import", "tests/fixtures/simple_changed.har", "-o"])
        .arg(&changed_db_path)
        .assert()
        .success();

    harlite()
        .arg("analyze")
        .arg(&db_path)
        .args(["--max-p95-total-ms", "1", "--json"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"entries\":2"))
        .stderr(predicate::str::contains("Threshold exceeded"));

    harlite()
        .args([
            "diff",
            "tests/fixtures/simple.har",
            "tests/fixtures/simple_changed.har",
            "--fail-on",
            "new-errors",
            "--format",
            "json",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"change\""))
        .stderr(predicate::str::contains("Threshold exceeded"));

    harlite()
        .arg("diff")
        .arg(&db_path)
        .arg(&changed_db_path)
        .args([
            "--method",
            "get",
            "--host",
            "API.EXAMPLE.COM",
            "--fail-on",
            "any",
            "--format",
            "json",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"change\""))
        .stderr(predicate::str::contains("Threshold exceeded"));

    harlite()
        .arg("analyze")
        .arg(&db_path)
        .args(["--max-p95-total-ms", "NaN"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("finite and non-negative"));
    harlite()
        .args([
            "diff",
            "tests/fixtures/simple.har",
            "tests/fixtures/simple_changed.har",
            "--max-total-regression-ms",
            "NaN",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("finite and non-negative"));

    let noise_left: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    let mut noise_right = noise_left.clone();
    let original_time = noise_right["log"]["entries"][0]["time"].as_f64().unwrap();
    noise_right["log"]["entries"][0]["time"] = json!(original_time + 1e-7);
    noise_right["log"]["entries"][0]["response"]["headers"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "name": "x-noise-test", "value": "changed" }));
    fs::write(&noise_left_path, serde_json::to_vec(&noise_left).unwrap()).unwrap();
    fs::write(&noise_right_path, serde_json::to_vec(&noise_right).unwrap()).unwrap();
    harlite()
        .arg("diff")
        .arg(&noise_left_path)
        .arg(&noise_right_path)
        .args([
            "--fail-on",
            "regression",
            "--max-total-regression-ms",
            "0",
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"change\":\"changed\""));
}

#[test]
fn test_diff_can_ignore_query_parameters_when_matching() {
    let tmp = TempDir::new().unwrap();
    let left_path = tmp.path().join("left.har");
    let right_path = tmp.path().join("right.har");
    let spelling_left_path = tmp.path().join("left-spelling.har");
    let spelling_right_path = tmp.path().join("right-spelling.har");
    let mut left: serde_json::Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    let mut right = left.clone();
    for entry in left["log"]["entries"].as_array_mut().unwrap() {
        let url = entry["request"]["url"].as_str().unwrap().to_string();
        entry["request"]["url"] = json!(format!("{url}?cacheBust=left"));
    }
    for entry in right["log"]["entries"].as_array_mut().unwrap() {
        let url = entry["request"]["url"].as_str().unwrap().to_string();
        entry["request"]["url"] = json!(format!("{url}?cacheBust=right"));
    }
    fs::write(&left_path, serde_json::to_vec(&left).unwrap()).unwrap();
    fs::write(&right_path, serde_json::to_vec(&right).unwrap()).unwrap();

    let mut spelling_left = left.clone();
    let mut spelling_right = right.clone();
    spelling_left["log"]["entries"][0]["request"]["url"] =
        json!("https://example.com?cacheBust=left");
    spelling_right["log"]["entries"][0]["request"]["url"] =
        json!("https://example.com/?cacheBust=right");
    fs::write(
        &spelling_left_path,
        serde_json::to_vec(&spelling_left).unwrap(),
    )
    .unwrap();
    fs::write(
        &spelling_right_path,
        serde_json::to_vec(&spelling_right).unwrap(),
    )
    .unwrap();

    harlite()
        .arg("diff")
        .arg(&left_path)
        .arg(&right_path)
        .args([
            "--ignore-query-param",
            "cacheBust",
            "--fail-on",
            "any",
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::eq("[]\n"));

    harlite()
        .arg("diff")
        .arg(&spelling_left_path)
        .arg(&spelling_right_path)
        .args([
            "--ignore-query-param",
            "cacheBust",
            "--fail-on",
            "any",
            "--format",
            "json",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Threshold exceeded"));
}

#[test]
fn test_redaction_accepts_new_relative_outputs() {
    let tmp = TempDir::new().unwrap();
    let source = tmp.path().join("source.db");
    harlite()
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&source)
        .assert()
        .success();
    for output in ["redacted.db", "./redacted-dot.db", "sub/redacted.db"] {
        fs::create_dir_all(tmp.path().join("sub")).unwrap();
        harlite()
            .current_dir(tmp.path())
            .args(["redact", "source.db", "-o", output])
            .assert()
            .success();
        assert!(tmp.path().join(output).is_file());
    }
    harlite()
        .current_dir(tmp.path())
        .args(["pii", "source.db", "--redact", "-o", "pii.db"])
        .assert()
        .success();
    assert!(tmp.path().join("pii.db").is_file());
}
