use assert_cmd::cargo::cargo_bin_cmd;
use serde_json::{json, Value};
use std::fs;
use tempfile::TempDir;

#[test]
fn repeated_headers_survive_storage_export_and_redaction() {
    let tmp = TempDir::new().unwrap();
    let mut har: Value = serde_json::from_str(include_str!("fixtures/simple.har")).unwrap();
    har["log"]["entries"].as_array_mut().unwrap().truncate(1);
    har["log"]["entries"][0]["request"]["headers"] = json!([
        {"name":"X-Test","value":"first"},{"name":"x-test","value":"second"},
        {"name":"Authorization","value":"secret-one"},{"name":"authorization","value":"secret-two"}]);
    har["log"]["entries"][0]["response"]["headers"] = json!([
        {"name":"Set-Cookie","value":"first=1"},{"name":"set-cookie","value":"second=2"},
        {"name":"Content-Type","value":"application/json"}]);
    let input = tmp.path().join("input.har");
    fs::write(&input, serde_json::to_vec(&har).unwrap()).unwrap();
    let db = tmp.path().join("source.db");
    cargo_bin_cmd!("harlite")
        .arg("import")
        .arg(&input)
        .arg("-o")
        .arg(&db)
        .assert()
        .success();
    let output = tmp.path().join("output.har");
    cargo_bin_cmd!("harlite")
        .arg("export")
        .arg(&db)
        .arg("-o")
        .arg(&output)
        .assert()
        .success();
    let exported: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
    let response = exported["log"]["entries"][0]["response"]["headers"]
        .as_array()
        .unwrap();
    let cookies: Vec<_> = response
        .iter()
        .filter(|h| h["name"] == "set-cookie")
        .map(|h| h["value"].as_str().unwrap())
        .collect();
    assert_eq!(cookies, ["first=1", "second=2"]);
    let snippet = cargo_bin_cmd!("harlite")
        .arg("request")
        .arg(&db)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let snippet = String::from_utf8(snippet).unwrap();
    assert!(snippet.contains("x-test: first") && snippet.contains("x-test: second"));
    let safe = tmp.path().join("safe.db");
    cargo_bin_cmd!("harlite")
        .arg("redact")
        .arg(&db)
        .arg("-o")
        .arg(&safe)
        .assert()
        .success();
    let conn = rusqlite::Connection::open(safe).unwrap();
    let headers: String = conn
        .query_row("SELECT request_headers FROM entries", [], |r| r.get(0))
        .unwrap();
    let headers: Value = serde_json::from_str(&headers).unwrap();
    assert_eq!(headers["authorization"], json!(["REDACTED", "REDACTED"]));
    assert_eq!(headers["x-test"], json!(["first", "second"]));
    let changed = tmp.path().join("changed.har");
    har["log"]["entries"][0]["request"]["headers"][0]["value"] = json!("changed-first");
    fs::write(&changed, serde_json::to_vec(&har).unwrap()).unwrap();
    cargo_bin_cmd!("harlite")
        .arg("diff")
        .arg(&input)
        .arg(&changed)
        .args(["--fail-on", "changed"])
        .assert()
        .failure();
}
