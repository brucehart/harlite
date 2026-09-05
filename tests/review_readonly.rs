use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use tempfile::TempDir;

#[test]
fn inspection_never_creates_a_missing_input() {
    let tmp = TempDir::new().unwrap();
    for command in [
        "info",
        "schema",
        "analyze",
        "waterfall",
        "openapi",
        "export-data",
        "otel",
        "report",
        "export",
        "request",
        "serve",
        "replay",
    ] {
        let input = tmp.path().join(format!("{command}.db"));
        cargo_bin_cmd!("harlite")
            .arg(command)
            .arg(&input)
            .assert()
            .failure();
        assert!(!input.exists(), "{command} created its input");
    }
}

#[test]
fn inspection_preserves_current_and_legacy_databases() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("source.db");
    cargo_bin_cmd!("harlite")
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&input)
        .assert()
        .success();
    for legacy in [false, true] {
        if legacy {
            let conn = rusqlite::Connection::open(&input).unwrap();
            conn.execute_batch(
                "ALTER TABLE entries DROP COLUMN tls_cipher_suite; DROP TABLE graphql_fields;",
            )
            .unwrap();
        }
        let before = fs::read(&input).unwrap();
        for command in [
            "info",
            "schema",
            "analyze",
            "waterfall",
            "openapi",
            "export-data",
            "otel",
            "report",
            "export",
            "request",
        ] {
            let output = tmp.path().join(format!("{command}-{legacy}.out"));
            let mut cmd = cargo_bin_cmd!("harlite");
            cmd.arg(command).arg(&input);
            if ["openapi", "export-data", "otel", "report", "export"].contains(&command) {
                cmd.arg("-o").arg(&output);
            }
            cmd.assert().success();
            assert_eq!(
                fs::read(&input).unwrap(),
                before,
                "{command} changed its input (legacy={legacy})"
            );
        }
    }
}
