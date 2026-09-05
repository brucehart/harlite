use assert_cmd::cargo::cargo_bin_cmd;
use serde_json::{json, Value};
use std::fs;
use tempfile::TempDir;

#[test]
fn privacy_outputs_remove_unscanned_metadata_at_every_level() {
    let tmp = TempDir::new().unwrap();
    let mut har: Value = serde_json::from_str(include_str!("fixtures/simple.har")).unwrap();
    har["log"]["entries"].as_array_mut().unwrap().truncate(1);
    har["log"]["_private"] = json!("REVIEW_SECRET");
    har["log"]["pages"] = json!([{"id":"page", "startedDateTime":"2024-01-15T10:30:00Z", "_private":"REVIEW_SECRET", "pageTimings":{"_private":"REVIEW_SECRET"}}]);
    let entry = &mut har["log"]["entries"][0];
    entry["_initiator"] = json!({"url":"https://example.com/REVIEW_SECRET"});
    entry["cache"] = json!({"private":"REVIEW_SECRET"});
    entry["request"]["_headersText"] = json!("Authorization: Bearer REVIEW_SECRET");
    entry["request"]["postData"] = json!({"text":"ordinary body", "_private":"REVIEW_SECRET"});
    entry["response"]["_private"] = json!("REVIEW_SECRET");
    entry["response"]["content"]["_private"] = json!("REVIEW_SECRET");
    entry["response"]["redirectURL"] = json!("https://example.com/REVIEW_SECRET");
    entry["timings"]["_private"] = json!("REVIEW_SECRET");
    let input = tmp.path().join("input.har");
    fs::write(&input, serde_json::to_vec(&har).unwrap()).unwrap();
    let db = tmp.path().join("input.db");
    cargo_bin_cmd!("harlite")
        .arg("import")
        .arg(&input)
        .args(["--bodies", "-o"])
        .arg(&db)
        .assert()
        .success();
    for source in [&input, &db] {
        let original = fs::read(source).unwrap();
        for command in ["redact", "pii"] {
            let output = tmp.path().join(format!(
                "{command}-{}",
                source.file_name().unwrap().to_str().unwrap()
            ));
            let mut cmd = cargo_bin_cmd!("harlite");
            cmd.arg(command).arg(source).arg("-o").arg(&output);
            if command == "pii" {
                cmd.arg("--redact");
            }
            cmd.assert().success();
            assert!(!fs::read(&output)
                .unwrap()
                .windows(b"REVIEW_SECRET".len())
                .any(|w| w == b"REVIEW_SECRET"));
            assert_eq!(fs::read(source).unwrap(), original);
            let dry = tmp.path().join("dry-output");
            let mut cmd = cargo_bin_cmd!("harlite");
            cmd.arg(command)
                .arg(source)
                .args(["--dry-run", "-o"])
                .arg(&dry);
            if command == "pii" {
                cmd.arg("--redact");
            }
            cmd.assert().success();
            assert!(!dry.exists());
            assert_eq!(fs::read(source).unwrap(), original);
        }
    }
}
