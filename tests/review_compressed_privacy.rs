#![cfg(feature = "compression")]
use assert_cmd::cargo::cargo_bin_cmd;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use std::{fs, io::Write, path::Path};
use tempfile::TempDir;

fn fixture(path: &Path, encoding: &str, corrupt: bool) {
    let plain = b"Contact private@example.com for details";
    let bytes = if corrupt {
        vec![0x1f, 0x8b, 0]
    } else if encoding == "gzip" {
        let mut out = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        out.write_all(plain).unwrap();
        out.finish().unwrap()
    } else {
        let mut out = Vec::new();
        {
            let mut writer = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
            writer.write_all(plain).unwrap();
        }
        out
    };
    let mut har: Value =
        serde_json::from_slice(&fs::read("tests/fixtures/simple.har").unwrap()).unwrap();
    har["log"]["entries"].as_array_mut().unwrap().truncate(1);
    let response = &mut har["log"]["entries"][0]["response"];
    response["headers"] = json!([{"name":"Content-Encoding","value":encoding},{"name":"Content-Length","value":bytes.len().to_string()}]);
    response["content"] = json!({"size":plain.len(),"mimeType":"text/plain","encoding":"base64","text":STANDARD.encode(bytes)});
    fs::write(path, serde_json::to_vec(&har).unwrap()).unwrap();
}
fn assert_clean(path: &Path) {
    let har: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let r = &har["log"]["entries"][0]["response"];
    let text = r["content"]["text"].as_str().unwrap();
    let bytes = if r["content"]["encoding"] == "base64" {
        STANDARD.decode(text).unwrap()
    } else {
        text.as_bytes().to_vec()
    };
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("private@example.com"), "{text}");
    assert!(text.contains("REDACTED"), "{text}");
    assert!(!r["headers"].as_array().unwrap().iter().any(|h| h["name"]
        .as_str()
        .unwrap()
        .eq_ignore_ascii_case("content-encoding")));
}
#[test]
fn compressed_privacy_har_and_database() {
    for encoding in ["gzip", "br"] {
        for command in ["redact", "pii"] {
            for database in [false, true] {
                let tmp = TempDir::new().unwrap();
                let har = tmp.path().join("input.har");
                fixture(&har, encoding, false);
                let input = if database {
                    let db = tmp.path().join("input.db");
                    cargo_bin_cmd!()
                        .arg("import")
                        .arg(&har)
                        .args(["--bodies", "-o"])
                        .arg(&db)
                        .assert()
                        .success();
                    db
                } else {
                    har
                };
                let before = fs::read(&input).unwrap();
                let output = tmp
                    .path()
                    .join(if database { "clean.db" } else { "clean.har" });
                let mut cmd = cargo_bin_cmd!();
                cmd.arg(command).arg(&input).arg("-o").arg(&output);
                if command == "redact" {
                    cmd.args(["--body-regex", "private@example\\.com"]);
                } else {
                    cmd.arg("--redact");
                }
                cmd.assert().success();
                assert_eq!(fs::read(&input).unwrap(), before);
                if database {
                    let exported = tmp.path().join("exported.har");
                    cargo_bin_cmd!()
                        .arg("export")
                        .arg(&output)
                        .args(["--bodies", "-o"])
                        .arg(&exported)
                        .assert()
                        .success();
                    assert_clean(&exported);
                } else {
                    assert_clean(&output);
                }
            }
        }
    }
}
#[test]
fn malformed_compression_does_not_publish_output() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("corrupt.har");
    fixture(&input, "gzip", true);
    for command in ["redact", "pii"] {
        let output = tmp.path().join(format!("{command}.har"));
        let mut cmd = cargo_bin_cmd!();
        cmd.arg(command).arg(&input).arg("-o").arg(&output);
        if command == "redact" {
            cmd.args(["--body-regex", "private"]);
        } else {
            cmd.arg("--redact");
        }
        cmd.assert().failure();
        assert!(!output.exists());
    }
}
