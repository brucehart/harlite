use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn exporters_reject_input_aliases_before_writing() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("source.db");
    cargo_bin_cmd!("harlite")
        .args(["import", "tests/fixtures/simple.har", "--bodies", "-o"])
        .arg(&input)
        .assert()
        .success();
    let before = fs::read(&input).unwrap();
    let hard = tmp.path().join("hard.db");
    fs::hard_link(&input, &hard).unwrap();
    let mut outputs = vec![input.clone(), hard];
    #[cfg(unix)]
    {
        let link = tmp.path().join("symbolic.db");
        std::os::unix::fs::symlink(&input, &link).unwrap();
        outputs.push(link);
    }
    for command in [
        "export",
        "report",
        "waterfall",
        "openapi",
        "export-data",
        "otel",
    ] {
        for output in &outputs {
            cargo_bin_cmd!("harlite")
                .arg(command)
                .arg(&input)
                .arg("-o")
                .arg(output)
                .assert()
                .failure()
                .stderr(predicate::str::contains("Output must be different"));
            assert_eq!(
                fs::read(&input).unwrap(),
                before,
                "{command} changed its source"
            );
        }
    }
    let har = tmp.path().join("source.har");
    fs::write(&har, include_bytes!("fixtures/simple.har")).unwrap();
    cargo_bin_cmd!("harlite")
        .arg("export")
        .arg(&har)
        .arg("-o")
        .arg(&har)
        .assert()
        .failure();
    assert_eq!(
        fs::read(har).unwrap(),
        include_bytes!("fixtures/simple.har")
    );
}

#[test]
fn successful_exports_replace_only_the_selected_output() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("source.db");
    cargo_bin_cmd!("harlite")
        .args(["import", "tests/fixtures/simple.har", "-o"])
        .arg(&input)
        .assert()
        .success();
    let before = fs::read(&input).unwrap();
    for command in [
        "export",
        "report",
        "waterfall",
        "openapi",
        "export-data",
        "otel",
    ] {
        let output = tmp.path().join(format!("{command}.out"));
        fs::write(&output, b"old output").unwrap();
        cargo_bin_cmd!("harlite")
            .arg(command)
            .arg(&input)
            .arg("-o")
            .arg(&output)
            .assert()
            .success();
        assert_ne!(fs::read(output).unwrap(), b"old output");
        assert_eq!(fs::read(&input).unwrap(), before);
    }
    assert!(!fs::read_dir(tmp.path()).unwrap().any(|p| p
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains("harlite-stage")));
}
