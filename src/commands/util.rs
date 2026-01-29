use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{HarliteError, Result};

pub fn resolve_database(database: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(db) = database {
        return Ok(db);
    }

    resolve_database_in_dir(Path::new("."))
}

fn resolve_database_in_dir(dir: &Path) -> Result<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("db") {
            continue;
        }
        candidates.push(path);
    }

    match candidates.len() {
        1 => Ok(candidates.remove(0)),
        0 => Err(HarliteError::InvalidArgs(
            "No database specified and no .db files found in the current directory".to_string(),
        )),
        n => Err(HarliteError::InvalidArgs(format!(
            "No database specified and found {} .db files in the current directory; please pass a database path",
            n
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_database_in_dir;
    use crate::error::HarliteError;
    use tempfile::TempDir;

    #[test]
    fn resolve_database_in_dir_returns_single_match() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("only.db");
        std::fs::write(&db_path, b"").unwrap();

        let resolved = resolve_database_in_dir(tmp.path()).unwrap();
        assert_eq!(resolved, db_path);
    }

    #[test]
    fn resolve_database_in_dir_errors_when_missing() {
        let tmp = TempDir::new().unwrap();
        let err = resolve_database_in_dir(tmp.path()).unwrap_err();
        match err {
            HarliteError::InvalidArgs(msg) => {
                assert!(msg.contains("no .db files"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn resolve_database_in_dir_errors_when_multiple() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("one.db"), b"").unwrap();
        std::fs::write(tmp.path().join("two.db"), b"").unwrap();

        let err = resolve_database_in_dir(tmp.path()).unwrap_err();
        match err {
            HarliteError::InvalidArgs(msg) => {
                assert!(msg.contains("found 2 .db files"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
