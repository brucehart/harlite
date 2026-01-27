use std::fs;
use std::path::PathBuf;

use crate::error::{HarliteError, Result};

pub fn resolve_database(database: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(db) = database {
        return Ok(db);
    }

    let mut candidates: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(".")? {
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
