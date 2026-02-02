use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};

use crate::error::{HarliteError, Result};

pub fn canonicalize_path_for_compare(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(fs::canonicalize(path)?);
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent_canon = fs::canonicalize(parent)?;
    let name = path.file_name().ok_or_else(|| {
        HarliteError::InvalidArgs("Output path must be a file".to_string())
    })?;
    Ok(parent_canon.join(name))
}

pub fn resolve_database(database: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(db) = database {
        return Ok(db);
    }

    resolve_database_in_dir(Path::new("."))
}

/// Parse a timestamp from various formats into a `DateTime<Utc>`.
///
/// Supports:
/// - RFC3339 format (e.g., "2024-01-15T10:30:00Z")
/// - Date format (e.g., "2024-01-15")
/// - Unix timestamp in seconds (e.g., 1705315800)
/// - Unix timestamp in milliseconds (e.g., 1705315800000)
///
/// Returns `None` if the input cannot be parsed.
pub fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(parsed.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }
    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(num) = trimmed.parse::<i64>() {
            return parse_timestamp_number(num);
        }
    }
    None
}

/// Parse a Unix timestamp (seconds or milliseconds) into a `DateTime<Utc>`.
pub fn parse_timestamp_number(value: i64) -> Option<DateTime<Utc>> {
    let dt = if value >= 1_000_000_000_000 {
        Utc.timestamp_millis_opt(value).single()?
    } else {
        Utc.timestamp_opt(value, 0).single()?
    };
    Some(dt)
}

pub fn parse_cert_expiry(value: &str) -> Option<DateTime<Utc>> {
    parse_timestamp(value)
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
