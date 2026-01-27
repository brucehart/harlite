use std::path::PathBuf;

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use crate::error::Result;

#[derive(Clone, Copy, Debug, Default)]
pub struct StatsOptions {
    pub json: bool,
}

#[derive(Debug, Serialize)]
struct StatsOutput {
    imports: i64,
    entries: i64,
    date_min: Option<String>,
    date_max: Option<String>,
    unique_hosts: i64,
    blobs: i64,
    blob_bytes: i64,
}

/// Show lightweight, script-friendly stats for a harlite database.
pub fn run_stats(database: PathBuf, options: &StatsOptions) -> Result<()> {
    let conn = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.execute_batch("PRAGMA query_only=ON;")?;

    let import_count: i64 = conn.query_row("SELECT COUNT(*) FROM imports", [], |row| row.get(0))?;

    // Prefer using imports.entry_count where available (fast) and only count entries for imports
    // that are missing entry_count, instead of falling back to counting all entries.
    let entry_count: i64 = conn.query_row(
        "SELECT COALESCE(SUM(entry_count), 0) + (\
             SELECT COUNT(*) FROM entries \
             WHERE import_id IN (SELECT id FROM imports WHERE entry_count IS NULL)\
         ) FROM imports",
        [],
        |row| row.get(0),
    )?;

    let date_range: (Option<String>, Option<String>) = conn.query_row(
        "SELECT MIN(started_at), MAX(started_at) FROM entries",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    let host_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT host) FROM entries WHERE host IS NOT NULL",
        [],
        |row| row.get(0),
    )?;

    let blob_stats: (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM blobs",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    let out = StatsOutput {
        imports: import_count,
        entries: entry_count,
        date_min: date_range
            .0
            .map(|s| s.split('T').next().unwrap_or(&s).to_string()),
        date_max: date_range
            .1
            .map(|s| s.split('T').next().unwrap_or(&s).to_string()),
        unique_hosts: host_count,
        blobs: blob_stats.0,
        blob_bytes: blob_stats.1,
    };

    if options.json {
        println!("{}", serde_json::to_string(&out)?);
        return Ok(());
    }

    println!("imports={}", out.imports);
    println!("entries={}", out.entries);
    println!("date_min={}", out.date_min.as_deref().unwrap_or(""));
    println!("date_max={}", out.date_max.as_deref().unwrap_or(""));
    println!("unique_hosts={}", out.unique_hosts);
    println!("blobs={}", out.blobs);
    println!("blob_bytes={}", out.blob_bytes);

    Ok(())
}
