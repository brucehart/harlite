use std::path::PathBuf;

use rusqlite::Connection;

use crate::error::Result;

/// Show summary information for a harlite database.
pub fn run_info(database: PathBuf) -> Result<()> {
    let conn = Connection::open(&database)?;

    let import_count: i64 = conn.query_row("SELECT COUNT(*) FROM imports", [], |row| row.get(0))?;

    let entry_count: i64 = conn.query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))?;

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

    println!("Database: {}", database.display());
    println!("Imports: {} files", import_count);
    println!("Entries: {}", entry_count);

    if let (Some(min), Some(max)) = date_range {
        let min_date = min.split('T').next().unwrap_or(&min);
        let max_date = max.split('T').next().unwrap_or(&max);
        if min_date == max_date {
            println!("Date: {}", min_date);
        } else {
            println!("Date range: {} to {}", min_date, max_date);
        }
    }

    println!("Unique hosts: {}", host_count);

    if blob_stats.0 > 0 {
        let size_kb = blob_stats.1 as f64 / 1024.0;
        if size_kb > 1024.0 {
            println!(
                "Stored blobs: {} ({:.1} MB)",
                blob_stats.0,
                size_kb / 1024.0
            );
        } else {
            println!("Stored blobs: {} ({:.1} KB)", blob_stats.0, size_kb);
        }
    }

    println!("\nTop hosts:");
    let mut stmt = conn.prepare(
        "SELECT host, COUNT(*) as cnt FROM entries WHERE host IS NOT NULL GROUP BY host ORDER BY cnt DESC LIMIT 5",
    )?;
    let hosts: Vec<(String, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    for (host, count) in hosts {
        println!("  {} ({})", host, count);
    }

    Ok(())
}
