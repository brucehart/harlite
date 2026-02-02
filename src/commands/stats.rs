use std::path::PathBuf;

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use chrono::{Duration, Utc};

use crate::commands::util::parse_cert_expiry;
use crate::error::Result;

#[derive(Clone, Copy, Debug, Default)]
pub struct StatsOptions {
    pub json: bool,
    pub cert_expiring_days: Option<u64>,
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
    certs_expiring: Option<i64>,
    certs_expiring_earliest: Option<String>,
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

    let extract_date = |s: &String| s.split('T').next().unwrap_or(s).to_string();

    let mut certs_expiring = None;
    let mut certs_expiring_earliest = None;

    if let Some(days) = options.cert_expiring_days {
        let cutoff = Utc::now() + Duration::days(days as i64);
        let mut stmt = conn.prepare(
            "SELECT DISTINCT tls_cert_subject, tls_cert_issuer, tls_cert_expiry \
             FROM entries WHERE tls_cert_expiry IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut count: i64 = 0;
        let mut earliest = None;
        for row in rows {
            let (_subject, _issuer, expiry_raw) = row?;
            if let Some(expiry) = parse_cert_expiry(&expiry_raw) {
                if expiry <= cutoff {
                    count += 1;
                    if earliest.map_or(true, |dt| expiry < dt) {
                        earliest = Some(expiry);
                    }
                }
            }
        }
        certs_expiring = Some(count);
        certs_expiring_earliest = earliest.map(|dt| dt.to_rfc3339());
    }

    let out = StatsOutput {
        imports: import_count,
        entries: entry_count,
        date_min: date_range.0.as_ref().map(extract_date),
        date_max: date_range.1.as_ref().map(extract_date),
        unique_hosts: host_count,
        blobs: blob_stats.0,
        blob_bytes: blob_stats.1,
        certs_expiring,
        certs_expiring_earliest,
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
    if let Some(certs_expiring) = out.certs_expiring {
        println!("certs_expiring={}", certs_expiring);
        println!(
            "certs_expiring_earliest={}",
            out.certs_expiring_earliest.as_deref().unwrap_or("")
        );
    }

    Ok(())
}
