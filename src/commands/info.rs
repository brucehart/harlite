use std::path::PathBuf;

use rusqlite::Connection;

use crate::commands::util::parse_cert_expiry;
use crate::error::Result;
use chrono::{Duration, Utc};

pub struct InfoOptions {
    pub cert_expiring_days: Option<u64>,
}

/// Show summary information for a harlite database.
pub fn run_info(database: PathBuf, options: &InfoOptions) -> Result<()> {
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

    if let Some(days) = options.cert_expiring_days {
        let now = Utc::now();
        let cutoff = now + Duration::days(days as i64);
        let mut stmt = conn.prepare(
            "SELECT DISTINCT host, tls_cert_subject, tls_cert_issuer, tls_cert_expiry, tls_version, tls_cipher_suite \
             FROM entries WHERE tls_cert_expiry IS NOT NULL",
        )?;
        let mut expiring = Vec::new();
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;

        for row in rows {
            let (host, subject, issuer, expiry_raw, tls_version, tls_cipher) = row?;
            if let Some(expiry) = parse_cert_expiry(&expiry_raw) {
                if expiry <= cutoff {
                    expiring.push((
                        host.unwrap_or_else(|| "<unknown host>".to_string()),
                        subject,
                        issuer,
                        expiry,
                        tls_version,
                        tls_cipher,
                    ));
                }
            }
        }

        expiring.sort_by_key(|entry| entry.3);

        println!("\nCertificates expiring within {} days:", days);
        if expiring.is_empty() {
            println!("  (none found)");
        } else {
            const MAX_CERT_ROWS: usize = 20;
            for (idx, (host, subject, issuer, expiry, tls_version, tls_cipher)) in
                expiring.iter().take(MAX_CERT_ROWS).enumerate()
            {
                let status = if *expiry < now { "expired" } else { "expiring" };
                let subject = subject.as_deref().unwrap_or("<unknown subject>");
                let issuer = issuer.as_deref().unwrap_or("<unknown issuer>");
                let version = tls_version.as_deref().unwrap_or("<unknown tls>");
                let cipher = tls_cipher.as_deref().unwrap_or("<unknown cipher>");
                println!(
                    "  {}. {} | {} | {} | {} | {} | {}",
                    idx + 1,
                    host,
                    subject,
                    issuer,
                    expiry.to_rfc3339(),
                    version,
                    cipher
                );
                if status == "expired" {
                    println!("     status: expired");
                }
            }
            if expiring.len() > MAX_CERT_ROWS {
                println!("  ... and {} more", expiring.len() - MAX_CERT_ROWS);
            }
        }
    }

    Ok(())
}
