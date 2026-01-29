use std::path::PathBuf;

use rusqlite::{Connection, OpenFlags};

use crate::error::Result;

#[derive(Debug)]
struct ImportRow {
    id: i64,
    source_file: String,
    imported_at: String,
    entry_count: i64,
    entries_skipped: i64,
    status: String,
    date_min: Option<String>,
    date_max: Option<String>,
}

/// List import metadata for a harlite database.
pub fn run_imports(database: PathBuf) -> Result<()> {
    let conn = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.execute_batch("PRAGMA query_only=ON;")?;

    let mut stmt = conn.prepare(
        "SELECT i.id,\n\
                i.source_file,\n\
                i.imported_at,\n\
                COALESCE(i.entry_count, COUNT(e.id)) AS entry_count,\n\
                COALESCE(i.entries_skipped, 0) AS entries_skipped,\n\
                COALESCE(i.status, 'complete') AS status,\n\
                MIN(e.started_at) AS date_min,\n\
                MAX(e.started_at) AS date_max\n\
         FROM imports i\n\
         LEFT JOIN entries e ON e.import_id = i.id\n\
         GROUP BY i.id\n\
         ORDER BY i.id",
    )?;

    let rows: Vec<ImportRow> = stmt
        .query_map([], |row| {
            Ok(ImportRow {
                id: row.get(0)?,
                source_file: row.get(1)?,
                imported_at: row.get(2)?,
                entry_count: row.get(3)?,
                entries_skipped: row.get(4)?,
                status: row.get(5)?,
                date_min: row.get(6)?,
                date_max: row.get(7)?,
            })
        })?
        .filter_map(|row| row.ok())
        .collect();

    if rows.is_empty() {
        println!("No imports found.");
        return Ok(());
    }

    let mut id_width = "ID".len();
    let mut source_width = "Source".len();
    let mut imported_width = "Imported At".len();
    let mut status_width = "Status".len();
    let mut entries_width = "Entries".len();
    let mut skipped_width = "Skipped".len();
    let mut range_width = "Date Range".len();

    let format_date = |value: &Option<String>| -> String {
        value
            .as_deref()
            .and_then(|s| s.split('T').next())
            .unwrap_or("")
            .to_string()
    };

    for row in &rows {
        id_width = id_width.max(row.id.to_string().len());
        source_width = source_width.max(row.source_file.len());
        imported_width = imported_width.max(row.imported_at.len());
        status_width = status_width.max(row.status.len());
        entries_width = entries_width.max(row.entry_count.to_string().len());
        skipped_width = skipped_width.max(row.entries_skipped.to_string().len());
        let min_date = format_date(&row.date_min);
        let max_date = format_date(&row.date_max);
        let range = if min_date.is_empty() && max_date.is_empty() {
            String::new()
        } else if min_date == max_date {
            min_date
        } else {
            format!("{min_date}..{max_date}")
        };
        range_width = range_width.max(range.len());
    }

    println!(
        "{:>id_w$}  {:<src_w$}  {:<imp_w$}  {:<stat_w$}  {:>ent_w$}  {:>skip_w$}  {:<rng_w$}",
        "ID",
        "Source",
        "Imported At",
        "Status",
        "Entries",
        "Skipped",
        "Date Range",
        id_w = id_width,
        src_w = source_width,
        imp_w = imported_width,
        stat_w = status_width,
        ent_w = entries_width,
        skip_w = skipped_width,
        rng_w = range_width,
    );

    for row in rows {
        let min_date = format_date(&row.date_min);
        let max_date = format_date(&row.date_max);
        let range = if min_date.is_empty() && max_date.is_empty() {
            String::new()
        } else if min_date == max_date {
            min_date
        } else {
            format!("{min_date}..{max_date}")
        };
        println!(
            "{:>id_w$}  {:<src_w$}  {:<imp_w$}  {:<stat_w$}  {:>ent_w$}  {:>skip_w$}  {:<rng_w$}",
            row.id,
            row.source_file,
            row.imported_at,
            row.status,
            row.entry_count,
            row.entries_skipped,
            range,
            id_w = id_width,
            src_w = source_width,
            imp_w = imported_width,
            stat_w = status_width,
            ent_w = entries_width,
            skip_w = skipped_width,
            rng_w = range_width,
        );
    }

    Ok(())
}
