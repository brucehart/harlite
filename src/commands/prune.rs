use std::fs;
use std::path::PathBuf;

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{HarliteError, Result};

const HASH_CHUNK: usize = 500;

/// Remove all records for a specific import and prune orphaned blobs.
pub fn run_prune(database: PathBuf, import_id: i64) -> Result<()> {
    let conn = Connection::open(&database)?;
    let external_root = database
        .parent()
        .map(|p| p.to_path_buf())
        .and_then(|p| p.canonicalize().ok());

    let import_exists: Option<String> = conn
        .query_row(
            "SELECT source_file FROM imports WHERE id = ?1",
            params![import_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(source_file) = import_exists else {
        return Err(HarliteError::InvalidArgs(format!(
            "Import id {import_id} not found"
        )));
    };

    let tx = conn.unchecked_transaction()?;

    let hashes: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT DISTINCT request_body_hash FROM entries WHERE import_id = ?1 AND request_body_hash IS NOT NULL\n\
             UNION\n\
             SELECT DISTINCT response_body_hash FROM entries WHERE import_id = ?1 AND response_body_hash IS NOT NULL\n\
             UNION\n\
             SELECT DISTINCT response_body_hash_raw FROM entries WHERE import_id = ?1 AND response_body_hash_raw IS NOT NULL",
        )?;
        let hashes = stmt
            .query_map(params![import_id], |row| row.get(0))?
            .filter_map(|row| row.ok())
            .collect();
        hashes
    };

    let entries_deleted = tx.execute("DELETE FROM entries WHERE import_id = ?1", params![import_id])?;
    let pages_deleted = tx.execute("DELETE FROM pages WHERE import_id = ?1", params![import_id])?;
    let imports_deleted = tx.execute("DELETE FROM imports WHERE id = ?1", params![import_id])?;

    let mut blobs_deleted = 0usize;
    let mut fts_deleted = 0usize;
    let mut external_deleted = 0usize;
    let mut external_skipped = 0usize;

    if !hashes.is_empty() {
        let has_fts: i64 = tx.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='response_body_fts'",
            [],
            |row| row.get(0),
        )?;

        for chunk in hashes.chunks(HASH_CHUNK) {
            let placeholders = std::iter::repeat("?")
                .take(chunk.len())
                .collect::<Vec<_>>()
                .join(", ");

            let mut params_vec: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len());
            for hash in chunk {
                params_vec.push(hash);
            }

            let sql_orphans = format!(
                "SELECT hash FROM blobs\n\
                 WHERE hash IN ({placeholders})\n\
                 AND NOT EXISTS (\n\
                     SELECT 1 FROM entries e\n\
                     WHERE e.request_body_hash = blobs.hash\n\
                        OR e.response_body_hash = blobs.hash\n\
                        OR e.response_body_hash_raw = blobs.hash\n\
                 )"
            );

            let orphan_hashes: Vec<String> = tx
                .prepare(&sql_orphans)?
                .query_map(params_vec.as_slice(), |row| row.get(0))?
                .filter_map(|row| row.ok())
                .collect();

            if orphan_hashes.is_empty() {
                continue;
            }

            let orphan_placeholders = std::iter::repeat("?")
                .take(orphan_hashes.len())
                .collect::<Vec<_>>()
                .join(", ");
            let mut orphan_params: Vec<&dyn rusqlite::ToSql> =
                Vec::with_capacity(orphan_hashes.len());
            for hash in &orphan_hashes {
                orphan_params.push(hash);
            }

            let external_paths: Vec<String> = tx
                .prepare(&format!(
                    "SELECT external_path FROM blobs WHERE hash IN ({orphan_placeholders}) AND external_path IS NOT NULL"
                ))?
                .query_map(orphan_params.as_slice(), |row| row.get(0))?
                .filter_map(|row| row.ok())
                .collect();

            for raw_path in external_paths {
                let candidate = PathBuf::from(&raw_path);
                let resolved = if candidate.is_absolute() {
                    candidate.canonicalize().ok()
                } else if let Some(root) = external_root.as_ref() {
                    let joined = root.join(&candidate);
                    let resolved = joined.canonicalize().ok();
                    if let Some(resolved_path) = resolved.as_ref() {
                        if !resolved_path.starts_with(root) {
                            external_skipped += 1;
                            continue;
                        }
                    }
                    resolved
                } else {
                    external_skipped += 1;
                    continue;
                };

                let Some(path) = resolved else {
                    continue;
                };
                if path.is_file() && fs::remove_file(&path).is_ok() {
                    external_deleted += 1;
                }
            }

            if has_fts > 0 {
                let fts_sql = format!(
                    "DELETE FROM response_body_fts WHERE hash IN ({orphan_placeholders})"
                );
                fts_deleted += tx.execute(&fts_sql, orphan_params.as_slice())?;
            }

            let blobs_sql = format!("DELETE FROM blobs WHERE hash IN ({orphan_placeholders})");
            blobs_deleted += tx.execute(&blobs_sql, orphan_params.as_slice())?;
        }
    }

    tx.commit()?;

    println!(
        "Pruned import {import_id} ({source_file}). Removed {imports_deleted} import record, {entries_deleted} entries, {pages_deleted} pages, {blobs_deleted} blobs, {fts_deleted} FTS rows, deleted {external_deleted} external files (skipped {external_skipped})."
    );

    Ok(())
}
