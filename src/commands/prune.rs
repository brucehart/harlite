use std::fs;
use std::path::PathBuf;

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{HarliteError, Result};

use super::util::{prepare_sensitive_write, ExternalPathPolicy};

const HASH_CHUNK: usize = 500;

#[derive(Clone, Debug, Default)]
pub struct PruneOptions {
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
}

/// Remove all records for a specific import and prune orphaned blobs.
pub fn run_prune(database: PathBuf, import_id: i64) -> Result<()> {
    run_prune_with_options(database, import_id, &PruneOptions::default())
}

/// Remove all records for a specific import with an explicit policy for
/// deleting external blob files.
pub fn run_prune_with_options(
    database: PathBuf,
    import_id: i64,
    options: &PruneOptions,
) -> Result<()> {
    let external_path_policy = ExternalPathPolicy::new(
        &database,
        options.allow_external_paths,
        options.external_path_root.as_deref(),
    )?;
    let conn = Connection::open(&database)?;
    prepare_sensitive_write(&conn)?;

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

    let has_graphql_fields: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='graphql_fields')",
        [],
        |row| row.get(0),
    )?;
    if has_graphql_fields {
        tx.execute(
            "DELETE FROM graphql_fields WHERE entry_id IN (SELECT id FROM entries WHERE import_id = ?1)",
            params![import_id],
        )?;
    }

    let entries_deleted = tx.execute(
        "DELETE FROM entries WHERE import_id = ?1",
        params![import_id],
    )?;
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
                let Some(path) = external_path_policy.resolve_file(&raw_path) else {
                    external_skipped += 1;
                    continue;
                };
                if fs::remove_file(&path).is_ok() {
                    external_deleted += 1;
                } else {
                    external_skipped += 1;
                }
            }

            if has_fts > 0 {
                let fts_sql =
                    format!("DELETE FROM response_body_fts WHERE hash IN ({orphan_placeholders})");
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
