use std::path::{Path, PathBuf};
use std::{fs, path};

use indicatif::ProgressBar;
use rusqlite::Connection;

use crate::db::{
    create_import, create_schema, insert_entry, insert_page, update_import_count,
    ExtractBodiesKind, ImportStats, InsertEntryOptions,
};
use crate::error::{HarliteError, Result};
use crate::har::parse_har_file;

/// Options for importing HAR files.
pub struct ImportOptions {
    pub output: Option<PathBuf>,
    pub store_bodies: bool,
    pub max_body_size: Option<usize>,
    pub text_only: bool,
    pub show_stats: bool,
    pub decompress_bodies: bool,
    pub keep_compressed: bool,
    pub extract_bodies_dir: Option<PathBuf>,
    pub extract_bodies_kind: ExtractBodiesKind,
    pub extract_bodies_shard_depth: u8,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            output: None,
            store_bodies: false,
            max_body_size: Some(100 * 1024),
            text_only: false,
            show_stats: false,
            decompress_bodies: false,
            keep_compressed: false,
            extract_bodies_dir: None,
            extract_bodies_kind: ExtractBodiesKind::Both,
            extract_bodies_shard_depth: 0,
        }
    }
}

/// Import one or more HAR files into a SQLite database.
pub fn run_import(files: &[PathBuf], options: &ImportOptions) -> Result<ImportStats> {
    if files.is_empty() {
        return Err(HarliteError::InvalidHar(
            "No input files specified".to_string(),
        ));
    }
    if options.keep_compressed && !options.decompress_bodies {
        return Err(HarliteError::InvalidArgs(
            "--keep-compressed requires --decompress-bodies".to_string(),
        ));
    }

    let output_path = match &options.output {
        Some(p) => p.clone(),
        None => {
            let first_file = &files[0];
            let stem = first_file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("output");
            PathBuf::from(format!("{}.db", stem))
        }
    };

    let conn = Connection::open(&output_path)?;
    create_schema(&conn)?;

    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

    let extract_dir = if let Some(dir) = &options.extract_bodies_dir {
        fs::create_dir_all(dir)?;
        Some(path::Path::new(dir).canonicalize()?)
    } else {
        None
    };

    let entry_options = InsertEntryOptions {
        store_bodies: options.store_bodies || extract_dir.is_some(),
        max_body_size: options.max_body_size,
        text_only: options.text_only,
        decompress_bodies: options.decompress_bodies,
        keep_compressed: options.keep_compressed,
        extract_bodies_dir: extract_dir,
        extract_bodies_kind: options.extract_bodies_kind,
        extract_bodies_shard_depth: options.extract_bodies_shard_depth,
    };

    let mut total_stats = ImportStats {
        entries_imported: 0,
        blobs_created: 0,
        blobs_deduplicated: 0,
        bytes_stored: 0,
        bytes_deduplicated: 0,
    };

    for file_path in files {
        let stats = import_single_file(&conn, file_path, &entry_options)?;
        total_stats.entries_imported += stats.entries_imported;
        total_stats.blobs_created += stats.blobs_created;
        total_stats.blobs_deduplicated += stats.blobs_deduplicated;
        total_stats.bytes_stored += stats.bytes_stored;
        total_stats.bytes_deduplicated += stats.bytes_deduplicated;
    }

    if options.show_stats {
        print_stats(&total_stats);
    }

    println!(
        "Imported {} entries to {}",
        total_stats.entries_imported,
        output_path.display()
    );

    Ok(total_stats)
}

fn import_single_file(
    conn: &Connection,
    path: &Path,
    options: &InsertEntryOptions,
) -> Result<ImportStats> {
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    println!("Importing {}...", file_name);

    let har = parse_har_file(path)?;
    let import_id = create_import(conn, file_name)?;

    if let Some(pages) = &har.log.pages {
        for page in pages {
            insert_page(conn, import_id, page)?;
        }
    }

    let entries = &har.log.entries;
    let pb = ProgressBar::new(entries.len() as u64);

    let mut stats = ImportStats {
        entries_imported: 0,
        blobs_created: 0,
        blobs_deduplicated: 0,
        bytes_stored: 0,
        bytes_deduplicated: 0,
    };

    let tx = conn.unchecked_transaction()?;

    for entry in entries {
        let (blob_created, bytes) = insert_entry(&tx, import_id, entry, options)?;
        stats.entries_imported += 1;

        if options.store_bodies {
            if blob_created {
                stats.blobs_created += 1;
                stats.bytes_stored += bytes;
            } else if bytes > 0 {
                stats.blobs_deduplicated += 1;
                stats.bytes_deduplicated += bytes;
            }
        }

        pb.inc(1);
    }

    tx.commit()?;
    pb.finish_and_clear();

    update_import_count(conn, import_id, stats.entries_imported)?;

    Ok(stats)
}

fn print_stats(stats: &ImportStats) {
    println!("\nImport Statistics:");
    println!("  Entries imported: {}", stats.entries_imported);
    if stats.blobs_created > 0 || stats.blobs_deduplicated > 0 {
        println!("  Unique blobs stored: {}", stats.blobs_created);
        println!("  Duplicate blobs skipped: {}", stats.blobs_deduplicated);
        println!("  Bytes stored: {} KB", stats.bytes_stored / 1024);
        println!(
            "  Bytes saved by deduplication: {} KB",
            stats.bytes_deduplicated / 1024
        );
    }
}
