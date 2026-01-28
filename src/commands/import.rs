use std::path::{Path, PathBuf};
use std::{fs, path};

use indicatif::ProgressBar;
use rusqlite::Connection;

use crate::db::{
    create_import, create_schema, insert_entry, insert_page, update_import_count, BlobStats,
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
        request: BlobStats::default(),
        response: BlobStats::default(),
    };

    for file_path in files {
        let stats = import_single_file(&conn, file_path, &entry_options)?;
        total_stats.entries_imported += stats.entries_imported;
        total_stats.request.add_assign(stats.request);
        total_stats.response.add_assign(stats.response);
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
        request: BlobStats::default(),
        response: BlobStats::default(),
    };

    let tx = conn.unchecked_transaction()?;

    for entry in entries {
        let entry_stats = insert_entry(&tx, import_id, entry, options)?;
        stats.entries_imported += 1;

        stats.request.add_assign(entry_stats.request);
        stats.response.add_assign(entry_stats.response);

        pb.inc(1);
    }

    tx.commit()?;
    pb.finish_and_clear();

    update_import_count(conn, import_id, stats.entries_imported)?;

    Ok(stats)
}

fn print_stats(stats: &ImportStats) {
    let total_created = stats.request.created + stats.response.created;
    let total_deduplicated = stats.request.deduplicated + stats.response.deduplicated;
    let total_bytes_stored = stats.request.bytes_stored + stats.response.bytes_stored;
    let total_bytes_deduplicated =
        stats.request.bytes_deduplicated + stats.response.bytes_deduplicated;

    println!("\nImport Statistics:");
    println!("  Entries imported: {}", stats.entries_imported);
    if total_created > 0 || total_deduplicated > 0 {
        println!("  Unique blobs stored: {}", total_created);
        println!("  Duplicate blobs skipped: {}", total_deduplicated);
        println!("  Bytes stored: {} KB", total_bytes_stored / 1024);
        println!(
            "  Bytes saved by deduplication: {} KB",
            total_bytes_deduplicated / 1024
        );
    }
}
