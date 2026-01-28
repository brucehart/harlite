use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::db::{create_schema, load_blobs_by_hashes, BlobRow};
use crate::error::Result;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum FtsTokenizer {
    Unicode61,
    Porter,
    Trigram,
}

impl FtsTokenizer {
    fn as_sql(&self) -> &'static str {
        match self {
            FtsTokenizer::Unicode61 => "unicode61",
            FtsTokenizer::Porter => "porter",
            FtsTokenizer::Trigram => "trigram",
        }
    }
}

fn is_text_mime_type(mime: Option<&str>) -> bool {
    match mime {
        None => false,
        Some(m) => {
            let m = m.to_lowercase();
            m.contains("text/")
                || m.contains("json")
                || m.contains("xml")
                || m.contains("javascript")
                || m.contains("css")
                || m.contains("html")
                || m.contains("x-www-form-urlencoded")
        }
    }
}

fn load_external_blob_content(mut blob: BlobRow, external_root: Option<&Path>) -> Result<BlobRow> {
    if !blob.content.is_empty() || blob.size <= 0 {
        return Ok(blob);
    }
    let Some(path) = &blob.external_path else {
        return Ok(blob);
    };
    let Some(root) = external_root else {
        return Ok(blob);
    };
    let candidate = PathBuf::from(path);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    };
    let resolved = match candidate.canonicalize() {
        Ok(p) => p,
        Err(_) => return Ok(blob),
    };
    if !resolved.starts_with(root) {
        return Ok(blob);
    }
    blob.content = std::fs::read(resolved)?;
    Ok(blob)
}

pub fn run_fts_rebuild(
    database: PathBuf,
    tokenizer: FtsTokenizer,
    max_body_size: Option<usize>,
    allow_external_paths: bool,
    external_path_root: Option<PathBuf>,
) -> Result<()> {
    let conn = Connection::open(&database)?;
    create_schema(&conn)?;
    let external_root = if allow_external_paths {
        let root = external_path_root
            .or_else(|| database.parent().map(|p| p.to_path_buf()))
            .ok_or_else(|| {
                crate::error::HarliteError::InvalidArgs(
                    "Cannot resolve external path root; pass --external-path-root".to_string(),
                )
            })?;
        Some(root.canonicalize()?)
    } else {
        None
    };

    let tokenizer = tokenizer.as_sql();
    conn.execute_batch("DROP TABLE IF EXISTS response_body_fts;")?;
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE response_body_fts USING fts5(hash UNINDEXED, body, tokenize = '{tokenizer}');"
    ))?;

    let mut stmt = conn.prepare(
        "SELECT DISTINCT response_body_hash FROM entries WHERE response_body_hash IS NOT NULL",
    )?;
    let hashes_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

    let mut hashes: Vec<String> = Vec::new();
    for h in hashes_iter {
        if let Ok(hash) = h {
            hashes.push(hash);
        }
    }

    // Keep ordering stable, but dedup defensively.
    let mut seen: HashSet<String> = HashSet::new();
    hashes.retain(|h| seen.insert(h.clone()));

    let tx = conn.unchecked_transaction()?;

    let mut indexed = 0usize;
    for chunk in hashes.chunks(900) {
        let blobs = load_blobs_by_hashes(&tx, chunk)?;
        for blob in blobs
            .into_iter()
            .map(|b| load_external_blob_content(b, external_root.as_deref()))
            .collect::<Result<Vec<_>>>()?
        {
            if blob.content.is_empty() {
                continue;
            }
            if max_body_size.is_some_and(|max| blob.content.len() > max) {
                continue;
            }
            if blob.mime_type.is_some() && !is_text_mime_type(blob.mime_type.as_deref()) {
                continue;
            }

            let Ok(text) = std::str::from_utf8(&blob.content) else {
                continue;
            };

            tx.execute(
                "INSERT INTO response_body_fts (hash, body) VALUES (?1, ?2)",
                rusqlite::params![blob.hash, text],
            )?;
            indexed += 1;
        }
    }

    tx.commit()?;
    println!(
        "Rebuilt response body FTS index (tokenizer={}) with {} documents",
        tokenizer, indexed
    );

    Ok(())
}
