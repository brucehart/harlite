use std::path::PathBuf;

use rusqlite::Connection;

use crate::db::SCHEMA;
use crate::error::Result;

/// Print the schema for a harlite database or the default schema.
pub fn run_schema(database: Option<PathBuf>) -> Result<()> {
    match database {
        None => {
            println!("{}", SCHEMA);
        }
        Some(path) => {
            let conn = Connection::open(&path)?;
            let mut stmt = conn.prepare(
                "SELECT sql FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY type DESC, name",
            )?;

            let schemas: Vec<String> = stmt
                .query_map([], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();

            for schema in schemas {
                println!("{};", schema);
                println!();
            }
        }
    }
    Ok(())
}
