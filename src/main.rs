use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process;

mod commands;
mod db;
mod error;
mod har;

use commands::{run_import, run_info, run_schema, ImportOptions};

#[derive(Parser)]
#[command(name = "harlite")]
#[command(about = "Import HAR files into SQLite. Query your web traffic with SQL.")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import HAR file(s) into a SQLite database
    Import {
        /// HAR file(s) to import
        #[arg(required = true)]
        files: Vec<PathBuf>,

        /// Output database file (default: <first-input>.db)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Store response bodies in the database
        #[arg(long)]
        bodies: bool,

        /// Maximum body size to store (e.g., "100KB", "1MB", "unlimited")
        #[arg(long, default_value = "100KB")]
        max_body_size: String,

        /// Only store text-based bodies (HTML, JSON, JS, CSS, XML)
        #[arg(long)]
        text_only: bool,

        /// Show deduplication statistics after import
        #[arg(long)]
        stats: bool,
    },

    /// Print the database schema
    Schema {
        /// Database file to inspect (omit for default schema)
        database: Option<PathBuf>,
    },

    /// Show information about a database
    Info {
        /// Database file to inspect
        database: PathBuf,
    },
}

fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim().to_lowercase();
    if s == "unlimited" {
        return None;
    }

    let (num, mult) = if s.ends_with("kb") {
        (s.trim_end_matches("kb").trim(), 1024)
    } else if s.ends_with("mb") {
        (s.trim_end_matches("mb").trim(), 1024 * 1024)
    } else if s.ends_with("gb") {
        (s.trim_end_matches("gb").trim(), 1024 * 1024 * 1024)
    } else {
        (s.as_str(), 1)
    };

    num.parse::<usize>().ok().map(|n| n * mult)
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Import {
            files,
            output,
            bodies,
            max_body_size,
            text_only,
            stats,
        } => {
            let options = ImportOptions {
                output,
                store_bodies: bodies,
                max_body_size: parse_size(&max_body_size),
                text_only,
                show_stats: stats,
            };
            run_import(&files, &options).map(|_| ())
        }

        Commands::Schema { database } => run_schema(database),

        Commands::Info { database } => run_info(database),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
