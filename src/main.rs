use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process;

mod commands;
mod db;
mod error;
mod har;

use commands::{run_export, ExportOptions};
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

    /// Export a SQLite database back to HAR format
    Export {
        /// Database file to export
        database: PathBuf,

        /// Output HAR file (default: <database>.har). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Include stored request/response bodies in the HAR (if present)
        #[arg(long)]
        bodies: bool,

        /// Write compact JSON (disable pretty-printing)
        #[arg(long)]
        compact: bool,

        /// Exact URL match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url: Vec<String>,

        /// URL substring match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_contains: Vec<String>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Vec<String>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Vec<String>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Vec<String>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Vec<i32>,

        /// Response MIME type substring match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        mime: Vec<String>,

        /// URL extension filter (repeatable, comma-separated allowed; e.g. 'js,css,json')
        #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append)]
        ext: Vec<String>,

        /// Filter by import source filename (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        source: Vec<String>,

        /// Filter by import source filename substring match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        source_contains: Vec<String>,

        /// Only export entries on/after this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,

        /// Only export entries on/before this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,

        /// Minimum request body size (e.g., '1KB', '500B')
        #[arg(long)]
        min_request_size: Option<String>,

        /// Maximum request body size (e.g., '100KB', 'unlimited')
        #[arg(long)]
        max_request_size: Option<String>,

        /// Minimum response body size (e.g., '1KB', '500B')
        #[arg(long)]
        min_response_size: Option<String>,

        /// Maximum response body size (e.g., '100KB', 'unlimited')
        #[arg(long)]
        max_response_size: Option<String>,
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

        Commands::Export {
            database,
            output,
            bodies,
            compact,
            url,
            url_contains,
            url_regex,
            host,
            method,
            status,
            mime,
            ext,
            source,
            source_contains,
            from,
            to,
            min_request_size,
            max_request_size,
            min_response_size,
            max_response_size,
        } => {
            let options = ExportOptions {
                output,
                pretty: !compact,
                include_bodies: bodies,
                url,
                url_contains,
                url_regex,
                host,
                method,
                status,
                mime_contains: mime,
                ext,
                source,
                source_contains,
                from,
                to,
                min_request_size,
                max_request_size,
                min_response_size,
                max_response_size,
            };
            run_export(database, &options)
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
