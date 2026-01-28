use clap::{CommandFactory, Parser, Subcommand};
use std::path::PathBuf;
use std::process;

mod commands;
mod db;
mod error;
mod har;
mod size;

use crate::db::ExtractBodiesKind;
use commands::StatsOptions;
use commands::{
    run_cdp, run_diff, run_export, run_fts_rebuild, run_import, run_imports, run_info, run_merge,
    run_prune, run_query, run_redact, run_schema, run_search, run_stats,
};
use commands::{
    CdpOptions, DedupStrategy, DiffOptions, ExportOptions, ImportOptions, MergeOptions,
    NameMatchMode, OutputFormat, QueryOptions, RedactOptions,
};

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

        /// Maximum body size to store (e.g., "100KB", "1.5MB", "1M", "100k", "unlimited")
        #[arg(long, default_value = "100KB")]
        max_body_size: String,

        /// Only store text-based bodies (HTML, JSON, JS, CSS, XML)
        #[arg(long)]
        text_only: bool,

        /// Show deduplication statistics after import
        #[arg(long)]
        stats: bool,

        /// Decompress response bodies based on Content-Encoding (gzip, br)
        #[arg(long)]
        decompress_bodies: bool,

        /// When decompressing, also store the original (compressed) response body
        #[arg(long)]
        keep_compressed: bool,

        /// Write bodies to external files under DIR (stored by hash); implies --bodies
        #[arg(long, value_name = "DIR")]
        extract_bodies: Option<PathBuf>,

        /// Which bodies to extract to files
        #[arg(long, value_enum, default_value = "both")]
        extract_bodies_kind: ExtractBodiesKind,

        /// Optional sharding depth for extracted bodies (each level uses 2 hex chars of the hash)
        #[arg(long, default_value_t = 0)]
        extract_bodies_shard_depth: u8,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Vec<String>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Vec<String>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Vec<i32>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Vec<String>,

        /// Only import entries on/after this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,

        /// Only import entries on/before this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
    },

    /// Capture network traffic from Chrome via CDP
    Cdp {
        /// Chrome host (CDP remote debugging address)
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Chrome remote debugging port
        #[arg(long, default_value_t = 9222)]
        port: u16,

        /// Target selector (id, URL, or title substring)
        #[arg(long)]
        target: Option<String>,

        /// Write captured HAR to this path
        #[arg(long, value_name = "FILE")]
        har: Option<PathBuf>,

        /// Output database file (imports captured entries)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Store response bodies (fetch via CDP)
        #[arg(long)]
        bodies: bool,

        /// Maximum body size to store (e.g., "100KB", "1.5MB", "unlimited")
        #[arg(long, default_value = "100KB")]
        max_body_size: String,

        /// Only store text-based bodies (HTML, JSON, JS, CSS, XML)
        #[arg(long)]
        text_only: bool,

        /// Stop capture after N seconds (omit to capture until Ctrl+C)
        #[arg(long, value_name = "SECONDS")]
        duration: Option<u64>,
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

    /// List import metadata for a database
    Imports {
        /// Database file to inspect
        database: PathBuf,
    },

    /// Remove entries for a specific import id
    Prune {
        /// Database file to modify
        database: PathBuf,

        /// Import id to remove
        #[arg(long)]
        import_id: i64,
    },

    /// Show lightweight database stats (script-friendly)
    Stats {
        /// Database file to inspect
        database: PathBuf,

        /// Output as JSON
        #[arg(long)]
        json: bool,
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

        /// Prefer raw/compressed response bodies when available
        #[arg(long)]
        bodies_raw: bool,

        /// Allow reading external blob paths from the database
        #[arg(long)]
        allow_external_paths: bool,

        /// Root directory for external blob paths (defaults to database directory)
        #[arg(long, value_name = "DIR")]
        external_path_root: Option<PathBuf>,

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

        /// Minimum request body size (e.g., '1KB', '1.5MB', '1M', '100k', '500B')
        #[arg(long)]
        min_request_size: Option<String>,

        /// Maximum request body size (e.g., '100KB', '1.5MB', '1M', '100k', 'unlimited')
        #[arg(long)]
        max_request_size: Option<String>,

        /// Minimum response body size (e.g., '1KB', '1.5MB', '1M', '100k', '500B')
        #[arg(long)]
        min_response_size: Option<String>,

        /// Maximum response body size (e.g., '100KB', '1.5MB', '1M', '100k', 'unlimited')
        #[arg(long)]
        max_response_size: Option<String>,
    },

    /// Redact sensitive headers/cookies in a harlite SQLite database
    Redact {
        /// Output database file (default: modify in-place)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Overwrite output database if it exists
        #[arg(long)]
        force: bool,

        /// Only report what would be redacted (no writes)
        #[arg(long)]
        dry_run: bool,

        /// Disable default redaction patterns
        #[arg(long)]
        no_defaults: bool,

        /// Header name pattern to redact (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        header: Vec<String>,

        /// Cookie name pattern to redact (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        cookie: Vec<String>,

        /// Query parameter name pattern to redact (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        query_param: Vec<String>,

        /// Regex pattern to redact from stored bodies (repeatable)
        #[arg(long = "body-regex", action = clap::ArgAction::Append)]
        body_regex: Vec<String>,

        /// Pattern matching mode for names
        #[arg(long = "match", value_enum, default_value = "wildcard")]
        match_mode: NameMatchMode,

        /// Replacement token to write for redacted values
        #[arg(long, default_value = "REDACTED")]
        token: String,

        /// Database file to redact (default: the only *.db in the current directory)
        database: Option<PathBuf>,
    },

    /// Compare two HAR files or two SQLite databases
    Diff {
        /// Left-hand HAR or database file
        left: PathBuf,

        /// Right-hand HAR or database file
        right: PathBuf,

        /// Output format
        #[arg(short, long, value_enum, default_value = "table")]
        format: OutputFormat,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Vec<String>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Vec<String>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Vec<i32>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Vec<String>,
    },

    /// Merge multiple harlite databases into one
    Merge {
        /// Database files to merge
        #[arg(required = true)]
        databases: Vec<PathBuf>,

        /// Output database file (default: <first-input>-merged.db)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Only report stats, do not write output
        #[arg(long)]
        dry_run: bool,

        /// Deduplication strategy for entries
        #[arg(long, value_enum, default_value = "hash")]
        dedup: DedupStrategy,
    },

    /// Run a SQL query against a harlite SQLite database
    Query {
        /// SQL string to execute (read-only)
        #[arg(required = true)]
        sql: String,

        /// Database file to query (default: the only *.db in the current directory)
        database: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum, default_value = "table")]
        format: OutputFormat,

        /// Limit rows (wraps the query)
        #[arg(long)]
        limit: Option<u64>,

        /// Offset rows (wraps the query)
        #[arg(long)]
        offset: Option<u64>,

        /// Suppress extra output (for piping)
        #[arg(long)]
        quiet: bool,
    },

    /// Search response bodies using SQLite full-text search (FTS5)
    Search {
        /// FTS query string (e.g., 'error NEAR/3 timeout')
        #[arg(required = true)]
        query: String,

        /// Database file to search (default: the only *.db in the current directory)
        database: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum, default_value = "table")]
        format: OutputFormat,

        /// Limit rows
        #[arg(long)]
        limit: Option<u64>,

        /// Offset rows
        #[arg(long)]
        offset: Option<u64>,

        /// Suppress extra output (for piping)
        #[arg(long)]
        quiet: bool,
    },

    /// Rebuild the response body FTS index for an existing database
    FtsRebuild {
        /// Database file to rebuild
        database: PathBuf,

        /// Tokenizer to use for the index
        #[arg(long, value_enum, default_value = "unicode61")]
        tokenizer: commands::FtsTokenizer,

        /// Maximum body size to index (e.g., '1MB', '1.5MB', '1M', '100k', 'unlimited')
        #[arg(long, default_value = "1MB")]
        max_body_size: String,

        /// Allow reading external blob paths from the database
        #[arg(long)]
        allow_external_paths: bool,

        /// Root directory for external blob paths (defaults to database directory)
        #[arg(long, value_name = "DIR")]
        external_path_root: Option<PathBuf>,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = (|| match cli.command {
        Commands::Import {
            files,
            output,
            bodies,
            max_body_size,
            text_only,
            stats,
            decompress_bodies,
            keep_compressed,
            extract_bodies,
            extract_bodies_kind,
            extract_bodies_shard_depth,
            host,
            method,
            status,
            url_regex,
            from,
            to,
        } => {
            let max_body_size = size::parse_size_bytes_usize(&max_body_size)?;
            let options = ImportOptions {
                output,
                store_bodies: bodies,
                max_body_size,
                text_only,
                show_stats: stats,
                decompress_bodies,
                keep_compressed,
                extract_bodies_dir: extract_bodies,
                extract_bodies_kind,
                extract_bodies_shard_depth,
                host,
                method,
                status,
                url_regex,
                from,
                to,
            };
            run_import(&files, &options).map(|_| ())
        }

        Commands::Cdp {
            host,
            port,
            target,
            har,
            output,
            bodies,
            max_body_size,
            text_only,
            duration,
        } => {
            let max_body_size = size::parse_size_bytes_usize(&max_body_size)?;
            let options = CdpOptions {
                host,
                port,
                target,
                output_har: har,
                output_db: output,
                store_bodies: bodies,
                max_body_size,
                text_only,
                duration_secs: duration,
            };
            run_cdp(&options)
        }

        Commands::Schema { database } => run_schema(database),

        Commands::Info { database } => run_info(database),

        Commands::Imports { database } => run_imports(database),

        Commands::Prune {
            database,
            import_id,
        } => run_prune(database, import_id),

        Commands::Stats { database, json } => {
            let options = StatsOptions { json };
            run_stats(database, &options)
        }

        Commands::Export {
            database,
            output,
            bodies,
            bodies_raw,
            allow_external_paths,
            external_path_root,
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
                include_bodies: bodies || bodies_raw,
                include_raw_response_bodies: bodies_raw,
                allow_external_paths,
                external_path_root,
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

        Commands::Redact {
            output,
            force,
            dry_run,
            no_defaults,
            header,
            cookie,
            query_param,
            body_regex,
            match_mode,
            token,
            database,
        } => {
            let options = RedactOptions {
                output,
                force,
                dry_run,
                no_defaults,
                headers: header,
                cookies: cookie,
                query_params: query_param,
                body_regexes: body_regex,
                match_mode,
                token,
            };
            run_redact(database, &options)
        }

        Commands::Diff {
            left,
            right,
            format,
            host,
            method,
            status,
            url_regex,
        } => {
            let options = DiffOptions {
                format,
                host,
                method,
                status,
                url_regex,
            };
            run_diff(left, right, &options)
        }

        Commands::Merge {
            databases,
            output,
            dry_run,
            dedup,
        } => {
            let options = MergeOptions {
                output,
                dry_run,
                dedup,
            };
            run_merge(databases, &options)
        }

        Commands::Query {
            sql,
            database,
            format,
            limit,
            offset,
            quiet,
        } => {
            let options = QueryOptions {
                format,
                limit,
                offset,
                quiet,
            };
            run_query(sql, database, &options)
        }

        Commands::Search {
            query,
            database,
            format,
            limit,
            offset,
            quiet,
        } => {
            let options = QueryOptions {
                format,
                limit,
                offset,
                quiet,
            };
            run_search(query, database, &options)
        }

        Commands::FtsRebuild {
            database,
            tokenizer,
            max_body_size,
            allow_external_paths,
            external_path_root,
        } => {
            let max_body_size = size::parse_size_bytes_usize(&max_body_size)?;
            run_fts_rebuild(
                database,
                tokenizer,
                max_body_size,
                allow_external_paths,
                external_path_root,
            )
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "harlite", &mut std::io::stdout());
            Ok(())
        }
    })();

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
