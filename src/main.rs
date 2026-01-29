use clap::{CommandFactory, Parser, Subcommand};
use std::path::PathBuf;
use std::process;

mod commands;
mod config;
mod db;
mod error;
mod har;
mod size;

use crate::config::{load_config, render_config, ResolvedConfig};
use crate::db::ExtractBodiesKind;
use commands::StatsOptions;
use commands::{
    run_cdp, run_diff, run_export, run_fts_rebuild, run_import, run_imports, run_info, run_merge,
    run_prune, run_query, run_redact, run_repl, run_schema, run_search, run_stats, run_watch,
};
use commands::{
    CdpOptions, DedupStrategy, DiffOptions, ExportOptions, ImportOptions, MergeOptions,
    NameMatchMode, OutputFormat, QueryOptions, RedactOptions, ReplOptions, WatchOptions,
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
        #[arg(long, action = clap::ArgAction::SetTrue)]
        bodies: Option<bool>,

        /// Maximum body size to store (e.g., "100KB", "1.5MB", "1M", "100k", "unlimited")
        #[arg(long)]
        max_body_size: Option<String>,

        /// Only store text-based bodies (HTML, JSON, JS, CSS, XML)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        text_only: Option<bool>,

        /// Show deduplication statistics after import
        #[arg(long, action = clap::ArgAction::SetTrue)]
        stats: Option<bool>,

        /// Skip entries already imported (by content hash)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        incremental: Option<bool>,

        /// Resume an incomplete import for the same source file (implies --incremental)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        resume: Option<bool>,

        /// Number of parallel import workers (0 = auto)
        #[arg(long)]
        jobs: Option<usize>,

        /// Read HAR files using a background reader thread (useful for large files)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        async_read: Option<bool>,

        /// Decompress response bodies based on Content-Encoding (gzip, br)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        decompress_bodies: Option<bool>,

        /// When decompressing, also store the original (compressed) response body
        #[arg(long, action = clap::ArgAction::SetTrue)]
        keep_compressed: Option<bool>,

        /// Write bodies to external files under DIR (stored by hash); implies --bodies
        #[arg(long, value_name = "DIR")]
        extract_bodies: Option<PathBuf>,

        /// Which bodies to extract to files
        #[arg(long, value_enum)]
        extract_bodies_kind: Option<ExtractBodiesKind>,

        /// Optional sharding depth for extracted bodies (each level uses 2 hex chars of the hash)
        #[arg(long)]
        extract_bodies_shard_depth: Option<u8>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Option<Vec<String>>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Option<Vec<String>>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Option<Vec<i32>>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Option<Vec<String>>,

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
        #[arg(long)]
        host: Option<String>,

        /// Chrome remote debugging port
        #[arg(long)]
        port: Option<u16>,

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
        #[arg(long, action = clap::ArgAction::SetTrue)]
        bodies: Option<bool>,

        /// Maximum body size to store (e.g., "100KB", "1.5MB", "unlimited")
        #[arg(long)]
        max_body_size: Option<String>,

        /// Only store text-based bodies (HTML, JSON, JS, CSS, XML)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        text_only: Option<bool>,

        /// Stop capture after N seconds (omit to capture until Ctrl+C)
        #[arg(long, value_name = "SECONDS")]
        duration: Option<u64>,
    },

    /// Watch a directory for new HAR files and auto-import
    Watch {
        /// Directory to watch for HAR files
        directory: PathBuf,

        /// Output database file (default: <directory-name>.db)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Watch subdirectories recursively
        #[arg(long, action = clap::ArgAction::SetTrue)]
        #[arg(long = "no-recursive", action = clap::ArgAction::SetFalse)]
        recursive: Option<bool>,

        /// Debounce window in milliseconds before considering a file ready
        #[arg(long, value_name = "MS")]
        debounce_ms: Option<u64>,

        /// Minimum stable time in milliseconds with no changes
        #[arg(long, value_name = "MS")]
        stable_ms: Option<u64>,

        /// Import existing HAR files on startup
        #[arg(long, action = clap::ArgAction::SetTrue)]
        import_existing: Option<bool>,

        /// Print `harlite info` after each import
        #[arg(long, action = clap::ArgAction::SetTrue)]
        post_info: Option<bool>,

        /// Print `harlite stats` after each import
        #[arg(long, action = clap::ArgAction::SetTrue)]
        post_stats: Option<bool>,

        /// Emit JSON when running post-import stats
        #[arg(long, action = clap::ArgAction::SetTrue)]
        post_stats_json: Option<bool>,

        /// Store response bodies in the database
        #[arg(long, action = clap::ArgAction::SetTrue)]
        bodies: Option<bool>,

        /// Maximum body size to store (e.g., "100KB", "1.5MB", "1M", "100k", "unlimited")
        #[arg(long)]
        max_body_size: Option<String>,

        /// Only store text-based bodies (HTML, JSON, JS, CSS, XML)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        text_only: Option<bool>,

        /// Show deduplication statistics after import
        #[arg(long, action = clap::ArgAction::SetTrue)]
        stats: Option<bool>,

        /// Skip entries already imported (by content hash)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        incremental: Option<bool>,

        /// Resume an incomplete import for the same source file (implies --incremental)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        resume: Option<bool>,

        /// Read HAR files using a background reader thread (useful for large files)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        async_read: Option<bool>,

        /// Decompress response bodies based on Content-Encoding (gzip, br)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        decompress_bodies: Option<bool>,

        /// When decompressing, also store the original (compressed) response body
        #[arg(long, action = clap::ArgAction::SetTrue)]
        keep_compressed: Option<bool>,

        /// Write bodies to external files under DIR (stored by hash); implies --bodies
        #[arg(long, value_name = "DIR")]
        extract_bodies: Option<PathBuf>,

        /// Which bodies to extract to files
        #[arg(long, value_enum)]
        extract_bodies_kind: Option<ExtractBodiesKind>,

        /// Optional sharding depth for extracted bodies (each level uses 2 hex chars of the hash)
        #[arg(long)]
        extract_bodies_shard_depth: Option<u8>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Option<Vec<String>>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Option<Vec<String>>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Option<Vec<i32>>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Option<Vec<String>>,

        /// Only import entries on/after this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,

        /// Only import entries on/before this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,
    },

    /// Print the database schema
    Schema {
        /// Database file to inspect (omit for default schema)
        database: Option<PathBuf>,
    },

    /// Print the resolved configuration
    Config,

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
        #[arg(long, action = clap::ArgAction::SetTrue)]
        json: Option<bool>,
    },

    /// Export a SQLite database back to HAR format
    Export {
        /// Database file to export
        database: PathBuf,

        /// Output HAR file (default: <database>.har). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Include stored request/response bodies in the HAR (if present)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        bodies: Option<bool>,

        /// Prefer raw/compressed response bodies when available
        #[arg(long, action = clap::ArgAction::SetTrue)]
        bodies_raw: Option<bool>,

        /// Allow reading external blob paths from the database
        #[arg(long, action = clap::ArgAction::SetTrue)]
        allow_external_paths: Option<bool>,

        /// Root directory for external blob paths (defaults to database directory)
        #[arg(long, value_name = "DIR")]
        external_path_root: Option<PathBuf>,

        /// Write compact JSON (disable pretty-printing)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        compact: Option<bool>,

        /// Exact URL match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url: Option<Vec<String>>,

        /// URL substring match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_contains: Option<Vec<String>>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Option<Vec<String>>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Option<Vec<String>>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Option<Vec<String>>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Option<Vec<i32>>,

        /// Response MIME type substring match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        mime: Option<Vec<String>>,

        /// URL extension filter (repeatable, comma-separated allowed; e.g. 'js,css,json')
        #[arg(long, value_delimiter = ',', action = clap::ArgAction::Append)]
        ext: Option<Vec<String>>,

        /// Filter by import source filename (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        source: Option<Vec<String>>,

        /// Filter by import source filename substring match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        source_contains: Option<Vec<String>>,

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
        #[arg(long, action = clap::ArgAction::SetTrue)]
        force: Option<bool>,

        /// Only report what would be redacted (no writes)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        dry_run: Option<bool>,

        /// Disable default redaction patterns
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_defaults: Option<bool>,

        /// Header name pattern to redact (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        header: Option<Vec<String>>,

        /// Cookie name pattern to redact (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        cookie: Option<Vec<String>>,

        /// Query parameter name pattern to redact (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        query_param: Option<Vec<String>>,

        /// Regex pattern to redact from stored bodies (repeatable)
        #[arg(long = "body-regex", action = clap::ArgAction::Append)]
        body_regex: Option<Vec<String>>,

        /// Pattern matching mode for names
        #[arg(long = "match", value_enum)]
        match_mode: Option<NameMatchMode>,

        /// Replacement token to write for redacted values
        #[arg(long)]
        token: Option<String>,

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
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Option<Vec<String>>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Option<Vec<String>>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Option<Vec<i32>>,

        /// URL regex match (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_regex: Option<Vec<String>>,
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
        #[arg(long, action = clap::ArgAction::SetTrue)]
        dry_run: Option<bool>,

        /// Deduplication strategy for entries
        #[arg(long, value_enum)]
        dedup: Option<DedupStrategy>,
    },

    /// Run a SQL query against a harlite SQLite database
    Query {
        /// SQL string to execute (read-only)
        #[arg(required = true)]
        sql: String,

        /// Database file to query (default: the only *.db in the current directory)
        database: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,

        /// Limit rows (wraps the query)
        #[arg(long)]
        limit: Option<u64>,

        /// Offset rows (wraps the query)
        #[arg(long)]
        offset: Option<u64>,

        /// Suppress extra output (for piping)
        #[arg(long, action = clap::ArgAction::SetTrue, conflicts_with = "no_quiet")]
        quiet: Option<bool>,

        /// Always show headers and row counts (overrides config quiet)
        #[arg(long = "no-quiet", action = clap::ArgAction::SetTrue, conflicts_with = "quiet")]
        no_quiet: bool,
    },

    /// Start an interactive SQL REPL
    Repl {
        /// Database file to open (default: the only *.db in the current directory)
        database: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,
    },

    /// Search response bodies using SQLite full-text search (FTS5)
    Search {
        /// FTS query string (e.g., 'error NEAR/3 timeout')
        #[arg(required = true)]
        query: String,

        /// Database file to search (default: the only *.db in the current directory)
        database: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,

        /// Limit rows
        #[arg(long)]
        limit: Option<u64>,

        /// Offset rows
        #[arg(long)]
        offset: Option<u64>,

        /// Suppress extra output (for piping)
        #[arg(long, action = clap::ArgAction::SetTrue, conflicts_with = "no_quiet")]
        quiet: Option<bool>,

        /// Always show headers and row counts (overrides config quiet)
        #[arg(long = "no-quiet", action = clap::ArgAction::SetTrue, conflicts_with = "quiet")]
        no_quiet: bool,
    },

    /// Rebuild the response body FTS index for an existing database
    FtsRebuild {
        /// Database file to rebuild
        database: PathBuf,

        /// Tokenizer to use for the index
        #[arg(long, value_enum)]
        tokenizer: Option<commands::FtsTokenizer>,

        /// Maximum body size to index (e.g., '1MB', '1.5MB', '1M', '100k', 'unlimited')
        #[arg(long)]
        max_body_size: Option<String>,

        /// Allow reading external blob paths from the database
        #[arg(long, action = clap::ArgAction::SetTrue)]
        allow_external_paths: Option<bool>,

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

    let result = (|| {
        let config = load_config()?;
        let resolved = ResolvedConfig::from_config(&config);

        match cli.command {
            Commands::Import {
                files,
                output,
                bodies,
                max_body_size,
                text_only,
                stats,
                incremental,
                resume,
                jobs,
                async_read,
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
                let defaults = &resolved.import;
                let max_body_size = size::parse_size_bytes_usize(
                    &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
                )?;
                let options = ImportOptions {
                    output: output.or_else(|| defaults.output.clone()),
                    store_bodies: bodies.unwrap_or(defaults.bodies),
                    max_body_size,
                    text_only: text_only.unwrap_or(defaults.text_only),
                    show_stats: stats.unwrap_or(defaults.stats),
                    incremental: incremental.unwrap_or(defaults.incremental),
                    resume: resume.unwrap_or(defaults.resume),
                    jobs: jobs.unwrap_or(defaults.jobs),
                    async_read: async_read.unwrap_or(defaults.async_read),
                    decompress_bodies: decompress_bodies.unwrap_or(defaults.decompress_bodies),
                    keep_compressed: keep_compressed.unwrap_or(defaults.keep_compressed),
                    extract_bodies_dir: extract_bodies.or_else(|| defaults.extract_bodies.clone()),
                    extract_bodies_kind: extract_bodies_kind
                        .unwrap_or(defaults.extract_bodies_kind),
                    extract_bodies_shard_depth: extract_bodies_shard_depth
                        .unwrap_or(defaults.extract_bodies_shard_depth),
                    host: host.unwrap_or_else(|| defaults.host.clone()),
                    method: method.unwrap_or_else(|| defaults.method.clone()),
                    status: status.unwrap_or_else(|| defaults.status.clone()),
                    url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                    from: from.or_else(|| defaults.from.clone()),
                    to: to.or_else(|| defaults.to.clone()),
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
                let defaults = &resolved.cdp;
                let max_body_size = size::parse_size_bytes_usize(
                    &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
                )?;
                let options = CdpOptions {
                    host: host.unwrap_or_else(|| defaults.host.clone()),
                    port: port.unwrap_or(defaults.port),
                    target: target.or_else(|| defaults.target.clone()),
                    output_har: har.or_else(|| defaults.har.clone()),
                    output_db: output.or_else(|| defaults.output.clone()),
                    store_bodies: bodies.unwrap_or(defaults.bodies),
                    max_body_size,
                    text_only: text_only.unwrap_or(defaults.text_only),
                    duration_secs: duration.or(defaults.duration),
                };
                run_cdp(&options)
            }

            Commands::Watch {
                directory,
                output,
                recursive,
                debounce_ms,
                stable_ms,
                import_existing,
                post_info,
                post_stats,
                post_stats_json,
                bodies,
                max_body_size,
                text_only,
                stats,
                incremental,
                resume,
                async_read,
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
                let defaults = &resolved.import;
                let output_override = output.clone();
                let max_body_size = size::parse_size_bytes_usize(
                    &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
                )?;
                let import_options = ImportOptions {
                    output: output_override.clone().or_else(|| defaults.output.clone()),
                    store_bodies: bodies.unwrap_or(defaults.bodies),
                    max_body_size,
                    text_only: text_only.unwrap_or(defaults.text_only),
                    show_stats: stats.unwrap_or(defaults.stats),
                    incremental: incremental.unwrap_or(defaults.incremental),
                    resume: resume.unwrap_or(defaults.resume),
                    jobs: 1,
                    async_read: async_read.unwrap_or(defaults.async_read),
                    decompress_bodies: decompress_bodies.unwrap_or(defaults.decompress_bodies),
                    keep_compressed: keep_compressed.unwrap_or(defaults.keep_compressed),
                    extract_bodies_dir: extract_bodies.or_else(|| defaults.extract_bodies.clone()),
                    extract_bodies_kind: extract_bodies_kind
                        .unwrap_or(defaults.extract_bodies_kind),
                    extract_bodies_shard_depth: extract_bodies_shard_depth
                        .unwrap_or(defaults.extract_bodies_shard_depth),
                    host: host.unwrap_or_else(|| defaults.host.clone()),
                    method: method.unwrap_or_else(|| defaults.method.clone()),
                    status: status.unwrap_or_else(|| defaults.status.clone()),
                    url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                    from: from.or_else(|| defaults.from.clone()),
                    to: to.or_else(|| defaults.to.clone()),
                };

                let watch_options = WatchOptions {
                    output: output_override.or_else(|| defaults.output.clone()),
                    recursive: recursive.unwrap_or(true),
                    debounce_ms: debounce_ms.unwrap_or(750),
                    stable_ms: stable_ms.unwrap_or(1000),
                    import_existing: import_existing.unwrap_or(false),
                    post_info: post_info.unwrap_or(false),
                    post_stats: post_stats.unwrap_or(false) || post_stats_json.unwrap_or(false),
                    post_stats_json: post_stats_json.unwrap_or(false),
                    import_options,
                };

                run_watch(directory, &watch_options)
            }

            Commands::Config => {
                let rendered = render_config(&resolved)?;
                println!("{rendered}");
                Ok(())
            }

            Commands::Schema { database } => run_schema(database),

            Commands::Info { database } => run_info(database),

            Commands::Imports { database } => run_imports(database),

            Commands::Prune {
                database,
                import_id,
            } => run_prune(database, import_id),

            Commands::Stats { database, json } => {
                let defaults = &resolved.stats;
                let options = StatsOptions {
                    json: json.unwrap_or(defaults.json),
                };
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
                let defaults = &resolved.export;
                let compact = compact.unwrap_or(defaults.compact);
                let bodies = bodies.unwrap_or(defaults.bodies);
                let bodies_raw = bodies_raw.unwrap_or(defaults.bodies_raw);
                let options = ExportOptions {
                    output: output.or_else(|| defaults.output.clone()),
                    pretty: !compact,
                    include_bodies: bodies || bodies_raw,
                    include_raw_response_bodies: bodies_raw,
                    allow_external_paths: allow_external_paths
                        .unwrap_or(defaults.allow_external_paths),
                    external_path_root: external_path_root
                        .or_else(|| defaults.external_path_root.clone()),
                    url: url.unwrap_or_else(|| defaults.url.clone()),
                    url_contains: url_contains.unwrap_or_else(|| defaults.url_contains.clone()),
                    url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                    host: host.unwrap_or_else(|| defaults.host.clone()),
                    method: method.unwrap_or_else(|| defaults.method.clone()),
                    status: status.unwrap_or_else(|| defaults.status.clone()),
                    mime_contains: mime.unwrap_or_else(|| defaults.mime.clone()),
                    ext: ext.unwrap_or_else(|| defaults.ext.clone()),
                    source: source.unwrap_or_else(|| defaults.source.clone()),
                    source_contains: source_contains
                        .unwrap_or_else(|| defaults.source_contains.clone()),
                    from: from.or_else(|| defaults.from.clone()),
                    to: to.or_else(|| defaults.to.clone()),
                    min_request_size: min_request_size
                        .or_else(|| defaults.min_request_size.clone()),
                    max_request_size: max_request_size
                        .or_else(|| defaults.max_request_size.clone()),
                    min_response_size: min_response_size
                        .or_else(|| defaults.min_response_size.clone()),
                    max_response_size: max_response_size
                        .or_else(|| defaults.max_response_size.clone()),
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
                let defaults = &resolved.redact;
                let options = RedactOptions {
                    output: output.or_else(|| defaults.output.clone()),
                    force: force.unwrap_or(defaults.force),
                    dry_run: dry_run.unwrap_or(defaults.dry_run),
                    no_defaults: no_defaults.unwrap_or(defaults.no_defaults),
                    headers: header.unwrap_or_else(|| defaults.header.clone()),
                    cookies: cookie.unwrap_or_else(|| defaults.cookie.clone()),
                    query_params: query_param.unwrap_or_else(|| defaults.query_param.clone()),
                    body_regexes: body_regex.unwrap_or_else(|| defaults.body_regex.clone()),
                    match_mode: match_mode.unwrap_or(defaults.match_mode),
                    token: token.unwrap_or_else(|| defaults.token.clone()),
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
                let defaults = &resolved.diff;
                let options = DiffOptions {
                    format: format.unwrap_or(defaults.format),
                    host: host.unwrap_or_else(|| defaults.host.clone()),
                    method: method.unwrap_or_else(|| defaults.method.clone()),
                    status: status.unwrap_or_else(|| defaults.status.clone()),
                    url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                };
                run_diff(left, right, &options)
            }

            Commands::Merge {
                databases,
                output,
                dry_run,
                dedup,
            } => {
                let defaults = &resolved.merge;
                let options = MergeOptions {
                    output: output.or_else(|| defaults.output.clone()),
                    dry_run: dry_run.unwrap_or(defaults.dry_run),
                    dedup: dedup.unwrap_or(defaults.dedup),
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
                no_quiet,
            } => {
                let defaults = &resolved.query;
                let quiet = if no_quiet { Some(false) } else { quiet };
                let options = QueryOptions {
                    format: format.unwrap_or(defaults.format),
                    limit: limit.or(defaults.limit),
                    offset: offset.or(defaults.offset),
                    quiet: quiet.unwrap_or(defaults.quiet),
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
                no_quiet,
            } => {
                let defaults = &resolved.search;
                let quiet = if no_quiet { Some(false) } else { quiet };
                let options = QueryOptions {
                    format: format.unwrap_or(defaults.format),
                    limit: limit.or(defaults.limit),
                    offset: offset.or(defaults.offset),
                    quiet: quiet.unwrap_or(defaults.quiet),
                };
                run_search(query, database, &options)
            }

            Commands::Repl { database, format } => {
                let defaults = &resolved.repl;
                let options = ReplOptions {
                    format: format.unwrap_or(defaults.format),
                };
                run_repl(database, &options)
            }

            Commands::FtsRebuild {
                database,
                tokenizer,
                max_body_size,
                allow_external_paths,
                external_path_root,
            } => {
                let defaults = &resolved.fts_rebuild;
                let max_body_size = size::parse_size_bytes_usize(
                    &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
                )?;
                run_fts_rebuild(
                    database,
                    tokenizer.unwrap_or(defaults.tokenizer),
                    max_body_size,
                    allow_external_paths.unwrap_or(defaults.allow_external_paths),
                    external_path_root.or_else(|| defaults.external_path_root.clone()),
                )
            }
            Commands::Completions { shell } => {
                let mut cmd = Cli::command();
                clap_complete::generate(shell, &mut cmd, "harlite", &mut std::io::stdout());
                Ok(())
            }
        }
    })();

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
