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
use commands::{
    run_analyze, run_cdp, run_diff, run_export, run_export_data, run_fts_rebuild, run_import,
    run_imports, run_info, run_merge, run_openapi, run_otel, run_pii, run_prune, run_query,
    run_redact, run_repl, run_replay, run_schema, run_search, run_stats, run_watch, run_waterfall,
};
use commands::{
    AnalyzeOptions, CdpOptions, DataExportFormat, DedupStrategy, DiffOptions, EntryFilterOptions,
    ExportDataOptions, ExportOptions, ImportOptions, MergeOptions, NameMatchMode, OpenApiOptions,
    OtelExportFormat, OtelExportOptions, OutputFormat, PiiOptions, QueryOptions, RedactOptions,
    ReplOptions, ReplayOptions, WatchOptions, WaterfallFormat, WaterfallGroupBy, WaterfallOptions,
};
use commands::{InfoOptions, StatsOptions};

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

        /// Report certificates expiring within N days
        #[arg(long)]
        cert_expiring_days: Option<u64>,
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

        /// Report certificates expiring within N days
        #[arg(long)]
        cert_expiring_days: Option<u64>,
    },

    /// Analyze performance timings and caching opportunities
    Analyze {
        /// Database file to inspect
        database: PathBuf,

        /// Output as JSON
        #[arg(long, action = clap::ArgAction::SetTrue)]
        json: Option<bool>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Option<Vec<String>>,

        /// HTTP method filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        method: Option<Vec<String>>,

        /// HTTP status filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        status: Option<Vec<i32>>,

        /// Only include entries on/after this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,

        /// Only include entries on/before this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,

        /// Threshold for slow requests by total time (ms)
        #[arg(long)]
        slow_total_ms: Option<f64>,

        /// Threshold for slow requests by TTFB (ms)
        #[arg(long)]
        slow_ttfb_ms: Option<f64>,

        /// Limit for top N lists
        #[arg(long)]
        top: Option<usize>,
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

    /// Export entries to CSV/JSONL/Parquet
    #[command(name = "export-data")]
    ExportData {
        /// Database file to export
        database: PathBuf,

        /// Output file (default: <database>.<ext>). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum, default_value_t = DataExportFormat::Jsonl)]
        format: DataExportFormat,

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

    /// Export entries as OpenTelemetry spans
    #[command(name = "otel")]
    Otel {
        /// Database file to export
        database: PathBuf,

        /// Output format (json, otlp-http, otlp-grpc)
        #[arg(short, long, value_enum, default_value_t = OtelExportFormat::Json)]
        format: OtelExportFormat,

        /// Output file for JSON (default: stdout). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// OTLP endpoint (required for otlp-http/otlp-grpc)
        #[arg(long)]
        endpoint: Option<String>,

        /// Service name for resource attributes
        #[arg(long, default_value = "harlite")]
        service_name: String,

        /// Extra resource attributes (key=value, repeatable)
        #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
        resource_attr: Option<Vec<String>>,

        /// Disable phase spans for timing breakdowns
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_phases: bool,

        /// Deterministic sampling rate (0.0 - 1.0)
        #[arg(long, default_value_t = 1.0)]
        sample_rate: f64,

        /// Maximum number of root spans to export
        #[arg(long)]
        max_spans: Option<usize>,

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

    /// Generate an OpenAPI schema from captured traffic
    #[command(name = "openapi")]
    OpenApi {
        /// Database file to inspect
        database: PathBuf,

        /// Output file (default: <database>-openapi.json). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// OpenAPI title
        #[arg(long)]
        title: Option<String>,

        /// OpenAPI version
        #[arg(long)]
        version: Option<String>,

        /// Sample up to N request/response bodies per operation (opt-in)
        #[arg(long)]
        sample_bodies: Option<usize>,

        /// Maximum body size to sample (e.g., '100KB', '1.5MB', '1M', '100k', 'unlimited')
        #[arg(long)]
        sample_body_max_size: Option<String>,

        /// Allow reading external blob paths from the database
        #[arg(long, action = clap::ArgAction::SetTrue)]
        allow_external_paths: Option<bool>,

        /// Root directory for external blob paths (defaults to database directory)
        #[arg(long, value_name = "DIR")]
        external_path_root: Option<PathBuf>,

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

    /// Export request waterfall timing data
    Waterfall {
        /// Database file to inspect
        database: PathBuf,

        /// Output file (default: stdout). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Output format
        #[arg(short, long, value_enum)]
        format: Option<WaterfallFormat>,

        /// Group requests by page, top-level navigation, or none
        #[arg(long, value_enum)]
        group_by: Option<WaterfallGroupBy>,

        /// Hostname filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        host: Option<Vec<String>>,

        /// Page id or title substring filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        page: Option<Vec<String>>,

        /// Only include entries on/after this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,

        /// Only include entries on/before this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        to: Option<String>,

        /// Width of the ASCII timeline (text format only)
        #[arg(long)]
        width: Option<usize>,
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

    /// Scan for PII in URLs and bodies
    Pii {
        /// Output format
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,

        /// Automatically redact findings (write changes)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        redact: Option<bool>,

        /// Output database file (default: modify in-place when --redact)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Overwrite output database if it exists
        #[arg(long, action = clap::ArgAction::SetTrue)]
        force: Option<bool>,

        /// Only report what would be redacted (no writes)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        dry_run: Option<bool>,

        /// Disable default PII patterns
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_defaults: Option<bool>,

        /// Disable default email detection
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_email: Option<bool>,

        /// Disable default phone detection
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_phone: Option<bool>,

        /// Disable default SSN detection
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_ssn: Option<bool>,

        /// Disable default credit card detection
        #[arg(long, action = clap::ArgAction::SetTrue)]
        no_credit_card: Option<bool>,

        /// Regex pattern to detect emails (repeatable)
        #[arg(long = "email-regex", action = clap::ArgAction::Append)]
        email_regex: Option<Vec<String>>,

        /// Regex pattern to detect phone numbers (repeatable)
        #[arg(long = "phone-regex", action = clap::ArgAction::Append)]
        phone_regex: Option<Vec<String>>,

        /// Regex pattern to detect SSNs (repeatable)
        #[arg(long = "ssn-regex", action = clap::ArgAction::Append)]
        ssn_regex: Option<Vec<String>>,

        /// Regex pattern to detect credit cards (repeatable)
        #[arg(long = "credit-card-regex", action = clap::ArgAction::Append)]
        credit_card_regex: Option<Vec<String>>,

        /// Replacement token to write for redacted values
        #[arg(long)]
        token: Option<String>,

        /// Database file to scan (default: the only *.db in the current directory)
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

    /// Replay requests against live servers and compare responses
    Replay {
        /// HAR file or SQLite database to replay
        input: PathBuf,

        /// Output format
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,

        /// Number of concurrent requests (0 = auto)
        #[arg(long)]
        concurrency: Option<usize>,

        /// Rate limit in requests per second
        #[arg(long)]
        rate_limit: Option<f64>,

        /// Request timeout in seconds
        #[arg(long)]
        timeout: Option<u64>,

        /// Allow unsafe methods (POST, PUT, DELETE, PATCH)
        #[arg(long, action = clap::ArgAction::SetTrue)]
        allow_unsafe: Option<bool>,

        /// Allow reading external blob paths from the database
        #[arg(long, action = clap::ArgAction::SetTrue)]
        allow_external_paths: Option<bool>,

        /// Root directory for external blob paths (defaults to database directory)
        #[arg(long, value_name = "DIR")]
        external_path_root: Option<PathBuf>,

        /// Exact URL filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url: Option<Vec<String>>,

        /// URL substring filter (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        url_contains: Option<Vec<String>>,

        /// URL regex filter (repeatable)
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

        /// Override host by URL regex (repeatable, format: '<regex>=<host[:port]>' )
        #[arg(long, action = clap::ArgAction::Append)]
        override_host: Option<Vec<String>>,

        /// Override header by URL regex (repeatable, format: '<regex>:<name>=<value>' or '<name>=<value>')
        #[arg(long, action = clap::ArgAction::Append)]
        override_header: Option<Vec<String>>,
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

            Commands::Info {
                database,
                cert_expiring_days,
            } => {
                let options = InfoOptions { cert_expiring_days };
                run_info(database, &options)
            }

            Commands::Imports { database } => run_imports(database),

            Commands::Prune {
                database,
                import_id,
            } => run_prune(database, import_id),

            Commands::Stats {
                database,
                json,
                cert_expiring_days,
            } => {
                let defaults = &resolved.stats;
                let options = StatsOptions {
                    json: json.unwrap_or(defaults.json),
                    cert_expiring_days: cert_expiring_days.or(defaults.cert_expiring_days),
                };
                run_stats(database, &options)
            }

            Commands::Analyze {
                database,
                json,
                host,
                method,
                status,
                from,
                to,
                slow_total_ms,
                slow_ttfb_ms,
                top,
            } => {
                let options = AnalyzeOptions {
                    json: json.unwrap_or(false),
                    host: host.unwrap_or_default(),
                    method: method.unwrap_or_default(),
                    status: status.unwrap_or_default(),
                    from,
                    to,
                    slow_total_ms: slow_total_ms.unwrap_or(1000.0),
                    slow_ttfb_ms: slow_ttfb_ms.unwrap_or(500.0),
                    top: top.unwrap_or(10),
                };
                run_analyze(database, &options)
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

            Commands::ExportData {
                database,
                output,
                format,
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
                let filters = EntryFilterOptions {
                    url: url.unwrap_or_default(),
                    url_contains: url_contains.unwrap_or_default(),
                    url_regex: url_regex.unwrap_or_default(),
                    host: host.unwrap_or_default(),
                    method: method.unwrap_or_default(),
                    status: status.unwrap_or_default(),
                    mime_contains: mime.unwrap_or_default(),
                    ext: ext.unwrap_or_default(),
                    source: source.unwrap_or_default(),
                    source_contains: source_contains.unwrap_or_default(),
                    from,
                    to,
                    min_request_size,
                    max_request_size,
                    min_response_size,
                    max_response_size,
                };
                let options = ExportDataOptions {
                    output,
                    format,
                    filters,
                };
                run_export_data(database, &options)
            }

            Commands::Otel {
                database,
                format,
                output,
                endpoint,
                service_name,
                resource_attr,
                no_phases,
                sample_rate,
                max_spans,
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
                let filters = EntryFilterOptions {
                    url: url.unwrap_or_default(),
                    url_contains: url_contains.unwrap_or_default(),
                    url_regex: url_regex.unwrap_or_default(),
                    host: host.unwrap_or_default(),
                    method: method.unwrap_or_default(),
                    status: status.unwrap_or_default(),
                    mime_contains: mime.unwrap_or_default(),
                    ext: ext.unwrap_or_default(),
                    source: source.unwrap_or_default(),
                    source_contains: source_contains.unwrap_or_default(),
                    from,
                    to,
                    min_request_size,
                    max_request_size,
                    min_response_size,
                    max_response_size,
                };
                let options = OtelExportOptions {
                    format,
                    output,
                    endpoint,
                    service_name,
                    resource_attr: resource_attr.unwrap_or_default(),
                    include_phases: !no_phases,
                    sample_rate,
                    max_spans,
                    filters,
                };
                run_otel(database, &options)
            }

            Commands::OpenApi {
                database,
                output,
                title,
                version,
                sample_bodies,
                sample_body_max_size,
                allow_external_paths,
                external_path_root,
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
                let filters = EntryFilterOptions {
                    url: url.unwrap_or_default(),
                    url_contains: url_contains.unwrap_or_default(),
                    url_regex: url_regex.unwrap_or_default(),
                    host: host.unwrap_or_default(),
                    method: method.unwrap_or_default(),
                    status: status.unwrap_or_default(),
                    mime_contains: mime.unwrap_or_default(),
                    ext: ext.unwrap_or_default(),
                    source: source.unwrap_or_default(),
                    source_contains: source_contains.unwrap_or_default(),
                    from,
                    to,
                    min_request_size,
                    max_request_size,
                    min_response_size,
                    max_response_size,
                };
                let options = OpenApiOptions {
                    output,
                    title,
                    version,
                    sample_bodies,
                    sample_body_max_size,
                    allow_external_paths: allow_external_paths.unwrap_or(false),
                    external_path_root,
                    filters,
                };
                run_openapi(database, &options)
            }

            Commands::Waterfall {
                database,
                output,
                format,
                group_by,
                host,
                page,
                from,
                to,
                width,
            } => {
                let options = WaterfallOptions {
                    output,
                    format: format.unwrap_or(WaterfallFormat::Text),
                    group_by: group_by.unwrap_or(WaterfallGroupBy::Page),
                    host: host.unwrap_or_default(),
                    page: page.unwrap_or_default(),
                    from,
                    to,
                    width,
                };
                run_waterfall(database, &options)
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

            Commands::Pii {
                format,
                redact,
                output,
                force,
                dry_run,
                no_defaults,
                no_email,
                no_phone,
                no_ssn,
                no_credit_card,
                email_regex,
                phone_regex,
                ssn_regex,
                credit_card_regex,
                token,
                database,
            } => {
                let defaults = &resolved.pii;
                let options = PiiOptions {
                    format: format.unwrap_or(defaults.format),
                    redact: redact.unwrap_or(defaults.redact),
                    output: output.or_else(|| defaults.output.clone()),
                    force: force.unwrap_or(defaults.force),
                    dry_run: dry_run.unwrap_or(defaults.dry_run),
                    no_defaults: no_defaults.unwrap_or(defaults.no_defaults),
                    no_email: no_email.unwrap_or(defaults.no_email),
                    no_phone: no_phone.unwrap_or(defaults.no_phone),
                    no_ssn: no_ssn.unwrap_or(defaults.no_ssn),
                    no_credit_card: no_credit_card.unwrap_or(defaults.no_credit_card),
                    email_regexes: email_regex.unwrap_or_else(|| defaults.email_regex.clone()),
                    phone_regexes: phone_regex.unwrap_or_else(|| defaults.phone_regex.clone()),
                    ssn_regexes: ssn_regex.unwrap_or_else(|| defaults.ssn_regex.clone()),
                    credit_card_regexes: credit_card_regex
                        .unwrap_or_else(|| defaults.credit_card_regex.clone()),
                    token: token.unwrap_or_else(|| defaults.token.clone()),
                };
                run_pii(database, &options)
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

            Commands::Replay {
                input,
                format,
                concurrency,
                rate_limit,
                timeout,
                allow_unsafe,
                allow_external_paths,
                external_path_root,
                url,
                url_contains,
                url_regex,
                host,
                method,
                status,
                override_host,
                override_header,
            } => {
                let defaults = &resolved.replay;
                let options = ReplayOptions {
                    format: format.unwrap_or(defaults.format),
                    concurrency: concurrency.unwrap_or(defaults.concurrency),
                    rate_limit: rate_limit.or(defaults.rate_limit),
                    timeout_secs: timeout.or(defaults.timeout_secs),
                    allow_unsafe: allow_unsafe.unwrap_or(defaults.allow_unsafe),
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
                    override_host: override_host.unwrap_or_else(|| defaults.override_host.clone()),
                    override_header: override_header
                        .unwrap_or_else(|| defaults.override_header.clone()),
                };
                run_replay(input, &options)
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
