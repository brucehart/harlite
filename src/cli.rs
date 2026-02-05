use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::commands;
use crate::commands::{
    DataExportFormat, DedupStrategy, NameMatchMode, OutputFormat, WaterfallFormat,
    WaterfallGroupBy,
};
#[cfg(feature = "otel")]
use crate::commands::OtelExportFormat;
#[cfg(feature = "serve")]
use crate::commands::MatchMode;
use crate::db::ExtractBodiesKind;

#[derive(Parser)]
#[command(name = "harlite")]
#[command(about = "Import HAR files into SQLite. Query your web traffic with SQL.")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
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

        /// Enable plugin by name (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        plugin: Option<Vec<String>>,

        /// Disable plugin by name (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        disable_plugin: Option<Vec<String>>,
    },

    /// Capture network traffic from Chrome via CDP
    #[cfg(feature = "cdp")]
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
    #[cfg(feature = "watch")]
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

        /// Enable plugin by name (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        plugin: Option<Vec<String>>,

        /// Disable plugin by name (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        disable_plugin: Option<Vec<String>>,
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

        /// Enable plugin by name (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        plugin: Option<Vec<String>>,

        /// Disable plugin by name (repeatable)
        #[arg(long, action = clap::ArgAction::Append)]
        disable_plugin: Option<Vec<String>>,
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
    #[cfg(feature = "otel")]
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

    /// Generate a self-contained HTML report (waterfall, slowest requests, errors)
    Report {
        /// Input SQLite database or HAR file
        input: PathBuf,

        /// Output HTML file (default: <input>.html). Use '-' for stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Report title
        #[arg(long)]
        title: Option<String>,

        /// Top N rows for slow/error tables
        #[arg(long)]
        top: Option<usize>,

        /// Threshold for slow requests by total time (ms)
        #[arg(long)]
        slow_total_ms: Option<f64>,

        /// Threshold for slow requests by TTFB (ms)
        #[arg(long)]
        slow_ttfb_ms: Option<f64>,

        /// Max number of entries rendered in the waterfall (stats still computed over all filtered entries)
        #[arg(long)]
        waterfall_limit: Option<usize>,

        /// Group waterfall requests by page, navigation, or none
        #[arg(long, value_enum)]
        group_by: Option<WaterfallGroupBy>,

        /// Page id or title substring filter (repeatable; DB uses pages table, HAR uses pageref/page title)
        #[arg(long, action = clap::ArgAction::Append)]
        page: Option<Vec<String>>,

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

        /// Filter by import source filename (repeatable; DB only)
        #[arg(long, action = clap::ArgAction::Append)]
        source: Option<Vec<String>>,

        /// Filter by import source filename substring match (repeatable; DB only)
        #[arg(long, action = clap::ArgAction::Append)]
        source_contains: Option<Vec<String>>,

        /// Only include entries on/after this timestamp (RFC3339) or date (YYYY-MM-DD)
        #[arg(long)]
        from: Option<String>,

        /// Only include entries on/before this timestamp (RFC3339) or date (YYYY-MM-DD)
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
    #[cfg(feature = "replay")]
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

    /// Serve recorded responses as a mock API server
    #[cfg(feature = "serve")]
    Serve {
        /// HAR file or SQLite database to serve
        input: PathBuf,

        /// Bind address
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// Port to listen on
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// Match mode (strict = method + full URL, fuzzy = method + host/path + query similarity)
        #[arg(long, value_enum, default_value_t = MatchMode::Strict)]
        match_mode: MatchMode,

        /// Allow reading external blob paths from the database
        #[arg(long, action = clap::ArgAction::SetTrue)]
        allow_external_paths: Option<bool>,

        /// Root directory for external blob paths (defaults to database directory)
        #[arg(long, value_name = "DIR")]
        external_path_root: Option<PathBuf>,

        /// TLS certificate (PEM)
        #[arg(long)]
        tls_cert: Option<PathBuf>,

        /// TLS private key (PEM)
        #[arg(long)]
        tls_key: Option<PathBuf>,
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
    #[cfg(feature = "repl")]
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
    #[cfg(feature = "completions")]
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}
