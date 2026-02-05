//! Stable, supported API surface for embedding harlite.
//!
//! This module re-exports the types and functions intended for external use.
//! Treat the contents of this module as SemVer-stable.

pub use crate::commands::{
    run_analyze, run_diff, run_export, run_export_data, run_fts_rebuild, run_import, run_imports,
    run_info, run_merge, run_openapi, run_pii, run_prune, run_query, run_redact, run_report,
    run_schema, run_search, run_stats, run_waterfall, AnalyzeOptions, DataExportFormat,
    DedupStrategy, DiffOptions, EntryFilterOptions, ExportDataOptions, ExportOptions, FtsTokenizer,
    ImportOptions, InfoOptions, NameMatchMode, OpenApiOptions, OutputFormat, PiiOptions,
    QueryOptions, RedactOptions, ReportOptions, StatsOptions, WaterfallFormat, WaterfallGroupBy,
    WaterfallOptions,
};
#[cfg(feature = "cdp")]
pub use crate::commands::{run_cdp, CdpOptions};
#[cfg(feature = "otel")]
pub use crate::commands::{run_otel, OtelExportFormat, OtelExportOptions};
#[cfg(feature = "repl")]
pub use crate::commands::{run_repl, ReplOptions};
#[cfg(feature = "replay")]
pub use crate::commands::{run_replay, ReplayOptions};
#[cfg(feature = "serve")]
pub use crate::commands::{run_serve, MatchMode, ServeOptions};
#[cfg(feature = "watch")]
pub use crate::commands::{run_watch, WatchOptions};
pub use crate::db::{
    create_import, create_import_with_status, create_schema, ensure_schema_upgrades,
    entry_content_hash, entry_hash_from_fields, insert_entry, insert_entry_with_hash, insert_page,
    load_blobs_by_hashes, load_entries, load_pages_for_imports, store_blob, BlobRow, BlobStats,
    EntryBlobStats, EntryHashFields, EntryInsertResult, EntryQuery, EntryRelations, EntryRow,
    ExtractBodiesKind, ImportStats, InsertEntryOptions, PageRow,
};
pub use crate::error::{HarliteError, Result};
pub use crate::graphql::{extract_graphql_info, GraphQLInfo};
pub use crate::har::{
    parse_har_file, parse_har_file_async, Browser, Content, Cookie, Creator, Entry, Extensions,
    Har, Header, Log, Page, PageTimings, PostData, PostParam, QueryParam, Request, Response,
    Timings,
};
pub use crate::plugins::{
    resolve_plugins, ExporterOutcome, PluginConfig, PluginContext, PluginKind, PluginPhase,
    PluginSet, PLUGIN_API_VERSION,
};
pub use crate::size::{parse_size_bytes_i64, parse_size_bytes_usize};
