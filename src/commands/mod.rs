mod analyze;
#[cfg_attr(not(feature = "serve"), allow(dead_code))]
mod body_codec;
mod check;
mod csv;
mod diff;
mod entry_filter;
mod export;
mod export_data;
mod fts;
mod import;
mod imports;
mod info;
mod merge;
mod metadata;
mod openapi;
#[cfg(feature = "otel")]
mod otel;
mod pii;
mod privacy_body;
mod prune;
mod query;
mod redact;
#[cfg(feature = "repl")]
mod repl;
#[cfg(feature = "replay")]
mod replay;
mod report;
mod request;
mod schema;
mod search;
#[cfg(feature = "serve")]
mod serve;
mod stats;
pub mod util;
#[cfg(feature = "watch")]
mod watch;
mod waterfall;

pub use analyze::{run_analyze, AnalyzeOptions};
pub use check::{run_check, CheckOptions};
pub use diff::{run_diff, DiffFailOn, DiffOptions};
pub use entry_filter::EntryFilterOptions;
pub use export::{run_export, ExportInputFormat, ExportOptions};
pub use export_data::{run_export_data, DataExportFormat, ExportDataOptions};
pub use fts::{run_fts_rebuild, FtsTokenizer};
pub use import::{run_import, ImportOptions};
pub use imports::run_imports;
pub use info::{run_info, InfoOptions};
pub use merge::{run_merge, DedupStrategy, MergeOptions};
pub use openapi::{run_openapi, OpenApiOptions};
#[cfg(feature = "otel")]
pub use otel::{run_otel, OtelExportFormat, OtelExportOptions};
pub use pii::{run_pii, run_pii_input, run_pii_with_external_paths, PiiOptions};
pub use prune::{run_prune, run_prune_with_options, PruneOptions};
pub use query::{run_query, OutputFormat, QueryOptions};
pub use redact::{
    run_redact, run_redact_input, run_redact_with_external_paths, NameMatchMode, RedactOptions,
};
#[cfg(feature = "repl")]
pub use repl::{run_repl, ReplOptions};
#[cfg(feature = "replay")]
pub use replay::{run_replay, ReplayOptions};
pub use report::{run_report, ReportOptions};
pub use request::{run_request_export, RequestExportFormat, RequestExportOptions};
pub use schema::run_schema;
pub use search::run_search;
#[cfg(feature = "serve")]
pub use serve::{run_serve, MatchMode, ServeOptions};
pub use stats::{run_stats, StatsOptions};
pub use waterfall::{run_waterfall, WaterfallFormat, WaterfallGroupBy, WaterfallOptions};
#[cfg(feature = "cdp")]
mod cdp;
#[cfg(feature = "cdp")]
pub use cdp::{run_cdp, CdpOptions};
#[cfg(feature = "watch")]
pub use watch::{run_watch, WatchOptions};
