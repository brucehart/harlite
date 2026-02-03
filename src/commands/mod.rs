mod diff;
mod analyze;
mod entry_filter;
mod export;
mod export_data;
mod openapi;
mod fts;
mod import;
mod imports;
mod info;
mod merge;
#[cfg(feature = "otel")]
mod otel;
mod prune;
mod query;
mod pii;
mod redact;
#[cfg(feature = "repl")]
mod repl;
#[cfg(feature = "replay")]
mod replay;
#[cfg(feature = "serve")]
mod serve;
mod schema;
mod search;
mod stats;
pub mod util;
#[cfg(feature = "watch")]
mod watch;
mod waterfall;

pub use diff::{run_diff, DiffOptions};
pub use analyze::{run_analyze, AnalyzeOptions};
pub use entry_filter::EntryFilterOptions;
pub use export::{run_export, ExportOptions};
pub use export_data::{run_export_data, DataExportFormat, ExportDataOptions};
pub use fts::{run_fts_rebuild, FtsTokenizer};
pub use import::{run_import, ImportOptions};
pub use imports::run_imports;
pub use info::{run_info, InfoOptions};
pub use merge::{run_merge, DedupStrategy, MergeOptions};
#[cfg(feature = "otel")]
pub use otel::{run_otel, OtelExportFormat, OtelExportOptions};
pub use prune::run_prune;
pub use query::{run_query, OutputFormat, QueryOptions};
pub use pii::{run_pii, PiiOptions};
pub use redact::{run_redact, NameMatchMode, RedactOptions};
#[cfg(feature = "repl")]
pub use repl::{run_repl, ReplOptions};
#[cfg(feature = "replay")]
pub use replay::{run_replay, ReplayOptions};
#[cfg(feature = "serve")]
pub use serve::{run_serve, MatchMode, ServeOptions};
pub use schema::run_schema;
pub use search::run_search;
pub use openapi::{run_openapi, OpenApiOptions};
pub use stats::{run_stats, StatsOptions};
pub use waterfall::{run_waterfall, WaterfallFormat, WaterfallGroupBy, WaterfallOptions};
#[cfg(feature = "cdp")]
mod cdp;
#[cfg(feature = "cdp")]
pub use cdp::{run_cdp, CdpOptions};
#[cfg(feature = "watch")]
pub use watch::{run_watch, WatchOptions};
