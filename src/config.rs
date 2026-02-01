use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::commands::{DedupStrategy, FtsTokenizer, NameMatchMode, OutputFormat};
use crate::db::ExtractBodiesKind;
use crate::error::{HarliteError, Result};

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub import: Option<ImportConfig>,
    #[serde(default)]
    pub cdp: Option<CdpConfig>,
    #[serde(default)]
    pub replay: Option<ReplayConfig>,
    #[serde(default)]
    pub export: Option<ExportConfig>,
    #[serde(default)]
    pub redact: Option<RedactConfig>,
    #[serde(default)]
    pub diff: Option<DiffConfig>,
    #[serde(default)]
    pub merge: Option<MergeConfig>,
    #[serde(default)]
    pub query: Option<QueryConfig>,
    #[serde(default)]
    pub search: Option<SearchConfig>,
    #[serde(default)]
    pub repl: Option<ReplConfig>,
    #[serde(default)]
    pub fts_rebuild: Option<FtsRebuildConfig>,
    #[serde(default)]
    pub stats: Option<StatsConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ImportConfig {
    pub output: Option<PathBuf>,
    pub bodies: Option<bool>,
    pub max_body_size: Option<String>,
    pub text_only: Option<bool>,
    pub stats: Option<bool>,
    pub incremental: Option<bool>,
    pub resume: Option<bool>,
    pub jobs: Option<usize>,
    pub async_read: Option<bool>,
    pub decompress_bodies: Option<bool>,
    pub keep_compressed: Option<bool>,
    pub extract_bodies: Option<PathBuf>,
    pub extract_bodies_kind: Option<ExtractBodiesKind>,
    pub extract_bodies_shard_depth: Option<u8>,
    pub host: Option<Vec<String>>,
    pub method: Option<Vec<String>>,
    pub status: Option<Vec<i32>>,
    pub url_regex: Option<Vec<String>>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct CdpConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub target: Option<String>,
    pub har: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub bodies: Option<bool>,
    pub max_body_size: Option<String>,
    pub text_only: Option<bool>,
    pub duration: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ReplayConfig {
    pub format: Option<OutputFormat>,
    pub concurrency: Option<usize>,
    pub rate_limit: Option<f64>,
    pub timeout_secs: Option<u64>,
    pub allow_unsafe: Option<bool>,
    pub allow_external_paths: Option<bool>,
    pub external_path_root: Option<PathBuf>,

    pub url: Option<Vec<String>>,
    pub url_contains: Option<Vec<String>>,
    pub url_regex: Option<Vec<String>>,
    pub host: Option<Vec<String>>,
    pub method: Option<Vec<String>>,
    pub status: Option<Vec<i32>>,

    pub override_host: Option<Vec<String>>,
    pub override_header: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ExportConfig {
    pub output: Option<PathBuf>,
    pub bodies: Option<bool>,
    pub bodies_raw: Option<bool>,
    pub allow_external_paths: Option<bool>,
    pub external_path_root: Option<PathBuf>,
    pub compact: Option<bool>,

    pub url: Option<Vec<String>>,
    pub url_contains: Option<Vec<String>>,
    pub url_regex: Option<Vec<String>>,
    pub host: Option<Vec<String>>,
    pub method: Option<Vec<String>>,
    pub status: Option<Vec<i32>>,
    pub mime: Option<Vec<String>>,
    pub ext: Option<Vec<String>>,
    pub source: Option<Vec<String>>,
    pub source_contains: Option<Vec<String>>,

    pub from: Option<String>,
    pub to: Option<String>,
    pub min_request_size: Option<String>,
    pub max_request_size: Option<String>,
    pub min_response_size: Option<String>,
    pub max_response_size: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RedactConfig {
    pub output: Option<PathBuf>,
    pub force: Option<bool>,
    pub dry_run: Option<bool>,
    pub no_defaults: Option<bool>,
    pub header: Option<Vec<String>>,
    pub cookie: Option<Vec<String>>,
    pub query_param: Option<Vec<String>>,
    pub body_regex: Option<Vec<String>>,
    pub match_mode: Option<NameMatchMode>,
    pub token: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DiffConfig {
    pub format: Option<OutputFormat>,
    pub host: Option<Vec<String>>,
    pub method: Option<Vec<String>>,
    pub status: Option<Vec<i32>>,
    pub url_regex: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct MergeConfig {
    pub output: Option<PathBuf>,
    pub dry_run: Option<bool>,
    pub dedup: Option<DedupStrategy>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct QueryConfig {
    pub format: Option<OutputFormat>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub quiet: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SearchConfig {
    pub format: Option<OutputFormat>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub quiet: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ReplConfig {
    pub format: Option<OutputFormat>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct FtsRebuildConfig {
    pub tokenizer: Option<FtsTokenizer>,
    pub max_body_size: Option<String>,
    pub allow_external_paths: Option<bool>,
    pub external_path_root: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct StatsConfig {
    pub json: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedConfig {
    pub import: ResolvedImportConfig,
    pub cdp: ResolvedCdpConfig,
    pub replay: ResolvedReplayConfig,
    pub export: ResolvedExportConfig,
    pub redact: ResolvedRedactConfig,
    pub diff: ResolvedDiffConfig,
    pub merge: ResolvedMergeConfig,
    pub query: ResolvedQueryConfig,
    pub search: ResolvedSearchConfig,
    pub repl: ResolvedReplConfig,
    pub fts_rebuild: ResolvedFtsRebuildConfig,
    pub stats: ResolvedStatsConfig,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedImportConfig {
    pub output: Option<PathBuf>,
    pub bodies: bool,
    pub max_body_size: String,
    pub text_only: bool,
    pub stats: bool,
    pub incremental: bool,
    pub resume: bool,
    pub jobs: usize,
    pub async_read: bool,
    pub decompress_bodies: bool,
    pub keep_compressed: bool,
    pub extract_bodies: Option<PathBuf>,
    pub extract_bodies_kind: ExtractBodiesKind,
    pub extract_bodies_shard_depth: u8,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub url_regex: Vec<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedCdpConfig {
    pub host: String,
    pub port: u16,
    pub target: Option<String>,
    pub har: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub bodies: bool,
    pub max_body_size: String,
    pub text_only: bool,
    pub duration: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedReplayConfig {
    pub format: OutputFormat,
    pub concurrency: usize,
    pub rate_limit: Option<f64>,
    pub timeout_secs: Option<u64>,
    pub allow_unsafe: bool,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,

    pub url: Vec<String>,
    pub url_contains: Vec<String>,
    pub url_regex: Vec<String>,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,

    pub override_host: Vec<String>,
    pub override_header: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedExportConfig {
    pub output: Option<PathBuf>,
    pub bodies: bool,
    pub bodies_raw: bool,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
    pub compact: bool,

    pub url: Vec<String>,
    pub url_contains: Vec<String>,
    pub url_regex: Vec<String>,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub mime: Vec<String>,
    pub ext: Vec<String>,
    pub source: Vec<String>,
    pub source_contains: Vec<String>,

    pub from: Option<String>,
    pub to: Option<String>,
    pub min_request_size: Option<String>,
    pub max_request_size: Option<String>,
    pub min_response_size: Option<String>,
    pub max_response_size: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedRedactConfig {
    pub output: Option<PathBuf>,
    pub force: bool,
    pub dry_run: bool,
    pub no_defaults: bool,
    pub header: Vec<String>,
    pub cookie: Vec<String>,
    pub query_param: Vec<String>,
    pub body_regex: Vec<String>,
    pub match_mode: NameMatchMode,
    pub token: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedDiffConfig {
    pub format: OutputFormat,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub url_regex: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedMergeConfig {
    pub output: Option<PathBuf>,
    pub dry_run: bool,
    pub dedup: DedupStrategy,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedQueryConfig {
    pub format: OutputFormat,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub quiet: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedSearchConfig {
    pub format: OutputFormat,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub quiet: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedReplConfig {
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedFtsRebuildConfig {
    pub tokenizer: FtsTokenizer,
    pub max_body_size: String,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedStatsConfig {
    pub json: bool,
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        Self {
            import: ResolvedImportConfig::default(),
            cdp: ResolvedCdpConfig::default(),
            replay: ResolvedReplayConfig::default(),
            export: ResolvedExportConfig::default(),
            redact: ResolvedRedactConfig::default(),
            diff: ResolvedDiffConfig::default(),
            merge: ResolvedMergeConfig::default(),
            query: ResolvedQueryConfig::default(),
            search: ResolvedSearchConfig::default(),
            repl: ResolvedReplConfig::default(),
            fts_rebuild: ResolvedFtsRebuildConfig::default(),
            stats: ResolvedStatsConfig::default(),
        }
    }
}

impl Default for ResolvedImportConfig {
    fn default() -> Self {
        Self {
            output: None,
            bodies: false,
            max_body_size: "100KB".to_string(),
            text_only: false,
            stats: false,
            incremental: false,
            resume: false,
            jobs: 0,
            async_read: false,
            decompress_bodies: false,
            keep_compressed: false,
            extract_bodies: None,
            extract_bodies_kind: ExtractBodiesKind::Both,
            extract_bodies_shard_depth: 0,
            host: Vec::new(),
            method: Vec::new(),
            status: Vec::new(),
            url_regex: Vec::new(),
            from: None,
            to: None,
        }
    }
}

impl Default for ResolvedCdpConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9222,
            target: None,
            har: None,
            output: None,
            bodies: false,
            max_body_size: "100KB".to_string(),
            text_only: false,
            duration: None,
        }
    }
}

impl Default for ResolvedReplayConfig {
    fn default() -> Self {
        Self {
            format: OutputFormat::Table,
            concurrency: 0,
            rate_limit: None,
            timeout_secs: None,
            allow_unsafe: false,
            allow_external_paths: false,
            external_path_root: None,
            url: Vec::new(),
            url_contains: Vec::new(),
            url_regex: Vec::new(),
            host: Vec::new(),
            method: Vec::new(),
            status: Vec::new(),
            override_host: Vec::new(),
            override_header: Vec::new(),
        }
    }
}

impl Default for ResolvedExportConfig {
    fn default() -> Self {
        Self {
            output: None,
            bodies: false,
            bodies_raw: false,
            allow_external_paths: false,
            external_path_root: None,
            compact: false,
            url: Vec::new(),
            url_contains: Vec::new(),
            url_regex: Vec::new(),
            host: Vec::new(),
            method: Vec::new(),
            status: Vec::new(),
            mime: Vec::new(),
            ext: Vec::new(),
            source: Vec::new(),
            source_contains: Vec::new(),
            from: None,
            to: None,
            min_request_size: None,
            max_request_size: None,
            min_response_size: None,
            max_response_size: None,
        }
    }
}

impl Default for ResolvedRedactConfig {
    fn default() -> Self {
        Self {
            output: None,
            force: false,
            dry_run: false,
            no_defaults: false,
            header: Vec::new(),
            cookie: Vec::new(),
            query_param: Vec::new(),
            body_regex: Vec::new(),
            match_mode: NameMatchMode::Wildcard,
            token: "REDACTED".to_string(),
        }
    }
}

impl Default for ResolvedDiffConfig {
    fn default() -> Self {
        Self {
            format: OutputFormat::Table,
            host: Vec::new(),
            method: Vec::new(),
            status: Vec::new(),
            url_regex: Vec::new(),
        }
    }
}

impl Default for ResolvedMergeConfig {
    fn default() -> Self {
        Self {
            output: None,
            dry_run: false,
            dedup: DedupStrategy::Hash,
        }
    }
}

impl Default for ResolvedQueryConfig {
    fn default() -> Self {
        Self {
            format: OutputFormat::Table,
            limit: None,
            offset: None,
            quiet: false,
        }
    }
}

impl Default for ResolvedSearchConfig {
    fn default() -> Self {
        Self {
            format: OutputFormat::Table,
            limit: None,
            offset: None,
            quiet: false,
        }
    }
}

impl Default for ResolvedReplConfig {
    fn default() -> Self {
        Self {
            format: OutputFormat::Table,
        }
    }
}

impl Default for ResolvedFtsRebuildConfig {
    fn default() -> Self {
        Self {
            tokenizer: FtsTokenizer::Unicode61,
            max_body_size: "1MB".to_string(),
            allow_external_paths: false,
            external_path_root: None,
        }
    }
}

impl Default for ResolvedStatsConfig {
    fn default() -> Self {
        Self { json: false }
    }
}

impl ResolvedConfig {
    pub fn from_config(config: &Config) -> Self {
        let mut resolved = Self::default();
        if let Some(cfg) = &config.import {
            resolved.import.apply(cfg);
        }
        if let Some(cfg) = &config.cdp {
            resolved.cdp.apply(cfg);
        }
        if let Some(cfg) = &config.replay {
            resolved.replay.apply(cfg);
        }
        if let Some(cfg) = &config.export {
            resolved.export.apply(cfg);
        }
        if let Some(cfg) = &config.redact {
            resolved.redact.apply(cfg);
        }
        if let Some(cfg) = &config.diff {
            resolved.diff.apply(cfg);
        }
        if let Some(cfg) = &config.merge {
            resolved.merge.apply(cfg);
        }
        if let Some(cfg) = &config.query {
            resolved.query.apply(cfg);
        }
        if let Some(cfg) = &config.search {
            resolved.search.apply(cfg);
        }
        if let Some(cfg) = &config.repl {
            resolved.repl.apply(cfg);
        }
        if let Some(cfg) = &config.fts_rebuild {
            resolved.fts_rebuild.apply(cfg);
        }
        if let Some(cfg) = &config.stats {
            resolved.stats.apply(cfg);
        }
        resolved
    }
}

impl ResolvedImportConfig {
    fn apply(&mut self, cfg: &ImportConfig) {
        if let Some(value) = cfg.output.clone() {
            self.output = Some(value);
        }
        if let Some(value) = cfg.bodies {
            self.bodies = value;
        }
        if let Some(value) = cfg.max_body_size.clone() {
            self.max_body_size = value;
        }
        if let Some(value) = cfg.text_only {
            self.text_only = value;
        }
        if let Some(value) = cfg.stats {
            self.stats = value;
        }
        if let Some(value) = cfg.incremental {
            self.incremental = value;
        }
        if let Some(value) = cfg.resume {
            self.resume = value;
        }
        if let Some(value) = cfg.jobs {
            self.jobs = value;
        }
        if let Some(value) = cfg.async_read {
            self.async_read = value;
        }
        if let Some(value) = cfg.decompress_bodies {
            self.decompress_bodies = value;
        }
        if let Some(value) = cfg.keep_compressed {
            self.keep_compressed = value;
        }
        if let Some(value) = cfg.extract_bodies.clone() {
            self.extract_bodies = Some(value);
        }
        if let Some(value) = cfg.extract_bodies_kind {
            self.extract_bodies_kind = value;
        }
        if let Some(value) = cfg.extract_bodies_shard_depth {
            self.extract_bodies_shard_depth = value;
        }
        if let Some(value) = cfg.host.clone() {
            self.host = value;
        }
        if let Some(value) = cfg.method.clone() {
            self.method = value;
        }
        if let Some(value) = cfg.status.clone() {
            self.status = value;
        }
        if let Some(value) = cfg.url_regex.clone() {
            self.url_regex = value;
        }
        if let Some(value) = cfg.from.clone() {
            self.from = Some(value);
        }
        if let Some(value) = cfg.to.clone() {
            self.to = Some(value);
        }
    }
}

impl ResolvedCdpConfig {
    fn apply(&mut self, cfg: &CdpConfig) {
        if let Some(value) = cfg.host.clone() {
            self.host = value;
        }
        if let Some(value) = cfg.port {
            self.port = value;
        }
        if let Some(value) = cfg.target.clone() {
            self.target = Some(value);
        }
        if let Some(value) = cfg.har.clone() {
            self.har = Some(value);
        }
        if let Some(value) = cfg.output.clone() {
            self.output = Some(value);
        }
        if let Some(value) = cfg.bodies {
            self.bodies = value;
        }
        if let Some(value) = cfg.max_body_size.clone() {
            self.max_body_size = value;
        }
        if let Some(value) = cfg.text_only {
            self.text_only = value;
        }
        if let Some(value) = cfg.duration {
            self.duration = Some(value);
        }
    }
}

impl ResolvedReplayConfig {
    fn apply(&mut self, cfg: &ReplayConfig) {
        if let Some(value) = cfg.format {
            self.format = value;
        }
        if let Some(value) = cfg.concurrency {
            self.concurrency = value;
        }
        if let Some(value) = cfg.rate_limit {
            self.rate_limit = Some(value);
        }
        if let Some(value) = cfg.timeout_secs {
            self.timeout_secs = Some(value);
        }
        if let Some(value) = cfg.allow_unsafe {
            self.allow_unsafe = value;
        }
        if let Some(value) = cfg.allow_external_paths {
            self.allow_external_paths = value;
        }
        if let Some(value) = cfg.external_path_root.clone() {
            self.external_path_root = Some(value);
        }
        if let Some(value) = cfg.url.clone() {
            self.url = value;
        }
        if let Some(value) = cfg.url_contains.clone() {
            self.url_contains = value;
        }
        if let Some(value) = cfg.url_regex.clone() {
            self.url_regex = value;
        }
        if let Some(value) = cfg.host.clone() {
            self.host = value;
        }
        if let Some(value) = cfg.method.clone() {
            self.method = value;
        }
        if let Some(value) = cfg.status.clone() {
            self.status = value;
        }
        if let Some(value) = cfg.override_host.clone() {
            self.override_host = value;
        }
        if let Some(value) = cfg.override_header.clone() {
            self.override_header = value;
        }
    }
}

impl ResolvedExportConfig {
    fn apply(&mut self, cfg: &ExportConfig) {
        if let Some(value) = cfg.output.clone() {
            self.output = Some(value);
        }
        if let Some(value) = cfg.bodies {
            self.bodies = value;
        }
        if let Some(value) = cfg.bodies_raw {
            self.bodies_raw = value;
        }
        if let Some(value) = cfg.allow_external_paths {
            self.allow_external_paths = value;
        }
        if let Some(value) = cfg.external_path_root.clone() {
            self.external_path_root = Some(value);
        }
        if let Some(value) = cfg.compact {
            self.compact = value;
        }
        if let Some(value) = cfg.url.clone() {
            self.url = value;
        }
        if let Some(value) = cfg.url_contains.clone() {
            self.url_contains = value;
        }
        if let Some(value) = cfg.url_regex.clone() {
            self.url_regex = value;
        }
        if let Some(value) = cfg.host.clone() {
            self.host = value;
        }
        if let Some(value) = cfg.method.clone() {
            self.method = value;
        }
        if let Some(value) = cfg.status.clone() {
            self.status = value;
        }
        if let Some(value) = cfg.mime.clone() {
            self.mime = value;
        }
        if let Some(value) = cfg.ext.clone() {
            self.ext = value;
        }
        if let Some(value) = cfg.source.clone() {
            self.source = value;
        }
        if let Some(value) = cfg.source_contains.clone() {
            self.source_contains = value;
        }
        if let Some(value) = cfg.from.clone() {
            self.from = Some(value);
        }
        if let Some(value) = cfg.to.clone() {
            self.to = Some(value);
        }
        if let Some(value) = cfg.min_request_size.clone() {
            self.min_request_size = Some(value);
        }
        if let Some(value) = cfg.max_request_size.clone() {
            self.max_request_size = Some(value);
        }
        if let Some(value) = cfg.min_response_size.clone() {
            self.min_response_size = Some(value);
        }
        if let Some(value) = cfg.max_response_size.clone() {
            self.max_response_size = Some(value);
        }
    }
}

impl ResolvedRedactConfig {
    fn apply(&mut self, cfg: &RedactConfig) {
        if let Some(value) = cfg.output.clone() {
            self.output = Some(value);
        }
        if let Some(value) = cfg.force {
            self.force = value;
        }
        if let Some(value) = cfg.dry_run {
            self.dry_run = value;
        }
        if let Some(value) = cfg.no_defaults {
            self.no_defaults = value;
        }
        if let Some(value) = cfg.header.clone() {
            self.header = value;
        }
        if let Some(value) = cfg.cookie.clone() {
            self.cookie = value;
        }
        if let Some(value) = cfg.query_param.clone() {
            self.query_param = value;
        }
        if let Some(value) = cfg.body_regex.clone() {
            self.body_regex = value;
        }
        if let Some(value) = cfg.match_mode {
            self.match_mode = value;
        }
        if let Some(value) = cfg.token.clone() {
            self.token = value;
        }
    }
}

impl ResolvedDiffConfig {
    fn apply(&mut self, cfg: &DiffConfig) {
        if let Some(value) = cfg.format {
            self.format = value;
        }
        if let Some(value) = cfg.host.clone() {
            self.host = value;
        }
        if let Some(value) = cfg.method.clone() {
            self.method = value;
        }
        if let Some(value) = cfg.status.clone() {
            self.status = value;
        }
        if let Some(value) = cfg.url_regex.clone() {
            self.url_regex = value;
        }
    }
}

impl ResolvedMergeConfig {
    fn apply(&mut self, cfg: &MergeConfig) {
        if let Some(value) = cfg.output.clone() {
            self.output = Some(value);
        }
        if let Some(value) = cfg.dry_run {
            self.dry_run = value;
        }
        if let Some(value) = cfg.dedup {
            self.dedup = value;
        }
    }
}

impl ResolvedQueryConfig {
    fn apply(&mut self, cfg: &QueryConfig) {
        if let Some(value) = cfg.format {
            self.format = value;
        }
        if let Some(value) = cfg.limit {
            self.limit = Some(value);
        }
        if let Some(value) = cfg.offset {
            self.offset = Some(value);
        }
        if let Some(value) = cfg.quiet {
            self.quiet = value;
        }
    }
}

impl ResolvedSearchConfig {
    fn apply(&mut self, cfg: &SearchConfig) {
        if let Some(value) = cfg.format {
            self.format = value;
        }
        if let Some(value) = cfg.limit {
            self.limit = Some(value);
        }
        if let Some(value) = cfg.offset {
            self.offset = Some(value);
        }
        if let Some(value) = cfg.quiet {
            self.quiet = value;
        }
    }
}

impl ResolvedReplConfig {
    fn apply(&mut self, cfg: &ReplConfig) {
        if let Some(value) = cfg.format {
            self.format = value;
        }
    }
}

impl ResolvedFtsRebuildConfig {
    fn apply(&mut self, cfg: &FtsRebuildConfig) {
        if let Some(value) = cfg.tokenizer {
            self.tokenizer = value;
        }
        if let Some(value) = cfg.max_body_size.clone() {
            self.max_body_size = value;
        }
        if let Some(value) = cfg.allow_external_paths {
            self.allow_external_paths = value;
        }
        if let Some(value) = cfg.external_path_root.clone() {
            self.external_path_root = Some(value);
        }
    }
}

impl ResolvedStatsConfig {
    fn apply(&mut self, cfg: &StatsConfig) {
        if let Some(value) = cfg.json {
            self.json = value;
        }
    }
}

pub fn load_config() -> Result<Config> {
    let mut config = Config::default();
    let paths = config_search_paths()?;
    for path in paths {
        if !path.exists() {
            continue;
        }
        let contents = fs::read_to_string(&path)?;
        let parsed: Config = toml::from_str(&contents).map_err(|err| {
            HarliteError::InvalidArgs(format!(
                "Failed to parse config {}: {}",
                path.display(),
                err
            ))
        })?;
        merge_config(&mut config, parsed);
    }
    Ok(config)
}

fn merge_config(base: &mut Config, other: Config) {
    merge_section(&mut base.import, other.import, ImportConfig::merge);
    merge_section(&mut base.cdp, other.cdp, CdpConfig::merge);
    merge_section(&mut base.export, other.export, ExportConfig::merge);
    merge_section(&mut base.redact, other.redact, RedactConfig::merge);
    merge_section(&mut base.diff, other.diff, DiffConfig::merge);
    merge_section(&mut base.merge, other.merge, MergeConfig::merge);
    merge_section(&mut base.query, other.query, QueryConfig::merge);
    merge_section(&mut base.search, other.search, SearchConfig::merge);
    merge_section(&mut base.repl, other.repl, ReplConfig::merge);
    merge_section(
        &mut base.fts_rebuild,
        other.fts_rebuild,
        FtsRebuildConfig::merge,
    );
    merge_section(&mut base.stats, other.stats, StatsConfig::merge);
}

fn merge_section<T>(base: &mut Option<T>, other: Option<T>, merge: fn(&mut T, T)) {
    if let Some(other_section) = other {
        match base {
            Some(existing) => merge(existing, other_section),
            None => *base = Some(other_section),
        }
    }
}

impl ImportConfig {
    fn merge(&mut self, other: ImportConfig) {
        merge_opt(&mut self.output, other.output);
        merge_opt(&mut self.bodies, other.bodies);
        merge_opt(&mut self.max_body_size, other.max_body_size);
        merge_opt(&mut self.text_only, other.text_only);
        merge_opt(&mut self.stats, other.stats);
        merge_opt(&mut self.incremental, other.incremental);
        merge_opt(&mut self.resume, other.resume);
        merge_opt(&mut self.jobs, other.jobs);
        merge_opt(&mut self.async_read, other.async_read);
        merge_opt(&mut self.decompress_bodies, other.decompress_bodies);
        merge_opt(&mut self.keep_compressed, other.keep_compressed);
        merge_opt(&mut self.extract_bodies, other.extract_bodies);
        merge_opt(&mut self.extract_bodies_kind, other.extract_bodies_kind);
        merge_opt(
            &mut self.extract_bodies_shard_depth,
            other.extract_bodies_shard_depth,
        );
        merge_opt(&mut self.host, other.host);
        merge_opt(&mut self.method, other.method);
        merge_opt(&mut self.status, other.status);
        merge_opt(&mut self.url_regex, other.url_regex);
        merge_opt(&mut self.from, other.from);
        merge_opt(&mut self.to, other.to);
    }
}

impl CdpConfig {
    fn merge(&mut self, other: CdpConfig) {
        merge_opt(&mut self.host, other.host);
        merge_opt(&mut self.port, other.port);
        merge_opt(&mut self.target, other.target);
        merge_opt(&mut self.har, other.har);
        merge_opt(&mut self.output, other.output);
        merge_opt(&mut self.bodies, other.bodies);
        merge_opt(&mut self.max_body_size, other.max_body_size);
        merge_opt(&mut self.text_only, other.text_only);
        merge_opt(&mut self.duration, other.duration);
    }
}

impl ExportConfig {
    fn merge(&mut self, other: ExportConfig) {
        merge_opt(&mut self.output, other.output);
        merge_opt(&mut self.bodies, other.bodies);
        merge_opt(&mut self.bodies_raw, other.bodies_raw);
        merge_opt(&mut self.allow_external_paths, other.allow_external_paths);
        merge_opt(&mut self.external_path_root, other.external_path_root);
        merge_opt(&mut self.compact, other.compact);
        merge_opt(&mut self.url, other.url);
        merge_opt(&mut self.url_contains, other.url_contains);
        merge_opt(&mut self.url_regex, other.url_regex);
        merge_opt(&mut self.host, other.host);
        merge_opt(&mut self.method, other.method);
        merge_opt(&mut self.status, other.status);
        merge_opt(&mut self.mime, other.mime);
        merge_opt(&mut self.ext, other.ext);
        merge_opt(&mut self.source, other.source);
        merge_opt(&mut self.source_contains, other.source_contains);
        merge_opt(&mut self.from, other.from);
        merge_opt(&mut self.to, other.to);
        merge_opt(&mut self.min_request_size, other.min_request_size);
        merge_opt(&mut self.max_request_size, other.max_request_size);
        merge_opt(&mut self.min_response_size, other.min_response_size);
        merge_opt(&mut self.max_response_size, other.max_response_size);
    }
}

impl RedactConfig {
    fn merge(&mut self, other: RedactConfig) {
        merge_opt(&mut self.output, other.output);
        merge_opt(&mut self.force, other.force);
        merge_opt(&mut self.dry_run, other.dry_run);
        merge_opt(&mut self.no_defaults, other.no_defaults);
        merge_opt(&mut self.header, other.header);
        merge_opt(&mut self.cookie, other.cookie);
        merge_opt(&mut self.query_param, other.query_param);
        merge_opt(&mut self.body_regex, other.body_regex);
        merge_opt(&mut self.match_mode, other.match_mode);
        merge_opt(&mut self.token, other.token);
    }
}

impl DiffConfig {
    fn merge(&mut self, other: DiffConfig) {
        merge_opt(&mut self.format, other.format);
        merge_opt(&mut self.host, other.host);
        merge_opt(&mut self.method, other.method);
        merge_opt(&mut self.status, other.status);
        merge_opt(&mut self.url_regex, other.url_regex);
    }
}

impl MergeConfig {
    fn merge(&mut self, other: MergeConfig) {
        merge_opt(&mut self.output, other.output);
        merge_opt(&mut self.dry_run, other.dry_run);
        merge_opt(&mut self.dedup, other.dedup);
    }
}

impl QueryConfig {
    fn merge(&mut self, other: QueryConfig) {
        merge_opt(&mut self.format, other.format);
        merge_opt(&mut self.limit, other.limit);
        merge_opt(&mut self.offset, other.offset);
        merge_opt(&mut self.quiet, other.quiet);
    }
}

impl SearchConfig {
    fn merge(&mut self, other: SearchConfig) {
        merge_opt(&mut self.format, other.format);
        merge_opt(&mut self.limit, other.limit);
        merge_opt(&mut self.offset, other.offset);
        merge_opt(&mut self.quiet, other.quiet);
    }
}

impl ReplConfig {
    fn merge(&mut self, other: ReplConfig) {
        merge_opt(&mut self.format, other.format);
    }
}

impl FtsRebuildConfig {
    fn merge(&mut self, other: FtsRebuildConfig) {
        merge_opt(&mut self.tokenizer, other.tokenizer);
        merge_opt(&mut self.max_body_size, other.max_body_size);
        merge_opt(&mut self.allow_external_paths, other.allow_external_paths);
        merge_opt(&mut self.external_path_root, other.external_path_root);
    }
}

impl StatsConfig {
    fn merge(&mut self, other: StatsConfig) {
        merge_opt(&mut self.json, other.json);
    }
}

fn merge_opt<T>(base: &mut Option<T>, other: Option<T>) {
    if other.is_some() {
        *base = other;
    }
}

fn config_search_paths() -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();

    if let Some(home) = home_dir() {
        if let Some(config_home) = config_home_dir(&home) {
            paths.push(config_home.join("harlite").join("harlite.toml"));
        }
        if let Some(appdata) = env::var_os("APPDATA") {
            paths.push(PathBuf::from(appdata).join("harlite").join("harlite.toml"));
        }
        paths.push(home.join(".harliterc"));
    }

    if let Ok(cwd) = env::current_dir() {
        let mut dirs = Vec::new();
        let mut current: Option<&Path> = Some(cwd.as_path());
        while let Some(dir) = current {
            dirs.push(dir.to_path_buf());
            current = dir.parent();
        }
        dirs.reverse();
        for dir in dirs {
            paths.push(dir.join(".harliterc"));
            paths.push(dir.join("harlite.toml"));
        }
    }

    Ok(paths)
}

fn config_home_dir(home: &Path) -> Option<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(xdg));
    }
    Some(home.join(".config"))
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
}

pub fn render_config(config: &ResolvedConfig) -> Result<String> {
    toml::to_string_pretty(config)
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to render config: {}", err)))
}
