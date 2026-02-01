use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use regex::Regex;
use rusqlite::{Connection, OpenFlags};
use url::Url;

use crate::db::{ensure_schema_upgrades, load_blobs_by_hashes, load_entries, BlobRow, EntryQuery};
use crate::error::{HarliteError, Result};
use crate::har::{parse_har_file, Entry as HarEntry, Header, PostData};

use super::OutputFormat;

pub struct ReplayOptions {
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

#[derive(Clone, Debug)]
struct ReplayEntry {
    index: usize,
    method: String,
    url: String,
    request_headers: HashMap<String, String>,
    request_body: Option<Vec<u8>>,
    original_status: Option<i32>,
    original_headers: HashMap<String, String>,
    original_body_size: Option<i64>,
}

#[derive(Debug, serde::Serialize)]
struct ReplayRow {
    method: String,
    url_original: String,
    url_replay: String,
    status_original: Option<i32>,
    status_replay: Option<i32>,
    status_changed: Option<bool>,
    header_changes: Option<usize>,
    body_size_original: Option<i64>,
    body_size_replay: Option<i64>,
    body_size_delta: Option<i64>,
    error: Option<String>,
}

#[derive(Clone)]
struct HostOverrideRule {
    pattern: Regex,
    host: String,
}

#[derive(Clone)]
struct HeaderOverrideRule {
    pattern: Regex,
    name: String,
    value: String,
}

#[derive(Clone)]
struct ReplayRuntime {
    allow_unsafe: bool,
    timeout: Option<Duration>,
    rate_limiter: Option<Arc<RateLimiter>>,
    host_overrides: Vec<HostOverrideRule>,
    header_overrides: Vec<HeaderOverrideRule>,
}

struct RateLimiter {
    interval: Duration,
    next_allowed: Mutex<Instant>,
}

impl RateLimiter {
    fn new(rate_limit: f64) -> Self {
        let interval = Duration::from_secs_f64(1.0 / rate_limit);
        Self {
            interval,
            next_allowed: Mutex::new(Instant::now()),
        }
    }

    fn wait(&self) {
        let sleep_for = {
            let mut next = self.next_allowed.lock().expect("rate limiter lock");
            let now = Instant::now();
            if *next > now {
                let dur = *next - now;
                *next += self.interval;
                Some(dur)
            } else {
                *next = now + self.interval;
                None
            }
        };

        if let Some(dur) = sleep_for {
            thread::sleep(dur);
        }
    }
}

pub fn run_replay(input: PathBuf, options: &ReplayOptions) -> Result<()> {
    if let Some(rate) = options.rate_limit {
        if rate <= 0.0 {
            return Err(HarliteError::InvalidArgs(
                "--rate-limit must be greater than 0".to_string(),
            ));
        }
    }

    let compiled = compile_rules(options)?;
    let mut entries = if is_db_path(&input) {
        load_entries_from_db(&input, options)?
    } else {
        load_entries_from_har(&input, options)?
    };

    if entries.is_empty() {
        return Ok(());
    }

    let concurrency = resolve_concurrency(options.concurrency, entries.len())?;
    let runtime = ReplayRuntime {
        allow_unsafe: options.allow_unsafe,
        timeout: options.timeout_secs.map(Duration::from_secs),
        rate_limiter: options.rate_limit.map(|rl| Arc::new(RateLimiter::new(rl))),
        host_overrides: compiled.host_overrides,
        header_overrides: compiled.header_overrides,
    };

    let (work_tx, work_rx) = mpsc::channel::<ReplayEntry>();
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (result_tx, result_rx) = mpsc::channel::<(usize, ReplayRow)>();

    for entry in entries.drain(..) {
        work_tx.send(entry).ok();
    }
    drop(work_tx);

    let runtime = Arc::new(runtime);
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let rx = Arc::clone(&work_rx);
        let tx = result_tx.clone();
        let runtime = Arc::clone(&runtime);
        handles.push(thread::spawn(move || worker_loop(rx, tx, runtime)));
    }
    drop(result_tx);

    let mut rows: Vec<(usize, ReplayRow)> = Vec::new();
    while let Ok(row) = result_rx.recv() {
        rows.push(row);
    }

    for handle in handles {
        let _ = handle.join();
    }

    rows.sort_by_key(|(idx, _)| *idx);
    let data: Vec<ReplayRow> = rows.into_iter().map(|(_, row)| row).collect();

    match options.format {
        OutputFormat::Json => write_json(&data),
        OutputFormat::Csv => write_csv(&data),
        OutputFormat::Table => write_table(&data),
    }
}

fn resolve_concurrency(configured: usize, total: usize) -> Result<usize> {
    if configured == 0 {
        let available = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        return Ok(available.max(1).min(total.max(1)));
    }
    Ok(configured.min(total.max(1)))
}

fn worker_loop(
    receiver: Arc<Mutex<mpsc::Receiver<ReplayEntry>>>,
    sender: mpsc::Sender<(usize, ReplayRow)>,
    runtime: Arc<ReplayRuntime>,
) {
    let agent = build_agent(runtime.timeout);
    loop {
        let entry = {
            let lock = receiver.lock().expect("worker receiver lock");
            lock.recv()
        };
        let Ok(entry) = entry else {
            break;
        };

        if let Some(limiter) = runtime.rate_limiter.as_ref() {
            limiter.wait();
        }

        let (idx, row) = replay_entry(&agent, entry, &runtime);
        let _ = sender.send((idx, row));
    }
}

fn build_agent(timeout: Option<Duration>) -> ureq::Agent {
    let mut builder = ureq::AgentBuilder::new();
    if let Some(timeout) = timeout {
        builder = builder.timeout_connect(timeout).timeout_read(timeout);
    }
    builder.build()
}

fn replay_entry(agent: &ureq::Agent, entry: ReplayEntry, runtime: &ReplayRuntime) -> (usize, ReplayRow) {
    let method = entry.method.to_ascii_uppercase();
    let mut row = ReplayRow {
        method: method.clone(),
        url_original: entry.url.clone(),
        url_replay: entry.url.clone(),
        status_original: entry.original_status,
        status_replay: None,
        status_changed: None,
        header_changes: None,
        body_size_original: entry.original_body_size,
        body_size_replay: None,
        body_size_delta: None,
        error: None,
    };

    if !runtime.allow_unsafe && !is_safe_method(&method) {
        row.error = Some(format!("skipped unsafe method {method}"));
        return (entry.index, row);
    }

    let parsed = match Url::parse(&entry.url) {
        Ok(url) => url,
        Err(err) => {
            row.error = Some(format!("invalid url: {err}"));
            return (entry.index, row);
        }
    };

    let mut replay_url = parsed.clone();
    for rule in &runtime.host_overrides {
        if rule.pattern.is_match(parsed.as_str()) {
            if let Err(err) = apply_host_override(&mut replay_url, &rule.host) {
                row.error = Some(format!("host override failed: {err}"));
                return (entry.index, row);
            }
        }
    }
    row.url_replay = replay_url.to_string();

    let mut headers = entry.request_headers.clone();
    headers.remove("content-length");

    for rule in &runtime.header_overrides {
        if rule.pattern.is_match(replay_url.as_str()) {
            headers.insert(rule.name.clone(), rule.value.clone());
        }
    }

    if let Some(host) = replay_url.host_str() {
        let host_header = if let Some(port) = replay_url.port() {
            format!("{host}:{port}")
        } else {
            host.to_string()
        };
        headers.insert("host".to_string(), host_header);
    }

    let request = agent.request(&method, replay_url.as_str());
    let request = apply_headers(request, &headers);

    let response = match entry.request_body {
        Some(body) if !body.is_empty() => request.send_bytes(&body),
        _ => request.call(),
    };

    let response = match response {
        Ok(resp) => resp,
        Err(ureq::Error::Status(_, resp)) => resp,
        Err(ureq::Error::Transport(err)) => {
            row.error = Some(err.to_string());
            return (entry.index, row);
        }
    };

    let replay_status = response.status();
    let replay_headers = response_headers_map(&response);
    let body_size = match read_response_size(response) {
        Ok(size) => Some(size),
        Err(err) => {
            row.error = Some(err.to_string());
            None
        }
    };

    row.status_replay = Some(i32::from(replay_status));
    row.status_changed = match (row.status_original, row.status_replay) {
        (Some(left), Some(right)) => Some(left != right),
        _ => None,
    };

    let header_changes = diff_header_count(&entry.original_headers, &replay_headers);
    row.header_changes = Some(header_changes);

    row.body_size_replay = body_size;
    row.body_size_delta = diff_i64(row.body_size_original, row.body_size_replay);

    (entry.index, row)
}

fn apply_headers(mut request: ureq::Request, headers: &HashMap<String, String>) -> ureq::Request {
    for (name, value) in headers {
        request = request.set(name, value);
    }
    request
}

fn read_response_size(response: ureq::Response) -> Result<i64> {
    let mut reader = response.into_reader();
    let bytes = io::copy(&mut reader, &mut io::sink())?;
    Ok(i64::try_from(bytes).unwrap_or(i64::MAX))
}

fn response_headers_map(response: &ureq::Response) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for name in response.headers_names() {
        let name_lc = name.to_ascii_lowercase();
        if let Some(value) = response.header(&name) {
            out.insert(name_lc, value.to_string());
        }
    }
    out
}

fn diff_header_count(left: &HashMap<String, String>, right: &HashMap<String, String>) -> usize {
    let mut keys: HashSet<&String> = HashSet::new();
    for k in left.keys() {
        keys.insert(k);
    }
    for k in right.keys() {
        keys.insert(k);
    }

    keys.into_iter()
        .filter(|k| left.get(*k) != right.get(*k))
        .count()
}

fn is_safe_method(method: &str) -> bool {
    matches!(method, "GET" | "HEAD" | "OPTIONS" | "TRACE")
}

fn apply_host_override(url: &mut Url, host: &str) -> Result<()> {
    if host.contains("//") {
        return Err(HarliteError::InvalidArgs(
            "override host must not include scheme".to_string(),
        ));
    }

    let (host_part, port) = split_host_port(host)?;
    url.set_host(Some(&host_part))?;
    if let Some(port) = port {
        url.set_port(Some(port))
            .map_err(|_| HarliteError::InvalidArgs("invalid override port".to_string()))?;
    } else {
        let _ = url.set_port(None);
    }
    Ok(())
}

fn split_host_port(host: &str) -> Result<(String, Option<u16>)> {
    if host.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "override host cannot be empty".to_string(),
        ));
    }

    if let Some(stripped) = host.strip_prefix('[') {
        if let Some(end) = stripped.find(']') {
            let host_part = &stripped[..end];
            let rest = &stripped[end + 1..];
            if let Some(port_str) = rest.strip_prefix(':') {
                let port = port_str.parse::<u16>().map_err(|_| {
                    HarliteError::InvalidArgs("invalid override port".to_string())
                })?;
                return Ok((host_part.to_string(), Some(port)));
            }
            return Ok((host_part.to_string(), None));
        }
    }

    if let Some(idx) = host.rfind(':') {
        let (host_part, port_str) = host.split_at(idx);
        if !host_part.is_empty() {
            let port = port_str[1..].parse::<u16>().map_err(|_| {
                HarliteError::InvalidArgs("invalid override port".to_string())
            })?;
            return Ok((host_part.to_string(), Some(port)));
        }
    }

    Ok((host.to_string(), None))
}

fn is_db_path(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
        return is_sqlite_file(path);
    };

    let ext = ext.to_ascii_lowercase();
    if matches!(ext.as_str(), "db" | "db3" | "sqlite" | "sqlite3") {
        return true;
    }

    is_sqlite_file(path)
}

fn is_sqlite_file(path: &Path) -> bool {
    let Ok(mut file) = File::open(path) else {
        return false;
    };

    let mut header = [0u8; 16];
    let Ok(read_len) = file.read(&mut header) else {
        return false;
    };
    if read_len < 16 {
        return false;
    }

    header == *b"SQLite format 3\0"
}

fn load_entries_from_db(path: &Path, options: &ReplayOptions) -> Result<Vec<ReplayEntry>> {
    // Best-effort schema upgrades: if the database is writable, run upgrades on a
    // separate read-write connection. If opening in read-write mode fails (e.g.
    // read-only filesystem), skip upgrades and continue with a read-only connection.
    if let Ok(upgrade_conn) = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        ensure_schema_upgrades(&upgrade_conn)?;
    }

    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    let mut query = EntryQuery::default();
    query.hosts = options.host.clone();
    query.methods = options.method.clone();
    query.statuses = options.status.clone();
    query.url_exact = options.url.clone();
    query.url_contains = options.url_contains.clone();

    let url_regexes = compile_url_regexes(&options.url_regex)?;

    let rows = load_entries(&conn, &query)?;

    let mut hashes: Vec<String> = rows
        .iter()
        .filter_map(|row| row.request_body_hash.clone())
        .collect();
    hashes.sort();
    hashes.dedup();

    let mut blobs = load_blobs_by_hashes(&conn, &hashes)?;
    let external_root = if options.allow_external_paths {
        let root = options
            .external_path_root
            .clone()
            .or_else(|| path.parent().map(|p| p.to_path_buf()))
            .ok_or_else(|| {
                HarliteError::InvalidArgs(
                    "Cannot resolve external path root; pass --external-path-root".to_string(),
                )
            })?;
        Some(root.canonicalize()?)
    } else {
        None
    };

    if options.allow_external_paths {
        for blob in &mut blobs {
            load_external_blob_content(blob, external_root.as_deref())?;
        }
    }

    let blob_map: HashMap<String, BlobRow> = blobs.into_iter().map(|b| (b.hash.clone(), b)).collect();

    let mut out = Vec::new();
    for (idx, row) in rows.into_iter().enumerate() {
        let Some(url) = row.url.clone() else { continue; };
        if !url_regexes.is_empty() && !url_regexes.iter().any(|re| re.is_match(&url)) {
            continue;
        }

        let method = row.method.clone().unwrap_or_else(|| "GET".to_string());
        let request_headers = headers_from_json(row.request_headers.as_deref());
        let original_headers = headers_from_json(row.response_headers.as_deref());
        let original_body_size = sanitize_size(row.response_body_size)
            .or_else(|| sanitize_size(row.response_body_size_raw));

        let request_body = row
            .request_body_hash
            .as_ref()
            .and_then(|hash| blob_map.get(hash))
            .and_then(|blob| if blob.content.is_empty() { None } else { Some(blob.content.clone()) });

        out.push(ReplayEntry {
            index: idx,
            method,
            url,
            request_headers,
            request_body,
            original_status: row.status,
            original_headers,
            original_body_size,
        });
    }

    Ok(out)
}

fn load_entries_from_har(path: &Path, options: &ReplayOptions) -> Result<Vec<ReplayEntry>> {
    let har = parse_har_file(path)?;
    let url_regexes = compile_url_regexes(&options.url_regex)?;
    let host_set: HashSet<String> = options
        .host
        .iter()
        .map(|h| h.trim().to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .collect();
    let method_set: HashSet<String> = options
        .method
        .iter()
        .map(|m| m.trim().to_ascii_uppercase())
        .filter(|m| !m.is_empty())
        .collect();
    let status_set: HashSet<i32> = options.status.iter().copied().collect();

    let mut out = Vec::new();
    for (idx, entry) in har.log.entries.into_iter().enumerate() {
        if !entry_matches_filters(&entry, options, &url_regexes, &host_set, &method_set, &status_set)
        {
            continue;
        }

        let method = entry.request.method.clone();
        let url = entry.request.url.clone();
        let mut request_headers = headers_from_list(&entry.request.headers);
        let original_headers = headers_from_list(&entry.response.headers);
        let original_body_size = sanitize_size(entry.response.body_size)
            .or_else(|| sanitize_size(Some(entry.response.content.size)));
        let request_body = post_data_to_body(&entry.request.post_data, &mut request_headers);

        out.push(ReplayEntry {
            index: idx,
            method,
            url,
            request_headers,
            request_body,
            original_status: Some(entry.response.status),
            original_headers,
            original_body_size,
        });
    }

    Ok(out)
}

fn entry_matches_filters(
    entry: &HarEntry,
    options: &ReplayOptions,
    url_regexes: &[Regex],
    host_set: &HashSet<String>,
    method_set: &HashSet<String>,
    status_set: &HashSet<i32>,
) -> bool {
    if !options.url.is_empty() && !options.url.contains(&entry.request.url) {
        return false;
    }

    if !options.url_contains.is_empty()
        && !options
            .url_contains
            .iter()
            .any(|needle| entry.request.url.contains(needle))
    {
        return false;
    }

    if !url_regexes.is_empty() && !url_regexes.iter().any(|re| re.is_match(&entry.request.url)) {
        return false;
    }

    if !host_set.is_empty() {
        let host = Url::parse(&entry.request.url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()));
        if host.as_deref().is_none_or(|h| !host_set.contains(h)) {
            return false;
        }
    }

    if !method_set.is_empty() && !method_set.contains(&entry.request.method.to_ascii_uppercase()) {
        return false;
    }

    if !status_set.is_empty() && !status_set.contains(&entry.response.status) {
        return false;
    }

    true
}

fn post_data_to_body(
    post_data: &Option<PostData>,
    headers: &mut HashMap<String, String>,
) -> Option<Vec<u8>> {
    let Some(post_data) = post_data else {
        return None;
    };

    if let Some(text) = &post_data.text {
        return Some(text.as_bytes().to_vec());
    }

    let Some(params) = &post_data.params else {
        return None;
    };

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for param in params {
        if let Some(value) = &param.value {
            serializer.append_pair(&param.name, value);
        }
    }
    let encoded = serializer.finish();
    if encoded.is_empty() {
        return None;
    }

    if !headers.contains_key("content-type") {
        headers.insert(
            "content-type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        );
    }

    Some(encoded.into_bytes())
}

fn headers_from_json(json: Option<&str>) -> HashMap<String, String> {
    let Some(json) = json else {
        return HashMap::new();
    };

    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return HashMap::new();
    };
    let Some(obj) = value.as_object() else {
        return HashMap::new();
    };

    obj.iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

fn headers_from_list(headers: &[Header]) -> HashMap<String, String> {
    headers
        .iter()
        .map(|h| (h.name.to_ascii_lowercase(), h.value.clone()))
        .collect()
}

fn sanitize_size(value: Option<i64>) -> Option<i64> {
    value.filter(|v| *v >= 0)
}

fn compile_url_regexes(values: &[String]) -> Result<Vec<Regex>> {
    values
        .iter()
        .map(|s| Regex::new(s))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(HarliteError::from)
}

struct CompiledOverrides {
    host_overrides: Vec<HostOverrideRule>,
    header_overrides: Vec<HeaderOverrideRule>,
}

fn compile_rules(options: &ReplayOptions) -> Result<CompiledOverrides> {
    let host_overrides = options
        .override_host
        .iter()
        .map(|raw| parse_host_override(raw))
        .collect::<Result<Vec<_>>>()?;

    let header_overrides = options
        .override_header
        .iter()
        .map(|raw| parse_header_override(raw))
        .collect::<Result<Vec<_>>>()?;

    Ok(CompiledOverrides {
        host_overrides,
        header_overrides,
    })
}

fn parse_host_override(raw: &str) -> Result<HostOverrideRule> {
    let (pattern_raw, host) = raw
        .split_once('=')
        .ok_or_else(|| HarliteError::InvalidArgs(
            "override-host must be '<url-regex>=<host>'".to_string(),
        ))?;
    let pattern = compile_override_pattern(pattern_raw)?;
    if host.trim().is_empty() {
        return Err(HarliteError::InvalidArgs(
            "override-host target cannot be empty".to_string(),
        ));
    }
    Ok(HostOverrideRule {
        pattern,
        host: host.trim().to_string(),
    })
}

fn parse_header_override(raw: &str) -> Result<HeaderOverrideRule> {
    let (left, value) = raw.split_once('=')
        .ok_or_else(|| HarliteError::InvalidArgs(
            "override-header must be '<url-regex>:<name>=<value>' or '<name>=<value>'".to_string(),
        ))?;

    let (pattern_raw, name) = match left.rsplit_once(':') {
        Some((pattern, name)) => (pattern, name),
        None => ("*", left),
    };

    let pattern = compile_override_pattern(pattern_raw)?;
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "override-header name cannot be empty".to_string(),
        ));
    }

    Ok(HeaderOverrideRule {
        pattern,
        name,
        value: value.to_string(),
    })
}

fn compile_override_pattern(raw: &str) -> Result<Regex> {
    let raw = raw.trim();
    if raw.is_empty() || raw == "*" {
        return Ok(Regex::new(".*")?);
    }
    Ok(Regex::new(raw)?)
}

fn load_external_blob_content(blob: &mut BlobRow, external_root: Option<&Path>) -> Result<()> {
    if !blob.content.is_empty() || blob.size <= 0 {
        return Ok(());
    }
    let Some(path) = &blob.external_path else {
        return Ok(());
    };
    let Some(root) = external_root else {
        return Ok(());
    };

    let candidate = PathBuf::from(path);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    };
    let resolved = match candidate.canonicalize() {
        Ok(p) => p,
        Err(_) => return Ok(()),
    };
    if !resolved.starts_with(root) {
        return Ok(());
    }
    blob.content = std::fs::read(resolved)?;
    Ok(())
}

fn diff_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(b - a),
        _ => None,
    }
}

fn write_json(rows: &[ReplayRow]) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, &rows)?;
    handle.write_all(b"\n")?;
    Ok(())
}

fn write_csv(rows: &[ReplayRow]) -> Result<()> {
    let columns = replay_columns();
    let mut out = io::stdout().lock();
    write_csv_row(&mut out, columns.iter().copied())?;
    for row in rows {
        let fields = replay_row_values(row);
        write_csv_row(&mut out, fields.iter().map(|s| s.as_str()))?;
    }
    Ok(())
}

fn write_table(rows: &[ReplayRow]) -> Result<()> {
    let columns = replay_columns();
    let mut data: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for row in rows {
        data.push(replay_row_values(row));
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &data {
        for (i, value) in row.iter().enumerate() {
            widths[i] = widths[i].max(value.chars().count());
        }
    }

    for width in &mut widths {
        *width = (*width).min(80).max(8);
    }

    let mut out = io::stdout().lock();
    write_table_row(&mut out, columns.iter().copied(), &widths)?;
    write_table_sep(&mut out, &widths)?;
    for row in data {
        write_table_row(&mut out, row.iter().map(|s| s.as_str()), &widths)?;
    }
    Ok(())
}

fn replay_columns() -> Vec<&'static str> {
    vec![
        "method",
        "url_original",
        "url_replay",
        "status_original",
        "status_replay",
        "status_changed",
        "header_changes",
        "body_size_original",
        "body_size_replay",
        "body_size_delta",
        "error",
    ]
}

fn replay_row_values(row: &ReplayRow) -> Vec<String> {
    vec![
        row.method.clone(),
        row.url_original.clone(),
        row.url_replay.clone(),
        opt_i32(row.status_original),
        opt_i32(row.status_replay),
        opt_bool(row.status_changed),
        opt_usize(row.header_changes),
        opt_i64(row.body_size_original),
        opt_i64(row.body_size_replay),
        opt_i64_signed(row.body_size_delta),
        row.error.clone().unwrap_or_default(),
    ]
}

fn opt_i32(value: Option<i32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_usize(value: Option<usize>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_i64_signed(value: Option<i64>) -> String {
    value.map(|v| format!("{v:+}")).unwrap_or_default()
}

fn opt_bool(value: Option<bool>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn write_csv_row<'a, I>(out: &mut impl Write, fields: I) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut first = true;
    for field in fields {
        if !first {
            out.write_all(b",")?;
        }
        first = false;
        write_csv_field(out, field)?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_csv_field(out: &mut impl Write, field: &str) -> Result<()> {
    let needs_quotes = field.contains([',', '"', '\n', '\r']);
    if !needs_quotes {
        out.write_all(field.as_bytes())?;
        return Ok(());
    }

    out.write_all(b"\"")?;
    for b in field.as_bytes() {
        if *b == b'"' {
            out.write_all(b"\"\"")?;
        } else {
            out.write_all(&[*b])?;
        }
    }
    out.write_all(b"\"")?;
    Ok(())
}

fn write_table_row<'a, I>(out: &mut impl Write, fields: I, widths: &[usize]) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut i = 0usize;
    for field in fields {
        if i > 0 {
            out.write_all(b" | ")?;
        }
        let width = widths.get(i).copied().unwrap_or(0);
        let field = truncate(field, width);
        out.write_all(field.as_bytes())?;
        let field_len = field.chars().count();
        if field_len < width {
            out.write_all(" ".repeat(width - field_len).as_bytes())?;
        }
        i += 1;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn write_table_sep(out: &mut impl Write, widths: &[usize]) -> Result<()> {
    for (i, w) in widths.iter().copied().enumerate() {
        if i > 0 {
            out.write_all(b"-+-")?;
        }
        out.write_all("-".repeat(w).as_bytes())?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    // Compare using character count, not byte length, to match column widths.
    if s.chars().count() <= max {
        return s.to_string();
    }
    if max <= 3 {
        return "...".to_string();
    }
    let mut end = 0usize;
    let mut chars_seen = 0usize;
    for (i, ch) in s.char_indices() {
        if chars_seen >= max.saturating_sub(3) {
            break;
        }
        end = i + ch.len_utf8();
        chars_seen += 1;
    }
    let mut out = s[..end].to_string();
    out.push_str("...");
    out
}
