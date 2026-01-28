use std::collections::HashMap;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};
use url::Url;

use crate::db::{
    create_import, create_schema, insert_entry, update_import_count, BlobStats, ImportStats,
    InsertEntryOptions,
};
use crate::error::{HarliteError, Result};
use crate::har::{
    Browser, Content, Creator, Entry, Extensions, Har, Header, Log, PostData, QueryParam, Request,
    Response, Timings,
};

/// Options for capturing traffic from Chrome via CDP.
pub struct CdpOptions {
    pub host: String,
    pub port: u16,
    pub target: Option<String>,
    pub output_har: Option<PathBuf>,
    pub output_db: Option<PathBuf>,
    pub store_bodies: bool,
    pub max_body_size: Option<usize>,
    pub text_only: bool,
    pub duration_secs: Option<u64>,
}

#[derive(Deserialize)]
struct VersionInfo {
    #[serde(rename = "Browser")]
    browser: Option<String>,
}

#[derive(Deserialize)]
struct TargetInfo {
    id: String,
    #[serde(rename = "type")]
    target_type: Option<String>,
    url: Option<String>,
    title: Option<String>,
    #[serde(rename = "webSocketDebuggerUrl")]
    web_socket_debugger_url: Option<String>,
}

#[derive(Deserialize)]
struct RequestWillBeSent {
    #[serde(rename = "requestId")]
    request_id: String,
    request: CdpRequest,
    timestamp: f64,
    #[serde(rename = "wallTime")]
    wall_time: Option<f64>,
    #[serde(rename = "redirectResponse")]
    redirect_response: Option<CdpResponse>,
}

#[derive(Deserialize)]
struct ResponseReceived {
    #[serde(rename = "requestId")]
    request_id: String,
    timestamp: f64,
    response: CdpResponse,
}

#[derive(Deserialize)]
struct LoadingFinished {
    #[serde(rename = "requestId")]
    request_id: String,
    timestamp: f64,
    #[serde(rename = "encodedDataLength")]
    encoded_data_length: f64,
}

#[derive(Deserialize)]
struct LoadingFailed {
    #[serde(rename = "requestId")]
    request_id: String,
    timestamp: f64,
    #[serde(rename = "errorText")]
    error_text: String,
}

#[derive(Deserialize)]
struct CdpRequest {
    url: String,
    method: String,
    headers: serde_json::Map<String, Value>,
    #[serde(rename = "postData")]
    post_data: Option<String>,
}

#[derive(Deserialize, Clone)]
struct CdpResponse {
    status: i32,
    #[serde(rename = "statusText")]
    status_text: String,
    headers: serde_json::Map<String, Value>,
    #[serde(rename = "mimeType")]
    mime_type: Option<String>,
    protocol: Option<String>,
    #[serde(rename = "remoteIPAddress")]
    remote_ip_address: Option<String>,
    #[serde(rename = "connectionId")]
    connection_id: Option<i64>,
}

#[derive(Deserialize)]
struct ResponseBodyResult {
    body: String,
    #[serde(rename = "base64Encoded")]
    base64_encoded: bool,
}

struct RequestRecord {
    request: CdpRequest,
    started_wall_time: Option<f64>,
    started_ts: f64,
    response: Option<CdpResponse>,
    response_received_ts: Option<f64>,
    end_ts: Option<f64>,
    encoded_data_len: Option<f64>,
    failed: Option<String>,
    body: Option<ResponseBodyResult>,
}

struct CaptureState {
    requests: HashMap<String, RequestRecord>,
    pending_body_requests: HashMap<u64, String>,
    entries: Vec<Entry>,
    capture_started_at: DateTime<Utc>,
    first_event_ts: Option<f64>,
}

impl CaptureState {
    fn new(capture_started_at: DateTime<Utc>) -> Self {
        Self {
            requests: HashMap::new(),
            pending_body_requests: HashMap::new(),
            entries: Vec::new(),
            capture_started_at,
            first_event_ts: None,
        }
    }
}

pub fn run_cdp(options: &CdpOptions) -> Result<()> {
    if options.output_har.is_none() && options.output_db.is_none() {
        return Err(HarliteError::InvalidArgs(
            "CDP capture requires --har and/or --output to be set".to_string(),
        ));
    }

    let base_url = format!("http://{}:{}", options.host, options.port);
    let version = fetch_version(&base_url)?;
    let target = select_target(&base_url, options.target.as_deref())?;
    let ws_url = target.web_socket_debugger_url.ok_or_else(|| {
        HarliteError::InvalidArgs("Selected target is missing webSocketDebuggerUrl".to_string())
    })?;

    println!(
        "Connecting to Chrome target {} ({})...",
        target.id,
        target.url.as_deref().unwrap_or("unknown target url")
    );

    let (mut socket, _) = connect(Url::parse(&ws_url)?)
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to connect to CDP: {err}")))?;
    set_socket_timeout(&mut socket, Duration::from_millis(200))?;

    let stop = Arc::new(AtomicBool::new(false));
    let stop_handler = stop.clone();
    ctrlc::set_handler(move || {
        stop_handler.store(true, Ordering::SeqCst);
    })
    .map_err(|err| HarliteError::InvalidArgs(format!("Failed to install Ctrl+C handler: {err}")))?;

    let mut next_id = 1_u64;
    send_command(&mut socket, &mut next_id, "Network.enable", json!({}))?;
    send_command(&mut socket, &mut next_id, "Page.enable", json!({}))?;

    let mut state = CaptureState::new(Utc::now());
    let start = Instant::now();

    println!(
        "Capturing network events{}...",
        options
            .duration_secs
            .map(|s| format!(" for {s}s"))
            .unwrap_or_else(|| " (press Ctrl+C to stop)".to_string())
    );

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if let Some(duration) = options.duration_secs {
            if start.elapsed() >= Duration::from_secs(duration) {
                break;
            }
        }

        match socket.read() {
            Ok(msg) => handle_message(&mut socket, &mut next_id, &mut state, options, msg)?,
            Err(tungstenite::Error::Io(err))
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue
            }
            Err(err) => {
                return Err(HarliteError::InvalidArgs(format!(
                    "CDP socket error: {err}"
                )))
            }
        }
    }

    finalize_pending_requests(&mut state, options)?;

    println!("Captured {} entries", state.entries.len());

    let har = build_har(&version, state.entries);

    if let Some(path) = &options.output_har {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, &har)?;
        println!("Wrote HAR to {}", path.display());
    }

    if let Some(db_path) = &options.output_db {
        import_entries(db_path, &har, options)?;
    }

    Ok(())
}

fn fetch_version(base_url: &str) -> Result<VersionInfo> {
    let url = format!("{base_url}/json/version");
    let response = ureq::get(&url)
        .call()
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to fetch {url}: {err}")))?;
    response
        .into_json::<VersionInfo>()
        .map_err(|err| HarliteError::InvalidArgs(format!("Invalid CDP version JSON: {err}")))
}

fn select_target(base_url: &str, target_hint: Option<&str>) -> Result<TargetInfo> {
    let url = format!("{base_url}/json/list");
    let response = ureq::get(&url)
        .call()
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to fetch {url}: {err}")))?;
    let targets: Vec<TargetInfo> = response
        .into_json()
        .map_err(|err| HarliteError::InvalidArgs(format!("Invalid CDP target JSON: {err}")))?;

    let mut candidates: Vec<TargetInfo> = targets
        .into_iter()
        .filter(|t| t.target_type.as_deref() == Some("page"))
        .collect();

    if let Some(hint) = target_hint {
        let hint_lower = hint.to_lowercase();
        candidates = candidates
            .into_iter()
            .filter(|t| {
                t.id.eq_ignore_ascii_case(hint)
                    || t.url
                        .as_deref()
                        .map(|u| u.to_lowercase().contains(&hint_lower))
                        .unwrap_or(false)
                    || t.title
                        .as_deref()
                        .map(|t| t.to_lowercase().contains(&hint_lower))
                        .unwrap_or(false)
            })
            .collect();
    }

    match candidates.len() {
        0 => Err(HarliteError::InvalidArgs(
            "No matching Chrome targets found".to_string(),
        )),
        1 => Ok(candidates.remove(0)),
        _ => Err(HarliteError::InvalidArgs(
            "Multiple Chrome targets matched; use --target to disambiguate".to_string(),
        )),
    }
}

fn set_socket_timeout(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    timeout: Duration,
) -> Result<()> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout))?,
        _ => {}
    }
    Ok(())
}

fn send_command(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    next_id: &mut u64,
    method: &str,
    params: Value,
) -> Result<u64> {
    let id = *next_id;
    *next_id += 1;
    let payload = json!({ "id": id, "method": method, "params": params });
    socket
        .send(Message::Text(payload.to_string()))
        .map_err(|err| HarliteError::InvalidArgs(format!("CDP send error: {err}")))?;
    Ok(id)
}

fn handle_message(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    next_id: &mut u64,
    state: &mut CaptureState,
    options: &CdpOptions,
    msg: Message,
) -> Result<()> {
    let text = match msg {
        Message::Text(value) => value,
        Message::Binary(value) => String::from_utf8(value)
            .map_err(|err| HarliteError::InvalidArgs(format!("CDP binary parse error: {err}")))?,
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) => return Ok(()),
        Message::Frame(_) => return Ok(()),
    };

    let value: Value = serde_json::from_str(&text)?;

    if let Some(id) = value.get("id").and_then(|v| v.as_u64()) {
        if let Some(request_id) = state.pending_body_requests.remove(&id) {
            if let Some(err) = value.get("error") {
                let message = err
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("CDP error");
                if let Some(record) = state.requests.get_mut(&request_id) {
                    record.failed = Some(message.to_string());
                }
            } else if let Some(result) = value.get("result") {
                let body: ResponseBodyResult = serde_json::from_value(result.clone())?;
                if let Some(record) = state.requests.get_mut(&request_id) {
                    record.body = Some(body);
                }
            }
            finalize_request(state, &request_id, options)?;
        }
        return Ok(());
    }

    let method = match value.get("method").and_then(|v| v.as_str()) {
        Some(m) => m,
        None => return Ok(()),
    };

    match method {
        "Network.requestWillBeSent" => {
            let params = value
                .get("params")
                .ok_or_else(|| HarliteError::InvalidArgs("Missing request params".to_string()))?;
            let event: RequestWillBeSent = serde_json::from_value(params.clone())?;
            let started_ts = event.timestamp;
            state.first_event_ts.get_or_insert(started_ts);
            let record = RequestRecord {
                request: event.request,
                started_wall_time: event.wall_time,
                started_ts,
                response: event.redirect_response,
                response_received_ts: None,
                end_ts: None,
                encoded_data_len: None,
                failed: None,
                body: None,
            };
            state.requests.insert(event.request_id, record);
        }
        "Network.responseReceived" => {
            let params = value
                .get("params")
                .ok_or_else(|| HarliteError::InvalidArgs("Missing response params".to_string()))?;
            let event: ResponseReceived = serde_json::from_value(params.clone())?;
            if let Some(record) = state.requests.get_mut(&event.request_id) {
                record.response = Some(event.response);
                record.response_received_ts = Some(event.timestamp);
            }
        }
        "Network.loadingFinished" => {
            let params = value
                .get("params")
                .ok_or_else(|| HarliteError::InvalidArgs("Missing loading params".to_string()))?;
            let event: LoadingFinished = serde_json::from_value(params.clone())?;
            if let Some(record) = state.requests.get_mut(&event.request_id) {
                record.end_ts = Some(event.timestamp);
                record.encoded_data_len = Some(event.encoded_data_length);
            }
            if options.store_bodies {
                let id = send_command(
                    socket,
                    next_id,
                    "Network.getResponseBody",
                    json!({"requestId": event.request_id}),
                )?;
                state
                    .pending_body_requests
                    .insert(id, event.request_id.clone());
            } else {
                finalize_request(state, &event.request_id, options)?;
            }
        }
        "Network.loadingFailed" => {
            let params = value.get("params").ok_or_else(|| {
                HarliteError::InvalidArgs("Missing loading failed params".to_string())
            })?;
            let event: LoadingFailed = serde_json::from_value(params.clone())?;
            if let Some(record) = state.requests.get_mut(&event.request_id) {
                record.end_ts = Some(event.timestamp);
                record.failed = Some(event.error_text);
            }
            finalize_request(state, &event.request_id, options)?;
        }
        _ => {}
    }

    Ok(())
}

fn finalize_pending_requests(state: &mut CaptureState, options: &CdpOptions) -> Result<()> {
    if !options.store_bodies {
        let pending: Vec<String> = state.requests.keys().cloned().collect();
        for request_id in pending {
            finalize_request(state, &request_id, options)?;
        }
        return Ok(());
    }

    let pending_without_body: Vec<String> = state
        .requests
        .iter()
        .filter_map(|(id, record)| {
            if record.end_ts.is_some() {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect();

    for request_id in pending_without_body {
        finalize_request(state, &request_id, options)?;
    }

    Ok(())
}

fn finalize_request(
    state: &mut CaptureState,
    request_id: &str,
    options: &CdpOptions,
) -> Result<()> {
    let record = match state.requests.remove(request_id) {
        Some(record) => record,
        None => return Ok(()),
    };

    let started_at = started_date_time(state, &record);
    let total_time_ms = record
        .end_ts
        .map(|end| ((end - record.started_ts) * 1000.0).max(0.0))
        .unwrap_or(0.0);

    let response_meta = record.response.clone().unwrap_or_else(|| CdpResponse {
        status: 0,
        status_text: record.failed.clone().unwrap_or_else(|| "".to_string()),
        headers: serde_json::Map::new(),
        mime_type: None,
        protocol: None,
        remote_ip_address: None,
        connection_id: None,
    });

    let request_headers = headers_from_map(&record.request.headers);
    let response_headers = headers_from_map(&response_meta.headers);

    let (content, response_body_size) = build_content(
        &record.body,
        response_meta.mime_type.as_deref(),
        record.encoded_data_len,
        options,
    );

    let request = Request {
        method: record.request.method.clone(),
        url: record.request.url.clone(),
        http_version: response_meta
            .protocol
            .clone()
            .unwrap_or_else(|| "HTTP/1.1".to_string()),
        cookies: None,
        headers: request_headers.clone(),
        query_string: query_params(&record.request.url),
        post_data: build_post_data(&record.request, &request_headers),
        headers_size: None,
        body_size: request_body_size(&record.request.post_data),
        extensions: Extensions::new(),
    };

    let response = Response {
        status: response_meta.status,
        status_text: response_meta.status_text,
        http_version: response_meta
            .protocol
            .clone()
            .unwrap_or_else(|| "HTTP/1.1".to_string()),
        cookies: None,
        headers: response_headers,
        content,
        redirect_url: None,
        headers_size: None,
        body_size: response_body_size,
        extensions: Extensions::new(),
    };

    let timings = build_timings(&record);

    let entry = Entry {
        pageref: None,
        started_date_time: started_at,
        time: total_time_ms,
        request,
        response,
        cache: None,
        timings: Some(timings),
        server_ip_address: response_meta.remote_ip_address,
        connection: response_meta.connection_id.map(|id| id.to_string()),
        extensions: Extensions::new(),
    };

    state.entries.push(entry);
    Ok(())
}

fn started_date_time(state: &CaptureState, record: &RequestRecord) -> String {
    if let Some(wall_time) = record.started_wall_time {
        if let Some(ts) = DateTime::<Utc>::from_timestamp(
            wall_time.trunc() as i64,
            ((wall_time.fract()) * 1_000_000_000.0) as u32,
        ) {
            return ts.to_rfc3339();
        }
    }

    let base_ts = state.first_event_ts.unwrap_or(record.started_ts);
    let offset_ms = ((record.started_ts - base_ts) * 1000.0) as i64;
    (state.capture_started_at + ChronoDuration::milliseconds(offset_ms)).to_rfc3339()
}

fn build_content(
    body: &Option<ResponseBodyResult>,
    mime_type: Option<&str>,
    encoded_len: Option<f64>,
    options: &CdpOptions,
) -> (Content, Option<i64>) {
    let mut content = Content {
        size: -1,
        compression: None,
        mime_type: mime_type.map(|s| s.to_string()),
        text: None,
        encoding: None,
        extensions: Extensions::new(),
    };
    let mut body_size = None;

    if let Some(body) = body {
        if options.text_only {
            if let Some(mime) = mime_type {
                if !is_text_mime_type(mime) {
                    return (content, None);
                }
            }
        }

        if body.base64_encoded {
            if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&body.body) {
                if !within_max_size(&decoded, options.max_body_size) {
                    return (content, None);
                }
                content.text = Some(body.body.clone());
                content.encoding = Some("base64".to_string());
                content.size = decoded.len() as i64;
                body_size = Some(decoded.len() as i64);
            }
        } else if within_max_size(body.body.as_bytes(), options.max_body_size) {
            content.text = Some(body.body.clone());
            content.size = body.body.len() as i64;
            body_size = Some(body.body.len() as i64);
        }
    } else if let Some(len) = encoded_len {
        content.size = len as i64;
        body_size = Some(len as i64);
    }

    (content, body_size)
}

fn headers_from_map(map: &serde_json::Map<String, Value>) -> Vec<Header> {
    map.iter()
        .map(|(name, value)| Header {
            name: name.clone(),
            value: value_to_string(value),
        })
        .collect()
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "".to_string(),
        Value::Array(values) => values
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(", "),
        Value::Object(_) => value.to_string(),
    }
}

fn query_params(url_str: &str) -> Option<Vec<QueryParam>> {
    let url = Url::parse(url_str).ok()?;
    let mut params = Vec::new();
    for (name, value) in url.query_pairs() {
        params.push(QueryParam {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    if params.is_empty() {
        None
    } else {
        Some(params)
    }
}

fn build_post_data(request: &CdpRequest, headers: &[Header]) -> Option<PostData> {
    let post_data = request.post_data.clone()?;
    let mime_type = headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-type"))
        .map(|h| h.value.clone());

    Some(PostData {
        mime_type,
        text: Some(post_data),
        params: None,
        extensions: Extensions::new(),
    })
}

fn build_timings(record: &RequestRecord) -> Timings {
    let mut wait = 0.0;
    let mut receive = 0.0;

    if let (Some(response_ts), Some(end_ts)) = (record.response_received_ts, record.end_ts) {
        wait = ((response_ts - record.started_ts) * 1000.0).max(0.0);
        receive = ((end_ts - response_ts) * 1000.0).max(0.0);
    } else if let Some(end_ts) = record.end_ts {
        wait = ((end_ts - record.started_ts) * 1000.0).max(0.0);
    }

    Timings {
        blocked: None,
        dns: None,
        connect: None,
        send: 0.0,
        wait,
        receive,
        ssl: None,
        extensions: Extensions::new(),
    }
}

fn within_max_size(body: &[u8], max_size: Option<usize>) -> bool {
    match max_size {
        Some(limit) => body.len() <= limit,
        None => true,
    }
}

fn is_text_mime_type(mime: &str) -> bool {
    let m = mime.to_lowercase();
    m.contains("text/")
        || m.contains("json")
        || m.contains("xml")
        || m.contains("javascript")
        || m.contains("css")
        || m.contains("html")
        || m.contains("x-www-form-urlencoded")
}

fn request_body_size(post_data: &Option<String>) -> Option<i64> {
    post_data.as_ref().map(|s| s.len() as i64)
}

fn build_har(version: &VersionInfo, entries: Vec<Entry>) -> Har {
    let creator = Creator {
        name: "harlite".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let browser = version.browser.as_ref().map(|raw| {
        let parts: Vec<&str> = raw.split('/').collect();
        if parts.len() >= 2 {
            Browser {
                name: parts[0].to_string(),
                version: parts[1..].join("/"),
            }
        } else {
            Browser {
                name: raw.to_string(),
                version: "unknown".to_string(),
            }
        }
    });

    Har {
        log: Log {
            version: Some("1.2".to_string()),
            creator: Some(creator),
            browser,
            pages: None,
            entries,
            extensions: Extensions::new(),
        },
    }
}

fn import_entries(path: &PathBuf, har: &Har, options: &CdpOptions) -> Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    create_schema(&conn)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

    let import_id = create_import(&conn, "cdp", Some(&har.log.extensions))?;

    let entry_options = InsertEntryOptions {
        store_bodies: options.store_bodies,
        max_body_size: options.max_body_size,
        text_only: options.text_only,
        decompress_bodies: false,
        keep_compressed: false,
        extract_bodies_dir: None,
        extract_bodies_kind: crate::db::ExtractBodiesKind::Both,
        extract_bodies_shard_depth: 0,
    };

    let mut stats = ImportStats {
        entries_imported: 0,
        request: BlobStats::default(),
        response: BlobStats::default(),
    };

    let tx = conn.unchecked_transaction()?;
    for entry in &har.log.entries {
        let entry_stats = insert_entry(&tx, import_id, entry, &entry_options)?;
        stats.entries_imported += 1;
        stats.request.add_assign(entry_stats.request);
        stats.response.add_assign(entry_stats.response);
    }
    tx.commit()?;

    update_import_count(&conn, import_id, stats.entries_imported)?;

    println!(
        "Imported {} entries to {}",
        stats.entries_imported,
        path.display()
    );
    Ok(())
}
