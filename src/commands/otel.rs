use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use rusqlite::Connection;
use serde::Serialize;
use url::Url;

use crate::db::{ensure_schema_upgrades, EntryRow};
use crate::error::{HarliteError, Result};

use super::entry_filter::{load_entries_with_filters, EntryFilterOptions};

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum OtelExportFormat {
    Json,
    OtlpHttp,
    OtlpGrpc,
}

pub struct OtelExportOptions {
    pub format: OtelExportFormat,
    pub output: Option<PathBuf>,
    pub endpoint: Option<String>,
    pub service_name: String,
    pub resource_attr: Vec<String>,
    pub include_phases: bool,
    pub sample_rate: f64,
    pub max_spans: Option<usize>,
    pub filters: EntryFilterOptions,
}

pub fn run_otel(database: PathBuf, options: &OtelExportOptions) -> Result<()> {
    let conn = Connection::open(&database)?;
    ensure_schema_upgrades(&conn)?;

    validate_sampling(options.sample_rate)?;

    let entries = load_entries_with_filters(&conn, &options.filters)?;
    let entries = sample_entries(entries, options.sample_rate);
    let entries = match options.max_spans {
        Some(max) => entries.into_iter().take(max).collect::<Vec<_>>(),
        None => entries,
    };

    let resource_attrs = build_resource_attributes(&options.service_name, &options.resource_attr)?;
    let mut spans: Vec<SpanRecord> = Vec::new();
    let mut skipped = 0usize;

    for entry in entries {
        match entry_to_spans(&entry, options.include_phases) {
            Some(mut entry_spans) => spans.append(&mut entry_spans),
            None => skipped += 1,
        }
    }

    match options.format {
        OtelExportFormat::Json => {
            let output_path = options.output.clone().unwrap_or_else(|| PathBuf::from("-"));
            let mut writer = open_output(&output_path)?;
            let payload = build_json_export(resource_attrs, spans);
            serde_json::to_writer(&mut writer, &payload)?;
            writer.write_all(b"\n")?;
            if output_path != PathBuf::from("-") {
                println!("Exported {} spans to {}", payload.span_count(), output_path.display());
            }
            if skipped > 0 {
                eprintln!("Skipped {} entries without a valid start timestamp", skipped);
            }
            Ok(())
        }
        OtelExportFormat::OtlpHttp => {
            let endpoint = options
                .endpoint
                .as_deref()
                .ok_or_else(|| HarliteError::InvalidArgs("--endpoint is required for OTLP export".to_string()))?;
            let endpoint = normalize_otlp_http_endpoint(endpoint)?;
            let request = build_otlp_request(resource_attrs, spans);
            send_otlp_http(&endpoint, request)?;
            if skipped > 0 {
                eprintln!("Skipped {} entries without a valid start timestamp", skipped);
            }
            Ok(())
        }
        OtelExportFormat::OtlpGrpc => {
            let endpoint = options
                .endpoint
                .as_deref()
                .ok_or_else(|| HarliteError::InvalidArgs("--endpoint is required for OTLP export".to_string()))?;
            let request = build_otlp_request(resource_attrs, spans);
            send_otlp_grpc(endpoint, request)?;
            if skipped > 0 {
                eprintln!("Skipped {} entries without a valid start timestamp", skipped);
            }
            Ok(())
        }
    }
}

fn open_output(path: &Path) -> Result<Box<dyn Write>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout().lock()));
    }
    Ok(Box::new(BufWriter::new(File::create(path)?)))
}

fn validate_sampling(sample_rate: f64) -> Result<()> {
    if !(0.0..=1.0).contains(&sample_rate) {
        return Err(HarliteError::InvalidArgs(
            "--sample-rate must be between 0.0 and 1.0".to_string(),
        ));
    }
    Ok(())
}

fn sample_entries(entries: Vec<EntryRow>, sample_rate: f64) -> Vec<EntryRow> {
    if sample_rate >= 1.0 {
        return entries;
    }
    if sample_rate <= 0.0 {
        return Vec::new();
    }
    let threshold = (sample_rate * (u64::MAX as f64)) as u64;
    entries
        .into_iter()
        .filter(|entry| {
            let mut hasher = blake3::Hasher::new();
            if let Some(hash) = entry.entry_hash.as_deref() {
                hasher.update(hash.as_bytes());
            }
            if let Some(url) = entry.url.as_deref() {
                hasher.update(url.as_bytes());
            }
            if let Some(started_at) = entry.started_at.as_deref() {
                hasher.update(started_at.as_bytes());
            }
            let digest = hasher.finalize();
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&digest.as_bytes()[..8]);
            u64::from_le_bytes(bytes) <= threshold
        })
        .collect()
}

#[derive(Clone, Debug)]
struct Attribute {
    key: String,
    value: AttrValue,
}

#[derive(Clone, Debug)]
enum AttrValue {
    String(String),
    Int(i64),
    Bool(bool),
}

#[derive(Clone, Debug)]
struct SpanRecord {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: String,
    kind: SpanKind,
    start_unix_nano: u64,
    end_unix_nano: u64,
    attributes: Vec<Attribute>,
    status: Option<SpanStatus>,
}

#[derive(Clone, Debug)]
struct SpanStatus {
    code: SpanStatusCode,
    message: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum SpanStatusCode {
    Ok,
    Error,
}

#[derive(Clone, Copy, Debug)]
enum SpanKind {
    Internal,
    Client,
}

fn build_resource_attributes(
    service_name: &str,
    extra: &[String],
) -> Result<Vec<Attribute>> {
    let mut attrs = vec![Attribute {
        key: "service.name".to_string(),
        value: AttrValue::String(service_name.to_string()),
    }];
    for item in extra {
        let Some((key, value)) = item.split_once('=') else {
            return Err(HarliteError::InvalidArgs(format!(
                "Resource attribute must be key=value, got: {item}"
            )));
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() {
            return Err(HarliteError::InvalidArgs(format!(
                "Resource attribute key cannot be empty: {item}"
            )));
        }
        if value.is_empty() {
            return Err(HarliteError::InvalidArgs(format!(
                "Resource attribute value cannot be empty: {item}"
            )));
        }
        attrs.push(Attribute {
            key: key.to_string(),
            value: AttrValue::String(value.to_string()),
        });
    }
    Ok(attrs)
}

fn entry_to_spans(entry: &EntryRow, include_phases: bool) -> Option<Vec<SpanRecord>> {
    let start = parse_started_at(entry.started_at.as_deref())?;
    let base_ns = start.timestamp_nanos_opt()? as i128;
    if base_ns < 0 {
        return None;
    }
    let base_ns = base_ns as u64;

    let trace_id = trace_id_for_entry(entry);
    let span_id = span_id_for_entry(entry, "request");
    let mut spans = Vec::new();

    let (name, mut attributes) = request_name_and_attributes(entry);
    let (start_ns, end_ns) = request_bounds(entry, base_ns);
    let status = status_from_http(entry.status);

    spans.push(SpanRecord {
        trace_id,
        span_id,
        parent_span_id: None,
        name,
        kind: SpanKind::Client,
        start_unix_nano: start_ns,
        end_unix_nano: end_ns,
        attributes: attributes.drain(..).collect(),
        status,
    });

    if include_phases {
        let phase_ranges = phase_ranges_ms(entry);
        for phase in phase_ranges {
            let phase_span_id = span_id_for_entry(entry, phase.name);
            spans.push(SpanRecord {
                trace_id,
                span_id: phase_span_id,
                parent_span_id: Some(span_id),
                name: format!("har.{}", phase.name),
                kind: SpanKind::Internal,
                start_unix_nano: base_ns + ms_to_ns(phase.start_ms),
                end_unix_nano: base_ns + ms_to_ns(phase.end_ms),
                attributes: vec![Attribute {
                    key: "har.phase".to_string(),
                    value: AttrValue::String(phase.name.to_string()),
                }],
                status: None,
            });
        }
    }

    Some(spans)
}

fn parse_started_at(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?;
    let parsed = DateTime::parse_from_rfc3339(value).ok()?;
    Some(parsed.with_timezone(&Utc))
}

fn trace_id_for_entry(entry: &EntryRow) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    if let Some(hash) = entry.entry_hash.as_deref() {
        hasher.update(hash.as_bytes());
    }
    if let Some(url) = entry.url.as_deref() {
        hasher.update(url.as_bytes());
    }
    if let Some(started_at) = entry.started_at.as_deref() {
        hasher.update(started_at.as_bytes());
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

fn span_id_for_entry(entry: &EntryRow, suffix: &str) -> [u8; 8] {
    let mut hasher = blake3::Hasher::new();
    if let Some(hash) = entry.entry_hash.as_deref() {
        hasher.update(hash.as_bytes());
    }
    if let Some(url) = entry.url.as_deref() {
        hasher.update(url.as_bytes());
    }
    if let Some(started_at) = entry.started_at.as_deref() {
        hasher.update(started_at.as_bytes());
    }
    hasher.update(suffix.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest.as_bytes()[..8]);
    out
}

fn request_name_and_attributes(entry: &EntryRow) -> (String, Vec<Attribute>) {
    let method = entry.method.clone().unwrap_or_else(|| "HTTP".to_string());
    let target = request_target(entry);
    let name = format!("{} {}", method, target);

    let mut attrs = Vec::new();
    if let Some(url) = entry.url.as_deref() {
        attrs.push(Attribute {
            key: "http.url".to_string(),
            value: AttrValue::String(url.to_string()),
        });
        attrs.push(Attribute {
            key: "url.full".to_string(),
            value: AttrValue::String(url.to_string()),
        });
    }

    attrs.push(Attribute {
        key: "http.method".to_string(),
        value: AttrValue::String(method.clone()),
    });
    attrs.push(Attribute {
        key: "http.request.method".to_string(),
        value: AttrValue::String(method),
    });

    if let Some(target) = entry.path.as_deref() {
        attrs.push(Attribute {
            key: "http.target".to_string(),
            value: AttrValue::String(target.to_string()),
        });
    }

    if let Some(status) = entry.status {
        attrs.push(Attribute {
            key: "http.status_code".to_string(),
            value: AttrValue::Int(i64::from(status)),
        });
        attrs.push(Attribute {
            key: "http.response.status_code".to_string(),
            value: AttrValue::Int(i64::from(status)),
        });
    }

    if let Some(size) = entry.request_body_size {
        attrs.push(Attribute {
            key: "http.request_content_length".to_string(),
            value: AttrValue::Int(size),
        });
    }

    if let Some(size) = entry.response_body_size {
        attrs.push(Attribute {
            key: "http.response_content_length".to_string(),
            value: AttrValue::Int(size),
        });
    }

    if let Some(size) = entry.response_body_size_raw {
        attrs.push(Attribute {
            key: "http.response_content_length_raw".to_string(),
            value: AttrValue::Int(size),
        });
    }

    if let Some(host) = entry.host.as_deref() {
        attrs.push(Attribute {
            key: "net.peer.name".to_string(),
            value: AttrValue::String(host.to_string()),
        });
    }

    if let Some(ip) = entry.server_ip.as_deref() {
        attrs.push(Attribute {
            key: "net.peer.ip".to_string(),
            value: AttrValue::String(ip.to_string()),
        });
    }

    if let Some(http_version) = entry.http_version.as_deref() {
        attrs.push(Attribute {
            key: "http.flavor".to_string(),
            value: AttrValue::String(http_version.to_string()),
        });
    }

    if let Some(cache_attrs) = cache_attributes(entry.response_headers.as_deref()) {
        attrs.extend(cache_attrs);
    }

    (name, attrs)
}

fn request_target(entry: &EntryRow) -> String {
    if let Some(path) = entry.path.as_deref() {
        if let Some(query) = entry.query_string.as_deref() {
            if !query.is_empty() {
                return format!("{}?{}", path, query);
            }
        }
        return path.to_string();
    }
    if let Some(url) = entry.url.as_deref() {
        return url.to_string();
    }
    "unknown".to_string()
}

fn cache_attributes(headers_json: Option<&str>) -> Option<Vec<Attribute>> {
    let headers = headers_from_json(headers_json)?;
    if headers.is_empty() {
        return None;
    }
    let mut attrs = Vec::new();
    if let Some(value) = headers.get("cache-control") {
        attrs.push(Attribute {
            key: "http.cache_control".to_string(),
            value: AttrValue::String(value.to_string()),
        });
    }
    if let Some(value) = headers.get("age") {
        if let Ok(parsed) = value.trim().parse::<i64>() {
            attrs.push(Attribute {
                key: "http.cache_age".to_string(),
                value: AttrValue::Int(parsed),
            });
        }
    }

    let cache_status = headers
        .get("x-cache")
        .or(headers.get("x-cache-status"))
        .or(headers.get("cf-cache-status"));
    if let Some(value) = cache_status {
        attrs.push(Attribute {
            key: "http.cache_status".to_string(),
            value: AttrValue::String(value.to_string()),
        });
        let hit = value.to_ascii_lowercase().contains("hit");
        attrs.push(Attribute {
            key: "http.cache_hit".to_string(),
            value: AttrValue::Bool(hit),
        });
    }

    Some(attrs)
}

fn headers_from_json(json: Option<&str>) -> Option<HashMap<String, String>> {
    let json = json?;
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return None;
    };
    let Some(obj) = value.as_object() else {
        return None;
    };
    let mut map = HashMap::new();
    for (key, value) in obj {
        if let Some(val) = value.as_str() {
            map.insert(key.to_string(), val.to_string());
        }
    }
    Some(map)
}

fn request_bounds(entry: &EntryRow, base_ns: u64) -> (u64, u64) {
    let total_ms = normalize_ms(entry.time_ms).unwrap_or_else(|| {
        let ranges = phase_ranges_ms(entry);
        ranges
            .last()
            .map(|phase| phase.end_ms)
            .unwrap_or(0.0)
    });
    let total_ns = ms_to_ns(total_ms);
    (base_ns, base_ns.saturating_add(total_ns))
}

fn status_from_http(status: Option<i32>) -> Option<SpanStatus> {
    let status = status?;
    if status >= 400 {
        Some(SpanStatus {
            code: SpanStatusCode::Error,
            message: Some(format!("HTTP {status}")),
        })
    } else {
        Some(SpanStatus {
            code: SpanStatusCode::Ok,
            message: None,
        })
    }
}

fn normalize_ms(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v >= 0.0 => Some(v),
        _ => None,
    }
}

struct PhaseRange {
    name: &'static str,
    start_ms: f64,
    end_ms: f64,
}

fn phase_ranges_ms(entry: &EntryRow) -> Vec<PhaseRange> {
    let mut out = Vec::new();
    let mut cursor = 0.0;

    let blocked = normalize_ms(entry.blocked_ms);
    let dns = normalize_ms(entry.dns_ms);
    let connect = normalize_ms(entry.connect_ms);
    let send = normalize_ms(entry.send_ms);
    let wait = normalize_ms(entry.wait_ms);
    let receive = normalize_ms(entry.receive_ms);
    let ssl = normalize_ms(entry.ssl_ms);

    if let Some(ms) = blocked {
        out.push(PhaseRange {
            name: "blocked",
            start_ms: cursor,
            end_ms: cursor + ms,
        });
        cursor += ms;
    }
    if let Some(ms) = dns {
        out.push(PhaseRange {
            name: "dns",
            start_ms: cursor,
            end_ms: cursor + ms,
        });
        cursor += ms;
    }

    if let Some(ms) = connect {
        let connect_start = cursor;
        out.push(PhaseRange {
            name: "connect",
            start_ms: connect_start,
            end_ms: connect_start + ms,
        });
        if let Some(ssl_ms) = ssl {
            if ssl_ms > 0.0 && ssl_ms <= ms {
                out.push(PhaseRange {
                    name: "ssl",
                    start_ms: connect_start + (ms - ssl_ms),
                    end_ms: connect_start + ms,
                });
            }
        }
        cursor += ms;
    } else if let Some(ms) = ssl {
        out.push(PhaseRange {
            name: "ssl",
            start_ms: cursor,
            end_ms: cursor + ms,
        });
        cursor += ms;
    }

    if let Some(ms) = send {
        out.push(PhaseRange {
            name: "send",
            start_ms: cursor,
            end_ms: cursor + ms,
        });
        cursor += ms;
    }
    if let Some(ms) = wait {
        out.push(PhaseRange {
            name: "wait",
            start_ms: cursor,
            end_ms: cursor + ms,
        });
        cursor += ms;
    }
    if let Some(ms) = receive {
        out.push(PhaseRange {
            name: "receive",
            start_ms: cursor,
            end_ms: cursor + ms,
        });
    }

    out
}

fn ms_to_ns(ms: f64) -> u64 {
    if ms <= 0.0 {
        return 0;
    }
    (ms * 1_000_000.0).round() as u64
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

fn normalize_otlp_http_endpoint(endpoint: &str) -> Result<String> {
    let parsed = Url::parse(endpoint).map_err(|_| {
        HarliteError::InvalidArgs("--endpoint must be a valid URL".to_string())
    })?;
    let trimmed = endpoint.trim_end_matches('/');
    let path = parsed.path().trim_end_matches('/');
    if path.ends_with("/v1/traces") {
        return Ok(trimmed.to_string());
    }
    let mut base = trimmed.to_string();
    base.push_str("/v1/traces");
    Ok(base)
}

fn build_json_export(resource_attrs: Vec<Attribute>, spans: Vec<SpanRecord>) -> JsonExport {
    JsonExport {
        resource_spans: vec![JsonResourceSpans {
            resource: JsonResource {
                attributes: attrs_to_json(resource_attrs),
            },
            scope_spans: vec![JsonScopeSpans {
                scope: JsonScope {
                    name: "harlite".to_string(),
                    version: None,
                },
                spans: spans.into_iter().map(span_to_json).collect(),
            }],
        }],
    }
}

fn span_to_json(span: SpanRecord) -> JsonSpan {
    JsonSpan {
        trace_id: hex_encode(&span.trace_id),
        span_id: hex_encode(&span.span_id),
        parent_span_id: span.parent_span_id.map(|id| hex_encode(&id)),
        name: span.name,
        kind: match span.kind {
            SpanKind::Internal => 1,
            SpanKind::Client => 3,
        },
        start_time_unix_nano: span.start_unix_nano,
        end_time_unix_nano: span.end_unix_nano,
        attributes: attrs_to_json(span.attributes),
        status: span.status.map(status_to_json),
    }
}

fn status_to_json(status: SpanStatus) -> JsonStatus {
    JsonStatus {
        code: match status.code {
            SpanStatusCode::Ok => 1,
            SpanStatusCode::Error => 2,
        },
        message: status.message,
    }
}

fn attrs_to_json(attrs: Vec<Attribute>) -> Vec<JsonKeyValue> {
    attrs
        .into_iter()
        .map(|attr| JsonKeyValue {
            key: attr.key,
            value: match attr.value {
                AttrValue::String(value) => JsonAnyValue {
                    string_value: Some(value),
                    ..Default::default()
                },
                AttrValue::Int(value) => JsonAnyValue {
                    int_value: Some(value),
                    ..Default::default()
                },
                AttrValue::Bool(value) => JsonAnyValue {
                    bool_value: Some(value),
                    ..Default::default()
                },
            },
        })
        .collect()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonExport {
    resource_spans: Vec<JsonResourceSpans>,
}

impl JsonExport {
    fn span_count(&self) -> usize {
        self.resource_spans
            .iter()
            .flat_map(|rs| rs.scope_spans.iter())
            .map(|ss| ss.spans.len())
            .sum()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonResourceSpans {
    resource: JsonResource,
    scope_spans: Vec<JsonScopeSpans>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonResource {
    attributes: Vec<JsonKeyValue>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonScopeSpans {
    scope: JsonScope,
    spans: Vec<JsonSpan>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonScope {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonSpan {
    trace_id: String,
    span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<String>,
    name: String,
    kind: i32,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: Vec<JsonKeyValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<JsonStatus>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonStatus {
    code: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonKeyValue {
    key: String,
    value: JsonAnyValue,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonAnyValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    string_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    int_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    double_value: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bool_value: Option<bool>,
}

fn build_otlp_request(
    resource_attrs: Vec<Attribute>,
    spans: Vec<SpanRecord>,
) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, InstrumentationScope, KeyValue,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{
        span, status, ResourceSpans, ScopeSpans, Span, Status,
    };

    let resource = Resource {
        attributes: resource_attrs
            .into_iter()
            .map(|attr| KeyValue {
                key: attr.key,
                value: Some(AnyValue {
                    value: Some(match attr.value {
                        AttrValue::String(value) => any_value::Value::StringValue(value),
                        AttrValue::Int(value) => any_value::Value::IntValue(value),
                        AttrValue::Bool(value) => any_value::Value::BoolValue(value),
                    }),
                }),
            })
            .collect(),
        dropped_attributes_count: 0,
    };

    let spans = spans
        .into_iter()
        .map(|span_record| {
            let status = span_record.status.map(|status| Status {
                code: match status.code {
                    SpanStatusCode::Ok => status::StatusCode::Ok as i32,
                    SpanStatusCode::Error => status::StatusCode::Error as i32,
                },
                message: status.message.unwrap_or_default(),
            });
            Span {
                trace_id: span_record.trace_id.to_vec(),
                span_id: span_record.span_id.to_vec(),
                parent_span_id: span_record
                    .parent_span_id
                    .map(|id| id.to_vec())
                    .unwrap_or_default(),
                name: span_record.name,
                kind: match span_record.kind {
                    SpanKind::Internal => span::SpanKind::Internal as i32,
                    SpanKind::Client => span::SpanKind::Client as i32,
                },
                start_time_unix_nano: span_record.start_unix_nano,
                end_time_unix_nano: span_record.end_unix_nano,
                attributes: span_record
                    .attributes
                    .into_iter()
                    .map(|attr| KeyValue {
                        key: attr.key,
                        value: Some(AnyValue {
                            value: Some(match attr.value {
                                AttrValue::String(value) => any_value::Value::StringValue(value),
                        AttrValue::Int(value) => any_value::Value::IntValue(value),
                        AttrValue::Bool(value) => any_value::Value::BoolValue(value),
                            }),
                        }),
                    })
                    .collect(),
                dropped_attributes_count: 0,
                events: Vec::new(),
                dropped_events_count: 0,
                links: Vec::new(),
                dropped_links_count: 0,
                status,
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();

    let scope_spans = ScopeSpans {
        scope: Some(InstrumentationScope {
            name: "harlite".to_string(),
            version: "".to_string(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        }),
        spans,
        schema_url: "".to_string(),
    };

    let resource_spans = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![scope_spans],
        schema_url: "".to_string(),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}

fn send_otlp_http(
    endpoint: &str,
    request: opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
) -> Result<()> {
    use prost::Message;

    let mut buf = Vec::new();
    request
        .encode(&mut buf)
        .map_err(|err| HarliteError::InvalidArgs(format!("OTLP encoding failed: {err}")))?;

    let response = ureq::post(endpoint)
        .set("Content-Type", "application/x-protobuf")
        .send_bytes(&buf);

    match response {
        Ok(_) => Ok(()),
        Err(ureq::Error::Status(code, resp)) => Err(HarliteError::InvalidArgs(format!(
            "OTLP HTTP export failed with status {}: {}",
            code,
            resp.status_text()
        ))),
        Err(err) => Err(HarliteError::InvalidArgs(format!(
            "OTLP HTTP export failed: {err}"
        ))),
    }
}

fn send_otlp_grpc(
    endpoint: &str,
    request: opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
) -> Result<()> {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;

    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{}", endpoint)
    };

    let rt = tokio::runtime::Runtime::new()
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to start runtime: {err}")))?;
    rt.block_on(async move {
        let mut client = TraceServiceClient::connect(endpoint)
            .await
            .map_err(|err| {
                HarliteError::InvalidArgs(format!("OTLP gRPC connect failed: {err}"))
            })?;
        client
            .export(request)
            .await
            .map_err(|err| HarliteError::InvalidArgs(format!("OTLP gRPC export failed: {err}")))?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::{phase_ranges_ms, request_bounds};
    use crate::db::EntryRow;

    fn entry_with_times() -> EntryRow {
        EntryRow {
            import_id: 1,
            page_id: None,
            started_at: Some("2024-01-15T12:00:00.000Z".to_string()),
            time_ms: Some(120.0),
            blocked_ms: Some(10.0),
            dns_ms: Some(20.0),
            connect_ms: Some(40.0),
            send_ms: Some(10.0),
            wait_ms: Some(30.0),
            receive_ms: Some(10.0),
            ssl_ms: Some(15.0),
            method: None,
            url: None,
            host: None,
            path: None,
            query_string: None,
            http_version: None,
            request_headers: None,
            request_cookies: None,
            request_body_hash: None,
            request_body_size: None,
            status: None,
            status_text: None,
            response_headers: None,
            response_cookies: None,
            response_body_hash: None,
            response_body_size: None,
            response_body_hash_raw: None,
            response_body_size_raw: None,
            response_mime_type: None,
            is_redirect: None,
            server_ip: None,
            connection_id: None,
            request_id: None,
            parent_request_id: None,
            initiator_type: None,
            initiator_url: None,
            initiator_line: None,
            initiator_column: None,
            redirect_url: None,
            tls_version: None,
            tls_cipher_suite: None,
            tls_cert_subject: None,
            tls_cert_issuer: None,
            tls_cert_expiry: None,
            entry_hash: None,
            entry_extensions: None,
            request_extensions: None,
            response_extensions: None,
            content_extensions: None,
            timings_extensions: None,
            post_data_extensions: None,
            graphql_operation_type: None,
            graphql_operation_name: None,
            graphql_top_level_fields: None,
        }
    }

    #[test]
    fn phase_ranges_respect_ssl_inside_connect() {
        let entry = entry_with_times();
        let phases = phase_ranges_ms(&entry);
        let ssl = phases.iter().find(|p| p.name == "ssl").unwrap();
        let connect = phases.iter().find(|p| p.name == "connect").unwrap();
        assert!(ssl.start_ms >= connect.start_ms);
        assert_eq!(ssl.end_ms, connect.end_ms);
    }

    #[test]
    fn request_bounds_use_total_time() {
        let entry = entry_with_times();
        let (start, end) = request_bounds(&entry, 1_000_000);
        assert!(end > start);
    }
}
