use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use url::Url;

use crate::db::{ensure_schema_upgrades, load_blobs_by_hashes, BlobRow, EntryRow};
use crate::error::{HarliteError, Result};
use crate::size;

use super::entry_filter::{load_entries_with_filters, EntryFilterOptions};

pub struct OpenApiOptions {
    pub output: Option<PathBuf>,
    pub title: Option<String>,
    pub version: Option<String>,
    pub sample_bodies: Option<usize>,
    pub sample_body_max_size: Option<String>,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
    pub filters: EntryFilterOptions,
}

pub fn run_openapi(database: PathBuf, options: &OpenApiOptions) -> Result<()> {
    let conn = Connection::open(&database)?;
    ensure_schema_upgrades(&conn)?;

    let entries = load_entries_with_filters(&conn, &options.filters)?;

    let output_path = match &options.output {
        Some(p) => p.clone(),
        None => {
            let stem = database
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("openapi");
            PathBuf::from(format!("{stem}-openapi.json"))
        }
    };

    let external_root = if options.allow_external_paths {
        let root = options
            .external_path_root
            .clone()
            .or_else(|| database.parent().map(|p| p.to_path_buf()))
            .ok_or_else(|| {
                HarliteError::InvalidArgs(
                    "Cannot resolve external path root; pass --external-path-root".to_string(),
                )
            })?;
        Some(root.canonicalize()?)
    } else {
        None
    };

    let sample_limit = options.sample_bodies.unwrap_or(0);
    let max_body_size = match options.sample_body_max_size.as_deref() {
        Some(value) => size::parse_size_bytes_i64(value)?,
        None => None,
    };

    let mut servers: BTreeSet<String> = BTreeSet::new();
    let mut paths: BTreeMap<String, BTreeMap<String, OperationData>> = BTreeMap::new();

    for entry in &entries {
        let Some(url) = entry.url.as_deref() else {
            continue;
        };
        let Ok(parsed) = Url::parse(url) else {
            continue;
        };
        let path = parsed.path().to_string();
        let path = if path.is_empty() { "/".to_string() } else { path };

        if let Some(server) = server_from_url(&parsed) {
            servers.insert(server);
        }

        let method = entry
            .method
            .as_deref()
            .unwrap_or("GET")
            .to_ascii_lowercase();

        let method_map = paths.entry(path).or_default();
        let op = method_map.entry(method).or_default();

        for (name, _) in parsed.query_pairs() {
            if !name.is_empty() {
                op.query_params.insert(name.to_string());
            }
        }

        if let Some(content_type) = request_content_type(entry) {
            op.request_content_types.insert(content_type.clone());
            if sample_limit > 0
                && is_json_mime(&content_type)
                && op.request_samples < sample_limit
                && should_sample_body(entry.request_body_size, max_body_size)
            {
                if let Some(hash) = entry.request_body_hash.as_ref() {
                    if let Some(schema) =
                        load_json_schema(&conn, hash, external_root.as_deref(), max_body_size)?
                    {
                        op.request_schema = merge_schema_option(op.request_schema.take(), schema);
                        op.request_samples += 1;
                    }
                }
            }
        }

        let status_key = entry
            .status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "default".to_string());
        let response = op.responses.entry(status_key).or_default();

        if let Some(content_type) = response_content_type(entry) {
            response.content_types.insert(content_type.clone());
            if sample_limit > 0
                && is_json_mime(&content_type)
                && response.samples < sample_limit
                && should_sample_body(entry.response_body_size, max_body_size)
            {
                if let Some(hash) = entry.response_body_hash.as_ref() {
                    if let Some(schema) =
                        load_json_schema(&conn, hash, external_root.as_deref(), max_body_size)?
                    {
                        response.schema = merge_schema_option(response.schema.take(), schema);
                        response.samples += 1;
                    }
                }
            }
        }
    }

    let spec = OpenApiSpec::from_parts(
        options.title.clone(),
        options.version.clone(),
        servers,
        paths,
    );

    let mut writer = open_output(&output_path)?;
    serde_json::to_writer_pretty(&mut writer, &spec)?;
    writer.write_all(b"\n")?;

    if output_path != PathBuf::from("-") {
        println!("Exported OpenAPI schema to {}", output_path.display());
    }

    Ok(())
}

fn open_output(path: &Path) -> Result<Box<dyn Write>> {
    if path == Path::new("-") {
        return Ok(Box::new(io::stdout().lock()));
    }
    Ok(Box::new(BufWriter::new(File::create(path)?)))
}

fn server_from_url(url: &Url) -> Option<String> {
    let scheme = url.scheme();
    let host = url.host_str()?;
    let port = url.port();
    let default_port = url.port_or_known_default();
    let mut base = format!("{}://{}", scheme, host);
    if let Some(port) = port {
        if Some(port) != default_port {
            base.push_str(&format!(":{port}"));
        }
    }
    Some(base)
}

fn headers_map(json: Option<&str>) -> HashMap<String, String> {
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
        .filter_map(|(k, v)| v.as_str().map(|s| (k.to_ascii_lowercase(), s.to_string())))
        .collect()
}

fn normalize_mime(value: &str) -> Option<String> {
    let trimmed = value.split(';').next()?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_ascii_lowercase())
    }
}

fn request_content_type(entry: &EntryRow) -> Option<String> {
    let headers = headers_map(entry.request_headers.as_deref());
    headers.get("content-type").and_then(|s| normalize_mime(s))
}

fn response_content_type(entry: &EntryRow) -> Option<String> {
    if let Some(mime) = entry.response_mime_type.as_deref() {
        return normalize_mime(mime);
    }
    let headers = headers_map(entry.response_headers.as_deref());
    headers.get("content-type").and_then(|s| normalize_mime(s))
}

fn is_json_mime(value: &str) -> bool {
    value.to_ascii_lowercase().contains("json")
}

fn should_sample_body(size: Option<i64>, max_size: Option<i64>) -> bool {
    match (size, max_size) {
        (Some(size), Some(limit)) => size > 0 && size <= limit,
        (Some(size), None) => size > 0,
        (None, Some(_)) => true,
        (None, None) => true,
    }
}

fn load_json_schema(
    conn: &Connection,
    hash: &str,
    external_root: Option<&Path>,
    max_size: Option<i64>,
) -> Result<Option<Schema>> {
    let blobs = load_blobs_by_hashes(conn, &[hash.to_string()])?;
    let Some(blob) = blobs.into_iter().next() else {
        return Ok(None);
    };
    let blob = load_external_blob_content(blob, external_root)?;
    if let Some(limit) = max_size {
        if blob.size > limit {
            return Ok(None);
        }
    }
    if blob.content.is_empty() {
        return Ok(None);
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&blob.content) else {
        return Ok(None);
    };
    Ok(Some(infer_schema(&value)))
}

fn load_external_blob_content(mut blob: BlobRow, external_root: Option<&Path>) -> Result<BlobRow> {
    if !blob.content.is_empty() || blob.size <= 0 {
        return Ok(blob);
    }
    let Some(path) = &blob.external_path else {
        return Ok(blob);
    };
    let Some(root) = external_root else {
        return Ok(blob);
    };

    let candidate = PathBuf::from(path);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    };
    let resolved = match candidate.canonicalize() {
        Ok(p) => p,
        Err(_) => return Ok(blob),
    };
    if !resolved.starts_with(root) {
        return Ok(blob);
    }
    blob.content = std::fs::read(resolved)?;
    Ok(blob)
}

#[derive(Default)]
struct OperationData {
    query_params: BTreeSet<String>,
    request_content_types: BTreeSet<String>,
    request_schema: Option<Schema>,
    request_samples: usize,
    responses: BTreeMap<String, ResponseData>,
}

#[derive(Default)]
struct ResponseData {
    content_types: BTreeSet<String>,
    schema: Option<Schema>,
    samples: usize,
}

#[derive(serde::Serialize)]
struct OpenApiSpec {
    openapi: String,
    info: Info,
    #[serde(skip_serializing_if = "Option::is_none")]
    servers: Option<Vec<Server>>,
    paths: BTreeMap<String, PathItem>,
}

#[derive(serde::Serialize)]
struct Info {
    title: String,
    version: String,
}

#[derive(serde::Serialize)]
struct Server {
    url: String,
}

#[derive(serde::Serialize)]
struct PathItem {
    #[serde(flatten)]
    operations: BTreeMap<String, Operation>,
}

#[derive(serde::Serialize)]
struct Operation {
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<Vec<Parameter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_body: Option<RequestBody>,
    responses: BTreeMap<String, Response>,
}

#[derive(serde::Serialize)]
struct Parameter {
    name: String,
    #[serde(rename = "in")]
    location: String,
    required: bool,
    schema: Schema,
}

#[derive(serde::Serialize)]
struct RequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<BTreeMap<String, MediaType>>,
}

#[derive(serde::Serialize)]
struct Response {
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<BTreeMap<String, MediaType>>,
}

#[derive(serde::Serialize)]
struct MediaType {
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<Schema>,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Schema {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    schema_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<BTreeMap<String, Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    items: Option<Box<Schema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_properties: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nullable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<Vec<Schema>>,
}

impl OpenApiSpec {
    fn from_parts(
        title: Option<String>,
        version: Option<String>,
        servers: BTreeSet<String>,
        paths: BTreeMap<String, BTreeMap<String, OperationData>>,
    ) -> Self {
        let info = Info {
            title: title.unwrap_or_else(|| "Captured API".to_string()),
            version: version.unwrap_or_else(|| "1.0.0".to_string()),
        };
        let servers = if servers.is_empty() {
            None
        } else {
            Some(servers.into_iter().map(|url| Server { url }).collect())
        };

        let mut out_paths: BTreeMap<String, PathItem> = BTreeMap::new();
        for (path, ops) in paths {
            let mut operations: BTreeMap<String, Operation> = BTreeMap::new();
            for (method, data) in ops {
                let parameters = if data.query_params.is_empty() {
                    None
                } else {
                    Some(
                        data.query_params
                            .into_iter()
                            .map(|name| Parameter {
                                name,
                                location: "query".to_string(),
                                required: false,
                                schema: Schema {
                                    schema_type: Some("string".to_string()),
                                    properties: None,
                                    items: None,
                                    additional_properties: None,
                                    nullable: None,
                                    one_of: None,
                                },
                            })
                            .collect(),
                    )
                };

                let request_body = if data.request_content_types.is_empty() {
                    None
                } else {
                    let mut content: BTreeMap<String, MediaType> = BTreeMap::new();
                    for mime in data.request_content_types {
                        content.insert(
                            mime,
                            MediaType {
                                schema: data.request_schema.clone(),
                            },
                        );
                    }
                    Some(RequestBody {
                        content: Some(content),
                    })
                };

                let mut responses: BTreeMap<String, Response> = BTreeMap::new();
                for (status, resp) in data.responses {
                    let content = if resp.content_types.is_empty() {
                        None
                    } else {
                        let mut content: BTreeMap<String, MediaType> = BTreeMap::new();
                        for mime in resp.content_types {
                            content.insert(
                                mime,
                                MediaType {
                                    schema: resp.schema.clone(),
                                },
                            );
                        }
                        Some(content)
                    };
                    responses.insert(
                        status.clone(),
                        Response {
                            description: format!("Status {status}"),
                            content,
                        },
                    );
                }

                if responses.is_empty() {
                    responses.insert(
                        "default".to_string(),
                        Response {
                            description: "Default response".to_string(),
                            content: None,
                        },
                    );
                }

                operations.insert(
                    method,
                    Operation {
                        parameters,
                        request_body,
                        responses,
                    },
                );
            }
            out_paths.insert(path, PathItem { operations });
        }

        Self {
            openapi: "3.0.3".to_string(),
            info,
            servers,
            paths: out_paths,
        }
    }
}

fn infer_schema(value: &serde_json::Value) -> Schema {
    match value {
        serde_json::Value::Null => Schema {
            schema_type: None,
            properties: None,
            items: None,
            additional_properties: None,
            nullable: Some(true),
            one_of: None,
        },
        serde_json::Value::Bool(_) => Schema {
            schema_type: Some("boolean".to_string()),
            properties: None,
            items: None,
            additional_properties: None,
            nullable: None,
            one_of: None,
        },
        serde_json::Value::Number(num) => Schema {
            schema_type: Some(if num.is_i64() { "integer" } else { "number" }.to_string()),
            properties: None,
            items: None,
            additional_properties: None,
            nullable: None,
            one_of: None,
        },
        serde_json::Value::String(_) => Schema {
            schema_type: Some("string".to_string()),
            properties: None,
            items: None,
            additional_properties: None,
            nullable: None,
            one_of: None,
        },
        serde_json::Value::Array(items) => {
            let merged = items.iter().map(infer_schema).reduce(merge_schema);
            Schema {
                schema_type: Some("array".to_string()),
                properties: None,
                items: merged.map(Box::new),
                additional_properties: None,
                nullable: None,
                one_of: None,
            }
        }
        serde_json::Value::Object(map) => {
            let mut properties: BTreeMap<String, Schema> = BTreeMap::new();
            for (key, value) in map {
                properties.insert(key.clone(), infer_schema(value));
            }
            Schema {
                schema_type: Some("object".to_string()),
                properties: Some(properties),
                items: None,
                additional_properties: Some(true),
                nullable: None,
                one_of: None,
            }
        }
    }
}

fn merge_schema_option(current: Option<Schema>, next: Schema) -> Option<Schema> {
    Some(match current {
        Some(existing) => merge_schema(existing, next),
        None => next,
    })
}

fn merge_schema(a: Schema, b: Schema) -> Schema {
    if a.schema_type.is_none() && a.nullable == Some(true) {
        let mut out = b;
        out.nullable = Some(true);
        return out;
    }
    if b.schema_type.is_none() && b.nullable == Some(true) {
        let mut out = a;
        out.nullable = Some(true);
        return out;
    }

    if a.schema_type == b.schema_type {
        return match a.schema_type.as_deref() {
            Some("object") => {
                let mut properties = a.properties.unwrap_or_default();
                if let Some(next_props) = b.properties {
                    for (key, value) in next_props {
                        let merged = match properties.remove(&key) {
                            Some(existing) => merge_schema(existing, value),
                            None => value,
                        };
                        properties.insert(key, merged);
                    }
                }
                Schema {
                    schema_type: Some("object".to_string()),
                    properties: Some(properties),
                    items: None,
                    additional_properties: Some(true),
                    nullable: a.nullable.or(b.nullable),
                    one_of: None,
                }
            }
            Some("array") => Schema {
                schema_type: Some("array".to_string()),
                properties: None,
                items: match (a.items, b.items) {
                    (Some(a_items), Some(b_items)) => Some(Box::new(merge_schema(*a_items, *b_items))),
                    (Some(items), None) => Some(items),
                    (None, Some(items)) => Some(items),
                    (None, None) => None,
                },
                additional_properties: None,
                nullable: a.nullable.or(b.nullable),
                one_of: None,
            },
            _ => Schema {
                schema_type: a.schema_type,
                properties: None,
                items: None,
                additional_properties: None,
                nullable: a.nullable.or(b.nullable),
                one_of: None,
            },
        };
    }

    Schema {
        schema_type: None,
        properties: None,
        items: None,
        additional_properties: None,
        nullable: None,
        one_of: Some(vec![a, b]),
    }
}
