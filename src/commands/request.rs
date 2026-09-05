use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::ValueEnum;
use serde::Serialize;
use url::Url;

use crate::db::{load_blobs_by_hashes, load_entries, EntryQuery, EntryRow};
use crate::error::{HarliteError, Result};
use crate::har::{Cookie, Entry, Header};

use super::util::{is_sqlite_file, write_bytes_atomic, ExternalPathPolicy};

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[clap(rename_all = "kebab-case")]
pub enum RequestExportFormat {
    Curl,
    Fetch,
    NodeFetch,
    #[value(name = "powershell")]
    PowerShell,
}

pub struct RequestExportOptions {
    pub format: RequestExportFormat,
    pub output: Option<PathBuf>,
    pub force: bool,
    pub include_sensitive: bool,
    pub indexes: Vec<usize>,
    pub limit: Option<usize>,
    pub url_contains: Vec<String>,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct RequestSnapshot {
    method: String,
    url: String,
    headers: Vec<Header>,
    body: Option<Vec<u8>>,
    body_mime_type: Option<String>,
    replace_content_type: bool,
}

#[derive(Clone, Debug)]
struct CapturedRequestBody {
    bytes: Vec<u8>,
    mime_type: Option<String>,
    replace_content_type: bool,
}

pub fn run_request_export(input: PathBuf, options: &RequestExportOptions) -> Result<()> {
    if options.indexes.contains(&0) {
        return Err(HarliteError::InvalidArgs(
            "Request indexes are one-based and must be greater than zero".to_string(),
        ));
    }
    let snapshots = if is_sqlite_file(&input) {
        load_database_requests(&input, options)?
    } else {
        load_har_requests(&input, options)?
    };
    let selected = select_requests(snapshots, options);
    if selected.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No requests matched the supplied filters".to_string(),
        ));
    }

    let mut rendered = String::new();
    if matches!(options.format, RequestExportFormat::NodeFetch) {
        rendered.push_str("import fetch from \"node-fetch\";\n\n");
    }
    for (index, request) in selected.iter().enumerate() {
        if index > 0 {
            rendered.push_str("\n\n");
        }
        rendered.push_str(&render_request(request, options)?);
    }
    rendered.push('\n');
    write_output(
        options.output.as_deref(),
        options.force,
        rendered.as_bytes(),
    )
}

fn load_database_requests(
    path: &Path,
    options: &RequestExportOptions,
) -> Result<Vec<RequestSnapshot>> {
    let connection = super::query::open_readonly_compatible_connection(path)?;

    let query = EntryQuery {
        url_contains: options.url_contains.clone(),
        statuses: options.status.clone(),
        ..EntryQuery::default()
    };
    let rows: Vec<_> = load_entries(&connection, &query)?
        .into_iter()
        .filter(|row| database_entry_matches(row, options))
        .collect();
    let hashes: Vec<String> = rows
        .iter()
        .filter_map(|row| row.request_body_hash.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let policy = ExternalPathPolicy::new(
        path,
        options.allow_external_paths,
        options.external_path_root.as_deref(),
    )?;
    let blobs = load_blobs_by_hashes(&connection, &hashes)?;
    let mut bodies = HashMap::new();
    for blob in blobs {
        let mut content = blob.content;
        if content.is_empty() && blob.size > 0 {
            if let Some(external_path) = blob.external_path.as_deref() {
                if let Some(path) = policy.resolve_file(external_path) {
                    content = std::fs::read(path)?;
                }
            }
        }
        if !content.is_empty() {
            bodies.insert(blob.hash, (content, blob.mime_type));
        }
    }

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let url = row.url?;
            let method = row.method.unwrap_or_else(|| "GET".to_string());
            let mut headers = headers_from_json(row.request_headers.as_deref());
            if options.include_sensitive {
                let cookies = cookies_from_json(row.request_cookies.as_deref());
                append_cookie_header(&mut headers, &cookies);
            }
            let body = row
                .request_body_hash
                .as_deref()
                .and_then(|hash| bodies.get(hash).cloned());
            let (body, body_mime_type) = body
                .map(|(body, mime_type)| (Some(body), mime_type))
                .unwrap_or((None, None));
            Some(RequestSnapshot {
                method,
                url,
                headers,
                body,
                body_mime_type,
                replace_content_type: false,
            })
        })
        .collect())
}

fn database_entry_matches(row: &EntryRow, options: &RequestExportOptions) -> bool {
    if !options.host.is_empty() {
        let host = row
            .host
            .as_deref()
            .map(str::to_ascii_lowercase)
            .or_else(|| {
                row.url.as_deref().and_then(|url| {
                    Url::parse(url)
                        .ok()
                        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
                })
            });
        if host.is_none_or(|host| {
            !options
                .host
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&host))
        }) {
            return false;
        }
    }
    options.method.is_empty()
        || row.method.as_deref().is_some_and(|method| {
            options
                .method
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(method))
        })
}

fn load_har_requests(path: &Path, options: &RequestExportOptions) -> Result<Vec<RequestSnapshot>> {
    let har = crate::har::parse_har_file(path)?;
    Ok(har
        .log
        .entries
        .iter()
        .filter(|entry| har_entry_matches(entry, options))
        .map(|entry| {
            let body = har_request_body(entry);
            let (body, body_mime_type, replace_content_type) = body
                .map(|body| (Some(body.bytes), body.mime_type, body.replace_content_type))
                .unwrap_or((None, None, false));
            RequestSnapshot {
                method: entry.request.method.clone(),
                url: entry.request.url.clone(),
                headers: request_headers(entry, options.include_sensitive),
                body,
                body_mime_type,
                replace_content_type,
            }
        })
        .collect())
}

fn append_content_type_header(
    headers: &mut Vec<Header>,
    body: Option<&[u8]>,
    mime: Option<&str>,
    replace_existing: bool,
) {
    if body.is_none() {
        return;
    }
    let Some(mime) = mime.map(str::trim).filter(|mime| !mime.is_empty()) else {
        return;
    };
    if replace_existing {
        headers.retain(|header| !header.name.eq_ignore_ascii_case("content-type"));
    } else if headers
        .iter()
        .any(|header| header.name.eq_ignore_ascii_case("content-type"))
    {
        return;
    }
    headers.push(Header {
        name: "Content-Type".to_string(),
        value: mime.to_string(),
    });
}

fn har_entry_matches(entry: &Entry, options: &RequestExportOptions) -> bool {
    if !options.url_contains.is_empty()
        && !options.url_contains.iter().any(|needle| {
            entry
                .request
                .url
                .to_ascii_lowercase()
                .contains(&needle.to_ascii_lowercase())
        })
    {
        return false;
    }
    if !options.host.is_empty() {
        let host = Url::parse(&entry.request.url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_ascii_lowercase));
        if host.is_none_or(|host| {
            !options
                .host
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&host))
        }) {
            return false;
        }
    }
    if !options.method.is_empty()
        && !options
            .method
            .iter()
            .any(|method| method.eq_ignore_ascii_case(&entry.request.method))
    {
        return false;
    }
    options.status.is_empty() || options.status.contains(&entry.response.status)
}

fn har_request_body(entry: &Entry) -> Option<CapturedRequestBody> {
    let post_data = entry.request.post_data.as_ref()?;
    let captured_mime = post_data.mime_type.clone();
    if let Some(text) = post_data.text.as_deref() {
        return Some(CapturedRequestBody {
            bytes: text.as_bytes().to_vec(),
            mime_type: captured_mime,
            replace_content_type: false,
        });
    }
    let params = post_data.params.as_ref()?;
    let mime = captured_mime
        .as_deref()
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    if mime.eq_ignore_ascii_case("application/x-www-form-urlencoded") {
        let body: String = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params.iter().map(|param| {
                (
                    param.name.as_str(),
                    param.value.as_deref().unwrap_or_default(),
                )
            }))
            .finish();
        return Some(CapturedRequestBody {
            bytes: body.into_bytes(),
            mime_type: captured_mime,
            replace_content_type: false,
        });
    }
    if mime.eq_ignore_ascii_case("multipart/form-data") {
        let boundary = multipart_boundary(params);
        return Some(CapturedRequestBody {
            bytes: render_multipart_body(params, &boundary),
            mime_type: Some(format!("multipart/form-data; boundary={boundary}")),
            replace_content_type: true,
        });
    }
    None
}

fn multipart_boundary(params: &[crate::har::PostParam]) -> String {
    let mut hasher = blake3::Hasher::new();
    for param in params {
        for value in [
            Some(param.name.as_str()),
            param.value.as_deref(),
            param.file_name.as_deref(),
            param.content_type.as_deref(),
        ] {
            let bytes = value.unwrap_or_default().as_bytes();
            hasher.update(&bytes.len().to_le_bytes());
            hasher.update(bytes);
        }
    }
    let hash = hasher.finalize().to_hex();
    format!("harlite-{}", &hash.as_str()[..24])
}

fn render_multipart_body(params: &[crate::har::PostParam], boundary: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for param in params {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"",
                multipart_quoted(&param.name)
            )
            .as_bytes(),
        );
        if let Some(file_name) = param.file_name.as_deref() {
            body.extend_from_slice(
                format!("; filename=\"{}\"", multipart_quoted(file_name)).as_bytes(),
            );
        }
        body.extend_from_slice(b"\r\n");
        if let Some(content_type) = param
            .content_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.contains(['\r', '\n']))
        {
            body.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
        }
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(param.value.as_deref().unwrap_or_default().as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn multipart_quoted(value: &str) -> String {
    value
        .replace(['\r', '\n'], " ")
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn headers_from_json(json: Option<&str>) -> Vec<Header> {
    let Some(json) = json else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    let Some(object) = value.as_object() else {
        return Vec::new();
    };
    object
        .iter()
        .map(|(name, value)| Header {
            name: name.clone(),
            value: value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string()),
        })
        .collect()
}

fn cookies_from_json(json: Option<&str>) -> Vec<Cookie> {
    json.and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default()
}

fn request_headers(entry: &Entry, include_sensitive: bool) -> Vec<Header> {
    let mut headers = entry.request.headers.clone();
    if include_sensitive {
        append_cookie_header(
            &mut headers,
            entry.request.cookies.as_deref().unwrap_or_default(),
        );
    }
    headers
}

fn append_cookie_header(headers: &mut Vec<Header>, cookies: &[Cookie]) {
    if cookies.is_empty()
        || headers
            .iter()
            .any(|header| header.name.eq_ignore_ascii_case("cookie"))
    {
        return;
    }
    let value = cookies
        .iter()
        .map(|cookie| format!("{}={}", cookie.name, cookie.value))
        .collect::<Vec<_>>()
        .join("; ");
    headers.push(Header {
        name: "Cookie".to_string(),
        value,
    });
}

fn select_requests(
    snapshots: Vec<RequestSnapshot>,
    options: &RequestExportOptions,
) -> Vec<RequestSnapshot> {
    let indexes: HashSet<usize> = options.indexes.iter().copied().collect();
    snapshots
        .into_iter()
        .enumerate()
        .filter(|(index, _)| indexes.is_empty() || indexes.contains(&(index + 1)))
        .map(|(_, request)| request)
        .take(options.limit.unwrap_or(usize::MAX))
        .collect()
}

fn render_request(request: &RequestSnapshot, options: &RequestExportOptions) -> Result<String> {
    let mut snapshot_headers = request.headers.clone();
    append_content_type_header(
        &mut snapshot_headers,
        request.body.as_deref(),
        request.body_mime_type.as_deref(),
        request.replace_content_type,
    );
    let headers: Vec<&Header> = snapshot_headers
        .iter()
        .filter(|header| include_header(&header.name, options.include_sensitive))
        .collect();
    match options.format {
        RequestExportFormat::Curl => Ok(render_curl(request, &headers)),
        RequestExportFormat::Fetch | RequestExportFormat::NodeFetch => {
            render_fetch(request, &headers)
        }
        RequestExportFormat::PowerShell => Ok(render_powershell(request, &headers)),
    }
}

fn include_header(name: &str, include_sensitive: bool) -> bool {
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty()
        || name.starts_with(':')
        || matches!(
            name.as_str(),
            "host"
                | "content-length"
                | "connection"
                | "keep-alive"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        )
    {
        return false;
    }
    include_sensitive || !is_sensitive_header_name(&name)
}

fn is_sensitive_header_name(name: &str) -> bool {
    if matches!(
        name,
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
    ) {
        return true;
    }

    let segments: Vec<&str> = name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.iter().any(|segment| {
        matches!(
            *segment,
            "auth"
                | "authentication"
                | "authorization"
                | "key"
                | "token"
                | "secret"
                | "password"
                | "credential"
                | "signature"
        )
    }) {
        return !matches!(name, "idempotency-key" | "sec-websocket-key");
    }

    name.contains("authorization")
        || name.contains("apikey")
        || name.contains("accesskey")
        || name.contains("privatekey")
        || name.contains("secretkey")
        || name.contains("token")
        || name.contains("secret")
        || name.contains("password")
        || name.contains("credential")
        || name.contains("signature")
}

fn render_curl(request: &RequestSnapshot, headers: &[&Header]) -> String {
    let mut command = Vec::new();
    command.push("curl".to_string());
    command.push(format!("--request {}", shell_quote(&request.method)));
    command.push(format!("--url {}", shell_quote(&request.url)));
    for header in headers {
        command.push(format!(
            "--header {}",
            shell_quote(&format!("{}: {}", header.name, header.value))
        ));
    }
    if let Some(body) = request.body.as_deref() {
        if !body.contains(&0) {
            if let Ok(text) = std::str::from_utf8(body) {
                if text.starts_with('@') {
                    command.push("--data-binary @-".to_string());
                    return format!("printf %s {} | {}", shell_quote(text), command.join(" "));
                }
                command.push(format!("--data-binary {}", shell_quote(text)));
                return command.join(" ");
            }
        }
        let encoded = STANDARD.encode(body);
        command.push("--data-binary @-".to_string());
        return format!(
            "printf %s {} | base64 --decode | {}",
            shell_quote(&encoded),
            command.join(" ")
        );
    }
    command.join(" ")
}

fn render_fetch(request: &RequestSnapshot, headers: &[&Header]) -> Result<String> {
    #[derive(Serialize)]
    struct FetchOptions<'a> {
        method: &'a str,
        headers: Vec<(&'a str, &'a str)>,
    }
    let options = FetchOptions {
        method: &request.method,
        headers: headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_str()))
            .collect(),
    };
    let options_json = serde_json::to_string_pretty(&options)?;
    let options_json = if let Some(body) = request.body.as_deref() {
        let body = if let Ok(text) = std::str::from_utf8(body) {
            serde_json::to_string(text)?
        } else {
            format!(
                "Uint8Array.from(atob({}), c => c.charCodeAt(0))",
                serde_json::to_string(&STANDARD.encode(body))?
            )
        };
        let closing = options_json
            .rfind('}')
            .expect("serialized fetch options object");
        format!(
            "{},\n  \"body\": {body}\n}}",
            options_json[..closing].trim_end()
        )
    } else {
        options_json
    };
    Ok(format!(
        "fetch({}, {})",
        serde_json::to_string(&request.url)?,
        options_json
    ))
}

fn render_powershell(request: &RequestSnapshot, headers: &[&Header]) -> String {
    let mut command = format!(
        "Invoke-WebRequest -Method {} -Uri {}",
        powershell_quote(&request.method),
        powershell_quote(&request.url)
    );
    if !headers.is_empty() {
        let mut combined: Vec<(&str, String)> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for header in headers {
            let normalized = header.name.to_ascii_lowercase();
            if let Some(index) = positions.get(&normalized).copied() {
                let separator = if normalized == "cookie" { "; " } else { ", " };
                combined[index].1.push_str(separator);
                combined[index].1.push_str(&header.value);
            } else {
                positions.insert(normalized, combined.len());
                combined.push((&header.name, header.value.clone()));
            }
        }
        let values = combined
            .iter()
            .map(|(name, value)| {
                format!("{} = {}", powershell_quote(name), powershell_quote(value))
            })
            .collect::<Vec<_>>()
            .join("; ");
        command.push_str(&format!(" -Headers @{{ {values} }}"));
    }
    if let Some(body) = request.body.as_deref() {
        if let Ok(text) = std::str::from_utf8(body) {
            command.push_str(&format!(" -Body {}", powershell_quote(text)));
        } else {
            command.push_str(&format!(
                " -Body ([Convert]::FromBase64String({}))",
                powershell_quote(&STANDARD.encode(body))
            ));
        }
    }
    command
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn powershell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn write_output(path: Option<&Path>, force: bool, bytes: &[u8]) -> Result<()> {
    let Some(path) = path else {
        io::stdout().lock().write_all(bytes)?;
        return Ok(());
    };
    if path == Path::new("-") {
        io::stdout().lock().write_all(bytes)?;
        return Ok(());
    }
    write_bytes_atomic(path, bytes, force)
}

#[cfg(test)]
mod tests {
    use super::{include_header, powershell_quote, shell_quote};

    #[test]
    fn excludes_sensitive_and_transport_headers_by_default() {
        assert!(!include_header("Authorization", false));
        assert!(!include_header("Content-Length", true));
        assert!(include_header("Accept", false));
        assert!(include_header("Authorization", true));
    }

    #[test]
    fn escapes_shells() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
        assert_eq!(powershell_quote("a'b"), "'a''b'");
    }
}
