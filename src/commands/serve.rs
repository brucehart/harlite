use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use clap::ValueEnum;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, StatusCode};
use rusqlite::{Connection, OpenFlags};
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use url::Url;

use crate::db::{ensure_schema_upgrades, load_blobs_by_hashes, load_entries, BlobRow, EntryQuery};
use crate::error::{HarliteError, Result};
use crate::har::{parse_har_file, Content, Header};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum MatchMode {
    Strict,
    Fuzzy,
}

pub struct ServeOptions {
    pub bind: String,
    pub port: u16,
    pub match_mode: MatchMode,
    pub allow_external_paths: bool,
    pub external_path_root: Option<PathBuf>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

#[derive(Clone)]
struct ServeEntry {
    method: String,
    url: String,
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
    mime_type: Option<String>,
    started_at: Option<String>,
    normalized: Option<NormalizedUrl>,
}

#[derive(Clone, Debug)]
struct NormalizedUrl {
    host: String,
    port: Option<u16>,
    path: String,
    query: Vec<(String, String)>,
}

#[derive(Clone)]
struct ServeState {
    entries: Vec<ServeEntry>,
    match_mode: MatchMode,
    scheme: String,
}

pub fn run_serve(input: PathBuf, options: &ServeOptions) -> Result<()> {
    if options.tls_cert.is_some() != options.tls_key.is_some() {
        return Err(HarliteError::InvalidArgs(
            "--tls-cert and --tls-key must be provided together".to_string(),
        ));
    }

    let entries = if is_db_path(&input) {
        load_entries_from_db(&input, options)?
    } else {
        load_entries_from_har(&input)?
    };

    if entries.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No entries found to serve".to_string(),
        ));
    }

    let scheme = if options.tls_cert.is_some() {
        "https".to_string()
    } else {
        "http".to_string()
    };

    let addr: SocketAddr = format!("{}:{}", options.bind, options.port)
        .parse()
        .map_err(|err| HarliteError::InvalidArgs(format!("Invalid bind address: {err}")))?;

    let state = Arc::new(ServeState {
        entries,
        match_mode: options.match_mode,
        scheme: scheme.clone(),
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let shutdown_signal = Arc::new(std::sync::Mutex::new(Some(shutdown_tx)));

    let ctrlc_state = shutdown_signal.clone();
    ctrlc::set_handler(move || {
        if let Ok(mut sender) = ctrlc_state.lock() {
            if let Some(tx) = sender.take() {
                let _ = tx.send(());
            }
        }
    })
    .map_err(|err| HarliteError::InvalidArgs(format!("Failed to set ctrl+c handler: {err}")))?;

    println!(
        "Serving {} entries on {}://{}:{}",
        state.entries.len(),
        scheme,
        options.bind,
        options.port
    );

    let rt = tokio::runtime::Runtime::new()
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to start runtime: {err}")))?;

    if options.tls_cert.is_some() {
        let tls_config = load_tls_config(
            options.tls_cert.as_ref().unwrap(),
            options.tls_key.as_ref().unwrap(),
        )?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));
        rt.block_on(run_tls_server(
            addr,
            state,
            tls_acceptor,
            shutdown_rx,
        ))
    } else {
        rt.block_on(run_plain_server(addr, state, shutdown_rx))
    }
}

async fn run_plain_server(
    addr: SocketAddr,
    state: Arc<ServeState>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let make_svc = make_service_fn(move |_| {
        let state = state.clone();
        async move {
            Ok::<_, std::convert::Infallible>(service_fn(move |req| {
                handle_request(req, state.clone())
            }))
        }
    });

    let server = hyper::Server::bind(&addr).serve(make_svc);

    server
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
        .map_err(|err| HarliteError::InvalidArgs(format!("Server error: {err}")))?;

    Ok(())
}

async fn run_tls_server(
    addr: SocketAddr,
    state: Arc<ServeState>,
    tls_acceptor: TlsAcceptor,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|err| HarliteError::InvalidArgs(format!("Bind failed: {err}")))?;

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            incoming = listener.accept() => {
                let (stream, _) = match incoming {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("Accept failed: {err}");
                        continue;
                    }
                };
                let tls_acceptor = tls_acceptor.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    let tls_stream = match tls_acceptor.accept(stream).await {
                        Ok(v) => v,
                        Err(err) => {
                            eprintln!("TLS handshake failed: {err}");
                            return;
                        }
                    };
                    let service = service_fn(move |req| handle_request(req, state.clone()));
                    if let Err(err) = hyper::server::conn::Http::new()
                        .serve_connection(tls_stream, service)
                        .await
                    {
                        eprintln!("Connection error: {err}");
                    }
                });
            }
        }
    }

    Ok(())
}

async fn handle_request(
    req: Request<Body>,
    state: Arc<ServeState>,
) -> std::result::Result<Response<Body>, std::convert::Infallible> {
    let method = req.method().as_str().to_string();
    let host = req
        .headers()
        .get(hyper::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or_else(|| req.uri().path());

    if host.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("Missing Host header"))
            .unwrap());
    }

    let full_url = format!("{}://{}{}", state.scheme, host, path);
    let normalized = normalize_url(&full_url);

    let entry = select_entry(
        &state.entries,
        &method,
        &full_url,
        normalized.as_ref(),
        state.match_mode,
    );

    match entry {
        Some(entry) => {
            println!("HIT {} {} -> {} ({})", method, path, entry.status, entry.url);
            Ok(build_response(entry))
        }
        None => {
            eprintln!("MISS {} {}", method, path);
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap())
        }
    }
}

fn build_response(entry: &ServeEntry) -> Response<Body> {
    let mut builder = Response::builder().status(entry.status);
    {
        let headers = builder.headers_mut().expect("headers mut");
        for (name, value) in &entry.headers {
            if name.eq_ignore_ascii_case("content-length")
                || name.eq_ignore_ascii_case("transfer-encoding")
            {
                continue;
            }
            if let Ok(header_name) = hyper::header::HeaderName::from_bytes(name.as_bytes()) {
                if let Ok(header_value) = hyper::header::HeaderValue::from_str(value) {
                    headers.append(header_name, header_value);
                }
            }
        }
        if !headers.contains_key(hyper::header::CONTENT_TYPE) {
            if let Some(mime) = entry.mime_type.as_ref() {
                if let Ok(value) = hyper::header::HeaderValue::from_str(mime) {
                    headers.insert(hyper::header::CONTENT_TYPE, value);
                }
            }
        }
        headers.insert(
            hyper::header::CONTENT_LENGTH,
            hyper::header::HeaderValue::from_str(&entry.body.len().to_string())
                .unwrap_or_else(|_| hyper::header::HeaderValue::from_static("0")),
        );
    }

    builder.body(Body::from(entry.body.clone())).unwrap()
}

fn select_entry<'a>(
    entries: &'a [ServeEntry],
    method: &str,
    full_url: &str,
    normalized: Option<&NormalizedUrl>,
    match_mode: MatchMode,
) -> Option<&'a ServeEntry> {
    match match_mode {
        MatchMode::Strict => entries
            .iter()
            .filter(|entry| {
                entry.method.eq_ignore_ascii_case(method) && entry.url == full_url
            })
            .max_by(|a, b| compare_recency(a, b)),
        MatchMode::Fuzzy => {
            let Some(req_norm) = normalized else {
                return None;
            };

            let mut best: Option<&ServeEntry> = None;
            let mut best_score = 0usize;

            for entry in entries.iter() {
                if !entry.method.eq_ignore_ascii_case(method) {
                    continue;
                }
                let Some(candidate) = entry.normalized.as_ref() else {
                    continue;
                };
                if !normalized_hosts_match(req_norm, candidate) {
                    continue;
                }

                let score = query_score(req_norm, candidate);
                let choose = match best {
                    None => true,
                    Some(current) => {
                        score > best_score
                            || (score == best_score
                                && compare_recency(entry, current) == Ordering::Greater)
                    }
                };

                if choose {
                    best = Some(entry);
                    best_score = score;
                }
            }

            best
        }
    }
}

fn compare_recency(a: &ServeEntry, b: &ServeEntry) -> Ordering {
    match (&a.started_at, &b.started_at) {
        (Some(left), Some(right)) => left.cmp(right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn normalize_url(url: &str) -> Option<NormalizedUrl> {
    let parsed = Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    let port = parsed.port_or_known_default();
    let path = normalize_path(parsed.path());
    let query = parsed
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<_>>();

    Some(NormalizedUrl {
        host,
        port,
        path,
        query,
    })
}

fn normalize_path(path: &str) -> String {
    if path.len() <= 1 {
        return path.to_string();
    }
    let mut trimmed = path.trim_end_matches('/').to_string();
    if trimmed.is_empty() {
        trimmed.push('/');
    }
    trimmed
}

fn normalized_hosts_match(request: &NormalizedUrl, entry: &NormalizedUrl) -> bool {
    if request.host != entry.host {
        return false;
    }
    if request.path != entry.path {
        return false;
    }
    match (request.port, entry.port) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

fn query_score(request: &NormalizedUrl, entry: &NormalizedUrl) -> usize {
    if request.query.is_empty() || entry.query.is_empty() {
        return 0;
    }

    let entry_set: HashSet<(String, String)> = entry.query.iter().cloned().collect();
    request
        .query
        .iter()
        .filter(|pair| entry_set.contains(pair))
        .count()
}

fn load_entries_from_db(path: &Path, options: &ServeOptions) -> Result<Vec<ServeEntry>> {
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

    let query = EntryQuery::default();
    let rows = load_entries(&conn, &query)?;

    let mut hashes: Vec<String> = Vec::new();
    for row in &rows {
        if let Some(hash) = row.response_body_hash.clone() {
            hashes.push(hash);
        } else if let Some(hash) = row.response_body_hash_raw.clone() {
            hashes.push(hash);
        }
    }
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

    let blob_map: HashMap<String, BlobRow> =
        blobs.into_iter().map(|b| (b.hash.clone(), b)).collect();

    let mut out = Vec::new();
    for row in rows {
        let Some(url) = row.url.clone() else { continue; };
        let method = row.method.unwrap_or_else(|| "GET".to_string());
        let status = row.status.and_then(|s| u16::try_from(s).ok()).unwrap_or(200);
        let headers_map = headers_from_json(row.response_headers.as_deref());
        let mut headers = headers_from_map(&headers_map);
        let has_content_encoding = headers_map
            .keys()
            .any(|name| name.eq_ignore_ascii_case("content-encoding"));

        let body_hash = if has_content_encoding {
            row.response_body_hash_raw.clone().or(row.response_body_hash.clone())
        } else {
            row.response_body_hash.clone().or(row.response_body_hash_raw.clone())
        };
        let body = body_hash
            .as_ref()
            .and_then(|hash| blob_map.get(hash))
            .map(|blob| Bytes::from(blob.content.clone()))
            .unwrap_or_else(Bytes::new);

        if has_content_encoding && row.response_body_hash_raw.is_none() {
            strip_content_encoding(&mut headers);
        }

        out.push(ServeEntry {
            method,
            url: url.clone(),
            status,
            headers,
            body,
            mime_type: row.response_mime_type,
            started_at: row.started_at,
            normalized: normalize_url(&url),
        });
    }

    Ok(out)
}

fn load_entries_from_har(path: &Path) -> Result<Vec<ServeEntry>> {
    let har = parse_har_file(path)?;
    let mut out = Vec::new();

    for entry in har.log.entries.into_iter() {
        let method = entry.request.method.clone();
        let url = entry.request.url.clone();
        let status = u16::try_from(entry.response.status).unwrap_or(200);
        let headers = headers_from_list(&entry.response.headers);
        let body = content_to_bytes(&entry.response.content)?;
        let mime_type = entry.response.content.mime_type.clone();

        out.push(ServeEntry {
            method,
            url: url.clone(),
            status,
            headers,
            body,
            mime_type,
            started_at: Some(entry.started_date_time.clone()),
            normalized: normalize_url(&url),
        });
    }

    Ok(out)
}

fn content_to_bytes(content: &Content) -> Result<Bytes> {
    let Some(text) = content.text.as_ref() else {
        return Ok(Bytes::new());
    };

    match content
        .encoding
        .as_ref()
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("base64") => STANDARD
            .decode(text.as_bytes())
            .map(Bytes::from)
            .map_err(|err| HarliteError::InvalidHar(format!("Invalid base64 body: {err}"))),
        _ => Ok(Bytes::from(text.as_bytes().to_vec())),
    }
}

fn headers_from_json(json: Option<&str>) -> HashMap<String, String> {
    json.and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
        .unwrap_or_default()
}

fn headers_from_map(headers: &HashMap<String, String>) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            if name.trim().is_empty() {
                None
            } else {
                Some((name.to_ascii_lowercase(), value.clone()))
            }
        })
        .collect()
}

fn headers_from_list(headers: &[Header]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|h| {
            if h.name.trim().is_empty() {
                None
            } else {
                Some((h.name.to_ascii_lowercase(), h.value.clone()))
            }
        })
        .collect()
}

fn strip_content_encoding(headers: &mut Vec<(String, String)>) {
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case("content-encoding"));
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

fn load_tls_config(cert_path: &Path, key_path: &Path) -> Result<rustls::ServerConfig> {
    let mut cert_reader = BufReader::new(File::open(cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to read certs: {err}")))?;
    if certs.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No certificates found in --tls-cert".to_string(),
        ));
    }

    let mut key_reader = BufReader::new(File::open(key_path)?);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|err| HarliteError::InvalidArgs(format!("Failed to read key: {err}")))?
        .ok_or_else(|| {
            HarliteError::InvalidArgs("No private key found in --tls-key".to_string())
        })?;

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| HarliteError::InvalidArgs(format!("Invalid TLS key/cert: {err}")))
}

fn is_db_path(path: &Path) -> bool {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
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

#[cfg(test)]
mod tests {
    use super::{
        build_response, headers_from_list, normalize_url, query_score, select_entry, strip_content_encoding,
        MatchMode, NormalizedUrl, ServeEntry,
    };
    use bytes::Bytes;
    use crate::har::Header;

    fn entry(method: &str, url: &str, started_at: Option<&str>) -> ServeEntry {
        ServeEntry {
            method: method.to_string(),
            url: url.to_string(),
            status: 200,
            headers: Vec::new(),
            body: Bytes::new(),
            mime_type: None,
            started_at: started_at.map(|s| s.to_string()),
            normalized: normalize_url(url),
        }
    }

    #[test]
    fn normalize_url_trims_trailing_slash() {
        let url = normalize_url("http://example.com/api/").unwrap();
        assert_eq!(url.path, "/api");
    }

    #[test]
    fn fuzzy_match_prefers_query_hits() {
        let entries = vec![
            entry("GET", "http://example.com/api?foo=1", Some("2024-01-01T00:00:00Z")),
            entry("GET", "http://example.com/api?foo=2", Some("2024-01-02T00:00:00Z")),
        ];

        let req = normalize_url("http://example.com/api?foo=2").unwrap();
        let found = select_entry(
            &entries,
            "GET",
            "http://example.com/api?foo=2",
            Some(&req),
            MatchMode::Fuzzy,
        )
        .unwrap();

        assert_eq!(found.url, "http://example.com/api?foo=2");
    }

    #[test]
    fn query_score_counts_matches() {
        let a = NormalizedUrl {
            host: "example.com".to_string(),
            port: Some(80),
            path: "/".to_string(),
            query: vec![("a".to_string(), "1".to_string())],
        };
        let b = NormalizedUrl {
            host: "example.com".to_string(),
            port: Some(80),
            path: "/".to_string(),
            query: vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ],
        };
        assert_eq!(query_score(&a, &b), 1);
    }

    #[test]
    fn headers_from_list_preserves_duplicates() {
        let headers = vec![
            Header {
                name: "Set-Cookie".to_string(),
                value: "a=1".to_string(),
            },
            Header {
                name: "Set-Cookie".to_string(),
                value: "b=2".to_string(),
            },
        ];
        let pairs = headers_from_list(&headers);
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn build_response_appends_duplicate_headers() {
        let mut entry = entry("GET", "http://example.com/", None);
        entry.headers = vec![
            ("set-cookie".to_string(), "a=1".to_string()),
            ("set-cookie".to_string(), "b=2".to_string()),
        ];
        let response = build_response(&entry);
        let values = response
            .headers()
            .get_all(hyper::header::SET_COOKIE)
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn strip_content_encoding_removes_header() {
        let mut headers = vec![
            ("content-encoding".to_string(), "gzip".to_string()),
            ("content-type".to_string(), "text/plain".to_string()),
        ];
        strip_content_encoding(&mut headers);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "content-type");
    }
}
