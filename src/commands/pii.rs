use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use percent_encoding::percent_decode_str;
use regex::{NoExpand, Regex};
use rusqlite::{params, Connection, OptionalExtension};
use url::Url;

use crate::db::store_blob;
use crate::error::{HarliteError, Result};
use crate::har::{Har, QueryParam};

use super::csv::write_csv_field;
use super::query::OutputFormat;
use super::util::{
    canonicalize_path_for_compare, delete_orphaned_blobs, derived_output_path,
    finalize_sensitive_write, is_sqlite_file, prepare_sensitive_write, resolve_database,
    write_json_atomic, ExternalPathPolicy, StagedDatabase,
};

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum PiiKind {
    Email,
    Phone,
    Ssn,
    CreditCard,
}

impl PiiKind {
    fn as_str(self) -> &'static str {
        match self {
            PiiKind::Email => "email",
            PiiKind::Phone => "phone",
            PiiKind::Ssn => "ssn",
            PiiKind::CreditCard => "credit_card",
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum PiiLocation {
    Url,
    RequestBody,
    ResponseBody,
}

impl PiiLocation {
    fn as_str(self) -> &'static str {
        match self {
            PiiLocation::Url => "url",
            PiiLocation::RequestBody => "request_body",
            PiiLocation::ResponseBody => "response_body",
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct PiiFinding {
    entry_id: i64,
    url: String,
    location: String,
    kind: String,
    count: u64,
}

pub struct PiiOptions {
    pub format: OutputFormat,
    pub redact: bool,
    pub output: Option<PathBuf>,
    pub force: bool,
    pub dry_run: bool,
    pub no_defaults: bool,
    pub no_email: bool,
    pub no_phone: bool,
    pub no_ssn: bool,
    pub no_credit_card: bool,
    pub email_regexes: Vec<String>,
    pub phone_regexes: Vec<String>,
    pub ssn_regexes: Vec<String>,
    pub credit_card_regexes: Vec<String>,
    pub token: String,
}

struct PiiMatchers {
    email: Vec<Regex>,
    phone: Vec<Regex>,
    ssn: Vec<Regex>,
    credit_card: Vec<Regex>,
}

impl PiiMatchers {
    fn is_empty(&self) -> bool {
        self.email.is_empty()
            && self.phone.is_empty()
            && self.ssn.is_empty()
            && self.credit_card.is_empty()
    }
}

#[derive(Clone)]
struct PiiRedactedBlob {
    new_hash: String,
    new_size: i64,
    text: String,
}

#[derive(Clone)]
struct PiiTextBlob {
    text: String,
    mime_type: Option<String>,
}

pub fn run_pii(database: Option<PathBuf>, options: &PiiOptions) -> Result<()> {
    run_pii_with_external_paths(database, options, false, None)
}

/// Scan or redact either a HAR file or a harlite database. The existing
/// database-only entry points remain available to library callers.
pub fn run_pii_input(
    input: Option<PathBuf>,
    options: &PiiOptions,
    allow_external_paths: bool,
    external_path_root: Option<&Path>,
) -> Result<()> {
    if input.as_deref().is_some_and(is_har_input) {
        return run_pii_har(input.expect("HAR input"), options);
    }
    run_pii_with_external_paths(input, options, allow_external_paths, external_path_root)
}

fn is_har_input(path: &Path) -> bool {
    if path == Path::new("-") {
        return true;
    }
    if path.exists() {
        return !is_sqlite_file(path);
    }
    !matches!(
        path.extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| extension.to_ascii_lowercase())
            .as_deref(),
        Some("db" | "db3" | "sqlite" | "sqlite3")
    )
}

fn validate_pii_options(options: &PiiOptions) -> Result<()> {
    if !options.redact {
        if options.output.is_some() {
            return Err(HarliteError::InvalidArgs(
                "PII output requires --redact".to_string(),
            ));
        }
        if options.force {
            return Err(HarliteError::InvalidArgs(
                "--force requires --redact".to_string(),
            ));
        }
        if options.dry_run {
            return Err(HarliteError::InvalidArgs(
                "--dry-run requires --redact".to_string(),
            ));
        }
    }
    Ok(())
}

fn run_pii_har(input: PathBuf, options: &PiiOptions) -> Result<()> {
    validate_pii_options(options)?;
    let matchers = build_matchers(options)?;
    if matchers.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No PII patterns provided".to_string(),
        ));
    }

    let mut har = crate::har::parse_har_file(&input)?;
    let findings = scan_har(&mut har, &matchers, options)?;
    if options.redact {
        super::metadata::discard_har_metadata(&mut har, options.dry_run);
    }

    if options.redact && !options.dry_run {
        let output = options
            .output
            .clone()
            .map(Ok)
            .unwrap_or_else(|| derived_output_path(&input, "-pii-redacted", "har"))?;
        if output == Path::new("-") {
            return Err(HarliteError::InvalidArgs(
                "PII redaction output cannot be standard output because findings are written there; use --output <FILE>"
                    .to_string(),
            ));
        }
        if input != Path::new("-")
            && canonicalize_path_for_compare(&input)? == canonicalize_path_for_compare(&output)?
        {
            return Err(HarliteError::InvalidArgs(
                "Output HAR must be different from input HAR".to_string(),
            ));
        }
        write_json_atomic(&output, &har, true, options.force)?;
    }

    write_findings(&findings, options.format)
}

fn scan_har(
    har: &mut Har,
    matchers: &PiiMatchers,
    options: &PiiOptions,
) -> Result<Vec<PiiFinding>> {
    let mut findings = Vec::new();
    for (index, entry) in har.log.entries.iter_mut().enumerate() {
        let entry_id = index as i64 + 1;
        let original_url = entry.request.url.clone();
        append_har_url_findings(
            &mut findings,
            entry_id,
            &original_url,
            entry.request.query_string.as_deref(),
            matchers,
        );
        if options.redact {
            if let Some(redacted) = redact_har_url(&original_url, matchers, &options.token) {
                entry.request.url = redacted;
            }
            redact_query_string(
                entry.request.query_string.as_mut(),
                matchers,
                &options.token,
            );
        }

        if let Some(post_data) = entry.request.post_data.as_mut() {
            let form_urlencoded = is_form_urlencoded_mime(post_data.mime_type.as_deref());
            if let Some(text) = post_data.text.as_mut() {
                append_findings(
                    &mut findings,
                    entry_id,
                    &original_url,
                    PiiLocation::RequestBody,
                    scan_body_text(text, form_urlencoded, matchers),
                );
                if options.redact {
                    if let Some((redacted, _)) =
                        redact_body_text(text, form_urlencoded, matchers, &options.token)
                    {
                        *text = redacted;
                        if entry.request.body_size.is_some_and(|size| size >= 0) {
                            entry.request.body_size = Some(text.len() as i64);
                        }
                    }
                }
            }
            if let Some(params) = post_data.params.as_mut() {
                for param in params {
                    for field in [
                        Some(&mut param.name),
                        param.value.as_mut(),
                        param.file_name.as_mut(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        scan_and_redact_har_field(
                            &mut findings,
                            entry_id,
                            &original_url,
                            field,
                            matchers,
                            options,
                        );
                    }
                }
            }
        }

        let response_form_urlencoded =
            is_form_urlencoded_mime(entry.response.content.mime_type.as_deref());
        let body_encoding = super::privacy_body::encoding(&entry.response.headers);
        if let Some(text) = entry.response.content.text.as_mut() {
            let encoded = entry
                .response
                .content
                .encoding
                .as_deref()
                .is_some_and(|encoding| encoding.eq_ignore_ascii_case("base64"));
            let bytes = if encoded {
                STANDARD.decode(text.as_bytes()).map_err(|e| {
                    crate::error::HarliteError::InvalidHar(format!("Invalid base64 body: {e}"))
                })?
            } else {
                text.as_bytes().to_vec()
            };
            let decoded = super::privacy_body::decode_text(&bytes, body_encoding.as_deref())?;
            if let Some(decoded) = decoded {
                append_findings(
                    &mut findings,
                    entry_id,
                    &original_url,
                    PiiLocation::ResponseBody,
                    scan_body_text(&decoded, response_form_urlencoded, matchers),
                );
                if options.redact {
                    if let Some((redacted, _)) = redact_body_text(
                        &decoded,
                        response_form_urlencoded,
                        matchers,
                        &options.token,
                    ) {
                        entry.response.content.size = redacted.len() as i64;
                        entry.response.body_size = Some(redacted.len() as i64);
                        entry.response.content.compression = None;
                        super::privacy_body::clear_headers(&mut entry.response.headers);
                        *text = if encoded {
                            STANDARD.encode(redacted.as_bytes())
                        } else {
                            redacted
                        };
                    }
                }
            }
        }
    }
    Ok(findings)
}

fn scan_and_redact_har_field(
    findings: &mut Vec<PiiFinding>,
    entry_id: i64,
    original_url: &str,
    field: &mut String,
    matchers: &PiiMatchers,
    options: &PiiOptions,
) {
    append_findings(
        findings,
        entry_id,
        original_url,
        PiiLocation::RequestBody,
        scan_text(field, matchers),
    );
    if options.redact {
        if let Some((redacted, _)) = redact_text(field, matchers, &options.token) {
            *field = redacted;
        }
    }
}

fn append_har_url_findings(
    findings: &mut Vec<PiiFinding>,
    entry_id: i64,
    original_url: &str,
    query_string: Option<&[QueryParam]>,
    matchers: &PiiMatchers,
) {
    let Ok(parsed) = Url::parse(original_url) else {
        append_findings(
            findings,
            entry_id,
            original_url,
            PiiLocation::Url,
            scan_text(&decode_url_component(original_url), matchers),
        );
        if let Some(params) = query_string {
            for param in params {
                append_url_component_findings(
                    findings,
                    entry_id,
                    original_url,
                    &param.name,
                    matchers,
                );
                append_url_component_findings(
                    findings,
                    entry_id,
                    original_url,
                    &param.value,
                    matchers,
                );
            }
        }
        return;
    };

    let parsed_pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    append_findings(
        findings,
        entry_id,
        original_url,
        PiiLocation::Url,
        scan_url_base(&parsed, matchers),
    );

    let mut seen = HashSet::new();
    for (name, value) in parsed_pairs {
        seen.insert((name.clone(), value.clone()));
        append_url_component_findings(findings, entry_id, original_url, &name, matchers);
        append_url_component_findings(findings, entry_id, original_url, &value, matchers);
    }
    if let Some(params) = query_string {
        for param in params {
            if seen.insert((param.name.clone(), param.value.clone())) {
                append_url_component_findings(
                    findings,
                    entry_id,
                    original_url,
                    &param.name,
                    matchers,
                );
                append_url_component_findings(
                    findings,
                    entry_id,
                    original_url,
                    &param.value,
                    matchers,
                );
            }
        }
    }
}

fn append_url_component_findings(
    findings: &mut Vec<PiiFinding>,
    entry_id: i64,
    original_url: &str,
    value: &str,
    matchers: &PiiMatchers,
) {
    append_findings(
        findings,
        entry_id,
        original_url,
        PiiLocation::Url,
        scan_text(&decode_url_component(value), matchers),
    );
}

fn redact_har_url(original_url: &str, matchers: &PiiMatchers, token: &str) -> Option<String> {
    let Ok(mut parsed) = Url::parse(original_url) else {
        let decoded = decode_url_component(original_url);
        return redact_text(&decoded, matchers, token).map(|(value, _)| value);
    };

    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();

    let mut changed = false;
    let username = decode_url_component(parsed.username());
    if let Some((redacted, _)) = redact_text(&username, matchers, token) {
        parsed.set_username(&redacted).ok()?;
        changed = true;
    }
    if let Some(password) = parsed.password().map(decode_url_component) {
        if let Some((redacted, _)) = redact_text(&password, matchers, token) {
            parsed.set_password(Some(&redacted)).ok()?;
            changed = true;
        }
    }
    if let Some(host) = parsed.host_str().map(str::to_string) {
        if let Some((redacted, _)) = redact_text(&host, matchers, token) {
            if parsed.set_host(Some(&redacted)).is_err() {
                parsed.set_host(Some("redacted.invalid")).ok()?;
            }
            changed = true;
        }
    }

    if let Some(segments) = parsed
        .path_segments()
        .map(|segments| segments.map(decode_url_component).collect::<Vec<_>>())
    {
        let mut redacted_segments = Vec::with_capacity(segments.len());
        let mut path_changed = false;
        for segment in segments {
            if let Some((redacted, _)) = redact_text(&segment, matchers, token) {
                redacted_segments.push(redacted);
                path_changed = true;
            } else {
                redacted_segments.push(segment);
            }
        }
        if path_changed {
            let mut output = parsed.path_segments_mut().ok()?;
            output.clear();
            for segment in &redacted_segments {
                output.push(segment);
            }
            drop(output);
            changed = true;
        }
    } else {
        let path = decode_url_component(parsed.path());
        if let Some((redacted, _)) = redact_text(&path, matchers, token) {
            parsed.set_path(&redacted);
            changed = true;
        }
    }

    if let Some(fragment) = parsed.fragment().map(decode_url_component) {
        if let Some((redacted, _)) = redact_text(&fragment, matchers, token) {
            parsed.set_fragment(Some(&redacted));
            changed = true;
        }
    }

    let mut redacted_pairs = Vec::with_capacity(pairs.len());
    let mut query_changed = false;
    for (name, value) in pairs {
        let redacted_name =
            redact_text(&decode_url_component(&name), matchers, token).map(|(value, _)| value);
        let redacted_value =
            redact_text(&decode_url_component(&value), matchers, token).map(|(value, _)| value);
        query_changed |= redacted_name.is_some() || redacted_value.is_some();
        redacted_pairs.push((
            redacted_name.unwrap_or(name),
            redacted_value.unwrap_or(value),
        ));
    }
    changed |= query_changed;
    if !changed {
        return None;
    }

    if query_changed {
        parsed.set_query(None);
        let mut query = parsed.query_pairs_mut();
        for (name, value) in redacted_pairs {
            query.append_pair(&name, &value);
        }
    }

    Some(parsed.into())
}

fn decode_url_component(value: &str) -> String {
    percent_decode_str(value).decode_utf8_lossy().into_owned()
}

fn is_form_urlencoded_mime(mime_type: Option<&str>) -> bool {
    mime_type
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| {
            value
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
}

fn scan_body_text(text: &str, form_urlencoded: bool, matchers: &PiiMatchers) -> PiiCounts {
    if !form_urlencoded {
        return scan_text(text, matchers);
    }

    let mut counts = PiiCounts::default();
    for (name, value) in url::form_urlencoded::parse(text.as_bytes()) {
        counts.add_assign(scan_text(&decode_url_component(&name), matchers));
        counts.add_assign(scan_text(&decode_url_component(&value), matchers));
    }
    counts
}

fn redact_body_text(
    text: &str,
    form_urlencoded: bool,
    matchers: &PiiMatchers,
    token: &str,
) -> Option<(String, u64)> {
    if !form_urlencoded {
        return redact_text(text, matchers, token);
    }

    let mut changed = false;
    let mut total = 0;
    let mut pairs = Vec::new();
    for (name, value) in url::form_urlencoded::parse(text.as_bytes()) {
        let name = name.into_owned();
        let value = value.into_owned();
        let redacted_name = redact_text(&decode_url_component(&name), matchers, token);
        let redacted_value = redact_text(&decode_url_component(&value), matchers, token);
        changed |= redacted_name.is_some() || redacted_value.is_some();
        total += redacted_name.as_ref().map_or(0, |(_, count)| *count);
        total += redacted_value.as_ref().map_or(0, |(_, count)| *count);
        pairs.push((
            redacted_name.map_or(name, |(value, _)| value),
            redacted_value.map_or(value, |(value, _)| value),
        ));
    }
    if !changed {
        return None;
    }

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in pairs {
        serializer.append_pair(&name, &value);
    }
    Some((serializer.finish(), total))
}

fn scan_url_base(parsed: &Url, matchers: &PiiMatchers) -> PiiCounts {
    let mut counts = PiiCounts::default();
    let mut scan_component = |value: &str| {
        counts.add_assign(scan_text(&decode_url_component(value), matchers));
    };

    if !parsed.username().is_empty() {
        scan_component(parsed.username());
    }
    if let Some(password) = parsed.password() {
        scan_component(password);
    }
    if let Some(host) = parsed.host_str() {
        scan_component(host);
    }
    if let Some(segments) = parsed.path_segments() {
        for segment in segments {
            scan_component(segment);
        }
    } else {
        scan_component(parsed.path());
    }
    if let Some(fragment) = parsed.fragment() {
        scan_component(fragment);
    }
    counts
}

fn redact_query_string(
    query_string: Option<&mut Vec<QueryParam>>,
    matchers: &PiiMatchers,
    token: &str,
) {
    let Some(params) = query_string else {
        return;
    };
    for param in params {
        if let Some((redacted, _)) =
            redact_text(&decode_url_component(&param.name), matchers, token)
        {
            param.name = redacted;
        }
        if let Some((redacted, _)) =
            redact_text(&decode_url_component(&param.value), matchers, token)
        {
            param.value = redacted;
        }
    }
}

fn write_findings(findings: &[PiiFinding], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => write_json(findings),
        OutputFormat::Csv => write_csv(findings),
        OutputFormat::Table => write_table(findings),
    }
}

/// Scan/redact PII with an explicit policy for externally stored bodies.
pub fn run_pii_with_external_paths(
    database: Option<PathBuf>,
    options: &PiiOptions,
    allow_external_paths: bool,
    external_path_root: Option<&Path>,
) -> Result<()> {
    validate_pii_options(options)?;

    let input_db = resolve_database(database)?;
    let external_paths =
        ExternalPathPolicy::new(&input_db, allow_external_paths, external_path_root)?;

    let matchers = build_matchers(options)?;
    if matchers.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No PII patterns provided".to_string(),
        ));
    }

    let write = options.redact && !options.dry_run;
    let staged_output = if write {
        if let Some(out) = &options.output {
            let input_cmp = canonicalize_path_for_compare(&input_db)?;
            let out_cmp = canonicalize_path_for_compare(out)?;
            if out_cmp == input_cmp {
                return Err(HarliteError::InvalidArgs(
                    "Output database must be different from input database".to_string(),
                ));
            }
            if out.exists() && !options.force {
                return Err(HarliteError::InvalidArgs(format!(
                    "Output database already exists: {} (use --force to overwrite)",
                    out.display()
                )));
            }
            Some(StagedDatabase::copy_from(&input_db, out, options.force)?)
        } else {
            None
        }
    } else {
        None
    };
    let target_db = staged_output
        .as_ref()
        .map(|staged| staged.path().to_path_buf())
        .unwrap_or_else(|| input_db.clone());

    let conn = if write {
        let conn = Connection::open(&target_db)?;
        prepare_sensitive_write(&conn)?;
        conn
    } else {
        super::query::open_readonly_connection(&target_db)?
    };

    if write {
        conn.execute_batch("BEGIN IMMEDIATE")?;
    }
    let work_conn = &conn;

    let mut stmt = work_conn.prepare(
        "SELECT id, url, host, path, query_string, request_body_hash, request_body_size, response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw, request_headers, response_headers FROM entries ORDER BY id",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, Option<String>>(12)?,
        ))
    })?;

    let mut update = work_conn.prepare(
        "UPDATE entries SET url=?1, host=?2, path=?3, query_string=?4, request_body_hash=?5, request_body_size=?6, response_body_hash=?7, response_body_size=?8, response_body_hash_raw=?9, response_body_size_raw=?10 WHERE id=?11",
    )?;

    let has_fts: bool = work_conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='response_body_fts'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;

    let mut findings: Vec<PiiFinding> = Vec::new();
    let mut text_cache: HashMap<String, Option<PiiTextBlob>> = HashMap::new();
    let mut redacted_cache: HashMap<String, Option<PiiRedactedBlob>> = HashMap::new();
    let mut changed_response_hashes: HashSet<String> = HashSet::new();

    for row in rows {
        let (
            entry_id,
            url,
            host,
            path,
            query_string,
            req_body_hash,
            req_body_size,
            resp_body_hash,
            resp_body_size,
            resp_body_hash_raw,
            resp_body_size_raw,
            request_headers,
            response_headers,
        ) = row?;

        let mut changed = false;
        let mut new_url = url.clone();
        let mut new_host = host;
        let mut new_path = path;
        let mut new_query_string = query_string.clone();
        let mut new_req_body_hash = req_body_hash.clone();
        let mut new_req_body_size = req_body_size;
        let mut new_resp_body_hash = resp_body_hash.clone();
        let mut new_resp_body_size = resp_body_size;
        let mut new_resp_body_hash_raw = resp_body_hash_raw.clone();
        let mut new_resp_body_size_raw = resp_body_size_raw;

        if let Some(url_str) = url.as_deref() {
            append_har_url_findings(&mut findings, entry_id, url_str, None, &matchers);

            if options.redact {
                if let Some(redacted) = redact_har_url(url_str, &matchers, &options.token) {
                    if redacted != url_str {
                        new_url = Some(redacted.clone());
                        if let Ok(parsed) = Url::parse(&redacted) {
                            new_host = parsed.host_str().map(str::to_string);
                            new_path = Some(parsed.path().to_string());
                            new_query_string = parsed.query().map(|q| q.to_string());
                        } else {
                            new_host = None;
                            new_path = None;
                            new_query_string = None;
                        }
                        changed = true;
                    }
                }
            }
        }

        if let Some(hash) = req_body_hash.as_deref() {
            if let Some(text) = load_blob_text(
                work_conn,
                hash,
                &mut text_cache,
                &external_paths,
                super::privacy_body::encoding_json(request_headers.as_deref()).as_deref(),
            )? {
                append_findings(
                    &mut findings,
                    entry_id,
                    url.as_deref().unwrap_or_default(),
                    PiiLocation::RequestBody,
                    scan_body_text(
                        &text.text,
                        is_form_urlencoded_mime(text.mime_type.as_deref()),
                        &matchers,
                    ),
                );

                if options.redact {
                    if let Some(redacted) = redact_blob_cached(
                        work_conn,
                        hash,
                        &matchers,
                        &options.token,
                        write,
                        &mut redacted_cache,
                        &external_paths,
                        super::privacy_body::encoding_json(request_headers.as_deref()).as_deref(),
                    )? {
                        new_req_body_hash = Some(redacted.new_hash);
                        new_req_body_size = Some(redacted.new_size);
                        if write {
                            let headers = super::privacy_body::clear_headers_json(
                                request_headers.as_deref(),
                            )?;
                            work_conn.execute(
                                "UPDATE entries SET request_headers=?1 WHERE id=?2",
                                params![headers, entry_id],
                            )?;
                        }
                        changed = true;
                    }
                }
            }
        }

        if let Some(hash) = resp_body_hash.as_deref() {
            if let Some(text) = load_blob_text(
                work_conn,
                hash,
                &mut text_cache,
                &external_paths,
                super::privacy_body::encoding_json(response_headers.as_deref()).as_deref(),
            )? {
                append_findings(
                    &mut findings,
                    entry_id,
                    url.as_deref().unwrap_or_default(),
                    PiiLocation::ResponseBody,
                    scan_body_text(
                        &text.text,
                        is_form_urlencoded_mime(text.mime_type.as_deref()),
                        &matchers,
                    ),
                );

                if options.redact {
                    if let Some(redacted) = redact_blob_cached(
                        work_conn,
                        hash,
                        &matchers,
                        &options.token,
                        write,
                        &mut redacted_cache,
                        &external_paths,
                        super::privacy_body::encoding_json(response_headers.as_deref()).as_deref(),
                    )? {
                        new_resp_body_hash = Some(redacted.new_hash.clone());
                        new_resp_body_size = Some(redacted.new_size);
                        if write {
                            let headers = super::privacy_body::clear_headers_json(
                                response_headers.as_deref(),
                            )?;
                            work_conn.execute(
                                "UPDATE entries SET response_headers=?1 WHERE id=?2",
                                params![headers, entry_id],
                            )?;
                        }
                        new_resp_body_hash_raw = None;
                        new_resp_body_size_raw = None;
                        changed = true;

                        if write {
                            changed_response_hashes.insert(hash.to_string());
                            if has_fts {
                                let has_old_fts = work_conn
                                    .query_row(
                                        "SELECT 1 FROM response_body_fts WHERE hash = ?1 LIMIT 1",
                                        params![hash],
                                        |row| row.get::<_, i64>(0),
                                    )
                                    .optional()?
                                    .is_some();
                                if has_old_fts {
                                    upsert_response_fts(
                                        work_conn,
                                        &redacted.new_hash,
                                        &redacted.text,
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }

        if changed && write {
            update.execute(params![
                new_url,
                new_host,
                new_path,
                new_query_string,
                new_req_body_hash,
                new_req_body_size,
                new_resp_body_hash,
                new_resp_body_size,
                new_resp_body_hash_raw,
                new_resp_body_size_raw,
                entry_id
            ])?;
        }
    }

    if write && has_fts && !changed_response_hashes.is_empty() {
        let mut check_stmt =
            work_conn.prepare("SELECT COUNT(*) FROM entries WHERE response_body_hash = ?1")?;
        let mut delete_stmt = work_conn.prepare("DELETE FROM response_body_fts WHERE hash = ?1")?;
        for hash in changed_response_hashes {
            let count: i64 = check_stmt.query_row([hash.as_str()], |row| row.get(0))?;
            if count == 0 {
                delete_stmt.execute([hash])?;
            }
        }
    }

    drop(update);
    drop(stmt);
    if options.redact {
        super::metadata::discard_database_metadata(work_conn, write)?;
    }
    if write {
        delete_orphaned_blobs(work_conn)?;
        conn.execute_batch("COMMIT")?;
        finalize_sensitive_write(&conn)?;
    }
    drop(conn);

    if let Some(staged) = staged_output {
        staged.publish()?;
    }

    write_findings(&findings, options.format)
}

fn append_findings(
    out: &mut Vec<PiiFinding>,
    entry_id: i64,
    url: &str,
    location: PiiLocation,
    counts: PiiCounts,
) {
    for (kind, count) in counts.iter() {
        if count == 0 {
            continue;
        }
        out.push(PiiFinding {
            entry_id,
            url: url.to_string(),
            location: location.as_str().to_string(),
            kind: kind.as_str().to_string(),
            count,
        });
    }
}

#[derive(Default)]
struct PiiCounts {
    email: u64,
    phone: u64,
    ssn: u64,
    credit_card: u64,
}

impl PiiCounts {
    fn add_assign(&mut self, other: Self) {
        self.email += other.email;
        self.phone += other.phone;
        self.ssn += other.ssn;
        self.credit_card += other.credit_card;
    }

    fn iter(&self) -> Vec<(PiiKind, u64)> {
        vec![
            (PiiKind::Email, self.email),
            (PiiKind::Phone, self.phone),
            (PiiKind::Ssn, self.ssn),
            (PiiKind::CreditCard, self.credit_card),
        ]
    }
}

fn scan_text(text: &str, matchers: &PiiMatchers) -> PiiCounts {
    PiiCounts {
        email: count_regexes(text, &matchers.email),
        phone: count_regexes(text, &matchers.phone),
        ssn: count_regexes(text, &matchers.ssn),
        credit_card: count_credit_cards(text, &matchers.credit_card),
    }
}

fn count_regexes(text: &str, regexes: &[Regex]) -> u64 {
    regexes
        .iter()
        .map(|re| re.find_iter(text).count() as u64)
        .sum()
}

fn count_credit_cards(text: &str, regexes: &[Regex]) -> u64 {
    let mut total = 0u64;
    for re in regexes {
        for m in re.find_iter(text) {
            if is_luhn_valid(m.as_str()) {
                total += 1;
            }
        }
    }
    total
}

fn redact_text(text: &str, matchers: &PiiMatchers, token: &str) -> Option<(String, u64)> {
    if matchers.is_empty() {
        return None;
    }

    let mut out = text.to_string();
    let mut total = 0u64;

    let (updated, count) = redact_with_regexes(&out, &matchers.email, token);
    out = updated;
    total += count;

    let (updated, count) = redact_with_regexes(&out, &matchers.phone, token);
    out = updated;
    total += count;

    let (updated, count) = redact_with_regexes(&out, &matchers.ssn, token);
    out = updated;
    total += count;

    let (updated, count) = redact_credit_cards(&out, &matchers.credit_card, token);
    out = updated;
    total += count;

    if total == 0 || out == text {
        return None;
    }
    Some((out, total))
}

fn redact_with_regexes(text: &str, regexes: &[Regex], token: &str) -> (String, u64) {
    let mut out = text.to_string();
    let mut total = 0u64;
    for re in regexes {
        let count = re.find_iter(&out).count() as u64;
        if count == 0 {
            continue;
        }
        total += count;
        out = re.replace_all(&out, NoExpand(token)).into_owned();
    }
    (out, total)
}

fn redact_credit_cards(text: &str, regexes: &[Regex], token: &str) -> (String, u64) {
    let mut out = text.to_string();
    let mut total = 0u64;
    for re in regexes {
        let mut count = 0u64;
        let replaced = re.replace_all(&out, |caps: &regex::Captures| {
            let m = caps.get(0).map(|c| c.as_str()).unwrap_or_default();
            if is_luhn_valid(m) {
                count += 1;
                token.to_string()
            } else {
                m.to_string()
            }
        });
        if count > 0 {
            out = replaced.into_owned();
            total += count;
        }
    }
    (out, total)
}

fn load_blob_text(
    conn: &Connection,
    hash: &str,
    cache: &mut HashMap<String, Option<PiiTextBlob>>,
    external_paths: &ExternalPathPolicy,
    encoding: Option<&str>,
) -> Result<Option<PiiTextBlob>> {
    let cache_key = format!("{hash}\0{}", encoding.unwrap_or_default());
    if let Some(existing) = cache.get(&cache_key) {
        return Ok(existing.clone());
    }

    let Some((content, mime_type)) = load_blob_for_pii(conn, hash, external_paths)? else {
        cache.insert(cache_key.clone(), None);
        return Ok(None);
    };

    let Some(text) = super::privacy_body::decode_text(&content, encoding)? else {
        cache.insert(cache_key, None);
        return Ok(None);
    };

    let blob = PiiTextBlob { text, mime_type };
    cache.insert(cache_key.clone(), Some(blob.clone()));
    Ok(Some(blob))
}

fn load_blob_for_pii(
    conn: &Connection,
    hash: &str,
    external_paths: &ExternalPathPolicy,
) -> Result<Option<(Vec<u8>, Option<String>)>> {
    let row = conn
        .query_row(
            "SELECT content, size, mime_type, external_path FROM blobs WHERE hash = ?1",
            params![hash],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?;

    let Some((mut content, size, mime_type, external_path)) = row else {
        return Ok(None);
    };

    if content.is_empty() && size > 0 {
        if let Some(path) = external_path {
            if let Some(path) = external_paths.resolve_file(&path) {
                let bytes = std::fs::read(path)?;
                content = bytes;
            }
        }
    }

    if content.is_empty() {
        if size > 0 {
            eprintln!("Warning: body {hash} is unavailable and was not inspected.");
        }
        return Ok(None);
    }

    Ok(Some((content, mime_type)))
}

#[allow(clippy::too_many_arguments)]
fn redact_blob_cached(
    conn: &Connection,
    hash: &str,
    matchers: &PiiMatchers,
    token: &str,
    write: bool,
    cache: &mut HashMap<String, Option<PiiRedactedBlob>>,
    external_paths: &ExternalPathPolicy,
    encoding: Option<&str>,
) -> Result<Option<PiiRedactedBlob>> {
    let cache_key = format!("{hash}\0{}", encoding.unwrap_or_default());
    if let Some(existing) = cache.get(&cache_key) {
        return Ok(existing.clone());
    }

    let Some((content, mime_type)) = load_blob_for_pii(conn, hash, external_paths)? else {
        cache.insert(cache_key.clone(), None);
        return Ok(None);
    };

    let Some(text) = super::privacy_body::decode_text(&content, encoding)? else {
        cache.insert(cache_key, None);
        return Ok(None);
    };

    let Some((redacted_text, _)) = redact_body_text(
        &text,
        is_form_urlencoded_mime(mime_type.as_deref()),
        matchers,
        token,
    ) else {
        cache.insert(cache_key.clone(), None);
        return Ok(None);
    };

    let bytes = redacted_text.as_bytes();
    let new_hash = if write {
        let (hash, _) = store_blob(conn, bytes, mime_type.as_deref(), None, true)?;
        hash
    } else {
        hash.to_string()
    };

    let redacted = PiiRedactedBlob {
        new_hash,
        new_size: bytes.len() as i64,
        text: redacted_text,
    };

    cache.insert(cache_key.clone(), Some(redacted.clone()));
    Ok(Some(redacted))
}

fn upsert_response_fts(conn: &Connection, hash: &str, text: &str) -> Result<()> {
    if text.is_empty() {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM response_body_fts WHERE hash = ?1",
        params![hash],
    )?;
    conn.execute(
        "INSERT INTO response_body_fts (hash, body) VALUES (?1, ?2)",
        params![hash, text],
    )?;
    Ok(())
}

fn build_matchers(options: &PiiOptions) -> Result<PiiMatchers> {
    let mut email_patterns: Vec<String> = Vec::new();
    let mut phone_patterns: Vec<String> = Vec::new();
    let mut ssn_patterns: Vec<String> = Vec::new();
    let mut credit_card_patterns: Vec<String> = Vec::new();

    if !options.no_defaults && !options.no_email {
        email_patterns.extend(default_email_patterns());
    }
    if !options.no_defaults && !options.no_phone {
        phone_patterns.extend(default_phone_patterns());
    }
    if !options.no_defaults && !options.no_ssn {
        ssn_patterns.extend(default_ssn_patterns());
    }
    if !options.no_defaults && !options.no_credit_card {
        credit_card_patterns.extend(default_credit_card_patterns());
    }

    email_patterns.extend(options.email_regexes.iter().cloned());
    phone_patterns.extend(options.phone_regexes.iter().cloned());
    ssn_patterns.extend(options.ssn_regexes.iter().cloned());
    credit_card_patterns.extend(options.credit_card_regexes.iter().cloned());

    Ok(PiiMatchers {
        email: compile_regexes(&email_patterns)?,
        phone: compile_regexes(&phone_patterns)?,
        ssn: compile_regexes(&ssn_patterns)?,
        credit_card: compile_regexes(&credit_card_patterns)?,
    })
}

fn compile_regexes(patterns: &[String]) -> Result<Vec<Regex>> {
    let mut out = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        out.push(Regex::new(pattern)?);
    }
    Ok(out)
}

fn default_email_patterns() -> Vec<String> {
    vec![r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b".to_string()]
}

fn default_phone_patterns() -> Vec<String> {
    vec![r"\b(?:\+?1[\s.-]?)?(?:\(?[2-9]\d{2}\)?[\s.-]?)\d{3}[\s.-]?\d{4}\b".to_string()]
}

fn default_ssn_patterns() -> Vec<String> {
    vec![r"\b\d{3}-\d{2}-\d{4}\b".to_string()]
}

fn default_credit_card_patterns() -> Vec<String> {
    vec![r"\b(?:\d[ -]*?){13,19}\b".to_string()]
}

fn is_luhn_valid(value: &str) -> bool {
    let digits: Vec<u32> = value
        .chars()
        .filter(|c| c.is_ascii_digit())
        .filter_map(|c| c.to_digit(10))
        .collect();
    if digits.len() < 13 || digits.len() > 19 {
        return false;
    }

    let mut sum = 0u32;
    let mut double = false;
    for digit in digits.into_iter().rev() {
        let mut val = digit;
        if double {
            val *= 2;
            if val > 9 {
                val -= 9;
            }
        }
        sum += val;
        double = !double;
    }
    sum % 10 == 0
}

fn write_json(rows: &[PiiFinding]) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, rows)?;
    handle.write_all(b"\n")?;
    Ok(())
}

fn write_csv(rows: &[PiiFinding]) -> Result<()> {
    let columns = pii_columns();
    let mut out = io::stdout().lock();
    write_csv_row(&mut out, columns.iter().copied())?;
    for row in rows {
        let fields = pii_row_values(row);
        write_csv_row(&mut out, fields.iter().map(|s| s.as_str()))?;
    }
    Ok(())
}

fn write_table(rows: &[PiiFinding]) -> Result<()> {
    let columns = pii_columns();
    let mut data: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for row in rows {
        data.push(pii_row_values(row));
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &data {
        for (i, value) in row.iter().enumerate() {
            widths[i] = widths[i].max(value.chars().count());
        }
    }

    for width in &mut widths {
        *width = (*width).clamp(8, 80);
    }

    let mut out = io::stdout().lock();
    write_table_row(&mut out, columns.iter().copied(), &widths)?;
    write_table_sep(&mut out, &widths)?;
    for row in data {
        write_table_row(&mut out, row.iter().map(|s| s.as_str()), &widths)?;
    }
    Ok(())
}

fn pii_columns() -> Vec<&'static str> {
    vec!["entry_id", "url", "location", "kind", "count"]
}

fn pii_row_values(row: &PiiFinding) -> Vec<String> {
    vec![
        row.entry_id.to_string(),
        row.url.clone(),
        row.location.clone(),
        row.kind.clone(),
        row.count.to_string(),
    ]
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

fn write_table_row<'a, I>(out: &mut impl Write, fields: I, widths: &[usize]) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    for (i, field) in fields.into_iter().enumerate() {
        let width = widths.get(i).copied().unwrap_or(8);
        let mut value = field.to_string();
        if value.chars().count() > width {
            let take = width.saturating_sub(3);
            value = value.chars().take(take).collect::<String>() + "...";
        }
        write!(out, "{:width$} ", value, width = width)?;
    }
    writeln!(out)?;
    Ok(())
}

fn write_table_sep(out: &mut impl Write, widths: &[usize]) -> Result<()> {
    for width in widths {
        for _ in 0..*width {
            out.write_all(b"-")?;
        }
        out.write_all(b" ")?;
    }
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn luhn_validation() {
        assert!(is_luhn_valid("4111 1111 1111 1111"));
        assert!(is_luhn_valid("4012888888881881"));
        assert!(!is_luhn_valid("4111 1111 1111 1112"));
        assert!(!is_luhn_valid("1234 5678 9012 3456"));
    }

    #[test]
    fn scan_text_counts_defaults() {
        let options = PiiOptions {
            format: OutputFormat::Table,
            redact: false,
            output: None,
            force: false,
            dry_run: false,
            no_defaults: false,
            no_email: false,
            no_phone: false,
            no_ssn: false,
            no_credit_card: false,
            email_regexes: Vec::new(),
            phone_regexes: Vec::new(),
            ssn_regexes: Vec::new(),
            credit_card_regexes: Vec::new(),
            token: "REDACTED".to_string(),
        };
        let matchers = build_matchers(&options).unwrap();
        let text = "email me at test@example.com or 415-555-1212. ssn 123-45-6789";
        let counts = scan_text(text, &matchers);
        assert_eq!(counts.email, 1);
        assert_eq!(counts.phone, 1);
        assert_eq!(counts.ssn, 1);
    }
}
