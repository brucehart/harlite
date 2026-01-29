use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use clap::ValueEnum;
use regex::{NoExpand, Regex, RegexBuilder};
use rusqlite::{params, Connection, OptionalExtension};
use url::Url;

use crate::db::store_blob;
use crate::error::{HarliteError, Result};

use super::util::resolve_database;

#[derive(Clone, Copy, Debug, ValueEnum, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NameMatchMode {
    Exact,
    Wildcard,
    Regex,
}

pub struct RedactOptions {
    pub output: Option<PathBuf>,
    pub force: bool,
    pub dry_run: bool,
    pub no_defaults: bool,
    pub headers: Vec<String>,
    pub cookies: Vec<String>,
    pub query_params: Vec<String>,
    pub body_regexes: Vec<String>,
    pub match_mode: NameMatchMode,
    pub token: String,
}

#[derive(Default)]
struct RedactionReport {
    entries_scanned: u64,
    entries_changed: u64,
    request_headers: u64,
    response_headers: u64,
    request_cookies: u64,
    response_cookies: u64,
    query_params: u64,
    request_bodies: u64,
    response_bodies: u64,
    body_matches: u64,
    matched_header_names: HashSet<String>,
    matched_cookie_names: HashSet<String>,
    matched_query_param_names: HashSet<String>,
}

impl RedactionReport {
    fn total(&self) -> u64 {
        self.request_headers
            + self.response_headers
            + self.request_cookies
            + self.response_cookies
            + self.query_params
            + self.body_matches
    }
}

enum NameMatcher {
    Exact(Vec<String>),
    Wildcard(Vec<String>),
    Regex(Vec<Regex>),
}

impl NameMatcher {
    fn new(mode: NameMatchMode, patterns: &[String]) -> Result<Self> {
        match mode {
            NameMatchMode::Exact => Ok(Self::Exact(
                patterns.iter().map(|p| p.trim().to_lowercase()).collect(),
            )),
            NameMatchMode::Wildcard => Ok(Self::Wildcard(
                patterns.iter().map(|p| p.trim().to_lowercase()).collect(),
            )),
            NameMatchMode::Regex => {
                let mut out: Vec<Regex> = Vec::new();
                for p in patterns {
                    let re = RegexBuilder::new(p).case_insensitive(true).build()?;
                    out.push(re);
                }
                Ok(Self::Regex(out))
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Exact(p) => p.is_empty(),
            Self::Wildcard(p) => p.is_empty(),
            Self::Regex(p) => p.is_empty(),
        }
    }

    fn matches(&self, name: &str) -> bool {
        let name_lc = name.to_lowercase();
        match self {
            Self::Exact(patterns) => patterns.iter().any(|p| p == &name_lc),
            Self::Wildcard(patterns) => patterns.iter().any(|p| wildcard_match(p, &name_lc)),
            Self::Regex(res) => res.iter().any(|re| re.is_match(name)),
        }
    }
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let (p, t) = (pattern.as_bytes(), text.as_bytes());
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut star_match_ti: usize = 0;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
            continue;
        }

        if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            pi += 1;
            star_match_ti = ti;
            continue;
        }

        if let Some(star_pi) = star {
            pi = star_pi + 1;
            star_match_ti += 1;
            ti = star_match_ti;
            continue;
        }

        return false;
    }

    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }

    pi == p.len()
}

fn default_header_patterns() -> Vec<String> {
    vec![
        "authorization".to_string(),
        "proxy-authorization".to_string(),
        "cookie".to_string(),
        "set-cookie".to_string(),
        "x-api-key".to_string(),
        "api-key".to_string(),
        "x-auth-token".to_string(),
        "x-csrf-token".to_string(),
        "csrf-token".to_string(),
        "x-xsrf-token".to_string(),
    ]
}

fn default_cookie_patterns() -> Vec<String> {
    vec!["*".to_string()]
}

fn redact_headers_json(
    json: &str,
    matcher: &NameMatcher,
    token: &str,
    matched_names: &mut HashSet<String>,
) -> Result<(String, u64)> {
    let mut value: serde_json::Value = serde_json::from_str(json)?;
    let Some(obj) = value.as_object_mut() else {
        return Ok((json.to_string(), 0));
    };

    let mut changed = 0u64;
    for (name, v) in obj.iter_mut() {
        if !matcher.matches(name) {
            continue;
        }

        let cur = v.as_str().unwrap_or_default();
        if cur == token {
            continue;
        }

        *v = serde_json::Value::String(token.to_string());
        changed += 1;
        matched_names.insert(name.to_string());
    }

    if changed == 0 {
        return Ok((json.to_string(), 0));
    }
    Ok((serde_json::to_string(&value)?, changed))
}

fn redact_cookies_json(
    json: &str,
    matcher: &NameMatcher,
    token: &str,
    matched_names: &mut HashSet<String>,
) -> Result<(String, u64)> {
    let mut value: serde_json::Value = serde_json::from_str(json)?;
    let Some(arr) = value.as_array_mut() else {
        return Ok((json.to_string(), 0));
    };

    let mut changed = 0u64;
    for item in arr.iter_mut() {
        let Some(obj) = item.as_object_mut() else {
            continue;
        };
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if name.is_empty() || !matcher.matches(&name) {
            continue;
        }

        let is_already_token = obj
            .get("value")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == token);
        if is_already_token {
            continue;
        }

        obj.insert(
            "value".to_string(),
            serde_json::Value::String(token.to_string()),
        );
        changed += 1;
        matched_names.insert(name);
    }

    if changed == 0 {
        return Ok((json.to_string(), 0));
    }
    Ok((serde_json::to_string(&value)?, changed))
}

fn redact_url_params(
    url: &str,
    matcher: &NameMatcher,
    token: &str,
    matched_names: &mut HashSet<String>,
) -> Option<(String, Option<String>, u64)> {
    let mut parsed = Url::parse(url).ok()?;
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    if pairs.is_empty() {
        return None;
    }

    let mut changed = 0u64;
    {
        let mut qp = parsed.query_pairs_mut();
        qp.clear();
        for (name, mut value) in pairs {
            if matcher.matches(&name) {
                matched_names.insert(name.clone());
                if value != token {
                    value = token.to_string();
                    changed += 1;
                }
            }
            qp.append_pair(&name, &value);
        }
    }

    if changed == 0 {
        return None;
    }

    let new_query = parsed.query().map(|q| q.to_string());
    let new_url: String = parsed.into();
    Some((new_url, new_query, changed))
}

#[derive(Clone)]
struct RedactedBlob {
    new_hash: String,
    new_size: i64,
    matches: u64,
    text: String,
}

fn redact_body_text(text: &str, regexes: &[Regex], token: &str) -> Option<(String, u64)> {
    if regexes.is_empty() {
        return None;
    }

    let mut out = text.to_string();
    let mut total_matches = 0u64;
    for re in regexes {
        let matches = re.find_iter(&out).count();
        if matches == 0 {
            continue;
        }
        total_matches += matches as u64;
        out = re.replace_all(&out, NoExpand(token)).into_owned();
    }

    if total_matches == 0 || out == text {
        return None;
    }

    Some((out, total_matches))
}

fn load_blob_for_redaction(
    conn: &Connection,
    hash: &str,
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
            if let Ok(bytes) = std::fs::read(path) {
                content = bytes;
            }
        }
    }

    if content.is_empty() {
        return Ok(None);
    }

    Ok(Some((content, mime_type)))
}

fn redact_blob_cached(
    conn: &Connection,
    hash: &str,
    regexes: &[Regex],
    token: &str,
    write: bool,
    cache: &mut HashMap<String, Option<RedactedBlob>>,
) -> Result<(Option<RedactedBlob>, bool)> {
    if let Some(existing) = cache.get(hash) {
        return Ok((existing.clone(), false));
    }

    let Some((content, mime_type)) = load_blob_for_redaction(conn, hash)? else {
        cache.insert(hash.to_string(), None);
        return Ok((None, true));
    };

    let text = match std::str::from_utf8(&content) {
        Ok(s) => s,
        Err(_) => {
            cache.insert(hash.to_string(), None);
            return Ok((None, true));
        }
    };

    let Some((redacted_text, matches)) = redact_body_text(text, regexes, token) else {
        cache.insert(hash.to_string(), None);
        return Ok((None, true));
    };

    let bytes = redacted_text.as_bytes();
    let new_hash = if write {
        let (hash, _) = store_blob(conn, bytes, mime_type.as_deref(), None, true)?;
        hash
    } else {
        hash.to_string()
    };

    let redacted = RedactedBlob {
        new_hash,
        new_size: bytes.len() as i64,
        matches,
        text: redacted_text,
    };

    cache.insert(hash.to_string(), Some(redacted.clone()));
    Ok((Some(redacted), true))
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

fn redact_entries(
    conn: &Connection,
    header_matcher: &NameMatcher,
    cookie_matcher: &NameMatcher,
    query_matcher: &NameMatcher,
    body_regexes: &[Regex],
    token: &str,
    write: bool,
) -> Result<RedactionReport> {
    let mut stmt = conn.prepare(
        "SELECT id, url, query_string, request_headers, response_headers, request_cookies, response_cookies, request_body_hash, request_body_size, response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw FROM entries ORDER BY id",
    )?;

    let mut report = RedactionReport::default();

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, Option<i64>>(12)?,
        ))
    })?;

    let mut update = conn.prepare(
        "UPDATE entries SET url=?1, query_string=?2, request_headers=?3, response_headers=?4, request_cookies=?5, response_cookies=?6, request_body_hash=?7, request_body_size=?8, response_body_hash=?9, response_body_size=?10, response_body_hash_raw=?11, response_body_size_raw=?12 WHERE id=?13",
    )?;

    let mut blob_cache: HashMap<String, Option<RedactedBlob>> = HashMap::new();
    let mut changed_response_hashes: HashSet<String> = HashSet::new();
    let has_fts: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='response_body_fts'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;

    for row in rows {
        let (
            id,
            url,
            query_string,
            req_h,
            resp_h,
            req_c,
            resp_c,
            req_body_hash,
            req_body_size,
            resp_body_hash,
            resp_body_size,
            resp_body_hash_raw,
            resp_body_size_raw,
        ) = row?;
        report.entries_scanned += 1;

        let mut changed = false;
        let mut new_url = url.clone();
        let mut new_query_string = query_string.clone();
        let mut new_req_h = req_h.clone();
        let mut new_resp_h = resp_h.clone();
        let mut new_req_c = req_c.clone();
        let mut new_resp_c = resp_c.clone();
        let mut new_req_body_hash = req_body_hash.clone();
        let mut new_req_body_size = req_body_size;
        let mut new_resp_body_hash = resp_body_hash.clone();
        let mut new_resp_body_size = resp_body_size;
        let mut new_resp_body_hash_raw = resp_body_hash_raw.clone();
        let mut new_resp_body_size_raw = resp_body_size_raw;

        if let Some(json) = req_h.as_deref() {
            let (out, n) = redact_headers_json(
                json,
                header_matcher,
                token,
                &mut report.matched_header_names,
            )?;
            if n > 0 {
                new_req_h = Some(out);
                report.request_headers += n;
                changed = true;
            }
        }
        if let Some(json) = resp_h.as_deref() {
            let (out, n) = redact_headers_json(
                json,
                header_matcher,
                token,
                &mut report.matched_header_names,
            )?;
            if n > 0 {
                new_resp_h = Some(out);
                report.response_headers += n;
                changed = true;
            }
        }
        if let Some(json) = req_c.as_deref() {
            let (out, n) = redact_cookies_json(
                json,
                cookie_matcher,
                token,
                &mut report.matched_cookie_names,
            )?;
            if n > 0 {
                new_req_c = Some(out);
                report.request_cookies += n;
                changed = true;
            }
        }
        if let Some(json) = resp_c.as_deref() {
            let (out, n) = redact_cookies_json(
                json,
                cookie_matcher,
                token,
                &mut report.matched_cookie_names,
            )?;
            if n > 0 {
                new_resp_c = Some(out);
                report.response_cookies += n;
                changed = true;
            }
        }
        if let Some(url_str) = url.as_deref() {
            if !query_matcher.is_empty() {
                if let Some((out, new_query, n)) = redact_url_params(
                    url_str,
                    query_matcher,
                    token,
                    &mut report.matched_query_param_names,
                ) {
                    new_url = Some(out);
                    new_query_string = new_query;
                    report.query_params += n;
                    changed = true;
                }
            }
        }
        if !body_regexes.is_empty() {
            if let Some(hash) = req_body_hash.as_deref() {
                let (redacted, counted) =
                    redact_blob_cached(conn, hash, body_regexes, token, write, &mut blob_cache)?;
                if let Some(redacted) = redacted {
                    if counted {
                        report.body_matches += redacted.matches;
                    }
                    report.request_bodies += 1;
                    changed = true;
                    if write {
                        new_req_body_hash = Some(redacted.new_hash);
                        new_req_body_size = Some(redacted.new_size);
                    }
                }
            }
            if let Some(hash) = resp_body_hash.as_deref() {
                let (redacted, counted) =
                    redact_blob_cached(conn, hash, body_regexes, token, write, &mut blob_cache)?;
                if let Some(redacted) = redacted {
                    if counted {
                        report.body_matches += redacted.matches;
                    }
                    report.response_bodies += 1;
                    changed = true;
                    if write {
                        new_resp_body_hash = Some(redacted.new_hash.clone());
                        new_resp_body_size = Some(redacted.new_size);
                        new_resp_body_hash_raw = None;
                        new_resp_body_size_raw = None;
                        changed_response_hashes.insert(hash.to_string());
                        if has_fts {
                            let has_old_fts = conn
                                .query_row(
                                    "SELECT 1 FROM response_body_fts WHERE hash = ?1 LIMIT 1",
                                    params![hash],
                                    |row| row.get::<_, i64>(0),
                                )
                                .optional()?
                                .is_some();
                            if has_old_fts {
                                upsert_response_fts(conn, &redacted.new_hash, &redacted.text)?;
                            }
                        }
                    }
                }
            }
        }

        if changed {
            report.entries_changed += 1;
            if write {
                update.execute(params![
                    new_url,
                    new_query_string,
                    new_req_h,
                    new_resp_h,
                    new_req_c,
                    new_resp_c,
                    new_req_body_hash,
                    new_req_body_size,
                    new_resp_body_hash,
                    new_resp_body_size,
                    new_resp_body_hash_raw,
                    new_resp_body_size_raw,
                    id
                ])?;
            }
        }
    }

    if write && has_fts && !changed_response_hashes.is_empty() {
        let mut check_stmt =
            conn.prepare("SELECT COUNT(*) FROM entries WHERE response_body_hash = ?1")?;
        let mut delete_stmt = conn.prepare("DELETE FROM response_body_fts WHERE hash = ?1")?;
        for hash in changed_response_hashes {
            let count: i64 = check_stmt.query_row([hash.as_str()], |row| row.get(0))?;
            if count == 0 {
                delete_stmt.execute([hash])?;
            }
        }
    }

    Ok(report)
}

pub fn run_redact(database: Option<PathBuf>, options: &RedactOptions) -> Result<()> {
    let input_db = resolve_database(database)?;

    let target_db = if options.dry_run {
        input_db.clone()
    } else if let Some(out) = &options.output {
        if out == &input_db {
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
        if out.exists() {
            fs::remove_file(out)?;
        }
        fs::copy(&input_db, out)?;
        out.clone()
    } else {
        input_db.clone()
    };

    let mut header_patterns: Vec<String> = Vec::new();
    let mut cookie_patterns: Vec<String> = Vec::new();
    let mut query_patterns: Vec<String> = Vec::new();
    // Only apply defaults when using wildcard mode, since defaults are wildcard patterns
    if !options.no_defaults && matches!(options.match_mode, NameMatchMode::Wildcard) {
        header_patterns.extend(default_header_patterns());
        cookie_patterns.extend(default_cookie_patterns());
    }
    header_patterns.extend(options.headers.iter().cloned());
    cookie_patterns.extend(options.cookies.iter().cloned());
    query_patterns.extend(options.query_params.iter().cloned());

    let header_matcher = NameMatcher::new(options.match_mode, &header_patterns)?;
    let cookie_matcher = NameMatcher::new(options.match_mode, &cookie_patterns)?;
    let query_matcher = NameMatcher::new(options.match_mode, &query_patterns)?;

    let body_regexes: Vec<Regex> = options
        .body_regexes
        .iter()
        .map(|p| Regex::new(p))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if header_matcher.is_empty()
        && cookie_matcher.is_empty()
        && query_matcher.is_empty()
        && body_regexes.is_empty()
    {
        let hint = if !matches!(options.match_mode, NameMatchMode::Wildcard) {
            " (defaults only available in wildcard mode)"
        } else {
            ""
        };
        return Err(HarliteError::InvalidArgs(format!(
            "No redaction patterns provided{}",
            hint
        )));
    }

    let mut conn = Connection::open(&target_db)?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;

    let report = if options.dry_run {
        redact_entries(
            &conn,
            &header_matcher,
            &cookie_matcher,
            &query_matcher,
            &body_regexes,
            &options.token,
            false,
        )?
    } else {
        let tx = conn.transaction()?;
        let report = redact_entries(
            &tx,
            &header_matcher,
            &cookie_matcher,
            &query_matcher,
            &body_regexes,
            &options.token,
            true,
        )?;
        tx.commit()?;
        report
    };

    if options.dry_run {
        println!(
            "Dry run: would redact {} values across {} entries in {}{}",
            report.total(),
            report.entries_changed,
            input_db.display(),
            options
                .output
                .as_ref()
                .map(|o| format!(" (output not written: {})", o.display()))
                .unwrap_or_default()
        );
    } else {
        println!(
            "Redacted {} values across {} entries in {}",
            report.total(),
            report.entries_changed,
            target_db.display()
        );
    }

    println!(
        "Breakdown: request_headers={}, response_headers={}, request_cookies={}, response_cookies={}, query_params={}, request_bodies={}, response_bodies={}, body_matches={}",
        report.request_headers,
        report.response_headers,
        report.request_cookies,
        report.response_cookies,
        report.query_params,
        report.request_bodies,
        report.response_bodies,
        report.body_matches
    );

    if !report.matched_header_names.is_empty() {
        let mut names: Vec<String> = report.matched_header_names.into_iter().collect();
        names.sort();
        println!("Matched headers: {}", names.join(", "));
    }
    if !report.matched_cookie_names.is_empty() {
        let mut names: Vec<String> = report.matched_cookie_names.into_iter().collect();
        names.sort();
        println!("Matched cookies: {}", names.join(", "));
    }
    if !report.matched_query_param_names.is_empty() {
        let mut names: Vec<String> = report.matched_query_param_names.into_iter().collect();
        names.sort();
        println!("Matched query params: {}", names.join(", "));
    }

    Ok(())
}
