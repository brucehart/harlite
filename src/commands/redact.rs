use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use clap::ValueEnum;
use regex::{Regex, RegexBuilder};
use rusqlite::{params, Connection};

use crate::error::{HarliteError, Result};

use super::util::resolve_database;

#[derive(Clone, Copy, Debug, ValueEnum)]
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
    matched_header_names: HashSet<String>,
    matched_cookie_names: HashSet<String>,
}

impl RedactionReport {
    fn total(&self) -> u64 {
        self.request_headers + self.response_headers + self.request_cookies + self.response_cookies
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

        obj.insert("value".to_string(), serde_json::Value::String(token.to_string()));
        changed += 1;
        matched_names.insert(name);
    }

    if changed == 0 {
        return Ok((json.to_string(), 0));
    }
    Ok((serde_json::to_string(&value)?, changed))
}

fn redact_entries(
    conn: &Connection,
    header_matcher: &NameMatcher,
    cookie_matcher: &NameMatcher,
    token: &str,
    write: bool,
) -> Result<RedactionReport> {
    let mut stmt = conn.prepare(
        "SELECT id, request_headers, response_headers, request_cookies, response_cookies FROM entries ORDER BY id",
    )?;

    let mut report = RedactionReport::default();

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;

    let mut update = conn.prepare(
        "UPDATE entries SET request_headers=?1, response_headers=?2, request_cookies=?3, response_cookies=?4 WHERE id=?5",
    )?;

    for row in rows {
        let (id, req_h, resp_h, req_c, resp_c) = row?;
        report.entries_scanned += 1;

        let mut changed = false;
        let mut new_req_h = req_h.clone();
        let mut new_resp_h = resp_h.clone();
        let mut new_req_c = req_c.clone();
        let mut new_resp_c = resp_c.clone();

        if let Some(json) = req_h.as_deref() {
            let (out, n) =
                redact_headers_json(json, header_matcher, token, &mut report.matched_header_names)?;
            if n > 0 {
                new_req_h = Some(out);
                report.request_headers += n;
                changed = true;
            }
        }
        if let Some(json) = resp_h.as_deref() {
            let (out, n) =
                redact_headers_json(json, header_matcher, token, &mut report.matched_header_names)?;
            if n > 0 {
                new_resp_h = Some(out);
                report.response_headers += n;
                changed = true;
            }
        }
        if let Some(json) = req_c.as_deref() {
            let (out, n) =
                redact_cookies_json(json, cookie_matcher, token, &mut report.matched_cookie_names)?;
            if n > 0 {
                new_req_c = Some(out);
                report.request_cookies += n;
                changed = true;
            }
        }
        if let Some(json) = resp_c.as_deref() {
            let (out, n) =
                redact_cookies_json(json, cookie_matcher, token, &mut report.matched_cookie_names)?;
            if n > 0 {
                new_resp_c = Some(out);
                report.response_cookies += n;
                changed = true;
            }
        }

        if changed {
            report.entries_changed += 1;
            if write {
                update.execute(params![new_req_h, new_resp_h, new_req_c, new_resp_c, id])?;
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
    // Only apply defaults when using wildcard mode, since defaults are wildcard patterns
    if !options.no_defaults && matches!(options.match_mode, NameMatchMode::Wildcard) {
        header_patterns.extend(default_header_patterns());
        cookie_patterns.extend(default_cookie_patterns());
    }
    header_patterns.extend(options.headers.iter().cloned());
    cookie_patterns.extend(options.cookies.iter().cloned());

    let header_matcher = NameMatcher::new(options.match_mode, &header_patterns)?;
    let cookie_matcher = NameMatcher::new(options.match_mode, &cookie_patterns)?;

    if header_matcher.is_empty() && cookie_matcher.is_empty() {
        let hint = if !matches!(options.match_mode, NameMatchMode::Wildcard) {
            " (defaults only available in wildcard mode)"
        } else {
            ""
        };
        return Err(HarliteError::InvalidArgs(
            format!("No redaction patterns provided{}", hint),
        ));
    }

    let mut conn = Connection::open(&target_db)?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;

    let report = if options.dry_run {
        redact_entries(&conn, &header_matcher, &cookie_matcher, &options.token, false)?
    } else {
        let tx = conn.transaction()?;
        let report = redact_entries(&tx, &header_matcher, &cookie_matcher, &options.token, true)?;
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
        "Breakdown: request_headers={}, response_headers={}, request_cookies={}, response_cookies={}",
        report.request_headers, report.response_headers, report.request_cookies, report.response_cookies
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

    Ok(())
}
