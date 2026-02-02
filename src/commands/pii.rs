use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::PathBuf;

use regex::{NoExpand, Regex};
use rusqlite::{params, Connection, OptionalExtension};
use url::Url;

use crate::db::store_blob;
use crate::error::{HarliteError, Result};

use super::query::OutputFormat;
use super::util::resolve_database;

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

pub fn run_pii(database: Option<PathBuf>, options: &PiiOptions) -> Result<()> {
    if !options.redact {
        if options.output.is_some() {
            return Err(HarliteError::InvalidArgs(
                "PII output database requires --redact".to_string(),
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

    let input_db = resolve_database(database)?;
    let write = options.redact && !options.dry_run;
    let target_db = if write {
        if let Some(out) = &options.output {
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
                std::fs::remove_file(out)?;
            }
            std::fs::copy(&input_db, out)?;
            out.clone()
        } else {
            input_db.clone()
        }
    } else {
        input_db.clone()
    };

    let matchers = build_matchers(options)?;
    if matchers.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "No PII patterns provided".to_string(),
        ));
    }

    let conn = if write {
        let conn = Connection::open(&target_db)?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        conn
    } else {
        super::query::open_readonly_connection(&target_db)?
    };

    let mut stmt = conn.prepare(
        "SELECT id, url, query_string, request_body_hash, request_body_size, response_body_hash, response_body_size, response_body_hash_raw, response_body_size_raw FROM entries ORDER BY id",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
        ))
    })?;

    let mut update = conn.prepare(
        "UPDATE entries SET url=?1, query_string=?2, request_body_hash=?3, request_body_size=?4, response_body_hash=?5, response_body_size=?6, response_body_hash_raw=?7, response_body_size_raw=?8 WHERE id=?9",
    )?;

    let has_fts: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='response_body_fts'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;

    let mut findings: Vec<PiiFinding> = Vec::new();
    let mut text_cache: HashMap<String, Option<String>> = HashMap::new();
    let mut redacted_cache: HashMap<String, Option<PiiRedactedBlob>> = HashMap::new();
    let mut changed_response_hashes: HashSet<String> = HashSet::new();

    for row in rows {
        let (
            entry_id,
            url,
            query_string,
            req_body_hash,
            req_body_size,
            resp_body_hash,
            resp_body_size,
            resp_body_hash_raw,
            resp_body_size_raw,
        ) = row?;

        let mut changed = false;
        let mut new_url = url.clone();
        let mut new_query_string = query_string.clone();
        let mut new_req_body_hash = req_body_hash.clone();
        let mut new_req_body_size = req_body_size;
        let mut new_resp_body_hash = resp_body_hash.clone();
        let mut new_resp_body_size = resp_body_size;
        let mut new_resp_body_hash_raw = resp_body_hash_raw.clone();
        let mut new_resp_body_size_raw = resp_body_size_raw;

        if let Some(url_str) = url.as_deref() {
            append_findings(
                &mut findings,
                entry_id,
                url_str,
                PiiLocation::Url,
                scan_text(url_str, &matchers),
            );

            if options.redact {
                if let Some((redacted, _)) = redact_text(url_str, &matchers, &options.token) {
                    if redacted != url_str {
                        new_url = Some(redacted.clone());
                        if let Ok(parsed) = Url::parse(&redacted) {
                            new_query_string = parsed.query().map(|q| q.to_string());
                        }
                        changed = true;
                    }
                }
            }
        }

        if let Some(hash) = req_body_hash.as_deref() {
            if let Some(text) = load_blob_text(&conn, hash, &mut text_cache)? {
                append_findings(
                    &mut findings,
                    entry_id,
                    url.as_deref().unwrap_or_default(),
                    PiiLocation::RequestBody,
                    scan_text(&text, &matchers),
                );

                if options.redact {
                    if let Some(redacted) = redact_blob_cached(
                        &conn,
                        hash,
                        &matchers,
                        &options.token,
                        write,
                        &mut redacted_cache,
                    )? {
                        new_req_body_hash = Some(redacted.new_hash);
                        new_req_body_size = Some(redacted.new_size);
                        changed = true;
                    }
                }
            }
        }

        if let Some(hash) = resp_body_hash.as_deref() {
            if let Some(text) = load_blob_text(&conn, hash, &mut text_cache)? {
                append_findings(
                    &mut findings,
                    entry_id,
                    url.as_deref().unwrap_or_default(),
                    PiiLocation::ResponseBody,
                    scan_text(&text, &matchers),
                );

                if options.redact {
                    if let Some(redacted) = redact_blob_cached(
                        &conn,
                        hash,
                        &matchers,
                        &options.token,
                        write,
                        &mut redacted_cache,
                    )? {
                        new_resp_body_hash = Some(redacted.new_hash.clone());
                        new_resp_body_size = Some(redacted.new_size);
                        new_resp_body_hash_raw = None;
                        new_resp_body_size_raw = None;
                        changed = true;

                        if write {
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
                                    upsert_response_fts(&conn, &redacted.new_hash, &redacted.text)?;
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
            conn.prepare("SELECT COUNT(*) FROM entries WHERE response_body_hash = ?1")?;
        let mut delete_stmt = conn.prepare("DELETE FROM response_body_fts WHERE hash = ?1")?;
        for hash in changed_response_hashes {
            let count: i64 = check_stmt.query_row([hash.as_str()], |row| row.get(0))?;
            if count == 0 {
                delete_stmt.execute([hash])?;
            }
        }
    }

    match options.format {
        OutputFormat::Json => write_json(&findings),
        OutputFormat::Csv => write_csv(&findings),
        OutputFormat::Table => write_table(&findings),
    }
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
    cache: &mut HashMap<String, Option<String>>,
) -> Result<Option<String>> {
    if let Some(existing) = cache.get(hash) {
        return Ok(existing.clone());
    }

    let Some((content, _mime_type)) = load_blob_for_pii(conn, hash)? else {
        cache.insert(hash.to_string(), None);
        return Ok(None);
    };

    let text = match std::str::from_utf8(&content) {
        Ok(s) => s.to_string(),
        Err(_) => {
            cache.insert(hash.to_string(), None);
            return Ok(None);
        }
    };

    cache.insert(hash.to_string(), Some(text.clone()));
    Ok(Some(text))
}

fn load_blob_for_pii(conn: &Connection, hash: &str) -> Result<Option<(Vec<u8>, Option<String>)>> {
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
    matchers: &PiiMatchers,
    token: &str,
    write: bool,
    cache: &mut HashMap<String, Option<PiiRedactedBlob>>,
) -> Result<Option<PiiRedactedBlob>> {
    if let Some(existing) = cache.get(hash) {
        return Ok(existing.clone());
    }

    let Some((content, mime_type)) = load_blob_for_pii(conn, hash)? else {
        cache.insert(hash.to_string(), None);
        return Ok(None);
    };

    let text = match std::str::from_utf8(&content) {
        Ok(s) => s,
        Err(_) => {
            cache.insert(hash.to_string(), None);
            return Ok(None);
        }
    };

    let Some((redacted_text, _)) = redact_text(text, matchers, token) else {
        cache.insert(hash.to_string(), None);
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

    cache.insert(hash.to_string(), Some(redacted.clone()));
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

fn write_csv_field(out: &mut impl Write, field: &str) -> Result<()> {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        out.write_all(b"\"")?;
        for ch in field.chars() {
            if ch == '"' {
                out.write_all(b"\"\"")?;
            } else {
                out.write_all(ch.to_string().as_bytes())?;
            }
        }
        out.write_all(b"\"")?;
    } else {
        out.write_all(field.as_bytes())?;
    }
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
