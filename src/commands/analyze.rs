use std::collections::HashMap;
use std::path::PathBuf;

use chrono::SecondsFormat;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::Connection;
use serde::Serialize;
use url::Url;

use crate::db::{ensure_schema_upgrades, load_entries, EntryQuery, EntryRow};
use crate::error::{HarliteError, Result};

pub struct AnalyzeOptions {
    pub json: bool,
    pub host: Vec<String>,
    pub method: Vec<String>,
    pub status: Vec<i32>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub slow_total_ms: f64,
    pub slow_ttfb_ms: f64,
    pub top: usize,
}

#[derive(Debug, Serialize)]
struct TimeStats {
    count: usize,
    min: f64,
    max: f64,
    avg: f64,
    p50: f64,
    p90: f64,
    p95: f64,
}

#[derive(Debug, Serialize)]
struct TimingAggregates {
    total_ms: Option<TimeStats>,
    ttfb_ms: Option<TimeStats>,
    dns_ms: Option<TimeStats>,
    connect_ms: Option<TimeStats>,
    ssl_ms: Option<TimeStats>,
    send_ms: Option<TimeStats>,
    receive_ms: Option<TimeStats>,
}

#[derive(Debug, Serialize)]
struct SlowEntry {
    method: String,
    url: String,
    host: Option<String>,
    status: Option<i32>,
    total_ms: Option<f64>,
    ttfb_ms: Option<f64>,
    dns_ms: Option<f64>,
    connect_ms: Option<f64>,
    ssl_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
struct SlowRequests {
    total_ms: Vec<SlowEntry>,
    ttfb_ms: Vec<SlowEntry>,
    total_count: usize,
    ttfb_count: usize,
}

#[derive(Debug, Serialize)]
struct ConnectionReuse {
    requests_with_connection_id: usize,
    unique_connection_ids: usize,
    reused_connection_ids: usize,
    requests_on_reused_connections: usize,
    reuse_rate: Option<f64>,
}

#[derive(Debug, Serialize)]
struct CacheCandidate {
    url: String,
    host: Option<String>,
    count: usize,
    total_response_bytes: i64,
    avg_response_bytes: i64,
}

#[derive(Debug, Serialize)]
struct CacheCandidates {
    total_requests: usize,
    unique_urls: usize,
    top: Vec<CacheCandidate>,
}

#[derive(Debug, Serialize)]
struct Filters {
    host: Vec<String>,
    method: Vec<String>,
    status: Vec<i32>,
    from: Option<String>,
    to: Option<String>,
}

#[derive(Debug, Serialize)]
struct Thresholds {
    slow_total_ms: f64,
    slow_ttfb_ms: f64,
    top: usize,
}

#[derive(Debug, Serialize)]
struct Bottleneck {
    phase: String,
    avg_ms: f64,
    share_of_total: Option<f64>,
}

#[derive(Debug, Serialize)]
struct AnalyzeOutput {
    entries: usize,
    filters: Filters,
    thresholds: Thresholds,
    aggregates: TimingAggregates,
    slow_requests: SlowRequests,
    connection_reuse: ConnectionReuse,
    cache_candidates: CacheCandidates,
    bottleneck: Option<Bottleneck>,
}

pub fn run_analyze(database: PathBuf, options: &AnalyzeOptions) -> Result<()> {
    let conn = Connection::open(&database)?;
    ensure_schema_upgrades(&conn)?;

    let from_started_at = match options.from.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, false)?),
        None => None,
    };
    let to_started_at = match options.to.as_deref() {
        Some(s) => Some(parse_started_at_bound(s, true)?),
        None => None,
    };

    let mut query = EntryQuery::default();
    query.hosts = options.host.clone();
    query.methods = options.method.clone();
    query.statuses = options.status.clone();
    query.from_started_at = from_started_at.clone();
    query.to_started_at = to_started_at.clone();

    let entries = load_entries(&conn, &query)?;

    let mut totals = Vec::new();
    let mut ttfb = Vec::new();
    let mut dns = Vec::new();
    let mut connect = Vec::new();
    let mut ssl = Vec::new();
    let mut send = Vec::new();
    let mut receive = Vec::new();

    let mut slow_total = Vec::new();
    let mut slow_ttfb = Vec::new();

    let mut connection_counts: HashMap<String, usize> = HashMap::new();
    let mut cache_map: HashMap<String, CacheCandidate> = HashMap::new();

    for row in &entries {
        let total_ms = normalize_ms(row.time_ms);
        let ttfb_ms = normalize_ms(row.wait_ms);
        let dns_ms = normalize_ms(row.dns_ms);
        let connect_ms = normalize_ms(row.connect_ms);
        let ssl_ms = normalize_ms(row.ssl_ms);
        let send_ms = normalize_ms(row.send_ms);
        let receive_ms = normalize_ms(row.receive_ms);

        if let Some(value) = total_ms {
            totals.push(value);
            if value >= options.slow_total_ms {
                slow_total.push(build_slow_entry(row, total_ms, ttfb_ms, dns_ms, connect_ms, ssl_ms));
            }
        }

        if let Some(value) = ttfb_ms {
            ttfb.push(value);
            if value >= options.slow_ttfb_ms {
                slow_ttfb.push(build_slow_entry(row, total_ms, ttfb_ms, dns_ms, connect_ms, ssl_ms));
            }
        }

        if let Some(value) = dns_ms {
            dns.push(value);
        }
        if let Some(value) = connect_ms {
            connect.push(value);
        }
        if let Some(value) = ssl_ms {
            ssl.push(value);
        }
        if let Some(value) = send_ms {
            send.push(value);
        }
        if let Some(value) = receive_ms {
            receive.push(value);
        }

        if let Some(conn_id) = row.connection_id.as_deref() {
            *connection_counts.entry(conn_id.to_string()).or_insert(0) += 1;
        }

        if is_cache_candidate(row) {
            let url = row.url.clone().unwrap_or_default();
            let host = row.host.clone().or_else(|| host_from_url(&url));
            let bytes = normalize_i64(row.response_body_size).unwrap_or(0);
            let entry = cache_map.entry(url.clone()).or_insert(CacheCandidate {
                url,
                host,
                count: 0,
                total_response_bytes: 0,
                avg_response_bytes: 0,
            });
            entry.count += 1;
            entry.total_response_bytes += bytes;
        }
    }

    slow_total.sort_by(|a, b| cmp_desc(a.total_ms, b.total_ms));
    slow_ttfb.sort_by(|a, b| cmp_desc(a.ttfb_ms, b.ttfb_ms));

    if slow_total.len() > options.top {
        slow_total.truncate(options.top);
    }
    if slow_ttfb.len() > options.top {
        slow_ttfb.truncate(options.top);
    }

    for candidate in cache_map.values_mut() {
        if candidate.count > 0 {
            candidate.avg_response_bytes = candidate.total_response_bytes / candidate.count as i64;
        }
    }
    let mut cache_candidates: Vec<CacheCandidate> = cache_map.into_values().collect();
    cache_candidates.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| b.total_response_bytes.cmp(&a.total_response_bytes))
    });
    let unique_urls = cache_candidates.len();
    if cache_candidates.len() > options.top {
        cache_candidates.truncate(options.top);
    }

    let reuse_stats = connection_reuse_stats(&connection_counts);

    let aggregates = TimingAggregates {
        total_ms: build_stats(&totals),
        ttfb_ms: build_stats(&ttfb),
        dns_ms: build_stats(&dns),
        connect_ms: build_stats(&connect),
        ssl_ms: build_stats(&ssl),
        send_ms: build_stats(&send),
        receive_ms: build_stats(&receive),
    };

    let bottleneck = detect_bottleneck(&aggregates);

    let output = AnalyzeOutput {
        entries: entries.len(),
        filters: Filters {
            host: options.host.clone(),
            method: options.method.clone(),
            status: options.status.clone(),
            from: from_started_at,
            to: to_started_at,
        },
        thresholds: Thresholds {
            slow_total_ms: options.slow_total_ms,
            slow_ttfb_ms: options.slow_ttfb_ms,
            top: options.top,
        },
        aggregates,
        slow_requests: SlowRequests {
            total_ms: slow_total,
            ttfb_ms: slow_ttfb,
            total_count: count_over_threshold(&entries, options.slow_total_ms, |r| normalize_ms(r.time_ms)),
            ttfb_count: count_over_threshold(&entries, options.slow_ttfb_ms, |r| normalize_ms(r.wait_ms)),
        },
        connection_reuse: reuse_stats,
        cache_candidates: CacheCandidates {
            total_requests: count_cache_candidates(&entries),
            unique_urls,
            top: cache_candidates,
        },
        bottleneck,
    };

    if options.json {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        render_text(&output);
    }

    Ok(())
}

fn parse_started_at_bound(s: &str, is_end: bool) -> Result<String> {
    let s = s.trim();
    if s.is_empty() {
        return Err(HarliteError::InvalidHar(
            "Empty timestamp bound".to_string(),
        ));
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Millis, true));
    }

    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
    let dt = if is_end {
        date.and_hms_milli_opt(23, 59, 59, 999)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid end date".to_string()))?
    } else {
        date.and_hms_opt(0, 0, 0)
            .and_then(|d| d.and_local_timezone(Utc).single())
            .ok_or_else(|| HarliteError::InvalidHar("Invalid start date".to_string()))?
    };
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn normalize_ms(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v >= 0.0 => Some(v),
        _ => None,
    }
}

fn normalize_i64(value: Option<i64>) -> Option<i64> {
    match value {
        Some(v) if v >= 0 => Some(v),
        _ => None,
    }
}

fn cmp_desc(a: Option<f64>, b: Option<f64>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(x), Some(y)) => y.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn build_stats(values: &[f64]) -> Option<TimeStats> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let count = sorted.len();
    let min = *sorted.first().unwrap_or(&0.0);
    let max = *sorted.last().unwrap_or(&0.0);
    let sum: f64 = sorted.iter().sum();
    let avg = sum / count as f64;
    let p50 = percentile(&sorted, 50.0);
    let p90 = percentile(&sorted, 90.0);
    let p95 = percentile(&sorted, 95.0);
    Some(TimeStats {
        count,
        min,
        max,
        avg,
        p50,
        p90,
        p95,
    })
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = (pct / 100.0) * (sorted.len() as f64 - 1.0);
    let idx = rank.round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn build_slow_entry(
    row: &EntryRow,
    total_ms: Option<f64>,
    ttfb_ms: Option<f64>,
    dns_ms: Option<f64>,
    connect_ms: Option<f64>,
    ssl_ms: Option<f64>,
) -> SlowEntry {
    let url = row.url.clone().unwrap_or_default();
    let host = row.host.clone().or_else(|| host_from_url(&url));
    SlowEntry {
        method: row.method.clone().unwrap_or_else(|| "-".to_string()),
        url,
        host,
        status: row.status,
        total_ms,
        ttfb_ms,
        dns_ms,
        connect_ms,
        ssl_ms,
    }
}

fn host_from_url(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
}

fn headers_from_json(json: Option<&str>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Some(json) = json else {
        return map;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return map;
    };
    let Some(obj) = value.as_object() else {
        return map;
    };
    for (k, v) in obj {
        if let Some(s) = v.as_str() {
            map.insert(k.to_ascii_lowercase(), s.to_string());
        }
    }
    map
}

fn is_cache_candidate(row: &EntryRow) -> bool {
    let method = row.method.as_deref().unwrap_or("").to_ascii_uppercase();
    if method != "GET" {
        return false;
    }
    if row.status != Some(200) {
        return false;
    }
    if normalize_i64(row.response_body_size).unwrap_or(0) <= 0 {
        return false;
    }
    let headers = headers_from_json(row.response_headers.as_deref());
    let cache_control = headers
        .get("cache-control")
        .map(|v| v.to_ascii_lowercase());
    if let Some(control) = cache_control {
        if control.contains("no-store")
            || control.contains("no-cache")
            || control.contains("max-age=0")
        {
            return false;
        }
    }
    let has_validator = headers.contains_key("etag")
        || headers.contains_key("last-modified")
        || headers.contains_key("expires");
    !has_validator
}

fn count_cache_candidates(entries: &[EntryRow]) -> usize {
    entries.iter().filter(|row| is_cache_candidate(row)).count()
}

fn connection_reuse_stats(connection_counts: &HashMap<String, usize>) -> ConnectionReuse {
    let requests_with_connection_id: usize = connection_counts.values().sum();
    let unique_connection_ids = connection_counts.len();
    let reused_connection_ids = connection_counts.values().filter(|v| **v > 1).count();
    let requests_on_reused_connections: usize =
        connection_counts.values().filter(|v| **v > 1).sum();
    let reuse_rate = if requests_with_connection_id > 0 {
        Some(requests_on_reused_connections as f64 / requests_with_connection_id as f64)
    } else {
        None
    };
    ConnectionReuse {
        requests_with_connection_id,
        unique_connection_ids,
        reused_connection_ids,
        requests_on_reused_connections,
        reuse_rate,
    }
}

fn detect_bottleneck(aggregates: &TimingAggregates) -> Option<Bottleneck> {
    let candidates = [
        ("dns", aggregates.dns_ms.as_ref()),
        ("connect", aggregates.connect_ms.as_ref()),
        ("ssl", aggregates.ssl_ms.as_ref()),
        ("send", aggregates.send_ms.as_ref()),
        ("ttfb", aggregates.ttfb_ms.as_ref()),
        ("receive", aggregates.receive_ms.as_ref()),
    ];
    let mut best: Option<(&str, &TimeStats)> = None;
    for (name, stats) in candidates {
        if let Some(stats) = stats {
            if best.is_none() || stats.avg > best.unwrap().1.avg {
                best = Some((name, stats));
            }
        }
    }
    let total_avg = aggregates.total_ms.as_ref().map(|s| s.avg);
    best.map(|(name, stats)| Bottleneck {
        phase: name.to_string(),
        avg_ms: stats.avg,
        share_of_total: total_avg.map(|avg| if avg > 0.0 { stats.avg / avg } else { 0.0 }),
    })
}

fn count_over_threshold<F>(entries: &[EntryRow], threshold: f64, fetch: F) -> usize
where
    F: Fn(&EntryRow) -> Option<f64>,
{
    entries
        .iter()
        .filter_map(|row| fetch(row))
        .filter(|v| *v >= threshold)
        .count()
}

fn render_text(output: &AnalyzeOutput) {
    println!("entries={}", output.entries);
    println!("filters.hosts={}", output.filters.host.join(","));
    println!("filters.methods={}", output.filters.method.join(","));
    let status_list = output
        .filters
        .status
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(",");
    println!("filters.status={}", status_list);
    println!(
        "filters.from={}",
        output.filters.from.as_deref().unwrap_or("")
    );
    println!("filters.to={}", output.filters.to.as_deref().unwrap_or(""));
    println!("thresholds.slow_total_ms={}", output.thresholds.slow_total_ms);
    println!("thresholds.slow_ttfb_ms={}", output.thresholds.slow_ttfb_ms);
    println!("thresholds.top={}", output.thresholds.top);

    print_stats_line("total_ms", &output.aggregates.total_ms);
    print_stats_line("ttfb_ms", &output.aggregates.ttfb_ms);
    print_stats_line("dns_ms", &output.aggregates.dns_ms);
    print_stats_line("connect_ms", &output.aggregates.connect_ms);
    print_stats_line("ssl_ms", &output.aggregates.ssl_ms);
    print_stats_line("send_ms", &output.aggregates.send_ms);
    print_stats_line("receive_ms", &output.aggregates.receive_ms);

    println!("slow.total_count={}", output.slow_requests.total_count);
    for entry in &output.slow_requests.total_ms {
        println!(
            "slow.total_ms={:.1} method={} status={} url={}",
            entry.total_ms.unwrap_or(0.0),
            entry.method,
            entry.status
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.url
        );
    }

    println!("slow.ttfb_count={}", output.slow_requests.ttfb_count);
    for entry in &output.slow_requests.ttfb_ms {
        println!(
            "slow.ttfb_ms={:.1} method={} status={} url={}",
            entry.ttfb_ms.unwrap_or(0.0),
            entry.method,
            entry.status
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.url
        );
    }

    println!(
        "connection_reuse.requests_with_connection_id={}",
        output.connection_reuse.requests_with_connection_id
    );
    println!(
        "connection_reuse.unique_connection_ids={}",
        output.connection_reuse.unique_connection_ids
    );
    println!(
        "connection_reuse.reused_connection_ids={}",
        output.connection_reuse.reused_connection_ids
    );
    println!(
        "connection_reuse.requests_on_reused_connections={}",
        output.connection_reuse.requests_on_reused_connections
    );
    if let Some(rate) = output.connection_reuse.reuse_rate {
        println!("connection_reuse.reuse_rate={:.3}", rate);
    }

    println!(
        "cache_candidates.total_requests={}",
        output.cache_candidates.total_requests
    );
    println!(
        "cache_candidates.unique_urls={}",
        output.cache_candidates.unique_urls
    );
    for candidate in &output.cache_candidates.top {
        println!(
            "cache_candidate.count={} avg_bytes={} url={}",
            candidate.count, candidate.avg_response_bytes, candidate.url
        );
    }

    if let Some(bottleneck) = &output.bottleneck {
        println!("bottleneck.phase={}", bottleneck.phase);
        println!("bottleneck.avg_ms={:.1}", bottleneck.avg_ms);
        if let Some(share) = bottleneck.share_of_total {
            println!("bottleneck.share_of_total={:.3}", share);
        }
    }
}

fn print_stats_line(name: &str, stats: &Option<TimeStats>) {
    if let Some(stats) = stats {
        println!(
            "stats.{}=count:{} min:{:.1} p50:{:.1} p90:{:.1} p95:{:.1} avg:{:.1} max:{:.1}",
            name,
            stats.count,
            stats.min,
            stats.p50,
            stats.p90,
            stats.p95,
            stats.avg,
            stats.max
        );
    } else {
        println!("stats.{}=count:0", name);
    }
}
