# harlite

Import HAR (HTTP Archive) files into SQLite. Query your web traffic with SQL.

## Why?

HAR files are JSON blobs that capture browser network activity. They're useful for debugging, performance analysis, and understanding how websites work — but querying them is painful:

```bash
# The old way: jq gymnastics
cat capture.har | jq '.log.entries[] | select(.response.status >= 400) | {url: .request.url, status: .response.status}'
```

With `harlite`, import once and query with SQL:

```bash
harlite import capture.har

harlite query "SELECT url, status FROM entries WHERE status >= 400"
```

Works great with AI coding agents like Codex and Claude — they already know SQL.

## Features

- **Fast imports** — Rust-native performance
- **Smart deduplication** — Response bodies stored once using content-addressable hashing (BLAKE3)
- **Flexible body storage** — Metadata-only by default, opt-in to store bodies
- **Optional body decompression** — Import gzip/br responses as decoded bytes
- **External body extraction** — Store body blobs as hashed files on disk (`--extract-bodies`)
- **Full-text search** — SQLite FTS5 over response bodies (`harlite search`)
- **Multi-file support** — Merge multiple HAR files into one database
- **Queryable headers** — Headers stored as JSON, queryable with SQLite JSON functions
- **Safe sharing** — Redact sensitive headers/cookies before sharing a database

## Installation

### Install with Cargo

```bash
cargo install harlite
```

### Build and run locally

```bash
git clone https://github.com/brucehart/harlite
cd harlite

# Requires Rust/Cargo >= 1.85
# Recommended: use rustup to manage toolchains
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup update stable

# Run without installing
cargo run -- --help

# Or install locally
cargo install --path .

# Or build a release binary
cargo build --release
./target/release/harlite --help
```
## Features

- **Fast imports** — Rust-native performance
- **Smart deduplication** — Response bodies stored once using content-addressable hashing (BLAKE3)
- **Flexible body storage** — Metadata-only by default, opt-in to store bodies
- **Multi-file support** — Merge multiple HAR files into one database
- **Queryable headers** — Headers stored as JSON, queryable with SQLite JSON functions
- **Safe sharing** — Redact sensitive headers/cookies before sharing a database

## Installation

### Install with Cargo

```bash
cargo install harlite
```

### Build and run locally

```bash
git clone https://github.com/brucehart/harlite
cd harlite

# Requires Rust/Cargo >= 1.85
# Recommended: use rustup to manage toolchains
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup update stable

# Run without installing
cargo run -- --help

# Or install locally
cargo install --path .

# Or build a release binary
cargo build --release
./target/release/harlite --help
```

## Quick Start

```bash
# Import a single HAR file (creates capture.db by default)
harlite import browsing-session.har

# Import multiple HAR files into one database
harlite import day1.har day2.har day3.har -o traffic.db

# Query with harlite
harlite query "SELECT method, url, status, time_ms FROM entries LIMIT 10" traffic.db

# Or query with sqlite3 / any SQLite tool
# sqlite3 traffic.db "SELECT method, url, status, time_ms FROM entries LIMIT 10"

# Or use any SQLite tool: DBeaver, datasette, Python, etc.
```

## Usage

### Import HAR files

```bash
# Basic import (creates <filename>.db)
harlite import capture.har

# Specify output database
harlite import capture.har -o mydata.db

# Import multiple files (merges into one database)
harlite import *.har -o all-traffic.db
```

### Import with response bodies

By default, `harlite` imports metadata only (URLs, headers, timing, status codes). Response bodies are **not stored** to keep databases small and imports fast.

```bash
# Include text bodies under 100KB (HTML, JSON, JS, CSS, XML)
harlite import capture.har --bodies --text-only

# Include all bodies under 500KB  
harlite import capture.har --bodies --max-body-size 500KB

# Decompress response bodies based on Content-Encoding (gzip, br)
harlite import capture.har --bodies --decompress-bodies

# Keep both decompressed and original (compressed) variants
harlite import capture.har --bodies --decompress-bodies --keep-compressed

# Extract bodies to files (stored by hash); implies --bodies
harlite import capture.har --extract-bodies ./bodies

# Extract only response bodies, with 2-level sharding (aa/bb/<hash>)
harlite import capture.har --extract-bodies ./bodies --extract-bodies-kind response --extract-bodies-shard-depth 2

# Include everything (warning: large databases)
harlite import capture.har --bodies --max-body-size unlimited

# Show deduplication stats after import
harlite import capture.har --bodies --stats
# Output:
#   Entries imported: 847
#   Unique response bodies: 203
#   Space saved by deduplication: 127 MB (74%)
```

Response bodies are automatically deduplicated using BLAKE3 hashing. If the same JavaScript bundle appears in 50 entries, it's stored only once.

### Full-text search (FTS5)

If you imported bodies, `harlite` maintains a SQLite FTS index over response bodies (text only):

```bash
harlite search "timeout NEAR/3 error" traffic.db
```

To rebuild the index (or change tokenizers):

```bash
harlite fts-rebuild traffic.db --tokenizer porter
```

### View schema

```bash
# Print the SQLite schema
harlite schema

# Print schema as it exists in a database
harlite schema traffic.db
```

### Database info

```bash
# Show summary statistics for a database
harlite info traffic.db

# Output:
#   Database: traffic.db
#   Imports: 3 files
#   Entries: 1,247
#   Date range: 2024-01-15 to 2024-01-17
#   Unique hosts: 23
#   Stored blobs: 156 (12.4 MB)
```

### Database stats

`harlite stats` is a faster, script-friendly alternative to `harlite info`.

```bash
harlite stats traffic.db
# imports=3
# entries=1247
# date_min=2024-01-15
# date_max=2024-01-17
# unique_hosts=23
# blobs=156
# blob_bytes=13002342

# JSON output
harlite stats traffic.db --json
```

### Export HAR files

Export a `harlite` SQLite database back to HAR format (optionally with bodies if they were stored during import):

```bash
# Export all entries (pretty-printed by default)
harlite export traffic.db -o traffic.har

# Export to stdout
harlite export traffic.db -o -

# Include stored request/response bodies (if present in the DB)
harlite export traffic.db --bodies -o traffic-with-bodies.har

# Compact JSON
harlite export traffic.db --compact -o traffic.min.har

# Filter examples
harlite export traffic.db --host api.example.com --status 200 --method GET -o api-get-200.har
harlite export traffic.db --url-regex 'example\\.com/(api|v1)/' -o filtered.har
harlite export traffic.db --from 2024-01-15 --to 2024-01-16 -o day1.har
harlite export traffic.db --ext js,css -o assets.har
harlite export traffic.db --source session1.har --source-contains chrome -o sources.har
harlite export traffic.db --mime json --min-response-size 1KB --max-response-size 200KB -o api-responses.har
```

Common filters:
- `--url`, `--url-contains`, `--url-regex`
- `--host`, `--method`, `--status`
- `--mime` (substring match), `--ext` (file extension)
- `--from` / `--to` (RFC3339 timestamp or `YYYY-MM-DD`)
- `--min-request-size` / `--max-request-size`, `--min-response-size` / `--max-response-size`
- `--source` / `--source-contains` (filters by `imports.source_file`)

Notes / gaps:
- HAR `timings` are reconstructed from the stored total duration (`time_ms`), so the breakdown is best-effort.
- Some HAR fields are not stored in the DB (e.g. `headersSize`, response `httpVersion`), so they may be omitted or approximated on export.

### Redact sensitive data

Redact common sensitive headers/cookies (by default: `authorization`, `cookie`, `set-cookie`, `x-api-key`, etc.) before sharing:

```bash
# Modify in-place
harlite redact traffic.db

# Write to a new database (recommended)
harlite redact traffic.db --output traffic.redacted.db

# Dry run (no writes)
harlite redact traffic.db --dry-run

# Customize patterns (wildcard match by default)
harlite redact traffic.db --no-defaults --match exact --header authorization --cookie sessionid

# Wildcard / regex name matching
harlite redact traffic.db --match wildcard --header '*token*'
harlite redact traffic.db --match regex --header '^(authorization|x-api-key)$'
```

### Query with harlite

Run ad-hoc SQL against a harlite SQLite database and format the results:

```bash
# Default output: table with headers
harlite query "SELECT method, url, status FROM entries LIMIT 5" traffic.db

# CSV / JSON output (includes headers / keys)
harlite query "SELECT host, COUNT(*) AS n FROM entries GROUP BY host" traffic.db --format csv
harlite query "SELECT host, COUNT(*) AS n FROM entries GROUP BY host" traffic.db --format json

# Apply limit/offset without editing your SQL (wraps the query)
harlite query "SELECT * FROM entries ORDER BY started_at" traffic.db --limit 100 --offset 200

# If you omit the database path, harlite will use the only *.db in the current directory (if exactly one exists)
harlite query "SELECT COUNT(*) AS entries FROM entries" --format json
```

## Database Schema

### `entries` table

The main table containing one row per HTTP request/response pair.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `import_id` | INTEGER | References `imports.id` |
| `page_id` | TEXT | References `pages.id` (if available) |
| `started_at` | TEXT | ISO 8601 timestamp |
| `time_ms` | REAL | Total request duration in milliseconds |
| `method` | TEXT | HTTP method (GET, POST, etc.) |
| `url` | TEXT | Full request URL |
| `host` | TEXT | Hostname extracted from URL |
| `path` | TEXT | Path extracted from URL |
| `query_string` | TEXT | Query string (without leading ?) |
| `http_version` | TEXT | HTTP version (HTTP/1.1, h2, etc.) |
| `request_headers` | TEXT | Request headers as JSON object |
| `request_cookies` | TEXT | Request cookies as JSON array |
| `request_body_hash` | TEXT | BLAKE3 hash referencing `blobs.hash` |
| `request_body_size` | INTEGER | Request body size in bytes |
| `status` | INTEGER | HTTP response status code |
| `status_text` | TEXT | HTTP response status text |
| `response_headers` | TEXT | Response headers as JSON object |
| `response_cookies` | TEXT | Response cookies as JSON array |
| `response_body_hash` | TEXT | BLAKE3 hash referencing `blobs.hash` |
| `response_body_size` | INTEGER | Response body size in bytes |
| `response_mime_type` | TEXT | Response MIME type |
| `is_redirect` | INTEGER | 1 if 3xx redirect, 0 otherwise |
| `server_ip` | TEXT | Server IP address (if available) |
| `connection_id` | TEXT | Connection ID (if available) |

### `blobs` table

Content-addressable storage for request/response bodies. Bodies are deduplicated by hash.

| Column | Type | Description |
|--------|------|-------------|
| `hash` | TEXT | BLAKE3 hash (primary key) |
| `content` | BLOB | Raw body content |
| `size` | INTEGER | Content size in bytes |
| `mime_type` | TEXT | MIME type (if known) |

### `pages` table

Page/document information from the HAR (if present).

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | Page ID from HAR |
| `import_id` | INTEGER | References `imports.id` |
| `started_at` | TEXT | Page load start time |
| `title` | TEXT | Page title |
| `on_content_load_ms` | REAL | DOMContentLoaded timing |
| `on_load_ms` | REAL | Window load timing |

### `imports` table

Tracks import history for auditing and multi-file management.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `source_file` | TEXT | Original HAR filename |
| `imported_at` | TEXT | Import timestamp |
| `entry_count` | INTEGER | Number of entries imported |

### Indexes

The following indexes are created for fast queries:

- `idx_entries_url` — URL lookups and LIKE queries
- `idx_entries_host` — Filter by domain
- `idx_entries_status` — Filter by status code
- `idx_entries_method` — Filter by HTTP method
- `idx_entries_mime` — Filter by content type
- `idx_entries_started` — Time range queries
- `idx_entries_import` — Filter by import source

## Example Queries

### Find slow requests

```sql
SELECT method, url, status, time_ms 
FROM entries 
WHERE time_ms > 1000 
ORDER BY time_ms DESC;
```

### List all API calls

```sql
SELECT method, url, status, response_body_size
FROM entries
WHERE url LIKE '%/api/%'
ORDER BY started_at;
```

### Count requests by domain

```sql
SELECT host, COUNT(*) as count, AVG(time_ms) as avg_time_ms
FROM entries
GROUP BY host
ORDER BY count DESC;
```

### Find failed requests

```sql
SELECT method, url, status, status_text
FROM entries
WHERE status >= 400
ORDER BY status;
```

### Show largest responses

```sql
SELECT url, response_mime_type, response_body_size
FROM entries
WHERE response_body_size IS NOT NULL
ORDER BY response_body_size DESC
LIMIT 20;
```

### Get response body for an entry

```sql
SELECT e.url, e.status, b.content
FROM entries e
JOIN blobs b ON e.response_body_hash = b.hash
WHERE e.url LIKE '%/api/users%';
```

### Find duplicate responses

Identify responses that appear multiple times (useful for finding redundant API calls or cached resources):

```sql
SELECT 
    b.hash,
    b.size,
    b.mime_type,
    COUNT(*) as times_seen,
    GROUP_CONCAT(DISTINCT e.host) as hosts
FROM blobs b
JOIN entries e ON e.response_body_hash = b.hash
GROUP BY b.hash
HAVING COUNT(*) > 1
ORDER BY b.size * COUNT(*) DESC;
```

### Calculate space saved by deduplication

```sql
SELECT 
    SUM(e.response_body_size) as total_if_duplicated,
    (SELECT SUM(size) FROM blobs) as actual_stored,
    SUM(e.response_body_size) - (SELECT SUM(size) FROM blobs) as bytes_saved
FROM entries e
WHERE e.response_body_hash IS NOT NULL;
```

### Extract JSON API responses

```sql
SELECT url, json_extract(response_headers, '$.content-type') as content_type
FROM entries
WHERE response_mime_type LIKE '%json%';
```

### Get requests in a time window

```sql
SELECT * FROM entries
WHERE started_at BETWEEN '2024-01-15T10:00:00' AND '2024-01-15T11:00:00';
```

### Find all unique endpoints (deduplicated)

```sql
SELECT DISTINCT method, host, path
FROM entries
WHERE host = 'api.example.com'
ORDER BY path;
```

### Analyze response headers

```sql
SELECT 
    url,
    json_extract(response_headers, '$.cache-control') as cache_control,
    json_extract(response_headers, '$.content-encoding') as encoding
FROM entries
WHERE json_extract(response_headers, '$.cache-control') IS NOT NULL;
```

### Requests by import source

```sql
SELECT 
    i.source_file,
    COUNT(*) as entries,
    MIN(e.started_at) as first_request,
    MAX(e.started_at) as last_request
FROM entries e
JOIN imports i ON e.import_id = i.id
GROUP BY i.id;
```

## Working with AI Agents

`harlite` is designed to work seamlessly with AI coding assistants:

```bash
# Import your browsing session
harlite import session.har -o api.db

# Ask Codex/Claude to analyze
# "Query api.db to find all POST requests to endpoints containing 'user' 
#  and show me the request bodies"
```

The AI can write SQL directly — no need to learn a custom query language.

### Tips for AI workflows

1. **Start with metadata-only imports** — faster iteration
2. **Use `harlite info`** to give the AI context about what's in the database
3. **Import with `--bodies --text-only`** when you need to analyze API responses
4. **The schema is stable** — AI can learn it once and reuse queries

## Tips

### Use with datasette

[Datasette](https://datasette.io/) provides an instant web UI for exploring SQLite databases:

```bash
pip install datasette
harlite import capture.har -o traffic.db
datasette traffic.db
# Opens browser to http://localhost:8001
```

### Export query results

```bash
# CSV export
sqlite3 -header -csv traffic.db "SELECT url, status FROM entries" > results.csv

# JSON export
sqlite3 -json traffic.db "SELECT url, status FROM entries" > results.json
```

### Merge multiple sessions

```bash
# Import from multiple HAR files
harlite import monday.har tuesday.har wednesday.har -o week.db

# Query across all sessions
sqlite3 week.db "SELECT source_file, COUNT(*) FROM entries GROUP BY source_file"
```

### Lightweight imports for large HAR files

```bash
# Skip bodies entirely for fastest import
harlite import huge-capture.har

# Or limit body size
harlite import huge-capture.har --bodies --max-body-size 10KB --text-only
```

## Building from Source

Requirements:
- Rust 1.70+

```bash
git clone https://github.com/brucehart/harlite
cd harlite
cargo build --release

# Run tests
cargo test

# Install locally
cargo install --path .
```

## License

MIT

## Contributing

Contributions welcome! Please open an issue to discuss major changes before submitting a PR.

## Roadmap

Future possibilities (not yet implemented):

- [ ] HAR 1.3 spec extensions

---

*Created by [Bruce Hart](https://bhart.org)

