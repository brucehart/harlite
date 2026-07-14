# AGENTS

This file is for AI coding agents (Codex, Claude Code, etc.). Keep instructions concise and repo-specific.

## Primary use case
Use the `harlite` binary to import HAR files into SQLite databases, then query/export/analyze them. This file is a runbook for agents that *use the CLI*, not for code changes.

## Repo layout
- `src/` — Rust source
- `tests/` — integration tests
- `schema.sql` — SQLite schema reference
- `target/` — build artifacts (do not edit)

## Install and run
- Install from crates.io: `cargo install harlite`
- Run help: `harlite --help`
- If running from source: `cargo run -- --help`

## CLI commands (subcommands)
- `import` — import HAR files into a SQLite database
- `query` — run SQL against a database
- `search` — full-text search over response bodies (FTS5)
- `fts-rebuild` — rebuild the FTS index
- `schema` — print the SQLite schema (built-in or from a DB)
- `info` — summarize database contents
- `stats` — script-friendly stats output (text or JSON)
- `analyze` — summarize timing/performance and enforce CI budgets
- `check` — validate HAR semantics or database/blob/FTS integrity
- `imports` — list import metadata
- `prune` — remove a specific import by id
- `export` — export a database back to HAR
- `merge` — merge multiple databases into one
- `redact` — redact sensitive headers/cookies/params/bodies
- `pii` — scan/redact PII in HAR files or databases
- `diff` — compare two HAR files or two databases
- `request` — export requests as cURL/Fetch/node-fetch/PowerShell snippets
- `report` — generate a self-contained HTML report (waterfall/slow/errors)

## Flags by command
### `import`
- `-o, --output <OUTPUT>`: output database file (default: `<first-input>.db`)
- `--bodies`: store response bodies in the database
- `--max-body-size <SIZE>`: limit body size (e.g., `100KB`, `1.5MB`, `unlimited`)
- `--text-only`: only store text bodies (HTML, JSON, JS, CSS, XML)
- `--stats`: print dedup stats after import
- `--decompress-bodies`: decode gzip/br bodies
- `--keep-compressed`: keep compressed body when decompressing
- `--extract-bodies <DIR>`: write bodies to files (implies `--bodies`)
- `--extract-bodies-kind <request|response|both>`: which bodies to extract
- `--extract-bodies-shard-depth <N>`: shard extracted files by hash depth
- `--host <HOST>`: hostname filter (repeatable)
- `--method <METHOD>`: HTTP method filter (repeatable)
- `--status <STATUS>`: HTTP status filter (repeatable)
- `--url-regex <REGEX>`: URL regex filter (repeatable)
- `--from <RFC3339|YYYY-MM-DD>`: only import on/after timestamp/date
- `--to <RFC3339|YYYY-MM-DD>`: only import on/before timestamp/date

### `schema`
- `[DATABASE]`: optional database to inspect (omit for default schema)

### `info`
- `<DATABASE>`: database to inspect

### `imports`
- `<DATABASE>`: database to inspect

### `prune`
- `--import-id <ID>`: import id to remove
- `--allow-external-paths`: also delete external blob files (off by default)
- `--external-path-root <DIR>`: restrict external file deletion to this root
- `<DATABASE>`: database to modify

### `stats`
- `--json`: JSON output
- `<DATABASE>`: database to inspect

### `analyze`
- `--max-p95-total-ms <MS>` / `--max-p95-ttfb-ms <MS>`: fail if timing budgets are exceeded
- `--max-errors <N>`: fail if the HTTP error count exceeds the budget
- `<DATABASE>`: database to analyze

### `check`
- `--json`: JSON output
- `--strict`: treat warnings as failures
- `--allow-external-paths`: verify trusted external blob files
- `--external-path-root <DIR>`: restrict external reads to this root
- `<INPUT>`: HAR, database, or `-` for HAR stdin

### `export`
- `-o, --output <OUTPUT>`: output HAR file (default: `<database>.har`, `-` for stdout)
- `--bodies`: include stored request/response bodies
- `--bodies-raw`: prefer raw/compressed bodies when available
- `--allow-external-paths`: allow reading external blob paths
- `--external-path-root <DIR>`: root dir for external blobs
- `--compact`: compact JSON (no pretty print)
- `--url <URL>`: exact URL filter (repeatable)
- `--url-contains <STR>`: URL substring filter (repeatable)
- `--url-regex <REGEX>`: URL regex filter (repeatable)
- `--host <HOST>`: hostname filter (repeatable)
- `--method <METHOD>`: HTTP method filter (repeatable)
- `--status <STATUS>`: HTTP status filter (repeatable)
- `--mime <MIME>`: response MIME substring filter (repeatable)
- `--ext <EXT>`: extension filter (repeatable, comma-separated allowed)
- `--source <FILE>`: import source filename filter (repeatable)
- `--source-contains <STR>`: import source substring filter (repeatable)
- `--from <RFC3339|YYYY-MM-DD>`: only export on/after timestamp/date
- `--to <RFC3339|YYYY-MM-DD>`: only export on/before timestamp/date
- `--min-request-size <SIZE>` / `--max-request-size <SIZE>`: request size filters
- `--min-response-size <SIZE>` / `--max-response-size <SIZE>`: response size filters

### `merge`
- `-o, --output <OUTPUT>`: output database file (default: `<first-input>-merged.db`)
- `--dry-run`: report stats only, no writes
- `--dedup <hash|exact>`: entry deduplication strategy

### `redact`
- `-o, --output <OUTPUT>`: output database/HAR (HAR defaults to a new sibling file)
- `--force`: overwrite output db if it exists
- `--dry-run`: report only, no writes
- `--no-defaults`: disable default redaction patterns
- `--header <NAME>`: header name pattern (repeatable)
- `--cookie <NAME>`: cookie name pattern (repeatable)
- `--query-param <NAME>`: query param name pattern (repeatable)
- `--body-regex <REGEX>`: body regex pattern (repeatable)
- `--match <exact|wildcard|regex>`: pattern match mode
- `--token <TOKEN>`: replacement token (default: `REDACTED`)
- `--allow-external-paths`: read external body files (off by default)
- `--external-path-root <DIR>`: restrict external body reads to this root

### `pii`
- `--redact`: write redacted findings back to the database
- `-o, --output <OUTPUT>`: output database when redacting
- `--allow-external-paths`: scan external body files (off by default)
- `--external-path-root <DIR>`: restrict external body reads to this root

### `diff`
- `<LEFT> <RIGHT>`: two HAR files or two databases to compare
- `-f, --format <table|csv|json>`: output format (table/JSON required)
- `--host <HOST>`: hostname filter (repeatable)
- `--method <METHOD>`: HTTP method filter (repeatable)
- `--status <STATUS>`: HTTP status filter (repeatable)
- `--url-regex <REGEX>`: URL regex filter (repeatable)
- `--fail-on <KIND>`: fail on any/added/changed/removed/new-errors/regression
- `--max-total-regression-ms <MS>` / `--max-ttfb-regression-ms <MS>`: timing gates
- `--max-response-size-increase <BYTES>` / `--max-new-errors <N>`: size/error gates
- `--ignore-query-param <NAME>`: ignore a query parameter while matching

### `request`
- `--format <curl|fetch|node-fetch|powershell>`: snippet format
- `--include-sensitive`: include sensitive headers (off by default)
- `--index <N>` / `--limit <N>`: select requests
- `--url-contains`, `--host`, `--method`, `--status`: request filters
- `<INPUT>`: HAR, database, or `-` for HAR stdin

### `query`
- `-f, --format <table|csv|json>`: output format
- `--limit <N>`: limit rows (wraps the query)
- `--offset <N>`: offset rows (wraps the query)
- `--quiet`: suppress extra output
- `<SQL> [DATABASE]`: query and optional db (default: only `*.db` in cwd)

### `search`
- `-f, --format <table|csv|json>`: output format
- `--limit <N>`: limit rows
- `--offset <N>`: offset rows
- `--quiet`: suppress extra output
- `<QUERY> [DATABASE]`: FTS query and optional db (default: only `*.db` in cwd)

### `fts-rebuild`
- `--tokenizer <unicode61|porter|trigram>`: tokenizer
- `--max-body-size <SIZE>`: max body size to index
- `--allow-external-paths`: allow reading external blob paths
- `--external-path-root <DIR>`: root dir for external blobs
- `<DATABASE>`: database to rebuild

## Schema
- Primary tables: `entries`, `blobs`, `pages`, `imports`.
- Full schema lives in `schema.sql`; `harlite schema` prints the live schema.

## Working with data
- The tool reads HAR files and writes SQLite `.db` files.
- Use `-` as a HAR input for streaming commands; pass `--output` when a command must derive a filename.
- Do not commit generated databases or large sample HAR files unless explicitly requested.
## Typical agent workflow
1) `harlite import session.har -o traffic.db`
2) `harlite info traffic.db`
3) `harlite query "<SQL>" traffic.db` or `harlite search "<fts query>" traffic.db`
4) (Optional) `harlite export traffic.db -o filtered.har`
5) (Optional) `harlite report traffic.db -o report.html`

## Example prompts for agents
- “Import this HAR, find all 4xx/5xx requests, and summarize the endpoints.”
- “Show top 20 slow requests and the average time per host.”
- “Search response bodies for ‘timeout’ and export those entries to a new HAR.”
