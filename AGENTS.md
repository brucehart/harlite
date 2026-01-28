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
- `imports` — list import metadata
- `prune` — remove a specific import by id
- `export` — export a database back to HAR
- `redact` — redact sensitive headers/cookies/params/bodies
- `diff` — compare two HAR files or two databases

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
- `<DATABASE>`: database to modify

### `stats`
- `--json`: JSON output
- `<DATABASE>`: database to inspect

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

### `redact`
- `-o, --output <OUTPUT>`: output database (default: in-place)
- `--force`: overwrite output db if it exists
- `--dry-run`: report only, no writes
- `--no-defaults`: disable default redaction patterns
- `--header <NAME>`: header name pattern (repeatable)
- `--cookie <NAME>`: cookie name pattern (repeatable)
- `--query-param <NAME>`: query param name pattern (repeatable)
- `--body-regex <REGEX>`: body regex pattern (repeatable)
- `--match <exact|wildcard|regex>`: pattern match mode
- `--token <TOKEN>`: replacement token (default: `REDACTED`)

### `diff`
- `<LEFT> <RIGHT>`: two HAR files or two databases to compare
- `-f, --format <table|csv|json>`: output format (table/JSON required)
- `--host <HOST>`: hostname filter (repeatable)
- `--method <METHOD>`: HTTP method filter (repeatable)
- `--status <STATUS>`: HTTP status filter (repeatable)
- `--url-regex <REGEX>`: URL regex filter (repeatable)

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
- Do not commit generated databases or large sample HAR files unless explicitly requested.
## Typical agent workflow
1) `harlite import session.har -o traffic.db`
2) `harlite info traffic.db`
3) `harlite query "<SQL>" traffic.db` or `harlite search "<fts query>" traffic.db`
4) (Optional) `harlite export traffic.db -o filtered.har`

## Example prompts for agents
- “Import this HAR, find all 4xx/5xx requests, and summarize the endpoints.”
- “Show top 20 slow requests and the average time per host.”
- “Search response bodies for ‘timeout’ and export those entries to a new HAR.”
