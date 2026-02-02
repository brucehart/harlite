# Plugin Developer Guide

This guide explains how to build external plugins for `harlite`.

## Overview

Plugins are **separate executables** (any language) that communicate with `harlite` via JSON over stdin/stdout. This keeps plugin execution isolated and predictable.

`harlite` supports three plugin kinds:

- `filter` — decide whether an entry should be included
- `transform` — modify entries during import/export
- `exporter` — consume the final HAR and optionally skip the default export

Plugins can run during import, export, or both (controlled by `phase`).

## Configuring plugins

Add plugin definitions to your `harlite.toml` (or `.harliterc`):

```toml
[[plugins]]
name = "sample-filter"
kind = "filter"
command = "plugins/sample_filter.py"
phase = "import"
enabled = true

[[plugins]]
name = "sample-transform"
kind = "transform"
command = "plugins/normalize_urls.sh"
phase = "both"

[[plugins]]
name = "sample-exporter"
kind = "exporter"
command = "plugins/custom_exporter"
phase = "export"
```

Notes:
- `name` must be unique.
- `command` can be absolute or relative to the current working directory.
- `enabled = false` disables the plugin by default.

## Enabling/disabling per run

You can override config at runtime:

```bash
harlite import capture.har --plugin sample-filter
harlite import capture.har --disable-plugin sample-filter

harlite export traffic.db --plugin sample-exporter
harlite watch ./captures --plugin sample-filter
```

If a plugin name is unknown, `harlite` exits with an error.

## Plugin API (v1)

All requests include:
- `api_version`: currently `"v1"`
- `event`: `filter_entry`, `transform_entry`, or `export`
- `phase`: `import`, `export`, or `both`
- `context`: metadata about the run
- `entry` or `har`: the payload

### Context object

```json
{
  "command": "import",
  "source": "/path/capture.har",
  "database": "/path/output.db",
  "output": null
}
```

Fields:
- `command`: `import` or `export`
- `source`: input HAR path (import), else null
- `database`: output DB (import) or input DB (export)
- `output`: output HAR path (export) or null

### Filter plugins

Request:

```json
{
  "api_version": "v1",
  "event": "filter_entry",
  "phase": "import",
  "context": { "command": "import", "source": "capture.har", "database": "traffic.db", "output": null },
  "entry": { ... }
}
```

Response:

```json
{ "allow": true }
```

If `allow` is `false`, the entry is skipped.

### Transform plugins

Request:

```json
{
  "api_version": "v1",
  "event": "transform_entry",
  "phase": "export",
  "context": { "command": "export", "source": null, "database": "traffic.db", "output": "traffic.har" },
  "entry": { ... }
}
```

Response:

```json
{ "entry": { ... } }
```

Returning `null` for `entry` leaves the entry unchanged.

### Exporter plugins

Request:

```json
{
  "api_version": "v1",
  "event": "export",
  "phase": "export",
  "context": { "command": "export", "source": null, "database": "traffic.db", "output": "traffic.har" },
  "har": { ... }
}
```

Response:

```json
{ "skip_default": true }
```

If `skip_default` is `true`, `harlite` will not write the default HAR output (your plugin is responsible for writing output elsewhere).

## Data structures

The `entry` and `har` payloads follow the HAR 1.2 format used by `harlite`.

Common locations:
- `entry.request.url`
- `entry.response.status`
- `entry.response.content.text`
- `har.log.entries`

Extensions are preserved and available under `extensions` fields (for example `entry.extensions`, `entry.request.extensions`, `entry.response.extensions`).

## Error handling

- If the plugin exits non-zero, `harlite` fails the command.
- If stdout is empty or invalid JSON, `harlite` fails the command.
- For filter plugins, missing `allow` is an error.

Use stderr for debug logs; `harlite` does not parse stderr.

## Performance guidance

- Keep plugins fast; they run per-entry for filters/transforms.
- Avoid heavy startup costs (preload state or reuse a persistent runtime if possible).
- Limit output to a single JSON object.

## Security considerations

Plugins run as separate processes with the same permissions as `harlite`. Treat plugin code as trusted.

## Sample plugins

See `plugins/sample_filter.py` and `plugins/sample_exporter.py` for minimal examples.
