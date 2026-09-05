# Changelog

All notable changes to harlite are documented here. The project follows Semantic Versioning.

## [0.5.0] - 2026-09-05

### Security

- Discard unscanned HAR extensions, cache metadata and duplicated initiator/redirect URLs from privacy outputs, with coverage information on stderr (#143).
- Reject export destinations that alias the input, including hard links and symlinks, and atomically publish completed export/report files (#140).
- Create secret-bearing staging files with Unix mode 0600 before copying data (#136).
- Inspect gzip/Brotli bodies before configured body redaction and PII matching, bound decompression, and keep changed body headers and sizes consistent (#145).
- Upgrade the Hyper/Tonic/OpenTelemetry stack to patched h2 0.4.19, addressing RUSTSEC-2026-0258 (#139).

### Fixed

- Serve available raw or decoded response bodies with matching compression headers, including externally stored bodies (#144).
- Preserve repeated HTTP header values through storage, HAR export, request snippets, mock serving, redaction and comparisons (#142).
- Retain available inline body data when merging an external placeholder with a complete blob; anchor external paths to their source database (#137).
- Prevent plugin pipe deadlocks by transferring stdin/stdout/stderr concurrently with configurable deadlines and output limits (#141).
- Accept bare relative output filenames in redaction and PII workflows (#133).
- Reject invalid captured HTTP status codes before starting the mock server (#135).
- Reject nonfinite and out-of-range replay rates without panicking (#134).
- Keep inspection inputs read-only, avoid creating missing databases, and migrate legacy schemas in private in-memory snapshots (#138).

### Compatibility notes

- Privacy outputs intentionally omit unscanned extension/cache metadata. Body redaction still requires configured patterns; PII detection retains its documented URL/body scope. Binary or unavailable bodies can remain uninspected, with warnings.
- Repeated header JSON values can now be arrays of strings. Single values remain strings, and legacy databases are supported. SQL consumers must handle either representation.
- Plugins default to a 30-second timeout and an 8 MiB output limit per stream. Configure `timeout_secs` and `max_output_bytes` when needed.
- Windows staging files retain inherited directory ACLs; mode 0600 protection applies on Unix. Windows plugin cleanup terminates the direct child; Unix cleanup also terminates its process group.
- Legacy database inspection may require memory for a complete snapshot. No source schema migration is performed by inspection.

## [0.4.0] - 2026-07-13

### Added

- Native release archives for Linux AMD64/ARM64, macOS AMD64/ARM64, and Windows AMD64.
- Keyless Sigstore-backed GitHub artifact attestations and SHA-256 checksums for release assets.
- `harlite check` for HAR semantic validation and SQLite/blob/FTS integrity checks.
- HAR-file support for `redact` and `pii`, including body and base64 response handling.
- `analyze` and `diff` failure budgets for CI performance and regression gates.
- `harlite request` exports captured requests as cURL, Fetch, node-fetch, or PowerShell snippets.
- HAR input from standard input (`-`) across compatible commands.

### Changed

- CI now enforces formatting, strict Clippy, all-feature tests, minimal-feature builds, MSRV compatibility, package verification, dependency auditing, and native Linux ARM64/macOS/Windows compilation.
- The CLI binary now uses the library crate directly instead of recompiling the implementation as duplicate modules.

[0.4.0]: https://github.com/brucehart/harlite/releases/tag/v0.4.0

[0.5.0]: https://github.com/brucehart/harlite/releases/tag/v0.5.0
