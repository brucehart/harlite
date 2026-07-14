# Changelog

All notable changes to harlite are documented here. The project follows Semantic Versioning.

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
