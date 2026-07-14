//! Harlite library crate.
//!
//! The stable, supported API surface is exposed via [`crate::api`] and
//! [`crate::prelude`]. These modules are intended for embedding harlite in
//! Rust applications and follow SemVer.
//!
//! Other modules are used by the CLI implementation and may change more
//! frequently. If you need something not in [`crate::api`], consider opening
//! an issue so it can be promoted to the supported surface.

pub mod api;
pub mod prelude;

pub mod commands;
pub mod db;
pub mod error;
pub mod graphql;
pub mod har;
pub mod plugins;
pub mod size;

mod cli;
mod config;
mod router;

/// Parse command-line arguments and run the harlite CLI.
///
/// This is primarily used by the packaged binary. Embedders should prefer the
/// stable functions and option types exposed through [`crate::api`].
pub fn run_cli() -> error::Result<()> {
    use clap::Parser;

    router::run(cli::Cli::parse())
}
