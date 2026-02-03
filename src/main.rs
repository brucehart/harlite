use clap::Parser;
use std::process;

mod cli;
mod commands;
mod config;
mod db;
mod error;
mod graphql;
mod har;
mod plugins;
mod router;
mod size;
use crate::cli::Cli;

fn main() {
    let cli = Cli::parse();

    let result = crate::router::run(cli);

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
