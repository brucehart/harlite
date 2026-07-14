use std::process;

fn main() {
    if let Err(e) = harlite::run_cli() {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
