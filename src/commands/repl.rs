use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};

use crate::error::{HarliteError, Result};

use super::query::{execute_query, open_readonly_connection, OutputFormat, QueryOptions};
use super::util::resolve_database;

pub struct ReplOptions {
    pub format: OutputFormat,
}

pub fn run_repl(database: Option<PathBuf>, options: &ReplOptions) -> Result<()> {
    let database = resolve_database(database)?;
    let conn = open_readonly_connection(&database)?;

    let schema = SchemaCache::load(&conn)?;
    let helper = ReplHelper::new(schema);

    let mut rl: Editor<ReplHelper, DefaultHistory> = Editor::new()
        .map_err(|e| HarliteError::InvalidArgs(format!("Failed to start REPL: {e}")))?;
    rl.set_helper(Some(helper));

    if let Some(path) = history_path() {
        let _ = rl.load_history(&path);
    }

    print_intro(&database, options.format);

    let mut format = options.format;
    loop {
        let prompt = format!("harlite({})> ", format.as_str());
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(input);

                if input.starts_with('.') {
                    match handle_meta_command(input, &conn, &mut format) {
                        Ok(ReplControl::Continue) => continue,
                        Ok(ReplControl::Exit) => break,
                        Err(e) => {
                            eprintln!("Error: {e}");
                            continue;
                        }
                    }
                }

                let options = QueryOptions {
                    format,
                    limit: None,
                    offset: None,
                    quiet: false,
                };
                if let Err(e) = execute_query(&conn, input, &options) {
                    eprintln!("Error: {e}");
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                return Err(HarliteError::InvalidArgs(format!("REPL error: {err}")));
            }
        }
    }

    if let Some(path) = history_path() {
        let _ = rl.append_history(&path);
    }

    Ok(())
}

enum ReplControl {
    Continue,
    Exit,
}

fn handle_meta_command(
    input: &str,
    conn: &Connection,
    format: &mut OutputFormat,
) -> Result<ReplControl> {
    let mut parts = input.split_whitespace();
    let cmd = parts.next().unwrap_or("");

    match cmd {
        ".exit" | ".quit" | ".q" => Ok(ReplControl::Exit),
        ".help" => {
            print_help();
            Ok(ReplControl::Continue)
        }
        ".mode" => {
            if let Some(mode) = parts.next() {
                *format = parse_output_format(mode)?;
                println!("Mode set to {}", format.as_str());
            } else {
                println!("Current mode: {}", format.as_str());
            }
            Ok(ReplControl::Continue)
        }
        ".slow" => {
            let limit =
                parts.next().unwrap_or("20").parse::<u64>().map_err(|_| {
                    HarliteError::InvalidArgs("Invalid limit for .slow".to_string())
                })?;
            let sql = format!(
                "SELECT started_at, time_ms, status, method, url FROM entries ORDER BY time_ms DESC LIMIT {}",
                limit
            );
            run_shortcut_query(conn, *format, &sql)?;
            Ok(ReplControl::Continue)
        }
        ".status" => {
            let sql =
                "SELECT status, COUNT(*) AS count FROM entries GROUP BY status ORDER BY count DESC";
            run_shortcut_query(conn, *format, sql)?;
            Ok(ReplControl::Continue)
        }
        ".tables" => {
            let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
            run_shortcut_query(conn, *format, sql)?;
            Ok(ReplControl::Continue)
        }
        _ => Err(HarliteError::InvalidArgs(format!(
            "Unknown command '{cmd}'. Try .help"
        ))),
    }
}

fn run_shortcut_query(conn: &Connection, format: OutputFormat, sql: &str) -> Result<()> {
    let options = QueryOptions {
        format,
        limit: None,
        offset: None,
        quiet: false,
    };
    execute_query(conn, sql, &options)
}

fn parse_output_format(value: &str) -> Result<OutputFormat> {
    match value.to_ascii_lowercase().as_str() {
        "table" => Ok(OutputFormat::Table),
        "csv" => Ok(OutputFormat::Csv),
        "json" => Ok(OutputFormat::Json),
        _ => Err(HarliteError::InvalidArgs(
            "Mode must be one of: table, csv, json".to_string(),
        )),
    }
}

fn print_intro(database: &Path, format: OutputFormat) {
    let db_name = database
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("database");
    println!(
        "harlite repl connected to {db_name}. Mode: {}. Type .help for commands.",
        format.as_str()
    );
}

fn print_help() {
    println!("Available commands:");
    println!("  .help                 Show this help");
    println!("  .mode [table|csv|json] Set or show output mode");
    println!("  .slow [N]              Show top N slowest requests (default 20)");
    println!("  .status                Show HTTP status code counts");
    println!("  .tables                List tables");
    println!("  .exit, .quit, .q        Exit the REPL");
    println!("You can also enter any read-only SQL statement.");
}

fn history_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)?;
    Some(home.join(".harlite_repl_history"))
}

struct SchemaCache {
    tables: Vec<String>,
    columns: Vec<String>,
    table_columns: HashMap<String, Vec<String>>,
}

impl SchemaCache {
    fn load(conn: &Connection) -> Result<Self> {
        let mut tables = Vec::new();
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for table in rows {
            tables.push(table?);
        }

        let mut table_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut column_set: BTreeSet<String> = BTreeSet::new();
        for table in &tables {
            let cols = load_table_columns(conn, table)?;
            for col in &cols {
                column_set.insert(col.clone());
            }
            table_columns.insert(table.clone(), cols);
        }

        Ok(Self {
            tables,
            columns: column_set.into_iter().collect(),
            table_columns,
        })
    }
}

fn load_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    let safe_table = table.replace('\'', "''");
    let mut stmt = conn.prepare(&format!("PRAGMA table_info('{safe_table}')"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for column in rows {
        columns.push(column?);
    }
    Ok(columns)
}

struct ReplHelper {
    keywords: Vec<String>,
    schema: SchemaCache,
}

impl ReplHelper {
    fn new(schema: SchemaCache) -> Self {
        Self {
            keywords: sql_keywords(),
            schema,
        }
    }
}

impl Helper for ReplHelper {}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let (start, prefix) = current_word(line, pos);
        if prefix.is_empty() {
            return Ok((pos, Vec::new()));
        }

        let mut matches = Vec::new();
        if let Some((table, col_prefix)) = table_prefix(&prefix, &self.schema.tables) {
            if let Some(columns) = self.schema.table_columns.get(table) {
                for col in columns {
                    if starts_with_case_insensitive(col, col_prefix) {
                        let replacement = format!("{}.{}", table, col);
                        matches.push(Pair {
                            display: replacement.clone(),
                            replacement,
                        });
                    }
                }
            }
        } else {
            let mut candidates: Vec<&String> = Vec::new();
            candidates.extend(self.schema.tables.iter());
            candidates.extend(self.schema.columns.iter());
            candidates.extend(self.keywords.iter());

            for candidate in candidates {
                if starts_with_case_insensitive(candidate, &prefix) {
                    matches.push(Pair {
                        display: candidate.clone(),
                        replacement: candidate.clone(),
                    });
                }
            }
        }

        Ok((start, matches))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;
}

impl Highlighter for ReplHelper {}
impl Validator for ReplHelper {}

fn current_word(line: &str, pos: usize) -> (usize, String) {
    let mut start = pos;
    for (idx, ch) in line[..pos].char_indices().rev() {
        if is_word_char(ch) {
            start = idx;
        } else {
            break;
        }
    }
    (start, line[start..pos].to_string())
}

fn is_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}

fn table_prefix<'a>(prefix: &'a str, tables: &'a [String]) -> Option<(&'a str, &'a str)> {
    let (table, col_prefix) = prefix.rsplit_once('.')?;
    if table.is_empty() {
        return None;
    }
    if tables.iter().any(|t| t == table) {
        Some((table, col_prefix))
    } else {
        None
    }
}

fn starts_with_case_insensitive(value: &str, prefix: &str) -> bool {
    value
        .to_ascii_lowercase()
        .starts_with(&prefix.to_ascii_lowercase())
}

fn sql_keywords() -> Vec<String> {
    vec![
        "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "OFFSET", "JOIN", "LEFT",
        "RIGHT", "INNER", "OUTER", "ON", "AND", "OR", "NOT", "AS", "DISTINCT", "COUNT", "AVG",
        "MIN", "MAX", "SUM", "LIKE", "GLOB", "IN", "IS", "NULL", "CASE", "WHEN", "THEN", "ELSE",
        "END", "PRAGMA",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect()
}
