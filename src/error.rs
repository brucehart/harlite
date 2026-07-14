use thiserror::Error;

/// Errors that can occur while parsing or importing HAR files.
#[derive(Error, Debug)]
pub enum HarliteError {
    /// IO error (file not found, permission denied, etc.).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parsing error from invalid HAR content.
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    /// SQLite database error.
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    /// Parquet encoding error (only available with the parquet feature).
    #[cfg(feature = "parquet")]
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Validation error for HAR data missing required fields.
    #[error("Invalid HAR file: {0}")]
    InvalidHar(String),

    /// URL parsing error for malformed URLs.
    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),

    /// Regex compilation error.
    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),

    /// Timestamp parsing error.
    #[error("Timestamp parsing error: {0}")]
    TimeParse(#[from] chrono::ParseError),

    /// Invalid command-line arguments or options.
    #[error("{0}")]
    InvalidArgs(String),

    /// Input validation completed and found one or more issues.
    #[error("Validation failed with {0} issue(s)")]
    ValidationFailed(usize),

    /// A user-supplied automation or regression threshold was exceeded.
    #[error("Threshold exceeded: {0}")]
    ThresholdExceeded(String),
}

/// Convenience result type for harlite operations.
pub type Result<T> = std::result::Result<T, HarliteError>;
