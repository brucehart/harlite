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

    /// Validation error for HAR data missing required fields.
    #[error("Invalid HAR file: {0}")]
    InvalidHar(String),

    /// URL parsing error for malformed URLs.
    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),
}

/// Convenience result type for harlite operations.
pub type Result<T> = std::result::Result<T, HarliteError>;
