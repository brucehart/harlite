use crate::error::{HarliteError, Result};

fn parse_size_bytes_u64(s: &str) -> Result<Option<u64>> {
    let raw = s.trim();
    if raw.is_empty() {
        return Err(HarliteError::InvalidArgs(
            "Size value cannot be empty".to_string(),
        ));
    }

    let lower = raw.to_lowercase();
    if lower == "unlimited" {
        return Ok(None);
    }

    let mut number_end = 0;
    for (idx, ch) in lower.char_indices() {
        if ch.is_ascii_digit() || ch == '.' {
            number_end = idx + ch.len_utf8();
        } else {
            break;
        }
    }

    let number_str = lower[..number_end].trim();
    if number_str.is_empty() {
        return Err(HarliteError::InvalidArgs(format!(
            "Invalid size value '{raw}'; expected a number like '1.5MB' or '100k'",
        )));
    }

    let unit_str = lower[number_end..].trim();
    if !unit_str.is_empty() && !unit_str.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(HarliteError::InvalidArgs(format!(
            "Invalid size value '{raw}'; expected a suffix like B, KB, MB, or GB",
        )));
    }

    let number: f64 = number_str.parse().map_err(|_| {
        HarliteError::InvalidArgs(format!(
            "Invalid size value '{raw}'; expected a number like '1.5MB'",
        ))
    })?;

    if !number.is_finite() || number < 0.0 {
        return Err(HarliteError::InvalidArgs(format!(
            "Invalid size value '{raw}'; size must be a positive number",
        )));
    }

    let multiplier = match unit_str {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        _ => {
            return Err(HarliteError::InvalidArgs(format!(
                "Invalid size unit '{unit_str}'; use B, KB, MB, GB, or 'unlimited'",
            )))
        }
    };

    let bytes = number * multiplier;
    if bytes > u64::MAX as f64 {
        return Err(HarliteError::InvalidArgs(format!(
            "Size value '{raw}' is too large",
        )));
    }

    Ok(Some(bytes.round() as u64))
}

pub fn parse_size_bytes_i64(s: &str) -> Result<Option<i64>> {
    match parse_size_bytes_u64(s)? {
        Some(value) => i64::try_from(value)
            .map(Some)
            .map_err(|_| HarliteError::InvalidArgs("Size value is too large".to_string())),
        None => Ok(None),
    }
}

pub fn parse_size_bytes_usize(s: &str) -> Result<Option<usize>> {
    match parse_size_bytes_u64(s)? {
        Some(value) => usize::try_from(value)
            .map(Some)
            .map_err(|_| HarliteError::InvalidArgs("Size value is too large".to_string())),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_size_bytes_u64;

    #[test]
    fn parses_decimal_and_short_units() {
        assert_eq!(parse_size_bytes_u64("1.5MB").unwrap(), Some(1_572_864));
        assert_eq!(parse_size_bytes_u64("1M").unwrap(), Some(1_048_576));
        assert_eq!(parse_size_bytes_u64("100k").unwrap(), Some(102_400));
        assert_eq!(parse_size_bytes_u64("500B").unwrap(), Some(500));
        assert_eq!(parse_size_bytes_u64("1.5 MB").unwrap(), Some(1_572_864));
    }

    #[test]
    fn handles_unlimited() {
        assert_eq!(parse_size_bytes_u64("unlimited").unwrap(), None);
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(parse_size_bytes_u64("").is_err());
        assert!(parse_size_bytes_u64("abc").is_err());
        assert!(parse_size_bytes_u64("1xb").is_err());
    }
}
