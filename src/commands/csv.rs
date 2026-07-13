use std::io::Write;

use crate::error::Result;

/// Write one spreadsheet-safe CSV field.
///
/// RFC 4180 quoting alone does not prevent spreadsheet applications from
/// evaluating attacker-controlled cells as formulas. Prefix suspicious fields
/// with an apostrophe before applying normal CSV escaping.
pub(crate) fn write_csv_field(out: &mut impl Write, field: &str) -> Result<()> {
    let trimmed = field.trim_start_matches(|ch: char| ch.is_whitespace() || ch == '\u{feff}');
    let formula = matches!(trimmed.chars().next(), Some('=' | '+' | '-' | '@'));

    let must_quote = formula
        || field.contains(',')
        || field.contains('"')
        || field.contains('\n')
        || field.contains('\r');
    if must_quote {
        out.write_all(b"\"")?;
    }
    if formula {
        out.write_all(b"'")?;
    }
    for ch in field.chars() {
        if ch == '"' {
            out.write_all(b"\"\"")?;
        } else {
            write!(out, "{ch}")?;
        }
    }
    if must_quote {
        out.write_all(b"\"")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_csv_field;

    #[test]
    fn neutralizes_spreadsheet_formulas_after_whitespace() {
        for value in ["=1+1", "+cmd", "-cmd", "@SUM(A1:A2)", " \t=1+1"] {
            let mut out = Vec::new();
            write_csv_field(&mut out, value).unwrap();
            let rendered = String::from_utf8(out).unwrap();
            assert!(rendered.starts_with("\"'"), "{value:?}: {rendered:?}");
        }
    }

    #[test]
    fn escapes_quotes_and_delimiters() {
        let mut out = Vec::new();
        write_csv_field(&mut out, "a,\"b\"").unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "\"a,\"\"b\"\"\"");
    }
}
