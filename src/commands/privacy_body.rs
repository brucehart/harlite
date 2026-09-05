use crate::error::Result;
use crate::har::Header;

pub(crate) fn encoding(headers: &[Header]) -> Option<String> {
    let values: Vec<_> = headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case("content-encoding"))
        .map(|h| h.value.as_str())
        .collect();
    (!values.is_empty()).then(|| values.join(","))
}

pub(crate) fn encoding_json(json: Option<&str>) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json?).ok()?;
    let values: Vec<_> = value
        .as_object()?
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-encoding"))
        .flat_map(|(_, value)| match value {
            serde_json::Value::Array(values) => {
                values.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()
            }
            value => value.as_str().into_iter().collect(),
        })
        .collect();
    (!values.is_empty()).then(|| values.join(","))
}

pub(crate) fn decode_text(bytes: &[u8], encoding: Option<&str>) -> Result<Option<String>> {
    let decoded = super::body_codec::decode_captured(bytes, encoding, 50 * 1024 * 1024)?;
    match String::from_utf8(decoded.bytes) {
        Ok(text) => Ok(Some(text)),
        Err(_) => {
            let kind = if decoded.encoded {
                "decoded compressed"
            } else {
                "binary"
            };
            eprintln!(
                "Warning: {kind} body was not inspected as UTF-8; original bytes are retained."
            );
            Ok(None)
        }
    }
}

pub(crate) fn clear_headers(headers: &mut Vec<Header>) {
    headers.retain(|h| !is_transport_header(&h.name));
}

fn is_transport_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-encoding") || name.eq_ignore_ascii_case("content-length")
}

pub(crate) fn clear_headers_json(json: Option<&str>) -> Result<Option<String>> {
    let Some(json) = json else {
        return Ok(None);
    };
    let mut value: serde_json::Value = serde_json::from_str(json)?;
    if let Some(headers) = value.as_object_mut() {
        headers.retain(|name, _| !is_transport_header(name));
    }
    Ok(Some(serde_json::to_string(&value)?))
}
