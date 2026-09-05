use super::Header;
use std::collections::HashMap;

/// Existing single-value header objects remain valid. Repeated names use an
/// ordered array of strings so no header occurrence is lost on import/export.
pub(crate) fn headers_from_json(json: Option<&str>) -> Vec<Header> {
    let Some(serde_json::Value::Object(values)) = json.and_then(|s| serde_json::from_str(s).ok())
    else {
        return Vec::new();
    };
    let mut headers = Vec::new();
    for (name, value) in values {
        let values = match value {
            serde_json::Value::Array(values) => values,
            value => vec![value],
        };
        for value in values {
            if let Some(value) = value.as_str() {
                headers.push(Header {
                    name: name.clone(),
                    value: value.into(),
                });
            }
        }
    }
    headers
}

/// Compatibility lookup for analyses that only use one value per field.
pub(crate) fn header_lookup(json: Option<&str>) -> HashMap<String, String> {
    headers_from_json(json)
        .into_iter()
        .map(|h| (h.name.to_ascii_lowercase(), h.value))
        .collect()
}
