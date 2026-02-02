use std::collections::{HashMap, HashSet};

use graphql_parser::query::{parse_query, Definition, OperationDefinition, Selection, SelectionSet};
use serde_json::Value;
use url::Url;

use crate::har::{Entry, Header, PostData, QueryParam};

#[derive(Debug, Clone, Default)]
pub struct GraphQLInfo {
    pub operation_type: Option<String>,
    pub operation_name: Option<String>,
    pub top_level_fields: Vec<String>,
}

#[derive(Default)]
struct GraphQLPayload {
    query: Option<String>,
    operation_name: Option<String>,
    detected: bool,
}

pub fn extract_graphql_info(entry: &Entry) -> Option<GraphQLInfo> {
    let request = &entry.request;
    let content_type = header_value(&request.headers, "content-type")
        .or_else(|| request.post_data.as_ref().and_then(|post| post.mime_type.clone()));

    let mut payload = GraphQLPayload::default();
    if is_graphql_content_type(content_type.as_deref()) {
        payload.detected = true;
    }
    if url_path_contains_graphql(&request.url) {
        payload.detected = true;
    }

    if let Some(params) = &request.query_string {
        apply_query_params(params, &mut payload);
    } else {
        apply_query_pairs(params_from_url(&request.url), &mut payload);
    }

    if let Some(post_data) = &request.post_data {
        apply_post_data(post_data, content_type.as_deref(), &mut payload);
    }

    if payload
        .query
        .as_ref()
        .is_some_and(|query| !query.trim().is_empty())
    {
        payload.detected = true;
    }

    if !payload.detected {
        return None;
    }

    let mut info = GraphQLInfo {
        operation_type: None,
        operation_name: payload.operation_name.clone(),
        top_level_fields: Vec::new(),
    };

    if let Some(query) = payload.query.as_deref() {
        if let Some(parsed) = parse_graphql_query(query, payload.operation_name.as_deref()) {
            info.operation_type = parsed.operation_type;
            info.operation_name = parsed.operation_name.or(info.operation_name);
            info.top_level_fields = parsed.top_level_fields;
        }
    }

    Some(info)
}

fn apply_query_params(params: &[QueryParam], payload: &mut GraphQLPayload) {
    for param in params {
        apply_param(&param.name, &param.value, payload);
    }
}

fn apply_post_data(post_data: &PostData, content_type: Option<&str>, payload: &mut GraphQLPayload) {
    if let Some(params) = &post_data.params {
        for param in params {
            if let Some(value) = &param.value {
                apply_param(&param.name, value, payload);
            }
        }
    }

    if let Some(text) = &post_data.text {
        let mime = post_data.mime_type.as_deref().or(content_type);
        if is_graphql_content_type(mime) {
            if !text.trim().is_empty() {
                payload.query = Some(text.to_string());
            }
            payload.detected = true;
            return;
        }

        if is_form_urlencoded(mime) {
            apply_query_pairs(parse_urlencoded_pairs(text), payload);
            return;
        }

        if is_json_mime(mime) || text.trim_start().starts_with('{') || text.trim_start().starts_with('[')
        {
            apply_json_payload(text, payload);
        }
    }
}

fn apply_param(name: &str, value: &str, payload: &mut GraphQLPayload) {
    let key = name.trim().to_ascii_lowercase();
    match key.as_str() {
        "query" => {
            if !value.trim().is_empty() {
                payload.query = Some(value.to_string());
            }
            payload.detected = true;
        }
        "operationname" | "operation_name" => {
            if !value.trim().is_empty() {
                payload.operation_name = Some(value.to_string());
            }
            payload.detected = true;
        }
        "extensions" => {
            if let Ok(value) = serde_json::from_str::<Value>(value) {
                apply_graphql_json(&value, payload);
            }
        }
        "persistedquery" | "sha256hash" => {
            payload.detected = true;
        }
        _ => {}
    }
}

fn apply_query_pairs(pairs: Vec<(String, String)>, payload: &mut GraphQLPayload) {
    for (name, value) in pairs {
        apply_param(&name, &value, payload);
    }
}

fn apply_json_payload(text: &str, payload: &mut GraphQLPayload) {
    let parsed = serde_json::from_str::<Value>(text).ok();
    let Some(value) = parsed else {
        return;
    };

    match value {
        Value::Array(items) => {
            if let Some(first) = items.first() {
                apply_graphql_json(first, payload);
            }
        }
        _ => apply_graphql_json(&value, payload),
    }
}

fn apply_graphql_json(value: &Value, payload: &mut GraphQLPayload) {
    if let Some(query) = value.get("query").and_then(Value::as_str) {
        if !query.trim().is_empty() {
            payload.query = Some(query.to_string());
        }
    }
    if let Some(name) = value.get("operationName").and_then(Value::as_str) {
        if !name.trim().is_empty() {
            payload.operation_name = Some(name.to_string());
        }
    }
    if value.get("extensions").and_then(|ext| ext.get("persistedQuery")).is_some() {
        payload.detected = true;
    }
    if payload.query.is_some() || payload.operation_name.is_some() {
        payload.detected = true;
    }
}

fn parse_graphql_query(query: &str, operation_name: Option<&str>) -> Option<GraphQLInfo> {
    let document = parse_query::<String>(query).ok()?;

    let mut fragments: HashMap<String, SelectionSet<'_, String>> = HashMap::new();
    let mut operations: Vec<OperationDefinition<'_, String>> = Vec::new();

    for def in document.definitions {
        match def {
            Definition::Operation(op) => operations.push(op),
            Definition::Fragment(fragment) => {
                fragments.insert(fragment.name, fragment.selection_set);
            }
        }
    }

    if operations.is_empty() {
        return None;
    }

    let selected = if let Some(name) = operation_name {
        operations
            .iter()
            .find(|op| op_name(op).is_some_and(|n| n == name))
            .or_else(|| operations.first())
    } else if operations.len() == 1 {
        operations.first()
    } else {
        operations
            .iter()
            .find(|op| op_name(op).is_none())
            .or_else(|| operations.first())
    }?;

    let op_type = operation_type(selected).to_string();
    let op_name = op_name(selected).cloned();
    let fields = collect_top_level_fields(op_selection_set(selected), &fragments);

    Some(GraphQLInfo {
        operation_type: Some(op_type),
        operation_name: op_name,
        top_level_fields: fields,
    })
}

fn op_name<'a>(op: &'a OperationDefinition<'a, String>) -> Option<&'a String> {
    match op {
        OperationDefinition::Query(q) => q.name.as_ref(),
        OperationDefinition::Mutation(m) => m.name.as_ref(),
        OperationDefinition::Subscription(s) => s.name.as_ref(),
        OperationDefinition::SelectionSet(_) => None,
    }
}

fn op_selection_set<'a>(op: &'a OperationDefinition<'a, String>) -> &'a SelectionSet<'a, String> {
    match op {
        OperationDefinition::Query(q) => &q.selection_set,
        OperationDefinition::Mutation(m) => &m.selection_set,
        OperationDefinition::Subscription(s) => &s.selection_set,
        OperationDefinition::SelectionSet(set) => set,
    }
}

fn operation_type(op: &OperationDefinition<'_, String>) -> &'static str {
    match op {
        OperationDefinition::Query(_) | OperationDefinition::SelectionSet(_) => "query",
        OperationDefinition::Mutation(_) => "mutation",
        OperationDefinition::Subscription(_) => "subscription",
    }
}

fn collect_top_level_fields<'a>(
    selection_set: &'a SelectionSet<'a, String>,
    fragments: &'a HashMap<String, SelectionSet<'a, String>>,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    collect_fields(selection_set, fragments, &mut out, &mut seen);
    out
}

fn collect_fields<'a>(
    selection_set: &'a SelectionSet<'a, String>,
    fragments: &'a HashMap<String, SelectionSet<'a, String>>,
    out: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for selection in &selection_set.items {
        match selection {
            Selection::Field(field) => {
                if seen.insert(field.name.clone()) {
                    out.push(field.name.clone());
                }
            }
            Selection::InlineFragment(fragment) => {
                collect_fields(&fragment.selection_set, fragments, out, seen);
            }
            Selection::FragmentSpread(spread) => {
                if let Some(fragment) = fragments.get(&spread.fragment_name) {
                    collect_fields(fragment, fragments, out, seen);
                }
            }
        }
    }
}

fn is_graphql_content_type(mime: Option<&str>) -> bool {
    mime.is_some_and(|m| m.to_ascii_lowercase().contains("graphql"))
}

fn is_json_mime(mime: Option<&str>) -> bool {
    mime.is_some_and(|m| m.to_ascii_lowercase().contains("json"))
}

fn is_form_urlencoded(mime: Option<&str>) -> bool {
    mime.is_some_and(|m| m.to_ascii_lowercase().contains("x-www-form-urlencoded"))
}

fn url_path_contains_graphql(url: &str) -> bool {
    if let Ok(parsed) = Url::parse(url) {
        let path = parsed.path().to_ascii_lowercase();
        return path.contains("graphql") || path.contains("gql");
    }
    false
}

fn params_from_url(url: &str) -> Vec<(String, String)> {
    Url::parse(url)
        .ok()
        .map(|u| {
            u.query_pairs()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_urlencoded_pairs(text: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(text.as_bytes())
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn header_value(headers: &[Header], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::extract_graphql_info;
    use crate::har::Har;
    use serde_json::json;

    fn load_entry(value: serde_json::Value) -> crate::har::Entry {
        let har: Har = serde_json::from_value(value).expect("parse har");
        har.log.entries.into_iter().next().expect("entry")
    }

    #[test]
    fn extracts_graphql_from_json_body() {
        let entry = load_entry(json!({
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "startedDateTime": "2024-01-15T10:30:00.000Z",
                        "time": 123.0,
                        "request": {
                            "method": "POST",
                            "url": "https://example.com/graphql",
                            "httpVersion": "HTTP/1.1",
                            "headers": [{"name": "Content-Type", "value": "application/json"}],
                            "cookies": [],
                            "postData": {
                                "mimeType": "application/json",
                                "text": "{\"query\":\"query GetUser { viewer { login } }\",\"operationName\":\"GetUser\"}"
                            },
                            "headersSize": 0,
                            "bodySize": 0
                        },
                        "response": {
                            "status": 200,
                            "statusText": "OK",
                            "httpVersion": "HTTP/1.1",
                            "headers": [],
                            "content": {"size": 0, "mimeType": "application/json"},
                            "headersSize": 0,
                            "bodySize": 0
                        }
                    }
                ]
            }
        }));

        let info = extract_graphql_info(&entry).expect("graphql info");
        assert_eq!(info.operation_type.as_deref(), Some("query"));
        assert_eq!(info.operation_name.as_deref(), Some("GetUser"));
        assert_eq!(info.top_level_fields, vec!["viewer".to_string()]);
    }

    #[test]
    fn detects_persisted_query_without_text() {
        let entry = load_entry(json!({
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "startedDateTime": "2024-01-15T10:30:00.000Z",
                        "time": 123.0,
                        "request": {
                            "method": "POST",
                            "url": "https://example.com/graphql",
                            "httpVersion": "HTTP/1.1",
                            "headers": [{"name": "Content-Type", "value": "application/json"}],
                            "cookies": [],
                            "postData": {
                                "mimeType": "application/json",
                                "text": "{\"operationName\":\"PersistedUser\",\"extensions\":{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"abc\"}}}"
                            },
                            "headersSize": 0,
                            "bodySize": 0
                        },
                        "response": {
                            "status": 200,
                            "statusText": "OK",
                            "httpVersion": "HTTP/1.1",
                            "headers": [],
                            "content": {"size": 0, "mimeType": "application/json"},
                            "headersSize": 0,
                            "bodySize": 0
                        }
                    }
                ]
            }
        }));

        let info = extract_graphql_info(&entry).expect("graphql info");
        assert_eq!(info.operation_name.as_deref(), Some("PersistedUser"));
        assert!(info.operation_type.is_none());
        assert!(info.top_level_fields.is_empty());
    }

    #[test]
    fn extracts_graphql_from_query_params() {
        let entry = load_entry(json!({
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "startedDateTime": "2024-01-15T10:30:00.000Z",
                        "time": 123.0,
                        "request": {
                            "method": "GET",
                            "url": "https://example.com/graphql?query=query%20Foo%20%7B%20viewer%20%7D&operationName=Foo",
                            "httpVersion": "HTTP/1.1",
                            "headers": [{"name": "Accept", "value": "application/json"}],
                            "cookies": [],
                            "queryString": [
                                {"name": "query", "value": "query Foo { viewer }"},
                                {"name": "operationName", "value": "Foo"}
                            ],
                            "headersSize": 0,
                            "bodySize": 0
                        },
                        "response": {
                            "status": 200,
                            "statusText": "OK",
                            "httpVersion": "HTTP/1.1",
                            "headers": [],
                            "content": {"size": 0, "mimeType": "application/json"},
                            "headersSize": 0,
                            "bodySize": 0
                        }
                    }
                ]
            }
        }));

        let info = extract_graphql_info(&entry).expect("graphql info");
        assert_eq!(info.operation_type.as_deref(), Some("query"));
        assert_eq!(info.operation_name.as_deref(), Some("Foo"));
        assert_eq!(info.top_level_fields, vec!["viewer".to_string()]);
    }
}
