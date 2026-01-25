#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::Result;

#[derive(Debug, Deserialize)]
pub struct Har {
    pub log: Log,
}

#[derive(Debug, Deserialize)]
pub struct Log {
    pub version: Option<String>,
    pub creator: Option<Creator>,
    pub browser: Option<Browser>,
    pub pages: Option<Vec<Page>>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Deserialize)]
pub struct Creator {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct Browser {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub started_date_time: String,
    pub id: String,
    pub title: Option<String>,
    pub page_timings: Option<PageTimings>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageTimings {
    pub on_content_load: Option<f64>,
    pub on_load: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub pageref: Option<String>,
    pub started_date_time: String,
    pub time: f64,
    pub request: Request,
    pub response: Response,
    pub cache: Option<serde_json::Value>,
    pub timings: Option<Timings>,
    pub server_ip_address: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub method: String,
    pub url: String,
    pub http_version: String,
    pub cookies: Option<Vec<Cookie>>,
    pub headers: Vec<Header>,
    pub query_string: Option<Vec<QueryParam>>,
    pub post_data: Option<PostData>,
    pub headers_size: Option<i64>,
    pub body_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub status: i32,
    pub status_text: String,
    pub http_version: String,
    pub cookies: Option<Vec<Cookie>>,
    pub headers: Vec<Header>,
    pub content: Content,
    pub redirect_url: Option<String>,
    pub headers_size: Option<i64>,
    pub body_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    pub size: i64,
    pub compression: Option<i64>,
    pub mime_type: Option<String>,
    pub text: Option<String>,
    pub encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub path: Option<String>,
    pub domain: Option<String>,
    pub expires: Option<String>,
    pub http_only: Option<bool>,
    pub secure: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct QueryParam {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostData {
    pub mime_type: Option<String>,
    pub text: Option<String>,
    pub params: Option<Vec<PostParam>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostParam {
    pub name: String,
    pub value: Option<String>,
    pub file_name: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Timings {
    pub blocked: Option<f64>,
    pub dns: Option<f64>,
    pub connect: Option<f64>,
    pub send: f64,
    pub wait: f64,
    pub receive: f64,
    pub ssl: Option<f64>,
}

/// Parse a HAR file from disk into strongly typed structures.
pub fn parse_har_file(path: &Path) -> Result<Har> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let har: Har = serde_json::from_reader(reader)?;
    Ok(har)
}

#[cfg(test)]
mod tests {
    use super::Har;

    #[test]
    fn parses_minimal_har() {
        let json = r#"
        {
          "log": {
            "entries": [
              {
                "startedDateTime": "2024-01-15T10:30:00.000Z",
                "time": 150.5,
                "request": {
                  "method": "GET",
                  "url": "https://example.com/",
                  "httpVersion": "HTTP/1.1",
                  "headers": []
                },
                "response": {
                  "status": 200,
                  "statusText": "OK",
                  "httpVersion": "HTTP/1.1",
                  "headers": [],
                  "content": {
                    "size": 0
                  }
                }
              }
            ]
          }
        }
        "#;

        let har: Har = serde_json::from_str(json).expect("HAR should parse");
        assert_eq!(har.log.entries.len(), 1);
        assert_eq!(har.log.entries[0].request.method, "GET");
    }
}
