#![allow(dead_code)]

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::Result;

#[derive(Debug, Serialize)]
pub struct Har {
    pub log: Log,
}

#[derive(Debug, Serialize)]
pub struct Log {
    pub version: Option<String>,
    pub creator: Option<Creator>,
    pub browser: Option<Browser>,
    pub pages: Option<Vec<Page>>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Creator {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Browser {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub started_date_time: String,
    pub id: String,
    pub title: Option<String>,
    pub page_timings: Option<PageTimings>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageTimings {
    pub on_content_load: Option<f64>,
    pub on_load: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub pageref: Option<String>,
    pub started_date_time: String,
    pub time: f64,
    pub request: Request,
    pub response: Response,
    pub cache: Option<serde_json::Value>,
    pub timings: Option<Timings>,
    #[serde(rename = "serverIPAddress", alias = "serverIpAddress")]
    pub server_ip_address: Option<String>,
    pub connection: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub status: i32,
    pub status_text: String,
    pub http_version: String,
    pub cookies: Option<Vec<Cookie>>,
    pub headers: Vec<Header>,
    pub content: Content,
    #[serde(rename = "redirectURL", alias = "redirectUrl")]
    pub redirect_url: Option<String>,
    pub headers_size: Option<i64>,
    pub body_size: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    pub size: i64,
    pub compression: Option<i64>,
    pub mime_type: Option<String>,
    pub text: Option<String>,
    pub encoding: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryParam {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostData {
    pub mime_type: Option<String>,
    pub text: Option<String>,
    pub params: Option<Vec<PostParam>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostParam {
    pub name: String,
    pub value: Option<String>,
    pub file_name: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
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
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let har = Har::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(har)
}

impl<'de> Deserialize<'de> for Har {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct HarVisitor;

        impl<'de> Visitor<'de> for HarVisitor {
            type Value = Har;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a HAR object with a log field")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Har, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut log: Option<Log> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "log" => {
                            if log.is_some() {
                                return Err(de::Error::duplicate_field("log"));
                            }
                            log = Some(map.next_value()?);
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let log = log.ok_or_else(|| de::Error::missing_field("log"))?;
                Ok(Har { log })
            }
        }

        deserializer.deserialize_map(HarVisitor)
    }
}

impl<'de> Deserialize<'de> for Log {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LogVisitor;

        impl<'de> Visitor<'de> for LogVisitor {
            type Value = Log;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a HAR log object")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Log, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut version: Option<String> = None;
                let mut creator: Option<Creator> = None;
                let mut browser: Option<Browser> = None;
                let mut pages: Option<Vec<Page>> = None;
                let mut seen_version = false;
                let mut seen_creator = false;
                let mut seen_browser = false;
                let mut seen_pages = false;
                let mut entries: Option<Vec<Entry>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "version" => {
                            if seen_version {
                                return Err(de::Error::duplicate_field("version"));
                            }
                            seen_version = true;
                            version = map.next_value::<Option<String>>()?;
                        }
                        "creator" => {
                            if seen_creator {
                                return Err(de::Error::duplicate_field("creator"));
                            }
                            seen_creator = true;
                            creator = map.next_value::<Option<Creator>>()?;
                        }
                        "browser" => {
                            if seen_browser {
                                return Err(de::Error::duplicate_field("browser"));
                            }
                            seen_browser = true;
                            browser = map.next_value::<Option<Browser>>()?;
                        }
                        "pages" => {
                            if seen_pages {
                                return Err(de::Error::duplicate_field("pages"));
                            }
                            seen_pages = true;
                            pages = map.next_value::<Option<Vec<Page>>>()?;
                        }
                        "entries" => {
                            if entries.is_some() {
                                return Err(de::Error::duplicate_field("entries"));
                            }
                            entries = Some(map.next_value_seed(EntriesSeed)?);
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let entries = entries.ok_or_else(|| de::Error::missing_field("entries"))?;

                Ok(Log {
                    version,
                    creator,
                    browser,
                    pages,
                    entries,
                })
            }
        }

        deserializer.deserialize_map(LogVisitor)
    }
}

struct EntriesSeed;

impl<'de> DeserializeSeed<'de> for EntriesSeed {
    type Value = Vec<Entry>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Vec<Entry>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(EntriesVisitor)
    }
}

struct EntriesVisitor;

impl<'de> Visitor<'de> for EntriesVisitor {
    type Value = Vec<Entry>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a list of HAR entries")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Vec<Entry>, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut entries = Vec::new();
        while let Some(entry) = seq.next_element()? {
            entries.push(entry);
        }
        Ok(entries)
    }
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
