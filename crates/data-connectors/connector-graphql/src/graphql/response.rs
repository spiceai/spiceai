/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Classify GraphQL HTTP response bodies before JSON decoding.

use std::fmt;
use std::time::{Duration, SystemTime};

use http::header::HeaderMap;
use reqwest::StatusCode;

use super::Error;

/// How a GraphQL HTTP body was classified before (or instead of) JSON decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseBodyFormat {
    Json,
    Html,
    Text,
    Empty,
    Incomplete,
}

impl fmt::Display for ResponseBodyFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json => write!(f, "JSON"),
            Self::Html => write!(f, "HTML"),
            Self::Text => write!(f, "text"),
            Self::Empty => write!(f, "empty"),
            Self::Incomplete => write!(f, "an incomplete body"),
        }
    }
}

impl ResponseBodyFormat {
    /// Short label used in user-facing messages (`HTML`, `text`, `empty`).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Html => "HTML",
            Self::Text => "text",
            Self::Empty => "empty",
            Self::Incomplete => "incomplete",
        }
    }
}

const PREVIEW_MAX_CHARS: usize = 200;

/// `Content-Type` values that are GraphQL/JSON payloads.
#[must_use]
pub fn is_json_media_type(content_type: &str) -> bool {
    let media_type = media_type(content_type);
    media_type.eq_ignore_ascii_case("application/json")
        || media_type.eq_ignore_ascii_case("application/graphql-response+json")
        || media_type.to_ascii_lowercase().ends_with("+json")
}

fn media_type(content_type: &str) -> &str {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .split_whitespace()
        .next()
        .unwrap_or(content_type)
}

fn is_html_media_type(content_type: &str) -> bool {
    let media_type = media_type(content_type);
    media_type.eq_ignore_ascii_case("text/html")
        || media_type.eq_ignore_ascii_case("application/xhtml+xml")
}

fn is_text_media_type(content_type: &str) -> bool {
    media_type(content_type)
        .to_ascii_lowercase()
        .starts_with("text/")
}

/// Body sniff used when `Content-Type` is missing or contradicts the payload.
#[must_use]
pub fn sniff_body_format(body: &str) -> ResponseBodyFormat {
    let trimmed = body.trim_start();
    if trimmed.is_empty() {
        return ResponseBodyFormat::Empty;
    }

    if trimmed.starts_with('<') {
        return ResponseBodyFormat::Html;
    }

    // The JSON parser is the final validator. A `{` / `[` prefix lets a missing
    // or unknown Content-Type still reach decode instead of failing as "text".
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return ResponseBodyFormat::Json;
    }

    ResponseBodyFormat::Text
}

/// Classify the HTTP body from `Content-Type` and, when needed, the payload.
#[must_use]
pub fn classify_response_body(
    content_type: Option<&str>,
    body: &[u8],
    content_length: Option<u64>,
    content_encoding: Option<&str>,
) -> ResponseBodyFormat {
    if is_truncated_body(body, content_length, content_encoding) {
        return ResponseBodyFormat::Incomplete;
    }

    if body.is_empty() || std::str::from_utf8(body).is_ok_and(|text| text.trim_start().is_empty()) {
        return ResponseBodyFormat::Empty;
    }

    let text = String::from_utf8_lossy(body);
    let sniffed = sniff_body_format(&text);

    match content_type {
        Some(ct) if is_json_media_type(ct) => {
            // A JSON Content-Type on an HTML/empty payload is a lying header.
            if matches!(
                sniffed,
                ResponseBodyFormat::Html | ResponseBodyFormat::Empty
            ) {
                sniffed
            } else {
                ResponseBodyFormat::Json
            }
        }
        Some(ct) if is_html_media_type(ct) => ResponseBodyFormat::Html,
        Some(ct) if is_text_media_type(ct) => {
            if matches!(sniffed, ResponseBodyFormat::Html | ResponseBodyFormat::Json) {
                sniffed
            } else {
                ResponseBodyFormat::Text
            }
        }
        Some(_) | None => sniffed,
    }
}

/// `Content-Length` is the encoded size. Only compare it to the received bytes
/// when the body was not compressed; otherwise a shorter decoded body is normal.
#[must_use]
pub fn is_truncated_body(
    body: &[u8],
    content_length: Option<u64>,
    content_encoding: Option<&str>,
) -> bool {
    let Some(expected) = content_length else {
        return false;
    };
    if expected == 0 {
        return false;
    }
    if content_encoding
        .is_some_and(|enc| !enc.trim().is_empty() && !enc.eq_ignore_ascii_case("identity"))
    {
        return body.is_empty();
    }
    (body.len() as u64) < expected
}

/// Strip tags and collapse whitespace so an HTML error page can be quoted.
///
/// An HTML error page contributes its title, not its stylesheet. Gateway
/// pages put the useful words in `<title>` and the rest in `<style>`.
#[must_use]
pub fn sanitize_preview(format: ResponseBodyFormat, body: &str) -> String {
    let text = match format {
        ResponseBodyFormat::Html => html_preview(body),
        ResponseBodyFormat::Empty => String::new(),
        ResponseBodyFormat::Incomplete | ResponseBodyFormat::Text | ResponseBodyFormat::Json => {
            collapse_whitespace(body)
        }
    };
    truncate_chars(&text, PREVIEW_MAX_CHARS)
}

fn html_preview(body: &str) -> String {
    if let Some(title) = html_title(body) {
        return title;
    }
    let stripped = strip_html_tags(&skip_embedded_blocks(body));
    if stripped.contains('{') {
        return String::new();
    }
    truncate_chars(&stripped, 80)
}

fn html_title(body: &str) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    let start = lower.find("<title")?;
    let after_open = body[start..].find('>')? + start + 1;
    let lower_rest = body[after_open..].to_ascii_lowercase();
    let end = lower_rest.find("</title>")? + after_open;
    let title = decode_basic_entities(&collapse_whitespace(&body[after_open..end]));
    if title.is_empty() || title.contains('{') {
        return None;
    }
    Some(truncate_chars(&title, 80))
}

/// Drop `<style>` and `<script>` bodies before the tag stripper runs. Their
/// contents sit between tags, so a tag stripper would otherwise quote the CSS.
fn skip_embedded_blocks(input: &str) -> String {
    let mut out = input.to_string();
    for (open, close) in [("<style", "</style>"), ("<script", "</script>")] {
        out = skip_block(&out, open, close);
    }
    out
}

fn skip_block(input: &str, open: &str, close: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let mut out = String::with_capacity(input.len());
    let mut index = 0;
    while let Some(relative) = lower[index..].find(open) {
        let start = index + relative;
        out.push_str(&input[index..start]);
        let after_open = start + open.len();
        if let Some(end_relative) = lower[after_open..].find(close) {
            index = after_open + end_relative + close.len();
        } else {
            return out;
        }
    }
    out.push_str(&input[index..]);
    out
}

fn decode_basic_entities(input: &str) -> String {
    input
        .replace("&middot;", "·")
        .replace("&nbsp;", " ")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn strip_html_tags(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    for c in input.chars() {
        match c {
            '<' => {
                in_tag = true;
                // Adjacent tags (`</h1><center>`) are word breaks, not glue.
                out.push(' ');
            }
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    collapse_whitespace(&out)
}

fn collapse_whitespace(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_space = true;
    for c in input.chars() {
        if c.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    let mut chars = input.chars();
    let preview: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

/// User-facing message for a non-JSON GraphQL HTTP response.
#[must_use]
pub fn unexpected_response_message(
    status: StatusCode,
    format: ResponseBodyFormat,
    preview: &str,
) -> String {
    match (status.is_success(), format) {
        (true, ResponseBodyFormat::Empty) => {
            format!(
                "upstream returned an empty response body (HTTP {})",
                status.as_u16()
            )
        }
        (true, ResponseBodyFormat::Incomplete) => {
            format!(
                "The GraphQL endpoint returned an incomplete response body (HTTP {status}). The body was shorter than the Content-Length header."
            )
        }
        (true, _) => {
            let preview_clause = if preview.is_empty() {
                String::new()
            } else {
                format!(" Preview: {preview}.")
            };
            format!(
                "The GraphQL endpoint returned {} instead of JSON (HTTP {status}). This often means the URL is wrong or the request was redirected to a login page.{preview_clause}",
                format.label()
            )
        }
        (false, ResponseBodyFormat::Empty) => {
            format!("The upstream server returned an empty response (HTTP {status}).")
        }
        (false, ResponseBodyFormat::Incomplete) => {
            format!(
                "The upstream server returned an incomplete response body (HTTP {status}). The body was shorter than the Content-Length header."
            )
        }
        (false, ResponseBodyFormat::Html) => {
            let page = if preview.is_empty() {
                String::new()
            } else {
                format!(" The page title was '{preview}'.")
            };
            format!(
                "The upstream server returned HTML instead of JSON (HTTP {status}).{page} This usually means a proxy in front of the API failed. If this continues, reduce the query or page size and verify the endpoint is reachable. See: https://spiceai.org/docs/components/data-connectors/graphql"
            )
        }
        (false, _) => {
            if preview.is_empty() {
                format!(
                    "The upstream server returned {} instead of JSON (HTTP {status}).",
                    format.label()
                )
            } else {
                format!(
                    "The upstream server returned {} instead of JSON (HTTP {status}). {preview}",
                    format.label()
                )
            }
        }
    }
}

/// User-facing detail for a body that was classified as JSON and then failed
/// to parse. A cut-off body (EOF mid-value) is a dropped connection, not a
/// document the server meant to send.
#[must_use]
pub fn json_parse_failure_detail(status: StatusCode, source: &serde_json::Error) -> String {
    if status.is_success() && source.is_eof() {
        format!(
            "The response was cut off before the JSON finished, at line {} column {}.",
            source.line(),
            source.column()
        )
    } else if status.is_server_error() {
        "The server returned a response that was not JSON. This is often a transient upstream error.".to_string()
    } else {
        "The response body could not be parsed as JSON.".to_string()
    }
}

/// Display text for a JSON parse failure. A 2xx is an invalid body, not an "upstream error".
#[must_use]
pub fn json_decode_error_message(status: StatusCode, detail: &str) -> String {
    if status.is_success() {
        format!("The GraphQL endpoint returned invalid JSON (HTTP {status}). {detail}")
    } else {
        format!("The upstream server returned an error (HTTP {status}). {detail}")
    }
}

/// Statuses the GraphQL client treats as transient (retry with backoff).
///
/// HTTP 403 is not included: a permission denial is permanent. A GitHub
/// secondary rate-limit 403 is classified as [`Error::RateLimited`] from the
/// JSON payload before this helper runs.
#[must_use]
pub fn is_transient_http_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}

/// Whether an unexpected (non-JSON) body should be retried.
#[must_use]
pub fn is_retryable_unexpected_response(status: StatusCode, format: ResponseBodyFormat) -> bool {
    match format {
        ResponseBodyFormat::Empty | ResponseBodyFormat::Incomplete => {
            status.is_success() || is_transient_http_status(status)
        }
        ResponseBodyFormat::Html | ResponseBodyFormat::Text | ResponseBodyFormat::Json => {
            is_transient_http_status(status)
        }
    }
}

/// Parse `Retry-After` / related cooldown headers from the response.
#[must_use]
pub fn retry_after_from_headers(headers: &HeaderMap) -> Option<Duration> {
    data_components::rate_limit::retry_after_duration(headers, SystemTime::now())
}

/// Build the typed error for a classified non-JSON body.
#[must_use]
pub fn unexpected_response_error(
    status: StatusCode,
    format: ResponseBodyFormat,
    body: &str,
    retry_after: Option<Duration>,
) -> Error {
    let preview = sanitize_preview(format, body);
    let message = unexpected_response_message(status, format, &preview);
    Error::UnexpectedResponse {
        status,
        format,
        preview,
        retry_after,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_media_types() {
        assert!(is_json_media_type("application/json"));
        assert!(is_json_media_type("application/json; charset=utf-8"));
        assert!(is_json_media_type("application/graphql-response+json"));
        assert!(is_json_media_type("application/problem+json"));
        assert!(is_json_media_type("Application/JSON"));
        assert!(!is_json_media_type("text/html"));
        assert!(!is_json_media_type("text/plain"));
    }

    #[test]
    fn sniff_html_and_empty() {
        assert_eq!(
            sniff_body_format("<html><body>502</body></html>"),
            ResponseBodyFormat::Html
        );
        assert_eq!(
            sniff_body_format("<!DOCTYPE html><html></html>"),
            ResponseBodyFormat::Html
        );
        assert_eq!(
            sniff_body_format("<center>nginx</center>"),
            ResponseBodyFormat::Html
        );
        assert_eq!(sniff_body_format(""), ResponseBodyFormat::Empty);
        assert_eq!(sniff_body_format("   \n"), ResponseBodyFormat::Empty);
        assert_eq!(sniff_body_format("{\"data\":{}}"), ResponseBodyFormat::Json);
        assert_eq!(sniff_body_format("[{\"id\":1}]"), ResponseBodyFormat::Json);
        assert_eq!(sniff_body_format("not html"), ResponseBodyFormat::Text);
    }

    #[test]
    fn classify_prefers_sniff_when_json_header_is_wrong() {
        let html = b"<html>nope</html>";
        assert_eq!(
            classify_response_body(Some("application/json"), html, None, None),
            ResponseBodyFormat::Html
        );
    }

    #[test]
    fn classify_empty_and_truncated() {
        assert_eq!(
            classify_response_body(Some("application/json"), b"", None, None),
            ResponseBodyFormat::Empty
        );
        assert_eq!(
            classify_response_body(Some("application/json"), b"", Some(48), None),
            ResponseBodyFormat::Incomplete
        );
        assert_eq!(
            classify_response_body(Some("application/json"), b"{", Some(80), None),
            ResponseBodyFormat::Incomplete
        );
        // Compressed: a shorter decoded body is not treated as truncation.
        assert_eq!(
            classify_response_body(
                Some("application/json"),
                b"{\"ok\":true}",
                Some(80),
                Some("gzip")
            ),
            ResponseBodyFormat::Json
        );
        // Empty decoded body with a non-zero Content-Length is incomplete even
        // when Content-Encoding is set (the advertised payload never arrived).
        assert_eq!(
            classify_response_body(Some("application/json"), b"", Some(48), Some("gzip")),
            ResponseBodyFormat::Incomplete
        );
    }

    #[test]
    fn classify_json_when_content_type_is_missing() {
        assert_eq!(
            classify_response_body(None, b"{\"data\":{}}", None, None),
            ResponseBodyFormat::Json
        );
        assert_eq!(
            classify_response_body(Some("application/octet-stream"), b"[1,2]", None, None),
            ResponseBodyFormat::Json
        );
        assert_eq!(
            classify_response_body(
                Some("text/plain; charset=utf-8"),
                b"{\"data\":{}}",
                None,
                None
            ),
            ResponseBodyFormat::Json
        );
    }

    #[test]
    fn classify_graphql_response_json() {
        assert_eq!(
            classify_response_body(
                Some("application/graphql-response+json; charset=utf-8"),
                b"{\"data\":{}}",
                None,
                None
            ),
            ResponseBodyFormat::Json
        );
    }

    #[test]
    fn html_preview_uses_the_title_and_drops_stylesheets() {
        let nginx = "<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center><hr><center>nginx</center></body></html>";
        let preview = sanitize_preview(ResponseBodyFormat::Html, nginx);
        assert_eq!(preview, "502 Bad Gateway");
        assert!(!preview.contains('<'));

        let github = "<html><head><title>Unicorn! &middot; GitHub</title><style>body { background-color: #f1f1f1; margin: 0; font-family: \"Helvetica Neue\", Helvetica, Arial, sans-serif; }</style></head><body><h1>504 Gateway Timeout</h1></body></html>";
        let preview = sanitize_preview(ResponseBodyFormat::Html, github);
        assert_eq!(preview, "Unicorn! · GitHub");
        assert!(!preview.contains("background-color"), "{preview}");
        assert!(!preview.contains("font-family"), "{preview}");
        assert!(!preview.contains('<'), "{preview}");
    }

    #[test]
    fn unexpected_messages_do_not_mention_json_decode() {
        let html_502 = unexpected_response_message(
            StatusCode::BAD_GATEWAY,
            ResponseBodyFormat::Html,
            "502 Bad Gateway",
        );
        assert!(html_502.contains("HTML"));
        assert!(html_502.contains("502"));
        assert!(html_502.contains("502 Bad Gateway"));
        assert!(html_502.contains("proxy in front of the API"));
        assert!(!html_502.contains("HTML from upstream proxy"));
        assert!(!html_502.contains('\n'), "{html_502}");
        assert!(!html_502.contains("Failed to decode response body as JSON"));

        let empty_200 = unexpected_response_message(StatusCode::OK, ResponseBodyFormat::Empty, "");
        assert_eq!(
            empty_200,
            "upstream returned an empty response body (HTTP 200)"
        );
        assert!(!empty_200.contains("upstream server returned an error"));

        let html_200 =
            unexpected_response_message(StatusCode::OK, ResponseBodyFormat::Html, "Sign in");
        assert!(html_200.contains("HTML instead of JSON"));
        assert!(html_200.contains("URL"));
        assert!(!html_200.contains("upstream server returned an error"));
    }

    #[test]
    fn json_decode_2xx_is_not_an_upstream_error() {
        let message = json_decode_error_message(
            StatusCode::OK,
            "The response body could not be parsed as JSON.",
        );
        assert!(message.contains("invalid JSON"));
        assert!(!message.contains("upstream server returned an error"));
    }

    #[test]
    fn a_cut_off_json_body_is_described_as_cut_off() {
        let err = serde_json::from_str::<serde_json::Value>("{\"data\":{\"repository\":\"")
            .expect_err("truncated JSON");
        assert!(err.is_eof(), "{err}");
        let detail = json_parse_failure_detail(StatusCode::OK, &err);
        assert!(detail.contains("cut off"), "{detail}");
        assert!(detail.contains("column"), "{detail}");
        assert!(!detail.contains("EOF"), "{detail}");
        assert!(!detail.contains("Technical details"), "{detail}");
        let message = json_decode_error_message(StatusCode::OK, &detail);
        assert!(message.contains("invalid JSON"), "{message}");
        assert!(message.contains("cut off"), "{message}");
        assert!(
            !message.contains("upstream server returned an error"),
            "{message}"
        );
    }

    #[test]
    fn retryability() {
        assert!(is_retryable_unexpected_response(
            StatusCode::OK,
            ResponseBodyFormat::Empty
        ));
        assert!(is_retryable_unexpected_response(
            StatusCode::OK,
            ResponseBodyFormat::Incomplete
        ));
        assert!(is_retryable_unexpected_response(
            StatusCode::BAD_GATEWAY,
            ResponseBodyFormat::Html
        ));
        assert!(is_retryable_unexpected_response(
            StatusCode::SERVICE_UNAVAILABLE,
            ResponseBodyFormat::Empty
        ));
        assert!(is_retryable_unexpected_response(
            StatusCode::TOO_MANY_REQUESTS,
            ResponseBodyFormat::Text
        ));
        assert!(!is_retryable_unexpected_response(
            StatusCode::OK,
            ResponseBodyFormat::Html
        ));
        assert!(!is_retryable_unexpected_response(
            StatusCode::BAD_REQUEST,
            ResponseBodyFormat::Html
        ));
        assert!(!is_retryable_unexpected_response(
            StatusCode::UNAUTHORIZED,
            ResponseBodyFormat::Text
        ));
        assert!(!is_retryable_unexpected_response(
            StatusCode::FORBIDDEN,
            ResponseBodyFormat::Html
        ));
        assert!(!is_retryable_unexpected_response(
            StatusCode::FORBIDDEN,
            ResponseBodyFormat::Text
        ));
    }
}
