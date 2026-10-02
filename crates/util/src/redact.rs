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

//! Rendering a URL that may carry credentials in places a user reads.
//!
//! A dataset's `from` and a connector's base URL can embed secrets in two
//! places: the userinfo (`https://user:password@host/…`) and the query string
//! (`?api_key=…`). Anything that prints such a URL where a user can read it —
//! an `EXPLAIN` plan, a log line, an error — goes through here first.

use std::borrow::Cow;

use url::Url;

/// Returns `url` with every part a secret can hide in removed: the userinfo,
/// the query string and the fragment. The scheme, host, port and path are
/// kept, so the result still says which endpoint is read without saying how
/// the request is authenticated.
#[must_use]
pub fn url_without_secrets(url: &Url) -> Url {
    let mut redacted = url.clone();
    // Both setters fail only on a URL that cannot be a base (`mailto:`,
    // `data:`), and such a URL has no userinfo to strip.
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted
}

/// Redacts `value` when it is a URL carrying userinfo, a query string or a
/// fragment, and returns it unchanged otherwise.
///
/// Built for a dataset's `from`, which is a URL for some connectors
/// (`https://…`, `s3://…`) and a bare locator for others (`postgres:orders`,
/// `spice.ai/org/app/datasets/x`). A value that is not an absolute URL with a
/// host, or that has nothing to strip, comes back byte-for-byte, so the text
/// users already match on does not change for a credential-free `from`.
#[must_use]
pub fn redact_url_str(value: &str) -> Cow<'_, str> {
    let Ok(url) = Url::parse(value) else {
        return Cow::Borrowed(value);
    };
    if url.host_str().is_none() {
        return Cow::Borrowed(value);
    }
    let has_part_to_strip = !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some();
    if !has_part_to_strip {
        return Cow::Borrowed(value);
    }
    Cow::Owned(url_without_secrets(&url).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: &str) -> Url {
        Url::parse(value).expect("test URL parses")
    }

    #[test]
    fn userinfo_query_and_fragment_are_removed_and_the_endpoint_is_kept() {
        let url = parse("http://user:hunter2@127.0.0.1:18997/api/data?api_key=SECRET123#frag");
        let redacted = url_without_secrets(&url);
        assert_eq!(redacted.as_str(), "http://127.0.0.1:18997/api/data");
        assert_eq!(redacted.username(), "");
        assert_eq!(redacted.password(), None);
        assert_eq!(redacted.query(), None);
        assert_eq!(redacted.fragment(), None);
    }

    #[test]
    fn a_url_with_nothing_to_strip_renders_unchanged() {
        for value in [
            "https://httpbin.org/json",
            "https://api.example.com/",
            "http://127.0.0.1:18997/api/data",
            "s3://bucket/prefix/",
        ] {
            let url = parse(value);
            assert_eq!(url_without_secrets(&url).as_str(), value, "{value}");
        }
    }

    #[test]
    fn a_from_with_credentials_is_redacted() {
        let value = "http://user:hunter2@127.0.0.1:18997/api/data?api_key=SECRET123";
        let redacted = redact_url_str(value);
        assert!(matches!(redacted, Cow::Owned(_)), "a redaction allocates");
        assert_eq!(redacted, "http://127.0.0.1:18997/api/data");
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("SECRET123"));
    }

    #[test]
    fn userinfo_alone_is_redacted() {
        assert_eq!(
            redact_url_str("ftp://user:pw@files.example.com/exports/"),
            "ftp://files.example.com/exports/"
        );
        assert_eq!(
            redact_url_str("https://token@api.example.com/v1"),
            "https://api.example.com/v1"
        );
    }

    #[test]
    fn a_query_alone_is_redacted() {
        assert_eq!(
            redact_url_str("https://httpbin.org/get?param1=value1&param2=value2"),
            "https://httpbin.org/get"
        );
    }

    /// A credential-free `from` is returned as the caller wrote it, not as a
    /// normalised URL: `https://api.example.com` must not grow a trailing
    /// slash, because users match log lines on the exact text.
    #[test]
    fn a_credential_free_from_is_returned_byte_for_byte() {
        for value in [
            "https://api.example.com",
            "https://httpbin.org/json",
            "s3://spiceai-demo-datasets/taxi_trips/2024/",
            "http://127.0.0.1:18997/api/data",
        ] {
            let redacted = redact_url_str(value);
            assert!(
                matches!(redacted, Cow::Borrowed(_)),
                "{value} should be borrowed"
            );
            assert_eq!(redacted, value);
        }
    }

    /// `from` values that are not URLs with a host pass through untouched,
    /// whatever characters they contain.
    #[test]
    fn a_non_url_from_passes_through() {
        for value in [
            "postgres:orders",
            "mysql:sales.orders",
            "spice.ai/spiceai/quickstart/datasets/taxi_trips",
            "duckdb:read_parquet('x.parquet')",
            "localhost:8080",
            "",
        ] {
            let redacted = redact_url_str(value);
            assert!(
                matches!(redacted, Cow::Borrowed(_)),
                "{value:?} should be borrowed"
            );
            assert_eq!(redacted, value);
        }
    }
}
