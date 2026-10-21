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

//! Spicerack.org registry for Spicepods.

use super::{Error, Result, ZipExtractionSnafu};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

/// Base URL for spicerack API.
fn get_spicerack_base_url() -> String {
    if let Ok(url) = std::env::var("SPICERACK_BASE_URL") {
        return url;
    }

    let version = env!("CARGO_PKG_VERSION");
    if version.ends_with("-dev") {
        "https://dev-api.spicerack.org/v1".to_string()
    } else {
        "https://api.spicerack.org/v1".to_string()
    }
}

/// How much of a registry error body to quote back to the user.
const MAX_QUOTED_MESSAGE_LEN: usize = 512;

/// Keys an error body may carry its human-readable text under.
const MESSAGE_KEYS: [&str; 3] = ["Message", "message", "error"];

/// Signatures a zip record can open with: local file header, end-of-central-directory (an empty
/// archive), and the spanned/split marker.
const ZIP_SIGNATURES: [&[u8]; 3] = [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"];

/// Whether a body the zip reader rejected still claims to be an archive.
///
/// A download cut short keeps its signature, so it is a damaged archive rather than a message the
/// registry meant for the user - and reporting it as one would quote the raw bytes back as if the
/// server had said them.
fn claims_to_be_an_archive(body: &[u8]) -> bool {
    ZIP_SIGNATURES
        .iter()
        .any(|signature| body.starts_with(signature))
}

/// Pull the human-readable message out of a registry error body.
///
/// Spicerack reports errors as `{"Message": "...", "Code": 0, "Type": "error"}`; anything else
/// falls back to the raw body so the server's own words always reach the user.
fn registry_error_message(body: &str) -> String {
    let parsed = serde_json::from_str::<serde_json::Value>(body).unwrap_or_default();
    let message = MESSAGE_KEYS
        .iter()
        .find_map(|key| parsed.get(key).and_then(serde_json::Value::as_str))
        .unwrap_or(body)
        .trim();

    if message.is_empty() {
        return "(the registry sent an empty body)".to_string();
    }

    // Keep the message on one line, and bound it so a stray HTML error page can't flood the
    // terminal.
    let mut quoted: String = message
        .chars()
        .take(MAX_QUOTED_MESSAGE_LEN)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if message.chars().nth(MAX_QUOTED_MESSAGE_LEN).is_some() {
        quoted.push('…');
    }
    quoted
}

/// Registry that fetches Spicepods from spicerack.org.
pub struct SpicerackRegistry;

impl SpicerackRegistry {
    pub async fn get_pod(
        &self,
        pod_full_path: &str,
        pods_dir: &Path,
        headers: &HashMap<String, String>,
        http_client: &reqwest::Client,
    ) -> Result<std::path::PathBuf> {
        self.get_pod_from(
            &get_spicerack_base_url(),
            pod_full_path,
            pods_dir,
            headers,
            http_client,
        )
        .await
    }

    async fn get_pod_from(
        &self,
        base_url: &str,
        pod_full_path: &str,
        pods_dir: &Path,
        headers: &HashMap<String, String>,
        http_client: &reqwest::Client,
    ) -> Result<std::path::PathBuf> {
        // Parse pod path and optional version (e.g., "spiceai/quickstart@v1.0")
        let (pod_path, pod_version) = if let Some(idx) = pod_full_path.find('@') {
            let (path, version) = pod_full_path.split_at(idx);
            (path, Some(&version[1..])) // Skip the '@'
        } else {
            (pod_full_path, None)
        };

        // Build URL
        let url = match pod_version {
            Some(version) => format!("{base_url}/spicepods/{pod_path}/{version}"),
            None => format!("{base_url}/spicepods/{pod_path}"),
        };

        // Make request
        let mut request = http_client.get(&url).header("Accept", "application/zip");

        for (key, value) in headers {
            request = request.header(key, value);
        }

        let response = request.send().await.map_err(|e| Error::FetchFailed {
            pod: pod_full_path.to_string(),
            message: e.to_string(),
        })?;

        // Check response status
        let status = response.status();
        if status.as_u16() == 404 {
            return Err(Error::NotFound {
                path: pod_full_path.to_string(),
            });
        }

        if !status.is_success() {
            return Err(Error::FetchFailed {
                pod: pod_full_path.to_string(),
                message: format!("HTTP {status}"),
            });
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("unset")
            .to_string();

        // Download to temp file
        let bytes = response.bytes().await.map_err(|e| Error::FetchFailed {
            pod: pod_full_path.to_string(),
            message: e.to_string(),
        })?;

        let mut temp_file = tempfile::NamedTempFile::new().map_err(|e| Error::Io {
            operation: "create temp file",
            path: "tempfile".to_string(),
            source: e,
        })?;

        temp_file.write_all(&bytes).map_err(|e| Error::Io {
            operation: "write temp file",
            path: "tempfile".to_string(),
            source: e,
        })?;

        // Extract zip
        let file = std::fs::File::open(temp_file.path()).map_err(|e| Error::Io {
            operation: "open temp file",
            path: temp_file.path().display().to_string(),
            source: e,
        })?;

        // The registry can answer a success status with an error body - a JSON payload under
        // `Content-Type: application/zip`. Reporting that as a corrupt archive throws the
        // server's own message away, so when the body is text the registry meant for the user,
        // quote it back instead. A body still carrying a zip signature is a damaged archive, not
        // a message, even when its bytes happen to be valid UTF-8. The zip reader decides what is
        // an archive, so a body it accepts is never diverted here.
        let mut archive = match zip::ZipArchive::new(file) {
            Ok(archive) => archive,
            Err(source) => {
                return Err(match std::str::from_utf8(&bytes) {
                    Ok(text) if !claims_to_be_an_archive(&bytes) => Error::NotAnArchive {
                        pod: pod_full_path.to_string(),
                        status: status.as_u16(),
                        content_type,
                        body_len: bytes.len(),
                        message: registry_error_message(text),
                    },
                    _ => Error::ZipExtraction { source },
                });
            }
        };

        // Create destination directory
        let dest_dir = pods_dir.join(pod_path);
        std::fs::create_dir_all(&dest_dir).map_err(|e| Error::Io {
            operation: "create directory",
            path: dest_dir.display().to_string(),
            source: e,
        })?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).context(ZipExtractionSnafu)?;

            // Sanitize path to prevent traversal attacks
            let file_name = match file.enclosed_name() {
                Some(name) => name.clone(),
                None => continue, // Skip files with invalid paths
            };

            let dest_path = dest_dir.join(&file_name);

            // Ensure destination is within dest_dir
            if !dest_path.starts_with(&dest_dir) {
                continue; // Skip files that would escape the destination
            }

            if file.is_dir() {
                std::fs::create_dir_all(&dest_path).map_err(|e| Error::Io {
                    operation: "create directory",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;
            } else {
                // Ensure parent directory exists
                if let Some(parent) = dest_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| Error::Io {
                        operation: "create directory",
                        path: parent.display().to_string(),
                        source: e,
                    })?;
                }

                let mut outfile = std::fs::File::create(&dest_path).map_err(|e| Error::Io {
                    operation: "create file",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;

                let mut contents = Vec::new();
                file.read_to_end(&mut contents).map_err(|e| Error::Io {
                    operation: "read from archive",
                    path: file_name.display().to_string(),
                    source: e,
                })?;

                outfile.write_all(&contents).map_err(|e| Error::Io {
                    operation: "write file",
                    path: dest_path.display().to_string(),
                    source: e,
                })?;
            }
        }

        Ok(dest_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The exact 152-byte body `api.spicerack.org` answers `GET /v1/spicepods/spiceai/quickstart`
    /// with, under `HTTP 200` and `Content-Type: application/zip`.
    const REGISTRY_ERROR_BODY: &str = "{\"Message\":\"invalid path \\\"spiceaiquickstartv0.1.08bb188f7b4106571cdec9b33f44963b04f928f7f\\\": selected encoding not supported\",\"Code\":0,\"Type\":\"error\"}\n";

    /// The manifest [`zip_with_manifest`] packs.
    const MANIFEST: &str = "version: v1\nkind: Spicepod\nname: quickstart\n";

    /// Build a Spicepod archive holding a single manifest.
    fn zip_with_manifest() -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        writer
            .start_file("spicepod.yaml", options)
            .expect("start manifest entry");
        writer
            .write_all(MANIFEST.as_bytes())
            .expect("write manifest entry");
        writer.finish().expect("finish archive").into_inner()
    }

    /// Fetch `spiceai/quickstart` from a registry that answers `response`.
    ///
    /// The returned `TempDir` is the pods dir, and has to outlive the assertions that read it.
    async fn fetch_pod_from(
        response: ResponseTemplate,
    ) -> (Result<std::path::PathBuf, Error>, tempfile::TempDir) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/spicepods/spiceai/quickstart"))
            .respond_with(response)
            .mount(&server)
            .await;

        let pods_dir = tempfile::tempdir().expect("create pods dir");
        let result = SpicerackRegistry
            .get_pod_from(
                &format!("{}/v1", server.uri()),
                "spiceai/quickstart",
                pods_dir.path(),
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await;
        (result, pods_dir)
    }

    /// Fetch a body the registry serves as an archive, whatever it actually contains.
    async fn fetch_pod_serving(
        body: impl Into<Vec<u8>>,
    ) -> (Result<std::path::PathBuf, Error>, tempfile::TempDir) {
        fetch_pod_from(ResponseTemplate::new(200).set_body_raw(body.into(), "application/zip"))
            .await
    }

    #[test]
    fn registry_error_message_reads_the_json_message_field() {
        assert_eq!(
            registry_error_message(REGISTRY_ERROR_BODY),
            "invalid path \"spiceaiquickstartv0.1.08bb188f7b4106571cdec9b33f44963b04f928f7f\": selected encoding not supported"
        );
        assert_eq!(
            registry_error_message(r#"{"message":"lowercase"}"#),
            "lowercase"
        );
        assert_eq!(
            registry_error_message(r#"{"error":"other key"}"#),
            "other key"
        );
    }

    #[test]
    fn registry_error_message_falls_back_to_the_raw_body() {
        assert_eq!(
            registry_error_message("upstream timed out"),
            "upstream timed out"
        );
        assert_eq!(
            registry_error_message(r#"{"Code":0}"#),
            r#"{"Code":0}"#,
            "a JSON body with no message key"
        );
        assert_eq!(
            registry_error_message("   \n  "),
            "(the registry sent an empty body)"
        );
        assert_eq!(
            registry_error_message(""),
            "(the registry sent an empty body)"
        );
    }

    #[test]
    fn registry_error_message_stays_on_one_line_and_bounded() {
        assert_eq!(
            registry_error_message("first line\nsecond\tline"),
            "first line second line"
        );

        let long = registry_error_message(&"x".repeat(MAX_QUOTED_MESSAGE_LEN + 10));
        assert_eq!(long.chars().count(), MAX_QUOTED_MESSAGE_LEN + 1);
        assert!(long.ends_with('\u{2026}'));
    }

    /// Regression test for #12116: the registry's own message has to survive, and the CLI must
    /// not blame the archive for a server-side error.
    #[tokio::test]
    async fn json_error_under_a_success_status_reports_the_registry_message() {
        let (result, pods_dir) = fetch_pod_serving(REGISTRY_ERROR_BODY).await;

        let message = result
            .expect_err("a JSON error body is not a Spicepod archive")
            .to_string();
        assert!(
            message.contains("selected encoding not supported"),
            "the registry's message must reach the user: {message}"
        );
        assert!(
            message.contains("content-type: application/zip")
                && message.contains(&format!("{}-byte", REGISTRY_ERROR_BODY.len())),
            "the mismatch between the declared type and the body must be reported: {message}"
        );
        assert!(
            !message.contains("EOCD") && !message.contains("extract"),
            "a server-side error must not be reported as a corrupt archive: {message}"
        );
        assert_eq!(
            std::fs::read_dir(pods_dir.path())
                .expect("read pods dir")
                .count(),
            0,
            "nothing should be written for a failed fetch"
        );
    }

    #[tokio::test]
    async fn an_empty_body_under_a_success_status_is_reported_as_such() {
        let (result, _pods_dir) = fetch_pod_serving(Vec::new()).await;

        let message = result
            .expect_err("an empty body is not a Spicepod archive")
            .to_string();
        assert!(
            message.contains("0-byte") && message.contains("(the registry sent an empty body)"),
            "{message}"
        );
    }

    /// A body the zip reader cannot make sense of and that is not text stays a corrupt archive.
    #[tokio::test]
    async fn a_corrupt_binary_body_is_still_reported_as_a_bad_archive() {
        let (result, pods_dir) =
            fetch_pod_serving(b"PK\x03\x04\xff\xfe\x00 truncated".to_vec()).await;

        let err = result.expect_err("a truncated archive cannot be extracted");
        assert!(
            matches!(err, Error::ZipExtraction { .. }),
            "{err} should stay a ZipExtraction"
        );
        assert_eq!(
            std::fs::read_dir(pods_dir.path())
                .expect("read pods dir")
                .count(),
            0,
            "a failed extraction must not leave an empty pod directory behind"
        );
    }

    /// A download cut short can leave bytes that are valid UTF-8, and quoting those back as the
    /// registry's own words would invent a message the server never sent. The zip signature is
    /// what separates a damaged archive from an error body.
    #[tokio::test]
    async fn a_truncated_archive_whose_bytes_are_text_is_still_reported_as_a_bad_archive() {
        let body = b"PK\x03\x04 truncated but perfectly valid UTF-8".to_vec();
        assert!(
            std::str::from_utf8(&body).is_ok(),
            "the body must be valid UTF-8 for this test to exercise the signature check"
        );

        let (result, _pods_dir) = fetch_pod_serving(body).await;

        let err = result.expect_err("a truncated archive cannot be extracted");
        assert!(
            matches!(err, Error::ZipExtraction { .. }),
            "{err} should stay a ZipExtraction"
        );
    }

    /// The zip reader decides what is an archive, so a body it accepts extracts even when it
    /// carries leading data and therefore starts with no zip signature.
    #[tokio::test]
    async fn an_archive_is_extracted_with_or_without_leading_data() {
        let mut with_preamble = b"#!/bin/sh\n# self-extracting preamble\n".to_vec();
        with_preamble.extend_from_slice(&zip_with_manifest());

        for (label, body) in [
            ("a bare archive", zip_with_manifest()),
            ("an archive with leading data", with_preamble),
        ] {
            let (result, pods_dir) = fetch_pod_serving(body).await;

            let dest = result.expect(label);
            assert_eq!(dest, pods_dir.path().join("spiceai/quickstart"), "{label}");
            assert_eq!(
                std::fs::read_to_string(dest.join("spicepod.yaml")).expect("read manifest"),
                MANIFEST,
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn a_missing_pod_is_still_reported_as_not_found() {
        let (result, _pods_dir) =
            fetch_pod_from(ResponseTemplate::new(404).set_body_string(REGISTRY_ERROR_BODY)).await;

        let err = result.expect_err("a 404 is not a Spicepod archive");
        assert!(
            matches!(err, Error::NotFound { .. }),
            "{err} should stay a NotFound"
        );
    }
}
