/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use super::Error;
use super::ModelSource;
use async_trait::async_trait;
use regex::Regex;
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::prelude::*;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, LazyLock};

// Matches model paths in these formats:
// - organization/model-name
// - organization/model-name:revision
// - huggingface:organization/model-name
// - hf:organization/model-name
// - huggingface:organization/model-name:revision
// - hf:organization/model-name:revision
// - huggingface.co/organization/model-name
// - huggingface.co/organization/model-name:revision
// - huggingface:huggingface.co/organization/model-name
// - hf:huggingface.co/organization/model-name
//
// Captures three named groups:
// - org: Organization name (allows word chars and hyphens)
// - model: Model name (allows word chars, hyphens, and dots)
// - revision: Optional revision/version (allows word chars, digits, hyphens, and dots)
static HUGGINGFACE_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    match Regex::new(
        r"\A(?:(?:huggingface|hf):)?(huggingface\.co\/)?(?<org>[\w\-]+)\/(?<model>[\w\-\.]+)(:(?<revision>[\w\d\-\.]+))?\z",
    ) {
        Ok(regex) => regex,
        Err(e) => {
            panic!("Regex is invalid: {e}");
        }
    }
});

pub struct Huggingface {}

#[async_trait]
impl ModelSource for Huggingface {
    async fn pull(&self, params: Arc<HashMap<String, SecretString>>) -> super::Result<String> {
        let name = params
            .get("name")
            .map(Secret::expose_secret)
            .map(ToString::to_string);

        let Some(name) = name else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "The 'name' parameter is required, and was not provided.",
            }
            .build());
        };

        let files_param = params
            .get("files")
            .map(Secret::expose_secret)
            .map(ToString::to_string);

        let files = match files_param {
            Some(files) => files.split(',').map(ToString::to_string).collect(),
            None => vec![],
        };

        // it is not copying local model into .spice folder
        let local_path = super::ensure_model_path(name.as_str())?;

        let remote_path = params
            .get("path")
            .map(Secret::expose_secret)
            .map(ToString::to_string);

        let Some(remote_path) = remote_path else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "The 'from' parameter is required, and was not provided.",
            }
            .build());
        };

        let Some(caps) = HUGGINGFACE_PATH_REGEX.captures(remote_path.as_str()) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: format!(
                    "The 'from' parameter is invalid for a huggingface source: {remote_path}.\nFor details, visit: https://spiceai.org/docs/components/models/huggingface#from-format"
                ),
            }
            .build());
        };

        let revision = match caps["revision"].to_owned() {
            s if s.is_empty() => "main".to_string(),
            s if s == "latest" => "main".to_string(),
            _ => caps["revision"].to_string(),
        };

        let versioned_path = format!("{local_path}/{revision}");

        let mut onnx_file_name = String::new();

        std::fs::create_dir_all(versioned_path.clone())
            .context(super::UnableToCreateModelPathSnafu {})?;

        let p = versioned_path.clone();

        for file in files {
            let file_name = format!("{p}/{file}");

            if std::fs::metadata(file_name.clone()).is_ok() {
                tracing::info!("File already exists: {}, skipping download", file_name);

                continue;
            }

            let download_url = format!(
                "https://huggingface.co/{}/{}/resolve/{}/{}",
                caps["org"].to_owned(),
                caps["model"].to_owned(),
                revision,
                file,
            );

            tracing::info!("Downloading model: {}", download_url);

            if file.to_lowercase().ends_with(".onnx") {
                onnx_file_name.clone_from(&file_name);
            }

            let client = reqwest::Client::new();
            let response = client
                .get(download_url)
                .bearer_auth(
                    params
                        .get("token")
                        .map(Secret::expose_secret)
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                )
                .send()
                .await
                .context(super::UnableToFetchModelSnafu {})?;

            if !response.status().is_success() {
                return Err(Error::UnableToDownloadModelFile {});
            }

            let mut file = std::fs::File::create(file_name.clone())
                .context(super::UnableToCreateModelPathSnafu {})?;
            let mut content = Cursor::new(response.bytes().await.unwrap_or_default());
            std::io::copy(&mut content, &mut file)
                .context(super::UnableToCreateModelPathSnafu {})?;

            tracing::info!("Downloaded: {}", file_name);
        }

        Ok(onnx_file_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_huggingface_path_regex() {
        let test_cases = vec![
            // Basic format
            (
                "organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With revision
            (
                "organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With huggingface: prefix
            (
                "huggingface:organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With hf: prefix
            (
                "hf:organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With huggingface: prefix and revision
            (
                "huggingface:organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With hf: prefix and revision
            (
                "hf:organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With huggingface.co domain
            (
                "huggingface.co/organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With huggingface.co domain and revision
            (
                "huggingface.co/organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With huggingface: prefix and huggingface.co domain
            (
                "huggingface:huggingface.co/organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With hf: prefix and huggingface.co domain
            (
                "hf:huggingface.co/organization/model-name",
                ("organization", "model-name", ""),
            ),
            // With huggingface: prefix, huggingface.co domain, and revision
            (
                "huggingface:huggingface.co/organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // With hf: prefix, huggingface.co domain, and revision
            (
                "hf:huggingface.co/organization/model-name:v1.0",
                ("organization", "model-name", "v1.0"),
            ),
            // Test hyphens in organization name
            ("my-org/model-name", ("my-org", "model-name", "")),
            // Test hyphens and dots in model name
            (
                "organization/my-model.v2",
                ("organization", "my-model.v2", ""),
            ),
            // Test complex revision with hyphens, dots, and numbers
            (
                "organization/model-name:v1.2-beta.3",
                ("organization", "model-name", "v1.2-beta.3"),
            ),
            // Test 'latest' revision (handled in code)
            (
                "organization/model-name:latest",
                ("organization", "model-name", "latest"),
            ),
        ];

        for (input, expected) in test_cases {
            let caps = HUGGINGFACE_PATH_REGEX
                .captures(input)
                .unwrap_or_else(|| panic!("Failed to match valid input: {input}"));

            assert_eq!(&caps["org"], expected.0, "org mismatch for input: {input}");
            assert_eq!(
                &caps["model"], expected.1,
                "model mismatch for input: {input}"
            );

            let revision = caps.name("revision").map_or("", |m| m.as_str());
            assert_eq!(revision, expected.2, "revision mismatch for input: {input}");
        }
    }

    #[test]
    fn test_invalid_huggingface_paths() {
        let invalid_paths = vec![
            "",                   // Empty string
            "invalid",            // No slash
            "/",                  // Just a slash
            "org/",               // Missing model name
            "/model",             // Missing organization
            "org/model:",         // Empty revision
            "org/model::",        // Double colon
            "huggingface:",       // Missing path
            "hf:",                // Missing path
            "huggingface:/",      // Invalid path
            "hf:/",               // Invalid path
            "huggingface.co",     // Missing path
            "huggingface.co/",    // Missing org and model
            "org/model/extra",    // Extra path component
            "@org/model",         // Invalid character in org
            "org/@model",         // Invalid character in model
            "org/model:@version", // Invalid character in revision
        ];

        for path in invalid_paths {
            assert!(
                HUGGINGFACE_PATH_REGEX.captures(path).is_none(),
                "Should not match invalid path: {path}"
            );
        }
    }
}
