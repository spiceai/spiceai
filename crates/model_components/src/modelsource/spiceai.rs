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

pub struct SpiceAI {}

use super::ModelSource;
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt::Write;
use std::io::Cursor;
use std::path::Path;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;

#[async_trait]
impl ModelSource for SpiceAI {
    #[allow(clippy::too_many_lines)]
    async fn pull(&self, params: Arc<HashMap<String, SecretString>>) -> super::Result<String> {
        let name = params
            .get("name")
            .map(SecretBox::expose_secret)
            .map(ToString::to_string);

        let Some(name) = name else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "The 'name' parameter is required, and was not provided.",
            }
            .build());
        };

        // it is not copying local model into .spice folder
        let local_path = super::ensure_model_path(name.as_str())?;

        let remote_path = params
            .get("path")
            .map(SecretBox::expose_secret)
            .map(ToString::to_string);

        let Some(remote_path) = remote_path else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "The 'from' parameter is required, and was not provided.",
            }
            .build());
        };

        let Ok(re) = Regex::new(
            r"\A(?:spice\.ai\/)?(?<org>[\w\-]+)\/(?<app>[\w\-]+)(?:\/models)?\/(?<model>[\w\-]+):(?<version>[\w\d\-\.]+)\z",
        ) else {
            unreachable!("Invalid regex for the spice.ai source");
        };

        let Some(caps) = re.captures(remote_path.as_str()) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: format!(
                    "The 'from' parameter is invalid for a spice.ai source: {remote_path}"
                ),
            }
            .build());
        };

        let default_url = if cfg!(feature = "dev") {
            "https://dev.spice.xyz".to_string()
        } else {
            "https://spice.ai".to_string()
        };

        let mut url = format!(
            "{}/api/orgs/{}/apps/{}/models/{}",
            default_url,
            caps["org"].to_owned(),
            caps["app"].to_owned(),
            caps["model"].to_owned(),
        );

        let version = match caps["version"].to_owned() {
            s if s.is_empty() => "latest".to_string(),
            _ => caps["version"].to_string(),
        };

        // Sanitize version to prevent path traversal (e.g., "../../etc")
        let sanitized_version = util::security::sanitize_filename(&version).map_err(|reason| {
            super::Error::UnableToLoadConfig {
                reason: format!("Invalid version in path: {reason}"),
            }
        })?;

        match sanitized_version.as_str() {
            "latest" => {}
            _ => {
                let _ = write!(url, "?training_run_id={sanitized_version}");
            }
        }

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(1800))
            .build()
            .context(super::UnableToFetchModelSnafu)?;
        let data: ModelRoot = client
            .get(url)
            .bearer_auth(
                params
                    .get("token")
                    .map(SecretBox::expose_secret)
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            )
            .send()
            .await
            .context(super::UnableToFetchModelSnafu)?
            .json()
            .await
            .context(super::UnableToFetchModelSnafu)?;

        // Given we are still actively developing the model response, we'll only fetch the frist
        // export url for now.
        // In future, we can use a proper static model response format to parse the body
        let download_url = data
            .artifacts
            .first()
            .context(super::UnableToParseMetadataSnafu)?
            .export_url
            .clone()
            .context(super::UnableToParseMetadataSnafu {})?;

        let versioned_path = Path::new(&local_path).join(&sanitized_version);
        let file_path = versioned_path.join("model.onnx");

        let file_exists = tokio::task::spawn_blocking({
            let file_path = file_path.clone();
            move || std::fs::metadata(&file_path).is_ok()
        })
        .await
        .unwrap_or(false);

        if file_exists {
            tracing::debug!(
                "File already exists: {}, skipping download",
                file_path.display()
            );
            return Ok(file_path.to_string_lossy().into_owned());
        }

        let response = client
            .get(download_url)
            .send()
            .await
            .context(super::UnableToFetchModelSnafu {})?;

        let bytes = response
            .bytes()
            .await
            .context(super::UnableToFetchModelSnafu)?;

        util::security::validate_non_empty_bytes(&bytes, "model.onnx")
            .map_err(|reason| super::Error::UnableToLoadConfig { reason })?;

        let versioned_path_clone = versioned_path.clone();
        let file_path_clone = file_path.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&versioned_path_clone)
                .context(super::UnableToCreateModelPathSnafu {})?;
            let mut file = std::fs::File::create(&file_path_clone).map_err(|e| {
                super::Error::UnableToLoadConfig {
                    reason: format!(
                        "Failed to create model file {}: {e}",
                        file_path_clone.display()
                    ),
                }
            })?;
            let mut content = Cursor::new(bytes);
            std::io::copy(&mut content, &mut file).map_err(|e| {
                super::Error::UnableToLoadConfig {
                    reason: format!(
                        "Failed to write model file {}: {e}",
                        file_path_clone.display()
                    ),
                }
            })?;
            Ok::<(), super::Error>(())
        })
        .await
        .map_err(|e| super::Error::UnableToLoadConfig {
            reason: format!("Task panicked while writing model file: {e}"),
        })?
        .map_err(|e| super::Error::UnableToLoadConfig {
            reason: format!("Failed to write model file: {e}"),
        })?;

        Ok(file_path.to_string_lossy().into_owned())
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct Artifact {
    cid: String,
    created_at: String,
    r#type: String,
    model_training_run_id: String,
    export_url: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
struct ModelRoot {
    sha: String,
    family: String,
    name: String,
    model_type: String,
    epochs: u64,
    training_entry_point: Option<String>,
    training_query: String,
    handler: Option<String>,
    inference_entry_point: Option<String>,
    inference_query: String,
    lookback_size: u64,
    forecast_size: u64,
    metadata: serde_json::Map<String, Value>,
    artifacts: Vec<Artifact>,
}
