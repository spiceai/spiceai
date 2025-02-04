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
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::prelude::*;
use spicepod::component::model::HUGGINGFACE_PATH_REGEX;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

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
