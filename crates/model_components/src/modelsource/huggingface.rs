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
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::prelude::*;
use spicepod::component::model::HUGGINGFACE_PATH_REGEX;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub struct Huggingface {}

#[async_trait]
impl ModelSource for Huggingface {
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

        let files_param = params
            .get("files")
            .map(SecretBox::expose_secret)
            .map(ToString::to_string);

        let files = match files_param {
            Some(files) => files
                .split(',')
                .map(str::trim)
                .filter(|file| !file.is_empty())
                .map(ToString::to_string)
                .collect(),
            None => vec![],
        };

        // it is not copying local model into .spice folder
        let local_path = super::ensure_model_path(name.as_str())?;
        let local_path = PathBuf::from(local_path);
        let root_dir = local_path
            .parent()
            .map_or_else(|| local_path.clone(), Path::to_path_buf);

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

        let Some(caps) = HUGGINGFACE_PATH_REGEX.captures(remote_path.as_str()) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: format!(
                    "The 'from' parameter is invalid for a huggingface source: {remote_path}. For details, visit: https://spiceai.org/docs/components/models/huggingface#from-format"
                ),
            }
            .build());
        };

        let revision = match caps["revision"].to_owned() {
            s if s.is_empty() => "main".to_string(),
            s if s == "latest" => "main".to_string(),
            _ => caps["revision"].to_string(),
        };

        let versioned_path = local_path.join(&revision);

        let mut onnx_file_name = String::new();

        std::fs::create_dir_all(&versioned_path).context(super::UnableToCreateModelPathSnafu {})?;

        for file in files {
            let download_url = format!(
                "https://huggingface.co/{}/{}/resolve/{}/{}",
                caps["org"].to_owned(),
                caps["model"].to_owned(),
                revision,
                file,
            );

            let file_path = resolve_model_file_path(&root_dir, &versioned_path, &file)?;

            if std::fs::metadata(&file_path).is_ok() {
                tracing::info!(
                    "File already exists: {}, skipping download",
                    file_path.display()
                );

                continue;
            }

            tracing::info!("Downloading model: {}", download_url);

            if file.to_lowercase().ends_with(".onnx") {
                onnx_file_name = file_path.to_string_lossy().into_owned();
            }

            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(1800))
                .build()
                .context(super::UnableToFetchModelSnafu {})?;
            let response = client
                .get(download_url)
                .bearer_auth(
                    params
                        .get("token")
                        .map(SecretBox::expose_secret)
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                )
                .send()
                .await
                .context(super::UnableToFetchModelSnafu {})?;

            if !response.status().is_success() {
                return Err(Error::UnableToDownloadModelFile {});
            }

            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).context(super::UnableToCreateModelPathSnafu {})?;
            }

            let mut file_handle = std::fs::File::create(&file_path)
                .context(super::UnableToCreateModelPathSnafu {})?;
            let bytes = response
                .bytes()
                .await
                .context(super::UnableToFetchModelSnafu {})?;
            let mut content = Cursor::new(bytes);
            std::io::copy(&mut content, &mut file_handle)
                .context(super::UnableToCreateModelPathSnafu {})?;

            tracing::info!("Downloaded: {}", file_path.display());
        }

        Ok(onnx_file_name)
    }
}

fn resolve_model_file_path(root_dir: &Path, base_dir: &Path, file: &str) -> super::Result<PathBuf> {
    let trimmed = file.trim();
    ensure!(
        !trimmed.is_empty(),
        super::InvalidModelFilePathSnafu {
            path: file.to_string(),
        }
    );

    let relative_path = Path::new(trimmed);
    ensure!(
        !relative_path.has_root(),
        super::InvalidModelFilePathSnafu {
            path: file.to_string(),
        }
    );

    let mut candidate = base_dir.to_path_buf();

    for component in relative_path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => candidate.push(segment),
            Component::ParentDir => {
                if candidate == root_dir {
                    return Err(super::InvalidModelFilePathSnafu {
                        path: file.to_string(),
                    }
                    .build());
                }
                if !candidate.pop() {
                    return Err(super::InvalidModelFilePathSnafu {
                        path: file.to_string(),
                    }
                    .build());
                }
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(super::InvalidModelFilePathSnafu {
                    path: file.to_string(),
                }
                .build());
            }
        }
    }

    ensure!(
        candidate.starts_with(root_dir),
        super::InvalidModelFilePathSnafu {
            path: file.to_string(),
        }
    );

    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_parent_directory_components() {
        let root = Path::new("/tmp/spice/models");
        let base = root.join("test/latest");
        let result = resolve_model_file_path(root, &base, "../../../weights.bin");
        assert!(matches!(
            result,
            Err(super::Error::InvalidModelFilePath { .. })
        ));
    }

    #[test]
    fn allows_relative_file() {
        let root = Path::new("/tmp/spice/models");
        let base = root.join("test/latest");
        let result = resolve_model_file_path(root, &base, "weights.bin").expect("valid path");
        assert!(result.ends_with("weights.bin"));
    }

    #[test]
    fn allows_relative_parent_within_root() {
        let root = Path::new("/tmp/spice/models");
        let base = root.join("test/latest");
        let result = resolve_model_file_path(root, &base, "../shared/model.gguf")
            .expect("valid parent path");
        assert!(result.ends_with("shared/model.gguf"));
        assert!(result.starts_with(root));
    }
}
