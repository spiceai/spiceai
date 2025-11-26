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

        // Use the model directory itself as the root for security boundary checks.
        // This ensures files cannot escape the current model's directory into sibling models.
        let root_dir = &local_path;

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

        // Sanitize revision to prevent path traversal (e.g., "../../../etc")
        let sanitized_revision =
            util::security::sanitize_filename(&revision).map_err(|reason| {
                super::Error::UnableToLoadConfig {
                    reason: format!("Invalid revision in path: {reason}"),
                }
            })?;

        let versioned_path = local_path.join(&sanitized_revision);

        let mut onnx_file_name = String::new();

        tokio::task::spawn_blocking({
            let versioned_path = versioned_path.clone();
            move || std::fs::create_dir_all(&versioned_path)
        })
        .await
        .map_err(|e| super::Error::UnableToLoadConfig {
            reason: format!("Task panicked while creating directory: {e}"),
        })?
        .context(super::UnableToCreateModelPathSnafu {})?;

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(1800))
            .build()
            .context(super::UnableToFetchModelSnafu {})?;

        for file in files {
            let trimmed_file = file.trim();
            let download_url = format!(
                "https://huggingface.co/{}/{}/resolve/{}/{}",
                caps["org"].to_owned(),
                caps["model"].to_owned(),
                sanitized_revision,
                trimmed_file,
            );

            let file_path = resolve_model_file_path(root_dir, &versioned_path, trimmed_file)?;

            let file_exists = tokio::task::spawn_blocking({
                let file_path = file_path.clone();
                move || std::fs::metadata(&file_path).is_ok()
            })
            .await
            .unwrap_or(false);

            if file_exists {
                tracing::info!(
                    "File already exists: {}, skipping download",
                    file_path.display()
                );

                continue;
            }

            tracing::info!("Downloading model: {}", download_url);

            if file_path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("onnx"))
            {
                onnx_file_name = file_path.to_string_lossy().into_owned();
            }

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

            let bytes = response
                .bytes()
                .await
                .context(super::UnableToFetchModelSnafu)?;

            util::security::validate_non_empty_bytes(&bytes, trimmed_file)
                .map_err(|reason| super::Error::UnableToLoadConfig { reason })?;

            let file_path_clone = file_path.clone();
            tokio::task::spawn_blocking(move || {
                if let Some(parent) = file_path_clone.parent() {
                    std::fs::create_dir_all(parent)
                        .context(super::UnableToCreateModelPathSnafu {})?;
                }
                let mut file = std::fs::File::create(file_path_clone)
                    .context(super::UnableToCreateModelPathSnafu {})?;
                let mut content = Cursor::new(bytes);
                std::io::copy(&mut content, &mut file)
                    .context(super::UnableToCreateModelPathSnafu {})?;
                Ok::<(), super::Error>(())
            })
            .await
            .map_err(|e| super::Error::UnableToLoadConfig {
                reason: format!("Task panicked while writing model file: {e}"),
            })??;

            tracing::info!("Downloaded: {}", file_path.display());
        }

        Ok(onnx_file_name)
    }
}

/// Resolves a model file path relative to a base directory, enforcing that the resulting path
/// does not escape the specified root directory. This function is security-critical: it prevents
/// directory traversal attacks by rejecting absolute paths, root-prefixed paths, and parent directory
/// components that would escape the root. Returns an error if the resolved path is invalid or outside
/// the root directory. Use this function whenever accepting user-supplied or external file paths.
///
/// # Parameters
/// - `root_dir`: The root directory that resolved paths must remain within.
/// - `base_dir`: The base directory from which relative paths are resolved.
/// - `file`: The file path to resolve (relative, not absolute).
///
/// # Errors
/// Returns `Error::InvalidModelFilePath` if the path is empty, absolute, escapes the root, or contains invalid components.
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
                if candidate == root_dir || !candidate.pop() {
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
