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

//! Local file system registry for Spicepods.

use super::{Error, IoSnafu, Result};
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const GENERIC_MANIFEST: &str = "spicepod.yaml";

/// Registry that fetches Spicepods from the local file system.
pub struct LocalFileRegistry;

impl LocalFileRegistry {
    #[expect(
        clippy::unused_async,
        reason = "Async for API consistency with SpicerackRegistry"
    )]
    pub async fn get_pod(
        &self,
        pod_path: &str,
        pods_dir: &Path,
        _headers: &HashMap<String, String>,
        _http_client: &reqwest::Client,
    ) -> Result<PathBuf> {
        // Handle file:// URLs
        let path_str = pod_path.strip_prefix("file://").unwrap_or(pod_path);
        let source_path = Path::new(path_str);

        // Check if source exists
        let metadata = std::fs::metadata(source_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::DirectoryNotFound {
                    path: pod_path.to_string(),
                }
            } else {
                Error::Io {
                    operation: "read",
                    path: pod_path.to_string(),
                    source: e,
                }
            }
        })?;

        if !metadata.is_dir() {
            return Err(Error::InvalidSpicepod {
                path: pod_path.to_string(),
            });
        }

        // Get canonical path for safe source/destination comparisons.
        let source_path = std::fs::canonicalize(source_path).context(IoSnafu {
            operation: "canonicalize",
            path: path_str,
        })?;

        // Get pod name from directory name
        let source_pod_name = source_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();
        let pod_name = source_pod_name.to_lowercase();

        let source_manifest = find_manifest(&source_path, &source_pod_name).ok_or_else(|| {
            Error::InvalidSpicepod {
                path: source_path.display().to_string(),
            }
        })?;

        let destination_dir = pods_dir.join(&pod_name);

        std::fs::create_dir_all(pods_dir).context(IoSnafu {
            operation: "create directory",
            path: pods_dir.display().to_string(),
        })?;
        let canonical_pods_dir = std::fs::canonicalize(pods_dir).context(IoSnafu {
            operation: "canonicalize",
            path: pods_dir.display().to_string(),
        })?;
        // The destination directory may not exist yet, so canonicalize the parent and join
        // the normalized pod directory name for stable source/destination comparisons.
        let comparable_destination_dir = canonical_pods_dir.join(&pod_name);

        if source_path != comparable_destination_dir
            && comparable_destination_dir.starts_with(&source_path)
        {
            return Err(Error::NestedLocalInstall {
                source_path: source_path.display().to_string(),
                destination_path: destination_dir.display().to_string(),
            });
        }

        // Create destination directory
        std::fs::create_dir_all(&destination_dir).context(IoSnafu {
            operation: "create directory",
            path: destination_dir.display().to_string(),
        })?;

        // Copy all files from source to the installed dependency directory.
        // When the source path is identical to the destination (e.g. `spice add ./spicepods/<pod>`
        // run from the app root, or re-adding an already-installed local pod), skip the copy
        // *and* the manifest normalization to avoid mutating the user's source files in place.
        if source_path == comparable_destination_dir {
            return Ok(destination_dir);
        }

        copy_dir_recursive(&source_path, &destination_dir)?;

        let destination_manifest = destination_dir.join(GENERIC_MANIFEST);
        if source_manifest.file_name().and_then(|name| name.to_str()) != Some(GENERIC_MANIFEST) {
            std::fs::copy(&source_manifest, &destination_manifest).context(IoSnafu {
                operation: "copy file",
                path: source_manifest.display().to_string(),
            })?;
        }
        remove_non_canonical_manifests(&destination_dir, &source_pod_name)?;

        Ok(destination_dir)
    }
}

fn find_manifest(source_path: &Path, pod_name: &str) -> Option<PathBuf> {
    let lowercase_pod_name = pod_name.to_lowercase();
    // Preserve the previous local-pod contract: a pod-named manifest wins over a generic
    // spicepod.yaml/spicepod.yml when both are present in the source directory.
    let mut candidate_names = vec![format!("{pod_name}.yaml"), format!("{pod_name}.yml")];

    if lowercase_pod_name != pod_name {
        candidate_names.push(format!("{lowercase_pod_name}.yaml"));
        candidate_names.push(format!("{lowercase_pod_name}.yml"));
    }

    push_unique_candidate(&mut candidate_names, GENERIC_MANIFEST.to_string());
    push_unique_candidate(&mut candidate_names, "spicepod.yml".to_string());

    candidate_names
        .into_iter()
        .map(|candidate_name| source_path.join(candidate_name))
        .find(|candidate_path| candidate_path.exists())
}

fn push_unique_candidate(candidate_names: &mut Vec<String>, candidate_name: String) {
    if !candidate_names.contains(&candidate_name) {
        candidate_names.push(candidate_name);
    }
}

fn manifest_aliases(pod_name: &str) -> Vec<String> {
    let lowercase_pod_name = pod_name.to_lowercase();
    let mut aliases = Vec::new();
    for candidate in [
        format!("{pod_name}.yaml"),
        format!("{pod_name}.yml"),
        format!("{lowercase_pod_name}.yaml"),
        format!("{lowercase_pod_name}.yml"),
        GENERIC_MANIFEST.to_string(),
        "spicepod.yml".to_string(),
    ] {
        push_unique_candidate(&mut aliases, candidate);
    }
    aliases
}

fn remove_non_canonical_manifests(destination_dir: &Path, pod_name: &str) -> Result<()> {
    let aliases: HashSet<String> = manifest_aliases(pod_name).into_iter().collect();
    for alias in aliases {
        if alias == GENERIC_MANIFEST {
            continue;
        }
        let alias_path = destination_dir.join(&alias);
        if alias_path.exists() {
            std::fs::remove_file(&alias_path).context(IoSnafu {
                operation: "remove file",
                path: alias_path.display().to_string(),
            })?;
        }
    }
    Ok(())
}

/// Recursively copy a directory and its contents.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    for entry in std::fs::read_dir(src).context(IoSnafu {
        operation: "read directory",
        path: src.display().to_string(),
    })? {
        let entry = entry.context(IoSnafu {
            operation: "read directory entry",
            path: src.display().to_string(),
        })?;

        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if entry
            .file_type()
            .context(IoSnafu {
                operation: "get file type",
                path: src_path.display().to_string(),
            })?
            .is_dir()
        {
            std::fs::create_dir_all(&dst_path).context(IoSnafu {
                operation: "create directory",
                path: dst_path.display().to_string(),
            })?;
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path).context(IoSnafu {
                operation: "copy file",
                path: src_path.display().to_string(),
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_url_strip() {
        let path = "file:///path/to/pod";
        let stripped = path.strip_prefix("file://").unwrap_or(path);
        assert_eq!(stripped, "/path/to/pod");
    }

    #[tokio::test]
    async fn copies_generic_yml_manifest_to_dependency_directory() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let source_dir = temp_dir.path().join("localpod");
        let pods_dir = temp_dir.path().join("spicepods");
        std::fs::create_dir_all(&source_dir).expect("source directory should be created");
        std::fs::write(
            source_dir.join("spicepod.yml"),
            "version: v2\nkind: Spicepod\nname: localpod\n",
        )
        .expect("spicepod.yml should be written");

        let installed_path = LocalFileRegistry
            .get_pod(
                source_dir.to_str().expect("source path should be utf-8"),
                &pods_dir,
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await
            .expect("local pod should be installed");

        assert_eq!(installed_path, pods_dir.join("localpod"));
        assert!(
            pods_dir.join("localpod").join("spicepod.yaml").exists(),
            "generic yaml manifest should be created for dependency loading"
        );
        assert!(
            !pods_dir.join("localpod").join("spicepod.yml").exists(),
            "original yml manifest should be normalized away"
        );
    }

    #[tokio::test]
    async fn accepts_pod_named_yml_manifest() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let source_dir = temp_dir.path().join("namedpod");
        let pods_dir = temp_dir.path().join("spicepods");
        std::fs::create_dir_all(&source_dir).expect("source directory should be created");
        std::fs::write(
            source_dir.join("namedpod.yml"),
            "version: v2\nkind: Spicepod\nname: namedpod\n",
        )
        .expect("namedpod.yml should be written");

        LocalFileRegistry
            .get_pod(
                source_dir.to_str().expect("source path should be utf-8"),
                &pods_dir,
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await
            .expect("local pod should be installed");

        assert!(
            pods_dir.join("namedpod").join("spicepod.yaml").exists(),
            "pod-named yml manifest should be normalized to spicepod.yaml"
        );
    }

    #[tokio::test]
    async fn pod_named_manifest_takes_precedence_over_generic_manifest() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let source_dir = temp_dir.path().join("namedpod");
        let pods_dir = temp_dir.path().join("spicepods");
        std::fs::create_dir_all(&source_dir).expect("source directory should be created");
        std::fs::write(
            source_dir.join("spicepod.yaml"),
            "version: v2\nkind: Spicepod\nname: generic\n",
        )
        .expect("generic manifest should be written");
        std::fs::write(
            source_dir.join("namedpod.yaml"),
            "version: v2\nkind: Spicepod\nname: named\n",
        )
        .expect("pod-named manifest should be written");

        LocalFileRegistry
            .get_pod(
                source_dir.to_str().expect("source path should be utf-8"),
                &pods_dir,
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await
            .expect("local pod should be installed");

        let installed_manifest =
            std::fs::read_to_string(pods_dir.join("namedpod").join("spicepod.yaml"))
                .expect("installed manifest should be readable");
        assert!(
            installed_manifest.contains("name: named"),
            "pod-named manifest should be used over generic manifest"
        );
    }

    #[tokio::test]
    async fn local_pod_install_lowercases_destination_dir() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let source_dir = temp_dir.path().join("LocalPod");
        let pods_dir = temp_dir.path().join("spicepods");
        std::fs::create_dir_all(&source_dir).expect("source directory should be created");
        std::fs::write(
            source_dir.join("LocalPod.yaml"),
            "version: v2\nkind: Spicepod\nname: localpod\n",
        )
        .expect("pod-named manifest should be written");

        let installed_path = LocalFileRegistry
            .get_pod(
                source_dir.to_str().expect("source path should be utf-8"),
                &pods_dir,
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await
            .expect("local pod should be installed");

        assert_eq!(installed_path, pods_dir.join("localpod"));
        assert!(pods_dir.join("localpod").join("spicepod.yaml").exists());
        let installed_names = std::fs::read_dir(&pods_dir)
            .expect("pods dir should be readable")
            .map(|entry| {
                entry
                    .expect("pods dir entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(installed_names, vec!["localpod".to_string()]);
    }

    #[tokio::test]
    async fn install_keeps_only_canonical_manifest_when_source_has_multiple_aliases() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let source_dir = temp_dir.path().join("localpod");
        let pods_dir = temp_dir.path().join("spicepods");
        std::fs::create_dir_all(&source_dir).expect("source directory should be created");
        std::fs::write(
            source_dir.join("localpod.yaml"),
            "version: v2\nkind: Spicepod\nname: pod_named\n",
        )
        .expect("pod-named manifest should be written");
        std::fs::write(
            source_dir.join("spicepod.yaml"),
            "version: v2\nkind: Spicepod\nname: generic_yaml\n",
        )
        .expect("generic yaml manifest should be written");
        std::fs::write(
            source_dir.join("spicepod.yml"),
            "version: v2\nkind: Spicepod\nname: generic_yml\n",
        )
        .expect("generic yml manifest should be written");

        LocalFileRegistry
            .get_pod(
                source_dir.to_str().expect("source path should be utf-8"),
                &pods_dir,
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await
            .expect("local pod should be installed");

        let installed_dir = pods_dir.join("localpod");
        let installed_manifest = std::fs::read_to_string(installed_dir.join("spicepod.yaml"))
            .expect("installed manifest should be readable");
        assert!(installed_manifest.contains("name: pod_named"));
        assert!(!installed_dir.join("spicepod.yml").exists());
        assert!(!installed_dir.join("localpod.yaml").exists());
    }

    #[tokio::test]
    async fn rejects_copying_source_into_nested_destination() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let source_dir = temp_dir.path().join("app");
        let pods_dir = source_dir.join("spicepods");
        std::fs::create_dir_all(&source_dir).expect("source directory should be created");
        std::fs::write(
            source_dir.join("spicepod.yaml"),
            "version: v2\nkind: Spicepod\nname: app\n",
        )
        .expect("spicepod.yaml should be written");

        let error = LocalFileRegistry
            .get_pod(
                source_dir.to_str().expect("source path should be utf-8"),
                &pods_dir,
                &HashMap::new(),
                &reqwest::Client::new(),
            )
            .await
            .expect_err("nested destination should be rejected");

        assert!(
            matches!(error, Error::NestedLocalInstall { .. }),
            "expected nested local install error"
        );
        assert!(
            !pods_dir.join("app").exists(),
            "destination directory should not be created"
        );
    }
}
