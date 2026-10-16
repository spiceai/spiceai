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

//! Where Spice keeps accelerator data on disk, and how a Cayenne acceleration's
//! metastore directory follows from its `cayenne_file_path` and
//! `cayenne_metadata_dir` parameters.
//!
//! These rules live below every crate that needs them so that the Spicepod
//! validation in `app` and the Cayenne accelerator resolve the same directory
//! from the same parameters: a check that reasons about a default the engine
//! computes elsewhere drifts the moment either side changes.

/// Base directory Spice stores accelerator data under (`<cwd>/.spice/data`).
#[must_use]
pub fn spice_data_base_path() -> String {
    let Ok(working_dir) = std::env::current_dir() else {
        return ".".to_string();
    };

    let base_folder = working_dir.join(".spice/data");
    base_folder.to_str().unwrap_or(".").to_string()
}

/// Returns true if the path is a local filesystem path (not a remote object store).
///
/// Local paths include:
/// - Absolute paths: `/data/cayenne`
/// - Relative paths: `./data`
/// - `file://` URIs: `file:///data/cayenne`
///
/// Remote paths (S3, etc.) return false.
#[must_use]
pub fn is_local_path(path: &str) -> bool {
    !path.contains("://") || path.starts_with("file://")
}

/// Strip a `file:`/`file://` scheme (including an optional authority such as
/// `localhost`) so on-disk storage detection receives a real filesystem path.
///
/// A configured Cayenne directory may be spelled as such a URI, and feeding
/// `file:///x` or `file://localhost/x` into `Path::new` would make `Auto` storage
/// detection misclassify it as `Unknown`. Returns a borrowed slice (no owned path), so
/// callers can pass the result directly as `&str`.
#[must_use]
pub fn fs_probe_path(path: &str) -> &str {
    if let Some(rest) = path.strip_prefix("file://") {
        // `rest` is either `/abs/path` (empty authority, e.g. `file:///x`) or
        // `authority/abs/path` (e.g. `localhost/abs/path`); the filesystem path
        // begins at the first '/'.
        match rest.find('/') {
            Some(slash) => &rest[slash..],
            None => rest,
        }
    } else {
        path.strip_prefix("file:").unwrap_or(path)
    }
}

/// The directory a Cayenne acceleration keeps its metastore in, from the raw
/// `cayenne_file_path` and `cayenne_metadata_dir` parameters.
///
/// Priority order:
/// 1. `cayenne_metadata_dir`, when set.
/// 2. `{cayenne_file_path}/metadata`, when `cayenne_file_path` is a local path. An
///    object-store path is skipped because the metastore cannot live on object storage.
/// 3. `{spice_data_base_path()}/metadata`, the default.
///
/// One `spiced` process keeps a single Cayenne catalog, so every file-mode Cayenne
/// acceleration in a Spicepod must resolve to the same directory; the Spicepod
/// validation and the accelerator both resolve through here.
#[must_use]
pub fn cayenne_metastore_dir(
    cayenne_file_path: Option<&str>,
    cayenne_metadata_dir: Option<&str>,
) -> String {
    if let Some(metadata_dir) = cayenne_metadata_dir {
        return metadata_dir.to_string();
    }

    if let Some(file_path) = cayenne_file_path
        && is_local_path(file_path)
    {
        let base = file_path.trim_end_matches('/');
        return format!("{base}/metadata");
    }

    format!("{}/metadata", spice_data_base_path())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_local_path_admits_local_spellings_and_refuses_object_stores() {
        assert!(is_local_path("/data/cayenne"));
        assert!(is_local_path("./data"));
        assert!(is_local_path("data/cayenne"));
        assert!(is_local_path("file:///data/cayenne"));
        assert!(is_local_path("file://localhost/data"));

        assert!(!is_local_path("s3://bucket/prefix"));
        assert!(!is_local_path("s3://bucket-usw2-az1-x-s3/prefix"));
        assert!(!is_local_path("gs://bucket/prefix"));
        assert!(!is_local_path("az://container/blob"));
    }

    #[test]
    fn fs_probe_path_strips_every_file_scheme_spelling() {
        assert_eq!(
            fs_probe_path("file:///data/cayenne/metadata"),
            "/data/cayenne/metadata"
        );
        assert_eq!(fs_probe_path("file:/data/cayenne"), "/data/cayenne");
        assert_eq!(
            fs_probe_path("file://localhost/data/cayenne"),
            "/data/cayenne"
        );
        assert_eq!(
            fs_probe_path("/data/cayenne/metadata"),
            "/data/cayenne/metadata"
        );
        assert_eq!(fs_probe_path("relative/metadata"), "relative/metadata");
        assert_eq!(fs_probe_path("s3://bucket/prefix"), "s3://bucket/prefix");
    }

    #[test]
    fn metastore_dir_follows_the_explicit_dir_then_a_local_file_path_then_the_default() {
        assert_eq!(
            cayenne_metastore_dir(Some("/mnt/a/cayenne/"), Some("/data/metadata")),
            "/data/metadata"
        );
        assert_eq!(
            cayenne_metastore_dir(Some("/mnt/a/cayenne/"), None),
            "/mnt/a/cayenne/metadata"
        );
        assert_eq!(
            cayenne_metastore_dir(Some("file:///mnt/a/cayenne"), None),
            "file:///mnt/a/cayenne/metadata"
        );

        let default = format!("{}/metadata", spice_data_base_path());
        assert_eq!(
            cayenne_metastore_dir(Some("s3://bucket/prefix"), None),
            default,
            "an object-store data path keeps the metastore at the default local location"
        );
        assert_eq!(cayenne_metastore_dir(None, None), default);
    }
}
