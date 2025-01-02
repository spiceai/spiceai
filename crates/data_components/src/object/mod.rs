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

use std::{path::PathBuf, sync::Arc};

pub mod metadata;
pub mod text;

use object_store::{ObjectMeta, ObjectStore};
use regex::Regex;
use snafu::ResultExt;
use url::Url;

#[derive(Debug, Clone)]
pub(crate) struct ObjectStoreContext {
    store: Arc<dyn ObjectStore>,

    // Directory-like prefix to filter objects in the store.
    prefix: Option<String>,

    // Filename filter to apply to post-[`Scan`].
    // [`object_store.list(`] does not support filtering by filename, or filename regex.
    filename_regex: Option<Regex>,
}

impl ObjectStoreContext {
    pub fn try_new(
        store: Arc<dyn ObjectStore>,
        url: &Url,
        extension: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (prefix, filename_regex_opt) = parse_prefix_and_regex(url, extension)?;
        let filename_regex = filename_regex_opt
            .map(|regex| Regex::new(&regex).boxed())
            .transpose()?;

        Ok(Self {
            store,
            prefix: Some(prefix),
            filename_regex,
        })
    }

    fn filename_in_scan(&self, meta: &ObjectMeta) -> bool {
        if let Some(regex) = &self.filename_regex {
            if let Some(filename) = meta.location.filename() {
                if !regex.is_match(filename) {
                    return false;
                }
            } else {
                return false; // Could not get the filename as a valid UTF-8 string
            }
        }
        true
    }
}

pub(crate) fn get_prefix(url: &Url) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    match url.scheme() {
        "ftp" | "sftp" => Ok(PathBuf::from(url.path())),
        _ => {
            let (_, obj_prefix) = object_store::parse_url(url)?;
            let obj_prefix_path = PathBuf::from(&obj_prefix.to_string()); // Convert to std::path::PathBuf
            Ok(obj_prefix_path)
        }
    }
}

pub(crate) fn parse_prefix_and_regex(
    url: &Url,
    extension: Option<String>,
) -> Result<(String, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    let prefix = get_prefix(url)?;

    if let Some(_ext) = prefix.extension() {
        // Prefix is not collection, but a single file
        let filename = prefix
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        Ok((
            prefix
                .to_string_lossy()
                .to_string()
                .strip_suffix(filename.as_str())
                .unwrap_or_default()
                .to_string(),
            Some(filename.to_string()),
        ))
    } else if let Some(ext) = extension {
        Ok((
            prefix.to_string_lossy().to_string(),
            Some(format!(r"^.*\{ext}$")),
        ))
    } else {
        Ok((prefix.to_string_lossy().to_string(), None))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn parse_prefix_and_regex() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use super::*;

        let url = Url::parse("file:///tmp/")?;
        let (prefix, regex) = parse_prefix_and_regex(&url, None)?;
        assert_eq!(prefix, "tmp");
        assert_eq!(regex, None);

        let url = Url::parse("file:///tmp/")?;
        let (prefix, regex) = parse_prefix_and_regex(&url, Some("txt".to_string()))?;
        assert_eq!(prefix, "tmp");
        assert_eq!(regex, Some(r"^.*\txt$".to_string()));

        let url = Url::parse("sftp://username:password@sftp.example.com:22/path/to/file.txt")?;
        let (prefix, regex) = parse_prefix_and_regex(&url, None)?;
        assert_eq!(prefix, "/path/to/");
        assert_eq!(regex, Some("file.txt".to_string()));

        let url = Url::parse("ftp://username:password@ftp.example.com:21/path/to/file")?;
        let (prefix, regex) = parse_prefix_and_regex(&url, Some("txt".to_string()))?;
        assert_eq!(prefix, "/path/to/file");
        assert_eq!(regex, Some(r"^.*\txt$".to_string()));
        Ok(())
    }
}
