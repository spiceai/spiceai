/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

pub mod metadata;
pub mod raw;

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

pub(crate) fn parse_prefix_and_regex(
    url: &Url,
    extension: Option<String>,
) -> Result<(String, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    let (_store, prefix) = object_store::parse_url(url)?;

    if let Some(_ext) = prefix.extension() {
        // Prefix is not collection, but a single file
        let filename = prefix.filename().unwrap_or_default();
        Ok((
            prefix
                .to_string()
                .strip_suffix(filename)
                .unwrap_or_default()
                .to_string(),
            Some(filename.to_string()),
        ))
    } else if let Some(ext) = extension {
        Ok((prefix.to_string(), Some(format!(r"^.*\{ext}$"))))
    } else {
        Ok((prefix.to_string(), None))
    }
}
