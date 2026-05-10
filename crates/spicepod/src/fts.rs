/*
Copyright 2026 The Spice.ai OSS Authors

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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::param::Params;

/// Dataset-level full-text search store configuration.
///
/// Selects which engine backs the full-text search index for this dataset and
/// supplies the connection parameters needed to reach it.
///
/// When absent, the runtime defaults to the built-in Tantivy in-process engine
/// (controlled by the column-level `full_text_search.index_store` field).
///
/// # Example
///
/// ```yaml
/// datasets:
///   - from: postgres:my_table
///     name: docs
///     acceleration:
///       enabled: true
///     full_text_search:
///       engine: elasticsearch
///       params:
///         endpoint: https://my-cluster.es.io:9200
///         index: spice-docs
///         user: ${secrets:ES_USER}
///         pass: ${secrets:ES_PASS}
///     columns:
///       - name: body
///         full_text_search:
///           enabled: true
///           row_id: id
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct FtsStore {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// FTS engine name. Currently only `"elasticsearch"` is supported.
    /// When absent (or when the `elasticsearch` feature is disabled), the
    /// runtime falls back to the Tantivy in-process engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,

    /// Engine-specific connection parameters.
    ///
    /// For `engine: elasticsearch`:
    /// - `endpoint` (required) — Elasticsearch cluster URL, e.g. `https://localhost:9200`
    /// - `index` (optional) — ES index name; defaults to the dataset name
    /// - `user` / `pass` (optional) — HTTP Basic Auth credentials
    /// - `client_timeout` / `connect_timeout` (optional) — timeout durations, e.g. `30s`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

const fn default_true() -> bool {
    true
}

impl Default for FtsStore {
    fn default() -> Self {
        Self {
            enabled: true,
            engine: None,
            params: None,
        }
    }
}
