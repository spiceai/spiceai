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
#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;

use serde::Deserialize;

pub mod tei;
mod util;
pub use util::{download_hf_file, link_files_into_tmp_dir};

/// Important fields from a model's `config.json`
#[derive(Debug, Deserialize)]
pub struct ModelConfig {
    pub hidden_size: i32,
    pub architectures: Vec<String>,
    pub model_type: String,
    #[serde(alias = "n_positions")]
    pub max_position_embeddings: usize,
    #[serde(default)]
    pub pad_token_id: usize,
    pub id2label: Option<HashMap<String, String>>,
    pub label2id: Option<HashMap<String, usize>>,
}
