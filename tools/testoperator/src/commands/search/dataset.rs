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

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use crate::args::SearchDatasetArg;
use test_framework::{
    anyhow,
    spiced::SpicedInstance,
    spicetest::search::{SearchConfig, SearchResult},
};

use super::mteb_quora;

/// The search benchmark dataset to run against. Each variant owns its own dataset
/// preparation, search-config construction, relevance-judgment loading, and result
/// transform, so adding a new dataset means adding a variant here rather than
/// threading a new string through `search/mod.rs`.
pub(crate) enum SearchDataset {
    QuoraRetrieval,
}

impl From<SearchDatasetArg> for SearchDataset {
    fn from(arg: SearchDatasetArg) -> Self {
        match arg {
            SearchDatasetArg::QuoraRetrieval => SearchDataset::QuoraRetrieval,
        }
    }
}

impl SearchDataset {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            SearchDataset::QuoraRetrieval => "quora_retrieval",
        }
    }

    pub(crate) async fn prepare(&self, spicepod_dir: &Path) -> anyhow::Result<()> {
        match self {
            SearchDataset::QuoraRetrieval => mteb_quora::prepare_dataset(spicepod_dir).await,
        }
    }

    pub(crate) async fn init_search_config(
        &self,
        spiced_instance: &SpicedInstance,
        search_limit: Option<usize>,
    ) -> anyhow::Result<SearchConfig> {
        match self {
            SearchDataset::QuoraRetrieval => {
                mteb_quora::init_search_config(spiced_instance, search_limit).await
            }
        }
    }

    pub(crate) async fn query_relevance_data(
        &self,
        spiced_instance: &SpicedInstance,
    ) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
        match self {
            SearchDataset::QuoraRetrieval => {
                mteb_quora::get_query_relevance_data(spiced_instance).await
            }
        }
    }

    pub(crate) fn transform_results(
        &self,
        search: &BTreeMap<String, SearchResult>,
    ) -> HashMap<String, HashMap<String, f64>> {
        match self {
            SearchDataset::QuoraRetrieval => mteb_quora::transform_search_results_for_eval(search),
        }
    }
}
