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

use super::harness;
use super::mteb::{self, MtebRepo};

const QUORA_RETRIEVAL_REPOSITORY: MtebRepo =
    MtebRepo::top_250("mteb/QuoraRetrieval_test_top_250_only_w_correct-v2");
const MIRACL_EN_RETRIEVAL_REPOSITORY: MtebRepo =
    MtebRepo::top_250("mteb/MIRACLRetrieval_en_top_250_only_w_correct-v2");
const FIQA_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/fiqa");
const TREC_COVID_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/trec-covid");
const ARGUANA_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/arguana");
const SCIDOCS_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/scidocs");
const SCIFACT_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/scifact");
const NFCORPUS_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/nfcorpus");
const TOUCHE2020_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard_sharded(
    "mteb/touche2020",
    &["corpus/corpus/0000.parquet", "corpus/corpus/0001.parquet"],
);

/// The search dataset to run against. Each built-in variant owns its own MTEB data preparation,
/// while `Custom` tests a customer-supplied spicepod as-is. Search-config construction,
/// relevance-judgment loading, and result mapping are shared by [`harness`], since every run
/// exposes the same fixed `corpus`/`test_queries`/`relevance_data` schema.
pub(crate) enum SearchDataset {
    Builtin(BuiltinDataset),
    /// A customer-supplied spicepod. No MTEB data is downloaded: the harness tests whatever the
    /// `--spicepod-path` spicepod defines for `corpus`, `test_queries`, and `relevance_data`.
    Custom,
}

/// A built-in MTEB retrieval benchmark dataset.
// The shared `Retrieval` suffix names the MTEB task type and mirrors each variant's
// `*_retrieval` name, so it is intentional rather than a redundant repeated affix.
#[expect(clippy::enum_variant_names)]
pub(crate) enum BuiltinDataset {
    QuoraRetrieval,
    MiraclEnRetrieval,
    FiqaRetrieval,
    TrecCovidRetrieval,
    ArguanaRetrieval,
    ScidocsRetrieval,
    ScifactRetrieval,
    NfcorpusRetrieval,
    Touche2020Retrieval,
}

impl From<Option<SearchDatasetArg>> for SearchDataset {
    fn from(arg: Option<SearchDatasetArg>) -> Self {
        match arg {
            None => SearchDataset::Custom,
            Some(arg) => SearchDataset::Builtin(BuiltinDataset::from(arg)),
        }
    }
}

impl From<SearchDatasetArg> for BuiltinDataset {
    fn from(arg: SearchDatasetArg) -> Self {
        match arg {
            SearchDatasetArg::QuoraRetrieval => BuiltinDataset::QuoraRetrieval,
            SearchDatasetArg::MiraclEnRetrieval => BuiltinDataset::MiraclEnRetrieval,
            SearchDatasetArg::FiqaRetrieval => BuiltinDataset::FiqaRetrieval,
            SearchDatasetArg::TrecCovidRetrieval => BuiltinDataset::TrecCovidRetrieval,
            SearchDatasetArg::ArguanaRetrieval => BuiltinDataset::ArguanaRetrieval,
            SearchDatasetArg::ScidocsRetrieval => BuiltinDataset::ScidocsRetrieval,
            SearchDatasetArg::ScifactRetrieval => BuiltinDataset::ScifactRetrieval,
            SearchDatasetArg::NfcorpusRetrieval => BuiltinDataset::NfcorpusRetrieval,
            SearchDatasetArg::Touche2020Retrieval => BuiltinDataset::Touche2020Retrieval,
        }
    }
}

impl BuiltinDataset {
    fn repository(&self) -> &'static MtebRepo {
        match self {
            BuiltinDataset::QuoraRetrieval => &QUORA_RETRIEVAL_REPOSITORY,
            BuiltinDataset::MiraclEnRetrieval => &MIRACL_EN_RETRIEVAL_REPOSITORY,
            BuiltinDataset::FiqaRetrieval => &FIQA_RETRIEVAL_REPOSITORY,
            BuiltinDataset::TrecCovidRetrieval => &TREC_COVID_RETRIEVAL_REPOSITORY,
            BuiltinDataset::ArguanaRetrieval => &ARGUANA_RETRIEVAL_REPOSITORY,
            BuiltinDataset::ScidocsRetrieval => &SCIDOCS_RETRIEVAL_REPOSITORY,
            BuiltinDataset::ScifactRetrieval => &SCIFACT_RETRIEVAL_REPOSITORY,
            BuiltinDataset::NfcorpusRetrieval => &NFCORPUS_RETRIEVAL_REPOSITORY,
            BuiltinDataset::Touche2020Retrieval => &TOUCHE2020_RETRIEVAL_REPOSITORY,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            BuiltinDataset::QuoraRetrieval => "quora_retrieval",
            BuiltinDataset::MiraclEnRetrieval => "miracl_en_retrieval",
            BuiltinDataset::FiqaRetrieval => "fiqa_retrieval",
            BuiltinDataset::TrecCovidRetrieval => "trec_covid_retrieval",
            BuiltinDataset::ArguanaRetrieval => "arguana_retrieval",
            BuiltinDataset::ScidocsRetrieval => "scidocs_retrieval",
            BuiltinDataset::ScifactRetrieval => "scifact_retrieval",
            BuiltinDataset::NfcorpusRetrieval => "nfcorpus_retrieval",
            BuiltinDataset::Touche2020Retrieval => "touche2020_retrieval",
        }
    }
}

impl SearchDataset {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            SearchDataset::Builtin(dataset) => dataset.name(),
            SearchDataset::Custom => "custom",
        }
    }

    pub(crate) async fn prepare(&self, spicepod_dir: &Path) -> anyhow::Result<()> {
        match self {
            // A custom run tests the user's spicepod as-is, so there is nothing to download.
            SearchDataset::Custom => Ok(()),
            SearchDataset::Builtin(dataset) => {
                mteb::prepare_dataset(dataset.repository(), spicepod_dir).await
            }
        }
    }

    pub(crate) async fn init_search_config(
        &self,
        spiced_instance: &SpicedInstance,
        search_limit: Option<usize>,
    ) -> anyhow::Result<SearchConfig> {
        // Both built-in and custom runs expose `test_queries` with `_id`/`text`, so the shared
        // loader builds the search config for either.
        harness::init_search_config(spiced_instance, search_limit).await
    }

    pub(crate) async fn query_relevance_data(
        &self,
        spiced_instance: &SpicedInstance,
    ) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
        harness::get_query_relevance_data(spiced_instance).await
    }

    pub(crate) fn transform_results(
        &self,
        search: &BTreeMap<String, SearchResult>,
    ) -> HashMap<String, HashMap<String, f64>> {
        match self {
            // Every built-in MTEB corpus declares `row_id: [_id]`, so the corpus id is the `_id`
            // primary-key field.
            SearchDataset::Builtin(_) => harness::transform_search_results_for_eval(search),
            // A custom corpus names its own `row_id` column, so read the sole primary-key field.
            SearchDataset::Custom => harness::transform_custom_search_results_for_eval(search),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchDataset, SearchDatasetArg};

    #[test]
    fn none_arg_maps_to_custom_run() {
        let dataset = SearchDataset::from(None);
        assert!(matches!(dataset, SearchDataset::Custom));
        assert_eq!(dataset.name(), "custom");
    }

    #[test]
    fn some_arg_maps_to_named_builtin() {
        let dataset = SearchDataset::from(Some(SearchDatasetArg::FiqaRetrieval));
        assert!(matches!(dataset, SearchDataset::Builtin(_)));
        assert_eq!(dataset.name(), "fiqa_retrieval");
    }
}
