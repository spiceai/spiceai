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
const MSMARCO_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard_sharded(
    "mteb/msmarco",
    &[
        "corpus/corpus/0000.parquet",
        "corpus/corpus/0001.parquet",
        "corpus/corpus/0002.parquet",
        "corpus/corpus/0003.parquet",
        "corpus/corpus/0004.parquet",
        "corpus/corpus/0005.parquet",
        "corpus/corpus/0006.parquet",
    ],
);
const STACKOVERFLOW_QA_RETRIEVAL_REPOSITORY: MtebRepo = MtebRepo::standard("mteb/stackoverflow-qa");

/// The search benchmark dataset to run against. Each variant owns its own dataset
/// preparation, search-config construction, relevance-judgment loading, and result
/// transform, so adding a new dataset means adding a variant here rather than
/// threading a new string through `search/mod.rs`.
// The shared `Retrieval` suffix names the MTEB task type and mirrors each variant's
// `*_retrieval` name, so it is intentional rather than a redundant repeated affix.
#[expect(clippy::enum_variant_names)]
pub(crate) enum SearchDataset {
    QuoraRetrieval,
    MiraclEnRetrieval,
    FiqaRetrieval,
    TrecCovidRetrieval,
    ArguanaRetrieval,
    ScidocsRetrieval,
    ScifactRetrieval,
    NfcorpusRetrieval,
    Touche2020Retrieval,
    MsmarcoRetrieval,
    StackoverflowQaRetrieval,
}

impl From<SearchDatasetArg> for SearchDataset {
    fn from(arg: SearchDatasetArg) -> Self {
        match arg {
            SearchDatasetArg::QuoraRetrieval => SearchDataset::QuoraRetrieval,
            SearchDatasetArg::MiraclEnRetrieval => SearchDataset::MiraclEnRetrieval,
            SearchDatasetArg::FiqaRetrieval => SearchDataset::FiqaRetrieval,
            SearchDatasetArg::TrecCovidRetrieval => SearchDataset::TrecCovidRetrieval,
            SearchDatasetArg::ArguanaRetrieval => SearchDataset::ArguanaRetrieval,
            SearchDatasetArg::ScidocsRetrieval => SearchDataset::ScidocsRetrieval,
            SearchDatasetArg::ScifactRetrieval => SearchDataset::ScifactRetrieval,
            SearchDatasetArg::NfcorpusRetrieval => SearchDataset::NfcorpusRetrieval,
            SearchDatasetArg::Touche2020Retrieval => SearchDataset::Touche2020Retrieval,
            SearchDatasetArg::MsmarcoRetrieval => SearchDataset::MsmarcoRetrieval,
            SearchDatasetArg::StackoverflowQaRetrieval => SearchDataset::StackoverflowQaRetrieval,
        }
    }
}

impl SearchDataset {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            SearchDataset::QuoraRetrieval => "quora_retrieval",
            SearchDataset::MiraclEnRetrieval => "miracl_en_retrieval",
            SearchDataset::FiqaRetrieval => "fiqa_retrieval",
            SearchDataset::TrecCovidRetrieval => "trec_covid_retrieval",
            SearchDataset::ArguanaRetrieval => "arguana_retrieval",
            SearchDataset::ScidocsRetrieval => "scidocs_retrieval",
            SearchDataset::ScifactRetrieval => "scifact_retrieval",
            SearchDataset::NfcorpusRetrieval => "nfcorpus_retrieval",
            SearchDataset::Touche2020Retrieval => "touche2020_retrieval",
            SearchDataset::MsmarcoRetrieval => "msmarco_retrieval",
            SearchDataset::StackoverflowQaRetrieval => "stackoverflow_qa_retrieval",
        }
    }

    pub(crate) async fn prepare(&self, spicepod_dir: &Path) -> anyhow::Result<()> {
        let dataset = match self {
            SearchDataset::QuoraRetrieval => &QUORA_RETRIEVAL_REPOSITORY,
            SearchDataset::MiraclEnRetrieval => &MIRACL_EN_RETRIEVAL_REPOSITORY,
            SearchDataset::FiqaRetrieval => &FIQA_RETRIEVAL_REPOSITORY,
            SearchDataset::TrecCovidRetrieval => &TREC_COVID_RETRIEVAL_REPOSITORY,
            SearchDataset::ArguanaRetrieval => &ARGUANA_RETRIEVAL_REPOSITORY,
            SearchDataset::ScidocsRetrieval => &SCIDOCS_RETRIEVAL_REPOSITORY,
            SearchDataset::ScifactRetrieval => &SCIFACT_RETRIEVAL_REPOSITORY,
            SearchDataset::NfcorpusRetrieval => &NFCORPUS_RETRIEVAL_REPOSITORY,
            SearchDataset::Touche2020Retrieval => &TOUCHE2020_RETRIEVAL_REPOSITORY,
            SearchDataset::MsmarcoRetrieval => &MSMARCO_RETRIEVAL_REPOSITORY,
            SearchDataset::StackoverflowQaRetrieval => &STACKOVERFLOW_QA_RETRIEVAL_REPOSITORY,
        };
        mteb::prepare_dataset(dataset, spicepod_dir).await
    }

    pub(crate) async fn init_search_config(
        &self,
        spiced_instance: &SpicedInstance,
        search_limit: Option<usize>,
    ) -> anyhow::Result<SearchConfig> {
        // Every MTEB dataset exposes the same `_id`/`text` query columns, so the shared loader
        // builds the search config for all variants.
        match self {
            SearchDataset::QuoraRetrieval
            | SearchDataset::MiraclEnRetrieval
            | SearchDataset::FiqaRetrieval
            | SearchDataset::TrecCovidRetrieval
            | SearchDataset::ArguanaRetrieval
            | SearchDataset::ScidocsRetrieval
            | SearchDataset::ScifactRetrieval
            | SearchDataset::NfcorpusRetrieval
            | SearchDataset::Touche2020Retrieval
            | SearchDataset::MsmarcoRetrieval
            | SearchDataset::StackoverflowQaRetrieval => {
                mteb::init_search_config(spiced_instance, search_limit).await
            }
        }
    }

    pub(crate) async fn query_relevance_data(
        &self,
        spiced_instance: &SpicedInstance,
    ) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
        match self {
            SearchDataset::QuoraRetrieval
            | SearchDataset::MiraclEnRetrieval
            | SearchDataset::FiqaRetrieval
            | SearchDataset::TrecCovidRetrieval
            | SearchDataset::ArguanaRetrieval
            | SearchDataset::ScidocsRetrieval
            | SearchDataset::ScifactRetrieval
            | SearchDataset::NfcorpusRetrieval
            | SearchDataset::Touche2020Retrieval
            | SearchDataset::MsmarcoRetrieval
            | SearchDataset::StackoverflowQaRetrieval => {
                mteb::get_query_relevance_data(spiced_instance).await
            }
        }
    }

    pub(crate) fn transform_results(
        &self,
        search: &BTreeMap<String, SearchResult>,
    ) -> HashMap<String, HashMap<String, f64>> {
        match self {
            SearchDataset::QuoraRetrieval
            | SearchDataset::MiraclEnRetrieval
            | SearchDataset::FiqaRetrieval
            | SearchDataset::TrecCovidRetrieval
            | SearchDataset::ArguanaRetrieval
            | SearchDataset::ScidocsRetrieval
            | SearchDataset::ScifactRetrieval
            | SearchDataset::NfcorpusRetrieval
            | SearchDataset::Touche2020Retrieval
            | SearchDataset::MsmarcoRetrieval
            | SearchDataset::StackoverflowQaRetrieval => {
                mteb::transform_search_results_for_eval(search)
            }
        }
    }
}
