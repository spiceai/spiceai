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

use super::CommonArgs;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
pub struct SearchTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// Target test dataset to run the search test against.
    #[arg(long)]
    pub(crate) benchmark_dataset: SearchDatasetArg,
}

/// Search benchmark dataset selector. Used both as the `--benchmark-dataset` CLI value and as the
/// `benchmark_dataset` field in `testoperator dispatch` search test files.
#[derive(Clone, Copy, ValueEnum, Debug, Deserialize, Serialize)]
// The shared `Retrieval` suffix names the MTEB task type (as opposed to Reranking or
// Classification) and mirrors the `*_retrieval` CLI/serde string of each variant, so it is
// intentional rather than a redundant repeated affix.
#[expect(clippy::enum_variant_names)]
pub enum SearchDatasetArg {
    /// MTEB `QuoraRetrieval` (`https://huggingface.co/datasets/mteb/QuoraRetrieval_test_top_250_only_w_correct-v2/`).
    #[value(name = "quora_retrieval")]
    #[serde(rename = "quora_retrieval")]
    QuoraRetrieval,
    /// MTEB `MIRACLRetrieval` English (`https://huggingface.co/datasets/mteb/MIRACLRetrieval_en_top_250_only_w_correct-v2/`).
    #[value(name = "miracl_en_retrieval")]
    #[serde(rename = "miracl_en_retrieval")]
    MiraclEnRetrieval,
    /// MTEB `FiQA2018` (`https://huggingface.co/datasets/mteb/fiqa`).
    #[value(name = "fiqa_retrieval")]
    #[serde(rename = "fiqa_retrieval")]
    FiqaRetrieval,
    /// MTEB `TRECCOVID` (`https://huggingface.co/datasets/mteb/trec-covid`).
    #[value(name = "trec_covid_retrieval")]
    #[serde(rename = "trec_covid_retrieval")]
    TrecCovidRetrieval,
    /// MTEB `ArguAna` (`https://huggingface.co/datasets/mteb/arguana`).
    #[value(name = "arguana_retrieval")]
    #[serde(rename = "arguana_retrieval")]
    ArguanaRetrieval,
    /// MTEB `SCIDOCS` (`https://huggingface.co/datasets/mteb/scidocs`).
    #[value(name = "scidocs_retrieval")]
    #[serde(rename = "scidocs_retrieval")]
    ScidocsRetrieval,
    /// MTEB `SciFact` (`https://huggingface.co/datasets/mteb/scifact`).
    #[value(name = "scifact_retrieval")]
    #[serde(rename = "scifact_retrieval")]
    ScifactRetrieval,
    /// MTEB `NFCorpus` (`https://huggingface.co/datasets/mteb/nfcorpus`).
    #[value(name = "nfcorpus_retrieval")]
    #[serde(rename = "nfcorpus_retrieval")]
    NfcorpusRetrieval,
    /// MTEB `Touche2020` argument retrieval (`https://huggingface.co/datasets/mteb/touche2020`).
    #[value(name = "touche2020_retrieval")]
    #[serde(rename = "touche2020_retrieval")]
    Touche2020Retrieval,
    /// MTEB `MSMARCO` (`https://huggingface.co/datasets/mteb/msmarco`).
    #[value(name = "msmarco_retrieval")]
    #[serde(rename = "msmarco_retrieval")]
    MsmarcoRetrieval,
    /// MTEB `StackOverflowQA` (`https://huggingface.co/datasets/mteb/stackoverflow-qa`).
    #[value(name = "stackoverflow_qa_retrieval")]
    #[serde(rename = "stackoverflow_qa_retrieval")]
    StackoverflowQaRetrieval,
}

impl std::fmt::Display for SearchDatasetArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchDatasetArg::QuoraRetrieval => write!(f, "quora_retrieval"),
            SearchDatasetArg::MiraclEnRetrieval => write!(f, "miracl_en_retrieval"),
            SearchDatasetArg::FiqaRetrieval => write!(f, "fiqa_retrieval"),
            SearchDatasetArg::TrecCovidRetrieval => write!(f, "trec_covid_retrieval"),
            SearchDatasetArg::ArguanaRetrieval => write!(f, "arguana_retrieval"),
            SearchDatasetArg::ScidocsRetrieval => write!(f, "scidocs_retrieval"),
            SearchDatasetArg::ScifactRetrieval => write!(f, "scifact_retrieval"),
            SearchDatasetArg::NfcorpusRetrieval => write!(f, "nfcorpus_retrieval"),
            SearchDatasetArg::Touche2020Retrieval => write!(f, "touche2020_retrieval"),
            SearchDatasetArg::MsmarcoRetrieval => write!(f, "msmarco_retrieval"),
            SearchDatasetArg::StackoverflowQaRetrieval => write!(f, "stackoverflow_qa_retrieval"),
        }
    }
}
