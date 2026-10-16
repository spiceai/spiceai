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

use std::path::{Path, PathBuf};

use super::CommonArgs;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
pub struct SearchTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// Built-in MTEB benchmark dataset to run the search test against. When omitted, the run is
    /// treated as a custom run: no MTEB data is downloaded, and the search harness tests the
    /// `corpus`, `test_queries`, and `relevance_data` tables defined in `--spicepod-path` as-is.
    #[arg(long)]
    pub(crate) benchmark_dataset: Option<SearchDatasetArg>,

    /// A parameterized SQL search pipeline: a path to a file that exists, or the SQL text. When
    /// set, each test query is sent to `POST /v1/sql` with this SQL, not to `POST /v1/search`. The
    /// SQL can use the named parameters `:query` (the query text) and `:limit` (the result limit).
    /// It must return an `_id` column (the corpus id) and can return a numeric `_score` column. Rows
    /// must be in rank order. A value that ends with `.sql` or has no whitespace must be a file.
    #[arg(long, value_name = "FILE_OR_SQL", value_parser = SqlPipelineArg::parse)]
    pub(crate) sql_pipeline: Option<SqlPipelineArg>,
}

/// The `--sql-pipeline` value: a file that holds the SQL, or the SQL text itself.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqlPipelineArg {
    File(PathBuf),
    Inline(String),
}

impl SqlPipelineArg {
    /// Makes a value that names a file into [`SqlPipelineArg::File`], and other values into
    /// [`SqlPipelineArg::Inline`]. A value that looks like a path (it ends with `.sql` or has no
    /// whitespace) but is not a file is an error, so that a mistyped path does not run as SQL.
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        if value.trim().is_empty() {
            return Err("the SQL pipeline is empty".to_string());
        }

        let path = Path::new(value);
        if path.is_file() {
            return Ok(Self::File(path.to_path_buf()));
        }
        let has_sql_extension = path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"));
        if has_sql_extension || !value.contains(char::is_whitespace) {
            return Err(format!("SQL pipeline file not found: {value}"));
        }

        Ok(Self::Inline(value.to_string()))
    }

    /// The name for telemetry: the file stem, or `inline` for SQL text.
    pub(crate) fn name(&self) -> String {
        match self {
            Self::File(path) => path.file_stem().map_or_else(
                || path.display().to_string(),
                |stem| stem.to_string_lossy().into_owned(),
            ),
            Self::Inline(_) => "inline".to_string(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::SqlPipelineArg;

    #[test]
    fn sql_pipeline_reads_existing_file_as_file() {
        let path = std::env::temp_dir().join(format!(
            "testoperator-sql-pipeline-{}.sql",
            std::process::id()
        ));
        std::fs::write(&path, "SELECT _id FROM corpus LIMIT :limit").expect("write pipeline");

        let arg = SqlPipelineArg::parse(&path.to_string_lossy()).expect("file parses");
        std::fs::remove_file(&path).expect("remove pipeline");

        assert_eq!(arg, SqlPipelineArg::File(path));
        assert_eq!(
            arg.name(),
            format!("testoperator-sql-pipeline-{}", std::process::id())
        );
    }

    #[test]
    fn sql_pipeline_reads_sql_text_as_inline() {
        let sql = "SELECT _id FROM text_search(corpus, :query) LIMIT :limit";
        let arg = SqlPipelineArg::parse(sql).expect("inline SQL parses");
        assert_eq!(arg, SqlPipelineArg::Inline(sql.to_string()));
        assert_eq!(arg.name(), "inline");
    }

    #[test]
    fn sql_pipeline_rejects_missing_path_and_empty_value() {
        for value in [
            "pipelines/missing.sql",
            "pipelines/missing",
            "my pipeline.sql",
            "my pipeline.SQL",
        ] {
            assert_eq!(
                SqlPipelineArg::parse(value),
                Err(format!("SQL pipeline file not found: {value}")),
                "{value} looks like a path, so it must not run as SQL"
            );
        }
        SqlPipelineArg::parse("  \n").expect_err("an empty value is rejected");
        // A directory is not a pipeline file.
        SqlPipelineArg::parse(&std::env::temp_dir().to_string_lossy())
            .expect_err("a directory is rejected");
    }
}
