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

use std::path::Path;

use hf_hub::{Repo, RepoType, api::tokio::ApiBuilder};
use test_framework::{
    anyhow,
    arrow::{self, array::RecordBatch},
    futures::TryStreamExt,
    spiced::SpicedInstance,
    spicetest::vector_search::{SearchConfig, SearchRequest},
};

/// The `QuoraRetrieval` MTEB dataset is a benchmark dataset used for evaluating retrieval models.
/// It consists of 177,163 rows and 1000 test queries.
/// `https://huggingface.co/datasets/mteb/QuoraRetrieval_test_top_250_only_w_correct-v2/`
///
/// Prepares the MTEB `QuoraRetrieval` dataset by downloading required files from Hugging Face
/// and copying them into the specified `spicepod_dir` directory.
pub(crate) async fn prepare_dataset(spicepod_dir: &Path) -> anyhow::Result<()> {
    println!("Preparing MTEB QuoraRetrieval dataset...");

    let hf_api = ApiBuilder::new()
        .with_progress(false)
        .build()
        .map_err(|e| {
            anyhow::anyhow!("Failed to initialize api to download huggingface dataset: {e}")
        })?;

    let repo = Repo::new(
        "datasets/mteb/QuoraRetrieval_test_top_250_only_w_correct-v2".to_string(),
        RepoType::Model,
    );

    let api_repo = hf_api.repo(repo);

    let data_path = api_repo
        .get("corpus/test-00000-of-00001.parquet")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download huggingface file: {e}"))?;

    let test_queries_path = api_repo
        .get("queries/test-00000-of-00001.parquet")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download huggingface file: {e}"))?;

    let scores_path = api_repo
        .get("data/test-00000-of-00001.parquet")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download huggingface file: {e}"))?;

    // Copy files to spicepod directory with new names
    let corpus_dest = spicepod_dir.join("corpus.parquet");
    std::fs::copy(&data_path, &corpus_dest)
        .map_err(|e| anyhow::anyhow!("Failed to copy corpus file: {e}"))?;
    println!("Corpus data saved to: {}", corpus_dest.display());

    let queries_dest = spicepod_dir.join("queries.parquet");
    std::fs::copy(&test_queries_path, &queries_dest)
        .map_err(|e| anyhow::anyhow!("Failed to copy queries file: {e}"))?;
    println!("Queries data saved to: {}", queries_dest.display());

    let data_dest = spicepod_dir.join("data.parquet");
    std::fs::copy(&scores_path, &data_dest)
        .map_err(|e| anyhow::anyhow!("Failed to copy data file: {e}"))?;
    println!("Data saved to: {}", data_dest.display());

    Ok(())
}

/// Initializes the search benchmark configuration for the `QuoraRetrieval` dataset.
pub(crate) async fn init_search_config(
    spiced_instance: &SpicedInstance,
    search_limit: Option<usize>,
) -> anyhow::Result<SearchConfig> {
    let mut spice_client = spiced_instance.spice_client(None, false).await?;

    // retrieve test queries from the quora dataset
    let records = execute_sql(
        &mut spice_client,
        "SELECT _id as id, text FROM test_queries",
    )
    .await?;

    let queries = to_search_requests(&records, search_limit)?;

    Ok(SearchConfig::new().add_requests(queries))
}

fn to_search_requests(
    records: &[RecordBatch],
    search_limit: Option<usize>,
) -> anyhow::Result<Vec<SearchRequest>> {
    let queries = records
        .iter()
        .map(|batch| {
            let id_column = batch
                .column_by_name("id")
                .ok_or_else(|| anyhow::anyhow!("Missing 'id' column"))?
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Failed to downcast 'id' column to LargeStringArray")
                })?;

            let text_column = batch
                .column_by_name("text")
                .ok_or_else(|| anyhow::anyhow!("Missing 'text' column"))?
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Failed to downcast 'text' column to LargeStringArray")
                })?;

            let queries = (0..batch.num_rows())
                .map(|i| {
                    let id = id_column.value(i).to_string();
                    let text = text_column.value(i).to_string();

                    let mut search_request = SearchRequest::new(id.clone(), text.clone());
                    if let Some(limit) = search_limit {
                        search_request = search_request.with_limit(limit);
                    }

                    Ok(search_request)
                })
                .collect::<Result<Vec<SearchRequest>, anyhow::Error>>()?;

            Ok(queries)
        })
        .collect::<Result<Vec<Vec<SearchRequest>>, anyhow::Error>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<SearchRequest>>();

    Ok(queries)
}

async fn execute_sql(
    spice_client: &mut spiceai::Client,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let res = spice_client
        .query(sql)
        .await?
        .try_collect::<Vec<RecordBatch>>()
        .await?;
    Ok(res)
}
