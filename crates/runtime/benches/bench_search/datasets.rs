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

use app::AppBuilder;
use hf_hub::{api::tokio::ApiBuilder, Repo, RepoType};
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    embeddings::{ColumnEmbeddingConfig, EmbeddingChunkConfig},
};

/// The `QuoraRetrieval` MTEB dataset is a benchmark dataset used for evaluating retrieval models.
/// It consists of 177,163 rows and 1000 test queries.
/// `https://huggingface.co/datasets/mteb/QuoraRetrieval_test_top_250_only_w_correct-v2/`
pub(crate) async fn add_mtep_quora_retrieval_dataset(
    app_builder: AppBuilder,
    acceleration: Option<Acceleration>,
    chunking: Option<EmbeddingChunkConfig>,
) -> Result<AppBuilder, String> {
    let hf_api = ApiBuilder::new()
        .with_progress(false)
        .build()
        .map_err(|e| format!("Failed to initialize api to download huggingface dataset: {e}"))?;

    let repo = Repo::new(
        "datasets/mteb/QuoraRetrieval_test_top_250_only_w_correct-v2".to_string(),
        RepoType::Model,
    );

    let api_repo = hf_api.repo(repo);

    let data_path = api_repo
        .get("corpus/test-00000-of-00001.parquet")
        .await
        .map_err(|e| format!("Failed to download huggingface file: {e}"))?;
    let data_path_str = data_path
        .to_str()
        .ok_or("Failed to convert PathBuf to str")?;

    let test_queries_path = api_repo
        .get("queries/test-00000-of-00001.parquet")
        .await
        .map_err(|e| format!("Failed to download huggingface file: {e}"))?;
    let test_queries_path_str = test_queries_path
        .to_str()
        .ok_or("Failed to convert PathBuf to str")?;

    let scores_path = api_repo
        .get("data/test-00000-of-00001.parquet")
        .await
        .map_err(|e| format!("Failed to download huggingface file: {e}"))?;
    let scores_path_str = scores_path
        .to_str()
        .ok_or("Failed to convert PathBuf to str")?;

    let mut ds_data = make_local_file_dataset(data_path_str, "data");
    ds_data.acceleration = acceleration;
    ds_data.embeddings = vec![ColumnEmbeddingConfig {
        column: "text".to_string(),
        model: "test_model".to_string(),
        primary_keys: Some(vec!["_id".to_string()]),
        chunking,
    }];

    Ok(app_builder
        .with_dataset(ds_data)
        .with_dataset(make_local_file_dataset(test_queries_path_str, "test_query"))
        .with_dataset(make_local_file_dataset(scores_path_str, "test_score")))
}

fn make_local_file_dataset(path: &str, name: &str) -> Dataset {
    Dataset::new(format!("file:{path}"), name.to_string())
}
