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
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use hf_hub::{Repo, RepoType, api::tokio::ApiBuilder};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use test_framework::{
    anyhow,
    arrow::{self, array::RecordBatch},
    futures::TryStreamExt,
    spiced::SpicedInstance,
    spicetest::search::{SearchConfig, SearchRequest, SearchResult},
};

/// Location of an MTEB retrieval dataset on Hugging Face: the repository, the revision, and the
/// path to each of the three files the loader needs (`corpus`, `queries`, and `qrels`).
///
/// Every MTEB retrieval dataset exposes the same logical columns regardless of layout: corpus
/// and queries carry `_id` and `text` string columns, and the relevance judgments carry
/// `query-id`, `corpus-id`, and `score` columns. Only the file paths and revision differ, so a
/// single loader serves both layouts. The standard layout adds a `title` string column to the
/// corpus, which the search index ignores.
pub(crate) struct MtebRepo {
    /// Repository id without the `datasets/` prefix, e.g. `mteb/fiqa`.
    pub repo: &'static str,
    pub revision: &'static str,
    /// One entry per corpus parquet shard. A large corpus is split across several files, so the
    /// loader downloads every shard and concatenates them; reading only the first shard would drop
    /// documents and understate recall.
    pub corpus_paths: &'static [&'static str],
    pub queries_path: &'static str,
    pub qrels_path: &'static str,
}

impl MtebRepo {
    /// The `mteb/*_top_250_only_w_correct-v2` reranking layout. Parquet files are committed on the
    /// `main` branch, and the relevance judgments live in `data/`. The `score` column is `int64`.
    pub(crate) const fn top_250(repo: &'static str) -> Self {
        Self {
            repo,
            revision: "main",
            corpus_paths: &["corpus/test-00000-of-00001.parquet"],
            queries_path: "queries/test-00000-of-00001.parquet",
            qrels_path: "data/test-00000-of-00001.parquet",
        }
    }

    /// The standard MTEB (BeIR-style) retrieval layout with a single-shard corpus. The source
    /// repository commits `jsonl`, so the loader reads the parquet that Hugging Face auto-converts
    /// onto the `refs/convert/parquet` branch: the corpus and queries configs, and the `default`
    /// config `test` split for the relevance judgments. The `score` column is `float64`.
    pub(crate) const fn standard(repo: &'static str) -> Self {
        Self::standard_sharded(repo, &["corpus/corpus/0000.parquet"])
    }

    /// The standard MTEB retrieval layout for a corpus split across several parquet shards. Pass
    /// every shard path (e.g. `corpus/corpus/0000.parquet`, `corpus/corpus/0001.parquet`); the
    /// loader concatenates them into a single `corpus.parquet`.
    pub(crate) const fn standard_sharded(
        repo: &'static str,
        corpus_paths: &'static [&'static str],
    ) -> Self {
        Self {
            repo,
            revision: "refs/convert/parquet",
            corpus_paths,
            queries_path: "queries/queries/0000.parquet",
            qrels_path: "default/test/0000.parquet",
        }
    }
}

/// Downloads the dataset files for `dataset` from Hugging Face and copies them into the specified
/// `spicepod_dir` directory as `corpus.parquet`, `queries.parquet`, and `data.parquet`.
pub(crate) async fn prepare_dataset(dataset: &MtebRepo, spicepod_dir: &Path) -> anyhow::Result<()> {
    println!("Preparing MTEB dataset {}...", dataset.repo);

    let corpus_dest = spicepod_dir.join("corpus.parquet");
    let queries_dest = spicepod_dir.join("queries.parquet");
    let data_dest = spicepod_dir.join("data.parquet");
    let has_all_files = corpus_dest.exists() && queries_dest.exists() && data_dest.exists();
    if has_all_files {
        return Ok(());
    }

    let hf_api = ApiBuilder::new()
        .with_progress(false)
        .build()
        .map_err(|e| {
            anyhow::anyhow!("Failed to initialize api to download huggingface dataset: {e}")
        })?;

    let repo = Repo::with_revision(
        format!("datasets/{}", dataset.repo),
        RepoType::Model,
        dataset.revision.to_string(),
    );

    let api_repo = hf_api.repo(repo);

    let mut corpus_shards = Vec::with_capacity(dataset.corpus_paths.len());
    for shard_path in dataset.corpus_paths {
        let shard = api_repo
            .get(shard_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to download huggingface file: {e}"))?;
        corpus_shards.push(shard);
    }

    let test_queries_path = api_repo
        .get(dataset.queries_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download huggingface file: {e}"))?;

    let scores_path = api_repo
        .get(dataset.qrels_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download huggingface file: {e}"))?;

    // Copy files to spicepod directory with new names. A single-shard corpus is copied verbatim to
    // preserve its exact parquet encoding; a multi-shard corpus is concatenated into one file.
    if let [single_shard] = corpus_shards.as_slice() {
        std::fs::copy(single_shard, &corpus_dest)
            .map_err(|e| anyhow::anyhow!("Failed to copy corpus file: {e}"))?;
    } else {
        concat_parquet_files(&corpus_shards, &corpus_dest)?;
    }
    println!("Corpus data saved to: {}", corpus_dest.display());

    std::fs::copy(&test_queries_path, &queries_dest)
        .map_err(|e| anyhow::anyhow!("Failed to copy queries file: {e}"))?;
    println!("Queries data saved to: {}", queries_dest.display());

    std::fs::copy(&scores_path, &data_dest)
        .map_err(|e| anyhow::anyhow!("Failed to copy data file: {e}"))?;
    println!("Data saved to: {}", data_dest.display());

    Ok(())
}

/// Concatenates the parquet `shards` into a single parquet file at `dest`. Every shard shares the
/// same schema (they are shards of one corpus config), so the writer takes the schema of the first
/// shard and appends the row groups of each shard in turn.
fn concat_parquet_files(shards: &[PathBuf], dest: &Path) -> anyhow::Result<()> {
    let mut writer: Option<ArrowWriter<File>> = None;

    for shard in shards {
        let file = File::open(shard)
            .map_err(|e| anyhow::anyhow!("Failed to open corpus shard {}: {e}", shard.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| anyhow::anyhow!("Failed to read corpus shard {}: {e}", shard.display()))?;

        // The first shard fixes the schema and creates the writer; later shards append to it.
        if writer.is_none() {
            let out = File::create(dest).map_err(|e| {
                anyhow::anyhow!("Failed to create corpus file {}: {e}", dest.display())
            })?;
            let schema = Arc::clone(builder.schema());
            writer = Some(
                ArrowWriter::try_new(out, schema, None)
                    .map_err(|e| anyhow::anyhow!("Failed to write corpus file: {e}"))?,
            );
        }
        let writer = writer.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Cannot prepare corpus: no corpus shards were provided")
        })?;

        let reader = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to read corpus shard {}: {e}", shard.display()))?;
        for batch in reader {
            let batch =
                batch.map_err(|e| anyhow::anyhow!("Failed to read corpus record batch: {e}"))?;
            writer
                .write(&batch)
                .map_err(|e| anyhow::anyhow!("Failed to write corpus record batch: {e}"))?;
        }
    }

    let Some(writer) = writer else {
        return Err(anyhow::anyhow!(
            "Cannot prepare corpus: no corpus shards were provided"
        ));
    };
    writer
        .close()
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("Failed to finalize corpus file: {e}"))
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

                    let mut search_request = SearchRequest::new(id, text);
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

pub(crate) async fn get_query_relevance_data(
    spiced_instance: &SpicedInstance,
) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
    let mut spice_client = spiced_instance.spice_client(None, false).await?;

    // Cast `score` to `BIGINT` so the loader handles both the `int64` judgments of the
    // `_top_250_only_w_correct-v2` layout and the `float64` judgments of the standard MTEB layout.
    // Relevance judgments are whole numbers, so the cast is exact.
    let records = execute_sql(
        &mut spice_client,
        r#"SELECT "query-id", "corpus-id", CAST(score AS BIGINT) AS score FROM relevance_data"#,
    )
    .await?;

    extract_query_relevance_from_batches(&records)
}

fn extract_query_relevance_from_batches(
    records: &[RecordBatch],
) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
    let mut query_relevance = HashMap::new();

    for batch in records {
        let query_id_column = batch
            .column_by_name("query-id")
            .ok_or_else(|| anyhow::anyhow!("Missing 'query-id' column"))?
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .ok_or_else(|| {
                anyhow::anyhow!("Failed to downcast 'query-id' column to LargeStringArray")
            })?;

        let corpus_id_column = batch
            .column_by_name("corpus-id")
            .ok_or_else(|| anyhow::anyhow!("Missing 'corpus-id' column"))?
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .ok_or_else(|| {
                anyhow::anyhow!("Failed to downcast 'corpus-id' column to LargeStringArray")
            })?;

        let score_column = batch
            .column_by_name("score")
            .ok_or_else(|| anyhow::anyhow!("Missing 'score' column"))?
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to downcast 'score' column to Int64Array"))?;

        for i in 0..batch.num_rows() {
            let query_id = query_id_column.value(i).to_string();
            let corpus_id = corpus_id_column.value(i).to_string();
            let score = i32::try_from(score_column.value(i))
                .map_err(|e| anyhow::anyhow!("Failed to convert score to i32: {e}"))?;

            query_relevance
                .entry(query_id)
                .or_insert_with(HashMap::new)
                .insert(corpus_id, score);
        }
    }

    Ok(query_relevance)
}

/// Converts raw vector search results into a structure suitable for evaluation.
/// The key is the search query ID, and the value is a map of matched corpus IDs and their scores.
/// Using query relevance data from the same dataset, this allows for evaluation of the search results.
pub(crate) fn transform_search_results_for_eval(
    search: &BTreeMap<String, SearchResult>,
) -> HashMap<String, HashMap<String, f64>> {
    let mut eval_results = HashMap::new();

    for (query_id, search_result) in search {
        let mut corpus_scores = HashMap::new();

        // Extract corpus IDs and scores from search response results
        for result in &search_result.response.results {
            // Try to extract corpus ID from primary key (looking for "_id" field)
            if let Some(corpus_id_value) = result.primary_key.get("_id") {
                let corpus_id = match corpus_id_value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => {
                        continue;
                    }
                };
                corpus_scores.insert(corpus_id, result.score);
            }
        }

        eval_results.insert(query_id.clone(), corpus_scores);
    }

    eval_results
}

async fn execute_sql(
    spice_client: &mut spiceai::Client,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let res = spice_client
        .sql(sql)
        .await?
        .try_collect::<Vec<RecordBatch>>()
        .await?;
    Ok(res)
}
