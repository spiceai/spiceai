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
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use hf_hub::{Repo, RepoType, api::tokio::ApiBuilder};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use test_framework::anyhow;

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
        tokio::fs::copy(single_shard, &corpus_dest)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to copy corpus file: {e}"))?;
    } else {
        concat_parquet_files(&corpus_shards, &corpus_dest)?;
    }
    println!("Corpus data saved to: {}", corpus_dest.display());

    tokio::fs::copy(&test_queries_path, &queries_dest)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to copy queries file: {e}"))?;
    println!("Queries data saved to: {}", queries_dest.display());

    tokio::fs::copy(&scores_path, &data_dest)
        .await
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
