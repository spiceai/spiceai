/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! What a `testoperator run search` run reads from the spicepod under test: the tables the harness
//! queries, the `benchmark_dataset` label, and the corpus search setup reported as telemetry. The
//! spicepod loads its own data, so the run knows nothing about any dataset beyond this contract.

use std::{collections::HashMap, path::Path};

use test_framework::{
    anyhow::{self, Context},
    app::App,
    opentelemetry::KeyValue,
    spicepod::{acceleration::Acceleration, semantic::Column, spec::SpicepodDefinition},
};

/// Tables the harness queries. Each may be a dataset or a view.
const REQUIRED_TABLES: [&str; 3] = ["corpus", "test_queries", "relevance_data"];

/// Fails fast with an actionable message when the spicepod is missing one of the fixed tables the
/// harness queries. Without this check the run would fail later with a raw `table not found` from
/// the harness SQL.
pub(crate) fn validate_required_tables(app: &App) -> anyhow::Result<()> {
    let missing: Vec<&str> = REQUIRED_TABLES
        .into_iter()
        .filter(|name| !has_table(app, name))
        .collect();

    if !missing.is_empty() {
        return Err(anyhow::anyhow!(
            "Failed to run search test ({}): spicepod is missing required table(s): {}. \
            A search spicepod must define `corpus`, `test_queries`, and `relevance_data` \
            (each may be a dataset or a view). \
            See: https://github.com/spiceai/spiceai/issues/12935",
            app.name,
            missing.join(", ")
        ));
    }

    Ok(())
}

/// Whether the spicepod defines a table with `name` as either a dataset or a view.
fn has_table(app: &App, name: &str) -> bool {
    app.datasets.iter().any(|ds| ds.name == name) || app.views.iter().any(|v| v.name == name)
}

/// The `benchmark_dataset` telemetry attribute of a run. It names the data a spicepod searches, so
/// runs of different spicepod configurations over the same data share one dashboard series.
///
/// The spicepod declares it as top-level metadata, next to the datasets it names:
///
/// ```yaml
/// metadata:
///   benchmark_dataset: fiqa_retrieval
/// ```
///
/// A spicepod without it reports `custom`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BenchmarkDataset(String);

impl BenchmarkDataset {
    /// The spicepod `metadata` key that holds the label.
    const METADATA_KEY: &str = "benchmark_dataset";
    /// The label of a spicepod that does not name its dataset.
    const UNLABELED: &str = "custom";

    /// Reads the label from the top-level `metadata` of the spicepod file at `path`.
    pub(crate) fn from_spicepod_file(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read spicepod {}", path.display()))?;
        let definition: SpicepodDefinition = yaml::from_str(&content)
            .with_context(|| format!("Failed to parse spicepod {}", path.display()))?;
        Self::from_metadata(&definition.metadata)
    }

    fn from_metadata(metadata: &HashMap<String, serde_json::Value>) -> anyhow::Result<Self> {
        match metadata.get(Self::METADATA_KEY) {
            None => Ok(Self(Self::UNLABELED.to_string())),
            Some(serde_json::Value::String(label)) if !label.trim().is_empty() => {
                Ok(Self(label.clone()))
            }
            Some(other) => Err(anyhow::anyhow!(
                "Spicepod `metadata.{}` must be a non-empty string, but it is `{other}`.",
                Self::METADATA_KEY
            )),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The search setup of the `corpus` table, which may be a dataset or a view.
struct CorpusSpec<'a> {
    acceleration: Option<&'a Acceleration>,
    columns: &'a [Column],
}

impl<'a> CorpusSpec<'a> {
    fn find(app: &'a App) -> Option<Self> {
        if let Some(ds) = app.datasets.iter().find(|ds| ds.name == "corpus") {
            return Some(Self {
                acceleration: ds.acceleration.as_ref(),
                columns: &ds.columns,
            });
        }
        app.views.iter().find(|v| v.name == "corpus").map(|v| Self {
            acceleration: v.acceleration.as_ref(),
            columns: &v.columns,
        })
    }
}

/// Telemetry attributes that describe how the corpus is accelerated and searched: `engine`,
/// `engine_mode`, `vector_search`, `model`, and `full_text_search`.
///
/// The search columns are the columns with `embeddings` or `full_text_search` set, whatever their
/// name (`text` for MTEB, `chunk` for `ConTEB`).
pub(crate) fn corpus_attributes(app: &App) -> Vec<KeyValue> {
    let Some(corpus) = CorpusSpec::find(app) else {
        return vec![];
    };

    let mut attributes = vec![];
    if let Some(acc) = corpus.acceleration {
        attributes.push(KeyValue::new("engine", acc.engine_name().to_string()));
        attributes.push(KeyValue::new("engine_mode", acc.mode.to_string()));
    }

    let embedding_model = corpus
        .columns
        .iter()
        .flat_map(|column| &column.embeddings)
        .find_map(|embed| app.embeddings.iter().find(|e| e.name == embed.model));
    if let Some(model) = embedding_model {
        attributes.push(KeyValue::new("vector_search", "true"));
        attributes.push(KeyValue::new("model", model.from.clone()));
    } else {
        attributes.push(KeyValue::new("vector_search", "false"));
    }

    let full_text_search = corpus.columns.iter().any(|column| {
        column
            .full_text_search
            .as_ref()
            .is_some_and(|fts| fts.enabled)
    });
    attributes.push(KeyValue::new(
        "full_text_search",
        full_text_search.to_string(),
    ));

    attributes
}

#[cfg(test)]
mod tests {
    use super::{BenchmarkDataset, corpus_attributes, validate_required_tables};
    use std::collections::HashMap;
    use test_framework::{
        app::{App, AppBuilder},
        spicepod::component::{dataset::Dataset, embeddings::Embeddings, view::View},
    };

    fn dataset(yaml: &str) -> Dataset {
        yaml::from_str(yaml).expect("valid dataset yaml")
    }

    fn attributes(app: &App) -> HashMap<String, String> {
        corpus_attributes(app)
            .into_iter()
            .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().into_owned()))
            .collect()
    }

    #[test]
    fn label_comes_from_spicepod_metadata() {
        let metadata = HashMap::from([(
            "benchmark_dataset".to_string(),
            serde_json::json!("fiqa_retrieval"),
        )]);
        let label = BenchmarkDataset::from_metadata(&metadata).expect("valid label");
        assert_eq!(label.as_str(), "fiqa_retrieval");
    }

    #[test]
    fn label_defaults_to_custom() {
        let label = BenchmarkDataset::from_metadata(&HashMap::new()).expect("valid label");
        assert_eq!(label.as_str(), "custom");
    }

    #[test]
    fn label_rejects_non_string_or_empty_values() {
        for value in [serde_json::json!(""), serde_json::json!(3)] {
            let metadata = HashMap::from([("benchmark_dataset".to_string(), value)]);
            BenchmarkDataset::from_metadata(&metadata).expect_err("label should be rejected");
        }
    }

    #[test]
    fn label_reads_spicepod_file() {
        let dir = std::env::temp_dir().join(format!("search-label-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("spicepod.yaml");
        std::fs::write(
            &path,
            "version: v1\nkind: Spicepod\nname: t\nmetadata:\n  benchmark_dataset: conteb_covid_qa\n",
        )
        .expect("write spicepod");

        let label = BenchmarkDataset::from_spicepod_file(&path).expect("valid label");
        assert_eq!(label.as_str(), "conteb_covid_qa");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn attributes_find_search_column_by_config_not_name() {
        // ConTEB searches a `chunk` column; no column is named `text`.
        let corpus = dataset(
            "
from: https://example.com/documents.parquet
name: corpus
acceleration:
  enabled: true
  engine: cayenne
  mode: file
columns:
  - name: chunk_id
  - name: chunk
    full_text_search:
      enabled: true
      row_id: [chunk_id]
    embeddings:
      - from: potion
        row_id: [chunk_id]
",
        );
        let app = AppBuilder::new("t")
            .with_dataset(corpus)
            .with_embedding(Embeddings::new(
                "model2vec:minishlab/potion-multilingual-128M",
                "potion",
            ))
            .build();

        let attrs = attributes(&app);
        assert_eq!(attrs["engine"], "cayenne");
        assert_eq!(attrs["engine_mode"], "file");
        assert_eq!(attrs["vector_search"], "true");
        assert_eq!(
            attrs["model"],
            "model2vec:minishlab/potion-multilingual-128M"
        );
        assert_eq!(attrs["full_text_search"], "true");
    }

    #[test]
    fn attributes_report_disabled_search_and_read_corpus_views() {
        let mut corpus = View::new("corpus".to_string()).with_sql("SELECT 1");
        corpus.columns = vec![
            yaml::from_str("name: text\nfull_text_search:\n  enabled: false\n")
                .expect("valid column yaml"),
        ];
        let app = AppBuilder::new("t").with_view(corpus).build();

        let attrs = attributes(&app);
        assert_eq!(attrs["vector_search"], "false");
        assert_eq!(attrs["full_text_search"], "false");
        assert!(!attrs.contains_key("model"));
    }

    #[test]
    fn attributes_are_empty_without_corpus() {
        assert!(corpus_attributes(&AppBuilder::new("t").build()).is_empty());
    }

    #[test]
    fn required_tables_accept_datasets_and_views() {
        let app = AppBuilder::new("t")
            .with_dataset(dataset(
                "from: https://example.com/c.parquet\nname: corpus\n",
            ))
            .with_view(View::new("test_queries".to_string()).with_sql("SELECT 1"))
            .build();
        let err = validate_required_tables(&app).expect_err("relevance_data is missing");
        assert!(err.to_string().contains("relevance_data"), "{err}");

        let app = AppBuilder::new("t")
            .with_dataset(dataset(
                "from: https://example.com/c.parquet\nname: corpus\n",
            ))
            .with_view(View::new("test_queries".to_string()).with_sql("SELECT 1"))
            .with_view(View::new("relevance_data".to_string()).with_sql("SELECT 1"))
            .build();
        validate_required_tables(&app).expect("all tables present");
    }
}
