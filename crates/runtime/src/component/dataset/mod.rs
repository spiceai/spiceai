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

use app::App;
use datafusion::sql::TableReference;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::{Runtime, dataaccelerator::AccelerationSource};

pub mod builder;

#[cfg(test)]
mod moved_tests;

// Config-only spec, config types, and config-only submodules live in
// `runtime-component`. Re-exported here for path compatibility
// (`crate::component::dataset::{DatasetSpec, Acceleration, acceleration, ...}`).
pub use runtime_component::dataset::{
    CheckAvailability, DatasetSpec, Error, FullTextSearchDatasetConfig, InvalidColumnTypeSnafu,
    InvalidConfigurationSnafu, OnSchemaChange, ReadyState, Result, TimeFormat,
    UnsupportedTypeAction, acceleration, declared_schema, declared_type, metadata, replication,
    schema_inference,
};
// `validate_identifier` is used by `builder` via `super::validate_identifier`.
use runtime_component::dataset::acceleration::Acceleration;
use runtime_component::validate_identifier;

/// `Arc<Runtime>`-bound wrapper over a [`DatasetSpec`]. Derefs to the spec so
/// `dataset.acceleration`, `dataset.columns`, `dataset.refresh_sql()`, etc. keep
/// working unchanged; `app`/`runtime` are the runtime handles the spec omits.
#[derive(Clone)]
pub struct Dataset {
    pub spec: DatasetSpec,
    pub app: Arc<App>,
    pub runtime: Arc<Runtime>,
}

impl Deref for Dataset {
    type Target = DatasetSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl DerefMut for Dataset {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
}

impl std::fmt::Debug for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dataset")
            .field("from", &self.from)
            .field("name", &self.name)
            .field("access", &self.access)
            .field("params", &self.params)
            .field("metadata", &self.metadata)
            .field("columns", &self.columns)
            .field("schema", &self.schema)
            .field("has_metadata_table", &self.has_metadata_table)
            .field("replication", &self.replication)
            .field("time_column", &self.time_column)
            .field("time_format", &self.time_format)
            .field("time_partition_column", &self.time_partition_column)
            .field("time_partition_format", &self.time_partition_format)
            .field("acceleration", &self.acceleration)
            .field("embeddings", &self.embeddings)
            .field("app", &self.app)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("on_schema_change", &self.on_schema_change)
            .field("ready_state", &self.ready_state)
            .field("metrics", &self.metrics)
            .field("vectors", &self.vectors)
            .field("full_text_search", &self.full_text_search)
            .field("drasi", &self.drasi)
            .field("check_availability", &self.check_availability)
            .field(
                "check_availability_interval",
                &self.check_availability_interval,
            )
            .finish_non_exhaustive()
    }
}

// Equality ignores the `app`/`runtime` handles — they are not part of a
// dataset's identity — so the runtime can compare datasets like-for-like across
// App reloads. It delegates to `DatasetSpec`'s configuration comparison.
impl PartialEq for Dataset {
    fn eq(&self, other: &Self) -> bool {
        self.spec == other.spec
    }
}

impl Dataset {
    #[must_use]
    pub fn app(&self) -> Arc<App> {
        Arc::clone(&self.app)
    }

    #[must_use]
    pub fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub fn with_params(mut self, params: std::collections::HashMap<String, String>) -> Self {
        self.spec.params = params;
        self
    }

    #[expect(clippy::result_large_err)]
    pub(crate) fn parse_table_reference(
        name: &str,
    ) -> std::result::Result<TableReference, crate::Error> {
        match TableReference::parse_str(name) {
            table_ref @ (TableReference::Bare { .. } | TableReference::Partial { .. }) => {
                Ok(table_ref)
            }
            TableReference::Full { catalog, .. } => crate::DatasetNameIncludesCatalogSnafu {
                catalog,
                name: name.to_string(),
            }
            .fail(),
        }
    }

    #[must_use]
    pub async fn is_accelerator_initialized(&self) -> bool {
        if let Some(acceleration_settings) = &self.acceleration {
            let Some(accelerator) = self
                .runtime()
                .accelerator_engine_registry()
                .get_accelerator_engine(acceleration_settings.engine)
                .await
            else {
                return false; // if the accelerator engine is not found, it's impossible for it to be initialized
            };

            return accelerator.is_initialized(self);
        }

        false
    }
}

impl AccelerationSource for Dataset {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(self.clone())
    }

    fn is_file_accelerated(&self) -> bool {
        // Deref resolves to `DatasetSpec::is_file_accelerated`.
        DatasetSpec::is_file_accelerated(self)
    }

    fn app(&self) -> Arc<app::App> {
        self.app()
    }

    fn secrets(&self) -> Arc<tokio::sync::RwLock<crate::secrets::Secrets>> {
        self.runtime.secrets()
    }

    fn acceleration(&self) -> Option<&Acceleration> {
        self.acceleration.as_ref()
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn connector_name(&self) -> Option<&str> {
        // `DatasetSpec::source()` is the authoritative `from:` parse: it recognizes
        // `://`, `:` AND `/` as delimiters and maps an empty value to `sink`.
        Some(DatasetSpec::source(self))
    }

    fn on_schema_change(&self) -> Option<OnSchemaChange> {
        Some(self.on_schema_change)
    }

    fn allows_write(&self) -> bool {
        // A read-write dataset requires BOTH `access: read_write` and a ReadWrite API
        // key, and `access()` is the check that folds those together.
        self.access().allows_write()
    }

    fn time_column(&self) -> Option<&str> {
        self.time_column.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialized_sources<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Vec<Arc<dyn runtime_acceleration::AccelerationSource>>>
                + Send
                + 'a,
        >,
    > {
        let app = self.app();
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move {
            let datasets: Vec<Arc<dyn runtime_acceleration::AccelerationSource>> =
                Arc::clone(&runtime)
                    .get_initialized_datasets(&app, crate::LogErrors(false))
                    .await
                    .into_iter()
                    .map(|ds| ds as Arc<dyn runtime_acceleration::AccelerationSource>)
                    .collect();
            #[cfg(feature = "duckdb")]
            {
                let views: Vec<Arc<dyn runtime_acceleration::AccelerationSource>> =
                    Arc::clone(&runtime)
                        .get_initialized_views(&app, crate::LogErrors(false))
                        .await
                        .into_iter()
                        .map(|v| v as Arc<dyn runtime_acceleration::AccelerationSource>)
                        .collect();
                datasets.into_iter().chain(views).collect()
            }
            #[cfg(not(feature = "duckdb"))]
            datasets
        })
    }

    fn checkpointer_factory(
        &self,
        snapshot_behavior: runtime_acceleration::snapshot::SnapshotBehavior,
    ) -> runtime_acceleration::dataset_checkpoint::DatasetCheckpointerFactory {
        crate::dataaccelerator::spice_sys::checkpointer_factory(
            self,
            self.runtime.accelerator_engine_registry(),
            snapshot_behavior,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_table_providers::util::column_reference::ColumnReference;

    use super::acceleration::{Acceleration, IndexType};
    use super::builder::DatasetBuilder;
    use super::*;
    use app::AppBuilder;
    use spicepod::{
        fts::FtsStore,
        param::Params,
        semantic::{ColumnLevelEmbeddingConfig, FullTextSearchConfig},
        vector::VectorStore,
    };

    #[test]
    fn test_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("foo".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::hashmap_to_option_string(&indexes_map);
        assert!(indexes_str == "foo:enabled;bar:unique" || indexes_str == "bar:unique;foo:enabled");
        let roundtrip_indexes_map: HashMap<String, IndexType> =
            datafusion_table_providers::util::hashmap_from_option_string(&indexes_str);

        let roundtrip_indexes_map = roundtrip_indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        let indexes_map = indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        assert_eq!(indexes_map, roundtrip_indexes_map);
    }

    #[test]
    fn test_compound_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("(foo, bar)".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::hashmap_to_option_string(&indexes_map);
        assert!(
            indexes_str == "(foo, bar):enabled;bar:unique"
                || indexes_str == "bar:unique;(foo, bar):enabled"
        );
        let roundtrip_indexes_map: HashMap<String, IndexType> =
            datafusion_table_providers::util::hashmap_from_option_string(&indexes_str);

        let roundtrip_indexes_map = roundtrip_indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        let indexes_map = indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        assert_eq!(indexes_map, roundtrip_indexes_map);
    }

    #[test]
    fn test_get_index_columns() {
        let column_ref = ColumnReference::try_from("foo").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["foo"]);

        let column_ref = ColumnReference::try_from("(foo, bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let column_ref = ColumnReference::try_from("(foo,bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let err = ColumnReference::try_from("(foo,bar").expect_err("invalid columns");
        assert_eq!(
            err.to_string(),
            "The column reference \"(foo,bar\" is missing a closing parenthensis."
        );
    }

    async fn create_dataset_with_params(params: HashMap<String, String>) -> Dataset {
        let spicepod_dataset =
            spicepod::component::dataset::Dataset::new("test".to_string(), "test".to_string());

        let app = AppBuilder::new("test")
            .with_dataset(spicepod_dataset.clone())
            .build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("valid dataset");

        dataset.params = params;
        dataset
    }

    fn params(entries: &[(&str, &str)]) -> Params {
        Params::from_string_map(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
    }

    async fn build_dataset(
        spicepod_dataset: spicepod::component::dataset::Dataset,
        app: app::App,
    ) -> Result<Dataset> {
        let runtime = crate::Runtime::builder().build().await;

        DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
    }

    #[tokio::test]
    async fn test_dataset_level_search_engine_configuration_is_preserved() {
        let app = AppBuilder::new("test").build();

        let mut spicepod_dataset =
            spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        spicepod_dataset.vectors = Some(VectorStore {
            enabled: true,
            engine: Some("elasticsearch".to_string()),
            partition_by: Vec::new(),
            params: Some(params(&[("endpoint", "http://es:9200"), ("metric", "dot")])),
        });
        spicepod_dataset.full_text_search = Some(FtsStore {
            enabled: true,
            engine: Some("elasticsearch".to_string()),
            params: Some(params(&[
                ("endpoint", "http://es:9200"),
                ("index", "docs_text"),
            ])),
        });

        let dataset = build_dataset(spicepod_dataset, app)
            .await
            .expect("direct search engine configuration should build");

        let vector_store = dataset.vectors.as_ref().expect("vectors should be enabled");
        assert_eq!(vector_store.engine.as_deref(), Some("elasticsearch"));
        let vector_params = vector_store
            .params
            .as_ref()
            .expect("vector params should merge")
            .as_string_map();
        assert_eq!(
            vector_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(vector_params.get("metric").map(String::as_str), Some("dot"));

        let fts_store = dataset
            .full_text_search
            .as_ref()
            .expect("full text search should be enabled");
        assert_eq!(fts_store.engine.as_deref(), Some("elasticsearch"));
        let fts_params = fts_store
            .params
            .as_ref()
            .expect("fts params should merge")
            .as_string_map();
        assert_eq!(
            fts_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(
            fts_params.get("index").map(String::as_str),
            Some("docs_text")
        );
    }

    #[tokio::test]
    async fn test_column_level_search_engine_overrides_enable_stores() {
        let app = AppBuilder::new("test").build();

        let mut column_fts = FullTextSearchConfig::enabled().with_row_id("id");
        column_fts.engine = Some("elasticsearch".to_string());
        column_fts.params = Some(params(&[
            ("endpoint", "http://es:9200"),
            ("index", "body_text"),
        ]));

        let mut spicepod_dataset =
            spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        spicepod_dataset.columns = vec![
            spicepod::semantic::Column::new("body")
                .with_embedding(ColumnLevelEmbeddingConfig {
                    model: "openai_embeddings".to_string(),
                    chunking: None,
                    row_ids: Some(vec!["id".to_string()]),
                    vector_size: None,
                    engine: Some("elasticsearch".to_string()),
                    params: Some(params(&[
                        ("endpoint", "http://es:9200"),
                        ("index", "body_vectors"),
                    ])),
                    aggregation: None,
                    max_elements_per_row: None,
                })
                .with_full_text_search(column_fts),
        ];

        let dataset = build_dataset(spicepod_dataset, app)
            .await
            .expect("column search engine overrides should build");

        let vector_store = dataset.vectors.as_ref().expect("vectors should be enabled");
        assert!(vector_store.enabled);
        assert_eq!(vector_store.engine, None);

        let column_embedding = dataset.columns[0]
            .embeddings
            .first()
            .expect("embedding should remain on column");
        assert_eq!(column_embedding.engine.as_deref(), Some("elasticsearch"));
        let embedding_params = column_embedding
            .params
            .as_ref()
            .expect("embedding params should merge")
            .as_string_map();
        assert_eq!(
            embedding_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(
            embedding_params.get("index").map(String::as_str),
            Some("body_vectors")
        );

        let fts_store = dataset
            .full_text_search
            .as_ref()
            .expect("column text engine should enable dataset fts store");
        assert_eq!(fts_store.engine.as_deref(), Some("elasticsearch"));
        let fts_store_params = fts_store
            .params
            .as_ref()
            .expect("column fts params should be promoted")
            .as_string_map();
        assert_eq!(
            fts_store_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(
            fts_store_params.get("index").map(String::as_str),
            Some("body_text")
        );

        let column_fts = dataset.columns[0]
            .full_text_search
            .as_ref()
            .expect("column fts config should remain");
        assert_eq!(column_fts.engine.as_deref(), Some("elasticsearch"));
    }

    #[tokio::test]
    async fn test_mixed_column_fts_params_error() {
        let app = AppBuilder::new("test").build();

        let mut first_fts = FullTextSearchConfig::enabled().with_row_id("id");
        first_fts.engine = Some("elasticsearch".to_string());
        first_fts.params = Some(params(&[("index", "body_text")]));

        let mut second_fts = FullTextSearchConfig::enabled().with_row_id("id");
        second_fts.engine = Some("elasticsearch".to_string());
        second_fts.params = Some(params(&[("index", "title_text")]));

        let mut spicepod_dataset =
            spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        spicepod_dataset.columns = vec![
            spicepod::semantic::Column::new("body").with_full_text_search(first_fts),
            spicepod::semantic::Column::new("title").with_full_text_search(second_fts),
        ];

        let err = build_dataset(spicepod_dataset, app)
            .await
            .expect_err("mixed column fts params should fail safely");

        assert!(
            err.to_string().contains("different parameters"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_get_dataset_param() {
        // Test case 1: Parameter is not set
        let dataset = create_dataset_with_params(HashMap::new()).await;
        assert!(dataset.get_param("test_param", true));
        assert!(!dataset.get_param("test_param", false));

        // Test case 2: Parameter is set to "true"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "true".to_string());
        let dataset = create_dataset_with_params(params).await;
        assert!(dataset.get_param("test_param", false));

        // Test case 3: Parameter is set to "false"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "false".to_string());
        let dataset = create_dataset_with_params(params).await;
        assert!(!dataset.get_param("test_param", true));

        // Test case 4: Parameter is set to an invalid boolean value
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "not_a_bool".to_string());
        let dataset = create_dataset_with_params(params).await;
        assert!(dataset.get_param("test_param", true));
        assert!(!dataset.get_param("test_param", false));

        // Test case 5: App is None
        assert!(dataset.get_param("test_param", true));
        assert!(!dataset.get_param("test_param", false));
    }

    #[tokio::test]
    async fn test_source() {
        let test_cases = vec![
            // Basic delimiter cases
            ("foo:bar", "foo"),
            ("foo/bar", "foo"),
            ("foo://bar", "foo"),
            // Empty and sink cases
            ("", "sink"),
            ("sink", "sink"),
            ("sink:", "sink"),
            ("sink/", "sink"),
            ("sink://", "sink"),
            // No delimiter case
            ("foo", "spice.ai"),
            // Multiple delimiters - should use first occurrence
            ("foo:bar:baz", "foo"),
            ("foo/bar/baz", "foo"),
            ("foo://bar://baz", "foo"),
            // Mixed delimiters - should handle "://" first
            ("foo://bar:baz", "foo"),
            ("foo://bar/baz", "foo"),
            ("foo:bar//baz", "foo"),
            ("foo/bar://baz", "foo"),
            // Edge cases with delimiters
            ("://bar", ""),
            (":bar", ""),
            ("/bar", ""),
            ("//bar", ""),
            // Common real-world patterns
            ("mysql://localhost", "mysql"),
            ("http://example.com", "http"),
            ("https://api.example.com", "https"),
            ("postgresql://localhost", "postgresql"),
            ("s3://bucket", "s3"),
            ("file:/path", "file"),
            ("snowflake://account", "snowflake"),
            // Special characters
            ("foo-bar:baz", "foo-bar"),
            ("foo_bar:baz", "foo_bar"),
            ("foo.bar:baz", "foo.bar"),
            // Unicode characters
            ("über:data", "über"),
            ("数据:source", "数据"),
            // Whitespace handling
            ("  foo:bar", "  foo"),
            ("foo  :bar", "foo  "),
            ("\tfoo:bar", "\tfoo"),
        ];

        for (input, expected) in test_cases {
            let app = app::AppBuilder::new("test").build();
            let rt = crate::Runtime::builder().build().await;

            let dataset = DatasetBuilder::try_new(input.to_string(), "test")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");
            assert_eq!(dataset.source(), expected, "Failed for input: {input}");
        }
    }

    #[tokio::test]
    async fn test_path() {
        let test_cases = vec![
            // Basic delimiter cases
            ("foo:bar", "bar"),
            ("foo/bar", "bar"),
            ("foo://bar", "bar"),
            // Empty cases
            ("", ""),
            (":", ""),
            ("/", ""),
            ("://", ""),
            // Multiple delimiters - should use first occurrence
            ("foo:bar:baz", "bar:baz"),
            ("foo/bar/baz", "bar/baz"),
            ("foo://bar://baz", "bar://baz"),
            // Mixed delimiters - should handle "://" first
            ("foo://bar:baz", "bar:baz"),
            ("foo://bar/baz", "bar/baz"),
            ("foo:bar//baz", "bar//baz"),
            ("foo/bar://baz", "bar://baz"),
            // Edge cases with delimiters
            ("://bar", "bar"),
            (":bar", "bar"),
            ("/bar", "bar"),
            ("//bar", "/bar"),
            // Common real-world patterns
            ("mysql://localhost:3306", "localhost:3306"),
            ("http://example.com/path", "example.com/path"),
            ("https://api.example.com/v1", "api.example.com/v1"),
            ("postgresql://localhost:5432/db", "localhost:5432/db"),
            ("s3://bucket/key", "bucket/key"),
            ("file:/path/to/file", "/path/to/file"),
            ("file:///path/to/file", "/path/to/file"),
            ("file://path/to/file", "path/to/file"),
            ("snowflake://account/db/schema", "account/db/schema"),
            // Special characters
            ("foo-bar:baz-qux", "baz-qux"),
            ("foo_bar:baz_qux", "baz_qux"),
            ("foo.bar:baz.qux", "baz.qux"),
            // Unicode characters
            ("source:数据", "数据"),
            ("来源:数据", "数据"),
            // Whitespace handling
            ("foo:  bar", "  bar"),
            ("foo:bar  ", "bar  "),
            ("foo:\tbar", "\tbar"),
            ("foo:\nbar", "\nbar"),
            // Query parameters
            ("mysql://host/db?param=value", "host/db?param=value"),
            ("http://example.com?q=1&r=2", "example.com?q=1&r=2"),
            // Authentication information
            ("mysql://user:pass@host/db", "user:pass@host/db"),
            ("https://token@api.com", "token@api.com"),
        ];

        for (input, expected) in test_cases {
            let app = app::AppBuilder::new("test").build();
            let rt = crate::Runtime::builder().build().await;

            let dataset = DatasetBuilder::try_new(input.to_string(), "test")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");
            assert_eq!(dataset.path(), expected, "Failed for input: {input}");
        }
    }
}
