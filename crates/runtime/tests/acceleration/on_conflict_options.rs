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

use crate::utils::{
    runtime_ready_check, runtime_ready_check_with_timeout_err, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};
use app::AppBuilder;
use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionContext, col, lit};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, OnConflictBehavior};
use spicepod::component::dataset::Dataset;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;
use std::time::Duration;
use test_framework::serde_yaml;

/// Builder for creating test `RecordBatches` with specific data patterns
#[derive(Debug)]
pub struct TestCsvDataBuilder {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    temp_file: tempfile::NamedTempFile,
}

impl TestCsvDataBuilder {
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            temp_file: tempfile::NamedTempFile::with_suffix(".csv")
                .expect("Failed to create temp file"),
        }
    }

    #[must_use]
    pub fn with_columns(columns: &[&str]) -> Self {
        let fields: Vec<Field> = columns
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Self::new(schema)
    }

    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn add_batch(self, data: &[Vec<Option<&str>>]) -> Self {
        let columns: Vec<ArrayRef> = (0..self.schema.fields().len())
            .map(|col_idx| {
                let column_data: Vec<Option<String>> = data
                    .iter()
                    .map(|row| row.get(col_idx).and_then(|v| v.map(ToString::to_string)))
                    .collect();
                Arc::new(StringArray::from(column_data)) as ArrayRef
            })
            .collect();

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .expect("Failed to create RecordBatch");
        let mut new_self = self;
        new_self.batches.push(batch);
        new_self
    }

    #[must_use]
    pub fn dataset(&self, name: &str) -> Dataset {
        let from = format!("file://{}", self.temp_file.path().display());
        Dataset::new(from, name)
    }

    #[allow(clippy::expect_used)]
    pub fn flush_to_file(&mut self) {
        let file = self.temp_file.as_file_mut();
        file.set_len(0).expect("Failed to truncate temp file");
        file.seek(SeekFrom::Start(0))
            .expect("Failed to seek to start");
        let mut writer = arrow::csv::Writer::new(file);
        for batch in &self.batches {
            writer.write(batch).expect("Failed to write batch");
        }
    }
}

/// Test the behavior of the vanilla `upsert` when there are duplicates in the incoming batch.
/// The behavior will be to error out, preventing the runtime from becoming ready because the acceleration isn't loaded.
#[tokio::test]
async fn test_acceleration_on_conflict_same_batch_upsert() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut data_builder =
                TestCsvDataBuilder::with_columns(&["foo", "bar"]).add_batch(&[
                    vec![Some("a"), Some("bar1")], // a, bar1
                    vec![Some("b"), Some("bar2")], // b, bar2
                    vec![Some("c"), Some("bar3")], // c, bar3
                    vec![Some("a"), Some("bar1")], // a, bar1 # conflict
                    vec![Some("e"), Some("bar5")], // e, bar5
                    vec![Some("f"), Some("bar6")], // f, bar6
                ]);
            data_builder.flush_to_file();

            let dataset = data_builder.dataset("test");
            let dataset = set_duckdb_acceleration(dataset);
            let dataset = set_primary_key(dataset, "foo");
            let dataset = set_on_conflict_behavior(dataset, OnConflictBehavior::Upsert);
            let app = AppBuilder::new("test_acceleration_on_conflict_same_batch_upsert")
                .with_dataset(dataset)
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            // Only wait 10 seconds, since we don't expect the runtime to become ready.
            assert!(
                runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(10))
                    .await
                    .is_err(),
                "Expected the runtime to error when loading data that violates the incoming batch constraints with only upsert"
            );
        })
        .await;

    Ok::<(), anyhow::Error>(())
}

/// Test the behavior of `upsert_dedup` when there are exact duplicates in the incoming batch.
/// The runtime will successfully load and the data will be deduplicated.
#[tokio::test]
async fn test_acceleration_on_conflict_same_batch_upsert_with_dedup() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut data_builder = TestCsvDataBuilder::with_columns(&["foo", "bar"]).add_batch(&[
                vec![Some("a"), Some("bar1")], // a, bar1
                vec![Some("b"), Some("bar2")], // b, bar2
                vec![Some("c"), Some("bar3")], // c, bar3
                vec![Some("a"), Some("bar1")], // a, bar1 # conflict
                vec![Some("e"), Some("bar5")], // e, bar5
                vec![Some("f"), Some("bar6")], // f, bar6
            ]);
            data_builder.flush_to_file();

            let dataset = data_builder.dataset("test");
            let dataset = set_duckdb_acceleration(dataset);
            let dataset = set_primary_key(dataset, "foo");
            let dataset = set_on_conflict_behavior(dataset, OnConflictBehavior::UpsertDedup);
            let app = AppBuilder::new("test_acceleration_on_conflict_same_batch_upsert_with_dedup")
                .with_dataset(dataset)
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = get_query_result(&rt, "SELECT * FROM test").await;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 5);
            assert_value(result, "foo", "a", "bar", "bar1").await;
        })
        .await;

    Ok::<(), anyhow::Error>(())
}

/// Test the behavior of `upsert_dedup` when there are conflicts in the incoming batch, but not exact duplicates.
/// The runtime should error since the de-duplication isn't sufficient.
#[tokio::test]
async fn test_acceleration_on_conflict_same_batch_upsert_with_dedup_not_exact_duplicates()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut data_builder =
                TestCsvDataBuilder::with_columns(&["foo", "bar"]).add_batch(&[
                    vec![Some("a"), Some("bar1")],  // a, bar1
                    vec![Some("b"), Some("bar2")],  // b, bar2
                    vec![Some("c"), Some("bar3")],  // c, bar3
                    vec![Some("a"), Some("bar10")], // a, bar10 # conflict
                    vec![Some("e"), Some("bar5")],  // e, bar5
                    vec![Some("f"), Some("bar6")],  // f, bar6
                ]);
            data_builder.flush_to_file();

            let dataset = data_builder.dataset("test");
            let dataset = set_duckdb_acceleration(dataset);
            let dataset = set_primary_key(dataset, "foo");
            let dataset = set_on_conflict_behavior(dataset, OnConflictBehavior::UpsertDedup);
            let app = AppBuilder::new("test_acceleration_on_conflict_same_batch_upsert_with_dedup_not_exact_duplicates")
                .with_dataset(dataset)
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            // Only wait 10 seconds, since we don't expect the runtime to become ready.
            assert!(
                runtime_ready_check_with_timeout_err(&rt, Duration::from_secs(10))
                    .await
                    .is_err(),
                "Expected the runtime to error when loading data that violates the incoming batch constraints with upsert_dedup and there aren't exact duplicates"
            );
        })
        .await;

    Ok::<(), anyhow::Error>(())
}

/// Test the behavior of `upsert_dedup_by_row_id` when there are conflicts in the incoming batch, but not exact duplicates.
/// The runtime will successfully load the data and the conflicting rows will resolve to the last one.
#[tokio::test]
async fn test_acceleration_on_conflict_same_batch_upsert_with_dedup_by_row_id_not_exact_duplicates()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut data_builder =
                TestCsvDataBuilder::with_columns(&["foo", "bar"]).add_batch(&[
                    vec![Some("a"), Some("bar1")],  // a, bar1
                    vec![Some("b"), Some("bar2")],  // b, bar2
                    vec![Some("c"), Some("bar3")],  // c, bar3
                    vec![Some("a"), Some("bar10")], // a, bar10 # conflict
                    vec![Some("e"), Some("bar5")],  // e, bar5
                    vec![Some("f"), Some("bar6")],  // f, bar6
                ]);
            data_builder.flush_to_file();

            let dataset = data_builder.dataset("test");
            let dataset = set_duckdb_acceleration(dataset);
            let dataset = set_primary_key(dataset, "foo");
            let dataset = set_on_conflict_behavior(dataset, OnConflictBehavior::UpsertDedupByRowId);
            let app = AppBuilder::new("test_acceleration_on_conflict_same_batch_upsert_with_dedup_by_row_id_not_exact_duplicates")
                .with_dataset(dataset)
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let result = get_query_result(&rt, "SELECT * FROM test").await;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 5);
            assert_value(result, "foo", "a", "bar", "bar10").await;
        })
        .await;

    Ok::<(), anyhow::Error>(())
}

#[allow(clippy::expect_used)]
async fn get_query_result(rt: &Runtime, sql: &str) -> Vec<RecordBatch> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .expect("Failed to run query")
        .data
        .try_collect()
        .await
        .expect("Failed to collect query results")
}

#[allow(clippy::expect_used)]
async fn assert_value(
    batches: Vec<RecordBatch>,
    key_name: &str,
    key_value: &str,
    target_col: &str,
    target_col_expected_value: &str,
) {
    let ctx = SessionContext::new();
    let df = ctx
        .read_batches(batches)
        .expect("Failed to create DataFrame from batches");
    let df = df
        .filter(col(key_name).eq(lit(key_value)))
        .expect("Failed to filter DataFrame");
    let df = df
        .select_columns(&[target_col])
        .expect("Failed to select target column");
    let result = df
        .collect()
        .await
        .expect("Failed to collect filtered DataFrame");
    assert_eq!(result.len(), 1, "Expected one batch in result");
    let result = result.into_iter().next().expect("Expected one batch");
    let value = result.column(0).as_string::<i32>().value(0);
    assert_eq!(value, target_col_expected_value);
}

#[allow(clippy::expect_used)]
fn set_duckdb_acceleration(mut dataset: Dataset) -> Dataset {
    let yaml = r"
                enabled: true
                engine: duckdb
                mode: memory
            ";
    let acceleration: Acceleration =
        serde_yaml::from_str(yaml).expect("Failed to parse Acceleration");
    dataset.acceleration = Some(acceleration);
    dataset
}

#[allow(clippy::expect_used)]
fn set_primary_key(mut dataset: Dataset, primary_key: &str) -> Dataset {
    let mut acceleration = dataset
        .acceleration
        .take()
        .expect("Failed to get Acceleration");
    acceleration.primary_key = Some(primary_key.to_string());
    dataset.acceleration = Some(acceleration);
    dataset
}

#[allow(clippy::expect_used)]
fn set_on_conflict_behavior(
    mut dataset: Dataset,
    on_conflict_behavior: OnConflictBehavior,
) -> Dataset {
    let mut acceleration = dataset
        .acceleration
        .take()
        .expect("Failed to get Acceleration");
    let primary_key = acceleration
        .primary_key
        .clone()
        .expect("No primary key set");
    acceleration.on_conflict = HashMap::from([(primary_key, on_conflict_behavior)]);
    dataset.acceleration = Some(acceleration);
    dataset
}
