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

use crate::acceleration::wait_for_checkpoints;
use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::RecordBatch;
use runtime::{Runtime, component::dataset::builder::DatasetBuilder};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
};
use std::sync::Arc;
use turso::Connection;

use crate::{
    acceleration::get_params,
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

fn get_dataset() -> Dataset {
    Dataset::new("https://public-data.spiceai.org/decimal.parquet", "decimal")
}

#[tokio::test]
async fn test_acceleration_turso_checkpoint() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let mut dataset = get_dataset();
            dataset.acceleration = Some(Acceleration {
                params: get_params(&Mode::File, Some("./decimal_turso.db".to_string()), "turso"),
                enabled: true,
                engine: Some("turso".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                refresh_sql: None,
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_acceleration_turso_metadata")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let app_ref = rt.app();
            let app_lock = app_ref.read().await;
            let Some(app) = app_lock.as_ref() else {
                return Err(anyhow!("Failed to obtain app from runtime"));
            };

            let cloned_rt = Arc::clone(&rt);
            let runtime_datasets = app
                .datasets
                .clone()
                .into_iter()
                .map(DatasetBuilder::try_from)
                .map(move |ds_builder| {
                    ds_builder
                        .map_err(|e| anyhow!("Failed to create dataset builder: {e}"))
                        .and_then(|ds_builder| {
                            ds_builder
                                .with_app(Arc::clone(app))
                                .with_runtime(Arc::clone(&cloned_rt))
                                .build()
                                .map_err(|e| anyhow!("Failed to build dataset: {e}"))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify checkpoints are created before shutting down runtime
            wait_for_checkpoints(runtime_datasets, 120).await?;

            rt.shutdown().await;
            drop(rt);

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // Connect to Turso database using libsql
            let db = turso::Builder::new_local("./decimal_turso.db")
                .build()
                .await
                .map_err(|e| anyhow!("Failed to build libsql database: {e}"))?;
            let conn = db
                .connect()
                .map_err(|e| anyhow!("Failed to connect to libsql database: {e}"))?;

            // Query checkpoint table
            let checkpoint_result = query_to_record_batches(
                &conn,
                "SELECT dataset_name FROM spice_sys_dataset_checkpoint",
            )
            .await?;

            let pretty = arrow::util::pretty::pretty_format_batches(&checkpoint_result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            // Query persisted data
            let persisted_records =
                query_to_record_batches(&conn, "SELECT * FROM decimal ORDER BY id").await?;

            let persisted_records_pretty =
                arrow::util::pretty::pretty_format_batches(&persisted_records)
                    .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(persisted_records_pretty);

            // Remove the file
            std::fs::remove_file("./decimal_turso.db").expect("remove file");

            Ok(())
        })
        .await
}

/// Helper function to convert libsql query results to Arrow `RecordBatches`
async fn query_to_record_batches(
    conn: &Connection,
    query: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};

    let mut stmt = conn
        .prepare(query)
        .await
        .map_err(|e| anyhow!("Failed to prepare statement: {e}"))?;
    let mut rows = stmt
        .query(())
        .await
        .map_err(|e| anyhow!("Failed to query: {e}"))?;

    let mut all_rows: Vec<Vec<turso::Value>> = Vec::new();

    // Collect all rows
    while let Some(row) = rows
        .next()
        .await
        .map_err(|e| anyhow!("Failed to fetch row: {e}"))?
    {
        let column_count = row.column_count();
        let mut row_values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let value = row
                .get_value(i)
                .map_err(|e| anyhow!("Failed to get value: {e}"))?;
            row_values.push(value);
        }
        all_rows.push(row_values);
    }

    if all_rows.is_empty() {
        // Return empty batch with inferred schema from the first query
        // For checkpoint query, we know it has dataset_name column
        let schema = if query.contains("spice_sys_dataset_checkpoint") {
            Arc::new(Schema::new(vec![Field::new(
                "dataset_name",
                DataType::Utf8,
                true,
            )]))
        } else {
            // For decimal table, infer from column names
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("value", DataType::Float64, true),
            ]))
        };
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    // Get column names from statement
    let column_count = all_rows[0].len();
    let mut fields = Vec::new();

    // Re-prepare statement to get column names
    let stmt = conn
        .prepare(query)
        .await
        .map_err(|e| anyhow!("Failed to prepare statement for column names: {e}"))?;

    let columns = stmt.columns();

    for i in 0..column_count {
        let field_name = columns
            .get(i)
            .map_or_else(|| format!("column_{i}"), |col| col.name().to_string());
        let data_type = match &all_rows[0][i] {
            turso::Value::Integer(_) => DataType::Int64,
            turso::Value::Real(_) => DataType::Float64,
            turso::Value::Null | turso::Value::Text(_) => DataType::Utf8,
            turso::Value::Blob(_) => DataType::Binary,
        };
        fields.push(Field::new(field_name, data_type, true));
    }

    let schema = Arc::new(Schema::new(fields));

    // Convert to Arrow arrays
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    for col_idx in 0..column_count {
        let column: Arc<dyn arrow::array::Array> = match &all_rows[0][col_idx] {
            turso::Value::Integer(_) => {
                let values: Vec<Option<i64>> = all_rows
                    .iter()
                    .map(|row| match &row[col_idx] {
                        turso::Value::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            turso::Value::Real(_) => {
                let values: Vec<Option<f64>> = all_rows
                    .iter()
                    .map(|row| match &row[col_idx] {
                        turso::Value::Real(f) => Some(*f),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            turso::Value::Text(_) => {
                let values: Vec<Option<String>> = all_rows
                    .iter()
                    .map(|row| match &row[col_idx] {
                        turso::Value::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            turso::Value::Blob(_) => {
                let values: Vec<Option<&[u8]>> = all_rows
                    .iter()
                    .map(|row| match &row[col_idx] {
                        turso::Value::Blob(b) => Some(b.as_slice()),
                        _ => None,
                    })
                    .collect();
                Arc::new(BinaryArray::from(values))
            }
            turso::Value::Null => {
                let values: Vec<Option<String>> = all_rows.iter().map(|_row| None).collect();
                Arc::new(StringArray::from(values))
            }
        };
        columns.push(column);
    }

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| anyhow!("Failed to create record batch: {e}"))?;

    Ok(vec![batch])
}
