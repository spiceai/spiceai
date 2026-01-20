/*
Copyright 2025 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]

//! Tests covering retention filter application at write time.

mod common;

use arrow::array::{Int64Array, RecordBatch};

use arrow::datatypes::{DataType, Field, Schema};

use cayenne::metadata::CreateTableOptions;

use cayenne::{CayenneTableProvider, MetadataCatalog};

use common::TestFixture;

use datafusion::datasource::TableProvider;

use datafusion::prelude::*;

use datafusion_execution::SendableRecordBatchStream;

use std::sync::Arc;

test_with_backends!(test_retention_filters_apply_on_insert_impl);
test_with_backends!(test_retention_filters_skip_when_no_matches_impl);

async fn test_retention_filters_apply_on_insert_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("retention_apply");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "retention_apply".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let retention_expr = col("value").lt(lit(3i64));
    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider = Arc::new(
        CayenneTableProvider::create_table_with_retention(
            catalog_arc,
            table_options,
            vec![retention_expr],
        )
        .await?,
    );

    // Insert rows with values 1..=5; retention should mark rows with value < 3 as deleted.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch.clone())]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    let inserted = table_provider.insert(sendable).await?;
    assert_eq!(inserted, 5, "Should insert all rows");

    // Retention should have created a delete file containing row IDs 0 and 1.
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    assert_eq!(
        delete_files.len(),
        1,
        "Expected a single deletion file created by retention"
    );

    let delete_file = &delete_files[0];
    assert_eq!(
        delete_file.delete_count, 2,
        "Expected two rows to be marked as deleted"
    );

    let file = std::fs::File::open(&delete_file.path)?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
    let mut deleted_row_ids = Vec::new();
    for batch in reader {
        let batch = batch?;
        let row_id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row_id column");
        for idx in 0..row_id_array.len() {
            deleted_row_ids.push(row_id_array.value(idx));
        }
    }
    assert_eq!(
        deleted_row_ids,
        vec![0, 1],
        "Retention should delete the first two logical rows"
    );

    // Query via DataFusion to ensure only rows >= 3 remain.
    let ctx = SessionContext::new();
    ctx.register_table(
        "retention_apply",
        Arc::clone(&table_provider) as Arc<dyn TableProvider>,
    )?;
    let df = ctx
        .sql("SELECT value FROM retention_apply ORDER BY value")
        .await?;
    let batches = df.collect().await?;
    let mut remaining_values = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        for idx in 0..values.len() {
            remaining_values.push(values.value(idx));
        }
    }
    assert_eq!(remaining_values, vec![3, 4, 5]);

    Ok(())
}

async fn test_retention_filters_skip_when_no_matches_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = fixture.data_path.join("retention_skip");
    std::fs::create_dir_all(&table_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "retention_skip".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: table_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let retention_expr = col("value").lt(lit(0i64));
    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_provider = CayenneTableProvider::create_table_with_retention(
        catalog_arc,
        table_options,
        vec![retention_expr],
    )
    .await?;

    // Insert rows that do not match the retention predicate.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )?;

    let stream = futures::stream::iter(vec![Ok(batch)]);
    let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        stream,
    );
    let sendable: SendableRecordBatchStream = Box::pin(adapter);

    table_provider.insert(sendable).await?;

    // No delete files should have been created.
    let delete_files = table_provider
        .catalog()
        .get_table_delete_files(table_provider.metadata().table_id)
        .await?;
    assert!(
        delete_files.is_empty(),
        "Retention should not create delete files when no rows match"
    );

    Ok(())
}
