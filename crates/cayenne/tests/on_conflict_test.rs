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
#![allow(clippy::clone_on_ref_ptr)]

//! Tests for primary key on-conflict handling in Cayenne.

mod common;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

// Run against all supported backends.
test_with_backends!(test_on_conflict_upsert_impl);

async fn test_on_conflict_upsert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "conflict_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string()
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    ctx.register_table(
        "conflict_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Initial insert.
    ctx.sql("INSERT INTO conflict_upsert VALUES (1, 'Alice'), (2, 'Bob')")
        .await?
        .collect()
        .await?;

    // Second insert with conflicting primary key (id=1) should upsert and drop old row.
    ctx.sql("INSERT INTO conflict_upsert VALUES (1, 'Updated')")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, name FROM conflict_upsert ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("id column");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("name column");

    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(names.value(0), "Updated");
    assert_eq!(names.value(1), "Bob");

    Ok(())
}
