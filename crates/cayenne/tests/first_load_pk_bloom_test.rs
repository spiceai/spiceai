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

//! First-load bloom publication and the next upsert's exact visible rows.

#![cfg(debug_assertions)]
#![expect(
    clippy::expect_used,
    reason = "test assertions require descriptive failures"
)]

use crate::common;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(first_load_bloom_repeated_keys_skip_next_rebuild_impl);
test_with_backends!(first_load_small_exact_keyset_installs_no_bloom_impl);

async fn first_load_bloom_repeated_keys_skip_next_rebuild_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    run_load(fixture, 1, true).await
}

async fn first_load_small_exact_keyset_installs_no_bloom_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    run_load(fixture, 16, false).await
}

async fn run_load(
    fixture: common::TestFixture,
    budget_mb: usize,
    want_bloom: bool,
) -> TestResult<()> {
    // A non-leading composite string/integer PK exercises the rebuild's column
    // order and versioned row encoding, rather than an integer-only shortcut.
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let name = format!("pk_load_{}", uuid::Uuid::now_v7().simple());
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: name.clone(),
                schema: Arc::clone(&schema),
                primary_key: vec!["tenant".to_string(), "id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "tenant".to_string(),
                    "id".to_string(),
                ]))),
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    pk_keyset_cache_mb: Some(budget_mb),
                    inline_max_rows: 0,
                    compaction_background_interval_ms: 0,
                    deletion_mode: DeletionMode::Key,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    let mut model = BTreeMap::new();
    let mut input = Vec::new();
    // Deterministic seeded input, with repeats within and across batches.
    let mut seed = 0x0001_5071_u64;
    for batch_number in 0..8_i64 {
        let mut rows = Vec::new();
        for row_number in 0..8_192_i64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let id = i64::try_from((seed >> 32) % 16_384).expect("seeded id fits i64");
            let tenant = format!("tenant-{}", id % 3);
            let value = batch_number * 8_192 + row_number;
            rows.push((tenant.clone(), id, value));
            model.insert((tenant, id), value);
        }
        input.push(batch(&schema, &rows)?);
    }
    common::insert_batches(table.as_ref(), input).await?;
    let (bloom, rebuilds) = table.first_load_pk_test_observation();
    assert_eq!(
        bloom, want_bloom,
        "first load's table-wide cache representation"
    );
    if want_bloom {
        assert_eq!(
            rebuilds, 0,
            "first load and warming must not rebuild the installed bloom"
        );
    }
    assert_rows(&ctx, &name, &model).await?;

    let updates: Vec<_> = (0..16_416_i64)
        .filter(|id| id % 2 == 0)
        .map(|id| (format!("tenant-{}", id % 3), id, 10_000 + id))
        .collect();
    for (tenant, id, value) in &updates {
        model.insert((tenant.clone(), *id), *value);
    }
    common::insert_batch(table.as_ref(), batch(&schema, &updates)?).await?;
    if want_bloom {
        assert_eq!(
            table.first_load_pk_test_observation(),
            (true, 0),
            "the next append must reuse the bloom without a keyset_rebuild phase"
        );
    }
    assert_rows(&ctx, &name, &model).await?;
    Ok(())
}

fn batch(schema: &Arc<Schema>, rows: &[(String, i64, i64)]) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.0.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )?)
}

async fn assert_rows(
    ctx: &SessionContext,
    name: &str,
    model: &BTreeMap<(String, i64), i64>,
) -> TestResult<()> {
    let batches = ctx
        .sql(&format!(
            "SELECT tenant, id, value FROM {name} ORDER BY tenant, id"
        ))
        .await?
        .collect()
        .await?;
    let mut observed = Vec::new();
    for batch in batches {
        let tenants = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tenant is Utf8");
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is Int64");
        for row in 0..batch.num_rows() {
            observed.push((
                (tenants.value(row).to_string(), ids.value(row)),
                values.value(row),
            ));
        }
    }
    let expected: Vec<_> = model
        .iter()
        .map(|(key, value)| (key.clone(), *value))
        .collect();
    // Compare the complete row vector: a map comparison alone hides duplicates.
    assert_eq!(
        observed, expected,
        "every live key appears once with its latest value"
    );
    Ok(())
}
