/*
Copyright 2026 The Spice.ai OSS Authors

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

//! A first load into an empty upsert table too large for an exact keyset caches
//! a filter of the keys it wrote, so neither the post-load warm nor the next
//! append rebuilds the index by scanning the table.

use super::*;
use crate::metadata::DeletionMode;
use crate::provider::pk_index::CachedPkIndex;
use arrow::array::{AsArray, StringArray};
use std::collections::BTreeMap;

const BATCHES: i64 = 8;
const ROWS_PER_BATCH: i64 = 8_192;
/// Ids the load draws from, so its keys repeat within and across batches.
const IDS: i64 = 32_768;

type Model = BTreeMap<(String, i64), i64>;

/// A composite key that does not lead the schema, so the filter is built from
/// the key columns, in key order, rather than from the leading column.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(String, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.0.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .expect("batch")
}

fn tenant(id: i64) -> String {
    format!("tenant-{}", id % 3)
}

/// Seeded rows whose keys repeat within and across batches, and the latest
/// value of each key.
fn load_batches() -> (Vec<RecordBatch>, Model) {
    let mut model = Model::new();
    let mut batches = Vec::new();
    let mut seed = 0x0001_5071_u64;
    for batch_number in 0..BATCHES {
        let mut rows = Vec::new();
        for row_number in 0..ROWS_PER_BATCH {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let id = i64::try_from((seed >> 33) % IDS.unsigned_abs()).expect("id fits i64");
            let value = batch_number * ROWS_PER_BATCH + row_number;
            rows.push((tenant(id), id, value));
            model.insert((tenant(id), id), value);
        }
        batches.push(batch(&rows));
    }
    (batches, model)
}

async fn write(provider: &CayenneTableProvider, batches: Vec<RecordBatch>) {
    let ctx = SessionContext::new();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema(), None).expect("source");
    let plan = provider
        .insert_into(&ctx.state(), source, InsertOp::Append)
        .await
        .expect("insert plan");
    collect(plan, ctx.task_ctx()).await.expect("append");
}

/// Every visible row, in key order. A table that kept two copies of a key
/// shows both.
async fn visible(provider: &CayenneTableProvider) -> Vec<((String, i64), i64)> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider.clone_for_write()))
        .expect("register");
    let batches = ctx
        .sql("SELECT tenant, id, value FROM t ORDER BY tenant, id, value")
        .await
        .expect("query")
        .collect()
        .await
        .expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let tenants = batch.column(0).as_string::<i32>();
        let ids = batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>();
        let values = batch
            .column(2)
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            rows.push((
                (tenants.value(row).to_string(), ids.value(row)),
                values.value(row),
            ));
        }
    }
    rows
}

fn expected(model: &Model) -> Vec<((String, i64), i64)> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), *value))
        .collect()
}

/// Load into an empty table whose keyset budget is `budget_mb`, warm its index
/// as the sink does after a first load, then upsert half the loaded ids and as
/// many new ones. Returns whether the table-wide index was a filter right
/// after the load, and whether any step rebuilt the index from a table scan.
async fn load_warm_and_upsert(mode: DeletionMode, budget_mb: usize) -> (bool, bool) {
    let label = format!("{mode:?}, {budget_mb} MiB");
    let runtime_env = SessionContext::new().runtime_env();
    let key = vec!["tenant".to_string(), "id".to_string()];
    let (provider, _catalog, _dir) = create_cdc_table_with_schema(
        "t",
        runtime_env,
        schema(),
        key.clone(),
        VortexConfig {
            deletion_mode: mode,
            pk_keyset_cache_mb: Some(budget_mb),
            inline_max_rows: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
        OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(key),
        ),
    )
    .await;
    let rebuilt = Arc::new(AtomicBool::new(false));
    {
        let rebuilt = Arc::clone(&rebuilt);
        *provider.test_post_keyset_capture_hook.lock() = Some(Box::new(move || {
            Box::pin(async move {
                rebuilt.store(true, Ordering::SeqCst);
            })
        }));
    }

    let (load, mut model) = load_batches();
    write(&provider, load).await;
    let filter_after_load = matches!(
        provider.pk_keyset_cache.lock().as_ref(),
        Some(CachedPkIndex::Bloom(_))
    );
    assert_eq!(visible(&provider).await, expected(&model), "{label}: load");

    provider.warm_pk_index().await.expect("warm");
    let updates: Vec<_> = (0..2 * IDS)
        .filter(|id| id % 2 == 0)
        .map(|id| (tenant(id), id, 1_000_000 + id))
        .collect();
    for (tenant, id, value) in &updates {
        model.insert((tenant.clone(), *id), *value);
    }
    write(&provider, vec![batch(&updates)]).await;
    assert_eq!(
        visible(&provider).await,
        expected(&model),
        "{label}: every key once, with its latest value, after the upsert"
    );
    (filter_after_load, rebuilt.load(Ordering::SeqCst))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_first_load_too_large_for_an_exact_keyset_caches_a_filter_of_its_keys() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        assert_eq!(
            load_warm_and_upsert(mode, 1).await,
            (true, false),
            "{mode:?}: the load caches a filter, and neither the warm nor the upsert scans the table"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_first_load_whose_exact_keyset_fits_keeps_the_exact_index() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        assert_eq!(
            load_warm_and_upsert(mode, 16).await,
            (false, true),
            "{mode:?}: the load caches no filter, and the warm builds the exact index"
        );
    }
}
