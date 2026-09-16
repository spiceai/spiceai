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

//! How the point-lookup index composes with the rest of the scan: predicates
//! that only look like an indexed equality, and position-delete vectors.
//!
//! Every check compares an indexed table against an identical unindexed one in
//! the same process, so a divergence is the index changing a result.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;

/// Above the inline caps, so the overwrite writes Vortex files the index can
/// address.
const ROWS: usize = 20_000;

async fn build_table(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    schema: Arc<Schema>,
    index_keys: Option<&str>,
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        lookup_index_keys: index_keys.map(|k| vec![k.to_string()]).unwrap_or_default(),
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), name);
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog = Arc::clone(&fixture.catalog);
    let catalog: Arc<dyn MetadataCatalog> = catalog;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .create(options)
            .await
            .expect("create table"),
    )
}

async fn overwrite(provider: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let schema = batch.schema();
    let exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        schema,
        None,
    )
    .expect("overwrite source");
    let plan = provider
        .insert_into(
            &ctx.state(),
            exec,
            datafusion_expr::dml::InsertOp::Overwrite,
        )
        .await
        .expect("overwrite plan");
    datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("overwrite");
}

async fn sql(provider: &Arc<CayenneTableProvider>, name: &str, sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    ctx.sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute")
}

fn rendered(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for column in 0..batch.num_columns() {
                let array = batch.column(column);
                cells.push(if array.is_null(row) {
                    "NULL".to_string()
                } else {
                    arrow::util::display::array_value_to_string(array, row).expect("render cell")
                });
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

fn counters(table: &str) -> cayenne::lookup_index::LookupIndexCounters {
    cayenne::lookup_index::counters_for_table(table).expect("indexed table has index state")
}

fn scored_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Utf8, false),
        Field::new("Score", DataType::Float64, false),
    ]))
}

/// A cast on the COLUMN side can map many stored values onto the literal:
/// `CAST(Score AS BIGINT) = 5` holds for 5.0, 5.2 and 5.7. The index is keyed on
/// the stored value, so it can only ever find 5.0 — answering such a predicate
/// from the index would silently drop the other two rows. It must scan instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_column_side_cast_is_never_answered_from_the_index() {
    const INDEXED: &str = "cast_indexed";
    const PLAIN: &str = "cast_plain";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        Arc::clone(&runtime_env),
        INDEXED,
        scored_schema(),
        Some("TenantId+Score"),
    )
    .await;
    let plain = build_table(&fixture, runtime_env, PLAIN, scored_schema(), None).await;

    let mut auto_id = Vec::with_capacity(ROWS + 3);
    let mut tenant = Vec::with_capacity(ROWS + 3);
    let mut score = Vec::with_capacity(ROWS + 3);
    for i in 0..ROWS {
        auto_id.push(i64::try_from(i).expect("fits i64"));
        tenant.push(format!("T{:04}", i % 50));
        score.push(f64::from(u32::try_from(i).expect("fits u32")) + 0.25);
    }
    for (offset, value) in [5.0, 5.2, 5.7].into_iter().enumerate() {
        auto_id.push(1_000_000 + i64::try_from(offset).expect("fits i64"));
        tenant.push("PLANTED".to_string());
        score.push(value);
    }
    let batch = RecordBatch::try_new(
        scored_schema(),
        vec![
            Arc::new(Int64Array::from(auto_id)),
            Arc::new(StringArray::from(tenant)),
            Arc::new(Float64Array::from(score)),
        ],
    )
    .expect("batch");
    overwrite(&indexed, batch.clone()).await;
    overwrite(&plain, batch).await;

    // The index is in place: a bare equality on both key columns uses it.
    let before = counters(INDEXED);
    let bare = "SELECT \"AutoId\" FROM {t} WHERE \"TenantId\" = 'PLANTED' AND \"Score\" = 5.2";
    assert_eq!(
        rendered(&sql(&indexed, INDEXED, &bare.replace("{t}", INDEXED)).await),
        rendered(&sql(&plain, PLAIN, &bare.replace("{t}", PLAIN)).await)
    );
    assert_eq!(
        counters(INDEXED).selected,
        before.selected + 1,
        "a bare equality on both key columns should use the index"
    );

    let casted = "SELECT \"AutoId\" FROM {t} WHERE \"TenantId\" = 'PLANTED' \
                  AND CAST(\"Score\" AS BIGINT) = 5 ORDER BY \"AutoId\"";
    let expected = rendered(&sql(&plain, PLAIN, &casted.replace("{t}", PLAIN)).await);
    assert_eq!(
        expected.len(),
        3,
        "control table: all three planted rows cast to 5"
    );
    let actual = rendered(&sql(&indexed, INDEXED, &casted.replace("{t}", INDEXED)).await);
    assert_eq!(
        actual, expected,
        "a column-side cast was answered from the index and dropped rows"
    );
}

fn service_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Utf8, false),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("Active", DataType::Int32, false),
    ]))
}

fn service_rows(rows: usize) -> RecordBatch {
    let mut auto_id = Vec::with_capacity(rows);
    let mut tenant = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut active = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = i64::try_from(i).expect("fits i64");
        // Every tenth key is shared by two rows, so a deleted candidate can sit
        // next to a live one under the same key.
        let key = id - i64::from(i % 10 == 1);
        auto_id.push(id);
        tenant.push(format!("AC{:032x}", key % 97));
        service.push(format!("MG{key:032x}"));
        active.push(1i32);
    }
    RecordBatch::try_new(
        service_schema(),
        vec![
            Arc::new(Int64Array::from(auto_id)),
            Arc::new(StringArray::from(tenant)),
            Arc::new(StringArray::from(service)),
            Arc::new(Int32Array::from(active)),
        ],
    )
    .expect("batch")
}

/// Position-delete vectors and a lookup row selection both restrict what a file
/// scan reads. Deleted candidates must stay hidden, and the index should keep
/// serving the lookup rather than falling back to a scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn position_deletes_compose_with_the_index() {
    const INDEXED: &str = "deletes_indexed";
    const PLAIN: &str = "deletes_plain";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        Arc::clone(&runtime_env),
        INDEXED,
        service_schema(),
        Some("TenantId+ServiceId"),
    )
    .await;
    let plain = build_table(&fixture, runtime_env, PLAIN, service_schema(), None).await;
    let batch = service_rows(ROWS);
    overwrite(&indexed, batch.clone()).await;
    overwrite(&plain, batch).await;

    // Keys whose rows were deleted, keys with a deleted and a live row, keys that
    // were never touched, and keys no row holds.
    let keys: Vec<i64> = (0..200).map(|i| i * 7 + i % 3).collect();
    let mut deleted = Vec::new();
    for (i, &id) in keys.iter().enumerate() {
        if i % 2 == 0 {
            deleted.push(id);
        }
        if i % 4 == 0 && id % 10 == 0 {
            deleted.push(id + 1);
        }
    }
    let deleted = deleted
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    // Delete through the ordinary DML path on both tables.
    for (provider, name) in [(&indexed, INDEXED), (&plain, PLAIN)] {
        sql(
            provider,
            name,
            &format!("DELETE FROM {name} WHERE \"AutoId\" IN ({deleted})"),
        )
        .await;
    }
    assert_eq!(
        rendered(
            &sql(
                &indexed,
                INDEXED,
                &format!("SELECT COUNT(*) FROM {INDEXED}")
            )
            .await
        ),
        rendered(&sql(&plain, PLAIN, &format!("SELECT COUNT(*) FROM {PLAIN}")).await)
    );
    assert_eq!(
        rendered(&sql(&plain, PLAIN, &format!("SELECT COUNT(*) FROM {PLAIN}")).await),
        vec![(ROWS - deleted.split(", ").count()).to_string()],
        "the control table did not apply the delete"
    );
    let query_for = |id: i64| {
        format!(
            "SELECT \"AutoId\" FROM {{t}} WHERE \"TenantId\" = 'AC{:032x}' \
             AND \"ServiceId\" = 'MG{:032x}' ORDER BY \"AutoId\"",
            id % 97,
            id
        )
    };

    // The index may be rebuilt for the post-delete snapshot in the background;
    // keep issuing lookups until one is served from it or the deadline passes.
    let deadline = Instant::now() + Duration::from_mins(2);
    let mut served = false;
    while !served && Instant::now() < deadline {
        let before = counters(INDEXED).selected;
        let _ = sql(
            &indexed,
            INDEXED,
            &query_for(keys[1]).replace("{t}", INDEXED),
        )
        .await;
        served = counters(INDEXED).selected > before;
        if !served {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    let before = counters(INDEXED);
    for &id in &keys {
        let query = query_for(id);
        assert_eq!(
            rendered(&sql(&indexed, INDEXED, &query.replace("{t}", INDEXED)).await),
            rendered(&sql(&plain, PLAIN, &query.replace("{t}", PLAIN)).await),
            "lookup {id} diverged from the control table after deletes"
        );
    }
    let after = counters(INDEXED);
    // Every lookup is answered by the index: a selection for a key some row
    // holds, an empty probe for a key none does. None falls back to a scan.
    let answered = (after.selected - before.selected) + (after.empty - before.empty);
    assert!(
        served
            && answered == u64::try_from(keys.len()).expect("fits")
            && after.selected > before.selected
            && after.snapshot_mismatch == before.snapshot_mismatch
            && after.unbuilt == before.unbuilt,
        "lookups on a table with position deletes were not served from the index: {before:?} -> {after:?}"
    );
}
