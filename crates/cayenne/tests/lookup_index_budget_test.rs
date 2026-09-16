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

//! The index's byte cap, and non-string key columns.
//!
//! Its own test binary so its tables' caps stay independent of the other
//! binary's, and its counters are not interleaved with theirs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;

const TABLE: &str = "svc_budget";
/// A second table small enough to fit under the same cap, so the cap test
/// cannot pass merely because an INT64 key never worked.
const SMALL_TABLE: &str = "svc_budget_small";
const ROWS: usize = 40_000;
/// Above the inline caps (`inline_max_rows`), so the overwrite actually writes
/// Vortex files. An inlined overwrite leaves the snapshot directory empty and
/// has no file-backed rows to address, so there is nothing to index.
const SMALL_ROWS: usize = 4_000;

/// A composite of an INT64 and a string: the encoding has to be type-general,
/// which it only is because keys go through the `RowConverter`.
const INDEX_KEY: &str = "TenantId+ServiceId";
/// Chosen to separate the two fixtures with room to spare. The cap bounds what a
/// build accumulates as well as the resident index: the 4,000-row build
/// accumulates ~0.2 MiB and resides in ~55 KiB, while the 40,000-row build
/// accumulates ~2 MiB, twice this, before it could compress anything.
const MAX_BYTES: usize = 1024 * 1024;

fn service_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Int64, false),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("Payload", DataType::Utf8, false),
    ]))
}

fn service_rows(rows: usize) -> RecordBatch {
    let mut auto_id = Vec::with_capacity(rows);
    let mut tenant = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut payload = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = i64::try_from(i).expect("fits i64");
        auto_id.push(id);
        tenant.push(id % 997);
        service.push(format!("SV{id:032x}"));
        payload.push(format!("payload-{id:08}"));
    }
    RecordBatch::try_new(
        service_schema(),
        vec![
            Arc::new(Int64Array::from(auto_id)),
            Arc::new(Int64Array::from(tenant)),
            Arc::new(StringArray::from(service)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("fixture batch")
}

async fn build_table(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
) -> Arc<CayenneTableProvider> {
    build_named(fixture, runtime_env, TABLE).await
}

async fn build_named(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        lookup_index_keys: vec![INDEX_KEY.to_string()],
        lookup_index_max_bytes: Some(MAX_BYTES),
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), name);
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: service_schema(),
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
    let exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        service_schema(),
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

async fn query(provider: &Arc<CayenneTableProvider>, sql: &str) -> Vec<RecordBatch> {
    query_on(provider, TABLE, sql).await
}

async fn query_on(provider: &Arc<CayenneTableProvider>, name: &str, sql: &str) -> Vec<RecordBatch> {
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

fn rows_of(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Over the cap, nothing is published and every query still answers correctly
/// from the ordinary scan.
///
/// The index is the one piece of Cayenne state that is always safe to drop — a
/// query that loses it scans instead — so exceeding the budget must degrade,
/// never fail and never answer from a partial index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_over_its_byte_cap_is_not_published() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let table = build_table(&fixture, Arc::clone(&runtime_env)).await;
    overwrite(&table, service_rows(ROWS)).await;

    // The write-time build ran and gave up; nothing may be published.
    assert!(
        table.verify_lookup_index_against_read_back().await.is_err(),
        "an index over the cap must not be published"
    );
    let counters = cayenne::lookup_index::counters_for_table(TABLE).expect("table has index state");
    assert_eq!(
        counters.index_bytes, 0,
        "an unpublished index must reserve nothing: {counters:?}"
    );

    // Results are unaffected — the composite key resolves by ordinary scan.
    let sql = format!(
        "SELECT \"AutoId\" FROM {TABLE} WHERE \"TenantId\" = 42 \
         AND \"ServiceId\" = 'SV{:032x}' ORDER BY \"AutoId\"",
        42
    );
    assert_eq!(rows_of(&query(&table, &sql).await), 1);

    let counters = cayenne::lookup_index::counters_for_table(TABLE).expect("table has index state");
    assert_eq!(
        counters.selected, 0,
        "no selection may be attached without a published index: {counters:?}"
    );
    assert_eq!(
        counters.access_plans_attached, 0,
        "no row selection may reach the scan: {counters:?}"
    );
    assert!(
        counters.unbuilt > 0,
        "an over-cap table should record its probes as unbuilt: {counters:?}"
    );

    // Every row is still reachable.
    let total = query(&table, &format!("SELECT COUNT(*) AS n FROM {TABLE}")).await;
    let count = total[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is i64")
        .value(0);
    assert_eq!(count, i64::try_from(ROWS).expect("fits i64"));
}

/// A composite `(INT64, Utf8)` key is indexed and used.
///
/// Keys are held in their stored types and compared through the `RowConverter`
/// encoding, so the index is not limited to string keys — and this is what keeps
/// the cap test above honest, by showing the same key shape working when it fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mixed_type_composite_key_is_indexed() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let table = build_named(&fixture, Arc::clone(&runtime_env), SMALL_TABLE).await;
    overwrite(&table, service_rows(SMALL_ROWS)).await;

    let report = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("an index that fits is published and verifiable");
    assert!(
        report.agrees(),
        "mixed-type key diverged: {:?}",
        report.mismatches
    );
    assert!(report.keys_compared > 0, "nothing compared: {report:?}");

    let counters =
        cayenne::lookup_index::counters_for_table(SMALL_TABLE).expect("table has index state");
    assert!(
        counters.index_bytes > 0,
        "a published index must reserve its bytes: {counters:?}"
    );
    println!(
        "mixed-type index: {} bytes reserved against a {MAX_BYTES}-byte cap",
        counters.index_bytes
    );

    // The INT64 half of the key resolves through the same encoding, including
    // the coercion a planner may apply to the literal.
    let before =
        cayenne::lookup_index::counters_for_table(SMALL_TABLE).expect("table has index state");
    let sql = format!(
        "SELECT \"AutoId\" FROM {SMALL_TABLE} WHERE \"TenantId\" = 42 \
         AND \"ServiceId\" = 'SV{:032x}' ORDER BY \"AutoId\"",
        42
    );
    let rows = query_on(&table, SMALL_TABLE, &sql).await;
    assert_eq!(
        rows_of(&rows),
        1,
        "mixed-type lookup returned the wrong rows"
    );
    let after =
        cayenne::lookup_index::counters_for_table(SMALL_TABLE).expect("table has index state");
    assert_eq!(
        after.selected,
        before.selected + 1,
        "the INT64 composite key did not use the index: {before:?} -> {after:?}"
    );
}
