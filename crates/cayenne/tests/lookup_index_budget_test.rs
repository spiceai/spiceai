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

//! Memory-pool admission of the secondary index, and non-string key columns.
//!
//! Each table builds against its own bounded query memory pool, so what one
//! table's index is admitted never depends on another's.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::common;

use common::lookup_index::{
    TableSpec, explain_total, int64_column, open_table, overwrite, query, runtime_with_pool,
};

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::VortexConfig;

use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;

const TABLE: &str = "svc_budget";
/// A second table small enough to fit under the same cap, so the cap test
/// cannot pass merely because an INT64 key never worked.
const SMALL_TABLE: &str = "svc_budget_small";
/// A table whose first write's index fits the pool and whose second's does not.
const PARTIAL_TABLE: &str = "svc_budget_partial";
const ROWS: usize = 40_000;
/// Above the inline caps (`inline_max_rows`), so the overwrite actually writes
/// Vortex files. An inlined overwrite leaves the snapshot directory empty and
/// has no file-backed rows to address, so there is nothing to index.
const SMALL_ROWS: usize = 4_000;

/// A composite of an INT64 and a string: the encoding has to be type-general,
/// which it only is because keys go through the `RowConverter`.
const INDEX_KEY: [&str; 2] = ["TenantId", "ServiceId"];
/// The query memory pool each table builds its index against, chosen to separate
/// the two fixtures with room to spare. The pool admits what a build accumulates
/// as well as the resident index: the 4,000-row build accumulates ~0.2 MiB and
/// resides in ~55 KiB, while the 40,000-row build accumulates ~2 MiB, twice
/// this, before it could compress anything.
const POOL_BYTES: usize = 1024 * 1024;

fn service_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Int64, false),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("Payload", DataType::Utf8, false),
    ]))
}

fn service_rows(rows: usize) -> RecordBatch {
    service_rows_from(0, rows)
}

/// `rows` rows whose `AutoId`s start at `offset`, so a second write's keys are
/// distinct from the first's.
fn service_rows_from(offset: usize, rows: usize) -> RecordBatch {
    let mut auto_id = Vec::with_capacity(rows);
    let mut tenant = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut payload = Vec::with_capacity(rows);
    for i in offset..offset + rows {
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

/// The indexed table `name`, in file mode unless `spec` says otherwise.
fn spec(name: &str) -> TableSpec<'_> {
    TableSpec::new(name, service_schema(), &[&INDEX_KEY])
}

fn rows_of(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// When the memory pool cannot fit the index, nothing is published and every
/// query still answers correctly from the ordinary scan.
///
/// The index is the one piece of Cayenne state that is always safe to go
/// without — a query that has none scans instead — so a pool that cannot fit it
/// must degrade, never fail and never answer from a partial index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_the_memory_pool_cannot_fit_is_not_published() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (runtime_env, pool) = runtime_with_pool(POOL_BYTES);
    let table = open_table(&fixture, Arc::clone(&runtime_env), spec(TABLE)).await;
    overwrite(&table, vec![service_rows(ROWS)]).await;

    // The write's runs were built and refused; no file may be covered.
    let verification = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    assert!(
        verification.files == 0 && verification.uncovered_files > 0,
        "an index the pool cannot fit must not cover any file: {verification:?}"
    );
    let counters = table
        .lookup_index_counters()
        .expect("table has index state");
    assert_eq!(
        counters.index_bytes, 0,
        "an unpublished index must reserve nothing: {counters:?}"
    );
    assert_eq!(
        counters.builds_unpublished, 1,
        "the refused write-time build must be counted: {counters:?}"
    );
    assert_eq!(
        pool.reserved(),
        0,
        "a refused build must give back what it accumulated"
    );

    // Results are unaffected — the composite key resolves by ordinary scan.
    let sql = format!(
        "SELECT \"AutoId\" FROM {TABLE} WHERE \"TenantId\" = 42 \
         AND \"ServiceId\" = 'SV{:032x}' ORDER BY \"AutoId\"",
        42
    );
    assert_eq!(rows_of(&query(&table, TABLE, &sql).await), 1);

    let counters = table
        .lookup_index_counters()
        .expect("table has index state");
    assert_eq!(
        counters.full, 0,
        "no selection may be attached without a published index: {counters:?}"
    );
    assert_eq!(
        counters.access_plans_attached, 0,
        "no row selection may reach the scan: {counters:?}"
    );
    assert!(
        counters.none > 0,
        "a table the pool refused should record its probes' coverage as none: {counters:?}"
    );

    // Every row is still reachable.
    let total = query(&table, TABLE, &format!("SELECT COUNT(*) AS n FROM {TABLE}")).await;
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
/// the refusal test above honest, by showing the same key shape working in the
/// same size of pool when it fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mixed_type_composite_key_is_indexed() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (runtime_env, pool) = runtime_with_pool(POOL_BYTES);
    let table = open_table(&fixture, Arc::clone(&runtime_env), spec(SMALL_TABLE)).await;
    overwrite(&table, vec![service_rows(SMALL_ROWS)]).await;

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

    let counters = table
        .lookup_index_counters()
        .expect("table has index state");
    assert!(
        counters.index_bytes > 0,
        "a published index must reserve its bytes: {counters:?}"
    );
    assert!(
        pool.reserved() >= usize::try_from(counters.index_bytes).expect("fits usize"),
        "the index's bytes must be reserved in the pool: {} reserved, {counters:?}",
        pool.reserved()
    );
    println!(
        "mixed-type index: {} bytes reserved in a {POOL_BYTES}-byte pool",
        counters.index_bytes
    );

    // The INT64 half of the key resolves through the same encoding, including
    // the coercion a planner may apply to the literal.
    let before = table
        .lookup_index_counters()
        .expect("table has index state");
    let sql = format!(
        "SELECT \"AutoId\" FROM {SMALL_TABLE} WHERE \"TenantId\" = 42 \
         AND \"ServiceId\" = 'SV{:032x}' ORDER BY \"AutoId\"",
        42
    );
    let rows = query(&table, SMALL_TABLE, &sql).await;
    assert_eq!(
        rows_of(&rows),
        1,
        "mixed-type lookup returned the wrong rows"
    );
    let after = table
        .lookup_index_counters()
        .expect("table has index state");
    assert_eq!(
        after.full,
        before.full + 1,
        "the INT64 composite key did not use the index: {before:?} -> {after:?}"
    );
}

/// A write whose index the pool cannot fit is read in full beside the index of
/// the writes that did fit, and the lookup says so: coverage `partial`, with
/// that file counted in `uncovered_files`, and never `full` — even through a
/// join's runtime filter, when the indexed files hold no candidate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lookup_reads_an_unindexed_write_in_full_beside_the_index() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (runtime_env, _pool) = runtime_with_pool(POOL_BYTES);
    // One file per write, so the background build of the append's file needs
    // as much memory as its write did and is refused too.
    let config = VortexConfig {
        target_vortex_file_size_mb: 256,
        ..VortexConfig::default()
    };
    let table = open_table(
        &fixture,
        Arc::clone(&runtime_env),
        spec(PARTIAL_TABLE).config(config),
    )
    .await;
    overwrite(&table, vec![service_rows(SMALL_ROWS)]).await;
    let indexed = table
        .lookup_index_counters()
        .expect("table has index state");
    assert!(
        indexed.index_bytes > 0,
        "the first write's index must fit: {indexed:?}"
    );
    common::insert_batches(&table, vec![service_rows_from(SMALL_ROWS, ROWS)])
        .await
        .expect("append");
    let refused = table
        .lookup_index_counters()
        .expect("table has index state");
    assert!(
        refused.builds_unpublished > indexed.builds_unpublished,
        "the append's index must be refused for this test to mean anything: \
         {indexed:?} -> {refused:?}"
    );

    let key = |id: usize| (id % 997, format!("SV{id:032x}"));
    for (id, what) in [
        (42, "the indexed write"),
        (SMALL_ROWS + 12_345, "the unindexed append"),
    ] {
        let (tenant, service) = key(id);
        let sql = format!(
            "SELECT \"AutoId\" FROM {PARTIAL_TABLE} WHERE \"TenantId\" = {tenant} \
             AND \"ServiceId\" = '{service}'"
        );
        let explain = query(&table, PARTIAL_TABLE, &format!("EXPLAIN {sql}")).await;
        let plan = arrow::util::pretty::pretty_format_batches(&explain)
            .expect("format plan")
            .to_string();
        assert!(
            plan.contains("lookup_index=(TenantId, ServiceId)")
                && explain_total(&plan, "uncovered_files") > 0,
            "a key in {what} must be partly covered, reading the unindexed file in full:\n{plan}"
        );
        let found = int64_column(&query(&table, PARTIAL_TABLE, &sql).await);
        assert_eq!(
            found,
            vec![i64::try_from(id).expect("fits i64")],
            "a key in {what} returned the wrong rows"
        );
    }

    // The join's runtime filter carries only the append's key, which no indexed
    // file holds. The unindexed file is still read, so the probe is a selection.
    let (tenant, service) = key(SMALL_ROWS + 12_345);
    let key_schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
    ]));
    let key_batch = RecordBatch::try_new(
        Arc::clone(&key_schema),
        vec![
            Arc::new(Int64Array::from(vec![
                i64::try_from(tenant).expect("fits i64"),
            ])),
            Arc::new(StringArray::from(vec![service])),
        ],
    )
    .expect("key batch");
    let keys = datafusion::datasource::MemTable::try_new(key_schema, vec![vec![key_batch]])
        .expect("key table");
    let ctx = SessionContext::new();
    ctx.register_table(PARTIAL_TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register table");
    ctx.register_table("keys", Arc::new(keys))
        .expect("register keys");
    let before = table
        .lookup_index_counters()
        .expect("table has index state");
    let rows = ctx
        .sql(&format!(
            "SELECT s.\"AutoId\" FROM keys k INNER JOIN {PARTIAL_TABLE} s \
             ON k.tenant = s.\"TenantId\" AND k.service = s.\"ServiceId\""
        ))
        .await
        .expect("join plan")
        .collect()
        .await
        .expect("join execution");
    assert_eq!(rows_of(&rows), 1, "the join lost the append's row");
    let after = table
        .lookup_index_counters()
        .expect("table has index state");
    assert_eq!(
        (after.partial - before.partial, after.full - before.full),
        (1, 0),
        "a runtime probe that reads an unindexed file is partly covered: \
         {before:?} -> {after:?}"
    );
}
