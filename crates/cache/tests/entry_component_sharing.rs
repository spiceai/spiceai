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

#![expect(
    clippy::expect_used,
    reason = "a test asserts by panicking with a message; `allow-expect-in-tests` covers \
              `#[cfg(test)]` code but not a `tests/` integration binary"
)]

//! What a cached result shares with the other entries of its shape.
//!
//! Every entry holds an `Arc<Schema>` and an `Arc<HashSet<TableReference>>`.
//! Neither arrives shared: `DataFusion` builds a schema per plan, and
//! `get_logical_plan_input_tables` builds a set per query. So a high-cardinality
//! point-lookup workload — the shape worth caching — leaves one copy of each per
//! entry unless the cache collapses them on the way in.
//!
//! Measured against the v2.2.1 release binary, 100,000 distinct
//! `SELECT 1 FROM t WHERE id = $v LIMIT 1` queries, cache-on minus cache-off,
//! `phys_footprint`: giving the output field a 4,000-char name cost **+4,175 B per
//! entry** and giving the table a 4,000-char name cost **+4,063 B per entry**,
//! while the same query carrying a 4,000-char SQL comment cost **-9 B**. So the
//! duplication is in these two components specifically, and not in the plan or
//! the SQL text, which the entry does not retain at all.
//!
//! These tests drive the real store path, [`to_cached_record_batch_stream`],
//! through the crate's public API only, and assert on pointer identity — the
//! one observation that distinguishes "one allocation, N pointers" from
//! "N allocations that happen to compare equal".

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cache::{QueryResultsCacheProvider, key::RawCacheKey, to_cached_record_batch_stream};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::StreamExt;
use spicepod::component::caching::SQLResultsCacheConfig;

/// Entries per test. Enough that N copies of a wide shape is unmistakable, few
/// enough to stay a fast unit-speed test.
const ENTRIES: usize = 64;

/// A field name long enough that a per-entry copy of the schema dominates
/// everything else the entry holds, matching the probe that measured it.
const WIDE_NAME_LEN: usize = 4_000;

/// Builds the schema a query over this shape produces — a **fresh allocation
/// every call**, which is what `DataFusion` does per plan.
///
/// Nothing here is cached or cloned from a previous call on purpose: a fixture
/// that handed back one `Arc` would make every sharing assertion below pass
/// without the cache doing anything. See `inputs_start_out_unshared`.
fn fresh_schema(field_count: usize, name_len: usize) -> SchemaRef {
    let fields: Vec<Field> = (0..field_count)
        .map(|i| {
            Field::new(
                format!("{}_{i}", "c".repeat(name_len)),
                DataType::Int64,
                true,
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// The input-table set a query over `table_name` produces — again a fresh
/// allocation every call, as `get_logical_plan_input_tables` builds one per query.
fn fresh_input_tables(table_name: &str) -> Arc<HashSet<TableReference>> {
    Arc::new(HashSet::from([TableReference::bare(table_name)]))
}

/// A zero-row result over `schema`.
///
/// Zero rows deliberately: with no Arrow arrays in the entry, everything these
/// tests measure is the schema and the table set, which is the case the byte
/// accounting handles worst — a 0-row result contributes no array bytes at all.
fn empty_stream(schema: &SchemaRef) -> SendableRecordBatchStream {
    let batches: Vec<Result<RecordBatch, datafusion::error::DataFusionError>> = vec![];
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(schema),
        futures::stream::iter(batches),
    ))
}

fn provider() -> Arc<QueryResultsCacheProvider> {
    let config = SQLResultsCacheConfig {
        max_size: Some("512MiB".to_string()),
        // The default is 1s, which would expire entries mid-test.
        item_ttl: Some("10m".to_string()),
        ..Default::default()
    };
    Arc::new(
        QueryResultsCacheProvider::try_new(&config, Box::new([]))
            .expect("results cache provider should build from a valid config"),
    )
}

/// Stores `ENTRIES` results, each built from its own freshly allocated schema
/// and input-table set, and returns what the cache kept.
async fn store_entries(
    field_count: usize,
    name_len: usize,
    table_name: &str,
) -> Vec<cache::result::query::CachedQueryResult> {
    let provider = provider();

    let mut stored_schemas = Vec::with_capacity(ENTRIES);
    let mut stored_tables = Vec::with_capacity(ENTRIES);
    for i in 0..ENTRIES {
        let schema = fresh_schema(field_count, name_len);
        let input_tables = fresh_input_tables(table_name);
        stored_schemas.push(Arc::clone(&schema));
        stored_tables.push(Arc::clone(&input_tables));

        let mut stream = to_cached_record_batch_stream(
            Arc::clone(&provider),
            empty_stream(&schema),
            RawCacheKey::new(i as u64),
            input_tables,
            std::time::Instant::now(),
        );
        // The store happens once the stream is drained, so drain it.
        while let Some(batch) = stream.next().await {
            batch.expect("the empty stream yields no errors");
        }
    }

    // Guard the fixture: if the inputs were already shared, every assertion
    // about the cache collapsing them would hold no matter what the cache did.
    inputs_start_out_unshared(&stored_schemas, &stored_tables);

    let mut entries = Vec::with_capacity(ENTRIES);
    for i in 0..ENTRIES {
        entries.push(
            provider
                .get_raw_key(&RawCacheKey::new(i as u64))
                .await
                .expect("cache access should succeed")
                .expect("every stored entry should still be resident"),
        );
    }
    entries
}

/// Asserts the fixture handed the cache genuinely distinct allocations.
fn inputs_start_out_unshared(schemas: &[SchemaRef], tables: &[Arc<HashSet<TableReference>>]) {
    for i in 1..schemas.len() {
        assert!(
            !Arc::ptr_eq(&schemas[0], &schemas[i]),
            "fixture is broken: the schemas handed to the cache were already one allocation, \
             so a sharing assertion could not fail"
        );
        assert!(
            !Arc::ptr_eq(&tables[0], &tables[i]),
            "fixture is broken: the input-table sets handed to the cache were already one \
             allocation, so a sharing assertion could not fail"
        );
    }
}

fn assert_all_share<T>(items: &[Arc<T>], what: &str) {
    let distinct = {
        let mut ptrs: Vec<usize> = items.iter().map(|a| Arc::as_ptr(a) as usize).collect();
        ptrs.sort_unstable();
        ptrs.dedup();
        ptrs.len()
    };
    assert_eq!(
        distinct,
        1,
        "{} entries over one shape kept {distinct} distinct {what} allocations; \
         the cache should hold one and point every entry at it",
        items.len()
    );
}

/// The schema half. One shape, many entries, one allocation.
#[tokio::test]
async fn entries_of_one_shape_share_a_single_schema() {
    let entries = store_entries(8, WIDE_NAME_LEN, "orders").await;
    let schemas: Vec<SchemaRef> = entries.iter().map(|e| e.schema.arc()).collect();
    assert_all_share(&schemas, "schema");
}

/// The input-table half, which is the same defect in the other component: the
/// set is rebuilt per query and the cache stores it as it arrives.
#[tokio::test]
async fn entries_reading_one_table_share_a_single_input_table_set() {
    let entries = store_entries(4, 16, &"t".repeat(WIDE_NAME_LEN)).await;
    let tables: Vec<Arc<HashSet<TableReference>>> =
        entries.iter().map(|e| e.input_tables.arc()).collect();
    assert_all_share(&tables, "input-table set");
}

/// Sharing has to hold as the shape gets wider, which is where a per-entry copy
/// stops being a rounding error.
#[tokio::test]
async fn a_wide_shape_is_still_one_allocation() {
    let entries = store_entries(200, 64, &"wide_table".repeat(64)).await;
    let schemas: Vec<SchemaRef> = entries.iter().map(|e| e.schema.arc()).collect();
    let tables: Vec<Arc<HashSet<TableReference>>> =
        entries.iter().map(|e| e.input_tables.arc()).collect();
    assert_all_share(&schemas, "schema");
    assert_all_share(&tables, "input-table set");
}

/// What the budget is told, once the shared parts are shared.
///
/// The exactness bar is deliberately loose: an allocation shared by every entry
/// and charged to nobody is an accepted, documented residual. What must not
/// happen is `max_size` charging every entry for a full private copy of
/// something they all point at, because then the budget scales with the number
/// of entries times the size of one shape rather than with what the cache holds.
///
/// A long table name is the probe: it is the entry's own field, it is charged
/// today, and one shape's worth of it is shared by every entry.
#[tokio::test]
async fn a_shared_shape_is_not_billed_to_every_entry() {
    let table_name = "t".repeat(WIDE_NAME_LEN);
    let entries = store_entries(200, 64, &table_name).await;

    // Not charging is only correct *because* the parts are shared. Asserted
    // together, because the pair is the invariant: an entry that holds a
    // private copy and is not billed for it is the worst of both, and a test
    // that checked only the charge would call that state a pass.
    let schemas: Vec<SchemaRef> = entries.iter().map(|e| e.schema.arc()).collect();
    let tables: Vec<Arc<HashSet<TableReference>>> =
        entries.iter().map(|e| e.input_tables.arc()).collect();
    assert_all_share(&schemas, "schema");
    assert_all_share(&tables, "input-table set");

    let per_entry = entries[0].memory_size();
    assert!(
        per_entry < WIDE_NAME_LEN as u64,
        "each of the {ENTRIES} entries is billed {per_entry} B, which covers a private copy of a \
         {WIDE_NAME_LEN}-char table name that every entry shares; the shared parts are being \
         charged per entry rather than once"
    );
}
