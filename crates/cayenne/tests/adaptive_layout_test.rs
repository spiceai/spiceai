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

//! Integration tests for default-on adaptive cold layout (F4).
//!
//! Under default settings (`sort_columns` empty), selective filter scans record
//! hot columns; compaction / sort-and-rewrite then clusters by those columns so
//! zone maps prune without spicepod setup.

#![allow(clippy::expect_used)]

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
use datafusion::prelude::{SessionContext, col, lit};

async fn create_default_table(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: Arc<Schema>,
    runtime_env: Arc<RuntimeEnv>,
) -> Arc<CayenneTableProvider> {
    // Explicit empty sort_columns — the production default.
    let vortex_config = VortexConfig {
        sort_columns: vec![],
        ..VortexConfig::default()
    };

    let context = CayenneContext::new(
        &vortex_config,
        Arc::clone(&runtime_env),
        "adaptive_layout_test",
    );

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let catalog_arc = Arc::clone(&fixture.catalog);
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog_arc;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_context(context)
            .create(options)
            .await
            .expect("table should be created"),
    )
}

async fn insert_rows(provider: &Arc<CayenneTableProvider>, table_name: &str, n: usize) {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let mut ids = Vec::with_capacity(n);
    let mut regions = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    for i in 0..n {
        let row = i64::try_from(i).expect("row index fits i64");
        ids.push(row);
        regions.push(if i % 2 == 0 { "west" } else { "east" });
        amounts.push(row * 10);
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(amounts)),
        ],
    )
    .expect("batch");

    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).expect("mem");
    ctx.register_table("src", Arc::new(mem)).expect("src");
    ctx.sql(&format!("INSERT INTO {table_name} SELECT * FROM src"))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert");
}

/// Selective filters on `region` should drive auto sort-column selection.
#[tokio::test]
async fn default_layout_learns_filter_columns_from_scans() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let provider =
        create_default_table(&fixture, "t_learn", Arc::clone(&schema), runtime_env).await;
    insert_rows(&provider, "t_learn", 200).await;

    // Before any filtered scan, auto sort columns are empty (no observations).
    assert!(
        provider.effective_sort_columns_for_rewrite().is_empty(),
        "without observations, default rewrite should not invent sort columns"
    );

    let ctx = SessionContext::new();
    let state = ctx.state();
    // Simulate the filter pushdown path: TableProvider::scan with data filters.
    for _ in 0..8 {
        let filters = vec![col("region").eq(lit("west"))];
        let _plan = provider
            .scan(&state, None, &filters, None)
            .await
            .expect("scan with region filter");
    }
    // A few amount filters so region still wins top-1.
    for _ in 0..2 {
        let filters = vec![col("amount").gt(lit(50_i64))];
        let _plan = provider
            .scan(&state, None, &filters, None)
            .await
            .expect("scan with amount filter");
    }

    let effective = provider.effective_sort_columns_for_rewrite();
    assert_eq!(
        effective.first().map(String::as_str),
        Some("region"),
        "hottest observed filter column should lead auto sort columns, got {effective:?}"
    );
    assert!(
        effective.len() <= 2,
        "auto cluster top-k is 2, got {effective:?}"
    );
}

/// Explicit `sort_columns` must never be overridden by observations.
#[tokio::test]
async fn configured_sort_columns_override_observations() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let vortex_config = VortexConfig {
        sort_columns: vec!["amount".to_string()],
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(
        &vortex_config,
        Arc::clone(&runtime_env),
        "adaptive_layout_override",
    );
    let options = CreateTableOptions {
        table_name: "t_override".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog_arc = Arc::clone(&fixture.catalog);
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog_arc;
    let provider = Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_context(context)
            .create(options)
            .await
            .expect("create"),
    );

    let ctx = SessionContext::new();
    let state = ctx.state();
    for _ in 0..20 {
        let _plan = provider
            .scan(&state, None, &[col("region").eq(lit("west"))], None)
            .await
            .expect("scan");
    }

    assert_eq!(
        provider.effective_sort_columns_for_rewrite(),
        vec!["amount".to_string()],
        "operator-configured sort_columns must win over observed filters"
    );
}

/// An **inference-derived** sort order must NOT shadow observed filter columns.
///
/// This is the regression that made the default-on adaptive layout inert on every
/// catalog-visible CDC deployment. Schema inference fills `cayenne_sort_columns`
/// from the source's declared order — the primary key, for a `PostgreSQL` CDC table
/// — so `sort_columns` is never empty in production, and every adaptive-layout
/// gate asked only "is `sort_columns` empty?". The feature therefore never
/// engaged, while every benchmark gate stayed green (CH-benCH's date predicates
/// are near-tautological, so the suite cannot see layout quality).
///
/// Contrast `configured_sort_columns_override_observations`: an *explicit* sort
/// order is operator intent and still wins. Only the *guess* yields.
#[tokio::test]
async fn inferred_sort_columns_yield_to_observations() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    // Exactly what schema inference produces for a PG CDC table: the primary key
    // as the sort order, tagged as inferred rather than as operator intent.
    let vortex_config = VortexConfig {
        sort_columns: vec!["id".to_string()],
        sort_columns_origin: cayenne::metadata::SortColumnsOrigin::Inferred,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(
        &vortex_config,
        Arc::clone(&runtime_env),
        "adaptive_layout_inferred",
    );
    let options = CreateTableOptions {
        table_name: "t_inferred".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog_arc = Arc::clone(&fixture.catalog);
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog_arc;
    let provider = Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_context(context)
            .create(options)
            .await
            .expect("create"),
    );

    // Before any scan there is nothing observed, so the inferred order is the
    // best available key — it must still be used, not discarded.
    assert_eq!(
        provider.effective_sort_columns_for_rewrite(),
        vec!["id".to_string()],
        "with nothing observed yet, an inferred sort order is the correct fallback"
    );

    let ctx = SessionContext::new();
    let state = ctx.state();
    for _ in 0..20 {
        let _plan = provider
            .scan(&state, None, &[col("region").eq(lit("west"))], None)
            .await
            .expect("scan");
    }

    // Now that a hot filter column has been measured, evidence outranks the guess.
    assert_eq!(
        provider.effective_sort_columns_for_rewrite(),
        vec!["region".to_string()],
        "observed filter columns must outrank an INFERRED sort order (pre-fix this \
         returned the inferred primary key, leaving the adaptive layout inert)"
    );
}

/// Default-path sort-and-rewrite after filter observations clusters by the hot column.
#[tokio::test]
async fn sort_and_rewrite_uses_observed_filter_column() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let provider =
        create_default_table(&fixture, "t_rewrite", Arc::clone(&schema), runtime_env).await;
    insert_rows(&provider, "t_rewrite", 500).await;

    // Force data to files (checkpoint) so rewrite has Vortex files to re-sort.
    // Small inline threshold via pressure: call sort_and_rewrite which
    // checkpoints inline first via visible_file_stream_for_rewrite.
    let ctx = SessionContext::new();
    let state = ctx.state();
    for _ in 0..10 {
        let _plan = provider
            .scan(&state, None, &[col("region").eq(lit("west"))], None)
            .await
            .expect("scan");
    }

    assert_eq!(
        provider
            .effective_sort_columns_for_rewrite()
            .first()
            .map(String::as_str),
        Some("region")
    );

    provider
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite with auto columns");

    // Row count preserved.
    let session = SessionContext::new();
    session
        .register_table("t_rewrite", Arc::clone(&provider) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = session
        .sql("SELECT COUNT(*) AS c FROM t_rewrite")
        .await
        .expect("count")
        .collect()
        .await
        .expect("collect");
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64")
        .value(0);
    assert_eq!(count, 500, "rewrite must preserve all rows");

    // Selective query still returns the right half of the rows.
    let west = session
        .sql("SELECT COUNT(*) FROM t_rewrite WHERE region = 'west'")
        .await
        .expect("west")
        .collect()
        .await
        .expect("collect");
    let west_count = west[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64")
        .value(0);
    assert_eq!(west_count, 250);
}
