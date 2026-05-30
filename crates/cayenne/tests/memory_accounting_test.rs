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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

//! Tests that Cayenne's off-pool resident state (PK keyset + deletion indexes)
//! is accounted against the `DataFusion` query `MemoryPool` that
//! `runtime.query.memory_limit` controls — making `memory_limit` reflect real
//! Cayenne RSS — and that this accounting never changes query results.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(test_accounting_tracks_keyset_and_deletions_impl);
test_with_backends!(test_accounting_against_bounded_pool_preserves_correctness_impl);
test_with_backends!(test_accounting_rehydrates_deletions_on_open_impl);
test_with_backends!(test_accounting_tracks_inline_file_deletions_impl);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// File-backed (inlining disabled) key-mode upsert config, so upserts produce
/// the deletion indexes that get accounted.
fn key_mode_file_config() -> VortexConfig {
    VortexConfig {
        deletion_mode: DeletionMode::Key,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        ..VortexConfig::default()
    }
}

/// Key-mode config where a multi-row first insert spills to a file, but a
/// single-row upsert stays inline and therefore exercises
/// `update_file_deletion_cache`.
fn key_mode_inline_upsert_config() -> VortexConfig {
    VortexConfig {
        deletion_mode: DeletionMode::Key,
        inline_max_rows: 2,
        inline_max_bytes: 1024 * 1024,
        inline_max_buffer_bytes: 1024 * 1024,
        ..VortexConfig::default()
    }
}

fn table_options(name: &str, base_path: String, vortex_config: VortexConfig) -> CreateTableOptions {
    CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path,
        partition_column: None,
        vortex_config,
    }
}

async fn insert_sql(ctx: &SessionContext, sql: &str) {
    ctx.sql(sql)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
}

/// The reservation tracks keyset bytes after inserts and deletion-index bytes
/// after upserts, and shrinks back to zero is observable on clear (exercised in
/// the unit test); here we assert monotone growth through the write sequence.
async fn test_accounting_tracks_keyset_and_deletions_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            table_options(
                "mem_track",
                fixture.data_path.to_string_lossy().to_string(),
                key_mode_file_config(),
            ),
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(
        "mem_track",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    assert_eq!(
        table.accounted_memory_bytes(),
        0,
        "a fresh table reserves nothing"
    );

    insert_sql(
        &ctx,
        "INSERT INTO mem_track VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
    )
    .await;
    let after_insert = table.accounted_memory_bytes();
    assert!(
        after_insert > 0,
        "the PK keyset must be accounted after inserts (got {after_insert})"
    );

    // Upsert existing keys -> deletion + insert-record indexes grow.
    insert_sql(&ctx, "INSERT INTO mem_track VALUES (1,'A'),(2,'B'),(3,'C')").await;
    let after_upsert = table.accounted_memory_bytes();
    assert!(
        after_upsert > after_insert,
        "deletion indexes must add to the reservation after upserts (insert={after_insert}, upsert={after_upsert})"
    );

    // Accounting must not change results: 5 rows, the three upserted are updated.
    let batches = ctx
        .sql("SELECT id, name FROM mem_track ORDER BY id")
        .await?
        .collect()
        .await?;
    let rows: Vec<(i64, String)> = batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id");
            let names = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name");
            (0..b.num_rows())
                .map(|i| (ids.value(i), names.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "A".to_string()),
            (2, "B".to_string()),
            (3, "C".to_string()),
            (4, "d".to_string()),
            (5, "e".to_string()),
        ],
        "upsert results must be correct regardless of memory accounting"
    );

    Ok(())
}

/// With a real bounded `GreedyMemoryPool` shared by the table and the query
/// session, the table's state shows up in the pool's reserved total, and queries
/// over data that fits return correct results.
async fn test_accounting_against_bounded_pool_preserves_correctness_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // 256 MiB: generous for the tiny test data, so this asserts coexistence
    // (accounting + bounded pool + correctness), not budget-exhaustion behavior.
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build()?,
    );
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&runtime_env));

    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            table_options(
                "mem_bounded",
                fixture.data_path.to_string_lossy().to_string(),
                key_mode_file_config(),
            ),
            Arc::clone(&runtime_env),
        )
        .await?,
    );
    ctx.register_table(
        "mem_bounded",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    insert_sql(
        &ctx,
        "INSERT INTO mem_bounded VALUES (1,'a'),(2,'b'),(3,'c')",
    )
    .await;
    insert_sql(&ctx, "INSERT INTO mem_bounded VALUES (1,'A'),(2,'B')").await;

    // The table's reservation is part of the shared pool's reserved total.
    let accounted = table.accounted_memory_bytes();
    assert!(accounted > 0, "table reserves against the bounded pool");
    assert!(
        pool.reserved() >= accounted,
        "the shared pool's reserved total ({}) must include the table's reservation ({accounted})",
        pool.reserved()
    );

    // Correctness under the bounded, shared pool.
    let batches = ctx
        .sql("SELECT count(*) AS n, sum(id) AS s FROM mem_bounded")
        .await?
        .collect()
        .await?;
    let batch = &batches[0];
    let n = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n");
    let s = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("s");
    assert_eq!(n.value(0), 3, "three live rows after upserts");
    assert_eq!(s.value(0), 6, "sum of ids 1+2+3");

    Ok(())
}

/// Existing deletion indexes loaded during provider construction must reserve
/// memory immediately, before any subsequent write or explicit cache refresh.
async fn test_accounting_rehydrates_deletions_on_open_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog),
            table_options(
                "mem_reopen",
                fixture.data_path.to_string_lossy().to_string(),
                key_mode_file_config(),
            ),
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(
        "mem_reopen",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    insert_sql(&ctx, "INSERT INTO mem_reopen VALUES (1,'a'),(2,'b')").await;
    insert_sql(&ctx, "INSERT INTO mem_reopen VALUES (1,'A')").await;
    assert!(
        table.accounted_memory_bytes() > 0,
        "source provider should have resident deletion state after upsert"
    );

    let reopened = Arc::new(
        CayenneTableProvider::new("mem_reopen", Arc::clone(&catalog), ctx.runtime_env()).await?,
    );
    let reopened_accounted = reopened.accounted_memory_bytes();
    assert!(
        reopened_accounted > 0,
        "reopened provider must account deletion indexes loaded from catalog"
    );

    let reopened_ctx = SessionContext::new_with_config_rt(SessionConfig::new(), ctx.runtime_env());
    reopened_ctx.register_table(
        "mem_reopen",
        reopened as Arc<dyn datafusion::datasource::TableProvider>,
    )?;
    let batches = reopened_ctx
        .sql("SELECT name FROM mem_reopen WHERE id = 1")
        .await?
        .collect()
        .await?;
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name");
    assert_eq!(names.value(0), "A", "reopen preserves upsert correctness");

    Ok(())
}

/// Inline upserts that replace file-backed rows update the provider's shared
/// deletion cache directly, so the table reservation must grow on that path too.
async fn test_accounting_tracks_inline_file_deletions_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            table_options(
                "mem_inline_delete",
                fixture.data_path.to_string_lossy().to_string(),
                key_mode_inline_upsert_config(),
            ),
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(
        "mem_inline_delete",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    insert_sql(
        &ctx,
        "INSERT INTO mem_inline_delete VALUES (1,'a'),(2,'b'),(3,'c')",
    )
    .await;
    let after_file_insert = table.accounted_memory_bytes();
    assert!(
        after_file_insert > 0,
        "file-backed insert should account the PK keyset"
    );

    insert_sql(&ctx, "INSERT INTO mem_inline_delete VALUES (1,'A')").await;
    let after_inline_upsert = table.accounted_memory_bytes();
    assert!(
        after_inline_upsert > after_file_insert,
        "inline upsert should account the file-backed deletion cache (before={after_file_insert}, after={after_inline_upsert})"
    );

    let batches = ctx
        .sql("SELECT id, name FROM mem_inline_delete ORDER BY id")
        .await?
        .collect()
        .await?;
    let rows: Vec<(i64, String)> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id");
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name");
            (0..batch.num_rows())
                .map(|row| (ids.value(row), names.value(row).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "A".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ],
        "inline file-deletion accounting must not affect visible rows"
    );

    Ok(())
}
