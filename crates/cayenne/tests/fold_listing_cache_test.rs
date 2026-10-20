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

//! A write into a key-deletion table that repeats keys (a refresh's first load
//! into an empty table, or a full refresh) is folded before it publishes: the
//! files holding superseded copies are rewritten and the originals removed. The
//! query that finds those copies lists the snapshot directory through the
//! table's `RuntimeEnv`, whose list-files cache every query plans through in
//! `spiced`. After the fold, that cache must not still list the removed
//! originals.
//!
//! On network block storage the fold's rewrite is a maintenance write through
//! the direct compaction writer, which runs in its own `RuntimeEnv`, so the
//! eviction `DataFusion` makes when it plans that write does not reach the
//! table's cache (spiceai/spiceai#15298).

use crate::common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use cayenne::MetadataCatalog;
use cayenne::metadata::{DeletionMode, StorageClass, VortexConfig};
use common::lookup_index::{TableSpec, file_mode_config, open_table};
use datafusion::catalog::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::dml::InsertOp;

const TABLE: &str = "loaded";
const BATCH_ROWS: i64 = 10_000;
const BATCHES: i64 = 8;
/// In every batch after the first, each row at position 999 mod 1000 repeats
/// the key of the row at 500 mod 1000 of the batch before it, so the load's
/// files hold superseded copies.
const REPEAT_EVERY: i64 = 1000;
/// Rows of new keys the append after the load writes.
const APPENDED_ROWS: i64 = 100;
/// Distinct keys of the first load: every repeated key's earlier copy is gone.
const LOADED_KEYS: i64 = BATCHES * BATCH_ROWS - (BATCHES - 1) * (BATCH_ROWS / REPEAT_EVERY);

#[derive(Clone, Copy, Debug)]
enum QueryEnv {
    /// The table's own `RuntimeEnv`, as in `spiced`.
    Shared,
    /// The table's `RuntimeEnv`, with its list-files cache cleared after the load.
    SharedCacheCleared,
}

#[derive(Clone, Copy, Debug)]
enum Compaction {
    /// No compaction runs, so the load's protected snapshot stays as published.
    Off,
    /// The table's default compaction triggers.
    Default,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    /// How the first load is written: a refresh's append into the empty table,
    /// or a full refresh's overwrite.
    load: InsertOp,
    deletion_mode: DeletionMode,
    storage_class: StorageClass,
    query_env: QueryEnv,
    compaction: Compaction,
}

impl Case {
    const fn new(deletion_mode: DeletionMode, storage_class: StorageClass) -> Self {
        Self {
            load: InsertOp::Append,
            deletion_mode,
            storage_class,
            query_env: QueryEnv::Shared,
            compaction: Compaction::Off,
        }
    }
}

test_with_backends!(key_mode_first_load_on_network_storage);
test_with_backends!(key_mode_first_load_on_network_storage_with_compaction);
test_with_backends!(key_mode_full_refresh_on_network_storage);
test_with_backends!(key_mode_first_load_on_network_storage_cache_cleared);
test_with_backends!(key_mode_first_load_on_unknown_storage);
test_with_backends!(position_mode_first_load_on_network_storage);

async fn key_mode_first_load_on_network_storage(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Case::new(DeletionMode::Key, StorageClass::Ebs)).await
}

async fn key_mode_first_load_on_network_storage_with_compaction(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(
        fixture,
        Case {
            compaction: Compaction::Default,
            ..Case::new(DeletionMode::Key, StorageClass::Ebs)
        },
    )
    .await
}

/// The same load written by a full refresh, which folds its repeats the same way.
async fn key_mode_full_refresh_on_network_storage(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(
        fixture,
        Case {
            load: InsertOp::Overwrite,
            ..Case::new(DeletionMode::Key, StorageClass::Ebs)
        },
    )
    .await
}

/// Perturbation: the same load, with the list-files cache cleared before the
/// queries.
async fn key_mode_first_load_on_network_storage_cache_cleared(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(
        fixture,
        Case {
            query_env: QueryEnv::SharedCacheCleared,
            ..Case::new(DeletionMode::Key, StorageClass::Ebs)
        },
    )
    .await
}

/// Control: the fold's rewrite runs in the table's `RuntimeEnv`.
async fn key_mode_first_load_on_unknown_storage(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Case::new(DeletionMode::Key, StorageClass::Unknown)).await
}

/// Control: position deletes hide the superseded copies; nothing is folded.
async fn position_mode_first_load_on_network_storage(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(
        fixture,
        Case::new(DeletionMode::Position, StorageClass::Ebs),
    )
    .await
}

fn config(case: Case) -> VortexConfig {
    let mut config = file_mode_config();
    config.deletion_mode = case.deletion_mode;
    config.data_storage_class = case.storage_class;
    config.inline_max_rows = 0;
    config.inline_max_buffer_bytes = 0;
    config.write_concurrency = Some(2);
    if matches!(case.compaction, Compaction::Off) {
        config.compaction_background_interval_ms = 0;
        config.compaction_trigger_files = usize::MAX;
        config.compaction_trigger_protected_snapshots = usize::MAX;
        config.compaction_trigger_snapshot_age_ms = u64::MAX;
    }
    config
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

fn batch(schema: &SchemaRef, ids: Vec<i64>, rows: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(rows.clone().collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                rows.map(|row| format!("{row:016x}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn first_load(schema: &SchemaRef) -> Vec<RecordBatch> {
    (0..BATCHES)
        .map(|b| {
            let rows = b * BATCH_ROWS..(b + 1) * BATCH_ROWS;
            let ids = rows
                .clone()
                .map(|row| {
                    if b > 0 && row % REPEAT_EVERY == REPEAT_EVERY - 1 {
                        row - BATCH_ROWS - (REPEAT_EVERY - 1 - 500)
                    } else {
                        row
                    }
                })
                .collect();
            batch(schema, ids, rows)
        })
        .collect()
}

async fn run(fixture: common::TestFixture, case: Case) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema();
    let runtime_env = Arc::new(RuntimeEnv::default());
    assert!(
        runtime_env.cache_manager.get_list_files_cache().is_some(),
        "premise: the runtime env caches directory listings, as spiced's does"
    );
    let spec = || {
        TableSpec::new(TABLE, Arc::clone(&schema), &[])
            .config(config(case))
            .upsert_key("id")
    };
    let provider = open_table(&fixture, Arc::clone(&runtime_env), spec()).await;
    common::write_batches(&provider, first_load(&schema), case.load).await?;

    // Which snapshot each cached-but-removed file belongs to.
    let table = fixture.catalog.get_table(TABLE).await?;
    let protected: HashSet<String> = fixture
        .catalog
        .get_all_snapshot_sequences(&table.table_id)
        .await?
        .into_keys()
        .collect();
    let stale = cached_files_missing_on_disk(&runtime_env);
    let stale_dirs: HashSet<String> = stale
        .iter()
        .filter_map(|path| {
            std::path::Path::new(path)
                .parent()
                .and_then(|dir| dir.file_name())
                .map(|name| name.to_string_lossy().into_owned())
        })
        .collect();
    let roles: Vec<&str> = stale_dirs
        .iter()
        .map(|dir| {
            if *dir == table.current_snapshot_id {
                "current"
            } else if protected.contains(dir) {
                "protected"
            } else {
                "neither"
            }
        })
        .collect();
    eprintln!(
        "{case:?}: {} cached files missing on disk, in snapshots {roles:?}",
        stale.len()
    );

    if matches!(case.query_env, QueryEnv::SharedCacheCleared) {
        runtime_env
            .cache_manager
            .get_list_files_cache()
            .expect("list-files cache")
            .clear();
    }
    let ctx = session(&runtime_env, &provider)?;
    let after_load = count(&ctx).await;
    let after_load_later = if matches!(case.compaction, Compaction::Default) {
        // Long enough for the post-load maintenance to run on this small table.
        tokio::time::sleep(Duration::from_secs(10)).await;
        Some(count(&ctx).await)
    } else {
        None
    };

    let appended = BATCHES * BATCH_ROWS..BATCHES * BATCH_ROWS + APPENDED_ROWS;
    let append = common::write_batches(
        &provider,
        vec![batch(
            &schema,
            appended.clone().map(|row| row + 1_000_000).collect(),
            appended,
        )],
        InsertOp::Append,
    )
    .await
    .map(|rows| i64::try_from(rows).unwrap_or(i64::MAX))
    .map_err(|e| e.to_string());
    let after_append = count(&ctx).await;

    // A restart: a new process-wide `RuntimeEnv`, the table reopened from the
    // metastore.
    drop(ctx);
    drop(provider);
    let restarted_env = Arc::new(RuntimeEnv::default());
    let reopened = open_table(&fixture, Arc::clone(&restarted_env), spec()).await;
    let after_restart = count(&session(&restarted_env, &reopened)?).await;

    eprintln!(
        "{case:?}: after load {after_load:?}, {after_load_later:?} later; append {append:?}; after append {after_append:?}; after restart {after_restart:?}"
    );
    let loaded = Ok((LOADED_KEYS, LOADED_KEYS));
    let with_append = Ok((LOADED_KEYS + APPENDED_ROWS, LOADED_KEYS + APPENDED_ROWS));
    assert_eq!(
        (
            after_load,
            after_load_later,
            append,
            after_append,
            after_restart
        ),
        (
            loaded.clone(),
            matches!(case.compaction, Compaction::Default).then_some(loaded),
            Ok(APPENDED_ROWS),
            with_append.clone(),
            with_append,
        ),
        "every query after the first load must read only the files that remain"
    );
    if matches!(case.query_env, QueryEnv::Shared) {
        assert_eq!(
            stale,
            Vec::<String>::new(),
            "the list-files cache must not list a file the fold removed"
        );
    }
    Ok(())
}

fn session<T: TableProvider + 'static>(
    runtime_env: &Arc<RuntimeEnv>,
    provider: &Arc<T>,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(runtime_env));
    ctx.register_table(TABLE, Arc::clone(provider) as Arc<dyn TableProvider>)?;
    Ok(ctx)
}

async fn count(ctx: &SessionContext) -> Result<(i64, i64), String> {
    let batches = ctx
        .sql(&format!("SELECT count(*), count(DISTINCT id) FROM {TABLE}"))
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    let batch = batches.first().ok_or("no batch")?;
    let column = |i: usize| -> Result<i64, String> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|a| !a.is_empty())
            .map(|a| a.value(0))
            .ok_or_else(|| format!("column {i} is not one Int64"))
    };
    Ok((column(0)?, column(1)?))
}

/// The files the runtime env's list-files cache lists that are not on disk.
fn cached_files_missing_on_disk(runtime_env: &Arc<RuntimeEnv>) -> Vec<String> {
    let Some(cache) = runtime_env.cache_manager.get_list_files_cache() else {
        return Vec::new();
    };
    let mut missing = Vec::new();
    for key in cache.list_entries().keys() {
        if let Some(listing) = cache.get(key) {
            for meta in listing.iter() {
                let path = format!("/{}", meta.location);
                if !std::path::Path::new(&path).exists() {
                    missing.push(path);
                }
            }
        }
    }
    missing.sort();
    missing
}
