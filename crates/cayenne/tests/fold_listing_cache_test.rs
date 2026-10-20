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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use cayenne::CayenneTableProvider;
use cayenne::metadata::{DeletionMode, StorageClass, VortexConfig};
use common::lookup_index::{
    REPEAT_EVERY, TableSpec, file_mode_config, keyed_rows, open_table, repeating_load,
};
use datafusion::catalog::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::dml::InsertOp;

const TABLE: &str = "loaded";
const BATCH_ROWS: i64 = 10_000;
const BATCHES: i64 = 8;
/// Rows of new keys the append after the load writes.
const APPENDED_ROWS: u64 = 100;
/// Distinct keys of the load: every repeated key's earlier copy is gone.
const LOADED_KEYS: i64 = BATCHES * BATCH_ROWS - (BATCHES - 1) * (BATCH_ROWS / REPEAT_EVERY);

#[derive(Clone, Copy, Debug)]
struct Case {
    /// How the load is written: a refresh's append into the empty table, or a
    /// full refresh's overwrite.
    load: InsertOp,
    deletion_mode: DeletionMode,
    storage_class: StorageClass,
    /// Clear the list-files cache after the load: the perturbation that pins a
    /// failure on the cached listing.
    clear_list_cache: bool,
}

impl Case {
    const fn new(deletion_mode: DeletionMode, storage_class: StorageClass) -> Self {
        Self {
            load: InsertOp::Append,
            deletion_mode,
            storage_class,
            clear_list_cache: false,
        }
    }
}

test_with_backends!(key_mode_first_load_on_network_storage);
test_with_backends!(key_mode_full_refresh_on_network_storage);
test_with_backends!(key_mode_first_load_on_network_storage_cache_cleared);
test_with_backends!(key_mode_first_load_on_unknown_storage);
test_with_backends!(position_mode_first_load_on_network_storage);

async fn key_mode_first_load_on_network_storage(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    run(fixture, Case::new(DeletionMode::Key, StorageClass::Ebs)).await
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
            clear_list_cache: true,
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
    // Nothing folds the load's snapshot away after it publishes.
    config.compaction_background_interval_ms = 0;
    config.compaction_trigger_files = usize::MAX;
    config.compaction_trigger_protected_snapshots = usize::MAX;
    config.compaction_trigger_snapshot_age_ms = u64::MAX;
    config
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

async fn drain(provider: &CayenneTableProvider) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(
        Duration::from_mins(1),
        provider.drain_in_flight_maintenance(),
    )
    .await??;
    Ok(())
}

async fn run(fixture: common::TestFixture, case: Case) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema();
    let runtime_env = Arc::new(RuntimeEnv::default());
    let spec = || {
        TableSpec::new(TABLE, Arc::clone(&schema), &[])
            .config(config(case))
            .upsert_key("id")
    };
    let provider = open_table(&fixture, Arc::clone(&runtime_env), spec()).await;
    common::write_batches(
        &provider,
        repeating_load(&schema, BATCHES, BATCH_ROWS),
        case.load,
    )
    .await?;

    let stale = cached_files_missing_on_disk(&runtime_env);
    if case.clear_list_cache {
        runtime_env
            .cache_manager
            .get_list_files_cache()
            .expect("list-files cache")
            .clear();
    }
    let ctx = session(&runtime_env, &provider)?;
    let after_load = count(&ctx).await;

    let appended = BATCHES * BATCH_ROWS..BATCHES * BATCH_ROWS + i64::try_from(APPENDED_ROWS)?;
    let append = common::write_batches(
        &provider,
        vec![keyed_rows(
            &schema,
            appended.clone().map(|row| row + 1_000_000).collect(),
            appended,
        )],
        InsertOp::Append,
    )
    .await
    .map_err(|e| e.to_string());
    let after_append = count(&ctx).await;

    // A restart: a new process-wide `RuntimeEnv`, the table reopened from the
    // metastore once this instance's maintenance has finished.
    drop(ctx);
    drain(&provider).await?;
    drop(provider);
    let restarted_env = Arc::new(RuntimeEnv::default());
    let reopened = open_table(&fixture, Arc::clone(&restarted_env), spec()).await;
    let after_restart = count(&session(&restarted_env, &reopened)?).await;

    let loaded = Ok((LOADED_KEYS, LOADED_KEYS));
    let appended_keys = LOADED_KEYS + i64::try_from(APPENDED_ROWS)?;
    let with_append = Ok((appended_keys, appended_keys));
    assert_eq!(
        (after_load, append, after_append, after_restart),
        (loaded, Ok(APPENDED_ROWS), with_append.clone(), with_append),
        "{case:?}: every query after the load must read only the files that remain"
    );
    if !case.clear_list_cache {
        assert_eq!(
            stale,
            Vec::<String>::new(),
            "{case:?}: the list-files cache must not list a file the fold removed"
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

/// `count(*)` and `count(DISTINCT id)`, or the query's error.
async fn count(ctx: &SessionContext) -> Result<(i64, i64), String> {
    let batches = ctx
        .sql(&format!("SELECT count(*), count(DISTINCT id) FROM {TABLE}"))
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    let batch = batches.first().ok_or("no batch")?;
    let column = |i: usize| batch.column(i).as_primitive::<Int64Type>().value(0);
    Ok((column(0), column(1)))
}

/// The files the runtime env's list-files cache lists that are not on disk.
fn cached_files_missing_on_disk(runtime_env: &Arc<RuntimeEnv>) -> Vec<String> {
    let cache = runtime_env
        .cache_manager
        .get_list_files_cache()
        .expect("the runtime env caches directory listings, as spiced's does");
    let mut missing: Vec<String> = cache
        .list_entries()
        .keys()
        .filter_map(|key| cache.get(key))
        .flat_map(|listing| {
            listing
                .iter()
                .map(|meta| format!("/{}", meta.location))
                .collect::<Vec<_>>()
        })
        .filter(|path| !std::path::Path::new(path).exists())
        .collect();
    missing.sort();
    missing
}
