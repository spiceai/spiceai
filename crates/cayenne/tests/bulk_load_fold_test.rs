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

//! Differential coverage of a first keyed load with repeated composite keys.

use crate::common;

use arrow::array::{AsArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::{TableProvider, memory::MemorySourceConfig};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use util::session_state::{SupersededReason, SupersededRows};

const SEED: u64 = 0x5eed_cafe_1234_5678;

test_with_backends!(bulk_load_round_robin_keeps_last_arrival_impl);
test_with_backends!(bulk_load_clustered_keeps_last_arrival_impl);

async fn bulk_load_round_robin_keeps_last_arrival_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    check_load(fixture, false).await
}

async fn bulk_load_clustered_keeps_last_arrival_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    check_load(fixture, true).await
}

async fn check_load(
    fixture: common::TestFixture,
    clustered: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("bulk load seed={SEED:#x}, clustered={clustered}");
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
        Field::new("padding", DataType::Utf8, false),
    ]));
    let superseded = Arc::new(SupersededRows::default());
    let mut config = SessionConfig::new().with_target_partitions(4);
    config.set_extension(Arc::clone(&superseded));
    let ctx = SessionContext::new_with_config(config);
    let keys = vec!["region".to_string(), "id".to_string()];
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: "loaded".into(),
                schema: Arc::clone(&schema),
                primary_key: keys.clone(),
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(keys))),
                base_path: fixture.data_path.to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: VortexConfig {
                    deletion_mode: DeletionMode::Key,
                    inline_max_rows: 0,
                    inline_max_buffer_bytes: 0,
                    target_vortex_file_size_mb: 1,
                    write_concurrency: Some(4),
                    cluster_by: if clustered { vec!["id".into()] } else { vec![] },
                    compaction_background_interval_ms: 0,
                    compaction_trigger_files: usize::MAX,
                    ..Default::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table("loaded", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let mut rng = SEED;
    let mut model = BTreeMap::new();
    let mut batches = Vec::new();
    let mut within_batch_dropped = 0_usize;
    // A fixed LCG supplies a reproducible history without a platform RNG.
    for batch_index in 0..32_i64 {
        let mut regions = Vec::new();
        let mut ids = Vec::new();
        let mut payloads = Vec::new();
        let mut batch_keys = std::collections::BTreeSet::new();
        for row in 0..4096_i64 {
            rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let region = format!("r{}", (rng >> 32) % 4);
            let id = i64::try_from((rng >> 40) % 8192)?;
            let payload = batch_index * 4096 + row;
            if !batch_keys.insert((region.clone(), id)) {
                within_batch_dropped += 1;
            }
            model.insert((region.clone(), id), payload);
            regions.push(region);
            ids.push(id);
            payloads.push(payload);
        }
        batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(regions)),
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(payloads)),
                Arc::new(StringArray::from(vec!["x".repeat(1024); 4096])),
            ],
        )?);
    }
    let input_rows = 32 * 4096;
    assert!(
        within_batch_dropped > 0,
        "seed={SEED:#x}: within-batch repeats required"
    );
    assert!(
        input_rows - model.len() > within_batch_dropped,
        "seed={SEED:#x}: cross-batch repeats required"
    );
    let input = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)?;
    let insert = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await?;
    collect(insert, ctx.task_ctx()).await?;
    let result = ctx
        .sql("SELECT region, id, payload FROM loaded ORDER BY region, id")
        .await?
        .collect()
        .await?;
    let actual: Vec<_> = result
        .iter()
        .flat_map(|batch| {
            let regions = batch.column(0).as_string::<i32>();
            let ids = batch.column(1).as_primitive::<Int64Type>();
            let payloads = batch.column(2).as_primitive::<Int64Type>();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        (regions.value(row).to_string(), ids.value(row)),
                        payloads.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(actual.len(), model.len(), "seed={SEED:#x}");
    assert_eq!(
        actual,
        model.clone().into_iter().collect::<Vec<_>>(),
        "seed={SEED:#x}"
    );
    assert_eq!(
        superseded.get(SupersededReason::Arrival),
        u64::try_from(input_rows - model.len())?,
        "seed={SEED:#x}"
    );
    assert_eq!(superseded.get(SupersededReason::Older), 0, "seed={SEED:#x}");
    let metadata = fixture.catalog.get_table("loaded").await?;
    // Key-mode appends publish protected snapshots alongside the base snapshot.
    let mut snapshot_ids: std::collections::BTreeSet<_> = fixture
        .catalog
        .get_all_snapshot_sequences(&metadata.table_id)
        .await?
        .into_keys()
        .collect();
    snapshot_ids.insert(metadata.current_snapshot_id.clone());
    let mut files = Vec::new();
    for snapshot_id in snapshot_ids {
        let snapshot_dir = std::path::Path::new(&metadata.path)
            .join(&metadata.table_id)
            .join(snapshot_id);
        for entry in std::fs::read_dir(snapshot_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "vortex") {
                files.push(path);
            }
        }
    }
    assert!(
        files.len() > 1,
        "seed={SEED:#x}: fold must retain multiple data files, got {}",
        files.len()
    );
    eprintln!(
        "seed={SEED:#x}: input={input_rows}, live={}, superseded={}, snapshot_files={}",
        model.len(),
        input_rows - model.len(),
        files.len()
    );
    Ok(())
}
