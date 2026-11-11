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

//! Sustained commits through the object-native catalog keep the number of log
//! objects bounded, and a catalog reopened from storage afterwards still holds
//! every committed row.
//!
//! The store's clock runs a quarter of the retention ahead per round, so a
//! floor's objects become deletable four rounds after it is published. With a
//! retention no run reaches, the same history keeps every object: the bound is
//! what the collection buys, not something the history stays under anyway.

use std::sync::Arc;
use std::time::Duration;

use cayenne::MetadataCatalog;
use cayenne::metadata::{CreateTableOptions, InlinedData, VortexConfig};
use cayenne::metastore::object_native::{NativeCatalogConfig, ObjectNativeCatalog};
use futures::TryStreamExt;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use object_store_occ::headlog::HeadLogConfig;
use object_store_sim::fault::FaultStore;

const ROUNDS: usize = 40;
const COMMITS_PER_ROUND: usize = 16;
const RETENTION: Duration = Duration::from_secs(3600);
/// Every fourth row is larger than the log's spill threshold, so its record's
/// payload is an object of its own.
const SPILLED_BYTES: usize = 20 * 1024;

fn config(retention: Duration) -> NativeCatalogConfig {
    NativeCatalogConfig {
        log: HeadLogConfig {
            batch_max_records: 4,
            ..HeadLogConfig::default()
        },
        checkpoint_after_batches: 1,
        retain_below_floor: retention,
        ..NativeCatalogConfig::default()
    }
}

/// Batches, spilled payloads and checkpoints of every log in the catalog.
async fn log_objects(store: &Arc<dyn ObjectStore>) -> usize {
    let listed: Vec<_> = store.list(None).try_collect().await.expect("list");
    listed
        .iter()
        .filter(|meta| {
            let path = meta.location.as_ref();
            ["/wal/", "/records/", "/checkpoints/"]
                .iter()
                .any(|directory| path.contains(directory))
        })
        .count()
}

/// Commit `ROUNDS * COMMITS_PER_ROUND` rows, one record each, and return the
/// log object count after each round and the store.
async fn sustained(retention: Duration) -> (Vec<usize>, Arc<dyn ObjectStore>, String) {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
    let catalog = ObjectNativeCatalog::create_with_config(
        Arc::clone(&store),
        Path::from("catalog"),
        config(retention),
    )
    .await
    .expect("create the catalog");
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "orders".to_string(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ])),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/unused/orders".to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("create table");
    let mut counts = Vec::with_capacity(ROUNDS);
    let mut sequence = 0_i64;
    for _ in 0..ROUNDS {
        for _ in 0..COMMITS_PER_ROUND {
            sequence += 1;
            let size = if sequence % 4 == 0 { SPILLED_BYTES } else { 64 };
            catalog
                .add_inlined_data(InlinedData {
                    inlined_id: String::new(),
                    table_id: table_id.clone(),
                    partition_key: None,
                    data_ipc: payload(sequence, size),
                    record_count: 1,
                    sequence_number: sequence,
                    created_at: String::new(),
                })
                .await
                .expect("commit a row");
        }
        // Collections run beside the commits; let the last one finish.
        tokio::time::sleep(Duration::from_millis(50)).await;
        counts.push(log_objects(&store).await);
        faults.advance_clock(RETENTION / 4);
    }
    (counts, store, table_id)
}

fn payload(sequence: i64, size: usize) -> Vec<u8> {
    let mut bytes = sequence.to_le_bytes().to_vec();
    bytes.resize(size, u8::try_from(sequence % 251).expect("small"));
    bytes
}

#[tokio::test]
async fn sustained_commits_keep_log_objects_bounded_and_every_row() {
    let (counts, store, table_id) = sustained(RETENTION).await;
    // Each round seals four batches and stages four checkpoints, and four of
    // its rows spill: 12 objects. A floor's objects go four rounds after it is
    // published, by a collection that runs at a later checkpoint, so at most
    // six rounds' worth are ever present.
    let per_round = 12;
    let bound = 6 * per_round;
    let (warmup, steady) = counts.split_at(ROUNDS / 4);
    eprintln!("LOG_OBJECTS warmup={warmup:?} steady={steady:?} bound={bound}");
    let max = *steady.iter().max().expect("rounds");
    assert!(
        max <= bound,
        "log objects per round {counts:?} exceed {bound}"
    );
    assert!(
        *counts.last().expect("rounds") >= 4 * per_round,
        "a retention's worth of objects stays: {counts:?}"
    );

    // Reopened from storage alone, the catalog holds every row in order.
    let reopened = ObjectNativeCatalog::open_with_config(
        Arc::clone(&store),
        Path::from("catalog"),
        config(RETENTION),
    )
    .await
    .expect("reopen");
    let mut rows = reopened
        .get_inlined_data(&table_id)
        .await
        .expect("rows after reopen");
    rows.sort_by_key(|row| row.sequence_number);
    let total = i64::try_from(ROUNDS * COMMITS_PER_ROUND).expect("small");
    assert_eq!(
        rows.iter().map(|row| row.sequence_number).collect::<Vec<_>>(),
        (1..=total).collect::<Vec<_>>()
    );
    for row in &rows {
        let size = if row.sequence_number % 4 == 0 {
            SPILLED_BYTES
        } else {
            64
        };
        assert_eq!(row.data_ipc, payload(row.sequence_number, size));
    }
}

/// Without a collection that comes due, the same history keeps every object.
#[tokio::test]
async fn sustained_commits_without_collection_grow_without_bound() {
    let (counts, _, _) = sustained(Duration::from_secs(100 * 365 * 24 * 3600)).await;
    eprintln!("LOG_OBJECTS uncollected={counts:?}");
    assert!(
        counts.windows(2).all(|pair| pair[1] > pair[0]),
        "every round adds objects: {counts:?}"
    );
    assert!(
        *counts.last().expect("rounds") > 6 * 12 * 4,
        "far past the collected bound: {counts:?}"
    );
}
