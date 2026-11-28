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

//! The sidecar shares `cayenne.db` with the metastore, so its writes wait for the
//! database's write lock as long as the metastore's do: a restore writes the sidecar
//! while the restores of other datasets hold that lock.

use std::{path::Path, time::Duration};

use accelerator_cayenne::CayenneAccelerator;
use data_accelerator_api::DataAccelerator;
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode},
    sidecar::OpenOption,
    testing::TestAccelerationSource,
};
use runtime_checkpoint_api::kafka::{KafkaCheckpoint, KafkaOffset};

fn source(dir: &Path) -> TestAccelerationSource {
    TestAccelerationSource::new("events").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        params: [
            (
                "cayenne_file_path".to_string(),
                dir.join("data").to_string_lossy().into_owned(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join("metadata").to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    })
}

fn kafka_checkpoint() -> KafkaCheckpoint {
    KafkaCheckpoint {
        consumer_group_id: "group-1".to_string(),
        topic: "events".to_string(),
        schema_json: "{}".to_string(),
        offsets: vec![KafkaOffset {
            topic: "events".to_string(),
            partition: 0,
            offset: 999,
        }],
    }
}

/// Takes `db`'s write lock now and releases it after `hold`.
fn hold_write_lock(db: &Path, hold: Duration) -> std::thread::JoinHandle<()> {
    let holder = rusqlite::Connection::open(db).expect("open the holder");
    holder
        .execute_batch("BEGIN IMMEDIATE")
        .expect("take the write lock");
    std::thread::spawn(move || {
        std::thread::sleep(hold);
        holder
            .execute_batch("COMMIT")
            .expect("release the write lock");
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sidecar_write_waits_out_a_write_lock_held_for_seconds() {
    let root = tempfile::tempdir().expect("tempdir");
    let source = source(root.path());
    let accelerator = CayenneAccelerator::new();
    accelerator.init(&source).await.expect("init");
    let sidecar = accelerator
        .sidecar(&source, OpenOption::CreateIfNotExists)
        .await
        .expect("sidecar");
    let store = sidecar.kafka_checkpoint_store().expect("kafka store");

    // Seconds, within the metastore's 30 s wait.
    let holder = hold_write_lock(
        &root.path().join("metadata").join("cayenne.db"),
        Duration::from_secs(6),
    );
    let upserted = store.upsert(&kafka_checkpoint()).await;
    holder.join().expect("holder");
    upserted.expect("the write waits for the lock");
}
