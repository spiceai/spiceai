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

//! A `mode: file_create` start recreates the table empty, so the source positions
//! its connector resumes from go with it: a kept position would skip every row the
//! new table does not have. A first start keeps the positions its connector records
//! before the table opens. Covers the accelerator's own clear, at the table open after
//! `init`; the runtime also clears them before it builds the data connector.

use std::{path::Path, sync::Arc};

use accelerator_cayenne::CayenneAccelerator;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry, DataAccelerator};
use datafusion::{common::TableReference, execution::context::SessionContext};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode},
    sidecar::OpenOption,
    snapshot::SnapshotBehavior,
    testing::TestAccelerationSource,
};
use runtime_checkpoint_api::{
    kafka::{KafkaCheckpoint, KafkaOffset},
    mysql_binlog::MySqlBinlogCheckpoint,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn source(dir: &Path) -> TestAccelerationSource {
    TestAccelerationSource::new("events").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::FileCreate,
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

/// A fresh process starting the dataset: `init`, then the table open.
async fn start(source: &TestAccelerationSource) -> Arc<CayenneAccelerator> {
    let (accelerator, registry) = init(source).await;
    open(&registry, source).await;
    accelerator
}

async fn init(
    source: &TestAccelerationSource,
) -> (Arc<CayenneAccelerator>, Arc<AcceleratorEngineRegistry>) {
    let accelerator = Arc::new(CayenneAccelerator::new());
    let registry = Arc::new(AcceleratorEngineRegistry::new());
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::clone(&accelerator) as Arc<dyn DataAccelerator>,
        )
        .await;
    accelerator.init(source).await.expect("init");
    (accelerator, registry)
}

async fn open(registry: &AcceleratorEngineRegistry, source: &TestAccelerationSource) {
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("events"),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(source),
            Arc::new(SessionContext::new()),
        )
        .await
        .expect("open the table");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_create_start_clears_the_positions_of_the_table_it_drops() {
    let root = tempfile::tempdir().expect("tempdir");
    let source = source(root.path());

    let first = start(&source).await;
    let sidecar = first
        .sidecar(&source, OpenOption::CreateIfNotExists)
        .await
        .expect("sidecar");
    // The load of the first table: its dataset checkpoint and its connector's positions.
    sidecar
        .dataset_checkpointer(SnapshotBehavior::Disabled)
        .await
        .expect("checkpointer")
        .checkpoint(&schema(), None)
        .await
        .expect("dataset checkpoint");
    sidecar
        .kafka_checkpoint_store()
        .expect("kafka store")
        .upsert(&kafka_checkpoint())
        .await
        .expect("kafka checkpoint");
    sidecar
        .mysql_binlog_store()
        .expect("mysql store")
        .upsert(&MySqlBinlogCheckpoint {
            binlog_file: "binlog.000002".to_string(),
            binlog_pos: 10_605,
            schema_json: None,
            gtid_executed: None,
            cursor_type: None,
            updated_at: None,
        })
        .await
        .expect("binlog position");
    drop((sidecar, first));

    let restarted = start(&source).await;
    let sidecar = restarted
        .sidecar(&source, OpenOption::OpenExisting)
        .await
        .expect("sidecar");
    assert!(
        sidecar
            .kafka_checkpoint_store()
            .expect("kafka store")
            .get()
            .await
            .expect("kafka get")
            .is_none(),
        "the Kafka offsets of the dropped table must not survive the recreate"
    );
    assert!(
        sidecar
            .mysql_binlog_store()
            .expect("mysql store")
            .get()
            .await
            .is_none(),
        "the binlog position of the dropped table must not survive the recreate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_start_keeps_the_positions_its_connector_records_before_the_table_opens() {
    let root = tempfile::tempdir().expect("tempdir");
    let source = source(root.path());

    let (accelerator, registry) = init(&source).await;
    accelerator
        .sidecar(&source, OpenOption::CreateIfNotExists)
        .await
        .expect("sidecar")
        .kafka_checkpoint_store()
        .expect("kafka store")
        .upsert(&kafka_checkpoint())
        .await
        .expect("kafka checkpoint");
    open(&registry, &source).await;

    let kept = accelerator
        .sidecar(&source, OpenOption::OpenExisting)
        .await
        .expect("sidecar")
        .kafka_checkpoint_store()
        .expect("kafka store")
        .get()
        .await
        .expect("kafka get");
    assert_eq!(
        kept.map(|checkpoint| checkpoint.consumer_group_id),
        Some("group-1".to_string()),
        "the consumer group the connector recorded for this start must stay"
    );
}
