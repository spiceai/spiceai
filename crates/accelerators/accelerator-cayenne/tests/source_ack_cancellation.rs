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

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::{Int64Array, ListArray, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig},
};
use data_accelerator_api::DataAccelerator;
use data_components::cdc::{self, ChangeEnvelope, CommitChange, CommitError};
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::{StreamExt, stream};
use runtime_acceleration::{
    acceleration::RefreshMode,
    change_sink::{ChangeBatch, ChangeSinkContext, Recovery, StorageDurability, WriteOptions},
};
use runtime_table::{
    accelerated::{refresh::Refresh, refresh_task::RefreshTaskBuilder},
    federated::FederatedTable,
};
use tokio::{
    io::AsyncWriteExt,
    runtime::Handle,
    sync::{Mutex, Notify, RwLock, Semaphore},
};

/// The source is an immutable newline-delimited file. Its checkpoint is a byte
/// offset into that file, so an unacknowledged record can be read again.
struct FileCommitter {
    checkpoint: PathBuf,
    offset: usize,
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    dropped: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl CommitChange for FileCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let permit = self.release.acquire().await.expect("source commit gate");
        let result = async {
            let mut file = tokio::fs::File::create(&self.checkpoint).await?;
            file.write_all(self.offset.to_string().as_bytes()).await?;
            file.sync_all().await
        }
        .await;
        drop(permit);
        result.map_err(|source| CommitError::UnableToCommitChange {
            source: Box::new(source),
        })
    }

    fn supports_deferral(&self) -> bool {
        true
    }
}

impl Drop for FileCommitter {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn change(table: &CayenneTableProvider, id: i64) -> cdc::ChangeBatch {
    let data = RecordBatch::try_new(table.schema(), vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("source row");
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        Arc::new(StringArray::from(vec!["id"])),
        None,
    )
    .expect("source primary key");
    let record = RecordBatch::try_new(
        Arc::new(cdc::changes_schema(&table.schema())),
        vec![
            Arc::new(StringArray::from(vec!["u"])),
            Arc::new(keys),
            Arc::new(StructArray::from(data)),
        ],
    )
    .expect("source change");
    cdc::ChangeBatch::try_new(record).expect("CDC envelope batch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_checkpoint_retains_source_ack_after_producer_cancellation() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let root = std::env::var_os("SOURCE_ACK_PROBE_DIR")
        .map_or_else(|| temp.path().to_path_buf(), PathBuf::from);
    tokio::fs::create_dir_all(&root)
        .await
        .expect("artifact directory");
    let source = root.join("source.log");
    let checkpoint = root.join("source.checkpoint");
    tokio::fs::write(&source, b"7\n9\n")
        .await
        .expect("source log");
    tokio::fs::write(&checkpoint, b"0")
        .await
        .expect("initial checkpoint");
    let input = tokio::fs::read_to_string(&source)
        .await
        .expect("read source");
    let mut records = input.split_inclusive('\n');
    let first = records.next().expect("first source record");
    let first_offset = first.len();
    let first_id = first.trim().parse::<i64>().expect("first source key");
    let second_id = records
        .next()
        .expect("second source record")
        .trim()
        .parse::<i64>()
        .expect("second source key");

    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", root.join("catalog.db").display()))
            .expect("native catalog"),
    );
    catalog.init().await.expect("initialize native catalog");
    let table = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: "source_ack".into(),
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                primary_key: vec!["id".into()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".into()]))),
                base_path: root.join("data").to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: VortexConfig {
                    cdc_durability: CdcDurability::Memory,
                    deletion_mode: DeletionMode::Key,
                    cdc_mem_tier_checkpoint_interval_ms: 0,
                    compaction_background_interval_ms: 0,
                    ..VortexConfig::default()
                },
            })
            .await
            .expect("create native table"),
    );
    let target = Arc::clone(&table) as Arc<dyn TableProvider>;
    let binding = ChangeSinkContext::new(TableReference::bare("source_ack"), Arc::clone(&target));
    let write_lock = Arc::clone(&binding.write_lock);
    let sink = CayenneAccelerator::new()
        .change_sink(binding, &Handle::current(), 2)
        .await
        .expect("bind native backend")
        .expect("native sink");
    let checkpoint_rows = Arc::new(AtomicU64::new(0));
    let callback_table = Arc::clone(&table);
    let callback_rows = Arc::clone(&checkpoint_rows);
    let task = RefreshTaskBuilder::new(
        Arc::default(),
        TableReference::bare("source_ack"),
        Arc::new(FederatedTable::new_unchecked(Arc::clone(&target))),
        None,
        target,
        Handle::current(),
        write_lock,
    )
    .with_change_sink(Some(sink.clone()))
    .with_on_stream_batch_process_callback(Some(Arc::new(Mutex::new(Box::new(move || {
        let table = Arc::clone(&callback_table);
        let rows = Arc::clone(&callback_rows);
        Box::pin(async move {
            rows.store(
                table
                    .checkpoint_mem_tier()
                    .await
                    .expect("checkpoint published row"),
                Ordering::SeqCst,
            );
        })
    })))))
    .build();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let envelope = ChangeEnvelope::from_parts(
        Box::new(FileCommitter {
            checkpoint: checkpoint.clone(),
            offset: first_offset,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            dropped: Arc::clone(&dropped),
            calls: Arc::clone(&calls),
        }),
        change(&table, first_id),
        true,
        false,
    );
    let producer = tokio::spawn(async move {
        task.start_changes_stream(
            Arc::new(RwLock::new(Refresh::new(RefreshMode::Changes))),
            Box::pin(stream::iter([Ok(envelope)]).chain(stream::pending())),
            None,
            None,
            Arc::new(AtomicBool::new(true)),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(30), entered.notified())
        .await
        .expect("published row reached the suspended source acknowledgement");
    assert_eq!(checkpoint_rows.load(Ordering::SeqCst), 1);
    assert!(table.has_slot_advancer(), "native CDC durability observer");
    producer.abort();
    assert!(
        producer
            .await
            .expect_err("producer was aborted")
            .is_cancelled()
    );
    let dropped_after_cancel = dropped.load(Ordering::SeqCst);
    let checkpoint_after_cancel = tokio::fs::read_to_string(&checkpoint)
        .await
        .expect("checkpoint");
    release.add_permits(1);

    // A later real storage checkpoint must retry the retained source prefix.
    // This direct write deliberately does not acknowledge the second source row.
    let receipt = sink
        .reserve()
        .await
        .expect("next capacity")
        .submit(
            ChangeBatch::cdc(change(&table, second_id)),
            WriteOptions {
                recovery: Recovery::Replayable,
                ..WriteOptions::default()
            },
        )
        .expect("admit next source row")
        .wait()
        .await
        .expect("next receipt");
    receipt.published().await.expect("next publication");
    assert!(matches!(receipt.durability, StorageDurability::Deferred(_)));
    sink.flush()
        .await
        .expect("retry source prefix at next checkpoint");
    let checkpoint_after_retry = tokio::fs::read_to_string(&checkpoint)
        .await
        .expect("checkpoint");
    ctx.register_table("source_ack", Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register native query target");
    let rows = ctx
        .sql("SELECT id FROM source_ack ORDER BY id")
        .await
        .expect("query")
        .collect()
        .await
        .expect("native rows");
    let actual_ids: Vec<i64> = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id array")
                .values()
                .iter()
                .copied()
        })
        .collect();
    sink.begin_close().wait().await.expect("close native sink");
    let result = serde_json::json!({
        "source": input,
        "first_source_byte_offset": first_offset,
        "native_checkpoint_rows": checkpoint_rows.load(Ordering::SeqCst),
        "source_commit_calls": calls.load(Ordering::SeqCst),
        "native_rows": actual_ids,
        "committer_dropped_after_cancel": dropped_after_cancel,
        "checkpoint_after_cancel": checkpoint_after_cancel,
        "checkpoint_after_retry": checkpoint_after_retry,
        "producer_join": "cancelled",
        "sink_close": "success"
    });
    tokio::fs::write(
        root.join("result.json"),
        serde_json::to_vec_pretty(&result).expect("result JSON"),
    )
    .await
    .expect("retain probe result");
    println!("{result}");
    assert_eq!(actual_ids, [first_id, second_id]);
    assert_eq!(checkpoint_after_cancel, "0");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "resume the in-flight source call"
    );
    assert_eq!(
        dropped_after_cancel, 0,
        "accepted source acknowledgement remains owned"
    );
    assert_eq!(
        checkpoint_after_retry,
        first_offset.to_string(),
        "source checkpoint advances only through the acknowledged record"
    );
}
