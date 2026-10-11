/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use super::{AcceleratedTable, Builder, refresh};
use crate::federated::FederatedTable;
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{common::TableReference, datasource::TableProvider, prelude::SessionContext};
use runtime_acceleration::change_sink::{
    ChangeBatch, ChangeSink, ChangeSinkContext, WriteOptions, provider::ProviderChangeSinkBackend,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    runtime::Handle,
    sync::{Mutex, oneshot},
};

const WAIT: Duration = Duration::from_secs(5);

pub(super) async fn table() -> (AcceleratedTable, Arc<dyn TableProvider>, Arc<Mutex<()>>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let source = Arc::new(
        data_components::arrow::write::MemTable::try_new(Arc::clone(&schema), vec![vec![]])
            .expect("source"),
    );
    let accelerator: Arc<dyn TableProvider> = Arc::new(
        data_components::arrow::write::MemTable::try_new(schema, vec![vec![]])
            .expect("Arrow accelerator"),
    );
    let write_lock = Arc::new(Mutex::new(()));
    let mut builder = Builder::new(
        runtime_status::RuntimeStatus::new(),
        TableReference::bare("drain_test"),
        Arc::new(FederatedTable::new_unchecked(source)),
        "arrow".into(),
        Arc::clone(&accelerator),
        refresh::Refresh::new(super::RefreshMode::Disabled),
        Handle::current(),
    );
    builder.accelerator_write_mutex(Arc::clone(&write_lock));
    (
        builder.build().await.expect("table"),
        accelerator,
        write_lock,
    )
}

#[tokio::test]
async fn change_sink_drain_joins_producer_and_retains_accepted_write() {
    tokio::time::timeout(WAIT, async {
        let (mut table, accelerator, write_lock) = table().await;
        let mut context =
            ChangeSinkContext::new(TableReference::bare("drain_test"), Arc::clone(&accelerator));
        context.write_lock = Arc::clone(&write_lock);
        let sink = ChangeSink::new(
            Arc::new(ProviderChangeSinkBackend::new(context)),
            SessionContext::new(),
            &Handle::current(),
            2,
        );
        table.change_sink = Some(sink.clone());
        let blocked_write = write_lock.lock().await;
        let schema = accelerator.schema();
        let row = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .expect("accepted row");
        let batch = ChangeBatch::append(schema, vec![row]).expect("append");
        let (accepted_tx, accepted) = oneshot::channel();
        let (producer_lifetime, cancelled) = oneshot::channel::<()>();
        let (published_tx, mut published) = oneshot::channel();
        let producer_sink = sink.clone();
        let producer = tokio::spawn(async move {
            producer_sink
                .enqueue(
                    batch,
                    WriteOptions::default(),
                    Box::new(move |result| {
                        let _ = published_tx.send(result);
                    }),
                )
                .await
                .expect("admitted append");
            accepted_tx.send(()).expect("report admission");
            std::future::pending::<()>().await;
            drop(producer_lifetime);
        });
        table.handlers.lock().push(producer);
        accepted.await.expect("producer admitted the write");
        let mut cancelled_waiter = Box::pin(table.drain_changes());
        assert!(futures::poll!(cancelled_waiter.as_mut()).is_pending());
        drop(cancelled_waiter);
        tokio::time::timeout(Duration::ZERO, table.drain_changes())
            .await
            .expect_err("drain remains pending");
        assert!(
            cancelled.await.is_err(),
            "the producer future must be destroyed"
        );
        assert!(matches!(
            published.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(sink.reserve().await.is_err(), "drain fences new admission");
        let first = table.begin_changes_drain();
        let second = table.begin_changes_drain();
        drop(blocked_write);
        first.wait().await.expect("first drain waiter");
        second.wait().await.expect("second drain waiter");
        table
            .drain_changes()
            .await
            .expect("retry observes completed drain");
        published
            .await
            .expect("retained publication callback")
            .expect("successful write");
        let rows = SessionContext::new()
            .read_table(accelerator)
            .expect("read accelerator")
            .collect()
            .await
            .expect("actual Arrow rows");
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let row = rows
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .expect("one row");
        assert_eq!(
            row.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id")
                .value(0),
            42
        );
    })
    .await
    .expect("drain must not hang");
}
