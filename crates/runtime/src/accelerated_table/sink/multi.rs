/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::TableProvider,
    error::DataFusionError,
    execution::{RecordBatchStream, SessionState},
    logical_expr::dml::InsertOp,
    physical_plan::collect,
    prelude::SessionContext,
};
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast, watch, Barrier, BarrierWaitResult};
use tokio::task::JoinSet;
use tokio_stream::wrappers::BroadcastStream;
use util::RetryError;

use crate::{
    accelerated_table::{refresh_task::retry_from_df_error, synchronized_table::SynchronizedTable},
    datafusion::error::find_datafusion_root,
    dataupdate::StreamingDataUpdateExecutionPlan,
};

pub(crate) struct MultiSink {
    original_table_provider: Arc<dyn TableProvider>,
    synchronized_tables: Vec<SynchronizedTable>,
}

impl MultiSink {
    pub fn new(
        original_table_provider: Arc<dyn TableProvider>,
        synchronized_tables: Vec<SynchronizedTable>,
    ) -> Self {
        Self {
            original_table_provider,
            synchronized_tables,
        }
    }

    pub fn add_synchronized_table(&mut self, synchronized_table: SynchronizedTable) {
        self.synchronized_tables.push(synchronized_table);
    }

    pub fn synchronized_tables(&self) -> &Vec<SynchronizedTable> {
        &self.synchronized_tables
    }

    async fn spawn_base_task(
        provider: Arc<dyn TableProvider>,
        ctx_state: SessionState,
        stream: RecordBatchBroadcastStream,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let insertion_plan = provider
            .insert_into(
                &ctx_state,
                Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(stream))),
                overwrite,
            )
            .await
            .map_err(|e| {
                RetryError::permanent(crate::accelerated_table::Error::FailedToWriteData {
                    source: find_datafusion_root(e),
                })
            })?;

        let _ = collect(insertion_plan, ctx_state.task_ctx())
            .await
            .map_err(retry_from_df_error);
        Ok(())
    }

    async fn spawn_parent_task(
        provider: Arc<dyn TableProvider>,
        ctx_state: SessionState,
        rx: broadcast::Receiver<RecordBatch>,
        schema: SchemaRef,
        parent_complete_tx: watch::Sender<bool>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let stream = RecordBatchBroadcastStream::new(rx, schema, StreamType::parent());

        let result = Self::spawn_base_task(provider, ctx_state, stream, overwrite).await;
        let _ = parent_complete_tx.send(true);
        result
    }

    async fn spawn_child_task(
        provider: Arc<dyn TableProvider>,
        ctx_state: SessionState,
        rx: broadcast::Receiver<RecordBatch>,
        schema: SchemaRef,
        barrier: Arc<Barrier>,
        parent_complete_rx: watch::Receiver<bool>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let stream = RecordBatchBroadcastStream::new(
            rx,
            schema,
            StreamType::child(barrier, parent_complete_rx),
        );

        Self::spawn_base_task(provider, ctx_state, stream, overwrite).await
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let schema = record_batch_stream.schema();
        let (tx, _) = broadcast::channel::<RecordBatch>(32);
        let mut join_set = JoinSet::new();

        let ctx = SessionContext::new();

        let (parent_complete_tx, parent_complete_rx) = watch::channel(false);
        let child_barrier = Arc::new(Barrier::new(self.synchronized_tables.len()));

        // Spawn primary task
        let primary_provider = Arc::clone(&self.original_table_provider);
        join_set.spawn(Self::spawn_parent_task(
            primary_provider,
            ctx.state(),
            tx.subscribe(),
            Arc::clone(&schema),
            parent_complete_tx,
            overwrite,
        ));

        // Spawn child tasks
        for provider in self
            .synchronized_tables
            .iter()
            .map(SynchronizedTable::child_accelerator)
        {
            join_set.spawn(Self::spawn_child_task(
                provider,
                ctx.state(),
                tx.subscribe(),
                Arc::clone(&schema),
                Arc::clone(&child_barrier),
                parent_complete_rx.clone(),
                overwrite,
            ));
        }

        // Process input stream
        let mut stream = record_batch_stream;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(retry_from_df_error)?;
            if tx.send(batch).is_err() {
                break;
            }
        }
        drop(tx);

        // Handle task results
        let mut errors = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => (),
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(RetryError::Permanent(
                    crate::accelerated_table::Error::FailedToWriteData {
                        source: DataFusionError::External(Box::new(e)),
                    },
                )),
            }
        }

        if let Some(first_error) = errors.into_iter().next() {
            return Err(first_error);
        }

        Ok(())
    }
}

// Stream implementation that wraps a broadcast::Receiver
struct RecordBatchBroadcastStream {
    inner: BroadcastStream<RecordBatch>,
    schema: SchemaRef,
    stream_type: StreamType,
    reached_end: bool,
}

enum StreamType {
    Parent,
    Child {
        barrier: Arc<Barrier>,
        parent_complete_rx: watch::Receiver<bool>,
        barrier_wait: Option<Pin<Box<dyn Future<Output = BarrierWaitResult> + Send>>>,
        parent_complete_rx_changed: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    },
}

impl StreamType {
    fn parent() -> Self {
        Self::Parent
    }

    fn child(barrier: Arc<Barrier>, parent_complete_rx: watch::Receiver<bool>) -> Self {
        Self::Child {
            barrier,
            parent_complete_rx,
            barrier_wait: None,
            parent_complete_rx_changed: None,
        }
    }
}

impl RecordBatchBroadcastStream {
    fn new(
        rx: broadcast::Receiver<RecordBatch>,
        schema: SchemaRef,
        stream_type: StreamType,
    ) -> Self {
        Self {
            inner: BroadcastStream::new(rx),
            schema,
            stream_type,
            reached_end: false,
        }
    }
}

impl Stream for RecordBatchBroadcastStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(
                result.map_err(|e| DataFusionError::External(Box::new(e))),
            )),
            Poll::Ready(None) => {
                if self.reached_end {
                    match &mut self.stream_type {
                        StreamType::Parent => Poll::Ready(None),
                        StreamType::Child {
                            barrier,
                            parent_complete_rx,
                            barrier_wait,
                            parent_complete_rx_changed,
                        } => {
                            // Check if parent is complete
                            if !*parent_complete_rx.borrow() {
                                if parent_complete_rx_changed.is_none() {
                                    let mut rx = parent_complete_rx.clone();
                                    *parent_complete_rx_changed = Some(Box::pin(async move {
                                        let _ = rx.changed().await;
                                    }));
                                }

                                // Safe to use as_mut() here since we just checked it's Some
                                if let Some(changed) = parent_complete_rx_changed.as_mut() {
                                    match changed.as_mut().poll(cx) {
                                        Poll::Ready(()) => {
                                            *parent_complete_rx_changed = None;
                                            if !*parent_complete_rx.borrow() {
                                                return Poll::Pending;
                                            }
                                        }
                                        Poll::Pending => return Poll::Pending,
                                    }
                                } else {
                                    // This shouldn't happen due to the check above, but handle it safely
                                    return Poll::Pending;
                                }
                            }

                            // Parent is complete, wait on barrier
                            if barrier_wait.is_none() {
                                let barrier = Arc::clone(barrier);
                                *barrier_wait = Some(Box::pin(async move { barrier.wait().await }));
                            }

                            // Safe to use as_mut() here since we just ensured it's Some
                            if let Some(wait) = barrier_wait.as_mut() {
                                match wait.as_mut().poll(cx) {
                                    Poll::Ready(_) => Poll::Ready(None),
                                    Poll::Pending => Poll::Pending,
                                }
                            } else {
                                // This shouldn't happen due to the check above, but handle it safely
                                Poll::Pending
                            }
                        }
                    }
                } else {
                    self.reached_end = true;
                    self.poll_next(cx)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RecordBatchBroadcastStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use std::time::Duration;
    use tokio::time::sleep;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn create_test_record_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            create_test_schema(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("valid record batch")
    }

    #[tokio::test]
    async fn test_data_transmission() {
        let (tx, rx) = broadcast::channel(32);
        let schema = create_test_schema();

        // Send batches first to ensure they're available
        let batches = vec![
            create_test_record_batch(&[1, 2, 3]),
            create_test_record_batch(&[4, 5, 6]),
        ];

        for batch in &batches {
            tx.send(batch.clone()).expect("to send record batch");
        }
        drop(tx);

        let mut stream = RecordBatchBroadcastStream::new(rx, schema, StreamType::parent());

        let mut received_values: Vec<i32> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.expect("valid record batch");
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("valid int32 array");
            received_values.extend(array.values());
        }

        assert_eq!(received_values, vec![1, 2, 3, 4, 5, 6]);
    }

    #[allow(clippy::cast_possible_wrap)]
    #[allow(clippy::similar_names)]
    #[tokio::test]
    async fn test_barrier_synchronization() {
        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx1) = broadcast::channel(32);
        let rx2 = tx.subscribe();
        let (parent_complete_tx, parent_complete_rx1) = watch::channel(false);
        let parent_complete_rx2 = parent_complete_rx1.clone();
        let schema = create_test_schema();

        let stream1 = RecordBatchBroadcastStream::new(
            rx1,
            Arc::clone(&schema),
            StreamType::child(Arc::clone(&barrier), parent_complete_rx1),
        );
        let stream2 = RecordBatchBroadcastStream::new(
            rx2,
            schema,
            StreamType::child(barrier, parent_complete_rx2),
        );

        let now = std::time::Instant::now();

        // Start both tasks
        let task1 = tokio::spawn({
            let mut stream = stream1;
            async move {
                let mut results = Vec::new();
                while let Some(result) = stream.next().await {
                    results.push(result.expect("valid record batch"));
                }
                (results, now.elapsed())
            }
        });

        let task2 = tokio::spawn({
            let mut stream = stream2;
            async move {
                let mut results = Vec::new();
                while let Some(result) = stream.next().await {
                    sleep(Duration::from_millis(100)).await;
                    results.push(result.expect("valid record batch"));
                }
                (results, now.elapsed())
            }
        });

        // Send data and signal completion
        tx.send(create_test_record_batch(&[1, 2, 3]))
            .expect("to send record batch");
        drop(tx);

        // Wait a bit before signaling parent completion
        sleep(Duration::from_millis(50)).await;
        parent_complete_tx
            .send(true)
            .expect("send completion signal");

        let (result1, result2) = tokio::join!(task1, task2);
        let (results1, time1) = result1.expect("task 1 completed");
        let (results2, time2) = result2.expect("task 2 completed");

        // Verify results
        assert_eq!(results1.len(), 1);
        assert_eq!(results2.len(), 1);
        assert!(
            time1.as_millis() >= 100,
            "Fast stream completed too quickly: {time1:?}"
        );
        assert!(
            (time1.as_millis() as i128 - time2.as_millis() as i128).abs() < 50,
            "Streams completed too far apart: {time1:?} vs {time2:?}"
        );
    }

    #[tokio::test]
    async fn test_error_handling() {
        let barrier = Arc::new(Barrier::new(1));
        let (tx, rx) = broadcast::channel::<RecordBatch>(1); // Small buffer to force lagging error
        let (_parent_complete_tx, parent_complete_rx) = watch::channel(false);
        let schema = create_test_schema();

        let mut stream = RecordBatchBroadcastStream::new(
            rx,
            schema,
            StreamType::child(barrier, parent_complete_rx),
        );

        // Fill the channel and cause a lagging error
        for i in 0..10 {
            tx.send(create_test_record_batch(&[i])).unwrap_or_default();
        }

        // Verify we get an error when reading from a lagged stream
        if let Some(result) = stream.next().await {
            assert!(result.is_err());
        } else {
            panic!("expected error");
        }
    }

    #[tokio::test]
    async fn test_child_waits_for_parent() {
        let barrier = Arc::new(Barrier::new(1));
        let (tx, rx) = broadcast::channel(32);
        let (parent_complete_tx, parent_complete_rx) = watch::channel(false);
        let schema = create_test_schema();

        // Create child stream
        let mut child_stream = RecordBatchBroadcastStream::new(
            rx,
            Arc::clone(&schema),
            StreamType::child(barrier, parent_complete_rx),
        );

        let now = std::time::Instant::now();

        // Start child processing task
        let child_task = tokio::spawn(async move {
            let mut results = Vec::new();
            while let Some(result) = child_stream.next().await {
                results.push(result.expect("valid record batch"));
            }
            (results, now.elapsed())
        });

        // Send test data
        tx.send(create_test_record_batch(&[1, 2, 3]))
            .expect("to send record batch");
        drop(tx);

        // Delay parent completion signal
        sleep(Duration::from_millis(100)).await;
        let _ = parent_complete_tx.send(true);

        let (results, time) = child_task.await.expect("child task completed");

        // Verify data was received
        assert_eq!(results.len(), 1);
        // Verify child waited for parent
        assert!(time.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_parent_doesnt_wait() {
        let (tx, rx_parent) = broadcast::channel(32);
        let rx_child = tx.subscribe();
        let schema = create_test_schema();
        let barrier = Arc::new(Barrier::new(1));
        let (parent_complete_tx, parent_complete_rx) = watch::channel(false);

        // Create parent and child streams
        let mut parent_stream =
            RecordBatchBroadcastStream::new(rx_parent, Arc::clone(&schema), StreamType::parent());
        let mut child_stream = RecordBatchBroadcastStream::new(
            rx_child,
            Arc::clone(&schema),
            StreamType::child(barrier, parent_complete_rx),
        );

        let now = std::time::Instant::now();

        // Start parent processing task
        let parent_task = tokio::spawn(async move {
            let mut results = Vec::new();
            while let Some(result) = parent_stream.next().await {
                results.push(result.expect("valid record batch"));
            }
            (results, now.elapsed())
        });

        // Start child processing task
        let child_task = tokio::spawn(async move {
            let mut results = Vec::new();
            while let Some(result) = child_stream.next().await {
                results.push(result.expect("valid record batch"));
            }
            (results, now.elapsed())
        });

        // Send test data
        tx.send(create_test_record_batch(&[1, 2, 3]))
            .expect("to send record batch");
        drop(tx);

        // Add delay before signaling parent completion
        sleep(Duration::from_millis(100)).await;
        let _ = parent_complete_tx.send(true);

        // Wait for both tasks and handle their results properly
        let (parent_result, child_result) = tokio::join!(parent_task, child_task);

        let (parent_results, parent_time) = parent_result.expect("parent task completed");
        let (child_results, child_time) = child_result.expect("child task completed");

        // Verify both received the data
        assert_eq!(parent_results.len(), 1);
        assert_eq!(child_results.len(), 1);

        // Verify parent completed quickly without waiting
        assert!(
            parent_time.as_millis() < 50,
            "Parent took too long: {parent_time:?}"
        );
        // Verify child waited for parent completion signal
        assert!(
            child_time.as_millis() >= 100,
            "Child didn't wait long enough: {child_time:?}"
        );
    }

    #[tokio::test]
    async fn test_multiple_children_synchronization() {
        let barrier = Arc::new(Barrier::new(3)); // 3 children
        let (tx, rx1) = broadcast::channel(32);
        let rx2 = tx.subscribe();
        let rx3 = tx.subscribe();
        let (parent_complete_tx, parent_complete_rx1) = watch::channel(false);
        let parent_complete_rx2 = parent_complete_rx1.clone();
        let parent_complete_rx3 = parent_complete_rx1.clone();
        let schema = create_test_schema();

        // Create three child streams
        let stream1 = RecordBatchBroadcastStream::new(
            rx1,
            Arc::clone(&schema),
            StreamType::child(Arc::clone(&barrier), parent_complete_rx1),
        );
        let stream2 = RecordBatchBroadcastStream::new(
            rx2,
            Arc::clone(&schema),
            StreamType::child(Arc::clone(&barrier), parent_complete_rx2),
        );
        let stream3 = RecordBatchBroadcastStream::new(
            rx3,
            schema,
            StreamType::child(barrier, parent_complete_rx3),
        );

        // Start all tasks with different processing delays
        let tasks = vec![
            tokio::spawn(process_stream(stream1, Duration::from_millis(50))),
            tokio::spawn(process_stream(stream2, Duration::from_millis(100))),
            tokio::spawn(process_stream(stream3, Duration::from_millis(150))),
        ];

        // Send test data
        tx.send(create_test_record_batch(&[1, 2, 3]))
            .expect("send success");
        drop(tx);

        sleep(Duration::from_millis(50)).await;
        parent_complete_tx.send(true).expect("signal sent");

        // Wait for all tasks
        let results = futures::future::join_all(tasks).await;

        // Verify all tasks completed successfully and got the same data
        for result in results {
            let (batches, _) = result.expect("task completed");
            assert_eq!(batches.len(), 1);
            // Verify batch contents
        }
    }

    async fn process_stream(
        mut stream: RecordBatchBroadcastStream,
        delay: Duration,
    ) -> (Vec<RecordBatch>, Duration) {
        let start = std::time::Instant::now();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            sleep(delay).await;
            results.push(result.expect("valid batch"));
        }
        (results, start.elapsed())
    }
}
