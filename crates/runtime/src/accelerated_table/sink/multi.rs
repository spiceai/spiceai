/*
Copyright 2024 The Spice.ai OSS Authors

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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    catalog::TableProvider, error::DataFusionError, execution::RecordBatchStream,
    physical_plan::collect, prelude::SessionContext,
};
use futures::{Stream, StreamExt};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_stream::wrappers::BroadcastStream;
use util::RetryError;

use crate::{
    accelerated_table::{refresh_task::retry_from_df_error, synchronized_table::SynchronizedTable},
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

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: bool,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let schema = record_batch_stream.schema();
        let (tx, _) = broadcast::channel::<RecordBatch>(32);
        let mut join_set = JoinSet::new();

        let ctx = SessionContext::new();

        // Spawn tasks for each table provider
        for provider in self.table_providers() {
            let rx = tx.subscribe();
            let ctx_state = ctx.state();
            let schema = Arc::clone(&schema);

            join_set.spawn(async move {
                let stream = RecordBatchBroadcastStream::new(rx, schema);

                let insertion_plan = provider
                    .insert_into(
                        &ctx_state,
                        Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(stream))),
                        overwrite,
                    )
                    .await
                    .map_err(|e| {
                        RetryError::permanent(crate::accelerated_table::Error::FailedToWriteData {
                            source: e,
                        })
                    })?;

                collect(insertion_plan, ctx_state.task_ctx())
                    .await
                    .map_err(retry_from_df_error)?;

                Ok(())
            });
        }

        // Process the input stream and broadcast to all receivers
        let mut stream = record_batch_stream;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(retry_from_df_error)?;

            if tx.send(batch).is_err() {
                break;
            }
        }

        // Drop the sender to signal completion to all receivers
        drop(tx);

        // Wait for all tasks to complete and collect any errors
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

        // If any tasks failed, return the first error
        if let Some(first_error) = errors.into_iter().next() {
            return Err(first_error);
        }

        Ok(())
    }

    fn table_providers(&self) -> Vec<Arc<dyn TableProvider>> {
        let mut table_providers = vec![Arc::clone(&self.original_table_provider)];
        table_providers.extend(
            self.synchronized_tables
                .iter()
                .map(SynchronizedTable::child_accelerator),
        );
        table_providers
    }
}

// Stream implementation that wraps a broadcast::Receiver
struct RecordBatchBroadcastStream {
    inner: BroadcastStream<RecordBatch>,
    schema: SchemaRef,
}

impl RecordBatchBroadcastStream {
    fn new(rx: broadcast::Receiver<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            inner: BroadcastStream::new(rx),
            schema,
        }
    }
}

impl Stream for RecordBatchBroadcastStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_next_unpin(cx)
            .map(|opt| opt.map(|res| res.map_err(|e| DataFusionError::External(Box::new(e)))))
    }
}

impl RecordBatchStream for RecordBatchBroadcastStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
