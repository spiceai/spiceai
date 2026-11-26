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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_tools::record_batch::try_cast_to;
use data_components::cdc;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, stream};
use snafu::Snafu;
use tokio::sync::mpsc;

use super::RefreshTask;
use super::ingress::{ChangeStreamItem, SourceItem};
use crate::accelerated::sink::RefreshWrite;

/// A channel close is a failed producer, never permission to publish its rows.
enum SnapshotInput {
    Batch(RecordBatch),
    Complete,
}

#[derive(Debug, Snafu)]
enum SnapshotError {
    #[snafu(display("CDC snapshot ended before its boundary"))]
    MissingBoundary,
    #[snafu(display("CDC snapshot source stream panicked"))]
    SourcePanicked,
    #[snafu(display("CDC snapshot append stopped receiving rows"))]
    AppendStopped,
    #[snafu(display("CDC snapshot producer stopped before completing the snapshot"))]
    ProducerStopped,
}

impl From<SnapshotError> for DataFusionError {
    fn from(error: SnapshotError) -> Self {
        Self::External(Box::new(error))
    }
}

impl RefreshTask {
    /// Stream one snapshot into the refresh append route. No source position is
    /// acknowledged here: the returned boundary stays behind the completed write.
    pub(super) async fn consume_snapshot<S>(
        &self,
        first: cdc::ChangeEnvelope,
        source: &mut S,
        built: &mut VecDeque<ChangeStreamItem>,
        carried: &mut Option<SourceItem>,
        source_ended: bool,
    ) -> Result<(cdc::ChangeEnvelope, u64)>
    where
        S: Stream<Item = SourceItem> + Unpin + Send,
    {
        let start = Instant::now();
        tracing::debug!(dataset = %self.dataset_name, rows = 0, elapsed_ms = 0,
            "Starting CDC snapshot bulk append");
        let schema = self.accelerator.schema();
        // One waiting batch bounds ingestion ahead of the writer; capacity is
        // awaited while try_join also polls the append, including its errors.
        let (sender, receiver) = mpsc::channel(1);
        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::try_unfold(receiver, |mut receiver| async move {
                match receiver.recv().await {
                    Some(SnapshotInput::Batch(batch)) => Ok(Some((batch, receiver))),
                    Some(SnapshotInput::Complete) => Ok(None),
                    None => Err(DataFusionError::from(SnapshotError::ProducerStopped)),
                }
            }),
        ));
        let feed = feed_snapshot(first, source, built, carried, source_ended, schema, sender);
        let sink = Arc::clone(&self.sink).read_owned().await;
        let write_lock = Arc::clone(&self.accelerator_write_mutex);
        let append = async move {
            let _guard = write_lock.lock_owned().await;
            sink.insert_into(input, InsertOp::Append, &RefreshWrite::default())
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))
        };
        // The refresh CPU runtime owns the insert when configured, matching a
        // refresh append. Cancelling this future aborts that outer task. Without
        // Complete the stream cannot reach publication, even if execution owns
        // a separately spawned consumer of its input.
        let write = async {
            match self.cpu_runtime.clone() {
                Some(runtime) => {
                    let context = runtime_request_context::RequestContext::current(
                        runtime_request_context::AsyncMarker::new().await,
                    );
                    tokio_util::task::AbortOnDropHandle::new(runtime.spawn(context.scope(append)))
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                }
                None => append.await,
            }
        };
        let ((), (boundary, rows)) = tokio::try_join!(write, feed)?;
        tracing::debug!(dataset = %self.dataset_name, rows,
            elapsed_ms = start.elapsed().as_secs_f64() * 1000.0,
            "Finished CDC snapshot bulk append");
        Ok((boundary, rows))
    }
}

async fn feed_snapshot<S>(
    first: cdc::ChangeEnvelope,
    source: &mut S,
    built: &mut VecDeque<ChangeStreamItem>,
    carried: &mut Option<SourceItem>,
    source_ended: bool,
    schema: SchemaRef,
    sender: mpsc::Sender<SnapshotInput>,
) -> Result<(cdc::ChangeEnvelope, u64)>
where
    S: Stream<Item = SourceItem> + Unpin + Send,
{
    let mut envelope = first;
    let mut rows = 0_u64;
    loop {
        let (committer, batch, _, _, _) = envelope
            .into_parts_offloaded()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // Snapshot operations are all inserts: selecting every upsert
        // row is the identity. Cast to the same schema as the CDC sink.
        let batch = try_cast_to(batch.data_batch(), Arc::clone(&schema))?;
        rows += batch.num_rows() as u64;
        sender
            .send(SnapshotInput::Batch(batch))
            .await
            .map_err(|_| DataFusionError::from(SnapshotError::AppendStopped))?;
        // The snapshot contract makes these safely droppable; unlike
        // the boundary's committer they acknowledge no source position.
        drop(committer);
        let next = if let Some(item) = built.pop_front() {
            Some(Ok(item))
        } else if let Some(item) = carried.take() {
            Some(item)
        } else if source_ended {
            None
        } else {
            source.next().await
        };
        envelope = match next {
            Some(Ok(Ok(envelope))) => envelope,
            Some(Ok(Err(error))) => return Err(DataFusionError::External(Box::new(error))),
            Some(Err(_)) => return Err(SnapshotError::SourcePanicked.into()),
            None => return Err(SnapshotError::MissingBoundary.into()),
        };
        if !envelope.is_snapshot_rows() {
            sender
                .send(SnapshotInput::Complete)
                .await
                .map_err(|_| DataFusionError::from(SnapshotError::AppendStopped))?;
            return Ok((envelope, rows));
        }
    }
}
