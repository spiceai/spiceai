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

//! Ends a refresh whose source stopped sending mid-stream.
//!
//! A connection the network stops carrying (a partition, a dropped route, a
//! peer that vanished without a FIN) gives a reading client no error: a client
//! that is only reading sends nothing, so TCP never notices, and without a
//! keepalive or a read timeout the read waits for ever. The MySQL connector sets
//! neither. A full refresh caught that way never finishes and never fails, and
//! it blocks every refresh queued behind it, while the runtime stays Ready.
//!
//! Once the source has sent its first batch, a refresh that waits longer than
//! [`SOURCE_STALL_TIMEOUT`] for the next one fails with a retriable error, so it
//! is logged, counted in `dataset_acceleration_refresh_errors` under
//! `reason="source_stalled"`, and retried on a new connection. The wait before
//! the first batch is not bounded: a source may take a long time to plan and
//! start a query.

use std::time::Duration;

use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::util::retriable_error::check_and_mark_retriable_error;
use futures::{StreamExt, stream};

/// How long a refresh waits for the next batch once its source has started
/// sending. Far above any gap a streaming source leaves between batches.
pub(crate) const SOURCE_STALL_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// The error a stalled refresh fails with. Its text is what
/// [`is_source_stalled_message`] recognises in an error chain.
#[derive(Debug)]
pub(crate) struct SourceStalled {
    dataset: String,
    waited: Duration,
}

const SOURCE_STALLED_MARKER: &str = "stopped sending data mid-refresh";

impl std::fmt::Display for SourceStalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The source of dataset '{}' {SOURCE_STALLED_MARKER}: no data for {}s after it started sending. The connection is treated as lost and the refresh is retried.",
            self.dataset,
            self.waited.as_secs()
        )
    }
}

impl std::error::Error for SourceStalled {}

/// Whether an error's text is a [`SourceStalled`] error's.
pub(crate) fn is_source_stalled_message(text: &str) -> bool {
    text.contains(SOURCE_STALLED_MARKER)
}

/// `source`, failing with [`SourceStalled`] when it goes `timeout` without a
/// batch after its first one.
pub(crate) fn with_stall_timeout(
    source: SendableRecordBatchStream,
    dataset: &str,
    timeout: Duration,
) -> SendableRecordBatchStream {
    let schema = source.schema();
    let dataset = dataset.to_string();
    let guarded = stream::unfold(
        (source, false, false),
        move |(mut source, started, ended)| {
            let dataset = dataset.clone();
            async move {
                if ended {
                    return None;
                }
                let next = if started {
                    if let Ok(next) = tokio::time::timeout(timeout, source.next()).await {
                        next
                    } else {
                        let error = DataFusionError::External(Box::new(SourceStalled {
                            dataset,
                            waited: timeout,
                        }));
                        return Some((
                            Err(check_and_mark_retriable_error(error)),
                            (source, started, true),
                        ));
                    }
                } else {
                    source.next().await
                };
                next.map(|item| (item, (source, true, false)))
            }
        },
    );
    Box::pin(RecordBatchStreamAdapter::new(schema, guarded))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_table_providers::util::retriable_error::is_retriable_error;

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .expect("batch")
    }

    /// A source that sends `batches`, then goes silent without ending: what a
    /// reading client sees on a connection the network stopped carrying.
    fn silent_after(batches: usize) -> SendableRecordBatchStream {
        let schema = batch().schema();
        let sent = stream::iter((0..batches).map(|_| Ok(batch())));
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            sent.chain(stream::pending()),
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_silent_after_its_first_batch_fails_retriably() {
        let mut guarded = with_stall_timeout(silent_after(2), "big", Duration::from_secs(600));
        assert!(guarded.next().await.expect("first").is_ok());
        assert!(guarded.next().await.expect("second").is_ok());
        let error = guarded
            .next()
            .await
            .expect("a stalled source yields an error")
            .expect_err("not a batch");
        assert!(is_retriable_error(&error), "retried, not permanent: {error}");
        assert!(is_source_stalled_message(&error.to_string()), "{error}");
        assert!(guarded.next().await.is_none(), "the stream ends after the error");
    }

    #[tokio::test(start_paused = true)]
    async fn the_wait_for_the_first_batch_is_not_bounded() {
        let mut guarded = with_stall_timeout(silent_after(0), "big", Duration::from_secs(600));
        let first = tokio::time::timeout(Duration::from_secs(3600), guarded.next()).await;
        assert!(first.is_err(), "no error before the source has sent anything");
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_that_ends_is_passed_through() {
        let source: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            batch().schema(),
            stream::iter(vec![Ok(batch()), Ok(batch())]),
        ));
        let rows: Vec<_> = with_stall_timeout(source, "big", Duration::from_secs(600))
            .collect()
            .await;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(Result::is_ok));
    }
}
